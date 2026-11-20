// Lineage: what computes each column of a query, read off its bound plan
// before any optimizer touches it. Every output column resolves through
// projections, aggregates, windows, set operations and CTEs down to the base
// scans, where a leaf is the scan, the column and its type at the source. The
// text names what a column is derived from and nothing about how the plan is
// shaped, so two plans that derive a column the same way read the same and a
// source whose column arrives under another type reads different.
//
// A load records its recipe's lineage per column on its imports row; a
// gloss on a column reads stale once the derivation behind it moved, which
// is SPEC §3's re-derivation, and the row-to-row diff of the imports is the
// drift record of what the source sends.

#include <set>
#include <cstdlib>
#include <cctype>
#include "glossql_grammar.hpp"

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/crypto/md5.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/macro_catalog_entry.hpp"
#include "duckdb/function/scalar_macro_function.hpp"
#include "duckdb/function/table_macro_function.hpp"
#include "duckdb/common/sql_identifier.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/prepared_statement.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/peg/compiled_grammar.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/set_operation_node.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_any_join.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"
#include "duckdb/planner/operator/logical_expression_get.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_join.hpp"
#include "duckdb/planner/operator/logical_materialized_cte.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"
#include "duckdb/planner/operator/logical_unnest.hpp"
#include "duckdb/planner/operator/logical_window.hpp"
#include "duckdb/planner/planner.hpp"

namespace duckdb {

//! What the walk could not follow: an operator or expression it does not know,
//! `<?>`, or `<?class>` naming the expression's class
static constexpr const char *UNKNOWN = "<?";

//! A set of columns no two rows of an operator's output share
typedef vector<ColumnBinding> Key;

//! A column of a lake table as one scan of it reads it: the scan's table index
//! tells two scans of the same table apart
struct Scanned {
	idx_t scan;
	string column;
	bool operator==(const Scanned &other) const {
		return scan == other.scan && column == other.column;
	}
};

template <class T>
static void AddOnce(vector<T> &into, const vector<T> &items) {
	for (auto &item : items) {
		if (std::find(into.begin(), into.end(), item) == into.end()) {
			into.push_back(item);
		}
	}
}

//! The walk's state: a derivation text per binding resolved so far, the CTEs'
//! outputs by their index, and whether every step was followed
struct Lineage {
	column_binding_map_t<string> text;
	//! per binding, the SHAPE_* bits of what computed it
	column_binding_map_t<uint8_t> shape;
	unordered_map<TableIndex, vector<string>> ctes;
	unordered_map<TableIndex, vector<uint8_t>> cte_shapes;
	bool complete = true;
	//! the pairs of scanned columns a join or a filter equates, each as
	//! scan.column, per side whether its rows are one per value there, and the
	//! number of the join or filter, counted in `equated`
	vector<QueryLineage::JoinPair> joins;
	idx_t equated = 0;
	//! the unique keys of what a table index yields: the groups of a GROUP BY,
	//! the targets of a DISTINCT, the partition of a row_number() filtered to 1
	unordered_map<TableIndex, vector<Key>> keys;
	//! the keys that hold on an operator's output
	unordered_map<const LogicalOperator *, vector<Key>> live;
	//! a row_number() column's partition, and the same per CTE by position
	column_binding_map_t<Key> row_numbers;
	unordered_map<TableIndex, vector<vector<idx_t>>> cte_keys;
	//! per binding, the scanned columns of lake tables it is computed from, each
	//! with the scan that reads it, and the literals it tests or returns
	column_binding_map_t<vector<Scanned>> leaves;
	column_binding_map_t<vector<string>> literals;
	unordered_map<TableIndex, vector<vector<Scanned>>> cte_leaves;
	unordered_map<TableIndex, vector<vector<string>>> cte_literals;
	//! per scan of a lake table: the table, and the conjuncts of the view's joins
	//! and filters that read that scan's columns alone
	std::map<idx_t, string> scan_tables;
	std::map<idx_t, vector<string>> conditions;
	//! per scan joined to the rows before it (a join's right input, a right
	//! join's left), the equalities of that join, which look its rows up
	std::map<idx_t, vector<string>> lookups;
	//! the mapping's walk of one branch: the child each union reads, and the
	//! alias of each scan of a table the view reads more than once, which its
	//! leaves carry as scan[alias].column
	std::map<const LogicalOperator *, idx_t> branch;
	std::map<idx_t, string> aliases;

	string Unknown(const string &what = string()) {
		complete = false;
		return UNKNOWN + what + ">";
	}
};

static string Text(const Expression &expr, Lineage &lineage);

//! How a column was computed, where that decides how it adds up: it holds an
//! aggregate; it divides by one (a ratio); it averages; it counts distinct
//! values. A sum over any of the last three keeps the bit: adding ratios,
//! averages or distinct counts gives no total.
static constexpr uint8_t SHAPE_AGGREGATE = 1;
static constexpr uint8_t SHAPE_RATIO = 2;
static constexpr uint8_t SHAPE_AVERAGE = 4;
static constexpr uint8_t SHAPE_DISTINCT = 8;

static uint8_t Shape(const Expression &expr, Lineage &lineage) {
	vector<uint8_t> parts;
	ExpressionIterator::EnumerateChildren(expr,
	                                      [&](const Expression &child) { parts.push_back(Shape(child, lineage)); });
	uint8_t all = 0;
	for (auto part : parts) {
		all |= part;
	}
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_COLUMN_REF: {
		auto entry = lineage.shape.find(expr.Cast<BoundColumnRefExpression>().Binding());
		return entry == lineage.shape.end() ? 0 : entry->second;
	}
	case ExpressionClass::BOUND_FUNCTION: {
		auto name = expr.Cast<BoundFunctionExpression>().Function().GetName().GetIdentifierName();
		if ((name == "/" || name == "//" || name == "divide") && parts.size() == 2 && (parts[1] & SHAPE_AGGREGATE)) {
			all |= SHAPE_RATIO;
		}
		return all;
	}
	case ExpressionClass::BOUND_AGGREGATE: {
		auto &aggregate = expr.Cast<BoundAggregateExpression>();
		auto name = aggregate.Function().GetName().GetIdentifierName();
		all |= SHAPE_AGGREGATE;
		if (name == "avg" || name == "mean" || name == "median" || StringUtil::StartsWith(name, "quantile")) {
			all |= SHAPE_AVERAGE;
		}
		if (aggregate.IsDistinct() || name == "approx_count_distinct") {
			all |= SHAPE_DISTINCT;
		}
		return all;
	}
	case ExpressionClass::BOUND_WINDOW: {
		auto &window = expr.Cast<BoundWindowExpression>();
		if (window.AggregateFunction()) {
			all |= SHAPE_AGGREGATE;
			auto name = window.AggregateFunction()->GetName().GetIdentifierName();
			if (name == "avg" || name == "mean" || name == "median" || StringUtil::StartsWith(name, "quantile")) {
				all |= SHAPE_AVERAGE;
			}
		}
		return all;
	}
	default:
		return all;
	}
}

static string ShapeName(uint8_t shape) {
	return shape & SHAPE_RATIO ? "ratio" : shape & SHAPE_AVERAGE ? "average" : shape & SHAPE_DISTINCT ? "distinct" : "";
}

//! The scanned columns an expression is computed from, in the order it names them
static void Leaves(const Expression &expr, Lineage &lineage, vector<Scanned> &into) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
		auto entry = lineage.leaves.find(expr.Cast<BoundColumnRefExpression>().Binding());
		if (entry != lineage.leaves.end()) {
			AddOnce(into, entry->second);
		}
		return;
	}
	ExpressionIterator::EnumerateChildren(expr, [&](const Expression &child) { Leaves(child, lineage, into); });
}

//! The literals an expression tests or returns, as written: an operand of a
//! comparison, IN, a conjunction, a CASE or a COALESCE, or the expression
//! itself; a cast passes its operand on, and an argument of any other function
//! or of arithmetic (a factor, a format, a date part) is none
static void Literals(const Expression &expr, bool counts, Lineage &lineage, vector<string> &into) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_COLUMN_REF: {
		auto entry = lineage.literals.find(expr.Cast<BoundColumnRefExpression>().Binding());
		if (entry != lineage.literals.end()) {
			AddOnce(into, entry->second);
		}
		return;
	}
	case ExpressionClass::BOUND_CONSTANT: {
		auto &value = expr.Cast<BoundConstantExpression>().GetValue();
		if (counts && !value.IsNull()) {
			AddOnce(into, {value.ToSQLString()});
		}
		return;
	}
	case ExpressionClass::BOUND_FUNCTION:
		// a comparison the binder makes a function keeps its own expression type
		if (!BoundCastExpression::IsCast(expr)) {
			counts = expr.GetExpressionType() != ExpressionType::BOUND_FUNCTION;
		}
		break;
	case ExpressionClass::LEGACY_BOUND_CAST:
		break;
	case ExpressionClass::LEGACY_BOUND_COMPARISON:
	case ExpressionClass::LEGACY_BOUND_BETWEEN:
	case ExpressionClass::BOUND_CONJUNCTION:
	case ExpressionClass::BOUND_OPERATOR:
	case ExpressionClass::BOUND_CASE:
		counts = true;
		break;
	default:
		counts = false;
	}
	ExpressionIterator::EnumerateChildren(expr,
	                                      [&](const Expression &child) { Literals(child, counts, lineage, into); });
}

//! Records what computes a binding: its derivation text, its shape, the
//! scanned columns and the literals
static void Derive(Lineage &lineage, ColumnBinding binding, const Expression &expr) {
	lineage.text[binding] = Text(expr, lineage);
	lineage.shape[binding] = Shape(expr, lineage);
	vector<Scanned> leaves;
	Leaves(expr, lineage, leaves);
	lineage.leaves[binding] = std::move(leaves);
	vector<string> literals;
	Literals(expr, true, lineage, literals);
	lineage.literals[binding] = std::move(literals);
}

//! Records a conjunct of a join or a filter as a condition of the one scan whose
//! columns it reads, where it reads one scan's alone
static void Condition(const vector<const Expression *> &operands, const string &text, Lineage &lineage) {
	vector<Scanned> leaves;
	for (auto operand : operands) {
		Leaves(*operand, lineage, leaves);
	}
	for (auto &leaf : leaves) {
		if (leaf.scan != leaves[0].scan) {
			return;
		}
	}
	if (!leaves.empty()) {
		AddOnce(lineage.conditions[leaves[0].scan], {text});
	}
}

//! Records an equality of a join between its two inputs as a lookup of the scans
//! the joined input reads (the right one, the left of a right join), where each
//! side reads a scan
static void Lookup(const Expression &left, const Expression &right, bool right_join, const string &text,
                   Lineage &lineage) {
	vector<Scanned> lefts;
	vector<Scanned> rights;
	Leaves(left, lineage, lefts);
	Leaves(right, lineage, rights);
	if (lefts.empty() || rights.empty()) {
		return;
	}
	for (auto &leaf : right_join ? lefts : rights) {
		AddOnce(lineage.lookups[leaf.scan], {text});
	}
}

//! The conditions of a predicate: each side of an AND
static void Conditions(const Expression &expr, Lineage &lineage) {
	if (expr.GetExpressionType() == ExpressionType::CONJUNCTION_AND) {
		ExpressionIterator::EnumerateChildren(expr, [&](const Expression &child) { Conditions(child, lineage); });
		return;
	}
	bool complete = lineage.complete;
	auto text = Text(expr, lineage);
	lineage.complete = complete;
	Condition({&expr}, text, lineage);
}

static vector<uint8_t> OutputShapes(LogicalOperator &op, Lineage &lineage) {
	vector<uint8_t> result;
	for (auto &binding : op.GetColumnBindings()) {
		auto entry = lineage.shape.find(binding);
		result.push_back(entry == lineage.shape.end() ? 0 : entry->second);
	}
	return result;
}

static string Children(const Expression &expr, Lineage &lineage) {
	vector<string> parts;
	ExpressionIterator::EnumerateChildren(expr,
	                                      [&](const Expression &child) { parts.push_back(Text(child, lineage)); });
	return StringUtil::Join(parts, ", ");
}

static string Texts(const vector<unique_ptr<Expression>> &exprs, Lineage &lineage) {
	vector<string> parts;
	for (auto &expr : exprs) {
		parts.push_back(Text(*expr, lineage));
	}
	return StringUtil::Join(parts, ", ");
}

//! An expression's type as a name in the text: is_null, coalesce, in
static string TypeName(const Expression &expr) {
	return StringUtil::Lower(ExpressionTypeToString(expr.GetExpressionType()));
}

//! A cast of a literal to the type it already has, as the binder leaves on a
//! string compared with text: it changes nothing, and the literal reads as written
static bool LiteralCast(const Expression &expr) {
	if (!BoundCastExpression::IsCast(expr)) {
		return false;
	}
	auto &child = BoundCastExpression::Child(expr.Cast<BoundFunctionExpression>());
	return child.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT &&
	       child.Cast<BoundConstantExpression>().GetValue().type() == expr.GetReturnType();
}

//! The derivation text of a bound expression: a column reference is what it
//! resolves to, a constant its literal as written, a function its name over
//! its arguments, an operator, a comparison, a conjunction or a CASE its own
//! name over its operands; an expression the walk cannot render is unknown
//! under its class's name
static string Text(const Expression &expr, Lineage &lineage) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_COLUMN_REF: {
		auto &ref = expr.Cast<BoundColumnRefExpression>();
		auto entry = lineage.text.find(ref.Binding());
		return entry == lineage.text.end() ? lineage.Unknown() : entry->second;
	}
	case ExpressionClass::BOUND_CONSTANT:
		return expr.Cast<BoundConstantExpression>().GetValue().ToSQLString();
	case ExpressionClass::BOUND_FUNCTION: {
		auto &function = expr.Cast<BoundFunctionExpression>();
		if (LiteralCast(expr)) {
			return Text(BoundCastExpression::Child(function), lineage);
		}
		if (BoundCastExpression::IsCast(expr)) {
			return (BoundCastExpression::IsTryCast(function) ? "try_cast(" : "cast(") + Children(expr, lineage) +
			       " as " + expr.GetReturnType().ToString() + ")";
		}
		// IS DISTINCT FROM reads is_distinct_from, as is_null does
		auto name = function.Function().GetName().GetIdentifierName();
		if (name.find(' ') != string::npos) {
			name = StringUtil::Lower(StringUtil::Replace(name, " ", "_"));
		}
		return name + "(" + Children(expr, lineage) + ")";
	}
	case ExpressionClass::BOUND_AGGREGATE: {
		auto &aggregate = expr.Cast<BoundAggregateExpression>();
		return aggregate.Function().GetName().GetIdentifierName() + (aggregate.IsDistinct() ? "(distinct " : "(") +
		       Children(expr, lineage) + ")";
	}
	case ExpressionClass::LEGACY_BOUND_CAST:
		return "cast(" + Children(expr, lineage) + " as " + expr.GetReturnType().ToString() + ")";
	case ExpressionClass::BOUND_WINDOW: {
		auto &window = expr.Cast<BoundWindowExpression>();
		auto name =
		    window.AggregateFunction() ? window.AggregateFunction()->GetName().GetIdentifierName() : TypeName(expr);
		auto text = name + "(" + Texts(window.GetChildren(), lineage) + ")";
		if (window.Filter()) {
			text += " filter(" + Text(*window.Filter(), lineage) + ")";
		}
		vector<string> over;
		if (!window.Partitions().empty()) {
			over.push_back("partition by " + Texts(window.Partitions(), lineage));
		}
		if (!window.OrderBy().empty()) {
			vector<string> orders;
			for (auto &order : window.OrderBy()) {
				orders.push_back(Text(*order.expression, lineage) +
				                 (order.type == OrderType::DESCENDING ? " desc" : ""));
			}
			over.push_back("order by " + StringUtil::Join(orders, ", "));
		}
		for (auto bound : {window.StartExpr().get(), window.EndExpr().get()}) {
			if (bound) {
				over.push_back("frame " + Text(*bound, lineage));
			}
		}
		return text + " over(" + StringUtil::Join(over, " ") + ")";
	}
	case ExpressionClass::BOUND_PARAMETER:
		return "$" + expr.Cast<BoundParameterExpression>().Identifier().GetIdentifierName();
	case ExpressionClass::LEGACY_BOUND_COMPARISON:
		return ExpressionTypeToOperator(expr.GetExpressionType()) + "(" + Children(expr, lineage) + ")";
	case ExpressionClass::LEGACY_BOUND_BETWEEN:
		return "between(" + Children(expr, lineage) + ")";
	case ExpressionClass::BOUND_CONJUNCTION:
		return string(expr.GetExpressionType() == ExpressionType::CONJUNCTION_AND ? "and" : "or") + "(" +
		       Children(expr, lineage) + ")";
	case ExpressionClass::BOUND_OPERATOR: {
		auto type = expr.GetExpressionType();
		auto name = type == ExpressionType::COMPARE_NOT_IN ? string("not_in") : TypeName(expr);
		return name + "(" + Children(expr, lineage) + ")";
	}
	case ExpressionClass::BOUND_CASE: {
		auto &bound = expr.Cast<BoundCaseExpression>();
		vector<string> parts;
		for (auto &check : bound.CaseChecks()) {
			parts.push_back("when " + Text(*check.when_expr, lineage) + " then " + Text(*check.then_expr, lineage));
		}
		parts.push_back("else " + Text(bound.Else(), lineage));
		return "case(" + StringUtil::Join(parts, ", ") + ")";
	}
	case ExpressionClass::BOUND_UNNEST:
		return "unnest(" + Children(expr, lineage) + ")";
	case ExpressionClass::BOUND_LAMBDA:
	case ExpressionClass::BOUND_LAMBDA_REF:
	case ExpressionClass::BOUND_DEFAULT:
		return TypeName(expr) + "(" + Children(expr, lineage) + ")";
	default:
		return lineage.Unknown(StringUtil::Lower(ExpressionClassToString(expr.GetExpressionClass())));
	}
}

//! The texts of an operator's output bindings, in order
static vector<string> Outputs(LogicalOperator &op, Lineage &lineage) {
	vector<string> result;
	for (auto &binding : op.GetColumnBindings()) {
		auto entry = lineage.text.find(binding);
		result.push_back(entry == lineage.text.end() ? lineage.Unknown() : entry->second);
	}
	return result;
}

//! A scan's name in a leaf: schema.table for a catalog table, the function's
//! name otherwise; the catalog is left out, an attached database's alias being
//! the session's
static string ScanName(LogicalGet &get) {
	auto table = get.GetTable();
	if (table) {
		return table->ParentSchema().name.GetIdentifierName() + "." + table->name.GetIdentifierName();
	}
	return string(get.function.GetDefinition()->name);
}

//! A derivation that is one scanned column, as scan.column without its type; empty otherwise
static string Leaf(const string &text) {
	auto colon = text.rfind(':');
	if (colon == string::npos || text.find('(') != string::npos || text.find(' ') != string::npos) {
		return string();
	}
	return text.substr(0, colon);
}

//! The binding of an expression that is one column reference
static bool Plain(const Expression &expr, ColumnBinding &binding) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
		return false;
	}
	binding = expr.Cast<BoundColumnRefExpression>().Binding();
	return true;
}

static bool Within(const Key &key, const vector<ColumnBinding> &columns) {
	for (auto &part : key) {
		if (std::find(columns.begin(), columns.end(), part) == columns.end()) {
			return false;
		}
	}
	return !key.empty();
}

//! A key that holds on an operator's output, kept under its columns' table index
static void Hold(Lineage &lineage, const LogicalOperator &op, const Key &key) {
	if (key.empty()) {
		return;
	}
	lineage.keys[key[0].table_index].push_back(key);
	lineage.live[&op].push_back(key);
}

//! The keys of the child that hold on the operator's output as they are
static void Keep(Lineage &lineage, const LogicalOperator &op, const LogicalOperator &child) {
	auto entry = lineage.live.find(&child);
	if (entry != lineage.live.end()) {
		auto keys = entry->second;
		auto &live = lineage.live[&op];
		live.insert(live.end(), keys.begin(), keys.end());
	}
}

//! Whether some key that holds on an operator's output lies within the columns
static bool UniqueOn(Lineage &lineage, const LogicalOperator &op, const vector<ColumnBinding> &columns) {
	auto entry = lineage.live.find(&op);
	if (entry == lineage.live.end()) {
		return false;
	}
	for (auto &key : entry->second) {
		if (Within(key, columns)) {
			return true;
		}
	}
	return false;
}

//! Two expressions one predicate equates
struct Equality {
	const Expression *left;
	const Expression *right;
};

//! The column equalities of a predicate: `a = b`, and each side of an AND
static void Equalities(const Expression &expr, vector<Equality> &equalities) {
	auto type = expr.GetExpressionType();
	if (type == ExpressionType::COMPARE_EQUAL) {
		vector<const Expression *> sides;
		ExpressionIterator::EnumerateChildren(expr, [&](const Expression &child) { sides.push_back(&child); });
		if (sides.size() == 2) {
			equalities.push_back({sides[0], sides[1]});
		}
	} else if (type == ExpressionType::CONJUNCTION_AND) {
		ExpressionIterator::EnumerateChildren(expr, [&](const Expression &child) { Equalities(child, equalities); });
	}
}

//! The equalities of one join or filter: a pair where each side is one scanned
//! column of another scan than the other's. A side is unique where the columns
//! the predicate equates cover a key of what yields them: a subquery grouped
//! by them, or reduced to one row per value of them
static void Equated(const vector<Equality> &equalities, Lineage &lineage) {
	vector<ColumnBinding> columns;
	for (auto &equality : equalities) {
		ColumnBinding binding;
		if (Plain(*equality.left, binding)) {
			columns.push_back(binding);
		}
		if (Plain(*equality.right, binding)) {
			columns.push_back(binding);
		}
	}
	auto unique = [&](const Expression &side) {
		ColumnBinding binding;
		if (!Plain(side, binding)) {
			return false;
		}
		auto entry = lineage.keys.find(binding.table_index);
		if (entry == lineage.keys.end()) {
			return false;
		}
		for (auto &key : entry->second) {
			if (Within(key, columns)) {
				return true;
			}
		}
		return false;
	};
	auto join = lineage.equated + 1;
	for (auto &equality : equalities) {
		bool complete = lineage.complete;
		auto a = Leaf(Text(*equality.left, lineage));
		auto b = Leaf(Text(*equality.right, lineage));
		lineage.complete = complete; // a side the walk cannot follow is no pair and no gap in the lineage
		if (a.empty() || b.empty() || a == b) {
			continue;
		}
		bool known = false;
		for (auto &pair : lineage.joins) {
			known = known ||
			        (pair.join == join && ((pair.left == a && pair.right == b) || (pair.left == b && pair.right == a)));
		}
		if (!known) {
			lineage.joins.push_back({a, b, unique(*equality.left), unique(*equality.right), join});
			lineage.equated = join;
		}
	}
}

static void Walk(LogicalOperator &op, Lineage &lineage) {
	if (op.type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE) {
		// the definition first, so the references to it resolve
		auto &cte = op.Cast<LogicalMaterializedCTE>();
		Walk(*op.children[0], lineage);
		lineage.ctes[cte.table_index] = Outputs(*op.children[0], lineage);
		lineage.cte_shapes[cte.table_index] = OutputShapes(*op.children[0], lineage);
		auto bindings = op.children[0]->GetColumnBindings();
		for (auto &binding : bindings) {
			lineage.cte_leaves[cte.table_index].push_back(lineage.leaves[binding]);
			lineage.cte_literals[cte.table_index].push_back(lineage.literals[binding]);
		}
		for (auto &key : lineage.live[op.children[0].get()]) {
			vector<idx_t> positions;
			for (auto &part : key) {
				auto at = std::find(bindings.begin(), bindings.end(), part);
				if (at == bindings.end()) {
					positions.clear();
					break;
				}
				positions.push_back(NumericCast<idx_t>(at - bindings.begin()));
			}
			if (!positions.empty()) {
				lineage.cte_keys[cte.table_index].push_back(positions);
			}
		}
		Walk(*op.children[1], lineage);
		Keep(lineage, op, *op.children[1]);
		return;
	}
	for (auto &child : op.children) {
		Walk(*child, lineage);
	}
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_GET: {
		auto &get = op.Cast<LogicalGet>();
		auto scan = ScanName(get);
		auto &column_ids = get.GetColumnIds();
		auto bindings = get.GetColumnBindings();
		if (column_ids.empty()) {
			// a scan no column of which is read: its rows alone
			lineage.text[bindings[0]] = scan;
			break;
		}
		auto alias = lineage.aliases.find(get.table_index.index);
		if (get.GetTable()) {
			lineage.scan_tables[get.table_index.index] = scan;
		}
		for (idx_t i = 0; i < column_ids.size() && i < bindings.size(); i++) {
			auto &column = column_ids[i];
			auto name = get.GetColumnName(column).GetIdentifierName();
			auto leaf = (alias == lineage.aliases.end() ? scan : scan + "[" + alias->second + "]") + "." + name;
			if (!column.IsVirtualColumn()) {
				leaf += ":" + get.returned_types[column.GetPrimaryIndex()].ToString();
				if (get.GetTable()) {
					lineage.leaves[bindings[i]] = {Scanned {get.table_index.index, name}};
				}
			}
			lineage.text[bindings[i]] = leaf;
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		auto &projection = op.Cast<LogicalProjection>();
		for (idx_t i = 0; i < projection.expressions.size(); i++) {
			Derive(lineage, ColumnBinding(projection.table_index, ProjectionIndex(i)), *projection.expressions[i]);
		}
		// a key holds where the projection passes each of its columns on as it is
		auto passed = [&](const Key &key, Key &result) {
			for (auto &part : key) {
				bool found = false;
				for (idx_t i = 0; i < projection.expressions.size() && !found; i++) {
					ColumnBinding binding;
					if (Plain(*projection.expressions[i], binding) && binding == part) {
						result.emplace_back(projection.table_index, ProjectionIndex(i));
						found = true;
					}
				}
				if (!found) {
					return false;
				}
			}
			return true;
		};
		auto below = lineage.live[op.children[0].get()];
		for (auto &key : below) {
			Key result;
			if (passed(key, result)) {
				Hold(lineage, op, result);
			}
		}
		for (idx_t i = 0; i < projection.expressions.size(); i++) {
			ColumnBinding binding;
			if (!Plain(*projection.expressions[i], binding)) {
				continue;
			}
			auto entry = lineage.row_numbers.find(binding);
			Key result;
			if (entry != lineage.row_numbers.end() && passed(Key(entry->second), result)) {
				lineage.row_numbers[ColumnBinding(projection.table_index, ProjectionIndex(i))] = result;
			}
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		auto &aggregate = op.Cast<LogicalAggregate>();
		for (idx_t i = 0; i < aggregate.groups.size(); i++) {
			Derive(lineage, ColumnBinding(aggregate.group_index, ProjectionIndex(i)), *aggregate.groups[i]);
		}
		for (idx_t i = 0; i < aggregate.expressions.size(); i++) {
			Derive(lineage, ColumnBinding(aggregate.aggregate_index, ProjectionIndex(i)), *aggregate.expressions[i]);
		}
		for (idx_t i = 0; i < aggregate.grouping_functions.size(); i++) {
			lineage.text[ColumnBinding(aggregate.groupings_index, ProjectionIndex(i))] = "grouping";
		}
		if (aggregate.grouping_sets.size() <= 1) {
			Key key;
			for (idx_t i = 0; i < aggregate.groups.size(); i++) {
				key.emplace_back(aggregate.group_index, ProjectionIndex(i));
			}
			Hold(lineage, op, key);
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_WINDOW: {
		auto &window = op.Cast<LogicalWindow>();
		for (idx_t i = 0; i < window.expressions.size(); i++) {
			Derive(lineage, ColumnBinding(window.window_index, ProjectionIndex(i)), *window.expressions[i]);
			if (window.expressions[i]->GetExpressionType() != ExpressionType::WINDOW_ROW_NUMBER) {
				continue;
			}
			Key partition;
			for (auto &part : window.expressions[i]->Cast<BoundWindowExpression>().Partitions()) {
				ColumnBinding binding;
				if (!Plain(*part, binding)) {
					partition.clear();
					break;
				}
				partition.push_back(binding);
			}
			if (!partition.empty()) {
				lineage.row_numbers[ColumnBinding(window.window_index, ProjectionIndex(i))] = partition;
			}
		}
		Keep(lineage, op, *op.children[0]);
		break;
	}
	case LogicalOperatorType::LOGICAL_UNNEST: {
		auto &unnest = op.Cast<LogicalUnnest>();
		for (idx_t i = 0; i < unnest.expressions.size(); i++) {
			Derive(lineage, ColumnBinding(unnest.unnest_index, ProjectionIndex(i)), *unnest.expressions[i]);
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_UNION:
	case LogicalOperatorType::LOGICAL_EXCEPT:
	case LogicalOperatorType::LOGICAL_INTERSECT: {
		auto &setop = op.Cast<LogicalSetOperation>();
		auto word = op.type == LogicalOperatorType::LOGICAL_UNION    ? "union"
		            : op.type == LogicalOperatorType::LOGICAL_EXCEPT ? "except"
		                                                             : "intersect";
		vector<vector<string>> sides;
		vector<vector<uint8_t>> side_shapes;
		// the mapping's walk of one branch reads the one child of the union it chose
		auto chosen = lineage.branch.find(&op);
		for (idx_t c = 0; c < op.children.size(); c++) {
			if (chosen != lineage.branch.end() && chosen->second != c) {
				continue;
			}
			auto &child = op.children[c];
			sides.push_back(Outputs(*child, lineage));
			side_shapes.push_back(OutputShapes(*child, lineage));
			auto bindings = child->GetColumnBindings();
			for (idx_t i = 0; i < setop.column_count && i < bindings.size(); i++) {
				ColumnBinding out(setop.table_index, ProjectionIndex(i));
				AddOnce(lineage.leaves[out], lineage.leaves[bindings[i]]);
				AddOnce(lineage.literals[out], lineage.literals[bindings[i]]);
			}
		}
		for (idx_t i = 0; i < setop.column_count; i++) {
			vector<string> parts;
			for (auto &side : sides) {
				parts.push_back(i < side.size() ? side[i] : lineage.Unknown());
			}
			lineage.text[ColumnBinding(setop.table_index, ProjectionIndex(i))] =
			    chosen != lineage.branch.end() && parts.size() == 1
			        ? parts[0]
			        : string(word) + "(" + StringUtil::Join(parts, ", ") + ")";
			uint8_t shape = 0;
			for (auto &side : side_shapes) {
				if (i < side.size()) {
					shape = static_cast<uint8_t>(static_cast<unsigned>(shape) | static_cast<unsigned>(side[i]));
				}
			}
			lineage.shape[ColumnBinding(setop.table_index, ProjectionIndex(i))] = shape;
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_CTE_REF: {
		auto &ref = op.Cast<LogicalCTERef>();
		auto entry = lineage.ctes.find(ref.cte_index);
		for (idx_t i = 0; i < ref.chunk_types.size(); i++) {
			lineage.text[ColumnBinding(ref.table_index, ProjectionIndex(i))] =
			    entry != lineage.ctes.end() && i < entry->second.size() ? entry->second[i] : lineage.Unknown();
			auto shapes = lineage.cte_shapes.find(ref.cte_index);
			lineage.shape[ColumnBinding(ref.table_index, ProjectionIndex(i))] =
			    shapes != lineage.cte_shapes.end() && i < shapes->second.size() ? shapes->second[i] : 0;
			auto leaves = lineage.cte_leaves.find(ref.cte_index);
			if (leaves != lineage.cte_leaves.end() && i < leaves->second.size()) {
				lineage.leaves[ColumnBinding(ref.table_index, ProjectionIndex(i))] = leaves->second[i];
			}
			auto literals = lineage.cte_literals.find(ref.cte_index);
			if (literals != lineage.cte_literals.end() && i < literals->second.size()) {
				lineage.literals[ColumnBinding(ref.table_index, ProjectionIndex(i))] = literals->second[i];
			}
		}
		auto keys = lineage.cte_keys.find(ref.cte_index);
		if (keys != lineage.cte_keys.end()) {
			for (auto &positions : keys->second) {
				Key key;
				for (auto position : positions) {
					key.emplace_back(ref.table_index, ProjectionIndex(position));
				}
				Hold(lineage, op, key);
			}
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_EXPRESSION_GET: {
		auto &values = op.Cast<LogicalExpressionGet>();
		for (idx_t i = 0; i < values.expr_types.size(); i++) {
			lineage.text[ColumnBinding(values.table_index, ProjectionIndex(i))] = "values";
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
	case LogicalOperatorType::LOGICAL_ASOF_JOIN:
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
	case LogicalOperatorType::LOGICAL_DEPENDENT_JOIN:
	case LogicalOperatorType::LOGICAL_JOIN: {
		auto &join = op.Cast<LogicalJoin>();
		if (join.join_type == JoinType::MARK) {
			lineage.text[ColumnBinding(join.mark_index, ProjectionIndex(0))] = "mark";
		}
		vector<Equality> equalities;
		if (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
		    op.type == LogicalOperatorType::LOGICAL_ASOF_JOIN || op.type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
			for (auto &condition : op.Cast<LogicalComparisonJoin>().conditions) {
				if (!condition.IsComparison()) {
					Equalities(condition.GetJoinExpression(), equalities);
					Conditions(condition.GetJoinExpression(), lineage);
					continue;
				}
				if (condition.GetComparisonType() == ExpressionType::COMPARE_EQUAL) {
					equalities.push_back({&condition.GetLHS(), &condition.GetRHS()});
				}
				bool complete = lineage.complete;
				auto text = ExpressionTypeToOperator(condition.GetComparisonType()) + "(" +
				            Text(condition.GetLHS(), lineage) + ", " + Text(condition.GetRHS(), lineage) + ")";
				lineage.complete = complete;
				Condition({&condition.GetLHS(), &condition.GetRHS()}, text, lineage);
				if (condition.GetComparisonType() == ExpressionType::COMPARE_EQUAL) {
					Lookup(condition.GetLHS(), condition.GetRHS(), join.join_type == JoinType::RIGHT, text, lineage);
				}
			}
		} else if (op.type == LogicalOperatorType::LOGICAL_ANY_JOIN) {
			Equalities(*op.Cast<LogicalAnyJoin>().condition, equalities);
			Conditions(*op.Cast<LogicalAnyJoin>().condition, lineage);
		}
		Equated(equalities, lineage);
		if (op.type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN || op.children.size() != 2) {
			break;
		}
		// a side's keys hold on the join where the other side matches each row once at most
		vector<ColumnBinding> columns;
		for (auto &equality : equalities) {
			ColumnBinding binding;
			if (Plain(*equality.left, binding)) {
				columns.push_back(binding);
			}
			if (Plain(*equality.right, binding)) {
				columns.push_back(binding);
			}
		}
		auto type = join.join_type;
		bool once =
		    type == JoinType::SEMI || type == JoinType::ANTI || type == JoinType::MARK || type == JoinType::SINGLE;
		bool both =
		    type == JoinType::INNER || type == JoinType::LEFT || type == JoinType::RIGHT || type == JoinType::OUTER;
		bool left_unique = UniqueOn(lineage, *op.children[0], columns);
		bool right_unique = UniqueOn(lineage, *op.children[1], columns);
		if (once || (both && right_unique)) {
			Keep(lineage, op, *op.children[0]);
		}
		if (both && left_unique) {
			Keep(lineage, op, *op.children[1]);
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_FILTER: {
		// a join written in the WHERE clause
		vector<Equality> equalities;
		for (auto &expr : op.expressions) {
			Equalities(*expr, equalities);
			Conditions(*expr, lineage);
		}
		Equated(equalities, lineage);
		Keep(lineage, op, *op.children[0]);
		// row_number() = 1 leaves one row per value of its partition
		for (auto &equality : equalities) {
			for (auto swap : {false, true}) {
				auto &column = swap ? *equality.right : *equality.left;
				auto &other = swap ? *equality.left : *equality.right;
				ColumnBinding binding;
				if (!Plain(column, binding)) {
					continue;
				}
				auto entry = lineage.row_numbers.find(binding);
				if (entry == lineage.row_numbers.end()) {
					continue;
				}
				bool complete = lineage.complete;
				auto one = Text(other, lineage);
				lineage.complete = complete;
				if (one == "1" || StringUtil::StartsWith(one, "cast(1 as ")) {
					Hold(lineage, op, Key(entry->second));
				}
			}
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_DISTINCT: {
		Keep(lineage, op, *op.children[0]);
		Key key;
		for (auto &target : op.Cast<LogicalDistinct>().distinct_targets) {
			ColumnBinding binding;
			if (!Plain(*target, binding)) {
				key.clear();
				break;
			}
			key.push_back(binding);
		}
		Hold(lineage, op, key);
		break;
	}
	case LogicalOperatorType::LOGICAL_LIMIT:
	case LogicalOperatorType::LOGICAL_ORDER_BY:
	case LogicalOperatorType::LOGICAL_TOP_N:
	case LogicalOperatorType::LOGICAL_SAMPLE:
		// the child's bindings are the outputs: rows leave, columns do not change
		Keep(lineage, op, *op.children[0]);
		break;
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
	case LogicalOperatorType::LOGICAL_POSITIONAL_JOIN:
	case LogicalOperatorType::LOGICAL_DUMMY_SCAN:
	case LogicalOperatorType::LOGICAL_EMPTY_RESULT:
		// the children's bindings are the outputs
		break;
	default:
		lineage.complete = false;
	}
}

//! Parses plain SQL into one SELECT, or nothing
static unique_ptr<SelectStatement> ParseOneSelect(const string &sql) {
	auto options = ParserOptions::Builtin();
	options.identifier_case_mode = IdentifierCaseMode::LOWERCASE;
	options.compiled_grammar = CompiledGrammar::DefaultGrammar();
	Parser parser(options);
	parser.ParseQuery(sql);
	if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::SELECT_STATEMENT) {
		return nullptr;
	}
	return unique_ptr_cast<SQLStatement, SelectStatement>(std::move(parser.statements[0]));
}

//! The lake tables a plan scans, as schema.table, each scan with the columns the
//! query reads of it, its select list, joins and filters included: the binder
//! names a scan's column once something refers to it
static void Scans(LogicalOperator &op, std::set<std::pair<string, vector<string>>> &scans) {
	if (op.type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op.Cast<LogicalGet>();
		auto table = get.GetTable();
		if (table) {
			std::set<string> columns;
			for (auto &id : get.GetColumnIds()) {
				if (!id.IsRowIdColumn() && id.GetPrimaryIndex() < get.names.size()) {
					columns.insert(get.names[id.GetPrimaryIndex()].GetIdentifierName());
				}
			}
			scans.insert({table->ParentSchema().name.GetIdentifierName() + "." + table->name.GetIdentifierName(),
			              vector<string>(columns.begin(), columns.end())});
		}
	}
	for (auto &child : op.children) {
		Scans(*child, scans);
	}
}

//===--------------------------------------------------------------------===//
// The mapping of a view: per output column and per branch of the union its
// rows come from, what computes it, with each scanned column, the alias and
// the conditions of the scan that reads it and the literals it tests or returns
//===--------------------------------------------------------------------===//

//! The first unions on the way down from an operator through what passes rows
//! on, either side of a join and a CTE's definition included
static void FirstUnions(LogicalOperator &op, unordered_map<TableIndex, LogicalOperator *> &ctes,
                        vector<LogicalOperator *> &found) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_UNION:
		found.push_back(&op);
		return;
	case LogicalOperatorType::LOGICAL_EXCEPT:
	case LogicalOperatorType::LOGICAL_INTERSECT:
		return;
	case LogicalOperatorType::LOGICAL_MATERIALIZED_CTE:
		ctes[op.Cast<LogicalMaterializedCTE>().table_index] = op.children[0].get();
		FirstUnions(*op.children[1], ctes, found);
		return;
	case LogicalOperatorType::LOGICAL_CTE_REF: {
		auto entry = ctes.find(op.Cast<LogicalCTERef>().cte_index);
		if (entry != ctes.end()) {
			FirstUnions(*entry->second, ctes, found);
		}
		return;
	}
	default:
		for (auto &child : op.children) {
			FirstUnions(*child, ctes, found);
		}
	}
}

typedef std::map<const LogicalOperator *, idx_t> Branch;

//! The branches of a union, one per child, a child that is a union itself
//! giving one per child of its own
static void Branches(LogicalOperator &setop, Branch branch, vector<Branch> &into) {
	for (idx_t c = 0; c < setop.children.size(); c++) {
		branch[&setop] = c;
		if (setop.children[c]->type == LogicalOperatorType::LOGICAL_UNION) {
			Branches(*setop.children[c], branch, into);
		} else {
			into.push_back(branch);
		}
	}
}

//! The scans of each lake table in a plan, by its name, in the order the binder made them
static void TableScans(LogicalOperator &op, std::map<string, vector<idx_t>> &scans) {
	if (op.type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op.Cast<LogicalGet>();
		if (get.GetTable()) {
			scans[StringUtil::Lower(get.GetTable()->name.GetIdentifierName())].push_back(get.table_index.index);
		}
	}
	for (auto &child : op.children) {
		TableScans(*child, scans);
	}
}

typedef std::map<string, const QueryNode *> CteScope;

//! A relation a query's text names: its schema as written (empty where it names none), its name and
//! the alias it gives it; or a function it calls, scalar or table, by schema and name
struct NamedRef {
	string schema;
	string table;
	string alias;
	bool function = false;
};

static void TableRefs(const QueryNode &node, CteScope scope, vector<NamedRef> &into);

static void TableRefs(const ParsedExpression &expr, const CteScope &scope, vector<NamedRef> &into) {
	if (expr.GetExpressionClass() == ExpressionClass::SUBQUERY) {
		TableRefs(*expr.Cast<SubqueryExpression>().Subquery()->node, scope, into);
		return;
	}
	if (expr.GetExpressionClass() == ExpressionClass::FUNCTION && !expr.Cast<FunctionExpression>().IsOperator()) {
		auto &name = expr.Cast<FunctionExpression>().GetQualifiedName();
		into.push_back({StringUtil::Lower(name.Schema().GetIdentifierName()),
		                StringUtil::Lower(name.Name().GetIdentifierName()), string(), true});
	}
	ParsedExpressionIterator::EnumerateChildren(expr,
	                                            [&](const ParsedExpression &child) { TableRefs(child, scope, into); });
}

static void TableRefs(const TableRef &ref, const CteScope &scope, vector<NamedRef> &into) {
	switch (ref.type) {
	case TableReferenceType::BASE_TABLE: {
		auto &name = ref.Cast<BaseTableRef>().GetQualifiedName();
		auto table = StringUtil::Lower(name.Name().GetIdentifierName());
		auto cte = scope.find(table);
		if (cte != scope.end() && name.Schema().empty()) {
			TableRefs(*cte->second, scope, into);
		} else {
			into.push_back(
			    {StringUtil::Lower(name.Schema().GetIdentifierName()), table, ref.alias.GetIdentifierName(), false});
		}
		break;
	}
	case TableReferenceType::JOIN: {
		auto &join = ref.Cast<JoinRef>();
		TableRefs(*join.left, scope, into);
		TableRefs(*join.right, scope, into);
		if (join.condition) {
			TableRefs(*join.condition, scope, into);
		}
		break;
	}
	case TableReferenceType::SUBQUERY:
		TableRefs(*ref.Cast<SubqueryRef>().subquery->node, scope, into);
		break;
	case TableReferenceType::TABLE_FUNCTION:
		TableRefs(*ref.Cast<TableFunctionRef>().function, scope, into);
		break;
	default:
		break;
	}
}

//! The lake tables a query's text names, each with the alias it gives it, in
//! the order the binder binds them: a CTE's at each place it is read; and the
//! functions it calls
static void TableRefs(const QueryNode &node, CteScope scope, vector<NamedRef> &into) {
	for (auto &cte : node.cte_map.map) {
		scope[StringUtil::Lower(cte.first.GetIdentifierName())] = cte.second->query_node.get();
	}
	switch (node.type) {
	case QueryNodeType::SELECT_NODE: {
		auto &select = node.Cast<SelectNode>();
		if (select.from_table) {
			TableRefs(*select.from_table, scope, into);
		}
		vector<const ParsedExpression *> exprs;
		exprs.push_back(select.where_clause.get());
		for (auto &group : select.groups.group_expressions) {
			exprs.push_back(group.get());
		}
		exprs.push_back(select.having.get());
		for (auto &item : select.select_list) {
			exprs.push_back(item.get());
		}
		exprs.push_back(select.qualify.get());
		for (auto expr : exprs) {
			if (expr) {
				TableRefs(*expr, scope, into);
			}
		}
		break;
	}
	case QueryNodeType::SET_OPERATION_NODE:
		for (auto &child : node.Cast<SetOperationNode>().children) {
			TableRefs(*child, scope, into);
		}
		break;
	default:
		break;
	}
}

//! The alias of each scan of a table the plan reads more than once, as the
//! view's SQL names it: the n-th scan the binder made is the n-th the text
//! names. A table the text names another number of times than the plan scans
//! it (a CTE kept once, a view read inside), or under one name each time, has
//! none
static std::map<idx_t, string> Aliases(LogicalOperator &plan, const string &view_sql) {
	std::map<idx_t, string> aliases;
	std::map<string, vector<idx_t>> scans;
	TableScans(plan, scans);
	auto options = ParserOptions::Builtin();
	options.identifier_case_mode = IdentifierCaseMode::LOWERCASE;
	options.compiled_grammar = CompiledGrammar::DefaultGrammar();
	Parser parser(options);
	parser.ParseQuery(view_sql);
	if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::CREATE_STATEMENT) {
		return aliases;
	}
	auto &info = *parser.statements[0]->Cast<CreateStatement>().info;
	if (info.type != CatalogType::VIEW_ENTRY || !info.Cast<CreateViewInfo>().query) {
		return aliases;
	}
	vector<NamedRef> refs;
	TableRefs(*info.Cast<CreateViewInfo>().query->node, CteScope(), refs);
	for (auto &table : scans) {
		std::sort(table.second.begin(), table.second.end());
		if (table.second.size() < 2) {
			continue;
		}
		vector<string> named;
		for (auto &ref : refs) {
			if (!ref.function && ref.table == table.first) {
				named.push_back(ref.alias.empty() ? ref.table : ref.alias);
			}
		}
		// one name for every scan, as each branch of a union may give, tells none apart
		if (named.size() != table.second.size() ||
		    std::all_of(named.begin(), named.end(), [&](const string &name) { return name == named[0]; })) {
			continue;
		}
		for (idx_t i = 0; i < named.size(); i++) {
			aliases[table.second[i]] = named[i];
		}
	}
	return aliases;
}

//! The mapping rows of a planned view, one walk per branch
static bool LeanLineage();
static vector<QueryLineage::Mapping> MappingOf(LogicalOperator &plan, const vector<string> &names,
                                               const string &view_sql) {
	vector<QueryLineage::Mapping> rows;
	auto aliases = Aliases(plan, view_sql);
	unordered_map<TableIndex, LogicalOperator *> ctes;
	vector<LogicalOperator *> unions;
	FirstUnions(plan, ctes, unions);
	vector<Branch> branches;
	if (unions.size() == 1) {
		Branches(*unions[0], Branch(), branches);
	} else {
		branches.emplace_back();
	}
	for (idx_t b = 0; b < branches.size(); b++) {
		Lineage lineage;
		lineage.branch = branches[b];
		lineage.aliases = aliases;
		Walk(plan, lineage);
		auto bindings = plan.GetColumnBindings();
		for (idx_t i = 0; i < names.size() && i < bindings.size(); i++) {
			auto text = lineage.text.count(bindings[i]) ? lineage.text[bindings[i]] : lineage.Unknown();
			auto &leaves = lineage.leaves[bindings[i]];
			// two scans of one table read alike in the text where no alias tells them apart
			bool told = true;
			for (auto &one : leaves) {
				for (auto &other : leaves) {
					told = told && (one.scan == other.scan || one.column != other.column ||
					                lineage.scan_tables[one.scan] != lineage.scan_tables[other.scan] ||
					                (aliases.count(one.scan) && aliases.count(other.scan)));
				}
			}
			QueryLineage::Mapping row {names[i],
			                           b + 1,
			                           text,
			                           "",
			                           "",
			                           "",
			                           "",
			                           "",
			                           lineage.literals[bindings[i]],
			                           told && !StringUtil::Contains(text, UNKNOWN)};
			if (leaves.empty()) {
				rows.push_back(row);
				continue;
			}
			for (auto &leaf : leaves) {
				auto scanned = row;
				scanned.table = lineage.scan_tables[leaf.scan];
				scanned.table_column = leaf.column;
				scanned.alias = aliases.count(leaf.scan) ? aliases[leaf.scan] : string();
				auto &conditions = lineage.conditions[leaf.scan];
				scanned.condition = conditions.size() == 1 ? conditions[0]
				                    : conditions.empty()   ? string()
				                                           : "and(" + StringUtil::Join(conditions, ", ") + ")";
				auto &lookups = lineage.lookups[leaf.scan];
				scanned.lookup = lookups.size() == 1 ? lookups[0]
				                 : lookups.empty()   ? string()
				                                     : "and(" + StringUtil::Join(lookups, ", ") + ")";
				rows.push_back(std::move(scanned));
			}
		}
	}
	return rows;
}

QueryLineage LineageOf(ClientContext &context, const string &sql, const string *view_sql) {
	QueryLineage result;
	auto statement = ParseOneSelect(sql);
	if (!statement) {
		result.complete = false;
		return result;
	}
	context.RunFunctionInTransaction([&]() {
		Planner planner(context);
		unique_ptr<SQLStatement> plain = std::move(statement);
		planner.CreatePlan(std::move(plain));
		Lineage lineage;
		Walk(*planner.plan, lineage);
		auto outputs = Outputs(*planner.plan, lineage);
		auto shapes = OutputShapes(*planner.plan, lineage);
		for (idx_t i = 0; i < planner.names.size(); i++) {
			result.columns.emplace_back(planner.names[i].GetIdentifierName(),
			                            i < outputs.size() ? outputs[i] : lineage.Unknown());
			result.shapes.push_back(i < shapes.size() ? ShapeName(shapes[i]) : "");
		}
		result.complete = lineage.complete;
		result.joins = lineage.joins;
		Scans(*planner.plan, result.scans);
		if (view_sql && !LeanLineage()) {
			vector<string> names;
			for (auto &name : planner.names) {
				names.push_back(name.GetIdentifierName());
			}
			result.mapping = MappingOf(*planner.plan, names, *view_sql);
		}
	});
	return result;
}

string LineageJson(const QueryLineage &lineage, const string &sql) {
	// a column the walk could not follow is re-derived whenever the SQL changed
	MD5Context md5;
	md5.Add(sql);
	auto unknown = "?" + md5.FinishHex();
	vector<string> members;
	for (auto &column : lineage.columns) {
		auto derivation = StringUtil::Contains(column.second, UNKNOWN) ? unknown : column.second;
		members.push_back(JsonString(column.first) + ":" + JsonString(derivation));
	}
	return "{" + StringUtil::Join(members, ",") + "}";
}

//===--------------------------------------------------------------------===//
// A view's lineage, a stored result (SPEC §6): what it scans, joins and maps
// and what computes each of its columns, planned for one view and stored as
// the result of `lineage` on it. Its inputs are the nodes the planning read:
// the view's definition, and that of every relation and macro its SQL names,
// through the views it reads and the macros it calls. The relations it reads are rows of `edges`, so
// that the lineage and the places are scans. Defining a view stores its
// lineage in the same write; `SELECT lineage() FROM <view>` computes it again
// once a node it read changed, as any result
//===--------------------------------------------------------------------===//

//! JSON text of a string, null where it is empty
static string JsonText(const string &text) {
	return text.empty() ? string("null") : JsonString(text);
}

static string JsonTexts(const vector<string> &texts) {
	vector<string> items;
	for (auto &text : texts) {
		items.push_back(JsonString(text));
	}
	return "[" + StringUtil::Join(items, ",") + "]";
}

//! The value a view's lineage stores: complete, its columns with their derivation and shape, the tables
//! it scans with the columns it reads of each, the column pairs its joins equate, what its joins and
//! filters require of each read of a table, and its mapping; each list in the order a definition read
//! serves it, so that the read takes it as stored
//! EXPERIMENT (branch core-layer): with GLOSSQL_LINEAGE_LEAN set, a column's derivation is stored as the
//! scanned columns it names, each once (schema.table.column:), and no mapping is stored
static bool LeanLineage() {
	auto lean = std::getenv("GLOSSQL_LINEAGE_LEAN");
	return lean && *lean;
}
static string LeafPaths(const string &text) {
	auto ident = [](char c) { return std::isalnum(static_cast<unsigned char>(c)) || c == '_'; };
	std::set<string> seen;
	string out;
	idx_t i = 0;
	while (i < text.size()) {
		if (!ident(text[i]) || (i > 0 && (ident(text[i - 1]) || text[i - 1] == '.'))) {
			i++;
			continue;
		}
		idx_t j = i, dots = 0;
		while (j < text.size() && (ident(text[j]) || (text[j] == '.' && j + 1 < text.size() && ident(text[j + 1])))) {
			dots += text[j] == '.';
			j++;
		}
		if (dots == 2 && j < text.size() && text[j] == ':') {
			auto path = text.substr(i, j - i);
			if (seen.insert(path).second) {
				out += (out.empty() ? "" : ", ") + path + ":";
			}
		}
		i = j > i ? j : i + 1;
	}
	return out;
}
static string LineageValue(const QueryLineage &lineage) {
	vector<string> columns;
	for (idx_t i = 0; i < lineage.columns.size(); i++) {
		auto shape = i < lineage.shapes.size() ? lineage.shapes[i] : string();
		columns.push_back("{\"name\":" + JsonString(lineage.columns[i].first) + ",\"derivation\":" +
		                  JsonString(LeanLineage() ? LeafPaths(lineage.columns[i].second) : lineage.columns[i].second) +
		                  ",\"shape\":" + JsonText(shape) + "}");
	}
	vector<string> scans;
	for (auto &scan : lineage.scans) {
		scans.push_back("{\"table_name\":" + JsonString(scan.first) + ",\"columns\":" + JsonTexts(scan.second) + "}");
	}
	// the pairs by join, the conditions by scan: in the order a definition read serves them
	auto ordered = lineage.joins;
	std::sort(ordered.begin(), ordered.end(), [](const QueryLineage::JoinPair &a, const QueryLineage::JoinPair &b) {
		return std::tie(a.join, a.left, a.right) < std::tie(b.join, b.left, b.right);
	});
	vector<string> joins;
	for (auto &join : ordered) {
		joins.push_back("{\"left_column\":" + JsonString(join.left) + ",\"right_column\":" + JsonString(join.right) +
		                ",\"left_unique\":" + (join.left_unique ? "true" : "false") + ",\"right_unique\":" +
		                (join.right_unique ? "true" : "false") + ",\"join_number\":" + to_string(join.join) + "}");
	}
	vector<string> mapping;
	for (auto &row : lineage.mapping) {
		mapping.push_back("{\"column\":" + JsonString(row.column) + ",\"branch\":" + to_string(row.branch) +
		                  ",\"derivation\":" + JsonText(row.derivation) + ",\"table_name\":" + JsonText(row.table) +
		                  ",\"table_column\":" + JsonText(row.table_column) + ",\"alias\":" + JsonText(row.alias) +
		                  ",\"condition\":" + JsonText(row.condition) + ",\"lookup\":" + JsonText(row.lookup) +
		                  ",\"literals\":" + JsonTexts(row.literals) +
		                  ",\"complete\":" + (row.complete ? "true" : "false") + "}");
	}
	// what the view's joins and filters require of each read of a table, once per table, branch and alias;
	// an empty alias or condition last
	vector<std::tuple<string, idx_t, string, string>> reads;
	for (auto &row : lineage.mapping) {
		if (!row.table.empty()) {
			reads.emplace_back(row.table, row.branch, row.alias, row.condition);
		}
	}
	auto last = [](const string &a, const string &b) {
		return a.empty() != b.empty() ? b.empty() : a < b;
	};
	std::sort(
	    reads.begin(), reads.end(),
	    [&](const std::tuple<string, idx_t, string, string> &a, const std::tuple<string, idx_t, string, string> &b) {
		    if (std::get<0>(a) != std::get<0>(b) || std::get<1>(a) != std::get<1>(b)) {
			    return std::tie(std::get<0>(a), std::get<1>(a)) < std::tie(std::get<0>(b), std::get<1>(b));
		    }
		    if (std::get<2>(a) != std::get<2>(b)) {
			    return last(std::get<2>(a), std::get<2>(b));
		    }
		    return last(std::get<3>(a), std::get<3>(b));
	    });
	reads.erase(std::unique(reads.begin(), reads.end()), reads.end());
	vector<string> conditions;
	for (auto &read : reads) {
		conditions.push_back(
		    "{\"table_name\":" + JsonString(std::get<0>(read)) + ",\"branch\":" + to_string(std::get<1>(read)) +
		    ",\"alias\":" + JsonText(std::get<2>(read)) + ",\"condition\":" + JsonText(std::get<3>(read)) + "}");
	}
	return "{\"complete\":" + string(lineage.complete ? "true" : "false") + ",\"columns\":[" +
	       StringUtil::Join(columns, ",") + "],\"scans\":[" + StringUtil::Join(scans, ",") + "],\"joins\":[" +
	       StringUtil::Join(joins, ",") + "],\"conditions\":[" + StringUtil::Join(conditions, ",") + "],\"mapping\":[" +
	       StringUtil::Join(mapping, ",") + "]}";
}

//! A relation or a macro of the lake by schema and name, as the catalog spells it, or null
static optional_ptr<CatalogEntry> LakeEntry(ClientContext &context, const string &lake, CatalogType type,
                                            const string &schema, const string &name) {
	EntryLookupInfo lookup(type, QualifiedName(Identifier(lake), Identifier(schema), Identifier(name)));
	return Catalog::GetEntry(context, lookup, OnEntryNotFound::RETURN_NULL);
}

static string PathOf(CatalogEntry &entry) {
	return entry.ParentSchema().name.GetIdentifierName() + "." + entry.name.GetIdentifierName();
}

ViewLineageResult PlanViewLineage(Inner &inner, const string &dataset, const string &view) {
	ViewLineageResult result;
	auto &context = *inner.con.context;
	auto &lake = inner.ws.lake;
	std::set<string> reads;
	std::set<pair<string, string>> nodes;
	string sql;
	context.RunFunctionInTransaction([&]() {
		auto entry = LakeEntry(context, lake, CatalogType::VIEW_ENTRY, dataset, view);
		if (!entry || entry->type != CatalogType::VIEW_ENTRY) {
			throw InvalidInputException("glossql: not_a_view: %s.%s is no view", dataset, view);
		}
		result.view = PathOf(*entry);
		nodes.insert({result.view, "definition"});
		// the relations and macros its SQL names, through the views it reads and the macros it calls, to any
		// depth, each once: a name the SQL leaves unqualified is its own schema's
		struct Step {
			string schema;
			const QueryNode *query;
			const ParsedExpression *expression;
		};
		vector<Step> walk {{dataset, entry->Cast<ViewCatalogEntry>().GetQuery().node.get(), nullptr}};
		std::set<string> seen {StringUtil::Lower(result.view)};
		for (idx_t i = 0; i < walk.size(); i++) {
			auto schema = walk[i].schema;
			vector<NamedRef> refs;
			if (walk[i].query) {
				TableRefs(*walk[i].query, CteScope(), refs);
			} else {
				TableRefs(*walk[i].expression, CteScope(), refs);
			}
			for (auto &ref : refs) {
				auto in = ref.schema.empty() ? schema : ref.schema;
				if (ref.function) {
					for (auto type : {CatalogType::TABLE_MACRO_ENTRY, CatalogType::MACRO_ENTRY}) {
						for (auto &where : {in, string("main")}) {
							auto macro = LakeEntry(context, lake, type, where, ref.table);
							if (!macro || macro->type != type) {
								continue;
							}
							auto path = PathOf(*macro);
							nodes.insert({path, "macro"});
							if (!seen.insert("macro " + StringUtil::Lower(path)).second) {
								continue;
							}
							auto home = macro->ParentSchema().name.GetIdentifierName();
							for (auto &function : macro->Cast<MacroCatalogEntry>().macros) {
								if (function->type == MacroType::TABLE_MACRO) {
									walk.push_back(
									    {home, function->Cast<TableMacroFunction>().query_node.get(), nullptr});
								} else if (function->type == MacroType::SCALAR_MACRO) {
									walk.push_back(
									    {home, nullptr, function->Cast<ScalarMacroFunction>().expression.get()});
								}
							}
						}
					}
					continue;
				}
				auto named = LakeEntry(context, lake, CatalogType::TABLE_ENTRY, in, ref.table);
				if (!named) {
					continue;
				}
				auto path = PathOf(*named);
				nodes.insert({path, "definition"});
				if (named->type != CatalogType::VIEW_ENTRY) {
					continue;
				}
				if (i == 0) {
					reads.insert(path);
				}
				if (seen.insert(StringUtil::Lower(path)).second) {
					walk.push_back({named->ParentSchema().name.GetIdentifierName(),
					                named->Cast<ViewCatalogEntry>().GetQuery().node.get(), nullptr});
				}
			}
		}
		sql = entry->ToSQL();
	});
	// planned once the walk has returned: LineageOf takes the context lock itself
	try {
		auto lineage = LineageOf(
		    context, "SELECT * FROM " + QuoteName(lake) + "." + QuoteName(dataset) + "." + QuoteName(view), &sql);
		result.value = LineageValue(lineage);
		for (auto &scan : lineage.scans) {
			nodes.insert({scan.first, "definition"});
			reads.insert(scan.first);
			for (auto &column : scan.second) {
				reads.insert(scan.first + "." + column);
			}
		}
	} catch (std::exception &ex) {
		ErrorData error(ex);
		result.error = "glossql: " + result.view + " does not plan: " + error.RawMessage();
	}
	result.reads.assign(reads.begin(), reads.end());
	result.nodes.assign(nodes.begin(), nodes.end());
	return result;
}

void StoreViewLineage(CatalogWrite &write, const string &dataset, const ViewLineageResult &lineage, int64_t read_at) {
	ResultRow row;
	row.subject = lineage.view;
	row.value = lineage.error.empty() ? Value(lineage.value) : Value();
	row.error = lineage.error.empty() ? Value() : Value(lineage.error);
	row.nodes = lineage.nodes;
	for (auto &read : lineage.reads) {
		row.edges.emplace_back(read, "reads");
	}
	StoreResults(write, dataset, "lineage", "lineage", read_at, {row});
}

bool RetireViewLineage(CatalogWrite &write, const string &dataset, const string &view) {
	auto &inner = write.inner;
	auto stored =
	    inner.Scalar("SELECT count(*) FROM " + inner.Current("results") + " WHERE dataset = " + QuoteText(dataset) +
		             " AND function = 'lineage' AND subject = " + QuoteText(view));
	if (stored.GetValue<int64_t>() == 0) {
		return false;
	}
	StoreResults(write, dataset, "lineage", "lineage", 0, {}, {view});
	return true;
}

} // namespace duckdb
