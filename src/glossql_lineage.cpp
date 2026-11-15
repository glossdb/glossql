// Lineage: what computes each column of a query, read off its bound plan
// before any optimizer touches it. Every output column resolves through
// projections, aggregates, windows, set operations and CTEs down to the base
// scans, where a leaf is the scan, the column and its type at the source. The
// text names what a column is derived from and nothing about how the plan is
// shaped, so two plans that derive a column the same way read the same and a
// source whose column arrives under another type reads different.
//
//   glossql_lineage(sql) -> (column, derivation, complete)
//
// A load records its recipe's lineage per column on its imports row; a
// gloss on a column reads stale once the derivation behind it moved, which
// is SPEC §3's re-derivation, and the row-to-row diff of the imports is the
// drift record of what the source sends.

#include "glossql_grammar.hpp"

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_search_path.hpp"
#include "duckdb/common/crypto/md5.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
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
#include "duckdb/storage/object_cache.hpp"

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
	//! scan.column, and per side whether its rows are one per value there
	std::map<std::pair<string, string>, std::pair<bool, bool>> joins;
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
	for (auto &equality : equalities) {
		bool complete = lineage.complete;
		auto a = Leaf(Text(*equality.left, lineage));
		auto b = Leaf(Text(*equality.right, lineage));
		lineage.complete = complete; // a side the walk cannot follow is no pair and no gap in the lineage
		if (a.empty() || b.empty() || a == b) {
			continue;
		}
		auto left = unique(*equality.left);
		auto right = unique(*equality.right);
		auto pair = a < b ? std::make_pair(a, b) : std::make_pair(b, a);
		auto flags = a < b ? std::make_pair(left, right) : std::make_pair(right, left);
		auto entry = lineage.joins.find(pair);
		if (entry == lineage.joins.end()) {
			lineage.joins[pair] = flags;
		} else {
			// the same pair joined twice is unique on a side where both joins are
			entry->second.first = entry->second.first && flags.first;
			entry->second.second = entry->second.second && flags.second;
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
					Equalities(condition.GetLHS(), equalities);
					Conditions(condition.GetLHS(), lineage);
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

static void TableRefs(const QueryNode &node, CteScope scope, vector<pair<string, string>> &into);

static void TableRefs(const ParsedExpression &expr, const CteScope &scope, vector<pair<string, string>> &into) {
	if (expr.GetExpressionClass() == ExpressionClass::SUBQUERY) {
		TableRefs(*expr.Cast<SubqueryExpression>().Subquery()->node, scope, into);
		return;
	}
	ParsedExpressionIterator::EnumerateChildren(expr,
	                                            [&](const ParsedExpression &child) { TableRefs(child, scope, into); });
}

static void TableRefs(const TableRef &ref, const CteScope &scope, vector<pair<string, string>> &into) {
	switch (ref.type) {
	case TableReferenceType::BASE_TABLE: {
		auto &name = ref.Cast<BaseTableRef>().GetQualifiedName();
		auto table = StringUtil::Lower(name.Name().GetIdentifierName());
		auto cte = scope.find(table);
		if (cte != scope.end() && name.Schema().empty()) {
			TableRefs(*cte->second, scope, into);
		} else {
			into.emplace_back(table, ref.alias.GetIdentifierName());
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
	default:
		break;
	}
}

//! The lake tables a query's text names, each with the alias it gives it, in
//! the order the binder binds them: a CTE's at each place it is read
static void TableRefs(const QueryNode &node, CteScope scope, vector<pair<string, string>> &into) {
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
//! it (a CTE kept once, a view read inside) has none
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
	vector<pair<string, string>> refs;
	TableRefs(*info.Cast<CreateViewInfo>().query->node, CteScope(), refs);
	for (auto &table : scans) {
		std::sort(table.second.begin(), table.second.end());
		if (table.second.size() < 2) {
			continue;
		}
		vector<string> named;
		for (auto &ref : refs) {
			if (ref.first == table.first) {
				named.push_back(ref.second.empty() ? ref.first : ref.second);
			}
		}
		if (named.size() != table.second.size()) {
			continue;
		}
		for (idx_t i = 0; i < named.size(); i++) {
			aliases[table.second[i]] = named[i];
		}
	}
	return aliases;
}

//! The mapping rows of a planned view, one walk per branch
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
		if (view_sql) {
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
// glossql_lineage(sql): the derivation of each column of a SELECT, as the
// session would plan it
//===--------------------------------------------------------------------===//

struct LineageBindData : public FunctionData {
	string sql;

	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<LineageBindData>();
		result->sql = sql;
		return result;
	}
	bool Equals(const FunctionData &other) const override {
		return sql == other.Cast<LineageBindData>().sql;
	}
};

struct LineageState : public GlobalTableFunctionState {
	bool done = false;
};

static unique_ptr<FunctionData> LineageBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result = make_uniq<LineageBindData>();
	result->sql = input.inputs[0].ToString();
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BOOLEAN, LogicalType::VARCHAR};
	names = {Identifier("column"), Identifier("derivation"), Identifier("complete"), Identifier("shape")};
	return result;
}

static unique_ptr<GlobalTableFunctionState> LineageInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<LineageState>();
}

static void LineageExec(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<LineageState>();
	if (state.done) {
		return;
	}
	state.done = true;
	auto &sql = data.bind_data->Cast<LineageBindData>().sql;
	Inner inner(context);
	auto lineage = LineageOf(*inner.con.context, sql);
	if (lineage.columns.empty()) {
		throw InvalidInputException("glossql_lineage takes one SELECT: %s", sql);
	}
	idx_t row = 0;
	for (auto &column : lineage.columns) {
		output.data[0].Append(Value(column.first));
		output.data[1].Append(Value(column.second));
		output.data[2].Append(Value::BOOLEAN(lineage.complete));
		output.data[3].Append(row < lineage.shapes.size() && !lineage.shapes[row].empty()
		                          ? Value(lineage.shapes[row])
								  : Value(LogicalType::VARCHAR));
		row++;
	}
	output.CheckCardinality(row);
}

//===--------------------------------------------------------------------===//
// glossql_views_lineage(dataset): the derivation of each column of every view
// of a dataset — a metric definition's view among them — as glossql_lineage gives one
// view's; NULL reads every dataset's views. A view that does not plan serves
// one row naming it, complete false and no column
//===--------------------------------------------------------------------===//

struct ViewsLineageBindData : public FunctionData {
	// empty when the argument is NULL: every dataset
	string dataset;
	bool every = false;
	//! glossql_views_scans: the tables each view scans instead of its columns' lineage
	bool scans = false;
	//! glossql_views_joins: the column pairs each view's joins equate
	bool joins = false;

	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<ViewsLineageBindData>();
		result->dataset = dataset;
		result->every = every;
		result->scans = scans;
		result->joins = joins;
		return result;
	}
	bool Equals(const FunctionData &other) const override {
		auto &that = other.Cast<ViewsLineageBindData>();
		return dataset == that.dataset && every == that.every && scans == that.scans && joins == that.joins;
	}
};

struct ViewsLineageRow {
	string dataset;
	string view;
	Value column;
	Value derivation;
	bool complete;
	Value shape = Value(LogicalType::VARCHAR);
};

struct ViewsLineageState : public GlobalTableFunctionState {
	bool read = false;
	vector<ViewsLineageRow> rows;
	idx_t offset = 0;
};

static unique_ptr<FunctionData> ViewsLineageBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result = make_uniq<ViewsLineageBindData>();
	result->every = input.inputs[0].IsNull();
	if (!result->every) {
		result->dataset = input.inputs[0].ToString();
	}
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                LogicalType::VARCHAR, LogicalType::BOOLEAN, LogicalType::VARCHAR};
	names = {Identifier("dataset"),    Identifier("view"),     Identifier("column"),
	         Identifier("derivation"), Identifier("complete"), Identifier("shape")};
	return result;
}

//! glossql_views_scans(dataset): every lake table each view of a dataset scans,
//! one row per view and scan with the columns the view reads of it; NULL reads
//! every dataset's views. A view that does not plan serves no row
static unique_ptr<FunctionData> ViewsScansBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result = make_uniq<ViewsLineageBindData>();
	result->every = input.inputs[0].IsNull();
	if (!result->every) {
		result->dataset = input.inputs[0].ToString();
	}
	result->scans = true;
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                LogicalType::LIST(LogicalType::VARCHAR)};
	names = {Identifier("dataset"), Identifier("view"), Identifier("table_name"), Identifier("columns")};
	return result;
}

//! glossql_views_joins(dataset): the pairs of scanned columns each view of a
//! dataset equates, in a join's condition or in its WHERE clause, one row per
//! view and pair as schema.table.column; NULL reads every dataset's views.
//! left_unique and right_unique say the view joins that side reduced to one
//! row per value: grouped by the join columns, DISTINCT on them, or filtered to
//! row_number() = 1 over them
static unique_ptr<FunctionData> ViewsJoinsBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result = make_uniq<ViewsLineageBindData>();
	result->every = input.inputs[0].IsNull();
	if (!result->every) {
		result->dataset = input.inputs[0].ToString();
	}
	result->joins = true;
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                LogicalType::VARCHAR, LogicalType::BOOLEAN, LogicalType::BOOLEAN};
	names = {Identifier("dataset"),      Identifier("view"),        Identifier("left_column"),
	         Identifier("right_column"), Identifier("left_unique"), Identifier("right_unique")};
	return result;
}

static unique_ptr<GlobalTableFunctionState> ViewsLineageInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<ViewsLineageState>();
}

//===--------------------------------------------------------------------===//
// A dataset's lineage kept: each view's lineage planned once per state of the
// catalogs a view binds against and kept in the object cache, so that every
// connection of the process reads the same, and a read that walks the lineage
// ten times plans each view once. The state is the catalog version of each
// attached database and the search path a view's names fall back to after its
// own schema. DuckLake's version is its schema version: a view defined or
// redefined, a table's columns changed, a table, a view or a macro created or
// dropped move it; a write to the store and a load of rows do not, and the
// lineage does not depend on them. A catalog that keeps no version (a remote
// one) is no part of the state
//===--------------------------------------------------------------------===//

struct ViewLineage {
	string dataset;
	string view;
	//! the view's CREATE VIEW statement, whose text names its scans' aliases
	string sql;
	//! false where the view does not plan
	bool planned = true;
	QueryLineage lineage;
};

struct KeptLineage : public ObjectCacheEntry {
	string state;
	vector<ViewLineage> views;

	static string ObjectType() {
		return "glossql_lineage";
	}
	string GetObjectType() override {
		return ObjectType();
	}
	optional_idx GetEstimatedCacheMemory() const override {
		idx_t bytes = 0;
		for (auto &view : views) {
			bytes += view.view.size() + 64;
			for (auto &column : view.lineage.columns) {
				bytes += column.first.size() + column.second.size() + 32;
			}
			for (auto &scan : view.lineage.scans) {
				bytes += scan.first.size() + 32;
				for (auto &column : scan.second) {
					bytes += column.size() + 32;
				}
			}
			bytes += view.lineage.joins.size() * 128 + view.sql.size();
			for (auto &row : view.lineage.mapping) {
				bytes += row.derivation.size() + row.condition.size() + 64 * (row.literals.size() + 3);
			}
		}
		return optional_idx(bytes);
	}
};

//! The state of the catalogs a view binds against, as the connection's transaction sees them
static string CatalogState(ClientContext &context) {
	string state;
	context.RunFunctionInTransaction([&]() {
		for (auto &db : DatabaseManager::Get(context).GetDatabases(context)) {
			if (db->IsSystem() || db->IsTemporary()) {
				continue;
			}
			auto version = db->GetCatalog().GetCatalogVersion(context);
			state += db->GetName().GetIdentifierName() + "=" +
			         (version.IsValid() ? to_string(version.GetIndex()) : string("-")) + ";";
		}
	});
	return state + "|" + CatalogSearchEntry::ListToString(ClientData::Get(context).catalog_search_path->GetSetPaths());
}

//! Every view of a dataset with its lineage: as kept while the state holds, else
//! planned on the inner connection and kept when the state held over the planning
static shared_ptr<KeptLineage> DatasetLineage(Inner &inner, const string &dataset, const string &state) {
	auto &context = *inner.con.context;
	auto &cache = ObjectCache::GetObjectCache(context);
	auto key = "glossql_lineage|" + inner.ws.lake + "|" + dataset;
	auto kept = cache.Get<KeptLineage>(key);
	if (kept && kept->state == state) {
		return kept;
	}
	auto fill = make_shared_ptr<KeptLineage>();
	auto listed =
	    inner.con.Query("SELECT view_name, sql FROM duckdb_views() WHERE database_name = " + QuoteText(inner.ws.lake) +
		                " AND schema_name = " + QuoteText(dataset) + " AND NOT internal ORDER BY view_name");
	if (listed->HasError()) {
		throw InvalidInputException("glossql_views_lineage: %s", listed->GetError());
	}
	while (auto chunk = listed->Fetch()) {
		for (idx_t i = 0; i < chunk->size(); i++) {
			ViewLineage view;
			view.dataset = dataset;
			view.view = chunk->GetValue(0, i).ToString();
			view.sql = chunk->GetValue(1, i).IsNull() ? string() : chunk->GetValue(1, i).ToString();
			fill->views.push_back(std::move(view));
		}
	}
	for (auto &view : fill->views) {
		try {
			view.lineage = LineageOf(context,
			                         "SELECT * FROM " + QuoteName(inner.ws.lake) + "." + QuoteName(view.dataset) + "." +
			                             QuoteName(view.view),
			                         &view.sql);
		} catch (std::exception &) {
			view.lineage = QueryLineage();
			view.planned = false;
		}
	}
	if (CatalogState(context) == state) {
		fill->state = state;
		cache.Put(key, fill);
	}
	return fill;
}

static void ViewsLineageExec(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<ViewsLineageState>();
	if (!state.read) {
		state.read = true;
		auto &bind = data.bind_data->Cast<ViewsLineageBindData>();
		Inner inner(context);
		auto catalogs = CatalogState(*inner.con.context);
		vector<string> datasets;
		if (bind.every) {
			auto listed = inner.con.Query("SELECT DISTINCT name FROM " + inner.Store("datasets") + " ORDER BY name");
			if (listed->HasError()) {
				throw InvalidInputException("glossql_views_lineage: %s", listed->GetError());
			}
			while (auto chunk = listed->Fetch()) {
				for (idx_t i = 0; i < chunk->size(); i++) {
					datasets.push_back(chunk->GetValue(0, i).ToString());
				}
			}
		} else {
			datasets.push_back(bind.dataset);
		}
		for (auto &dataset : datasets) {
			auto kept = DatasetLineage(inner, dataset, catalogs);
			for (auto &view : kept->views) {
				auto &lineage = view.lineage;
				if (bind.joins) {
					for (auto &join : lineage.joins) {
						state.rows.push_back({view.dataset, view.view, Value(join.first.first),
						                      Value(join.first.second), join.second.first,
						                      Value::BOOLEAN(join.second.second)});
					}
					continue;
				}
				if (bind.scans) {
					for (auto &scan : lineage.scans) {
						vector<Value> columns;
						for (auto &column : scan.second) {
							columns.emplace_back(column);
						}
						state.rows.push_back({view.dataset, view.view, Value(scan.first),
						                      Value::LIST(LogicalType::VARCHAR, std::move(columns)), true});
					}
					continue;
				}
				if (!view.planned || lineage.columns.empty()) {
					state.rows.push_back({view.dataset, view.view, Value(), Value(), false});
					continue;
				}
				for (idx_t i = 0; i < lineage.columns.size(); i++) {
					auto &column = lineage.columns[i];
					auto shape = i < lineage.shapes.size() && !lineage.shapes[i].empty() ? Value(lineage.shapes[i])
					                                                                     : Value(LogicalType::VARCHAR);
					state.rows.push_back(
					    {view.dataset, view.view, Value(column.first), Value(column.second), lineage.complete, shape});
				}
			}
		}
	}
	bool scans = data.bind_data->Cast<ViewsLineageBindData>().scans;
	idx_t count = 0;
	while (state.offset < state.rows.size() && count < STANDARD_VECTOR_SIZE) {
		auto &row = state.rows[state.offset];
		output.data[0].Append(Value(row.dataset));
		output.data[1].Append(Value(row.view));
		output.data[2].Append(row.column);
		output.data[3].Append(row.derivation);
		if (!scans) {
			output.data[4].Append(Value::BOOLEAN(row.complete));
			output.data[5].Append(row.shape);
		}
		state.offset++;
		count++;
	}
	output.CheckCardinality(count);
}

//===--------------------------------------------------------------------===//
// glossql_views_mapping(dataset): the mapping of every view of a dataset, one
// row per output column, branch of the union its rows come from (1 where it
// has none) and scanned column of a lake table: the branch's derivation, where
// a table the view reads more than once carries the alias of each scan as
// scan[alias].column; the table and its column; the alias; the conditions the
// view's joins and filters put on that scan alone; the literals the
// derivation tests or returns; and complete, false where the walk could not
// follow a step or two scans of one table read alike. A column computed from
// no lake column has one row with no table. NULL reads every dataset's views;
// a view that does not plan serves one row naming it, complete false
//===--------------------------------------------------------------------===//

struct ViewsMappingState : public GlobalTableFunctionState {
	bool read = false;
	vector<vector<Value>> rows;
	idx_t offset = 0;
};

static unique_ptr<FunctionData> ViewsMappingBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result = make_uniq<ViewsLineageBindData>();
	result->every = input.inputs[0].IsNull();
	if (!result->every) {
		result->dataset = input.inputs[0].ToString();
	}
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR,
	                LogicalType::VARCHAR, LogicalType::INTEGER,
	                LogicalType::VARCHAR, LogicalType::VARCHAR,
	                LogicalType::VARCHAR, LogicalType::VARCHAR,
	                LogicalType::VARCHAR, LogicalType::LIST(LogicalType::VARCHAR),
	                LogicalType::BOOLEAN};
	for (auto name : {"dataset", "view", "column", "branch", "derivation", "table_name", "table_column", "alias",
	                  "condition", "literals", "complete"}) {
		names.emplace_back(name);
	}
	return result;
}

static unique_ptr<GlobalTableFunctionState> ViewsMappingInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<ViewsMappingState>();
}

static void ViewsMappingExec(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<ViewsMappingState>();
	if (!state.read) {
		state.read = true;
		auto &bind = data.bind_data->Cast<ViewsLineageBindData>();
		Inner inner(context);
		auto catalogs = CatalogState(*inner.con.context);
		vector<string> datasets;
		if (bind.every) {
			auto listed = inner.con.Query("SELECT DISTINCT name FROM " + inner.Store("datasets") + " ORDER BY name");
			if (listed->HasError()) {
				throw InvalidInputException("glossql_views_mapping: %s", listed->GetError());
			}
			while (auto chunk = listed->Fetch()) {
				for (idx_t i = 0; i < chunk->size(); i++) {
					datasets.push_back(chunk->GetValue(0, i).ToString());
				}
			}
		} else {
			datasets.push_back(bind.dataset);
		}
		auto text = [](const string &value) {
			return value.empty() ? Value(LogicalType::VARCHAR) : Value(value);
		};
		for (auto &dataset : datasets) {
			auto kept = DatasetLineage(inner, dataset, catalogs);
			for (auto &view : kept->views) {
				if (!view.planned || view.lineage.mapping.empty()) {
					state.rows.push_back({Value(view.dataset), Value(view.view), Value(), Value(), Value(), Value(),
					                      Value(), Value(), Value(), Value(LogicalType::LIST(LogicalType::VARCHAR)),
					                      Value::BOOLEAN(false)});
					continue;
				}
				for (auto &row : view.lineage.mapping) {
					vector<Value> literals;
					for (auto &literal : row.literals) {
						literals.emplace_back(literal);
					}
					state.rows.push_back({Value(view.dataset), Value(view.view), Value(row.column),
					                      Value::INTEGER(NumericCast<int32_t>(row.branch)), Value(row.derivation),
					                      text(row.table), text(row.table_column), text(row.alias), text(row.condition),
					                      Value::LIST(LogicalType::VARCHAR, std::move(literals)),
					                      Value::BOOLEAN(row.complete)});
				}
			}
		}
	}
	idx_t count = 0;
	while (state.offset < state.rows.size() && count < STANDARD_VECTOR_SIZE) {
		auto &row = state.rows[state.offset];
		for (idx_t c = 0; c < row.size(); c++) {
			output.data[c].Append(row[c]);
		}
		state.offset++;
		count++;
	}
	output.CheckCardinality(count);
}

void RegisterGlossLineage(ExtensionLoader &loader) {
	loader.RegisterFunction(TableFunction("glossql_views_mapping", {LogicalType::VARCHAR}, ViewsMappingExec,
	                                      ViewsMappingBind, ViewsMappingInit));
	loader.RegisterFunction(
	    TableFunction("glossql_lineage", {LogicalType::VARCHAR}, LineageExec, LineageBind, LineageInit));
	loader.RegisterFunction(TableFunction("glossql_views_lineage", {LogicalType::VARCHAR}, ViewsLineageExec,
	                                      ViewsLineageBind, ViewsLineageInit));
	loader.RegisterFunction(TableFunction("glossql_views_scans", {LogicalType::VARCHAR}, ViewsLineageExec,
	                                      ViewsScansBind, ViewsLineageInit));
	loader.RegisterFunction(TableFunction("glossql_views_joins", {LogicalType::VARCHAR}, ViewsLineageExec,
	                                      ViewsJoinsBind, ViewsLineageInit));
}

} // namespace duckdb
