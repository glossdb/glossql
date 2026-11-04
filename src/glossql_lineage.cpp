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
// A landing records its recipe's lineage per column on its imports row; a
// gloss on a column reads stale once the derivation behind it moved, which
// is SPEC §3's re-derivation, and the row-to-row diff of the imports is the
// drift record of what the source sends.

#include "glossql_grammar.hpp"

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/crypto/md5.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/peg/compiled_grammar.hpp"
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"
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

//! What the walk could not follow: an operator or expression it does not know
static constexpr const char *UNKNOWN = "<?>";

//! The walk's state: a derivation text per binding resolved so far, the CTEs'
//! outputs by their index, and whether every step was followed
struct Lineage {
	column_binding_map_t<string> text;
	unordered_map<TableIndex, vector<string>> ctes;
	bool complete = true;

	string Unknown() {
		complete = false;
		return UNKNOWN;
	}
};

static string Text(const Expression &expr, Lineage &lineage);

static string Children(const Expression &expr, Lineage &lineage) {
	vector<string> parts;
	ExpressionIterator::EnumerateChildren(expr,
	                                      [&](const Expression &child) { parts.push_back(Text(child, lineage)); });
	return StringUtil::Join(parts, ", ");
}

//! The derivation text of a bound expression: a column reference is what it
//! resolves to, a constant its literal, a function its name over its
//! children, anything else its expression type over its children
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
		auto type = expr.GetExpressionType();
		if (type == ExpressionType::OPERATOR_CAST) {
			return "cast(" + Children(expr, lineage) + " as " + expr.GetReturnType().ToString() + ")";
		}
		return function.Function().GetName().GetIdentifierName() + "(" + Children(expr, lineage) + ")";
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
		auto name = window.AggregateFunction() ? window.AggregateFunction()->GetName().GetIdentifierName()
		                                       : StringUtil::Lower(ExpressionTypeToString(expr.GetExpressionType()));
		return name + "(" + Children(expr, lineage) + ") over";
	}
	case ExpressionClass::BOUND_PARAMETER:
		return "$" + expr.Cast<BoundParameterExpression>().Identifier().GetIdentifierName();
	case ExpressionClass::LEGACY_BOUND_COMPARISON:
	case ExpressionClass::LEGACY_BOUND_BETWEEN:
	case ExpressionClass::BOUND_CONJUNCTION:
	case ExpressionClass::BOUND_OPERATOR:
	case ExpressionClass::BOUND_CASE:
	case ExpressionClass::BOUND_UNNEST:
		return "unnest(" + Children(expr, lineage) + ")";
	case ExpressionClass::BOUND_LAMBDA:
	case ExpressionClass::BOUND_LAMBDA_REF:
	case ExpressionClass::BOUND_DEFAULT:
		return StringUtil::Lower(ExpressionTypeToString(expr.GetExpressionType())) + "(" + Children(expr, lineage) +
		       ")";
	default:
		return lineage.Unknown();
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

static void Walk(LogicalOperator &op, Lineage &lineage) {
	if (op.type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE) {
		// the definition first, so the references to it resolve
		auto &cte = op.Cast<LogicalMaterializedCTE>();
		Walk(*op.children[0], lineage);
		lineage.ctes[cte.table_index] = Outputs(*op.children[0], lineage);
		Walk(*op.children[1], lineage);
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
		for (idx_t i = 0; i < column_ids.size() && i < bindings.size(); i++) {
			auto &column = column_ids[i];
			auto leaf = scan + "." + get.GetColumnName(column).GetIdentifierName();
			if (!column.IsVirtualColumn()) {
				leaf += ":" + get.returned_types[column.GetPrimaryIndex()].ToString();
			}
			lineage.text[bindings[i]] = leaf;
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		auto &projection = op.Cast<LogicalProjection>();
		for (idx_t i = 0; i < projection.expressions.size(); i++) {
			lineage.text[ColumnBinding(projection.table_index, ProjectionIndex(i))] =
			    Text(*projection.expressions[i], lineage);
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		auto &aggregate = op.Cast<LogicalAggregate>();
		for (idx_t i = 0; i < aggregate.groups.size(); i++) {
			lineage.text[ColumnBinding(aggregate.group_index, ProjectionIndex(i))] =
			    Text(*aggregate.groups[i], lineage);
		}
		for (idx_t i = 0; i < aggregate.expressions.size(); i++) {
			lineage.text[ColumnBinding(aggregate.aggregate_index, ProjectionIndex(i))] =
			    Text(*aggregate.expressions[i], lineage);
		}
		for (idx_t i = 0; i < aggregate.grouping_functions.size(); i++) {
			lineage.text[ColumnBinding(aggregate.groupings_index, ProjectionIndex(i))] = "grouping";
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_WINDOW: {
		auto &window = op.Cast<LogicalWindow>();
		for (idx_t i = 0; i < window.expressions.size(); i++) {
			lineage.text[ColumnBinding(window.window_index, ProjectionIndex(i))] =
			    Text(*window.expressions[i], lineage);
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_UNNEST: {
		auto &unnest = op.Cast<LogicalUnnest>();
		for (idx_t i = 0; i < unnest.expressions.size(); i++) {
			lineage.text[ColumnBinding(unnest.unnest_index, ProjectionIndex(i))] =
			    Text(*unnest.expressions[i], lineage);
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
		for (auto &child : op.children) {
			sides.push_back(Outputs(*child, lineage));
		}
		for (idx_t i = 0; i < setop.column_count; i++) {
			vector<string> parts;
			for (auto &side : sides) {
				parts.push_back(i < side.size() ? side[i] : lineage.Unknown());
			}
			lineage.text[ColumnBinding(setop.table_index, ProjectionIndex(i))] =
			    string(word) + "(" + StringUtil::Join(parts, ", ") + ")";
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_CTE_REF: {
		auto &ref = op.Cast<LogicalCTERef>();
		auto entry = lineage.ctes.find(ref.cte_index);
		for (idx_t i = 0; i < ref.chunk_types.size(); i++) {
			lineage.text[ColumnBinding(ref.table_index, ProjectionIndex(i))] =
			    entry != lineage.ctes.end() && i < entry->second.size() ? entry->second[i] : lineage.Unknown();
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
		break;
	}
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
	case LogicalOperatorType::LOGICAL_POSITIONAL_JOIN:
	case LogicalOperatorType::LOGICAL_FILTER:
	case LogicalOperatorType::LOGICAL_LIMIT:
	case LogicalOperatorType::LOGICAL_ORDER_BY:
	case LogicalOperatorType::LOGICAL_TOP_N:
	case LogicalOperatorType::LOGICAL_DISTINCT:
	case LogicalOperatorType::LOGICAL_SAMPLE:
	case LogicalOperatorType::LOGICAL_DUMMY_SCAN:
	case LogicalOperatorType::LOGICAL_EMPTY_RESULT:
		// the children's bindings are the outputs: rows change, columns do not
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

QueryLineage LineageOf(ClientContext &context, const string &sql) {
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
		for (idx_t i = 0; i < planner.names.size(); i++) {
			result.columns.emplace_back(planner.names[i].GetIdentifierName(),
			                            i < outputs.size() ? outputs[i] : lineage.Unknown());
		}
		result.complete = lineage.complete;
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
		return std::move(result);
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
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BOOLEAN};
	names = {Identifier("column"), Identifier("derivation"), Identifier("complete")};
	return std::move(result);
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
		row++;
	}
	output.CheckCardinality(row);
}

//===--------------------------------------------------------------------===//
// glossql_views_lineage(dataset): the derivation of each column of every view
// of a dataset — a grounding's view among them — as glossql_lineage gives one
// view's; a view that does not plan serves one row naming it, complete
// false and no column
//===--------------------------------------------------------------------===//

struct ViewsLineageBindData : public FunctionData {
	string dataset;

	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<ViewsLineageBindData>();
		result->dataset = dataset;
		return std::move(result);
	}
	bool Equals(const FunctionData &other) const override {
		return dataset == other.Cast<ViewsLineageBindData>().dataset;
	}
};

static unique_ptr<FunctionData> ViewsLineageBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result = make_uniq<ViewsLineageBindData>();
	result->dataset = input.inputs[0].ToString();
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BOOLEAN};
	names = {Identifier("view"), Identifier("column"), Identifier("derivation"), Identifier("complete")};
	return std::move(result);
}

static void ViewsLineageExec(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<LineageState>();
	if (state.done) {
		return;
	}
	state.done = true;
	auto &dataset = data.bind_data->Cast<ViewsLineageBindData>().dataset;
	Inner inner(context);
	vector<string> views;
	auto listed = inner.con.Query(
	    "SELECT table_name FROM information_schema.tables WHERE table_catalog = " + QuoteText(inner.ws.lake) +
	    " AND table_schema = " + QuoteText(dataset) + " AND table_type = 'VIEW' ORDER BY table_name");
	if (listed->HasError()) {
		throw InvalidInputException("glossql_views_lineage: %s", listed->GetError());
	}
	while (auto chunk = listed->Fetch()) {
		for (idx_t i = 0; i < chunk->size(); i++) {
			views.push_back(chunk->GetValue(0, i).ToString());
		}
	}
	idx_t row = 0;
	for (auto &view : views) {
		QueryLineage lineage;
		bool planned = true;
		try {
			lineage = LineageOf(*inner.con.context, "SELECT * FROM " + QuoteName(inner.ws.lake) + "." +
			                                            QuoteName(dataset) + "." + QuoteName(view));
		} catch (std::exception &) {
			planned = false;
		}
		if (!planned || lineage.columns.empty()) {
			output.data[0].Append(Value(view));
			output.data[1].Append(Value());
			output.data[2].Append(Value());
			output.data[3].Append(Value::BOOLEAN(false));
			row++;
			continue;
		}
		for (auto &column : lineage.columns) {
			output.data[0].Append(Value(view));
			output.data[1].Append(Value(column.first));
			output.data[2].Append(Value(column.second));
			output.data[3].Append(Value::BOOLEAN(lineage.complete));
			row++;
		}
	}
	output.CheckCardinality(row);
}

void RegisterGlossLineage(ExtensionLoader &loader) {
	loader.RegisterFunction(
	    TableFunction("glossql_lineage", {LogicalType::VARCHAR}, LineageExec, LineageBind, LineageInit));
	loader.RegisterFunction(TableFunction("glossql_views_lineage", {LogicalType::VARCHAR}, ViewsLineageExec,
	                                      ViewsLineageBind, LineageInit));
}

} // namespace duckdb
