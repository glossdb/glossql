// Statements that run SQL: DECLARE DATASET, DECLARE RECIPE, PROBE, IMPORT.
//
// They execute on a second connection to the same database — DuckDB's own
// pattern for work done while the calling connection's context lock is held
// (src/main/external_resources_manager.cpp) — so the recipe SQL is planned
// and run by DuckDB itself, at the source:
//   * a file source (parquet | csv | json) is a directory or URL prefix; the
//     recipe's read_parquet / read_csv / read_json paths resolve under it
//     through DuckDB's file_search_path;
//   * a relational source is a database DuckDB has ATTACHed (postgres, mysql,
//     sqlite, duckdb, ...); the recipe runs with that database as the default
//     catalog, DuckDB pushing down what it can. The plugin manages no
//     connections of its own.
// What one inner statement does is atomic; the glossql statement itself is
// not atomic with a transaction the caller may have open.

#include "glossql_grammar.hpp"

#include <algorithm>
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/file_open_flags.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/prepared_statement.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_search_path.hpp"
#include "duckdb/main/client_data.hpp"
#include <functional>
#include <set>
#include <map>

namespace duckdb {

//===--------------------------------------------------------------------===//
// The inner connection
//===--------------------------------------------------------------------===//

string LakeName(ClientContext &context) {
	auto &variables = ClientConfig::GetConfig(context).user_variables;
	auto named = variables.find(Identifier("glossql_workspace"));
	if (named != variables.end() && !named->second.IsNull()) {
		return named->second.ToString();
	}
	vector<string> lakes;
	for (auto &database : DatabaseManager::Get(context).GetDatabases(context)) {
		if (database->GetCatalog().GetCatalogType() == "ducklake") {
			lakes.push_back(database->GetName().GetIdentifierName());
		}
	}
	if (lakes.size() == 1) {
		return lakes[0];
	}
	std::sort(lakes.begin(), lakes.end());
	if (lakes.empty()) {
		throw InvalidInputException(
		    "glossql: no DuckLake is attached: the workspace is a lake, ATTACH 'ducklake:...' first");
	}
	throw InvalidInputException(
	    "glossql: %d DuckLakes are attached (%s): SET VARIABLE glossql_workspace = 'name' picks one", lakes.size(),
	    StringUtil::Join(lakes, ", "));
}

//! The lake, and the metadata catalog behind it: one the user named is visible
//! to information_schema; the hidden default is reached by its name
static Workspace FindWorkspace(ClientContext &context, Connection &con) {
	Workspace ws;
	ws.lake = LakeName(context);
	auto visible = con.Query("SELECT table_catalog, table_schema FROM information_schema.tables"
	                         " WHERE table_name = 'ducklake_snapshot' LIMIT 1");
	if (!visible->HasError()) {
		auto chunk = visible->Fetch();
		if (chunk && chunk->size() == 1) {
			ws.meta = chunk->GetValue(0, 0).ToString();
			ws.meta_schema = chunk->GetValue(1, 0).ToString();
			return ws;
		}
	}
	ws.meta = "__ducklake_metadata_" + ws.lake;
	for (auto schema : {"main", "public"}) {
		auto probe =
		    con.Query("SELECT 1 FROM " + QuoteName(ws.meta) + "." + QuoteName(schema) + ".ducklake_snapshot LIMIT 0");
		if (!probe->HasError()) {
			ws.meta_schema = schema;
			return ws;
		}
	}
	throw InvalidInputException("glossql: the metadata catalog of DuckLake '%s' was not found", ws.lake);
}

string Workspace::Store(const string &relation) const {
	return QuoteName(meta) + "." + QuoteName(meta_schema) + "." + QuoteName("glossql_" + relation);
}

string Workspace::Meta(const string &table) const {
	return QuoteName(meta) + "." + QuoteName(meta_schema) + "." + QuoteName("ducklake_" + table);
}

string Workspace::Table(const string &dataset, const string &name) const {
	return QuoteName(lake) + "." + QuoteName(dataset) + "." + QuoteName(name);
}

string JsonString(const string &text) {
	string result = "\"";
	for (auto c : text) {
		switch (c) {
		case '"':
			result += "\\\"";
			break;
		case '\\':
			result += "\\\\";
			break;
		case '\n':
			result += "\\n";
			break;
		case '\r':
			result += "\\r";
			break;
		case '\t':
			result += "\\t";
			break;
		default:
			if (static_cast<unsigned char>(c) < 0x20) {
				result += StringUtil::Format("\\u%04x", static_cast<int>(static_cast<unsigned char>(c)));
			} else {
				result += c;
			}
		}
	}
	return result + "\"";
}

string JsonList(const vector<string> &items) {
	vector<string> quoted;
	for (auto &item : items) {
		quoted.push_back(JsonString(item));
	}
	return "[" + StringUtil::Join(quoted, ",") + "]";
}

string DatasetOf(const string &subject) {
	return subject.substr(0, subject.find('.'));
}

string JsonList(const Value &list) {
	vector<string> items;
	for (auto &item : ListValue::GetChildren(list)) {
		items.push_back(item.ToString());
	}
	return JsonList(items);
}

Inner::Inner(ClientContext &context) : con(*context.db), ws(FindWorkspace(context, con)) {
	// a body resolves names and files as the caller's session does: the same search paths
	auto &paths = ClientData::Get(context).catalog_search_path->GetSetPaths();
	if (!paths.empty()) {
		Run("SET search_path = " + QuoteText(CatalogSearchEntry::ListToString(paths)));
	}
	Value files;
	if (context.TryGetCurrentSetting(Identifier("file_search_path"), files) && !files.IsNull() &&
	    !files.ToString().empty()) {
		Run("SET file_search_path = " + QuoteText(files.ToString()));
	}
}

string Inner::Seq(const string &relation) const {
	return "(SELECT coalesce(max(seq), 0) + 1 FROM " + Store(relation) + ")";
}

Value Inner::Snapshot() {
	return Scalar("SELECT id::BIGINT FROM ducklake_current_snapshot(" + QuoteText(ws.lake) + ")");
}

unique_ptr<QueryResult> Inner::Run(const string &sql) {
	auto result = con.Query(sql);
	if (result->HasError()) {
		throw InvalidInputException("glossql: %s", result->GetError());
	}
	return result;
}

Value Inner::Scalar(const string &sql) {
	auto result = Run(sql);
	auto chunk = result->Fetch();
	if (!chunk || chunk->size() == 0) {
		return Value();
	}
	return chunk->GetValue(0, 0);
}

vector<Value> Inner::Row(const string &sql) {
	auto result = Run(sql);
	auto chunk = result->Fetch();
	vector<Value> row;
	if (!chunk || chunk->size() == 0) {
		return row;
	}
	for (idx_t c = 0; c < chunk->ColumnCount(); c++) {
		row.push_back(chunk->GetValue(c, 0));
	}
	return row;
}

string Inner::Store(const string &relation) const {
	return ws.Store(relation);
}

string Inner::Table(const string &schema, const string &name) const {
	return ws.Table(schema, name);
}

string Inner::FunctionSchema(const string &dataset, const string &name) {
	for (auto &scope : {dataset, string("GLOBAL")}) {
		auto found = Scalar("SELECT count(*) FROM " + Store("functions") + " WHERE name = " + QuoteText(name) +
		                    " AND scope = " + QuoteText(scope));
		if (found.GetValue<int64_t>() > 0) {
			return scope == "GLOBAL" ? "glossql" : scope;
		}
	}
	return "";
}

//! A declared source, as its newest row stands
struct SourceInfo {
	string name;
	string type;
	string location;
};

static SourceInfo LookupSource(Inner &inner, const string &name) {
	auto row = inner.Row("SELECT settings::JSON->>'type', settings::JSON->>'location' FROM " + inner.Store("sources") +
	                     " WHERE name = " + QuoteText(name) + " ORDER BY seq DESC LIMIT 1");
	if (row.empty()) {
		throw InvalidInputException("glossql: source '%s' is not declared", name);
	}
	return SourceInfo {name, row[0].ToString(), row[1].ToString()};
}

//! Points the inner connection at the source: relative file paths resolve under
//! a file source's location; a relational source's database becomes the default
static void AtSource(Inner &inner, const SourceInfo &source) {
	if (source.type == "relational_db") {
		auto attached =
		    inner.Scalar("SELECT count(*) FROM duckdb_databases() WHERE database_name = " + QuoteText(source.location));
		if (attached.GetValue<int64_t>() == 0) {
			throw InvalidInputException("glossql: source '%s' names database '%s', which is not attached — "
			                            "ATTACH it first (postgres, mysql, sqlite, duckdb, ...)",
			                            source.name, source.location);
		}
		inner.Run("USE " + QuoteName(source.location));
		return;
	}
	inner.Run("SET file_search_path = " + QuoteText(source.location));
}

//===--------------------------------------------------------------------===//
// Shared shapes: one outcome row
//===--------------------------------------------------------------------===//

struct RunnerBindData : public FunctionData {
	vector<Value> inputs;

	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<RunnerBindData>();
		result->inputs = inputs;
		return std::move(result);
	}
	bool Equals(const FunctionData &other_p) const override {
		return inputs == other_p.Cast<RunnerBindData>().inputs;
	}
};

struct RunnerState : public GlobalTableFunctionState {
	bool done = false;
};

static unique_ptr<FunctionData> OutcomeBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result = make_uniq<RunnerBindData>();
	result->inputs = input.inputs;
	return_types.push_back(LogicalType::VARCHAR);
	names.emplace_back("outcome");
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> RunnerInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<RunnerState>();
}

static void Outcome(DataChunk &output, const string &text) {
	output.data[0].Append(Value(text));
}

//===--------------------------------------------------------------------===//
// DECLARE DATASET name SET (purpose: ...): a schema plus a row, the settings as JSON
//===--------------------------------------------------------------------===//

static void DatasetExec(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<RunnerState>();
	if (state.done) {
		return;
	}
	state.done = true;
	auto &inputs = data.bind_data->Cast<RunnerBindData>().inputs;
	auto name = inputs[0].ToString();

	// the schema is the lake's and the row the store's: two catalogs, two commits;
	// an unchanged re-declaration is a no-op (SPEC §3)
	Inner inner(context);
	auto standing = inner.Row("SELECT settings FROM " + inner.Store("datasets") + " WHERE name = " + QuoteText(name) +
	                          " ORDER BY seq DESC LIMIT 1");
	if (!standing.empty() && standing[0].ToString() == inputs[1].ToString()) {
		Outcome(output, "dataset " + name + " unchanged: no-op");
		return;
	}
	inner.Run("CREATE SCHEMA IF NOT EXISTS " + QuoteName(inner.ws.lake) + "." + QuoteName(name));
	inner.Run("INSERT INTO " + inner.Store("datasets") + " VALUES (" + inner.Seq("datasets") + ", " + QuoteText(name) +
	          ", " + inputs[1].ToSQLString() + ", now())");
	Outcome(output, "declared dataset " + name + " (schema " + inner.ws.lake + "." + name + ")");
}

TableFunction GlossDatasetFunction() {
	return TableFunction("glossql_declare_dataset", {LogicalType::VARCHAR, LogicalType::VARCHAR}, DatasetExec,
	                     OutcomeBind, RunnerInit);
}

//===--------------------------------------------------------------------===//
// Landing: the recipe SQL becomes the table, whole or merged by key
//===--------------------------------------------------------------------===//

struct RecipeRow {
	string source;
	//! the SET pairs as JSON; `key` names the table's key, one column or 'a, b'
	string settings;
	string sql;

	vector<string> Key() const {
		vector<string> result;
		for (auto &column : StringUtil::Split(JsonStringField(settings, "key"), ',')) {
			auto name = column;
			StringUtil::Trim(name);
			if (!name.empty()) {
				result.push_back(name);
			}
		}
		return result;
	}
};

static RecipeRow NewestRecipe(Inner &inner, const string &dataset, const string &table) {
	auto row = inner.Row("SELECT source, settings, sql FROM " + inner.Store("recipes") + " WHERE dataset = " +
	                     QuoteText(dataset) + " AND table_name = " + QuoteText(table) + " ORDER BY seq DESC LIMIT 1");
	if (row.empty()) {
		throw InvalidInputException("glossql: %s.%s has no recipe — tables come from recipes", dataset, table);
	}
	return RecipeRow {row[0].ToString(), row[1].ToString(), row[2].ToString()};
}

//! What a landing wrote: the table's rows, how they joined it (`mode`: replace
//! or merge, the key being the recipe's), and the rows dropped on the way
//! (SPEC §3), NULL where the host cannot count them
struct Landed {
	int64_t rows;
	string mode;
	//! the outcome's wording of the mode
	string how;
	Value dropped;
	//! the lake's snapshot the landing made
	Value snapshot;

	//! The outcome's row accounting, sized to what the counts can say
	string Summary() const {
		auto summary = to_string(rows) + " rows";
		if (!dropped.IsNull()) {
			summary += ", " + dropped.ToString() + " dropped";
		}
		return summary + " (" + how + ")";
	}
};

//! The recipe is the landing: what its SQL yields is what the table gets, so
//! nothing is lost between source and table. The one reader that drops rows
//! is one told to ignore errors; when it stores its rejects, the landing's
//! connection holds them and the count is theirs, one per line; when it does
//! not, the host cannot count what it skipped
static Value DroppedRows(Inner &inner, const string &sql) {
	// looked up, never probed: a failing statement would abort the landing's transaction
	auto stored = inner.Scalar("SELECT count(*) FROM information_schema.tables WHERE table_catalog = 'temp'"
	                           " AND table_schema = 'main' AND table_name = 'reject_errors'");
	if (stored.GetValue<int64_t>() > 0) {
		return inner.Scalar("SELECT count(DISTINCT (scan_id, file_id, line)) FROM temp.main.reject_errors");
	}
	auto lower = StringUtil::Lower(sql);
	if (StringUtil::Contains(lower, "ignore_errors") || StringUtil::Contains(lower, "store_rejects")) {
		return Value();
	}
	return Value::BIGINT(0);
}

//! Runs the recipe at its source and lands the result
static Landed Land(Inner &inner, const string &dataset, const string &table, const RecipeRow &recipe, bool replace) {
	auto target = inner.Table(dataset, table);
	AtSource(inner, LookupSource(inner, recipe.source));
	Landed landed;
	auto key = recipe.Key();
	if (replace || key.empty()) {
		inner.Run("CREATE OR REPLACE TABLE " + target + " AS " + recipe.sql);
		landed.mode = "replace";
		landed.how = "replaced";
	} else {
		vector<string> on;
		for (auto &column : key) {
			on.push_back("t." + QuoteName(column) + " = s." + QuoteName(column));
		}
		inner.Run("MERGE INTO " + target + " AS t USING (" + recipe.sql + ") AS s ON " + StringUtil::Join(on, " AND ") +
		          " WHEN MATCHED THEN UPDATE WHEN NOT MATCHED THEN INSERT");
		landed.mode = "merge";
		landed.how = "merged by " + StringUtil::Join(key, ", ");
	}
	landed.rows = inner.Scalar("SELECT count(*) FROM " + target).GetValue<int64_t>();
	landed.dropped = DroppedRows(inner, recipe.sql);
	landed.snapshot = inner.Snapshot();
	// what computed each column, read off the recipe's plan at the source: the
	// re-derivation rule compares landings by it (SPEC §3), and the landings' diff
	// is the drift record of what the source sends
	string lineage = "{}";
	try {
		lineage = LineageJson(LineageOf(*inner.con.context, recipe.sql), recipe.sql);
	} catch (std::exception &) {
		// a recipe the planner will not walk again lands without its lineage
	}
	// the landing is the lake's commit, one snapshot; its row in the store follows it
	inner.Run("INSERT INTO " + inner.Store("imports") + " VALUES (" + inner.Seq("imports") + ", " + QuoteText(dataset) +
	          ", " + QuoteText(table) + ", " + QuoteText(landed.mode) + ", " + to_string(landed.rows) + ", " +
	          landed.dropped.ToSQLString() + ", " + QuoteText(lineage) + ", " + landed.snapshot.ToSQLString() +
	          ", now())");
	return landed;
}

//===--------------------------------------------------------------------===//
// DECLARE RECIPE name ON dataset FROM source [SET (key: col)] AS $$sql$$: the
// settings as JSON, the key one column or 'a, b' (SPEC §3)
//===--------------------------------------------------------------------===//

static void RecipeExec(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<RunnerState>();
	if (state.done) {
		return;
	}
	state.done = true;
	auto &inputs = data.bind_data->Cast<RunnerBindData>().inputs;
	auto name = inputs[0].ToString();
	auto dataset = inputs[1].ToString();
	RecipeRow recipe {inputs[2].ToString(), inputs[3].ToString(), inputs[4].ToString()};

	Inner inner(context);
	// identity is content (SPEC §3): an unchanged re-declaration is a no-op
	auto standing = inner.Row("SELECT source, settings, sql FROM " + inner.Store("recipes") +
	                          " WHERE dataset = " + QuoteText(dataset) + " AND table_name = " + QuoteText(name) +
	                          " ORDER BY seq DESC LIMIT 1");
	if (!standing.empty() && standing[0].ToString() == recipe.source && standing[1].ToString() == recipe.settings &&
	    standing[2].ToString() == recipe.sql) {
		Outcome(output, "recipe " + name + " unchanged: no-op");
		return;
	}
	auto dataset_exists =
	    inner.Scalar("SELECT count(*) FROM " + inner.Store("datasets") + " WHERE name = " + QuoteText(dataset));
	if (dataset_exists.GetValue<int64_t>() == 0) {
		throw InvalidInputException("glossql: dataset '%s' is not declared", dataset);
	}

	auto landed = Land(inner, dataset, name, recipe, true);
	inner.Run("INSERT INTO " + inner.Store("recipes") + " VALUES (" + inner.Seq("recipes") + ", " + QuoteText(dataset) +
	          ", " + QuoteText(name) + ", " + QuoteText(recipe.source) + ", " + QuoteText(recipe.settings) + ", " +
	          QuoteText(recipe.sql) + ", now())");
	landed.how = standing.empty() ? "new" : "superseded";
	Outcome(output, "landed " + dataset + "." + name + ": " + landed.Summary());
}

TableFunction GlossRecipeFunction() {
	return TableFunction(
	    "glossql_declare_recipe",
	    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	    RecipeExec, OutcomeBind, RunnerInit);
}

//===--------------------------------------------------------------------===//
// IMPORT [dataset.]table: the recipe runs again over its source
//===--------------------------------------------------------------------===//

static void ImportExec(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<RunnerState>();
	if (state.done) {
		return;
	}
	state.done = true;
	auto &inputs = data.bind_data->Cast<RunnerBindData>().inputs;
	auto dataset = inputs[0].IsNull() ? CurrentDataset(context) : inputs[0].ToString();
	auto table = inputs[1].ToString();

	Inner inner(context);
	auto recipe = NewestRecipe(inner, dataset, table);
	auto landed = Land(inner, dataset, table, recipe, false);
	Outcome(output, "imported " + dataset + "." + table + ": " + landed.Summary());
}

TableFunction GlossImportFunction() {
	return TableFunction("glossql_import", {LogicalType::VARCHAR, LogicalType::VARCHAR}, ImportExec, OutcomeBind,
	                     RunnerInit);
}

//===--------------------------------------------------------------------===//
// PROBE source AS $$sql$$: the recipe rehearsal, landing nothing
//===--------------------------------------------------------------------===//

struct ProbeState : public GlobalTableFunctionState {
	unique_ptr<Inner> inner;
	unique_ptr<QueryResult> result;
	unique_ptr<DataChunk> chunk;
};

static unique_ptr<FunctionData> ProbeBind(ClientContext &context, TableFunctionBindInput &input,
                                          vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result = make_uniq<RunnerBindData>();
	result->inputs = input.inputs;
	auto source = input.inputs[0].ToString();
	auto sql = input.inputs[1].ToString();

	// the result always carries its schema: prepared at the source, run nowhere yet
	Inner inner(context);
	AtSource(inner, LookupSource(inner, source));
	auto prepared = inner.con.Prepare(sql);
	if (prepared->HasError()) {
		throw InvalidInputException("glossql: probe of %s: %s", source, prepared->GetError());
	}
	return_types = prepared->GetTypes();
	names = prepared->GetNames();
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> ProbeInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<ProbeState>();
}

static void ProbeExec(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<ProbeState>();
	auto &inputs = data.bind_data->Cast<RunnerBindData>().inputs;
	if (!state.inner) {
		state.inner = make_uniq<Inner>(context);
		AtSource(*state.inner, LookupSource(*state.inner, inputs[0].ToString()));
		state.result = state.inner->Run(inputs[1].ToString());
	}
	state.chunk = state.result->Fetch();
	if (!state.chunk || state.chunk->size() == 0) {
		return;
	}
	output.Reference(*state.chunk);
}

TableFunction GlossProbeFunction() {
	return TableFunction("glossql_probe", {LogicalType::VARCHAR, LogicalType::VARCHAR}, ProbeExec, ProbeBind,
	                     ProbeInit);
}

//===--------------------------------------------------------------------===//
// DECLARE FUNCTION name FOR (dataset | GLOBAL) AS $$sql$$ [RETURNS aspect]
//
// A function is a DuckDB table macro — a UDF callable from any SQL — in the
// dataset's schema, or in glossql for GLOBAL. Role by shape (SPEC §6): with
// RETURNS it is a measurement and takes `subject`; without, a detector that
// takes `threshold`. The body's $subject / $threshold become the parameter.
//===--------------------------------------------------------------------===//

string ReplacePlaceholder(const string &sql, const string &name, const string &replacement) {
	string result;
	auto needle = "$" + name;
	idx_t pos = 0;
	while (pos < sql.size()) {
		auto hit = sql.find(needle, pos);
		if (hit == string::npos) {
			result += sql.substr(pos);
			break;
		}
		result += sql.substr(pos, hit - pos);
		auto end = hit + needle.size();
		bool whole = end >= sql.size() || !(StringUtil::CharacterIsAlphaNumeric(sql[end]) || sql[end] == '_');
		result += whole ? replacement : needle;
		pos = end;
	}
	return result;
}

//! Two store values as the same declaration: both NULL, or the same text
static bool SameValue(const Value &before, const Value &after) {
	if (before.IsNull() || after.IsNull()) {
		return before.IsNull() == after.IsNull();
	}
	return before.ToString() == after.ToString();
}

static void FunctionExec(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<RunnerState>();
	if (state.done) {
		return;
	}
	state.done = true;
	auto &inputs = data.bind_data->Cast<RunnerBindData>().inputs;
	auto name = inputs[0].ToString();
	auto scope = inputs[1].ToString();
	auto script = inputs[2].ToString();
	auto &returns = inputs[3];

	Inner inner(context);
	// identity is content (SPEC §3): an unchanged re-declaration is a no-op, so a kit replays silently
	auto standing = inner.Row("SELECT scope, script, returns FROM " + inner.Store("functions") +
	                          " WHERE name = " + QuoteText(name) + " ORDER BY seq DESC LIMIT 1");
	if (!standing.empty() && standing[0].ToString() == scope && standing[1].ToString() == script &&
	    SameValue(standing[2], returns)) {
		Outcome(output, "function " + name + " unchanged: no-op");
		return;
	}
	auto schema = scope == "GLOBAL" ? string("glossql") : scope;
	if (scope != "GLOBAL") {
		auto declared =
		    inner.Scalar("SELECT count(*) FROM " + inner.Store("datasets") + " WHERE name = " + QuoteText(scope));
		if (declared.GetValue<int64_t>() == 0) {
			throw InvalidInputException("glossql: dataset '%s' is not declared", scope);
		}
	}
	if (!returns.IsNull()) {
		// a measurement is a table macro, a UDF for any SQL: its $subject is the parameter, named so
		// that no column of the body (a glossary's subject, say) can collide with it
		inner.Run("CREATE OR REPLACE MACRO " + inner.Table(schema, name) + "(\"$subject\") AS TABLE " +
		          ReplacePlaceholder(script, "subject", "\"$subject\""));
	}
	inner.Run("INSERT INTO " + inner.Store("functions") + " VALUES (" + inner.Seq("functions") + ", " +
	          QuoteText(name) + ", " + QuoteText(scope) + ", " + QuoteText(script) + ", " + returns.ToSQLString() +
	          ", now())");
	// a detector's body is run by ATTEST() over its witness's slots, $threshold bound there
	Outcome(output, returns.IsNull() ? "declared detector " + name + " for " + scope
	                                 : "declared measurement " + schema + "." + name + "($subject)");
}

//===--------------------------------------------------------------------===//
// DECLARE WITNESS name ON aspect [BY (AGENT, HUMAN)] [DETECTOR fn] [THRESHOLD n]
//===--------------------------------------------------------------------===//

static void WitnessExec(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<RunnerState>();
	if (state.done) {
		return;
	}
	state.done = true;
	auto &inputs = data.bind_data->Cast<RunnerBindData>().inputs;
	auto name = inputs[0].ToString();
	auto aspect = inputs[1].ToString();
	auto speakers = ListValue::GetChildren(inputs[2]);
	auto &detector = inputs[3];
	auto &threshold = inputs[4];

	Inner inner(context);
	auto declared = inner.Row("SELECT aspect, speakers, detector, threshold FROM " + inner.Store("witnesses") +
	                          " WHERE name = " + QuoteText(name) + " ORDER BY seq DESC LIMIT 1");
	if (!declared.empty() && declared[0].ToString() == aspect && declared[1].ToString() == JsonList(inputs[2]) &&
	    SameValue(declared[2], detector) &&
	    (declared[3].IsNull() || threshold.IsNull() ? declared[3].IsNull() == threshold.IsNull()
	                                                : declared[3].GetValue<double>() == threshold.GetValue<double>())) {
		Outcome(output, "witness " + name + " unchanged: no-op");
		return;
	}
	auto standing = inner.Row("SELECT kind, schema::JSON->>'x-kind' FROM " + inner.Store("aspects") +
	                          " WHERE name = " + QuoteText(aspect) + " ORDER BY seq DESC LIMIT 1");
	if (standing.empty()) {
		throw InvalidInputException("glossql: aspect '%s' is not declared", aspect);
	}
	auto &kind = standing[0];
	if (!standing[1].IsNull() && standing[1].ToString() == "grounding") {
		throw InvalidInputException("glossql: a witness on grounding schema '%s' is refused: it judges nothing",
		                            aspect);
	}
	if (kind.ToString() == "MEASUREMENT" && !speakers.empty()) {
		throw InvalidInputException("glossql: BY is refused on a MEASUREMENT aspect: measurements are never glossed");
	}
	if (!detector.IsNull()) {
		auto row = inner.Row("SELECT returns FROM " + inner.Store("functions") +
		                     " WHERE name = " + QuoteText(detector.ToString()) + " ORDER BY seq DESC LIMIT 1");
		if (row.empty()) {
			throw InvalidInputException("glossql: detector '%s' is not a declared function", detector.ToString());
		}
		if (!row[0].IsNull()) {
			throw InvalidInputException("glossql: '%s' returns %s: a measurement, not a detector", detector.ToString(),
			                            row[0].ToString());
		}
	}
	inner.Run("INSERT INTO " + inner.Store("witnesses") + " VALUES (" + inner.Seq("witnesses") + ", " +
	          QuoteText(name) + ", " + QuoteText(aspect) + ", " + QuoteText(JsonList(inputs[2])) + ", " +
	          detector.ToSQLString() + ", " + threshold.ToSQLString() + ", now())");
	Outcome(output, "declared witness " + name + " on " + aspect);
}

TableFunction GlossWitnessFunction() {
	return TableFunction("glossql_declare_witness",
	                     {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::LIST(LogicalType::VARCHAR),
	                      LogicalType::VARCHAR, LogicalType::DOUBLE},
	                     WitnessExec, OutcomeBind, RunnerInit);
}

TableFunction GlossFunctionFunction() {
	return TableFunction("glossql_declare_function",
	                     {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                     FunctionExec, OutcomeBind, RunnerInit);
}

//===--------------------------------------------------------------------===//
// SELECT f(), g() FROM subject: extraction computes each measurement for the
// subject and lands one measurements row per call (SPEC §6)
//===--------------------------------------------------------------------===//

struct ExtractState : public GlobalTableFunctionState {
	bool done = false;
};

static unique_ptr<FunctionData> ExtractBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result = make_uniq<RunnerBindData>();
	result->inputs = input.inputs;
	for (auto column : {"function", "subject", "aspect"}) {
		return_types.push_back(LogicalType::VARCHAR);
		names.emplace_back(column);
	}
	return_types.push_back(LogicalType::JSON());
	names.emplace_back("value");
	// true when this call computed the measurement, false when the landed row served
	return_types.push_back(LogicalType::BOOLEAN);
	names.emplace_back("computed");
	return std::move(result);
}

//! What a body read (SPEC §6): the lake's tables as <dataset>.<table> and the
//! store relations as glossql.<relation>, by name; ** when a read could not be
//! named, such as a file, a catalog function or another attached database;
//! empty for a body that read nothing
struct Reads {
	std::set<string> names;
	bool everything = false;

	string Text() const {
		if (everything) {
			return "**";
		}
		return StringUtil::Join(vector<string>(names.begin(), names.end()), ",");
	}
};

//! Table functions that read no input
static bool ReadsNothing(const string &function) {
	static const std::set<string> PURE = {"range", "generate_series", "repeat", "repeat_row", "unnest"};
	return PURE.count(function) > 0;
}

static void CollectReads(LogicalOperator &op, const Workspace &ws, Reads &reads) {
	if (op.type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op.Cast<LogicalGet>();
		auto table = get.GetTable();
		if (table) {
			auto catalog = string(table->ParentCatalog().GetName());
			auto schema = table->ParentSchema().name.GetIdentifierName();
			auto name = table->name.GetIdentifierName();
			if (catalog == ws.lake) {
				reads.names.insert(schema + "." + name);
			} else if (catalog == ws.meta && schema == ws.meta_schema && StringUtil::StartsWith(name, "glossql_")) {
				reads.names.insert("glossql." + name.substr(8));
			} else {
				reads.everything = true;
			}
		} else if (!ReadsNothing(string(get.function.GetDefinition()->name))) {
			reads.everything = true;
		}
	}
	for (auto &child : op.children) {
		CollectReads(*child, ws, reads);
	}
}

//! The views a relation stands on: itself when it is a view, and the views its
//! query reads, through views over views — legs the plan does not name, since
//! it expands a view to its tables. A name resolves as the binder resolves it,
//! an unqualified one in the view's own schema
static void ViewsOf(ClientContext &context, const Workspace &ws, const string &schema, const string &table,
                    std::set<string> &views) {
	auto entry = RelationEntry(context, schema, table);
	if (!entry || entry->type != CatalogType::VIEW_ENTRY ||
	    !views.insert(schema + "." + entry->name.GetIdentifierName()).second) {
		return;
	}
	auto query = entry->Cast<ViewCatalogEntry>().GetQuery().Copy();
	std::function<void(unique_ptr<ParsedExpression> &)> expressions;
	std::function<void(TableRef &)> refs = [&](TableRef &ref) {
		if (ref.type != TableReferenceType::BASE_TABLE) {
			return;
		}
		auto &name = ref.Cast<BaseTableRef>().GetQualifiedName();
		auto catalog = name.Catalog().GetIdentifierName();
		if (!catalog.empty() && catalog != ws.lake) {
			return;
		}
		auto in = name.Schema().empty() ? schema : name.Schema().GetIdentifierName();
		ViewsOf(context, ws, in, name.Name().GetIdentifierName(), views);
	};
	expressions = [&](unique_ptr<ParsedExpression> &child) {
		if (child->GetExpressionClass() == ExpressionClass::SUBQUERY) {
			auto &subquery = child->Cast<SubqueryExpression>();
			ParsedExpressionIterator::EnumerateQueryNodeChildren(*subquery.Subquery()->node, expressions, refs);
		}
		ParsedExpressionIterator::EnumerateChildren(*child, expressions);
	};
	ParsedExpressionIterator::EnumerateQueryNodeChildren(*query->Cast<SelectStatement>().node, expressions, refs);
}

//! The reads of a call on a subject, enumerated from its bound plan: views
//! expand to their tables, subject_column() to the subject's, before any
//! optimizer touches it; the views the subject stands on are named beside,
//! their definitions being legs too
static string ReadsOf(Inner &inner, const string &call, const string &subject) {
	Reads reads;
	auto &context = *inner.con.context;
	unique_ptr<SQLStatement> statement = ParseSelect("SELECT * FROM " + call);
	context.RunFunctionInTransaction([&]() {
		Planner planner(context);
		planner.CreatePlan(std::move(statement));
		CollectReads(*planner.plan, inner.ws, reads);
		auto parts = StringUtil::Split(subject, '.');
		if (parts.size() >= 2 && subject.find(" -> ") == string::npos && subject.find(" <-> ") == string::npos) {
			std::set<string> views;
			ViewsOf(context, inner.ws, parts[0], parts[1], views);
			reads.names.insert(views.begin(), views.end());
		}
	});
	return reads.Text();
}

//! The pin a computation lands at: the dataset's tables, the store relations
//! and whatever else the reads named, each at its version now
static string PinOf(Inner &inner, const string &dataset, const string &reads) {
	vector<string> named;
	for (auto &name : StringUtil::Split(reads, ',')) {
		named.push_back(QuoteText(name));
	}
	named.push_back("''");
	return inner
	    .Scalar("SELECT coalesce(string_agg(name || ':' || version, ',' ORDER BY name || ':' || version), '') FROM (" +
	            LegsSQL(inner.ws) + ") WHERE starts_with(name, " + QuoteText(dataset + ".") +
	            ") OR starts_with(name, 'glossql.') OR name IN (" + StringUtil::Join(named, ", ") + ")")
	    .ToString();
}

//! The newest landed row for the call while what it read is unchanged, else NULL
static Value ServedValue(Inner &inner, const string &dataset, const string &name, const string &subject) {
	return inner.Scalar("SELECT value FROM (WITH legs AS (" + LegsSQL(inner.ws) + ") SELECT m.value, " +
	                    StandsSQL(inner.ws, "m") + " AS stands FROM " + inner.Store("measurements") +
	                    " m WHERE m.dataset = " + QuoteText(dataset) + " AND m.function = " + QuoteText(name) +
	                    " AND m.subject = " + QuoteText(subject) + " ORDER BY m.seq DESC LIMIT 1) WHERE stands");
}

//! What extraction serves of a value: its top-level summary object when the body
//! carries one, the whole value otherwise (SPEC §6)
static string Summary(Inner &inner, const string &value) {
	return inner
	    .Scalar("SELECT CASE WHEN json_type(v, '$.summary') = 'OBJECT' THEN json_extract(v, '$.summary')::VARCHAR"
	            " ELSE v::VARCHAR END FROM (SELECT " +
	            QuoteText(value) + "::JSON AS v)")
	    .ToString();
}

static unique_ptr<GlobalTableFunctionState> ExtractInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<ExtractState>();
}

//! The value by result shape: one row and one column is the value, one row is an
//! object of its columns, many rows are an array of row objects. The call runs
//! once, into a table of the inner connection's own: a body may be costly or
//! reach outside the process (glossql_service), and the shape, the count and the
//! value are read off what that one run returned
static Value ValueOf(Inner &inner, const string &call) {
	const string once = "__gloss_value";
	inner.Run("CREATE OR REPLACE TEMP TABLE " + once + " AS SELECT * FROM " + call);
	auto shape = inner.Run("SELECT * FROM " + once + " LIMIT 0");
	auto columns = shape->ColumnCount();
	auto rows = inner.Scalar("SELECT count(*) FROM " + once).GetValue<int64_t>();
	string sql;
	if (rows == 1 && columns == 1) {
		sql = "SELECT to_json(" + QuoteName(shape->ColumnName(0).GetIdentifierName()) + ") FROM " + once;
	} else if (rows == 1) {
		sql = "SELECT to_json(t) FROM " + once + " AS t";
	} else {
		sql = "SELECT to_json(list(t)) FROM " + once + " AS t";
	}
	auto value = inner.Scalar(sql);
	inner.Run("DROP TABLE " + once);
	return value;
}

static void ExtractExec(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<ExtractState>();
	if (state.done) {
		return;
	}
	state.done = true;
	auto &inputs = data.bind_data->Cast<RunnerBindData>().inputs;
	auto subject = ResolveSubject(context, inputs[0].ToString());
	auto dataset = DatasetOf(context, subject);

	Inner inner(context);
	idx_t row = 0;
	for (auto &call : ListValue::GetChildren(inputs[1])) {
		auto name = call.ToString();
		auto schema = inner.FunctionSchema(dataset, name);
		if (schema.empty()) {
			throw InvalidInputException("glossql: %s is not a declared function for %s", name, dataset);
		}
		auto scope = schema == "glossql" ? "GLOBAL" : schema;
		auto returns =
		    inner.Scalar("SELECT returns FROM " + inner.Store("functions") + " WHERE name = " + QuoteText(name) +
			             " AND scope = " + QuoteText(scope) + " ORDER BY seq DESC LIMIT 1");
		if (returns.IsNull()) {
			throw InvalidInputException("glossql: %s is a detector: it runs at read, through ATTEST()", name);
		}
		output.data[0].Append(Value(name));
		output.data[1].Append(Value(subject));
		output.data[2].Append(returns);
		// served while what it read is unchanged: a repeat is a serve, not a rerun (SPEC §6)
		auto served = ServedValue(inner, dataset, name, subject);
		if (!served.IsNull()) {
			output.data[3].Append(Value(Summary(inner, served.ToString())));
			output.data[4].Append(Value::BOOLEAN(false));
			row++;
			continue;
		}
		auto invocation = inner.Table(schema, name) + "(" + QuoteText(subject) + ")";
		auto reads = ReadsOf(inner, invocation, subject);
		auto value = ValueOf(inner, invocation);
		if (value.IsNull()) {
			value = Value("null");
		}
		// the aspect's schema is the one contract: the output is validated against it (SPEC §6)
		auto contract = inner.Scalar("SELECT schema::VARCHAR FROM " + inner.Store("aspects") +
		                             " WHERE name = " + returns.ToSQLString() + " ORDER BY seq DESC LIMIT 1");
		if (contract.IsNull()) {
			throw InvalidInputException("glossql: %s returns %s, which is not a declared aspect", name,
			                            returns.ToString());
		}
		auto error = JsonSchemaCheck(contract.ToString(), value.ToString());
		if (!error.empty()) {
			throw InvalidInputException("glossql: the output of %s does not conform to the schema of %s: %s", name,
			                            returns.ToString(), error);
		}
		// landed at its pin, with what it read: the next extraction serves it while those are unchanged
		inner.Run("INSERT INTO " + inner.Store("measurements") + " VALUES (" + inner.Seq("measurements") + ", " +
		          QuoteText(dataset) + ", " + QuoteText(name) + ", " + QuoteText(subject) + ", " +
		          returns.ToSQLString() + ", " + QuoteText(PinOf(inner, dataset, reads)) + ", " +
		          QuoteText(value.ToString()) + ", now(), " + QuoteText(reads) + ")");
		output.data[3].Append(Value(Summary(inner, value.ToString())));
		output.data[4].Append(Value::BOOLEAN(true));
		row++;
	}
	output.CheckCardinality(row);
}

TableFunction GlossExtractFunction() {
	return TableFunction("glossql_extract", {LogicalType::VARCHAR, LogicalType::LIST(LogicalType::VARCHAR)},
	                     ExtractExec, ExtractBind, ExtractInit);
}

//! glossql_select_or_extract(subject, calls, text): the extract rule captured
//! SELECT f(), g() FROM subject. It is an extraction when a call names a
//! declared function; otherwise it is the plain SQL it looks like, parsed with
//! the stock grammar so the rule does not capture it again.
static unique_ptr<TableRef> SelectOrExtractBindReplace(ClientContext &context, TableFunctionBindInput &input) {
	bool declared = false;
	try {
		Inner inner(context);
		auto dataset = DatasetOf(context, ResolveSubject(context, input.inputs[0].ToString()));
		for (auto &call : ListValue::GetChildren(input.inputs[1])) {
			declared = declared || !inner.FunctionSchema(dataset, call.ToString()).empty();
		}
	} catch (std::exception &) {
		// no store, no functions
	}
	if (!declared) {
		return make_uniq<SubqueryRef>(ParseSelect(input.inputs[2].ToString()));
	}
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(ConstantExpression::String(input.inputs[0].ToString()));
	vector<unique_ptr<ParsedExpression>> calls;
	for (auto &call : ListValue::GetChildren(input.inputs[1])) {
		calls.push_back(ConstantExpression::String(call.ToString()));
	}
	children.push_back(make_uniq<FunctionExpression>(Identifier("list_value"), std::move(calls)));
	auto ref = make_uniq<TableFunctionRef>();
	ref->function = make_uniq<FunctionExpression>(Identifier("glossql_extract"), std::move(children));
	return std::move(ref);
}

static TableFunction GlossSelectOrExtractFunction() {
	TableFunction function("glossql_select_or_extract",
	                       {LogicalType::VARCHAR, LogicalType::LIST(LogicalType::VARCHAR), LogicalType::VARCHAR},
	                       nullptr);
	function.bind_replace = SelectOrExtractBindReplace;
	return function;
}

//===--------------------------------------------------------------------===//
// subject_column(subject): the subject's column as a relation named v
//===--------------------------------------------------------------------===//

static unique_ptr<TableRef> SubjectColumnBindReplace(ClientContext &context, TableFunctionBindInput &input) {
	auto subject = ResolveSubject(context, input.inputs[0].ToString());
	auto parts = StringUtil::Split(subject, '.');
	if (parts.size() != 3) {
		throw InvalidInputException("glossql: subject_column takes dataset.table.column, got '%s'", subject);
	}
	auto sql = "SELECT " + QuoteName(parts[2]) + " AS v FROM " + QuoteName(LakeName(context)) + "." +
	           QuoteName(parts[0]) + "." + QuoteName(parts[1]);
	return make_uniq<SubqueryRef>(ParseSelect(sql));
}

static TableFunction SubjectColumnFunction() {
	TableFunction function("subject_column", {LogicalType::VARCHAR}, nullptr);
	function.bind_replace = SubjectColumnBindReplace;
	return function;
}

//===--------------------------------------------------------------------===//
// dataset_cells(subject [, views]): every cell of a dataset's tables, or of one
// table, as one relation — (table, column, ci, column_type, rn, val). The structure
// screens a kit runs over a dataset are SQL over it: a self-join on rn puts a
// row's columns side by side, a join on val matches values across columns.
// val is text; column_type carries the stored type, so a screen matches values
// within one type only; ci numbers the columns in table order, rn the rows of
// each table. Nested columns stay out. Generated at bind, like subject_column,
// over the dataset's own relations (DatasetColumns); a view's rows come from
// glossql_kept_view, kept once per store version, a table's from the table
//===------------------------------------------------------------------===//

static bool IsScalarType(const string &type) {
	return !StringUtil::EndsWith(type, "[]") && !StringUtil::StartsWith(type, "STRUCT") &&
	       !StringUtil::StartsWith(type, "MAP") && !StringUtil::StartsWith(type, "UNION") &&
	       !StringUtil::StartsWith(type, "LIST");
}

static unique_ptr<TableRef> DatasetCellsBindReplace(ClientContext &context, TableFunctionBindInput &input) {
	Inner inner(context);
	auto subject = ResolveSubject(context, input.inputs[0].ToString());
	auto parts = StringUtil::Split(subject, '.');
	if (parts.empty() || parts.size() > 2) {
		throw InvalidInputException("glossql: dataset_cells takes a dataset or dataset.table, got '%s'", subject);
	}
	// a second argument true (or 'all') takes the dataset's views too — a grounding
	// is one — each bound at bind and skipped when it does not, as a screen skips a
	// grounding that fails; 'views' takes the views alone, so that a read over the
	// groundings — a metric's series, a plan's rows — binds no table
	bool views = false;
	bool only_views = false;
	if (input.inputs.size() > 1 && !input.inputs[1].IsNull()) {
		if (input.inputs[1].type().id() == LogicalTypeId::VARCHAR) {
			auto mode = input.inputs[1].ToString();
			if (mode == "views") {
				views = only_views = true;
			} else if (mode == "all") {
				views = true;
			} else if (mode != "tables") {
				throw InvalidInputException("glossql: dataset_cells takes tables, views or all, got '%s'", mode);
			}
		} else {
			views = input.inputs[1].GetValue<bool>();
		}
	}
	const string empty = "SELECT NULL::VARCHAR AS \"table\", NULL::VARCHAR AS \"column\", NULL::BIGINT AS ci,"
	                     " NULL::VARCHAR AS column_type, NULL::BIGINT AS rn, NULL::VARCHAR AS val WHERE false";
	// the columns per table, in the order listed
	vector<string> tables;
	std::map<string, vector<pair<string, string>>> columns;
	std::set<string> kept;
	for (auto &column : DatasetColumns(context, parts[0], parts.size() == 2 ? parts[1] : string(), views)) {
		if (only_views && !column.view) {
			continue;
		}
		if (columns.find(column.table) == columns.end()) {
			tables.push_back(column.table);
		}
		columns[column.table].emplace_back(column.column, column.type);
		if (column.view) {
			kept.insert(column.table);
		}
	}
	if (tables.empty()) {
		return make_uniq<SubqueryRef>(ParseSelect(empty));
	}
	// the version every kept view is read under, once for the bind
	string version = kept.empty() ? string() : StoreVersion(inner);
	string sql;
	idx_t ci = 0;
	for (auto &table : tables) {
		string from = kept.count(table) ? "glossql_kept_view(" + QuoteText(parts[0]) + ", " + QuoteText(table) + ", " +
		                                      QuoteText(version) + ")"
		                                : inner.Table(parts[0], table);
		// the row number under a name no table's column takes, served as rn
		string select = "SELECT row_number() OVER () AS __gloss_rn";
		string names;
		string meta;
		for (auto &column : columns[table]) {
			if (!IsScalarType(column.second)) {
				continue;
			}
			auto name = QuoteName(column.first);
			select += ", " + name + "::VARCHAR AS " + name;
			names += (names.empty() ? "" : ", ") + name;
			meta += (meta.empty() ? "" : ", ") + string("(") + QuoteText(column.first) + ", " + to_string(ci) + ", " +
			        QuoteText(column.second) + ")";
			ci++;
		}
		if (names.empty()) {
			continue;
		}
		sql += (sql.empty() ? "" : " UNION ALL ") + string("SELECT ") + QuoteText(table) +
		       " AS \"table\", u.\"column\", m.ci, m.column_type, u.__gloss_rn AS rn, u.val FROM (FROM (" + select +
		       " FROM " + from + ") UNPIVOT INCLUDE NULLS (val FOR \"column\" IN (" + names + "))) u JOIN (VALUES " +
		       meta + ") m(\"column\", ci, column_type) USING (\"column\")";
	}
	if (sql.empty()) {
		return make_uniq<SubqueryRef>(ParseSelect(empty));
	}
	return make_uniq<SubqueryRef>(ParseSelect(sql));
}

static TableFunctionSet DatasetCellsFunctions() {
	TableFunctionSet set("dataset_cells");
	TableFunction tables("dataset_cells", {LogicalType::VARCHAR}, nullptr);
	tables.bind_replace = DatasetCellsBindReplace;
	set.AddFunction(tables);
	TableFunction with_views("dataset_cells", {LogicalType::VARCHAR, LogicalType::BOOLEAN}, nullptr);
	with_views.bind_replace = DatasetCellsBindReplace;
	set.AddFunction(with_views);
	TableFunction which("dataset_cells", {LogicalType::VARCHAR, LogicalType::VARCHAR}, nullptr);
	which.bind_replace = DatasetCellsBindReplace;
	set.AddFunction(which);
	return set;
}

//===--------------------------------------------------------------------===//
// Registration
//===--------------------------------------------------------------------===//

//===--------------------------------------------------------------------===//
// glossql_kit(path): a kit is a script of declarations — aspects, functions,
// witnesses — read from anything DuckDB's file system opens (a local path, or a
// URL once httpfs is loaded), run statement by statement on a second
// connection under the glossql grammar, one row per statement. Each statement
// commits on its own, as it does from a script; a failing statement stops the
// load and the ones before it stand. A kit run again, as the opener does at
// every start-up, is silent: its unchanged declarations are no-ops and no kits
// row lands; its LOAD lines are what bring an extension into the process
//===--------------------------------------------------------------------===//

struct KitState : public GlobalTableFunctionState {
	bool ran = false;
	vector<pair<string, string>> rows;
	idx_t offset = 0;
};

static unique_ptr<FunctionData> KitBind(ClientContext &context, TableFunctionBindInput &input,
                                        vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result = make_uniq<RunnerBindData>();
	result->inputs = input.inputs;
	return_types.push_back(LogicalType::VARCHAR);
	names.emplace_back("statement");
	return_types.push_back(LogicalType::VARCHAR);
	names.emplace_back("outcome");
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> KitInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<KitState>();
}

static string ReadScript(ClientContext &context, const string &path) {
	auto &fs = FileSystem::GetFileSystem(context);
	auto handle = fs.OpenFile(path, FileOpenFlags::FILE_FLAGS_READ);
	auto size = handle->GetFileSize();
	string text(size, '\0');
	idx_t read = 0;
	while (read < size) {
		auto got = handle->Read((void *)(text.data() + read), size - read);
		if (got <= 0) {
			break;
		}
		read += NumericCast<idx_t>(got);
	}
	text.resize(read);
	return text;
}

//! The kit's name: the file's stem, or its directory's name for a file called kit
static string KitName(const string &path) {
	auto parts = StringUtil::Split(path, '/');
	while (!parts.empty() && parts.back().empty()) {
		parts.pop_back();
	}
	if (parts.empty()) {
		return path;
	}
	auto file = parts.back();
	auto dot = file.rfind('.');
	auto stem = dot == string::npos ? file : file.substr(0, dot);
	if (stem == "kit" && parts.size() > 1) {
		return parts[parts.size() - 2];
	}
	return stem;
}

string FirstLine(const string &text) {
	auto trimmed = text;
	StringUtil::Trim(trimmed);
	auto newline = trimmed.find('\n');
	return newline == string::npos ? trimmed : trimmed.substr(0, newline);
}

static void KitExec(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<KitState>();
	if (!state.ran) {
		state.ran = true;
		auto path = data.bind_data->Cast<RunnerBindData>().inputs[0].ToString();
		auto script = ReadScript(context, path);
		Inner inner(context);
		inner.Run("SET active_grammar_extensions = ['glossql']");
		auto statements = inner.con.ExtractStatements(script);
		for (auto &statement : statements) {
			auto head = FirstLine(statement->query);
			auto result = inner.con.Query(std::move(statement));
			if (result->HasError()) {
				throw InvalidInputException("glossql: kit %s stopped at statement %d (%s): %s", KitName(path),
				                            state.rows.size() + 1, head, result->GetError());
			}
			string outcome = "ok";
			auto chunk = result->Fetch();
			if (chunk && chunk->ColumnCount() == 1 && chunk->size() == 1 &&
			    chunk->data[0].GetType().id() == LogicalTypeId::VARCHAR) {
				outcome = chunk->GetValue(0, 0).ToString();
			}
			state.rows.emplace_back(head, outcome);
		}
		bool landed = false;
		for (auto &row : state.rows) {
			landed = landed || (row.second != "ok" && !StringUtil::EndsWith(row.second, ": no-op"));
		}
		if (landed) {
			inner.Run("INSERT INTO " + inner.Store("kits") + " VALUES (" + inner.Seq("kits") + ", " +
			          QuoteText(KitName(path)) + ", " + QuoteText(path) + ", " + to_string(state.rows.size()) +
			          ", now())");
		}
	}
	idx_t count = 0;
	while (state.offset < state.rows.size() && count < STANDARD_VECTOR_SIZE) {
		output.data[0].Append(Value(state.rows[state.offset].first));
		output.data[1].Append(Value(state.rows[state.offset].second));
		state.offset++;
		count++;
	}
	output.CheckCardinality(count);
}

static TableFunction GlossKitFunction() {
	return TableFunction("glossql_kit", {LogicalType::VARCHAR}, KitExec, KitBind, KitInit);
}

void RegisterGlossRunner(ExtensionLoader &loader) {
	loader.RegisterFunction(GlossKitFunction());
	loader.RegisterFunction(GlossDatasetFunction());
	loader.RegisterFunction(GlossRecipeFunction());
	loader.RegisterFunction(GlossImportFunction());
	loader.RegisterFunction(GlossProbeFunction());
	loader.RegisterFunction(GlossFunctionFunction());
	loader.RegisterFunction(GlossWitnessFunction());
	loader.RegisterFunction(GlossExtractFunction());
	loader.RegisterFunction(GlossSelectOrExtractFunction());
	loader.RegisterFunction(SubjectColumnFunction());
	loader.RegisterFunction(DatasetCellsFunctions());
}

} // namespace duckdb
