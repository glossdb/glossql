// Statements that run SQL: DECLARE DATASET, DECLARE RECIPE, PREVIEW, IMPORT.
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
//
// A write to the store is one transaction of the catalogue database
// (Inner::Write). It takes the lake's next snapshot id as DuckLake takes it,
// from the primary key of ducklake_snapshot, and its rows carry that id as
// their begin marker, so the data and the catalog have one order. A lake
// change DuckLake itself commits (a schema, a load, a macro) comes first and
// its row in the store follows.

#include "glossql_grammar.hpp"

#include <algorithm>
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/file_open_flags.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/common/crypto/md5.hpp"
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
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_search_path.hpp"
#include "duckdb/main/client_data.hpp"
#include <chrono>
#include <functional>
#include <set>
#include <tuple>
#include <map>
#include <thread>

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
		auto preview =
		    con.Query("SELECT 1 FROM " + QuoteName(ws.meta) + "." + QuoteName(schema) + ".ducklake_snapshot LIMIT 0");
		if (!preview->HasError()) {
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
	// and sees what the caller's run sees: a primitive's own reads keep to the run's closure,
	// and the read functions to the run's subject
	if (auto closure = RunClosureOf(context)) {
		SetRunClosure(*con.context, std::move(closure));
	}
	Value subject;
	if (ClientConfig::GetConfig(context).GetUserVariable(string(RUN_SUBJECT), subject)) {
		ClientConfig::GetConfig(*con.context).SetUserVariable(RUN_SUBJECT, subject);
	}
}

string Inner::Current(const string &relation) const {
	return "(SELECT * FROM " + Store(relation) + " WHERE end_snapshot IS NULL)";
}

int64_t CatalogWrite::Run(const string &sql) {
	auto result = inner.con.Query(sql);
	if (result->HasError()) {
		// the catalog database's own error, its class kept: Inner::Write tells a lost race from a refusal
		result->ThrowError();
	}
	wrote = true;
	auto chunk = result->Fetch();
	if (!chunk || chunk->size() == 0 || chunk->ColumnCount() == 0 || chunk->GetValue(0, 0).IsNull()) {
		return 0;
	}
	return chunk->GetValue(0, 0).GetValue<int64_t>();
}

int64_t CatalogWrite::End(const string &relation, const string &where) {
	return Run("UPDATE " + inner.Store(relation) + " SET end_snapshot = " + to_string(snapshot) +
	           " WHERE end_snapshot IS NULL AND (" + where + ")");
}

void CatalogWrite::Insert(const string &relation, const string &values) {
	Run("INSERT INTO " + inner.Store(relation) + " VALUES (" + to_string(snapshot) + ", NULL, " + values + ")");
}

int64_t CatalogWrite::NewCatalogId() {
	return next_catalog_id++;
}

void CatalogWrite::Changed(const string &entry) {
	changes.push_back(entry);
}

// A failed write lost a race when another writer overtook it: the catalog
// database refused the snapshot row because its id is taken (DuckDB's primary key,
// raised once the other writer commits), reports a transaction conflict with a
// writer still open (DuckDB's write-write conflict), or the lake's newest snapshot
// is past the one the write read (Postgres raises its unique or serialization
// error, of whatever class the scanner gives it, once the other writer has
// committed). Any other failure is the write's own and is refused as it is.
bool Inner::Overtaken(const ErrorData &error, int64_t head, bool taking_snapshot) {
	if (error.Type() == ExceptionType::TRANSACTION || (taking_snapshot && error.Type() == ExceptionType::CONSTRAINT)) {
		return true;
	}
	if (head < 0) {
		return false;
	}
	auto newest = con.Query("SELECT max(snapshot_id) > " + to_string(head) + " FROM " + ws.Meta("snapshot"));
	if (newest->HasError()) {
		return false;
	}
	auto chunk = newest->Fetch();
	return chunk && chunk->size() > 0 && chunk->GetValue(0, 0) == Value::BOOLEAN(true);
}

int64_t Inner::Write(ClientContext &context, const std::function<void(CatalogWrite &)> &body) {
	// the tries a write takes before it gives the caller the last reason
	static constexpr idx_t TRIES = 60;
	string lost;
	for (idx_t attempt = 0; attempt < TRIES; attempt++) {
		Run("BEGIN");
		int64_t head_id = -1;
		bool taking_snapshot = false;
		try {
			auto head = Row("SELECT snapshot_id, schema_version, next_catalog_id, next_file_id FROM " +
			                ws.Meta("snapshot") + " ORDER BY snapshot_id DESC LIMIT 1");
			head_id = head[0].GetValue<int64_t>();
			CatalogWrite write(*this, head_id + 1, head[2].GetValue<int64_t>());
			body(write);
			if (!write.wrote) {
				con.Query("ROLLBACK");
				return 0;
			}
			// the snapshot as DuckLake writes one: a lake change moves the schema version, and its
			// change text names it; a write to the store alone has no change text
			auto schema_version = head[1].GetValue<int64_t>() + (write.changes.empty() ? 0 : 1);
			taking_snapshot = true;
			write.Run("INSERT INTO " + ws.Meta("snapshot") + " VALUES (" + to_string(write.snapshot) + ", now(), " +
			          to_string(schema_version) + ", " + to_string(write.next_catalog_id) + ", " + head[3].ToString() +
			          ")");
			write.Run("INSERT INTO " + ws.Meta("snapshot_changes") + " VALUES (" + to_string(write.snapshot) + ", " +
			          (write.changes.empty() ? string("NULL") : QuoteText(StringUtil::Join(write.changes, ","))) +
			          ", " + QuoteText(DeclaredBy(context)) + ", NULL, NULL)");
			write.Run("COMMIT");
			return write.snapshot;
		} catch (std::exception &ex) {
			ErrorData error(ex);
			con.Query("ROLLBACK");
			if (!Overtaken(error, head_id, taking_snapshot)) {
				throw;
			}
			lost = FirstLine(error.Message());
			std::this_thread::sleep_for(std::chrono::milliseconds(1 + (std::rand() % (5 * (attempt + 1)))));
		}
	}
	throw InvalidInputException("glossql: the write lost %d races to other writers; the last: %s", TRIES, lost);
}

Value Inner::Snapshot() {
	return Scalar("SELECT id::BIGINT FROM ducklake_current_snapshot(" + QuoteText(ws.lake) + ")");
}

// The reason of a statement the inner connection refused. A refusal, raised by
// error() in a function body or by the extension itself, is carried as it was
// raised: its error class and the prefix are the ones the rethrow adds, once.
// An error of another class keeps its class's name.
static string Reason(ErrorData &error) {
	if (error.Type() != ExceptionType::INVALID_INPUT) {
		return error.Message();
	}
	const auto &reason = error.RawMessage();
	return StringUtil::StartsWith(reason, "glossql: ") ? reason.substr(9) : reason;
}

unique_ptr<QueryResult> Inner::Run(const string &sql) {
	auto result = con.Query(sql);
	if (result->HasError()) {
		throw InvalidInputException("glossql: %s", Reason(result->GetErrorObject()));
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
		auto found = Scalar("SELECT count(*) FROM " + Current("functions") + " WHERE name = " + QuoteText(name) +
		                    " AND scope = " + QuoteText(scope));
		if (found.GetValue<int64_t>() > 0) {
			return scope == "GLOBAL" ? "main" : scope;
		}
	}
	return "";
}

//! A declared source, as its newest row holds
struct SourceInfo {
	string name;
	string type;
	string location;
};

static SourceInfo LookupSource(Inner &inner, const string &name) {
	auto row = inner.Row("SELECT settings::JSON->>'type', settings::JSON->>'location' FROM " + inner.Store("sources") +
	                     " WHERE name = " + QuoteText(name) + " AND end_snapshot IS NULL");
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
		return result;
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
	return result;
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
	if (name.find('.') != string::npos) {
		throw InvalidInputException("glossql: a dataset's name carries no dot (SPEC §4): %s", name);
	}

	// the schema is the lake's commit and its row in the store follows;
	// an unchanged re-declaration is a no-op (SPEC §3)
	Inner inner(context);
	inner.Run("CREATE SCHEMA IF NOT EXISTS " + QuoteName(inner.ws.lake) + "." + QuoteName(name));
	auto written = inner.Write(context, [&](CatalogWrite &write) {
		auto current =
		    inner.Row("SELECT settings FROM " + inner.Current("datasets") + " WHERE name = " + QuoteText(name));
		if (!current.empty() && current[0].ToString() == inputs[1].ToString()) {
			return;
		}
		write.End("datasets", "name = " + QuoteText(name));
		write.Insert("datasets",
		             QuoteText(name) + ", " + inputs[1].ToSQLString() + ", now(), " + QuoteText(DeclaredBy(context)));
	});
	Outcome(output, written == 0 ? "dataset " + name + " unchanged: no-op"
	                             : "declared dataset " + name + " (schema " + inner.ws.lake + "." + name + ")");
}

TableFunction GlossDatasetFunction() {
	return TableFunction("glossql_declare_dataset", {LogicalType::VARCHAR, LogicalType::VARCHAR}, DatasetExec,
	                     OutcomeBind, RunnerInit);
}

//===--------------------------------------------------------------------===//
// Load: the recipe SQL becomes the table, whole or merged by key
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
	                     QuoteText(dataset) + " AND table_name = " + QuoteText(table) + " AND end_snapshot IS NULL");
	if (row.empty()) {
		throw InvalidInputException("glossql: %s.%s has no recipe — tables come from recipes", dataset, table);
	}
	return RecipeRow {row[0].ToString(), row[1].ToString(), row[2].ToString()};
}

//! What a load wrote: the table's rows, how they joined it (`mode`: replace
//! or merge, the key being the recipe's), and the rows dropped on the way
//! (SPEC §3), NULL where the host cannot count them
struct Loaded {
	int64_t rows;
	string mode;
	//! the outcome's wording of the mode
	string how;
	Value dropped;
	//! the lake's snapshot the load made
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

//! The recipe is the load: what its SQL yields is what the table gets, so
//! nothing is lost between source and table. The one reader that drops rows
//! is one told to ignore errors; when it stores its rejects, the load's
//! connection holds them and the count is theirs, one per line; when it does
//! not, the host cannot count what it skipped
static Value DroppedRows(Inner &inner, const string &sql) {
	// looked up, never previewed: a failing statement would abort the load's transaction
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

//! A keyed recipe's result carries each key once (SPEC §3): the keys the
//! result carries more than once, counted at the source before anything is stored;
//! the refusal names the key, how many, and one of them. Which row holds is
//! the recipe's to say, so the result is never deduplicated here
static void KeyOnce(Inner &inner, const vector<string> &key, const string &sql) {
	vector<string> columns;
	vector<string> texts;
	for (auto &column : key) {
		columns.push_back(QuoteName(column));
		texts.push_back(QuoteName(column) + "::VARCHAR");
	}
	auto spelled = StringUtil::Join(columns, ", ");
	auto example = key.size() == 1 ? texts[0] : "'(' || concat_ws(', ', " + StringUtil::Join(texts, ", ") + ") || ')'";
	auto twice = inner.Row("SELECT count(*), min(" + example + ") FROM (SELECT " + spelled + " FROM (" + sql +
	                       ") GROUP BY ALL HAVING count(*) > 1)");
	auto keys = twice[0].GetValue<int64_t>();
	if (keys > 0) {
		throw InvalidInputException("glossql: the recipe's result carries key %s more than once: %d keys, %s among "
		                            "them; a keyed recipe's result carries each key once (SPEC §3)",
		                            StringUtil::Join(key, ", "), keys, twice[1].ToString());
	}
}

//! Runs the recipe at its source and writes the result
static Loaded LoadTable(ClientContext &context, Inner &inner, const string &dataset, const string &table,
                        const RecipeRow &recipe, bool replace) {
	auto target = inner.Table(dataset, table);
	AtSource(inner, LookupSource(inner, recipe.source));
	Loaded loaded;
	auto key = recipe.Key();
	if (!key.empty()) {
		KeyOnce(inner, key, recipe.sql);
	}
	if (replace || key.empty()) {
		inner.Run("CREATE OR REPLACE TABLE " + target + " AS " + recipe.sql);
		loaded.mode = "replace";
		loaded.how = "replaced";
	} else {
		vector<string> on;
		for (auto &column : key) {
			on.push_back("t." + QuoteName(column) + " = s." + QuoteName(column));
		}
		inner.Run("MERGE INTO " + target + " AS t USING (" + recipe.sql + ") AS s ON " + StringUtil::Join(on, " AND ") +
		          " WHEN MATCHED THEN UPDATE WHEN NOT MATCHED THEN INSERT");
		loaded.mode = "merge";
		loaded.how = "merged by " + StringUtil::Join(key, ", ");
	}
	loaded.rows = inner.Scalar("SELECT count(*) FROM " + target).GetValue<int64_t>();
	loaded.dropped = DroppedRows(inner, recipe.sql);
	// the load's own commit, whatever another writer committed since
	loaded.snapshot = inner.Scalar("SELECT * FROM ducklake_last_committed_snapshot(" + QuoteText(inner.ws.lake) + ")");
	// what computed each column, read off the recipe's plan at the source: the
	// re-derivation rule compares loads by it (SPEC §3), and the loads' diff
	// is the drift record of what the source sends
	string lineage = "{}";
	try {
		lineage = LineageJson(LineageOf(*inner.con.context, recipe.sql), recipe.sql);
	} catch (std::exception &) {
		// a recipe the planner will not walk again is stored without its lineage
	}
	// the load is the lake's commit, one snapshot; its row in the store follows it and names
	// that snapshot, and the table's earlier load row ends
	inner.Write(context, [&](CatalogWrite &write) {
		write.End("imports", "dataset = " + QuoteText(dataset) + " AND table_name = " + QuoteText(table));
		write.Insert("imports", QuoteText(dataset) + ", " + QuoteText(table) + ", " + QuoteText(loaded.mode) + ", " +
		                            to_string(loaded.rows) + ", " + loaded.dropped.ToSQLString() + ", " +
		                            QuoteText(lineage) + ", " + loaded.snapshot.ToSQLString() + ", now()");
	});
	return loaded;
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
	if (name.find('.') != string::npos) {
		throw InvalidInputException("glossql: a table's name carries no dot (SPEC §4): %s", name);
	}
	RecipeRow recipe {inputs[2].ToString(), inputs[3].ToString(), inputs[4].ToString()};

	Inner inner(context);
	// identity is content (SPEC §3): an unchanged re-declaration is a no-op
	auto current = inner.Row("SELECT source, settings, sql FROM " + inner.Store("recipes") + " WHERE dataset = " +
	                         QuoteText(dataset) + " AND table_name = " + QuoteText(name) + " AND end_snapshot IS NULL");
	if (!current.empty() && current[0].ToString() == recipe.source && current[1].ToString() == recipe.settings &&
	    current[2].ToString() == recipe.sql) {
		Outcome(output, "recipe " + name + " unchanged: no-op");
		return;
	}
	auto dataset_exists =
	    inner.Scalar("SELECT count(*) FROM " + inner.Current("datasets") + " WHERE name = " + QuoteText(dataset));
	if (dataset_exists.GetValue<int64_t>() == 0) {
		throw InvalidInputException("glossql: dataset '%s' is not declared", dataset);
	}

	auto loaded = LoadTable(context, inner, dataset, name, recipe, true);
	inner.Write(context, [&](CatalogWrite &write) {
		write.End("recipes", "dataset = " + QuoteText(dataset) + " AND table_name = " + QuoteText(name));
		write.Insert("recipes", QuoteText(dataset) + ", " + QuoteText(name) + ", " + QuoteText(recipe.source) + ", " +
		                            QuoteText(recipe.settings) + ", " + QuoteText(recipe.sql) + ", now(), " +
		                            QuoteText(DeclaredBy(context)));
	});
	loaded.how = current.empty() ? "new" : "superseded";
	Outcome(output, "loaded " + dataset + "." + name + ": " + loaded.Summary());
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
	auto loaded = LoadTable(context, inner, dataset, table, recipe, false);
	Outcome(output, "imported " + dataset + "." + table + ": " + loaded.Summary());
}

TableFunction GlossImportFunction() {
	return TableFunction("glossql_import", {LogicalType::VARCHAR, LogicalType::VARCHAR}, ImportExec, OutcomeBind,
	                     RunnerInit);
}

//===--------------------------------------------------------------------===//
// PREVIEW source AS $$sql$$: the recipe rehearsal, load nothing
//===--------------------------------------------------------------------===//

struct PreviewState : public GlobalTableFunctionState {
	unique_ptr<Inner> inner;
	unique_ptr<QueryResult> result;
	unique_ptr<DataChunk> chunk;
};

static unique_ptr<FunctionData> PreviewBind(ClientContext &context, TableFunctionBindInput &input,
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
		throw InvalidInputException("glossql: preview of %s: %s", source, prepared->GetError());
	}
	return_types = prepared->GetTypes();
	names = prepared->GetNames();
	return result;
}

static unique_ptr<GlobalTableFunctionState> PreviewInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<PreviewState>();
}

static void PreviewExec(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<PreviewState>();
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

TableFunction GlossPreviewFunction() {
	return TableFunction("glossql_preview", {LogicalType::VARCHAR, LogicalType::VARCHAR}, PreviewExec, PreviewBind,
	                     PreviewInit);
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

//! What a body reads, taken from its text as written (SPEC §6): DuckDB's parse of
//! it as JSON (json_serialize_sql), every node of the tree, the names of its CTEs
//! subtracted, the macros it calls followed through their definitions. One JSON
//! object per read: kind `read` a call of a read function (annotations, results,
//! definition), its what and its where as written, `constant` false where one of
//! them is not a constant; `data` a table of the lake named in FROM, or a
//! primitive over the data of the subject; `catalog` the catalog reached
//! otherwise; `other` a table function that reads neither (range, unnest). A
//! detector's body reads the annotations its test hands it, a relation by that name
static string BodyReads(Inner &inner, const string &script, bool detector) {
	auto text = detector ? "WITH annotations AS (SELECT 1) SELECT * FROM (" + script + ")" : script;
	return inner
	    .Scalar(
	        "WITH RECURSIVE boundary(fn) AS (VALUES ('annotations'), ('results'), ('definition')),"
	        " macros AS (SELECT function_name AS fn, any_value(function_type) AS function_type,"
	        " any_value(macro_definition) AS macro_definition FROM duckdb_functions()"
	        " WHERE macro_definition IS NOT NULL AND database_name <> 'system'"
	        " AND function_name NOT IN (SELECT fn FROM boundary) GROUP BY function_name),"
	        " src(via, depth, sql_text) AS (SELECT '', 0, 'SELECT * FROM (' || " +
	        QuoteText(text) +
	        " || ')' UNION SELECT s.via || CASE WHEN s.via = '' THEN '' ELSE ' > ' END || m.fn, s.depth + 1,"
	        " CASE WHEN m.function_type = 'table_macro' THEN 'SELECT * FROM (' || m.macro_definition || ')'"
	        " ELSE 'SELECT ' || m.macro_definition END"
	        " FROM src s, unnest(json_extract(json_serialize_sql(s.sql_text), '$..*')) t(n) JOIN macros m ON m.fn ="
	        " coalesce(json_extract_string(n, '$.function_name'), json_extract_string(n, '$.function.function_name'))"
	        " WHERE s.depth < 8 AND (json_extract_string(n, '$.class') = 'FUNCTION'"
	        " OR json_extract_string(n, '$.type') = 'TABLE_FUNCTION')),"
	        " tree AS (SELECT via, json_serialize_sql(sql_text) AS j FROM src),"
	        " ctes AS (SELECT DISTINCT via, unnest(json_extract_string(j, '$..cte_map.map[*].key')) AS cte FROM tree),"
	        " nodes AS (SELECT via, n FROM tree, unnest(json_extract(j, '$..*')) t(n)),"
	        " calls AS (SELECT via, json_extract_string(n, '$.function.function_name') AS fn,"
	        " [CASE WHEN json_extract_string(a, '$.expression.class') = 'CONSTANT'"
	        " THEN json_extract_string(a, '$.expression.literal.text')"
	        " WHEN json_extract_string(a, '$.expression.class') = 'PARAMETER'"
	        " THEN '$' || json_extract_string(a, '$.expression.identifier')"
	        " ELSE '<' || json_extract_string(a, '$.expression.class') || '>' END"
	        " FOR a IN json_extract(n, '$.function.arguments[*]')] AS args"
	        " FROM nodes WHERE json_extract_string(n, '$.type') = 'TABLE_FUNCTION'),"
	        " tables AS (SELECT via, nullif(json_extract_string(n, '$.schema_name'), '') AS schema_name,"
	        " json_extract_string(n, '$.table_name') AS table_name FROM nodes"
	        " WHERE json_extract_string(n, '$.type') = 'BASE_TABLE'),"
	        " reads AS (SELECT DISTINCT via, 'read' AS kind, fn AS read,"
	        " CASE WHEN fn = 'definition' THEN 'definition' ELSE args[1] END AS what, args[-1] AS \"where\","
	        " NOT coalesce(list_bool_or([x IS NULL OR starts_with(x, '<') OR starts_with(x, '$')"
	        " FOR x IN args]), false) AS constant FROM calls WHERE fn IN (SELECT fn FROM boundary)"
	        " UNION ALL SELECT DISTINCT via, CASE WHEN fn IN ('subject_column', 'dataset_cells',"
	        " 'glossql_relationship_checks') THEN 'data' WHEN fn LIKE 'glossql_%' OR fn IN ('glossary', 'tests')"
	        " THEN 'catalog' ELSE 'other' END, fn, array_to_string(args, ', '), NULL, NULL FROM calls"
	        " WHERE fn NOT IN (SELECT fn FROM boundary) AND fn NOT IN (SELECT fn FROM macros)"
	        " UNION ALL SELECT DISTINCT t.via, CASE WHEN t.schema_name = 'main' OR regexp_replace(t.table_name,"
	        " '_history$', '') IN ('datasets', 'kits', 'sources', 'recipes', 'imports', 'aspects', 'glossary',"
	        " 'relationships', 'functions', 'measurements', 'inputs', 'tests') THEN 'catalog' ELSE 'data' END,"
	        " coalesce(t.schema_name || '.', '') || t.table_name, NULL, NULL, NULL FROM tables t"
	        " WHERE NOT EXISTS (SELECT 1 FROM ctes c WHERE c.via = t.via AND c.cte = t.table_name"
	        " AND t.schema_name IS NULL))"
	        " SELECT coalesce(to_json(list({'kind': kind, 'read': read, 'what': what, 'where': \"where\","
	        " 'constant': constant, 'via': via} ORDER BY kind, read, what, \"where\", via)), '[]')::VARCHAR FROM reads")
	    .ToString();
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
	auto reads = BodyReads(inner, script, returns.IsNull());
	auto current = inner.Row("SELECT scope, script, returns, reads FROM " + inner.Current("functions") +
	                         " WHERE name = " + QuoteText(name) + " ORDER BY begin_snapshot DESC LIMIT 1");
	if (!current.empty() && current[0].ToString() == scope && current[1].ToString() == script &&
	    SameValue(current[2], returns) && SameValue(current[3], Value(reads))) {
		Outcome(output, "function " + name + " unchanged: no-op");
		return;
	}
	auto schema = scope == "GLOBAL" ? string("main") : scope;
	if (scope != "GLOBAL") {
		auto declared =
		    inner.Scalar("SELECT count(*) FROM " + inner.Current("datasets") + " WHERE name = " + QuoteText(scope));
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
	// the macro is the lake's commit and its row in the store follows
	inner.Write(context, [&](CatalogWrite &write) {
		write.End("functions", "name = " + QuoteText(name) + " AND scope = " + QuoteText(scope));
		write.Insert("functions", QuoteText(name) + ", " + QuoteText(scope) + ", " + QuoteText(script) + ", " +
		                              returns.ToSQLString() + ", " + QuoteText(reads) + ", now(), " +
		                              QuoteText(DeclaredBy(context)));
	});
	// a detector's body is run by TESTS() over its test's values, $threshold bound there
	Outcome(output, returns.IsNull() ? "declared detector " + name + " for " + scope
	                                 : "declared measurement " + schema + "." + name + "($subject)");
}

//===--------------------------------------------------------------------===//
// DECLARE TEST name ON aspect [BY (AGENT, HUMAN)] [DETECTOR fn] [THRESHOLD n]
//===--------------------------------------------------------------------===//

static void TestExec(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<RunnerState>();
	if (state.done) {
		return;
	}
	state.done = true;
	auto &inputs = data.bind_data->Cast<RunnerBindData>().inputs;
	auto name = inputs[0].ToString();
	auto aspect = inputs[1].ToString();
	auto actor_kinds = ListValue::GetChildren(inputs[2]);
	auto &detector = inputs[3];
	auto &threshold = inputs[4];

	Inner inner(context);
	auto written = inner.Write(context, [&](CatalogWrite &write) {
		auto declared = inner.Row("SELECT aspect, actors, detector, threshold FROM " + inner.Current("tests") +
		                          " WHERE name = " + QuoteText(name) + " ORDER BY begin_snapshot DESC LIMIT 1");
		if (!declared.empty() && declared[0].ToString() == aspect && declared[1].ToString() == JsonList(inputs[2]) &&
		    SameValue(declared[2], detector) &&
		    (declared[3].IsNull() || threshold.IsNull()
		         ? declared[3].IsNull() == threshold.IsNull()
				 : declared[3].GetValue<double>() == threshold.GetValue<double>())) {
			return;
		}
		auto current = inner.Row("SELECT kind, schema::JSON->>'x-kind' FROM " + inner.Current("aspects") +
		                         " WHERE name = " + QuoteText(aspect));
		if (current.empty()) {
			throw InvalidInputException("glossql: aspect '%s' is not declared", aspect);
		}
		auto &kind = current[0];
		if (!current[1].IsNull() && current[1].ToString() == "definition") {
			throw InvalidInputException("glossql: a test on definition schema '%s' is refused: it judges nothing",
			                            aspect);
		}
		if (kind.ToString() == "MEASUREMENT" && !actor_kinds.empty()) {
			throw InvalidInputException(
			    "glossql: BY is refused on a MEASUREMENT aspect: measurements are never glossed");
		}
		if (!detector.IsNull()) {
			auto row = inner.Row("SELECT returns FROM " + inner.Current("functions") + " WHERE name = " +
			                     QuoteText(detector.ToString()) + " ORDER BY begin_snapshot DESC LIMIT 1");
			if (row.empty()) {
				throw InvalidInputException("glossql: detector '%s' is not a declared function", detector.ToString());
			}
			if (!row[0].IsNull()) {
				throw InvalidInputException("glossql: '%s' returns %s: a measurement, not a detector",
				                            detector.ToString(), row[0].ToString());
			}
		}
		write.End("tests", "aspect = " + QuoteText(aspect));
		write.Insert("tests", QuoteText(name) + ", " + QuoteText(aspect) + ", " + QuoteText(JsonList(inputs[2])) +
		                          ", " + detector.ToSQLString() + ", " + threshold.ToSQLString() + ", now(), " +
		                          QuoteText(DeclaredBy(context)));
	});
	Outcome(output, written == 0 ? "test " + name + " unchanged: no-op" : "declared test " + name + " on " + aspect);
}

TableFunction GlossTestFunction() {
	return TableFunction("glossql_declare_test",
	                     {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::LIST(LogicalType::VARCHAR),
	                      LogicalType::VARCHAR, LogicalType::DOUBLE},
	                     TestExec, OutcomeBind, RunnerInit);
}

TableFunction GlossFunctionFunction() {
	return TableFunction("glossql_declare_function",
	                     {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                     FunctionExec, OutcomeBind, RunnerInit);
}

//===--------------------------------------------------------------------===//
// SELECT f(), g() FROM subject: extraction computes each measurement for the
// subject and writes one measurements row per call (SPEC §6)
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
	// true when this call computed the measurement, false when the stored row served
	return_types.push_back(LogicalType::BOOLEAN);
	names.emplace_back("computed");
	return result;
}

//! What a body read (SPEC §6): the lake's tables as <dataset>.<table> and the
//! store relations as main.<relation>, by name; ** when a read could not be
//! named, such as a file, a catalog function or another attached database;
//! empty for a body that read nothing
struct Reads {
	std::set<string> names;
	bool everything = false;
	//! each scan of a relation that is read by a key, with the keys the filters above it name:
	//! the stored results by function, the annotations by property
	std::map<const LogicalOperator *, std::set<string>> measurements;
	std::map<const LogicalOperator *, std::set<string>> glossary;

	//! A scan names the keys its filter equates the key column with:
	//! main.measurements/<function>, main.glossary/<property>. A scan with no
	//! such filter names the whole relation
	void Close() {
		for (auto &scan : measurements) {
			if (scan.second.empty()) {
				names.insert("main.measurements");
			}
			for (auto &function : scan.second) {
				names.insert("main.measurements/" + function);
			}
		}
		for (auto &scan : glossary) {
			if (scan.second.empty()) {
				names.insert("main.glossary");
			}
			for (auto &aspect : scan.second) {
				names.insert("main.glossary/" + aspect);
			}
		}
	}

	//! `**` when the reads could not be enumerated, the names it did enumerate
	//! after it: a store relation the body scanned is among its inputs even then
	string Text() const {
		auto listed = StringUtil::Join(vector<string>(names.begin(), names.end()), ",");
		if (everything) {
			return listed.empty() ? "**" : "**," + listed;
		}
		return listed;
	}
};

//! Table functions whose scan is no input the plan must name: those that read
//! nothing, and those whose bind names what they read (BindRead)
static bool ReadsNothing(const string &function) {
	static const std::set<string> NAMED = {"range",
	                                       "generate_series",
	                                       "repeat",
	                                       "repeat_row",
	                                       "unnest",
	                                       "glossql_cached",
	                                       "glossql_columns",
	                                       "glossql_views_lineage",
	                                       "glossql_views_scans",
	                                       "glossql_views_joins"};
	return NAMED.count(function) > 0;
}

//! The operator in the subtree that owns a column binding's table index
static optional_ptr<LogicalOperator> OwnerOf(LogicalOperator &op, TableIndex table_index) {
	for (auto index : op.GetTableIndex()) {
		if (index == table_index) {
			return &op;
		}
	}
	for (auto &child : op.children) {
		auto found = OwnerOf(*child, table_index);
		if (found) {
			return found;
		}
	}
	return nullptr;
}

//! A store relation that is read by a key: its table, and the column that is the key
struct KeyedRelation {
	const char *table;
	const char *column;
};
static constexpr KeyedRelation RESULTS_BY_FUNCTION = {"glossql_measurements", "function"};
static constexpr KeyedRelation ANNOTATIONS_BY_PROPERTY = {"glossql_glossary", "aspect"};

//! The scan of the keyed relation whose key column an expression is, followed
//! through the projections between (a view expands to one), or none
static optional_ptr<LogicalOperator> KeyColumnOf(LogicalOperator &root, const Workspace &ws,
                                                 const KeyedRelation &relation, Expression &expr) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
		return nullptr;
	}
	auto binding = expr.Cast<BoundColumnRefExpression>().Binding();
	auto owner = OwnerOf(root, binding.table_index);
	if (!owner) {
		return nullptr;
	}
	if (owner->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		auto &projection = owner->Cast<LogicalProjection>();
		if (binding.column_index.GetIndex() >= projection.expressions.size()) {
			return nullptr;
		}
		return KeyColumnOf(*owner, ws, relation, *projection.expressions[binding.column_index.GetIndex()]);
	}
	if (owner->type != LogicalOperatorType::LOGICAL_GET) {
		return nullptr;
	}
	auto &get = owner->Cast<LogicalGet>();
	auto table = get.GetTable();
	if (!table || string(table->ParentCatalog().GetName()) != ws.meta ||
	    table->ParentSchema().name.GetIdentifierName() != ws.meta_schema ||
	    table->name.GetIdentifierName() != relation.table) {
		return nullptr;
	}
	auto &column_ids = get.GetColumnIds();
	if (binding.column_index.GetIndex() >= column_ids.size()) {
		return nullptr;
	}
	auto column = column_ids[binding.column_index.GetIndex()].GetPrimaryIndex();
	return column < get.names.size() && get.names[column] == relation.column ? owner : nullptr;
}

//! The value an expression is whatever the row: a literal, under the casts the
//! binder wraps one in, or an expression over literals alone (a body's
//! `split_part($subject, '.', 2)`, its subject being a literal); none otherwise
static bool ConstantOf(ClientContext &context, Expression &expr, string &value) {
	if (!expr.IsFoldable()) {
		return false;
	}
	Value folded;
	if (!ExpressionExecutor::TryEvaluateScalar(context, expr, folded) || folded.IsNull()) {
		return false;
	}
	value = folded.ToString();
	return true;
}

//! The keys a filter's expression restricts a keyed relation's scan to: `key =
//! 'x'`, `key IN (...)`, each side of an AND. An OR restricts a scan only where
//! every side of it does. Returns whether the expression restricted any scan
static bool KeyPredicates(ClientContext &context, LogicalOperator &root, const Workspace &ws,
                          const KeyedRelation &relation, Expression &expr,
                          std::map<const LogicalOperator *, std::set<string>> &scans) {
	auto type = expr.GetExpressionType();
	if (type == ExpressionType::COMPARE_EQUAL || type == ExpressionType::COMPARE_IN) {
		optional_ptr<LogicalOperator> scan;
		vector<string> constants;
		bool other = false;
		ExpressionIterator::EnumerateChildren(expr, [&](Expression &child) {
			string constant;
			if (ConstantOf(context, child, constant)) {
				constants.push_back(constant);
			} else if (!scan) {
				scan = KeyColumnOf(root, ws, relation, child);
				other = other || !scan;
			} else {
				other = true;
			}
		});
		if (!scan || other || constants.empty()) {
			return false;
		}
		for (auto &constant : constants) {
			scans[scan.get()].insert(constant);
		}
		return true;
	}
	if (type == ExpressionType::CONJUNCTION_AND) {
		bool any = false;
		ExpressionIterator::EnumerateChildren(
		    expr, [&](Expression &child) { any = KeyPredicates(context, root, ws, relation, child, scans) || any; });
		return any;
	}
	if (type == ExpressionType::CONJUNCTION_OR) {
		// a side that names no key lets every row through: the OR restricts nothing
		std::map<const LogicalOperator *, std::set<string>> sides;
		bool every = true;
		ExpressionIterator::EnumerateChildren(expr, [&](Expression &child) {
			every = KeyPredicates(context, root, ws, relation, child, sides) && every;
		});
		if (!every || sides.size() != 1) {
			return false;
		}
		for (auto &side : sides) {
			scans[side.first].insert(side.second.begin(), side.second.end());
		}
		return true;
	}
	return false;
}

//! Whether an expression calls glossql_sees: the store views' narrowing to a run's closure
static bool CallsSees(Expression &expr) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION &&
	    expr.Cast<BoundFunctionExpression>().Function().GetName().GetIdentifierName() == "glossql_sees") {
		return true;
	}
	bool found = false;
	ExpressionIterator::EnumerateChildren(expr, [&](Expression &child) { found = found || CallsSees(child); });
	return found;
}

static void CollectReads(ClientContext &context, LogicalOperator &op, const Workspace &ws, Reads &reads,
                         optional_ptr<LogicalOperator> parent = nullptr) {
	if (op.type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op.Cast<LogicalGet>();
		auto table = get.GetTable();
		if (table) {
			auto catalog = string(table->ParentCatalog().GetName());
			auto schema = table->ParentSchema().name.GetIdentifierName();
			auto name = table->name.GetIdentifierName();
			bool store = catalog == ws.meta && schema == ws.meta_schema;
			// rows about subjects are read through the store's views, which keep a run to its
			// closure: a scan of their table outside those views could read past it
			if (store && StringUtil::StartsWith(name, "glossql_") && ScopedInput("main." + name.substr(8))) {
				bool narrowed = false;
				if (parent && parent->type == LogicalOperatorType::LOGICAL_FILTER) {
					for (auto &expr : parent->expressions) {
						narrowed = narrowed || CallsSees(*expr);
					}
				}
				if (!narrowed) {
					throw InvalidInputException("glossql: a function reads %s past the catalog's views: it reads the "
					                            "glossary, the results and the declarations through main.%s, "
					                            "GLOSSARY() or TESTS(), which keep it to its subject's closure",
					                            name, name.substr(8));
				}
			}
			if (catalog == ws.lake) {
				reads.names.insert(schema + "." + name);
			} else if (store && name == RESULTS_BY_FUNCTION.table) {
				reads.measurements.emplace(&op, std::set<string>());
			} else if (store && name == ANNOTATIONS_BY_PROPERTY.table) {
				reads.glossary.emplace(&op, std::set<string>());
			} else if (store && StringUtil::StartsWith(name, "glossql_")) {
				reads.names.insert("main." + name.substr(8));
			} else {
				reads.everything = true;
			}
		} else if (!ReadsNothing(string(get.function.GetDefinition()->name))) {
			reads.everything = true;
		}
	}
	if (op.type == LogicalOperatorType::LOGICAL_FILTER) {
		for (auto &expr : op.expressions) {
			KeyPredicates(context, op, ws, RESULTS_BY_FUNCTION, *expr, reads.measurements);
			KeyPredicates(context, op, ws, ANNOTATIONS_BY_PROPERTY, *expr, reads.glossary);
		}
	}
	for (auto &child : op.children) {
		CollectReads(context, *child, ws, reads, &op);
	}
}

//! The views a relation holds on: itself when it is a view, and the views its
//! query reads, through views over views — parts the plan does not name, since
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

//! What a bind read beside the plan (ClientContextState): a primitive whose
//! bind reads the catalog and the tables' shapes to write its SQL, or one
//! column's counts to refuse, names those inputs here, since the plan it
//! leaves cannot; the measurement's reads take them with the plan's, so the
//! row misses when one of them moves, as it does when a table the plan reads
//! moves
struct BindReads : public ClientContextState {
	std::set<string> names;

	static BindReads &Of(ClientContext &context) {
		return *context.registered_state->GetOrCreate<BindReads>("glossql_bind_reads");
	}
};

//! A bind names an input it read: a lake relation as dataset.table, a store
//! relation as main.name, the dataset's every relation as *
void BindRead(ClientContext &context, const string &name) {
	BindReads::Of(context).names.insert(name);
}

//! A read of one property's values: its annotations, and the other functions'
//! results of it, each within the reading result's closure
void BindProperty(ClientContext &context, const string &aspect) {
	BindRead(context, "main.glossary/" + aspect);
	BindRead(context, "main.measurements@" + aspect);
}

//! The reads of a call on a subject, enumerated from its bound plan: views
//! expand to their tables, subject_column() to the subject's, before any
//! optimizer touches it; the views the subject holds on are named beside,
//! their definitions being parts too, and so is what a bind read beside the plan
static string ReadsOf(Inner &inner, const string &call, const string &subject) {
	Reads reads;
	auto &context = *inner.con.context;
	auto &side = BindReads::Of(context);
	side.names.clear();
	unique_ptr<SQLStatement> statement = ParseSelect("SELECT * FROM " + call);
	context.RunFunctionInTransaction([&]() {
		Planner planner(context);
		planner.CreatePlan(std::move(statement));
		CollectReads(context, *planner.plan, inner.ws, reads);
		reads.Close();
		reads.names.insert(side.names.begin(), side.names.end());
		auto parts = StringUtil::Split(subject, '.');
		if (parts.size() >= 2 && subject.find(" -> ") == string::npos && subject.find(" <-> ") == string::npos) {
			std::set<string> views;
			ViewsOf(context, inner.ws, parts[0], parts[1], views);
			reads.names.insert(views.begin(), views.end());
		}
	});
	return reads.Text();
}

//! The closure of a subject (SPEC §6): the subjects whose glossary rows and
//! results a result on it can read. The dataset: everything in it. A view, or
//! a column of one: the view and its columns, its definition (the dataset's row
//! of the view's property), the tables and columns it scans and joins on and
//! the views it reads, each with its own closure, the metric a plan names
//! (x-plan-of), the results stored on the dataset, and the results stored on
//! each relationship between the tables it reads. A loaded table, or a
//! column of one: the tables the dataset's declared relationships connect it
//! to, itself among them, and their columns. Anything else (a relationship's
//! pair path): the dataset. `determinants` takes what the closure was drawn
//! from: the dataset's shape, and its relationships or its views' declarations
static shared_ptr<RunClosure> ClosureOf(Inner &inner, const string &subject, const string &dataset,
                                        std::set<string> &determinants) {
	auto closure = make_shared_ptr<RunClosure>();
	closure->dataset = dataset;
	auto parts = SubjectParts(subject);
	bool pair = subject.find(" -> ") != string::npos || subject.find(" <-> ") != string::npos;
	if (subject == dataset || pair || parts.size() < 2 || parts[0] != dataset) {
		closure->whole = true;
		return closure;
	}
	determinants.insert(string(LAKE_SHAPE) + "/" + dataset);
	auto ds = QuoteText(dataset);
	auto prefix = dataset + ".";
	// the dataset's relations and their columns, its views among them
	std::map<string, vector<string>> columns;
	std::set<string> views;
	auto rows = inner.Run("SELECT table_name, column_name, table_type = 'VIEW' FROM glossql_columns(" + ds + ")");
	while (auto chunk = rows->Fetch()) {
		for (idx_t r = 0; r < chunk->size(); r++) {
			auto relation = chunk->GetValue(0, r).ToString();
			columns[relation].push_back(chunk->GetValue(1, r).ToString());
			if (chunk->GetValue(2, r).GetValue<bool>()) {
				views.insert(relation);
			}
		}
	}
	auto add_relation = [&](const string &relation) {
		closure->subjects.insert(prefix + relation);
		for (auto &column : columns[relation]) {
			closure->subjects.insert(prefix + relation + "." + column);
		}
	};
	auto &table = parts[1];
	if (!views.count(table)) {
		// a loaded table: the tables its dataset's declared relationships connect it to
		determinants.insert("main.relationships");
		std::map<string, std::set<string>> edges;
		auto sides =
		    inner.Run("SELECT left_side, right_side FROM " + inner.Current("relationships") + " WHERE dataset = " + ds);
		while (auto chunk = sides->Fetch()) {
			for (idx_t r = 0; r < chunk->size(); r++) {
				auto left = SubjectParts(chunk->GetValue(0, r).ToString());
				auto right = SubjectParts(chunk->GetValue(1, r).ToString());
				if (left.size() == 3 && right.size() == 3) {
					edges[left[1]].insert(right[1]);
					edges[right[1]].insert(left[1]);
				}
			}
		}
		std::set<string> reached {table};
		vector<string> next {table};
		while (!next.empty()) {
			auto at = next.back();
			next.pop_back();
			for (auto &other : edges[at]) {
				if (reached.insert(other).second) {
					next.push_back(other);
				}
			}
		}
		for (auto &relation : reached) {
			add_relation(relation);
		}
		return closure;
	}
	// a view: what it scans and joins on, the views it reads, the metric a plan names
	determinants.insert("main.aspects");
	std::map<string, vector<string>> scanned;
	auto scans = inner.Run("SELECT view, table_name, columns FROM glossql_views_scans(" + ds + ")");
	while (auto chunk = scans->Fetch()) {
		for (idx_t r = 0; r < chunk->size(); r++) {
			auto relation = chunk->GetValue(1, r).ToString();
			auto &into = scanned[chunk->GetValue(0, r).ToString()];
			into.push_back(relation);
			auto read = chunk->GetValue(2, r);
			for (auto &column : ListValue::GetChildren(read)) {
				into.push_back(relation + "." + column.ToString());
			}
		}
	}
	auto joins = inner.Run("SELECT view, left_column, right_column FROM glossql_views_joins(" + ds + ")");
	while (auto chunk = joins->Fetch()) {
		for (idx_t r = 0; r < chunk->size(); r++) {
			auto &into = scanned[chunk->GetValue(0, r).ToString()];
			for (idx_t side = 1; side <= 2; side++) {
				auto column = SubjectParts(chunk->GetValue(side, r).ToString());
				if (column.size() == 3) {
					into.push_back(column[0] + "." + column[1]);
					into.push_back(column[0] + "." + column[1] + "." + column[2]);
				}
			}
		}
	}
	std::map<string, string> plans;
	auto planned =
	    inner.Run("SELECT name, json_extract_string(schema, '$.x-plan-of') FROM " + inner.Current("aspects") +
		          " WHERE kind = 'VIEW' AND schema::JSON->>'x-plan-of' IS NOT NULL");
	while (auto chunk = planned->Fetch()) {
		for (idx_t r = 0; r < chunk->size(); r++) {
			plans[chunk->GetValue(0, r).ToString()] = chunk->GetValue(1, r).ToString();
		}
	}
	// the relationships between the tables a view reads: their checks' results, one per relationship
	determinants.insert("main.relationships");
	vector<std::tuple<string, string, string>> relationships;
	auto declared = inner.Run("SELECT left_side, right_side, left_side || ' ' || op || ' ' || right_side FROM " +
	                          inner.Current("relationships") + " WHERE dataset = " + ds);
	while (auto chunk = declared->Fetch()) {
		for (idx_t r = 0; r < chunk->size(); r++) {
			auto left = SubjectParts(chunk->GetValue(0, r).ToString());
			auto right = SubjectParts(chunk->GetValue(1, r).ToString());
			if (left.size() == 3 && right.size() == 3) {
				relationships.emplace_back(prefix + left[1], prefix + right[1], chunk->GetValue(2, r).ToString());
			}
		}
	}
	std::map<string, std::set<string>> held;
	auto &context = *inner.con.context;
	context.RunFunctionInTransaction([&]() {
		for (auto &view : views) {
			ViewsOf(context, inner.ws, dataset, view, held[view]);
		}
	});
	std::set<string> visited;
	std::function<void(const string &)> add_view = [&](const string &view) {
		if (!visited.insert(view).second) {
			return;
		}
		add_relation(view);
		closure->properties.insert(std::make_pair(dataset, view));
		closure->results.insert(dataset);
		std::set<string> tables;
		for (auto &read : scanned[view]) {
			if (StringUtil::StartsWith(read, prefix)) {
				closure->subjects.insert(read);
				tables.insert(read);
			}
		}
		for (auto &relationship : relationships) {
			if (tables.count(std::get<0>(relationship)) && tables.count(std::get<1>(relationship))) {
				closure->results.insert(std::get<2>(relationship));
			}
		}
		for (auto &other : held[view]) {
			if (StringUtil::StartsWith(other, prefix) && views.count(other.substr(prefix.size()))) {
				add_view(other.substr(prefix.size()));
			}
		}
		if (plans.count(view) && views.count(plans[view])) {
			add_view(plans[view]);
		}
	};
	add_view(table);
	return closure;
}

//! What a computation read, each input at its version now (SPEC §6): the names
//! its reads resolve to, an input that is not there with no version. A part
//! the lake or the store versions as a whole is read from the parts; an input
//! that names rows of the closure is a digest of those rows, and the closure
//! itself is an input beside them. Read before the computation runs, so that a
//! change under it is a change after it. Fills the properties the run sees the
//! declarations of
static vector<pair<string, Value>> InputsOf(Inner &inner, const string &dataset, const string &function,
                                            const string &reads, RunClosure *closure) {
	vector<pair<string, Value>> inputs;
	std::set<string> names;
	for (auto &read : StringUtil::Split(reads, ',')) {
		auto name = InputName(read, dataset, function);
		if (!name.empty()) {
			names.insert(name);
		}
	}
	vector<string> parts;
	vector<string> scoped;
	for (auto &name : names) {
		(closure && ScopedInput(name) ? scoped : parts).push_back("(" + QuoteText(name) + ")");
	}
	if (!parts.empty()) {
		auto rows = inner.Run("SELECT i.name, p.version FROM (VALUES " + StringUtil::Join(parts, ", ") +
		                      ") i(name) LEFT JOIN (" + PartsSQL(inner.ws) + ") p ON p.name = i.name");
		while (auto chunk = rows->Fetch()) {
			for (idx_t r = 0; r < chunk->size(); r++) {
				inputs.emplace_back(chunk->GetValue(0, r).ToString(), chunk->GetValue(1, r));
			}
		}
	}
	if (!scoped.empty()) {
		auto members = JsonList(closure->Members());
		inputs.emplace_back(CLOSURE_INPUT, Value(members));
		auto rows = inner.Run("WITH members AS (" + MembersSQL("SELECT 0 AS id, " + QuoteText(members) + " AS list") +
		                      "), reads AS (SELECT 0 AS id, " + QuoteText(dataset) + " AS dataset, " +
		                      QuoteText(function) + " AS function, input FROM (VALUES " +
		                      StringUtil::Join(scoped, ", ") + ") t(input)), " + ScopedSQL(inner.ws) +
		                      " SELECT input, version FROM sc_versions UNION ALL SELECT DISTINCT aspect, NULL FROM"
		                      " sc_props WHERE aspect IS NOT NULL");
		while (auto chunk = rows->Fetch()) {
			for (idx_t r = 0; r < chunk->size(); r++) {
				auto version = chunk->GetValue(1, r);
				if (version.IsNull()) {
					closure->named.insert(chunk->GetValue(0, r).ToString());
				} else {
					inputs.emplace_back(chunk->GetValue(0, r).ToString(), version);
				}
			}
		}
	}
	std::sort(inputs.begin(), inputs.end(),
	          [](const pair<string, Value> &a, const pair<string, Value> &b) { return a.first < b.first; });
	return inputs;
}

//! A run's closure held on its connection while it plans and runs, and let go
//! after it, a failure among the ways out
struct RunWithin {
	explicit RunWithin(Inner &inner) : context(*inner.con.context) {
	}
	~RunWithin() {
		Release();
	}
	void Hold(shared_ptr<RunClosure> held) {
		closure = std::move(held);
		SetRunClosure(context, closure);
	}
	void Release() {
		SetRunClosure(context, nullptr);
	}
	ClientContext &context;
	shared_ptr<RunClosure> closure;
};

//! The stored row for the call while it is current, (value, error), else empty
static vector<Value> Served(Inner &inner, const string &dataset, const string &name, const string &subject) {
	return inner.Row("SELECT value, error FROM (" + CurrentResultsSQL(inner.ws) +
	                 ") WHERE current AND dataset = " + QuoteText(dataset) + " AND function = " + QuoteText(name) +
	                 " AND subject = " + QuoteText(subject));
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
	// the read functions a body reads the catalog through walk from the subject this run computes for
	ClientConfig::GetConfig(*inner.con.context).SetUserVariable(RUN_SUBJECT, Value(subject));
	idx_t row = 0;
	for (auto &call : ListValue::GetChildren(inputs[1])) {
		auto name = call.ToString();
		auto schema = inner.FunctionSchema(dataset, name);
		if (schema.empty()) {
			throw InvalidInputException("glossql: %s is not a declared function for %s", name, dataset);
		}
		auto scope = schema == "main" ? "GLOBAL" : schema;
		auto returns =
		    inner.Scalar("SELECT returns FROM " + inner.Store("functions") + " WHERE name = " + QuoteText(name) +
			             " AND scope = " + QuoteText(scope) + " AND end_snapshot IS NULL");
		if (returns.IsNull()) {
			throw InvalidInputException("glossql: %s is a detector: it runs at read, through TESTS()", name);
		}
		output.data[0].Append(Value(name));
		output.data[1].Append(Value(subject));
		output.data[2].Append(returns);
		// served while it is current: a repeat is a serve, not a rerun (SPEC §6). A stored failure is not
		// served to a call: it may come from what is no input (a service down, a configuration), so a
		// call computes again; it stays current for a read of what is due
		auto served = Served(inner, dataset, name, subject);
		if (!served.empty() && served[1].IsNull()) {
			output.data[3].Append(Value(Summary(inner, served[0].ToString())));
			output.data[4].Append(Value::BOOLEAN(false));
			row++;
			continue;
		}
		// the aspect's schema is the one contract: the output is validated against it (SPEC §6)
		auto contract = inner.Scalar("SELECT schema::VARCHAR FROM " + inner.Store("aspects") +
		                             " WHERE name = " + returns.ToSQLString() + " AND end_snapshot IS NULL");
		if (contract.IsNull()) {
			throw InvalidInputException("glossql: %s returns %s, which is not a declared aspect", name,
			                            returns.ToString());
		}
		auto invocation = inner.Table(schema, name) + "(" + QuoteText(subject) + ")";
		auto reads = ReadsOf(inner, invocation, subject);
		// a body that reads rows about subjects runs within its subject's closure: planned
		// again there, its reads and its run see the rows of the closure and no other
		RunWithin within(inner);
		std::set<string> determinants;
		for (auto &read : StringUtil::Split(reads, ',')) {
			if (!within.closure && ScopedInput(read)) {
				within.Hold(ClosureOf(inner, subject, dataset, determinants));
				reads = ReadsOf(inner, invocation, subject);
			}
		}
		for (auto &determinant : determinants) {
			reads += "," + determinant;
		}
		auto inputs = InputsOf(inner, dataset, name, reads, within.closure.get());
		// a computation that fails is a result too: stored with its error, which serves while it is current
		Value value;
		string error;
		try {
			value = ValueOf(inner, invocation);
			if (value.IsNull()) {
				value = Value("null");
			}
			auto breach = JsonSchemaCheck(contract.ToString(), value.ToString());
			if (!breach.empty()) {
				error = StringUtil::Format("glossql: the output of %s does not conform to the schema of %s: %s", name,
				                           returns.ToString(), breach);
			}
		} catch (InvalidInputException &e) {
			error = ErrorData(e).RawMessage();
		}
		within.Release();
		if (!error.empty()) {
			value = Value();
		}
		// stored with what it read, each input at its version: the next extraction serves it
		// while those are unchanged. The result it replaces ends, and its inputs with it
		inner.Write(context, [&](CatalogWrite &write) {
			auto key = "dataset = " + QuoteText(dataset) + " AND function = " + QuoteText(name) +
			           " AND subject = " + QuoteText(subject);
			auto replaced =
			    inner.Scalar("SELECT begin_snapshot FROM " + inner.Current("measurements") + " WHERE " + key);
			if (!replaced.IsNull()) {
				write.End("inputs", "begin_snapshot = " + replaced.ToString());
				write.End("measurements", key);
			}
			write.Insert("measurements", QuoteText(dataset) + ", " + QuoteText(name) + ", " + QuoteText(subject) +
			                                 ", " + returns.ToSQLString() + ", " +
			                                 (value.IsNull() ? string("NULL") : QuoteText(value.ToString())) +
			                                 ", now(), " + (error.empty() ? string("NULL") : QuoteText(error)));
			for (auto &input : inputs) {
				write.Insert("inputs", QuoteText(input.first) + ", " + input.second.ToSQLString());
			}
		});
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
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
		// a column is a subject, never a relation plain SQL reads: a call on one
		// that names no declared function is a misspelt function
		string resolved;
		try {
			resolved = ResolveSubject(context, input.inputs[0].ToString());
		} catch (std::exception &) {
		}
		if (SubjectParts(resolved).size() == 3) {
			vector<string> names;
			for (auto &call : ListValue::GetChildren(input.inputs[1])) {
				names.push_back(call.ToString() + "()");
			}
			throw InvalidInputException(
			    "glossql: %s is no declared function over %s; SELECT name FROM functions lists them",
			    StringUtil::Join(names, ", "), resolved);
		}
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
	return ref;
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
	auto parts = SubjectParts(subject);
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
// glossql_parts(): what a stored result can read, each at its version now —
// (name, version, kind, result). A part of the lake or the store is one row
// for every result; an input that names rows of a closure is one row per
// current result that read it, `result` its begin_snapshot and kind closure.
// Joined to `inputs` on the name, and the result where one is named, it says
// which result is out of date and which input moved
//===--------------------------------------------------------------------===//

static unique_ptr<TableRef> PartsBindReplace(ClientContext &context, TableFunctionBindInput &input) {
	Inner inner(context);
	return make_uniq<SubqueryRef>(
	    ParseSelect("WITH " + HeldSQL(inner.ws) + " SELECT name, version, kind, NULL::BIGINT AS result FROM (" +
		            PartsSQL(inner.ws) + ") UNION ALL SELECT input, version, 'closure', id FROM sc_versions"));
}

static TableFunction PartsFunction() {
	TableFunction function("glossql_parts", {}, nullptr);
	function.bind_replace = PartsBindReplace;
	return function;
}

//===--------------------------------------------------------------------===//
// glossql_views_definitions(dataset): the annotation that defines each view
// of a dataset, (view, body), the newest on the dataset. Read through the
// glossary's view by each view's property by name, so a result that read it
// names the properties of the views it read, and no other annotation
//===--------------------------------------------------------------------===//

static unique_ptr<TableRef> ViewsDefinitionsBindReplace(ClientContext &context, TableFunctionBindInput &input) {
	Inner inner(context);
	auto dataset = input.inputs[0].IsNull() ? string() : input.inputs[0].ToString();
	BindRead(context, "main.aspects");
	vector<string> views;
	auto rows = inner.Run("SELECT name FROM " + inner.Current("aspects") + " WHERE kind = 'VIEW' ORDER BY name");
	while (auto chunk = rows->Fetch()) {
		for (idx_t i = 0; i < chunk->size(); i++) {
			views.push_back(QuoteText(chunk->GetValue(0, i).ToString()));
		}
	}
	if (views.empty() || dataset.empty()) {
		return make_uniq<SubqueryRef>(ParseSelect("SELECT NULL::VARCHAR AS view, NULL::JSON AS body WHERE false"));
	}
	return make_uniq<SubqueryRef>(
	    ParseSelect("SELECT aspect AS view, body FROM " + QuoteName(inner.ws.lake) +
		            ".main.glossary WHERE dataset = " + QuoteText(dataset) + " AND subject = " + QuoteText(dataset) +
		            " AND aspect IN (" + StringUtil::Join(views, ", ") +
		            ") QUALIFY row_number() OVER (PARTITION BY aspect ORDER BY begin_snapshot DESC) = 1"));
}

static TableFunction ViewsDefinitionsFunction() {
	TableFunction function("glossql_views_definitions", {LogicalType::VARCHAR}, nullptr);
	function.bind_replace = ViewsDefinitionsBindReplace;
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
	// the relations are listed at bind: one stored or dropped is a change the plan cannot name
	BindRead(context, parts.size() == 1 ? "*" : parts[0] + "." + parts[1]);
	// a second argument true (or 'all') takes the dataset's views too — a metric definition
	// is one — each bound at bind and skipped when it does not, as a screen skips a
	// metric definition that fails; 'views' takes the views alone, so that a read over the
	// metric definitions — a metric's series, a plan's rows — binds no table
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
	// cells := n bounds what a screen reads: a table whose scalar columns times its
	// rows would pass the bound is read as a sample of its rows, the bound divided
	// by its width as the share of the rows kept, and rn numbers the rows read
	// (SPEC §6). A row is kept by the hash of its columns, so the sample is the
	// same rows every run under any thread count; DuckDB's seeded sampling is
	// repeatable on one thread only. Its size is the share of the rows, about
	// the bound, and rn says what it was
	int64_t cells = 0;
	auto bound = input.named_parameters.find("cells");
	if (bound != input.named_parameters.end() && !bound->second.IsNull()) {
		cells = bound->second.GetValue<int64_t>();
		if (cells <= 0) {
			throw InvalidInputException("glossql: dataset_cells takes a positive cells bound, got %d", cells);
		}
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
		if (cells > 0) {
			int64_t width = 0;
			for (auto &column : columns[table]) {
				width += IsScalarType(column.second) ? 1 : 0;
			}
			auto rows = std::max<int64_t>(1, cells / std::max<int64_t>(1, width));
			from = "(SELECT * FROM " + from + " WHERE (hash(" + names +
			       ") % 1000000)::BIGINT < (SELECT least(1000000.0, " + to_string(rows) +
			       " * 1000000.0 / greatest(count(*), 1)) FROM " + from + "))";
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
	tables.GetSignature().WithTypedKwargs("options",
	                                      [](TypedKwargs &options) { options.Add("cells", LogicalType::BIGINT); });
	set.AddFunction(tables);
	TableFunction with_views("dataset_cells", {LogicalType::VARCHAR, LogicalType::BOOLEAN}, nullptr);
	with_views.bind_replace = DatasetCellsBindReplace;
	with_views.GetSignature().WithTypedKwargs("options",
	                                          [](TypedKwargs &options) { options.Add("cells", LogicalType::BIGINT); });
	set.AddFunction(with_views);
	TableFunction which("dataset_cells", {LogicalType::VARCHAR, LogicalType::VARCHAR}, nullptr);
	which.bind_replace = DatasetCellsBindReplace;
	which.GetSignature().WithTypedKwargs("options",
	                                     [](TypedKwargs &options) { options.Add("cells", LogicalType::BIGINT); });
	set.AddFunction(which);
	return set;
}

//===--------------------------------------------------------------------===//
// glossql_relationship_checks(subject): whether the declared relationships
// hold, per edge over its own columns: a dataset's every one, or one
// relationship named by its pair path. The quality kit's relationship_checks
// wraps it. An edge's key columns are read from the two tables directly, typed and
// linear in their rows, where a screen over dataset_cells pivoted every cell
// of a table back into rows per edge. Per edge: the filled child rows (every
// key column present), the orphans (child keys no parent row carries; inside one
// table, child keys that reach more than one parent key on the same rows),
// and for an edge across two tables the temporal pairs: the first three date
// columns of each side, joined on the edge, how often the child's date
// precedes the parent's. Keys compare as text, as the cells did
//===--------------------------------------------------------------------===//

struct EdgeSide {
	string table;
	vector<string> key_columns;
	//! the first three date columns, in the table's order
	vector<string> dates;
};

//! [dataset.]table.column or [dataset.]table.(a, b), as the store spells a side
static EdgeSide ParseSide(ClientContext &context, const string &dataset, const string &side) {
	auto parts = SubjectParts(side);
	EdgeSide result;
	if (parts.size() != 3) {
		return result;
	}
	result.table = parts[1];
	auto &key = parts[2];
	if (key.size() > 2 && key.front() == '(' && key.back() == ')') {
		result.key_columns = StringUtil::Split(key.substr(1, key.size() - 2), ", ");
	} else {
		result.key_columns.push_back(key);
	}
	for (auto &column : DatasetColumns(context, dataset, result.table, false)) {
		if (result.dates.size() < 3 && (column.type == "DATE" || StringUtil::StartsWith(column.type, "TIMESTAMP"))) {
			result.dates.push_back(column.column);
		}
	}
	return result;
}

//! One edge's rows: (seq, relationship, filled, orphans, pair_seq, child_column, parent_column, joined, precedes)
static string EdgeSQL(Inner &inner, const string &dataset, idx_t seq, const string &relationship, const EdgeSide &child,
                      const EdgeSide &parent) {
	bool two = child.key_columns.size() == 2;
	auto keys = [&](const EdgeSide &s, const string &k1, const string &k2) {
		return QuoteName(s.key_columns[0]) + "::VARCHAR AS " + k1 + ", " +
		       (two ? QuoteName(s.key_columns[1]) + "::VARCHAR" : "NULL::VARCHAR") + " AS " + k2;
	};
	auto present = [&](const EdgeSide &s) {
		return QuoteName(s.key_columns[0]) + " IS NOT NULL" +
		       (two ? " AND " + QuoteName(s.key_columns[1]) + " IS NOT NULL" : "");
	};
	auto dates = [&](const EdgeSide &s, const string &prefix) {
		string out;
		for (idx_t i = 0; i < s.dates.size(); i++) {
			out += ", CAST(" + QuoteName(s.dates[i]) + " AS TIMESTAMP) AS " + prefix + to_string(i + 1);
		}
		return out;
	};
	string sql = "(WITH c AS (SELECT " + keys(child, "k1", "k2") + dates(child, "d") + " FROM " +
	             inner.Table(dataset, child.table) + " WHERE " + present(child) + "), p AS (SELECT " +
	             keys(parent, "k1", "k2") + dates(parent, "d") + " FROM " + inner.Table(dataset, parent.table) +
	             " WHERE " + present(parent) + ")";
	string join_orphans = "(SELECT count(*) FROM c WHERE NOT EXISTS (SELECT 1 FROM p WHERE p.k1 = c.k1 AND p.k2 IS NOT "
	                      "DISTINCT FROM c.k2))";
	string orphans = join_orphans;
	if (child.table == parent.table) {
		// a nest: the parent key columns do not key the table, and a child key is orphaned by
		// reaching more than one parent key on the rows it sits on
		sql += ", same_rows AS (SELECT " + keys(child, "k1", "k2") + ", " + keys(parent, "p1", "p2") + " FROM " +
		       inner.Table(dataset, child.table) + " WHERE " + present(child) + " AND " + present(parent) + ")";
		orphans = "CASE WHEN (SELECT count(DISTINCT (k1, k2)) <> count(*) FROM p) THEN (SELECT coalesce(sum(n) FILTER "
		          "(WHERE k > 1), 0)::BIGINT FROM (SELECT k1, k2, count(DISTINCT (p1, p2)) AS k, count(*) AS n FROM "
		          "same_rows "
		          "GROUP BY k1, k2)) ELSE " +
		          join_orphans + " END";
	}
	sql += ", e AS (SELECT (SELECT count(*) FROM c) AS filled, " + orphans + " AS orphans)";
	const string no_pairs = "SELECT NULL::BIGINT AS pair_seq, NULL::VARCHAR AS child_column, NULL::VARCHAR AS "
	                        "parent_column, NULL::BIGINT AS joined, NULL::BIGINT AS precedes WHERE false";
	if (child.table == parent.table || child.dates.empty() || parent.dates.empty()) {
		sql += ", t AS (" + no_pairs + ")";
	} else {
		// every date pair counted over one join, then one row per pair that joined
		string counts;
		string rows;
		idx_t pair_seq = 0;
		for (idx_t i = 0; i < child.dates.size(); i++) {
			for (idx_t j = 0; j < parent.dates.size(); j++) {
				pair_seq++;
				auto cd = "c.d" + to_string(i + 1);
				auto pd = "p.d" + to_string(j + 1);
				auto n = to_string(pair_seq);
				counts += (counts.empty() ? "" : ", ") + string("count(*) FILTER (WHERE ") + cd + " IS NOT NULL AND " +
				          pd + " IS NOT NULL) AS j" + n + ", count(*) FILTER (WHERE " + cd + " < " + pd + ") AS p" + n;
				rows += (rows.empty() ? "" : " UNION ALL ") + string("SELECT ") + n + "::BIGINT AS pair_seq, " +
				        QuoteText(child.dates[i]) + " AS child_column, " + QuoteText(parent.dates[j]) +
				        " AS parent_column, j" + n + " AS joined, p" + n + " AS precedes FROM pairs";
			}
		}
		sql += ", pairs AS MATERIALIZED (SELECT " + counts +
		       " FROM c JOIN p ON p.k1 = c.k1 AND p.k2 IS NOT DISTINCT FROM c.k2), t AS (" + rows + ")";
	}
	sql += " SELECT " + to_string(seq) + "::BIGINT AS seq, " + QuoteText(relationship) +
	       " AS relationship, e.filled, e.orphans, t.pair_seq, t.child_column, t.parent_column, t.joined, t.precedes "
	       "FROM e LEFT JOIN (SELECT * FROM t WHERE joined > 0) t ON true)";
	return sql;
}

static unique_ptr<TableRef> RelationshipChecksBindReplace(ClientContext &context, TableFunctionBindInput &input) {
	Inner inner(context);
	// a dataset's every declared relationship, or one relationship by its pair path
	auto subject = ResolveSubject(context, input.inputs[0].ToString());
	bool one = subject.find(" -> ") != string::npos || subject.find(" <-> ") != string::npos;
	auto dataset = one ? DatasetOf(subject) : subject;
	BindRead(context, "main.relationships");
	vector<string> edges;
	auto rows =
	    inner.Run("SELECT left_side, op, right_side FROM " + inner.Store("relationships") +
		          " WHERE dataset = " + QuoteText(dataset) + " AND end_snapshot IS NULL ORDER BY begin_snapshot");
	while (auto chunk = rows->Fetch()) {
		for (idx_t i = 0; i < chunk->size(); i++) {
			auto left = chunk->GetValue(0, i).ToString();
			auto op = chunk->GetValue(1, i).ToString();
			auto right = chunk->GetValue(2, i).ToString();
			if (one && left + " " + op + " " + right != subject) {
				continue;
			}
			auto child = ParseSide(context, dataset, left);
			auto parent = ParseSide(context, dataset, right);
			// key columns are one column or a pair, the same count on both sides
			if (child.key_columns.empty() || child.key_columns.size() != parent.key_columns.size() ||
			    child.key_columns.size() > 2) {
				continue;
			}
			edges.push_back(EdgeSQL(inner, dataset, edges.size(), left + " " + op + " " + right, child, parent));
		}
	}
	if (edges.empty()) {
		return make_uniq<SubqueryRef>(
		    ParseSelect("SELECT false AS applicable, 'no declared relationships' AS reason, NULL::BIGINT AS seq,"
			            " NULL::VARCHAR AS relationship, NULL::BIGINT AS filled, NULL::BIGINT AS orphans,"
			            " NULL::DOUBLE AS orphan_rate, NULL::BIGINT AS pair_seq, NULL::VARCHAR AS child_column,"
			            " NULL::VARCHAR AS parent_column, NULL::BIGINT AS joined, NULL::BIGINT AS precedes,"
			            " NULL::DOUBLE AS precedes_rate"));
	}
	string sql =
	    "SELECT true AS applicable, NULL::VARCHAR AS reason, seq, relationship, filled, orphans, CASE WHEN filled "
	    "= 0 THEN 0.0 ELSE orphans / filled::DOUBLE END AS orphan_rate, pair_seq, child_column, parent_column, "
	    "joined, precedes, CASE WHEN joined IS NULL THEN NULL ELSE precedes / joined::DOUBLE END AS "
	    "precedes_rate FROM (" +
	    StringUtil::Join(edges, " UNION ALL ") + ") ORDER BY seq, pair_seq";
	return make_uniq<SubqueryRef>(ParseSelect(sql));
}

static TableFunction RelationshipChecksFunction() {
	TableFunction function("glossql_relationship_checks", {LogicalType::VARCHAR}, nullptr);
	function.bind_replace = RelationshipChecksBindReplace;
	return function;
}

//===--------------------------------------------------------------------===//
// glossql_additivity_cells(column): the cells the stock/flow evidence reads; the
// quality kit's additivity_anchors is the screen over them. A column's own
// trajectory cannot decide stock from flow; the evidence is a movement an
// event table carries per entity and period, aligned with the column's table
// over the declared relationships. Per alignment, both sides as cells at the
// day: the measure side, entity and period to the column's sum, once per
// measure axis and alignment key; the event side, entity and period to each
// movement column's sum, the event table's numeric columns minus the key columns of
// declared edges, once per event axis. Beside them one row per numeric column
// of each event table, a declared key column flagged, an integer with its distinct
// share over its filled rows, so the screen sets the key-shaped ones aside.
// An alignment is a declared edge between the two tables, two edges meeting
// at one dimension key, or an entity borrowed one hop: a measure table whose
// every edge is stored on a document keys on the document's dimension, and an
// event table that aligns no other way keys through its master. A side's time
// axis is each of its own date columns, else one borrowed over an edge. A
// column that is not numeric, a table with no axis, or no alignment at all is
// one row that says why. What the bind reads beside the plan, the subject's
// table, the catalog's edges and roles and the tables the edges connect, it
// names as reads, so a measurement over it misses when one of them moves.
// Rows: (side, event, align, measure_align, axis, term, identifier, entity,
// period, value, reason)
//===--------------------------------------------------------------------===//

static bool IsIntegerType(const string &type) {
	static const std::set<string> INTEGERS = {"TINYINT",  "SMALLINT",  "INTEGER",  "BIGINT",  "HUGEINT",
	                                          "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT", "UHUGEINT"};
	return INTEGERS.count(type) > 0;
}

static bool IsNumericType(const string &type) {
	return IsIntegerType(type) || type == "FLOAT" || type == "DOUBLE" || StringUtil::StartsWith(type, "DECIMAL");
}

static bool IsTemporalType(const string &type) {
	return type == "DATE" || StringUtil::StartsWith(type, "TIMESTAMP");
}

//! One direction of a declared relationship: the child table and its key columns onto the parent's
struct Pointer {
	string src_t;
	vector<string> src_cols;
	string dst_t;
	vector<string> dst_cols;
};

//! A join one hop away: this table's key columns onto the hop table's key
struct Hop {
	bool set = false;
	string table;
	vector<string> fk;
	vector<string> key;
};

//! A side's time axis: its own date column, or one borrowed over an edge
struct Axis {
	string label;
	string column;
	Hop borrowed;
};

//! How the measure table and an event table key the same entity: the key columns on
//! each side, and the document each side borrows its entity through, if any
struct Align {
	vector<string> m_cols;
	vector<string> e_cols;
	Hop via;
	Hop e_via;
};

//! A table's columns with their types, by table
using Schemas = std::map<string, vector<pair<string, string>>>;

static string ColsLabel(const vector<string> &cols) {
	return cols.size() == 1 ? cols[0] : "(" + StringUtil::Join(cols, ", ") + ")";
}

//! The entity as text: one column cast, several joined by a bar
static string EntityExpr(const string &base, const vector<string> &cols) {
	vector<string> parts;
	for (auto &column : cols) {
		parts.push_back("CAST(" + base + "." + QuoteName(column) + " AS VARCHAR)");
	}
	return StringUtil::Join(parts, " || '|' || ");
}

static string NotNullGuard(const string &base, const vector<string> &cols) {
	vector<string> parts;
	for (auto &column : cols) {
		parts.push_back(base + "." + QuoteName(column) + " IS NOT NULL");
	}
	return StringUtil::Join(parts, " AND ");
}

static string JoinOn(const string &left, const vector<string> &lcols, const string &right,
                     const vector<string> &rcols) {
	vector<string> parts;
	for (idx_t i = 0; i < lcols.size() && i < rcols.size(); i++) {
		parts.push_back(left + "." + QuoteName(lcols[i]) + " = " + right + "." + QuoteName(rcols[i]));
	}
	return StringUtil::Join(parts, " AND ");
}

//! Whether the table owns an entity key: an edge onto a leaf, a table with no
//! edges of its own. A table that does never borrows one through a document
static bool OwnsEntity(const vector<Pointer> &pointers, const string &table) {
	std::set<string> documents;
	for (auto &p : pointers) {
		documents.insert(p.src_t);
	}
	for (auto &p : pointers) {
		if (p.src_t == table && p.dst_t != table && !documents.count(p.dst_t)) {
			return true;
		}
	}
	return false;
}

//! Every table's time axes: its own date columns; only a table with none
//! borrows one declared hop away
static std::map<string, vector<Axis>> TimeAxes(const vector<string> &tables, const Schemas &schemas,
                                               const vector<Pointer> &pointers) {
	std::map<string, vector<Axis>> all;
	for (auto &table : tables) {
		vector<Axis> axes;
		for (auto &column : schemas.at(table)) {
			if (IsTemporalType(column.second)) {
				axes.push_back(Axis {table + "." + column.first, column.first, Hop()});
			}
		}
		if (axes.empty()) {
			for (auto &p : pointers) {
				if (p.src_t != table || p.dst_t == table) {
					continue;
				}
				for (auto &column : schemas.at(p.dst_t)) {
					if (!IsTemporalType(column.second)) {
						continue;
					}
					auto label = p.dst_t + "." + column.first + " via " + ColsLabel(p.src_cols);
					bool seen = false;
					for (auto &axis : axes) {
						seen = seen || axis.label == label;
					}
					if (!seen) {
						axes.push_back(Axis {label, column.first, Hop {true, p.dst_t, p.src_cols, p.dst_cols}});
					}
				}
			}
		}
		all[table] = axes;
	}
	return all;
}

//! The entity alignments between the measure table and one event table: a
//! declared edge between the two, two edges meeting at the same dimension key,
//! then the two borrowed shapes, each under its own gate; deduplicated by what
//! they join on
static vector<Align> Alignments(const string &table, const string &ev, const vector<Pointer> &pointers,
                                bool owns_entity) {
	vector<Align> aligns;
	for (auto &p : pointers) {
		if (p.src_t == table && p.dst_t == ev) {
			aligns.push_back(Align {p.src_cols, p.dst_cols, Hop(), Hop()});
		}
		if (p.src_t == ev && p.dst_t == table) {
			aligns.push_back(Align {p.dst_cols, p.src_cols, Hop(), Hop()});
		}
	}
	for (auto &p : pointers) {
		if (p.src_t != table) {
			continue;
		}
		for (auto &q : pointers) {
			if (q.src_t == ev && q.dst_t == p.dst_t && q.dst_cols == p.dst_cols && p.dst_t != table && p.dst_t != ev) {
				aligns.push_back(Align {p.src_cols, q.src_cols, Hop(), Hop()});
			}
		}
	}
	// a borrowed entity, measure side: only for a measure table whose every edge
	// is stored on a document; it joins the document and keys on the document's
	// dimension column, which the event side carries on a direct edge of its own
	if (!owns_entity) {
		for (auto &p1 : pointers) {
			if (p1.src_t != table || p1.dst_t == table) {
				continue;
			}
			for (auto &p2 : pointers) {
				if (p2.src_t != p1.dst_t || p2.dst_t == table || p2.dst_t == ev) {
					continue;
				}
				for (auto &q : pointers) {
					if (q.src_t == ev && q.dst_t == p2.dst_t && q.dst_cols == p2.dst_cols) {
						aligns.push_back(
						    Align {p2.src_cols, q.src_cols, Hop {true, p1.dst_t, p1.src_cols, p1.dst_cols}, Hop()});
					}
				}
			}
		}
	}
	// a borrowed entity, event side: only when the event table aligned no other
	// way; a document-keyed event table joins its master and keys on the
	// master's columns, which meet the measure's dimension key or the measure
	// table itself
	if (aligns.empty()) {
		for (auto &q1 : pointers) {
			if (q1.src_t != ev || q1.dst_t == ev || q1.dst_t == table) {
				continue;
			}
			for (auto &q2 : pointers) {
				if (q2.src_t != q1.dst_t || q2.dst_t == ev) {
					continue;
				}
				Hop e_via {true, q1.dst_t, q1.src_cols, q1.dst_cols};
				if (q2.dst_t == table) {
					aligns.push_back(Align {q2.dst_cols, q2.src_cols, Hop(), e_via});
					continue;
				}
				for (auto &p : pointers) {
					if (p.src_t == table && p.dst_t == q2.dst_t && p.dst_cols == q2.dst_cols) {
						aligns.push_back(Align {p.src_cols, q2.src_cols, Hop(), e_via});
					}
				}
			}
		}
	}
	std::set<string> seen;
	vector<Align> deduped;
	for (auto &a : aligns) {
		auto key = (a.via.set ? a.via.table + ">" : "") + (a.e_via.set ? ">" + a.e_via.table : "") +
		           StringUtil::Join(a.m_cols, ",") + "=" + StringUtil::Join(a.e_cols, ",");
		if (seen.insert(key).second) {
			deduped.push_back(a);
		}
	}
	return deduped;
}

//! One side of an alignment as its cells read it: the FROM clause with the
//! time table and the document joined where borrowed, the time expression, the
//! entity as text and the guard that every key column and the time are present
struct SideSQL {
	string from;
	string texpr;
	string entity;
	string guard;
};

static SideSQL SideOf(Inner &inner, const string &dataset, const string &table, const Axis &axis, const Hop &hop,
                      const vector<string> &cols) {
	SideSQL side;
	side.from = inner.Table(dataset, table) + " s";
	if (axis.borrowed.set) {
		side.from += " JOIN " + inner.Table(dataset, axis.borrowed.table) + " tt ON " +
		             JoinOn("s", axis.borrowed.fk, "tt", axis.borrowed.key);
		side.texpr = "tt." + QuoteName(axis.column);
	} else {
		side.texpr = "s." + QuoteName(axis.column);
	}
	string base = "s";
	if (hop.set) {
		side.from += " JOIN " + inner.Table(dataset, hop.table) + " dd ON " + JoinOn("s", hop.fk, "dd", hop.key);
		base = "dd";
	}
	side.entity = EntityExpr(base, cols);
	side.guard = NotNullGuard(base, cols) + " AND " + side.texpr + " IS NOT NULL";
	return side;
}

//! One branch of the cells relation: its eleven columns as SQL expressions over a FROM clause
static string CellsBranch(const vector<string> &columns, const string &from) {
	static const vector<string> NAMES = {"side",       "event",  "align",  "measure_align", "axis",  "term",
	                                     "identifier", "entity", "period", "value",         "reason"};
	string sql = "SELECT ";
	for (idx_t i = 0; i < NAMES.size(); i++) {
		sql += (i == 0 ? "" : ", ") + columns[i] + " AS " + NAMES[i];
	}
	return sql + (from.empty() ? "" : " FROM " + from);
}

static string ReasonBranch(const string &reason) {
	return CellsBranch({"'reason'", "NULL::VARCHAR", "NULL::VARCHAR", "NULL::VARCHAR", "NULL::VARCHAR", "NULL::VARCHAR",
	                    "NULL::BOOLEAN", "NULL::VARCHAR", "NULL::TIMESTAMP", "NULL::DOUBLE", QuoteText(reason)},
	                   "");
}

//! An alignment's two labels: the measure side's key, which the measure cells
//! carry, and the whole alignment as the anchor names it
static pair<string, string> AlignLabels(const string &table, const string &ev, const Align &al) {
	auto m_align = (al.via.set ? al.via.table + ">" : "") + ColsLabel(al.m_cols);
	string align_label;
	if (al.via.set) {
		align_label =
		    table + " via " + al.via.table + "." + ColsLabel(al.m_cols) + " = " + ev + "." + ColsLabel(al.e_cols);
	} else if (al.e_via.set) {
		align_label =
		    table + "." + ColsLabel(al.m_cols) + " = " + ev + " via " + al.e_via.table + "." + ColsLabel(al.e_cols);
	} else {
		align_label = table + "." + ColsLabel(al.m_cols) + " = " + ev + "." + ColsLabel(al.e_cols);
	}
	return {m_align, align_label};
}

static unique_ptr<TableRef> AdditivityCellsBindReplace(ClientContext &context, TableFunctionBindInput &input) {
	Inner inner(context);
	auto subject = ResolveSubject(context, input.inputs[0].ToString());
	auto parts = SubjectParts(subject);
	if (parts.size() != 3) {
		throw InvalidInputException("glossql: glossql_additivity_cells takes dataset.table.column, got '%s'", subject);
	}
	auto &dataset = parts[0];
	auto &table = parts[1];
	auto &column = parts[2];
	BindRead(context, dataset + "." + table);
	auto reason = [](const string &text) {
		return make_uniq<SubqueryRef>(ParseSelect(ReasonBranch(text)));
	};
	// the dataset's loaded tables and their columns, by name
	vector<string> tables;
	Schemas schemas;
	for (auto &c : DatasetColumns(context, dataset, string(), false)) {
		if (schemas.find(c.table) == schemas.end()) {
			tables.push_back(c.table);
		}
		schemas[c.table].emplace_back(c.column, c.type);
	}
	std::sort(tables.begin(), tables.end());
	string type;
	if (schemas.count(table)) {
		for (auto &c : schemas[table]) {
			if (c.first == column) {
				type = c.second;
			}
		}
	}
	if (type.empty()) {
		return reason("no column " + subject + " on a loaded table: the evidence reads a table's column");
	}
	if (!IsNumericType(type)) {
		return reason(subject + " is " + type + ", not a number: the evidence reads a numeric measure");
	}
	// the dataset's declared edges as pointers, both ways for <->, over tables it has
	vector<Pointer> pointers;
	BindRead(context, "main.relationships");
	auto rows =
	    inner.Run("SELECT left_side, op, right_side FROM " + inner.Store("relationships") +
		          " WHERE dataset = " + QuoteText(dataset) + " AND end_snapshot IS NULL ORDER BY begin_snapshot");
	while (auto chunk = rows->Fetch()) {
		for (idx_t i = 0; i < chunk->size(); i++) {
			auto child = ParseSide(context, dataset, chunk->GetValue(0, i).ToString());
			auto parent = ParseSide(context, dataset, chunk->GetValue(2, i).ToString());
			if (child.key_columns.empty() || child.key_columns.size() != parent.key_columns.size() ||
			    !schemas.count(child.table) || !schemas.count(parent.table)) {
				continue;
			}
			pointers.push_back(Pointer {child.table, child.key_columns, parent.table, parent.key_columns});
			if (chunk->GetValue(1, i).ToString() == "<->") {
				pointers.push_back(Pointer {parent.table, parent.key_columns, child.table, child.key_columns});
			}
		}
	}
	std::set<pair<string, string>> key_columns;
	for (auto &p : pointers) {
		BindRead(context, dataset + "." + p.src_t);
		BindRead(context, dataset + "." + p.dst_t);
		for (auto &c : p.src_cols) {
			key_columns.insert({p.src_t, c});
		}
		for (auto &c : p.dst_cols) {
			key_columns.insert({p.dst_t, c});
		}
	}
	// the measure column screened as the event side's columns are: a declared key column
	// is an identifier, no measure; so is an integer distinct on nine filled rows
	// in ten, read exactly here, the one read of data at bind, since it spares
	// every cell the screen would otherwise build before refusing
	if (key_columns.count({table, column})) {
		return reason(subject + " is a key column of a declared relationship: an identifier, no measure");
	}
	auto axes = TimeAxes(tables, schemas, pointers);
	auto &m_axes = axes[table];
	if (m_axes.empty()) {
		return reason("no period axis: no date column on " + table + " or one declared hop away");
	}
	if (IsIntegerType(type)) {
		auto q = QuoteName(column);
		auto shape =
		    inner.Row("SELECT count(" + q + "), count(DISTINCT " + q + ") FROM " + inner.Table(dataset, table));
		auto filled = shape.empty() ? 0 : shape[0].GetValue<int64_t>();
		auto distinct = shape.empty() ? 0 : shape[1].GetValue<int64_t>();
		if (filled > 0 && distinct >= 2 && distinct * 10 >= filled * 9) {
			return reason(subject + " is key-shaped: " + to_string(distinct) + " distinct values in " +
			              to_string(filled) + " filled rows, an identifier, no measure; gloss role key");
		}
	}
	// the movement pool is what the catalog says is a measure: the columns whose
	// role claim holds, current or stale, read collapsed. Nothing is guessed from
	// a type; an event table with no measure glossed abstains and names the gloss
	std::map<string, std::set<string>> measures;
	std::set<string> roled;
	BindProperty(context, "role");
	auto roles = inner.Run("SELECT subject, value->>'value' FROM glossql_glossary(" + QuoteText(dataset) +
	                       ", 'role', false) WHERE state IN ('current', 'stale')");
	while (auto chunk = roles->Fetch()) {
		for (idx_t i = 0; i < chunk->size(); i++) {
			auto claimed = SubjectParts(chunk->GetValue(0, i).ToString());
			if (claimed.size() != 3) {
				continue;
			}
			roled.insert(claimed[1]);
			auto role = chunk->GetValue(1, i);
			if (!role.IsNull() && role.ToString() == "measure") {
				measures[claimed[1]].insert(claimed[2]);
			}
		}
	}
	bool owns = OwnsEntity(pointers, table);
	vector<string> branches;
	std::set<string> measured;
	for (auto &ev : tables) {
		if (ev == table) {
			continue;
		}
		auto aligns = Alignments(table, ev, pointers, owns);
		auto &e_axes = axes[ev];
		if (aligns.empty() || e_axes.empty()) {
			continue;
		}
		// the movement pool: the event table's numeric columns glossed a measure that are no declared key column
		vector<pair<string, string>> numeric;
		vector<string> pool;
		idx_t candidates = 0;
		for (auto &c : schemas[ev]) {
			if (!IsNumericType(c.second)) {
				continue;
			}
			numeric.push_back(c);
			if (key_columns.count({ev, c.first})) {
				continue;
			}
			candidates++;
			if (measures[ev].count(c.first)) {
				pool.push_back(c.first);
			}
		}
		if (candidates == 0) {
			// nothing numeric beyond its key columns: no movement to name, nothing required
			continue;
		}
		if (pool.empty()) {
			// every anchor on this event table abstains, one row per alignment and axis pair
			string why = roled.count(ev)
			                 ? "no measure among " + ev + "'s roles: gloss role measure on its movement columns"
			                 : "no role glossed on " + ev + ": gloss role on its columns";
			for (auto &al : aligns) {
				auto labels = AlignLabels(table, ev, al);
				for (auto &eaxis : e_axes) {
					for (auto &maxis : m_axes) {
						branches.push_back(CellsBranch({"'unglossed'", QuoteText(ev), QuoteText(labels.second),
						                                QuoteText(labels.first), QuoteText(eaxis.label),
						                                QuoteText(maxis.label), "NULL::BOOLEAN", "NULL::VARCHAR",
						                                "NULL::TIMESTAMP", "NULL::DOUBLE", QuoteText(why)},
						                               ""));
					}
				}
			}
			continue;
		}
		bool emitted = false;
		for (auto &al : aligns) {
			vector<string> terms;
			for (auto &c : pool) {
				if (std::find(al.e_cols.begin(), al.e_cols.end(), c) == al.e_cols.end()) {
					terms.push_back(c);
				}
			}
			if (terms.empty()) {
				continue;
			}
			emitted = true;
			auto labels = AlignLabels(table, ev, al);
			auto &m_align = labels.first;
			auto &align_label = labels.second;
			// the measure side, once per axis and alignment key
			for (auto &maxis : m_axes) {
				if (!measured.insert(maxis.label + "|" + m_align).second) {
					continue;
				}
				auto side = SideOf(inner, dataset, table, maxis, al.via, al.m_cols);
				auto from = "(SELECT " + side.entity + " AS __gloss_entity, date_trunc('day', CAST(" + side.texpr +
				            " AS TIMESTAMP)) AS __gloss_period, SUM(CAST(s." + QuoteName(column) +
				            " AS DOUBLE)) AS __gloss_value FROM " + side.from + " WHERE " + side.guard + " AND s." +
				            QuoteName(column) + " IS NOT NULL GROUP BY 1, 2)";
				branches.push_back(CellsBranch({"'measure'", "NULL::VARCHAR", QuoteText(m_align), "NULL::VARCHAR",
				                                QuoteText(maxis.label), "NULL::VARCHAR", "NULL::BOOLEAN",
				                                "__gloss_entity", "__gloss_period", "__gloss_value", "NULL::VARCHAR"},
				                               from));
			}
			// the entities the measure side can align: those with four days or more on
			// any measure axis, a superset of the viable at every grain. The event side
			// is summed for them alone, so an event table far wider and longer than the
			// measure's costs what the alignment needs and nothing more
			string viable;
			for (auto &maxis : m_axes) {
				auto side = SideOf(inner, dataset, table, maxis, al.via, al.m_cols);
				viable += (viable.empty() ? "" : " UNION ") + string("SELECT ") + side.entity + " AS __gloss_e FROM " +
				          side.from + " WHERE " + side.guard + " AND s." + QuoteName(column) +
				          " IS NOT NULL GROUP BY 1 HAVING count(DISTINCT date_trunc('day', CAST(" + side.texpr +
				          " AS TIMESTAMP))) >= 4";
			}
			// the event side, once per event axis: every term's sum per entity and day,
			// unpivoted, beside one alignment row that holds whether or not any cell does
			string sums;
			string names;
			for (auto &t : terms) {
				sums += ", SUM(CAST(s." + QuoteName(t) + " AS DOUBLE)) AS " + QuoteName(t);
				names += (names.empty() ? "" : ", ") + QuoteName(t);
			}
			for (auto &eaxis : e_axes) {
				branches.push_back(
				    CellsBranch({"'alignment'", QuoteText(ev), QuoteText(align_label), QuoteText(m_align),
					             QuoteText(eaxis.label), "NULL::VARCHAR", "NULL::BOOLEAN", "NULL::VARCHAR",
					             "NULL::TIMESTAMP", "NULL::DOUBLE", "NULL::VARCHAR"},
					            ""));
				auto side = SideOf(inner, dataset, ev, eaxis, al.e_via, al.e_cols);
				auto from = "(FROM (SELECT " + side.entity + " AS __gloss_entity, date_trunc('day', CAST(" +
				            side.texpr + " AS TIMESTAMP)) AS __gloss_period" + sums + " FROM " + side.from + " WHERE " +
				            side.guard + " AND " + side.entity + " IN (SELECT __gloss_e FROM (" + viable +
				            ")) GROUP BY 1, 2) UNPIVOT (__gloss_value FOR __gloss_term IN (" + names + "))) u";
				branches.push_back(
				    CellsBranch({"'event'", QuoteText(ev), QuoteText(align_label), QuoteText(m_align),
					             QuoteText(eaxis.label), "u.__gloss_term", "NULL::BOOLEAN", "u.__gloss_entity",
					             "u.__gloss_period", "u.__gloss_value", "NULL::VARCHAR"},
					            from));
			}
		}
		if (!emitted) {
			continue;
		}
		// the event table's numeric columns: a key column flagged, an integer with its distinct share
		string aggs;
		string names;
		string flags;
		for (auto &c : numeric) {
			auto q = QuoteName(c.first);
			bool is_key = key_columns.count({ev, c.first}) > 0;
			string agg = "NULL::DOUBLE";
			if (!is_key && IsIntegerType(c.second)) {
				agg = "CASE WHEN count(" + q + ") > 0 AND approx_count_distinct(" + q +
				      ") >= 2 THEN approx_count_distinct(" + q + ")::DOUBLE / count(" + q + ") END";
			}
			aggs += (aggs.empty() ? "" : ", ") + agg + " AS " + q;
			names += (names.empty() ? "" : ", ") + q;
			flags += (flags.empty() ? "" : ", ") + string("(") + QuoteText(c.first) + ", " +
			         (is_key ? "true" : "false") + ")";
		}
		auto from = "(FROM (SELECT " + aggs + " FROM " + inner.Table(dataset, ev) +
		            ") UNPIVOT INCLUDE NULLS (__gloss_value FOR __gloss_term IN (" + names + "))) u JOIN (VALUES " +
		            flags + ") v(__gloss_term, __gloss_identifier) USING (__gloss_term)";
		branches.push_back(CellsBranch({"'column'", QuoteText(ev), "NULL::VARCHAR", "NULL::VARCHAR", "NULL::VARCHAR",
		                                "u.__gloss_term", "v.__gloss_identifier", "NULL::VARCHAR", "NULL::TIMESTAMP",
		                                "u.__gloss_value", "NULL::VARCHAR"},
		                               from));
	}
	if (branches.empty()) {
		return reason("no declared relationships connect " + table + " to an event table with a time axis");
	}
	return make_uniq<SubqueryRef>(ParseSelect(StringUtil::Join(branches, " UNION ALL ")));
}

static TableFunction AdditivityCellsFunction() {
	TableFunction function("glossql_additivity_cells", {LogicalType::VARCHAR}, nullptr);
	function.bind_replace = AdditivityCellsBindReplace;
	return function;
}

//===--------------------------------------------------------------------===//
// Registration
//===--------------------------------------------------------------------===//

//===--------------------------------------------------------------------===//
// glossql_kit(path): a kit is a script of declarations — aspects, functions,
// tests — and the SQL beside them, read from anything DuckDB's file system
// opens (a local path, or a URL once httpfs is loaded), run statement by
// statement on a second connection under the glossql grammar, one row per
// statement. Each statement commits on its own, as it does from a script; a
// failing statement stops the load, the ones before it stay and no kits row
// is stored, so the next load runs the kit again. A kit that ran writes a kits row
// with the hash of its text. Run again with that text, as the opener does at
// every start-up, the kit holds: what it declared and created is in the lake,
// so one row says so and only its LOAD and INSTALL lines run, which bring an
// extension into the process. A changed text runs whole and writes a new row
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
	return result;
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

//! The hash of a kit's text, as its kits row records it
static string ScriptHash(const string &script) {
	MD5Context md5;
	md5.Add(script);
	char digest[MD5Context::MD5_HASH_LENGTH_TEXT + 1];
	md5.FinishHex(digest);
	return string(digest, MD5Context::MD5_HASH_LENGTH_TEXT);
}

static void KitExec(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<KitState>();
	if (!state.ran) {
		state.ran = true;
		auto path = data.bind_data->Cast<RunnerBindData>().inputs[0].ToString();
		auto script = ReadScript(context, path);
		auto name = KitName(path);
		auto hash = ScriptHash(script);
		Inner inner(context);
		inner.Run("SET active_grammar_extensions = ['glossql']");
		// every declaration the kit makes names it as its author
		inner.Run("SET VARIABLE glossql_declared_by = " + QuoteText("kit:" + name));
		auto newest = inner.Scalar("SELECT hash FROM " + inner.Current("kits") + " WHERE name = " + QuoteText(name));
		bool current = !newest.IsNull() && newest.ToString() == hash;
		auto statements = inner.con.ExtractStatements(script);
		for (auto &statement : statements) {
			auto head = FirstLine(statement->query);
			if (current && statement->type != StatementType::LOAD_STATEMENT) {
				continue;
			}
			auto result = inner.con.Query(std::move(statement));
			if (result->HasError()) {
				throw InvalidInputException("glossql: kit %s stopped at statement %d (%s): %s", name,
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
		if (current) {
			state.rows.emplace_back(name, "current");
		} else {
			inner.Write(context, [&](CatalogWrite &write) {
				write.End("kits", "name = " + QuoteText(name));
				write.Insert("kits", QuoteText(name) + ", " + QuoteText(path) + ", " + to_string(state.rows.size()) +
				                         ", now(), " + QuoteText(hash));
			});
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
	loader.RegisterFunction(GlossPreviewFunction());
	loader.RegisterFunction(GlossFunctionFunction());
	loader.RegisterFunction(GlossTestFunction());
	loader.RegisterFunction(GlossExtractFunction());
	loader.RegisterFunction(GlossSelectOrExtractFunction());
	loader.RegisterFunction(SubjectColumnFunction());
	loader.RegisterFunction(PartsFunction());
	loader.RegisterFunction(DatasetCellsFunctions());
	loader.RegisterFunction(RelationshipChecksFunction());
	loader.RegisterFunction(ViewsDefinitionsFunction());
	loader.RegisterFunction(AdditivityCellsFunction());
}

} // namespace duckdb
