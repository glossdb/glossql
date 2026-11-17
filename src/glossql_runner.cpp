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
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/peg/compiled_grammar.hpp"
#include "duckdb/parser/common_table_expression_info.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/parameter_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/set_operation_node.hpp"
#include "duckdb/parser/query_node/recursive_cte_node.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/parsed_data/create_info.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/expressionlistref.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/parser/tableref/pivotref.hpp"
#include "duckdb/parser/tableref/showref.hpp"
#include "duckdb/catalog/catalog_entry/macro_catalog_entry.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/function/scalar_macro_function.hpp"
#include "duckdb/function/table_macro_function.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_search_path.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/storage/object_cache.hpp"
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
//! A lake's metadata catalog as found, kept per instance while that catalog stays attached: every
//! connection a read opens finds it, and the lookup reads the information schema of every catalog
struct FoundWorkspace : public ObjectCacheEntry {
	Workspace ws;
	weak_ptr<AttachedDatabase> meta;

	static string ObjectType() {
		return "glossql_workspace";
	}
	string GetObjectType() override {
		return ObjectType();
	}
	optional_idx GetEstimatedCacheMemory() const override {
		return optional_idx(0);
	}
};

static Workspace LookUpWorkspace(ClientContext &context, Connection &con);

static Workspace FindWorkspace(ClientContext &context, Connection &con) {
	auto lake = LakeName(context);
	auto &cache = ObjectCache::GetObjectCache(context);
	auto key = "glossql_workspace|" + lake;
	auto &databases = DatabaseManager::Get(context);
	auto found = cache.Get<FoundWorkspace>(key);
	if (found) {
		auto meta = found->meta.lock();
		if (meta && meta == databases.GetDatabase(Identifier(found->ws.meta))) {
			return found->ws;
		}
	}
	auto ws = LookUpWorkspace(context, con);
	auto fill = make_shared_ptr<FoundWorkspace>();
	fill->ws = ws;
	fill->meta = databases.GetDatabase(Identifier(ws.meta));
	if (fill->meta.lock()) {
		cache.Put(key, fill);
	}
	return ws;
}

static Workspace LookUpWorkspace(ClientContext &context, Connection &con) {
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
	bool qualified = std::all_of(paths.begin(), paths.end(), [](const CatalogSearchEntry &path) {
		return !path.GetCatalog().empty() && !path.GetSchema().empty();
	});
	if (!paths.empty() && qualified) {
		// the caller's paths, set as they stand: each names its catalog and schema, which the caller's own
		// SET verified
		ClientData::Get(*con.context).catalog_search_path->Set(paths, CatalogSetPathType::SET_DIRECTLY);
	} else if (!paths.empty()) {
		Run("SET search_path = " + QuoteText(CatalogSearchEntry::ListToString(paths)));
	}
	Value files;
	if (context.TryGetCurrentSetting(Identifier("file_search_path"), files) && !files.IsNull() &&
	    !files.ToString().empty()) {
		Run("SET file_search_path = " + QuoteText(files.ToString()));
	}
	// a read the second connection runs reads the same lake, where several are attached
	ClientConfig::GetConfig(*con.context).SetUserVariable("glossql_workspace", Value(ws.lake));
	// and the read functions walk from the caller's run's subject
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
		inner.catalog_failed = true;
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
// is past the one the write read and the failure is a conflict: a statement of
// the catalog write failed (Postgres raises its unique or serialization error, of
// whatever class the scanner gives it, once the other writer has committed), or
// the error says it is one, whatever class it arrives in. Any other failure is the
// write's own and is refused as it is, on the first try.
bool Inner::Overtaken(const ErrorData &error, int64_t head, bool taking_snapshot) {
	if (error.Type() == ExceptionType::TRANSACTION || (taking_snapshot && error.Type() == ExceptionType::CONSTRAINT)) {
		return true;
	}
	if (head < 0 || !(catalog_failed || StringUtil::Contains(StringUtil::Lower(error.RawMessage()), "conflict"))) {
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
		catalog_failed = false;
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
		} catch (...) {
			con.Query("ROLLBACK");
			throw;
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
		switch (result->GetErrorObject().Type()) {
		// a conflict, an interrupt and the engine's own failures keep their class: a write tells a lost race
		// by it, and a computation stores no result for them
		case ExceptionType::TRANSACTION:
		case ExceptionType::INTERRUPT:
		case ExceptionType::FATAL:
		case ExceptionType::INTERNAL:
		case ExceptionType::OUT_OF_MEMORY:
			result->ThrowError();
			break;
		default:
			break;
		}
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
//! or merge, the key being the recipe's), the rows dropped on the way
//! (SPEC §3), NULL where the host cannot count them, the rows the recipe's
//! result carried, the rows a merge inserted or updated, and of a whole
//! replace of a large table its fingerprint and whether it holds the same
//! rows as the table before
struct Loaded {
	int64_t rows;
	int64_t source_rows = 0;
	Value changed;
	Value fingerprint;
	Value identical;
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
//! the recipe's to say, so the result is never deduplicated here. Returns
//! the keys the result carries, which are its rows
static int64_t KeyOnce(Inner &inner, const vector<string> &key, const string &sql) {
	vector<string> columns;
	vector<string> texts;
	for (auto &column : key) {
		columns.push_back(QuoteName(column));
		texts.push_back(QuoteName(column) + "::VARCHAR");
	}
	auto spelled = StringUtil::Join(columns, ", ");
	auto example = key.size() == 1 ? texts[0] : "'(' || concat_ws(', ', " + StringUtil::Join(texts, ", ") + ") || ')'";
	auto twice = inner.Row("SELECT count(*) FILTER (WHERE n > 1), min(" + example +
	                       ") FILTER (WHERE n > 1), count(*) FROM (SELECT " + spelled + ", count(*) AS n FROM (" + sql +
	                       ") GROUP BY ALL)");
	auto keys = twice[0].GetValue<int64_t>();
	if (keys > 0) {
		throw InvalidInputException("glossql: the recipe's result carries key %s more than once: %d keys, %s among "
		                            "them; a keyed recipe's result carries each key once (SPEC §3)",
		                            StringUtil::Join(key, ", "), keys, twice[1].ToString());
	}
	return twice[2].GetValue<int64_t>();
}

//! The rows from which a whole replace is fingerprinted: the goals kit's
//! load_modes() names a replace that changed nothing from this size on
static constexpr int64_t FINGERPRINT_ROWS = 100000;

//! Runs the recipe at its source and writes the result
static Loaded LoadTable(ClientContext &context, Inner &inner, const string &dataset, const string &table,
                        const RecipeRow &recipe, bool replace) {
	auto target = inner.Table(dataset, table);
	AtSource(inner, LookupSource(inner, recipe.source));
	Loaded loaded;
	auto key = recipe.Key();
	int64_t keys = 0;
	if (!key.empty()) {
		keys = KeyOnce(inner, key, recipe.sql);
	}
	if (replace || key.empty()) {
		// a recipe that returns the table's columns again replaces its rows and leaves its definition as it is
		auto shape = [&](const string &relation) {
			auto result = inner.Run("SELECT * FROM " + relation + " LIMIT 0");
			string text;
			for (idx_t i = 0; i < result->ColumnCount(); i++) {
				text += result->ColumnName(i).GetIdentifierName() + " " + result->GetTypes()[i].ToString() + ",";
			}
			return text;
		};
		bool held = false;
		try {
			held = shape(target) == shape("(" + recipe.sql + ")");
		} catch (std::exception &) {
		}
		if (held) {
			inner.Run("BEGIN");
			try {
				inner.Run("DELETE FROM " + target);
				inner.Run("INSERT INTO " + target + " " + recipe.sql);
				inner.Run("COMMIT");
			} catch (...) {
				inner.con.Query("ROLLBACK");
				throw;
			}
		} else {
			inner.Run("CREATE OR REPLACE TABLE " + target + " AS " + recipe.sql);
		}
		loaded.mode = "replace";
		loaded.how = "replaced";
		loaded.source_rows = inner.Scalar("SELECT count(*) FROM " + target).GetValue<int64_t>();
		// a large table's fingerprint: the sum of its rows' hashes, one scan of the table written, compared
		// with the load before's. Equal rows give equal sums; two different tables that sum alike pass as
		// identical, which hides a suggestion and changes no data
		if (loaded.source_rows >= FINGERPRINT_ROWS) {
			loaded.fingerprint = inner.Scalar("SELECT sum(hash(*COLUMNS(*))::HUGEINT)::VARCHAR FROM " + target);
			auto before = inner.Row("SELECT rows, fingerprint FROM " + inner.Store("imports") +
			                        " WHERE dataset = " + QuoteText(dataset) + " AND table_name = " + QuoteText(table) +
			                        " AND end_snapshot IS NULL");
			if (!before.empty() && !before[1].IsNull()) {
				loaded.identical = Value::BOOLEAN(before[0].GetValue<int64_t>() == loaded.source_rows &&
				                                  before[1].ToString() == loaded.fingerprint.ToString());
			}
		}
	} else {
		vector<string> on;
		for (auto &column : key) {
			on.push_back("t." + QuoteName(column) + " = s." + QuoteName(column));
		}
		// a matched row is updated only where a value it would write differs from the one it holds:
		// a load of the same rows writes nothing, and the table's version stays. The update writes
		// the recipe's columns by position, cast to the table's types, and is compared so
		auto ours = inner.Run("SELECT * FROM " + target + " LIMIT 0");
		auto theirs = inner.Run("SELECT * FROM (" + recipe.sql + ") LIMIT 0");
		string differs;
		if (ours->ColumnCount() == theirs->ColumnCount()) {
			vector<string> columns;
			for (idx_t i = 0; i < ours->ColumnCount(); i++) {
				columns.push_back("t." + QuoteName(ours->ColumnName(i).GetIdentifierName()) +
				                  " IS DISTINCT FROM CAST(s." + QuoteName(theirs->ColumnName(i).GetIdentifierName()) +
				                  " AS " + ours->GetTypes()[i].ToString() + ")");
			}
			differs = " AND (" + StringUtil::Join(columns, " OR ") + ")";
		}
		loaded.changed = inner.Scalar("MERGE INTO " + target + " AS t USING (" + recipe.sql + ") AS s ON " +
		                              StringUtil::Join(on, " AND ") + " WHEN MATCHED" + differs +
		                              " THEN UPDATE WHEN NOT MATCHED THEN INSERT");
		loaded.source_rows = keys;
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
		                            QuoteText(lineage) + ", " + loaded.snapshot.ToSQLString() + ", now(), " +
		                            to_string(loaded.source_rows) + ", " + loaded.changed.ToSQLString() + ", " +
		                            loaded.fingerprint.ToSQLString() + ", " + loaded.identical.ToSQLString());
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

	auto taken = KindNameTaken(inner, dataset, name);
	if (!taken.empty()) {
		throw InvalidInputException(
		    "glossql: asset_name_taken: %s is an asset a kit declares; load the table under another name", taken);
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

//===--------------------------------------------------------------------===//
// What a body reads (SPEC §6), taken from its text as written: DuckDB's parse
// of it, every node of the tree, a CTE's name subtracted inside the query that
// defines it, the macros it calls followed through their definitions as the
// catalog resolves them. One JSON object per read: kind `read` a call of a read
// function (annotations, results, definition), its what and its where as
// written, `constant` false where one of them is not a constant; `data` a table
// or view of the lake named in FROM, by its canonical dataset.table, a name
// without a dataset kept as written and taken in the subject's dataset, a table
// the lake does not hold yet by its name, or a primitive over the data of the
// subject; `macro` a macro followed, by its canonical name, whose declaration
// a result of the function reads; `catalog` the catalog reached otherwise;
// `other` a table function that reads nothing (range, unnest); `untracked` what
// the engine cannot follow: a body that does not parse, a table outside the
// lake, a table function that reads outside it, a function the catalog does not
// hold, a macro past the depth the walk follows. A declaration whose body makes
// a read no input can follow is refused (UntrackedRead). A primitive that reads the catalog at its bind, out of the
// body's text, is stated here by what it reads: glossql_additivity_cells reads
// the dataset's data, its roles and its relationships (the lineage's
// `relates`). A detector's body reads the annotations its test hands it, a
// relation by that name
//===--------------------------------------------------------------------===//

//! One read of a body, as functions.reads stores it; an empty what or where is null
struct BodyRead {
	string kind;
	string read;
	string what;
	string where;
	//! -1 null, 0 false, 1 true
	int constant = -1;
	string via;

	bool operator<(const BodyRead &other) const {
		return std::tie(kind, read, what, where, via, constant) <
		       std::tie(other.kind, other.read, other.what, other.where, other.via, other.constant);
	}
};

//! The relations of the catalog by name: a table named so in no dataset, or in the lake's main, is one
static bool IsCatalogRelation(const string &name) {
	auto base = StringUtil::Lower(name);
	if (StringUtil::EndsWith(base, "_history")) {
		base = base.substr(0, base.size() - 8);
	}
	for (auto relation : {"datasets", "kits", "sources", "recipes", "imports", "aspects", "glossary", "relationships",
	                      "functions", "measurements", "inputs", "tests"}) {
		if (base == relation) {
			return true;
		}
	}
	return false;
}

//! The table functions that read nothing: any other a body calls past the read functions and the
//! data's primitives reads what the engine cannot name (duckdb_tables(), a file)
static bool ReadsNothing(const string &function) {
	for (auto name : {"range", "generate_series", "unnest", "repeat", "repeat_row"}) {
		if (function == name) {
			return true;
		}
	}
	return false;
}

//! How many macros deep a walk follows a body: a macro past it is untracked
static constexpr idx_t MACRO_DEPTH = 8;

class BodyWalker {
public:
	//! dataset: the one an unqualified table name is taken in, empty where that is the subject's
	BodyWalker(ClientContext &context, string lake_p, string dataset_p)
	    : context(context), lake(std::move(lake_p)), dataset(std::move(dataset_p)) {
	}

	std::set<BodyRead> reads;

	void Script(const string &script, bool detector) {
		auto text = script;
		StringUtil::RTrim(text);
		while (!text.empty() && (text.back() == ';' || StringUtil::CharacterIsSpace(text.back()))) {
			text.pop_back();
		}
		auto options = ParserOptions::Builtin();
		options.compiled_grammar = CompiledGrammar::DefaultGrammar();
		Parser parser(options);
		try {
			parser.ParseQuery(text);
		} catch (std::exception &ex) {
			Add("untracked", "the body does not parse: " + FirstLine(ErrorData(ex).RawMessage()));
			return;
		}
		if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::SELECT_STATEMENT) {
			Add("untracked", "the body is not one SELECT");
			return;
		}
		std::set<string> ctes;
		if (detector) {
			ctes.insert("annotations");
		}
		Node(*parser.statements[0]->Cast<SelectStatement>().node, ctes, Env(), "", 0);
	}

private:
	//! An argument as the walk reads it: a constant's text, else what it is
	struct Arg {
		bool constant;
		string text;
	};
	//! A macro's parameters, by lower-case name, as its call passes them
	using Env = std::map<string, Arg>;

	ClientContext &context;
	string lake;
	string dataset;

	void Add(const string &kind, const string &read, const string &what = string(), const string &where = string(),
	         int constant = -1, const string &via = string()) {
		reads.insert({kind, read, what, where, constant, via});
	}

	optional_ptr<CatalogEntry> Lookup(CatalogType type, const QualifiedName &name) {
		try {
			return Catalog::GetEntry(context, EntryLookupInfo(type, name), OnEntryNotFound::RETURN_NULL);
		} catch (std::exception &) {
			return nullptr;
		}
	}

	static string Lower(const Identifier &name) {
		return StringUtil::Lower(name.GetIdentifierName());
	}

	//! Whether a name is unqualified, or in the lake's main schema
	bool InMain(const QualifiedName &name) {
		auto &catalog = name.Catalog();
		auto &schema = name.Schema();
		return (catalog.empty() || StringUtil::CIEquals(catalog.GetIdentifierName(), lake)) &&
		       (schema.empty() || Lower(schema) == "main");
	}

	Arg ArgOf(const ParsedExpression &expr, const Env &env) {
		switch (expr.GetExpressionClass()) {
		case ExpressionClass::CONSTANT: {
			auto &literal = expr.Cast<ConstantExpression>().GetLiteral();
			if (literal.IsNull()) {
				return {false, "<NULL>"};
			}
			return {true, literal.text};
		}
		case ExpressionClass::PARAMETER:
			return {false, "$" + expr.Cast<ParameterExpression>().Identifier().GetIdentifierName()};
		case ExpressionClass::COLUMN_REF: {
			auto &column = expr.Cast<ColumnRefExpression>();
			if (!column.IsQualified()) {
				auto bound = env.find(Lower(column.GetColumnName()));
				if (bound != env.end()) {
					return bound->second;
				}
			}
			break;
		}
		default:
			break;
		}
		return {false, "<" + ExpressionClassToString(expr.GetExpressionClass()) + ">"};
	}

	void Node(const QueryNode &node, std::set<string> ctes, const Env &env, const string &via, idx_t depth) {
		// a CTE's name stands for the CTE inside the query that defines it, and its children
		for (auto &cte : node.cte_map.map) {
			ctes.insert(Lower(cte.first));
		}
		for (auto &cte : node.cte_map.map) {
			if (cte.second->query_node) {
				Node(*cte.second->query_node, ctes, env, via, depth);
			}
		}
		switch (node.type) {
		case QueryNodeType::SELECT_NODE: {
			auto &select = node.Cast<SelectNode>();
			for (auto &expr : select.select_list) {
				Expr(*expr, ctes, env, via, depth);
			}
			for (auto &expr : select.groups.group_expressions) {
				Expr(*expr, ctes, env, via, depth);
			}
			for (auto expr : {select.where_clause.get(), select.having.get(), select.qualify.get()}) {
				if (expr) {
					Expr(*expr, ctes, env, via, depth);
				}
			}
			if (select.from_table) {
				Ref(*select.from_table, ctes, env, via, depth);
			}
			break;
		}
		case QueryNodeType::SET_OPERATION_NODE:
			for (auto &child : node.Cast<SetOperationNode>().children) {
				Node(*child, ctes, env, via, depth);
			}
			break;
		case QueryNodeType::RECURSIVE_CTE_NODE: {
			auto &recursive = node.Cast<RecursiveCTENode>();
			ctes.insert(Lower(recursive.ctename));
			Node(*recursive.left, ctes, env, via, depth);
			Node(*recursive.right, ctes, env, via, depth);
			break;
		}
		default:
			Add("untracked", "a statement that is not a query", "", "", -1, via);
			return;
		}
		// ORDER BY, LIMIT and DISTINCT ON hold expressions too
		vector<const ParsedExpression *> held;
		for (auto &modifier : node.modifiers) {
			switch (modifier->type) {
			case ResultModifierType::ORDER_MODIFIER:
				for (auto &order : modifier->Cast<OrderModifier>().orders) {
					held.push_back(order.expression.get());
				}
				break;
			case ResultModifierType::LIMIT_MODIFIER:
				held.push_back(modifier->Cast<LimitModifier>().limit.get());
				held.push_back(modifier->Cast<LimitModifier>().offset.get());
				break;
			case ResultModifierType::DISTINCT_MODIFIER:
				for (auto &target : modifier->Cast<DistinctModifier>().distinct_on_targets) {
					held.push_back(target.get());
				}
				break;
			default:
				break;
			}
		}
		for (auto expr : held) {
			if (expr) {
				Expr(*expr, ctes, env, via, depth);
			}
		}
	}

	void Ref(const TableRef &ref, const std::set<string> &ctes, const Env &env, const string &via, idx_t depth) {
		switch (ref.type) {
		case TableReferenceType::BASE_TABLE:
			Table(ref.Cast<BaseTableRef>().GetQualifiedName(), ctes, via);
			break;
		case TableReferenceType::JOIN: {
			auto &join = ref.Cast<JoinRef>();
			Ref(*join.left, ctes, env, via, depth);
			Ref(*join.right, ctes, env, via, depth);
			if (join.condition) {
				Expr(*join.condition, ctes, env, via, depth);
			}
			break;
		}
		case TableReferenceType::SUBQUERY:
			Node(*ref.Cast<SubqueryRef>().subquery->node, ctes, env, via, depth);
			break;
		case TableReferenceType::TABLE_FUNCTION: {
			auto &function = ref.Cast<TableFunctionRef>();
			if (function.function && function.function->GetExpressionClass() == ExpressionClass::FUNCTION) {
				Call(function.function->Cast<FunctionExpression>(), true, ctes, env, via, depth);
			}
			if (function.subquery) {
				Node(*function.subquery->node, ctes, env, via, depth);
			}
			break;
		}
		case TableReferenceType::EXPRESSION_LIST:
			for (auto &row : ref.Cast<ExpressionListRef>().values) {
				for (auto &expr : row) {
					Expr(*expr, ctes, env, via, depth);
				}
			}
			break;
		case TableReferenceType::PIVOT: {
			auto &pivot = ref.Cast<PivotRef>();
			Ref(*pivot.source, ctes, env, via, depth);
			for (auto &expr : pivot.aggregates) {
				Expr(*expr, ctes, env, via, depth);
			}
			break;
		}
		case TableReferenceType::SHOW_REF: {
			auto &show = ref.Cast<ShowRef>();
			if (show.query) {
				Node(*show.query, ctes, env, via, depth);
			} else {
				Add("untracked", "SHOW", "", "", -1, via);
			}
			break;
		}
		default:
			break;
		}
	}

	void Expr(const ParsedExpression &expr, const std::set<string> &ctes, const Env &env, const string &via,
	          idx_t depth) {
		switch (expr.GetExpressionClass()) {
		case ExpressionClass::SUBQUERY: {
			auto &subquery = expr.Cast<SubqueryExpression>();
			if (subquery.GetChild()) {
				Expr(*subquery.GetChild(), ctes, env, via, depth);
			}
			Node(*subquery.Subquery()->node, ctes, env, via, depth);
			return;
		}
		case ExpressionClass::FUNCTION:
			Call(expr.Cast<FunctionExpression>(), false, ctes, env, via, depth);
			break;
		default:
			break;
		}
		ParsedExpressionIterator::EnumerateChildren(
		    expr, [&](const ParsedExpression &child) { Expr(child, ctes, env, via, depth); });
	}

	//! A table named in FROM: a CTE in scope reads no table; a name of the catalog's relations,
	//! unqualified or in main, reads the catalog; a table or view of a dataset is data, by its
	//! canonical name; an unqualified name is taken in the function's dataset, or kept as written
	//! for the subject's; a table of another database is untracked
	void Table(const QualifiedName &name, const std::set<string> &ctes, const string &via) {
		auto table = name.Name().GetIdentifierName();
		bool bare = name.Schema().empty() && name.Catalog().empty();
		if (bare && ctes.count(StringUtil::Lower(table))) {
			return;
		}
		if (InMain(name) && (IsCatalogRelation(table) || !bare)) {
			Add("catalog", bare ? table : "main." + table, "", "", -1, via);
			return;
		}
		if (bare && dataset.empty()) {
			Add("data", table, "", "", -1, via);
			return;
		}
		auto entry = Lookup(CatalogType::TABLE_ENTRY,
		                    bare ? QualifiedName(Identifier(lake), Identifier(dataset), Identifier(table)) : name);
		if (!entry) {
			// a table the lake does not hold yet is read by its name: its version is `gone` until it is
			// loaded. One named in another database is outside the lake
			if (bare) {
				Add("data", dataset + "." + table, "", "", -1, via);
			} else if (name.Catalog().empty() || StringUtil::CIEquals(name.Catalog().GetIdentifierName(), lake)) {
				Add("data", name.Schema().GetIdentifierName() + "." + table, "", "", -1, via);
			} else {
				Add("untracked", name.ToString(), "", "", -1, via);
			}
			return;
		}
		if (!StringUtil::CIEquals(entry->ParentCatalog().GetName().GetIdentifierName(), lake)) {
			Add("untracked", name.ToString(), "", "", -1, via);
			return;
		}
		auto schema = entry->ParentSchema().name.GetIdentifierName();
		if (StringUtil::Lower(schema) == "main") {
			Add("catalog", "main." + entry->name.GetIdentifierName(), "", "", -1, via);
			return;
		}
		Add("data", schema + "." + entry->name.GetIdentifierName(), "", "", -1, via);
	}

	void Call(const FunctionExpression &function, bool table, const std::set<string> &ctes, const Env &env,
	          const string &via, idx_t depth) {
		auto &name = function.GetQualifiedName();
		auto fn = Lower(name.Name());
		auto &arguments = function.GetArguments();
		vector<Arg> args;
		for (auto &argument : arguments) {
			args.push_back(ArgOf(argument.GetExpression(), env));
		}
		auto texts = [&]() {
			vector<string> list;
			for (auto &arg : args) {
				list.push_back(arg.text);
			}
			return StringUtil::Join(list, ", ");
		};
		bool main = InMain(name);
		if (main && (fn == "annotations" || fn == "results" || fn == "definition")) {
			bool constant = !args.empty();
			for (auto &arg : args) {
				constant = constant && arg.constant;
			}
			Add("read", fn, fn == "definition" ? "definition" : (args.empty() ? "" : args[0].text),
			    args.empty() ? "" : args.back().text, constant ? 1 : 0, via);
			return;
		}
		if (table && main && fn == "glossql_additivity_cells") {
			Add("data", "glossql_additivity_cells", "data", "the dataset", -1, via);
			Add("read", "annotations", "role", "the dataset", 1, via);
			Add("read", "lineage", "relates", "the dataset", 1, via);
			return;
		}
		if (table && main &&
		    (fn == "subject_column" || fn == "dataset_cells" || fn == "glossql_relationship_checks" ||
		     fn == "branch_rows")) {
			Add("data", fn, texts(), "", -1, via);
			return;
		}
		if (function.IsOperator()) {
			return;
		}
		auto macro = Lookup(table ? CatalogType::TABLE_MACRO_ENTRY : CatalogType::MACRO_ENTRY, name);
		if (macro && !macro->ParentCatalog().IsSystemCatalog()) {
			Follow(macro->Cast<MacroCatalogEntry>(), arguments, env, via, depth);
			return;
		}
		if (table) {
			if (main && (StringUtil::StartsWith(fn, "glossql_") || fn == "glossary" || fn == "tests")) {
				Add("catalog", fn, texts(), "", -1, via);
			} else if (ReadsNothing(fn)) {
				Add("other", fn, texts(), "", -1, via);
			} else {
				Add("untracked", fn + "(" + texts() + ")", "", "", -1, via);
			}
			return;
		}
		if (!macro && !Lookup(CatalogType::SCALAR_FUNCTION_ENTRY, name) &&
		    !Lookup(CatalogType::AGGREGATE_FUNCTION_ENTRY, name) && !Lookup(CatalogType::TABLE_FUNCTION_ENTRY, name)) {
			Add("untracked", "no function " + name.ToString(), "", "", -1, via);
		}
	}

	//! A macro the body calls, followed as its call reads it: a parameter the call passes a constant
	//! reads as that constant, one it leaves out as its default
	void Follow(MacroCatalogEntry &entry, const vector<FunctionArgument> &arguments, const Env &env, const string &via,
	            idx_t depth) {
		auto qualified = entry.ParentCatalog().GetName().GetIdentifierName() + "." +
		                 entry.ParentSchema().name.GetIdentifierName() + "." + entry.name.GetIdentifierName();
		auto path = via.empty() ? entry.name.GetIdentifierName() : via + " > " + entry.name.GetIdentifierName();
		if (depth >= MACRO_DEPTH) {
			Add("untracked", "macros deeper than " + to_string(MACRO_DEPTH) + ": " + path, "", "", -1, via);
			return;
		}
		Add("macro", qualified, "", "", -1, via);
		idx_t positional = 0;
		for (auto &argument : arguments) {
			positional += argument.HasName() ? 0 : 1;
		}
		// the overload whose parameters the call fills, else the first
		optional_ptr<MacroFunction> chosen;
		for (auto &macro : entry.macros) {
			if (!chosen || (macro->parameters.size() >= positional &&
			                macro->parameters.size() - macro->default_parameters.size() <= positional &&
			                chosen->parameters.size() < positional)) {
				chosen = macro.get();
			}
		}
		if (!chosen) {
			return;
		}
		Env inner;
		idx_t next = 0;
		for (auto &parameter : chosen->parameters) {
			auto name = parameter->Cast<ColumnRefExpression>().GetColumnName();
			auto key = Lower(name);
			bool bound = false;
			for (auto &argument : arguments) {
				if (argument.HasName() && Lower(argument.GetName()) == key) {
					inner[key] = ArgOf(argument.GetExpression(), env);
					bound = true;
				}
			}
			if (!bound) {
				while (next < arguments.size() && arguments[next].HasName()) {
					next++;
				}
				if (next < arguments.size()) {
					inner[key] = ArgOf(arguments[next++].GetExpression(), env);
				} else {
					auto fallback = chosen->default_parameters.find(name);
					inner[key] = fallback != chosen->default_parameters.end() ? ArgOf(*fallback->second, Env())
					                                                          : Arg {false, "<missing>"};
				}
			}
		}
		if (chosen->type == MacroType::TABLE_MACRO) {
			Node(*chosen->Cast<TableMacroFunction>().query_node, std::set<string>(), inner, path, depth + 1);
		} else if (chosen->type == MacroType::SCALAR_MACRO) {
			Expr(*chosen->Cast<ScalarMacroFunction>().expression, std::set<string>(), inner, path, depth + 1);
		}
	}
};

//! The places a where names, as the lineage kit lists them (place_list())
static std::set<string> PlaceNames(Inner &inner) {
	std::set<string> places;
	auto rows = inner.Run("SELECT p.\"where\" FROM (SELECT unnest(main.place_list()) AS p)");
	while (auto chunk = rows->Fetch()) {
		for (idx_t r = 0; r < chunk->size(); r++) {
			places.insert(chunk->GetValue(0, r).ToString());
		}
	}
	return places;
}

//! What a body reads (BodyWalker). `dataset` is the function's own, empty for a GLOBAL one, whose
//! unqualified names are taken in the subject's dataset when it computes
static std::set<BodyRead> BodyReadSet(Inner &inner, const string &script, bool detector, const string &dataset) {
	BodyWalker walker(*inner.con.context, inner.ws.lake, dataset);
	inner.con.context->RunFunctionInTransaction([&]() { walker.Script(script, detector); });
	return walker.reads;
}

//! The first read of a body no input can follow, as the refusal names it, or empty: the catalog
//! reached past the read functions, a read function's what or where that is not a constant, a where
//! that is no place, dataset_cells() over a dataset that is not the subject's, and what the walk
//! cannot follow (SPEC §6). `dataset` is the function's own, empty for a GLOBAL one
static string UntrackedRead(const std::set<BodyRead> &reads, const std::set<string> &places, const string &dataset) {
	for (auto &read : reads) {
		string named;
		if (read.kind == "catalog" || read.kind == "untracked") {
			named = read.what.empty() ? read.read : read.read + "(" + read.what + ")";
		} else if (read.kind == "read" && (read.constant != 1 || !places.count(read.where))) {
			named = read.read + "(" + read.what + ", " + read.where + ")";
		} else if (read.kind == "data" && read.read == "dataset_cells" && read.where.empty()) {
			auto args = StringUtil::Split(read.what, ", ");
			bool place = false;
			for (idx_t i = 1; i < args.size(); i++) {
				place = place || places.count(args[i]);
			}
			auto first = args.empty() ? string() : args[0];
			bool constant = !first.empty() && first[0] != '<' && first[0] != '$';
			if (!place && constant && first.find('.') == string::npos && first != dataset) {
				named = "dataset_cells(" + read.what + ")";
			}
		}
		if (!named.empty()) {
			return read.via.empty() ? named : named + " through " + read.via;
		}
	}
	return string();
}

//! The refusal of a body's read no input can follow
static string UntrackedRefusal(const string &function, const string &read) {
	return StringUtil::Format("glossql: untracked_read: %s reads %s, past annotations(), results(), definition() "
	                          "and the data: read the catalog through them, with a constant what and a place",
	                          function, read);
}

//! What a body reads, as functions.reads stores it: a JSON list of the reads in their order
static string ReadsJson(const std::set<BodyRead> &reads) {
	auto text = [](const string &value) {
		return value.empty() ? string("null") : JsonString(value);
	};
	vector<string> items;
	for (auto &read : reads) {
		items.push_back("{\"kind\":" + JsonString(read.kind) + ",\"read\":" + JsonString(read.read) +
		                ",\"what\":" + text(read.what) + ",\"where\":" + text(read.where) + ",\"constant\":" +
		                (read.constant < 0 ? "null"
		                 : read.constant   ? "true"
		                                   : "false") +
		                ",\"via\":" + JsonString(read.via) + "}");
	}
	return "[" + StringUtil::Join(items, ",") + "]";
}

//! The cycle a function's reads close over the functions' reads, as the path of function names
//! from it back to it, or empty: a read of results reads that function's; a read of annotations reads
//! the values of the functions that return the property
static string ReadsCycle(Inner &inner, const string &name, const Value &returns, const string &reads) {
	auto edges = "WITH f AS (SELECT name, returns, reads FROM " + inner.Current("functions") + " WHERE name <> " +
	             QuoteText(name) + " UNION ALL SELECT " + QuoteText(name) + ", " + returns.ToSQLString() + ", " +
	             QuoteText(reads) +
	             "), r AS (SELECT f.name, u.r.read, u.r.what FROM f, unnest(from_json(f.reads, '[{\"kind\": "
	             "\"VARCHAR\", \"read\": \"VARCHAR\", \"what\": \"VARCHAR\"}]')) u(r) WHERE u.r.kind = 'read'),"
	             " p AS (SELECT a.name, 'x-kind ' || json_extract_string(a.schema, '$.x-kind') AS kind FROM " +
	             inner.Current("aspects") +
	             " a), e AS (SELECT r.name AS src, r.what AS dst FROM r WHERE r.read = 'results'"
	             " UNION SELECT r.name, g.name FROM r JOIN f g ON g.returns = r.what OR g.returns IN"
	             " (SELECT p.name FROM p WHERE p.kind = r.what) WHERE r.read = 'annotations'),"
	             " walk(n, path) AS (SELECT dst, [src, dst] FROM e WHERE src = " +
	             QuoteText(name) +
	             " AND dst <> src UNION ALL SELECT e.dst, list_append(w.path, e.dst) FROM walk w JOIN e ON e.src = w.n"
	             " WHERE w.n <> " +
	             QuoteText(name) + " AND e.dst <> e.src AND NOT list_contains(w.path[2:], e.dst))";
	auto cycle = inner.Scalar("WITH RECURSIVE " + edges.substr(5) +
	                          " SELECT array_to_string(path, ' > ') FROM walk WHERE n = " + QuoteText(name) +
	                          " ORDER BY len(path) LIMIT 1");
	return cycle.IsNull() ? string() : cycle.ToString();
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
	auto body = BodyReadSet(inner, script, returns.IsNull(), scope == "GLOBAL" ? string() : scope);
	// a read no input can follow would leave its result out of date once stored (SPEC §6)
	auto untracked = UntrackedRead(body, PlaceNames(inner), scope == "GLOBAL" ? string() : scope);
	if (!untracked.empty()) {
		throw InvalidInputException(UntrackedRefusal(name, untracked));
	}
	// identity is content (SPEC §3): an unchanged re-declaration is a no-op, so a kit replays silently
	auto reads = ReadsJson(body);
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
	// a function whose results it reads, directly or through others, reads its own: no value of the
	// two settles. Its own results apart, which are never its input (SPEC §6)
	auto cycle = ReadsCycle(inner, name, returns, reads);
	if (!cycle.empty()) {
		throw InvalidInputException("glossql: reads_cycle: %s reads results that read its own (%s)", name, cycle);
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

//! A read of a stored result (SPEC §6): a read function's call with its what and its where; data,
//! what the assets of its where read of the lake; the function's own declaration and the property
//! it returns. A body that reads past them is refused at its declaration (UntrackedRead)
struct Read {
	string read;
	string what;
	string where;

	bool operator<(const Read &other) const {
		return std::tie(read, what, where) < std::tie(other.read, other.what, other.where);
	}
};

//! The data a body reads, as a place: subject_column(), the relationship checks and branch_rows()
//! the subject's own; dataset_cells() its dataset's tables, its views ('views'), both ('all', true), the tables
//! and views of a place, or the one table it names; a dataset it names is the subject's, since one
//! that is not is refused at the declaration. A table the body names in FROM is that table: one
//! named without its dataset is taken in the subject's, by the name the lake spells it with, or as
//! written where the lake holds none yet, whose version is `gone` until it is loaded
static Read DataRead(ClientContext &context, const std::set<string> &places, const string &read, const string &args,
                     const string &dataset) {
	if (read == "subject_column" || read == "glossql_relationship_checks" || read == "branch_rows") {
		return {"data", "data", "itself"};
	}
	if (read == "dataset_cells") {
		auto list = StringUtil::Split(args, ", ");
		string what = "tables";
		for (idx_t i = 1; i < list.size(); i++) {
			if (places.count(list[i])) {
				return {"data", "data", list[i]};
			}
			if (list[i] == "views") {
				what = "views";
			} else if (list[i] == "all" || list[i] == "true") {
				what = "data";
			}
		}
		auto first = list.empty() ? string() : list[0];
		bool constant = !first.empty() && first[0] != '<' && first[0] != '$';
		if (constant && first.find('.') != string::npos) {
			return {"data", what, first};
		}
		if (constant && first != dataset) {
			throw InternalException("glossql: dataset_cells(%s) passed the declaration", args);
		}
		return {"data", what, "the dataset"};
	}
	if (read.find('.') != string::npos) {
		return {"data", "data", read};
	}
	auto entry = RelationEntry(context, dataset, read);
	if (!entry) {
		return {"data", "data", dataset + "." + read};
	}
	return {"data", "data", dataset + "." + entry->name.GetIdentifierName()};
}

//! The reads of a result of a function, from the reads its declaration stored (functions.reads), each
//! one an input can follow (UntrackedRead). A function's own results are no input of its own
static vector<Read> ReadsOfFunction(ClientContext &context, Inner &inner, const string &name, const string &scope,
                                    const string &dataset) {
	std::set<Read> reads {{"function", "", ""}, {"property", "", ""}};
	auto places = PlaceNames(inner);
	auto rows = inner.Run("SELECT r.kind, r.read, r.what, r.\"where\" FROM " + inner.Current("functions") +
	                      " f, unnest(from_json(f.reads, '[{\"kind\": \"VARCHAR\", \"read\": \"VARCHAR\", \"what\":"
	                      " \"VARCHAR\", \"where\": \"VARCHAR\"}]')) u(r) WHERE f.name = " +
	                      QuoteText(name) + " AND f.scope = " + QuoteText(scope));
	auto text = [](const Value &value) {
		return value.IsNull() ? string() : value.ToString();
	};
	while (auto chunk = rows->Fetch()) {
		for (idx_t r = 0; r < chunk->size(); r++) {
			auto kind = text(chunk->GetValue(0, r));
			auto read = text(chunk->GetValue(1, r));
			auto what = text(chunk->GetValue(2, r));
			auto where = text(chunk->GetValue(3, r));
			if (kind == "read") {
				if (read != "results" || what != name) {
					reads.insert({read, what, where});
				}
			} else if (kind == "data") {
				reads.insert(where.empty() ? DataRead(context, places, read, what, dataset)
				                           : Read {"data", what, where});
			} else if (kind == "catalog" || kind == "untracked") {
				throw InternalException("glossql: %s's read %s passed the declaration", name, read);
			}
		}
	}
	return vector<Read>(reads.begin(), reads.end());
}

//! The nodes a result of the function on the subject reads (docs/specification/tables/inputs.md),
//! (asset, what), taken before the body runs: per read, the nodes of its place's assets and those its
//! place's walk passes (main.place_nodes()); and the declarations of the function, of each macro its
//! body follows and of the property it fills
static vector<pair<string, string>> NodesRead(Inner &inner, const string &dataset, const string &subject,
                                              const string &function, const string &scope, const string &returns,
                                              const vector<Read> &reads) {
	vector<string> wanted {"(NULL, NULL, NULL)"};
	for (auto &read : reads) {
		if (read.read != "function" && read.read != "property") {
			wanted.push_back("(" + QuoteText(read.read) + ", " + QuoteText(read.what) + ", " + QuoteText(read.where) +
			                 ")");
		}
	}
	// $d the dataset, $s the subject, $f the function, $p the property it fills; a table a body names
	// is no place, and is its own asset
	string sql = R"(WITH w("read", what, "where") AS (VALUES $wanted),
pl AS (SELECT p."where", p.anchored FROM (SELECT unnest(main.place_list()) AS p)),
wa AS (SELECT *, CASE WHEN "where" IN (SELECT "where" FROM pl WHERE anchored) THEN $d ELSE $s END AS anchor
       FROM w WHERE "read" IS NOT NULL),
a AS (SELECT "read", what, "where", anchor AS asset FROM wa WHERE "where" = 'itself'
      UNION ALL SELECT wa."read", wa.what, wa."where", p.asset FROM wa
                JOIN main.places($d) p ON p."where" = wa."where" AND p.subject = wa.anchor
      UNION ALL SELECT "read", what, "where", "where" FROM wa WHERE "where" NOT IN (SELECT "where" FROM pl)),
props AS (SELECT DISTINCT wa.what, x.name, from_json(x.grains, '["VARCHAR"]') AS grains FROM wa JOIN $aspects x
          ON x.name = wa.what
          OR (starts_with(wa.what, 'x-kind ') AND json_extract_string(x.schema, '$.x-kind') = substr(wa.what, 8))
          WHERE wa."read" = 'annotations'),
-- the assets an annotations read returns rows for: those of the property's grains
ann AS (SELECT a.asset, p.name FROM a JOIN props p ON p.what = a.what
        WHERE a."read" = 'annotations' AND (len(p.grains) = 0 OR list_contains(p.grains, main.asset_grain(a.asset)))),
vr AS MATERIALIZED (FROM main.view_reads($d)),
kind AS MATERIALIZED (FROM glossql_kind_assets($d)),
rd AS (SELECT t.table_name AS r FROM a JOIN main.tables_read($d) t ON t.asset = a.asset
       WHERE a."read" = 'data' AND (a.what = 'data' OR (a.what = 'tables' AND t.asset_kind = 'table')
                                    OR (a.what = 'views' AND t.asset_kind = 'view'))
       UNION SELECT asset FROM a WHERE "read" = 'data' AND "where" NOT IN (SELECT "where" FROM pl)),
rv AS (SELECT r FROM rd UNION SELECT vr.relation FROM rd JOIN vr ON vr.view = rd.r)
SELECT DISTINCT asset, what FROM (
  SELECT n.asset, n.what FROM wa JOIN main.place_nodes($d) n ON n."where" = wa."where" AND n.subject = wa.anchor
  UNION ALL SELECT asset, u.k || ' ' || name FROM ann, unnest(['annotation', 'value']) u(k)
  UNION ALL SELECT split_part(asset, '.', 1) || '.' || split_part(asset, '.', 2), 'definition' FROM ann
            WHERE contains(asset, '.') AND main.asset_grain(asset) <> 'RELATIONSHIP'
  UNION ALL SELECT name, 'property' FROM props
  UNION ALL SELECT t.detector, 'function' FROM $tests t JOIN props p ON p.name = t.aspect WHERE t.detector IS NOT NULL
  UNION ALL SELECT substr(what, 8), 'properties' FROM wa WHERE "read" = 'annotations' AND starts_with(what, 'x-kind ')
            AND NOT ("where" = 'itself' AND $s IN (SELECT asset FROM kind))
  -- a result on an asset of a kind a kit declares reads the node that declares it
  UNION ALL SELECT node_asset, node_what FROM kind WHERE asset = $s
  UNION ALL SELECT asset, 'result ' || what FROM a WHERE "read" = 'results'
  UNION ALL SELECT asset, 'definition' FROM a WHERE "read" = 'definition'
  UNION ALL SELECT vr.relation, u.k FROM a JOIN vr ON vr.view = a.asset, unnest(['definition', 'recipe']) u(k)
            WHERE a."read" = 'definition'
  UNION ALL SELECT r, CASE WHEN r IN (SELECT schema_name || '.' || view_name FROM duckdb_views()
                                      WHERE database_name = $lake) THEN 'definition' ELSE 'data' END FROM rv
  UNION ALL SELECT $f, 'function' UNION ALL SELECT $p, 'property'
  UNION ALL SELECT substr(u.r.read, strpos(u.r.read, '.') + 1), 'macro'
            FROM $functions f, unnest(from_json(f.reads, '[{"kind": "VARCHAR", "read": "VARCHAR"}]')) u(r)
            WHERE f.name = $f AND f.scope = $scope AND u.r.kind = 'macro') ORDER BY ALL)";
	for (auto &term : vector<pair<string, string>> {{"$wanted", StringUtil::Join(wanted, ", ")},
	                                                {"$aspects", inner.Current("aspects")},
	                                                {"$tests", inner.Current("tests")},
	                                                {"$functions", inner.Current("functions")},
	                                                {"$lake", QuoteText(inner.ws.lake)},
	                                                {"$scope", QuoteText(scope)},
	                                                {"$d", QuoteText(dataset)},
	                                                {"$s", QuoteText(subject)},
	                                                {"$f", QuoteText(function)},
	                                                {"$p", QuoteText(returns)}}) {
		sql = StringUtil::Replace(sql, term.first, term.second);
	}
	vector<pair<string, string>> nodes;
	auto rows = inner.Run(sql);
	while (auto chunk = rows->Fetch()) {
		for (idx_t r = 0; r < chunk->size(); r++) {
			nodes.emplace_back(chunk->GetValue(0, r).ToString(), chunk->GetValue(1, r).ToString());
		}
	}
	return nodes;
}

//! The stored row for the call while it is current, (value, error), else empty
static vector<Value> Served(Inner &inner, const string &dataset, const string &name, const string &subject) {
	return inner.Row("SELECT value, error FROM (" +
	                 CurrentResultsSQL(inner, "dataset = " + QuoteText(dataset) + " AND function = " + QuoteText(name) +
	                                              " AND subject = " + QuoteText(subject)) +
	                 ") WHERE current");
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

//! Whether the body raised an error: one of the engine's own, an interrupt, a fatal or internal
//! error or memory run out, is no result of the body
static bool RaisedByBody(const ErrorData &error) {
	switch (error.Type()) {
	case ExceptionType::INTERRUPT:
	case ExceptionType::FATAL:
	case ExceptionType::INTERNAL:
	case ExceptionType::OUT_OF_MEMORY:
		return false;
	default:
		return true;
	}
}

//! Takes a function's reads again from its body, and stores them where they moved (BodyReadSet).
//! Returns the read no input can follow, where a macro the body calls now makes one, or empty: the
//! reads are stored all the same, so a result reads the declarations of the macros they follow
static string RetakeReads(ClientContext &context, Inner &inner, const string &name, const string &scope,
                          const Value &returns) {
	auto current = inner.Row("SELECT script, reads, declared_at, declared_by FROM " + inner.Current("functions") +
	                         " WHERE name = " + QuoteText(name) + " AND scope = " + QuoteText(scope));
	if (current.empty()) {
		return string();
	}
	auto script = current[0].ToString();
	auto dataset = scope == "GLOBAL" ? string() : scope;
	auto body = BodyReadSet(inner, script, returns.IsNull(), dataset);
	auto untracked = UntrackedRead(body, PlaceNames(inner), dataset);
	auto reads = ReadsJson(body);
	if (SameValue(current[1], Value(reads))) {
		return untracked;
	}
	inner.Write(context, [&](CatalogWrite &write) {
		write.End("functions", "name = " + QuoteText(name) + " AND scope = " + QuoteText(scope));
		write.Insert("functions", QuoteText(name) + ", " + QuoteText(scope) + ", " + QuoteText(script) + ", " +
		                              returns.ToSQLString() + ", " + QuoteText(reads) + ", " +
		                              current[2].ToSQLString() + ", " + current[3].ToSQLString());
	});
	return untracked;
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
	// a body runs in the subject's dataset, and the read functions it reads the catalog through walk
	// from the subject this run computes for
	inner.Run("USE " + QuoteName(inner.ws.lake) + "." + QuoteName(dataset));
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
		// what the body reads is taken again: a macro it follows may have changed, a table it names
		// may exist now. The function's row follows where it moved. A macro that now reads past the
		// read functions makes the computation fail, stored with the function's declaration as its
		// input: a change to the macros it follows makes it out of date
		auto untracked = RetakeReads(context, inner, name, scope, returns);
		// the snapshot it reads at and the nodes it reads, taken before it runs: a change under it is a
		// change after it
		auto read_at = inner.Scalar("SELECT max(snapshot_id) FROM " + inner.ws.Meta("snapshot"));
		auto nodes =
		    NodesRead(inner, dataset, subject, name, scope, returns.ToString(),
			          untracked.empty() ? ReadsOfFunction(context, inner, name, scope, dataset) : vector<Read>());
		// a computation that fails is a result too: stored with its error, which serves while it is current.
		// An error the body did not raise (an interrupt, the engine's own) stores nothing
		Value value;
		string error;
		try {
			if (!untracked.empty()) {
				throw InvalidInputException(UntrackedRefusal(name, untracked));
			}
			value = ValueOf(inner, invocation);
			if (value.IsNull()) {
				value = Value("null");
			}
			auto breach = JsonSchemaCheck(contract.ToString(), value.ToString());
			if (!breach.empty()) {
				error = StringUtil::Format("glossql: the output of %s does not conform to the schema of %s: %s", name,
				                           returns.ToString(), breach);
			}
		} catch (std::exception &ex) {
			ErrorData raised(ex);
			if (!RaisedByBody(raised)) {
				throw;
			}
			error = raised.RawMessage();
		}
		if (context.IsInterrupted()) {
			throw InterruptException();
		}
		// a body that refuses its subject (not_a_<kind>) computed nothing: the refusal stores no result
		auto code = StringUtil::StartsWith(error, "glossql: ") ? error.substr(9) : error;
		if (StringUtil::StartsWith(code, "not_a_")) {
			throw InvalidInputException(error);
		}
		if (!error.empty()) {
			value = Value();
		}
		// stored with the nodes it read. The result it replaces ends, and its inputs with it; the same
		// value and error keep the snapshot they changed at, so what read them stays current
		auto failure = error.empty() ? Value() : Value(error);
		inner.Write(context, [&](CatalogWrite &write) {
			auto key = "dataset = " + QuoteText(dataset) + " AND function = " + QuoteText(name) +
			           " AND subject = " + QuoteText(subject);
			auto replaced = inner.Row("SELECT begin_snapshot, value, error, changed_at FROM " +
			                          inner.Current("measurements") + " WHERE " + key);
			auto changed_at = Value::BIGINT(write.snapshot);
			if (!replaced.empty()) {
				if (SameValue(replaced[1], value) && SameValue(replaced[2], failure)) {
					changed_at = replaced[3];
				}
				write.End("inputs", "begin_snapshot = " + replaced[0].ToString());
				write.End("measurements", key);
			}
			write.Insert("measurements", QuoteText(dataset) + ", " + QuoteText(name) + ", " + QuoteText(subject) +
			                                 ", " + returns.ToSQLString() + ", " +
			                                 (value.IsNull() ? string("NULL") : QuoteText(value.ToString())) +
			                                 ", now(), " + failure.ToSQLString() + ", " + read_at.ToString() + ", " +
			                                 changed_at.ToString());
			vector<string> rows;
			for (auto &node : nodes) {
				rows.push_back("(" + to_string(write.snapshot) + ", NULL, " + QuoteText(node.first) + ", " +
				               QuoteText(node.second) + ")");
			}
			if (!rows.empty()) {
				write.Run("INSERT INTO " + inner.Store("inputs") + " VALUES " + StringUtil::Join(rows, ", "));
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
// glossql_outdated(): each stored result that is out of date or possibly so —
// (dataset, function, subject, asset, what, reason): a node it read changed
// after it read (changed), or a result it read is out of date or possibly so
// and has not been computed again (possibly)
//===--------------------------------------------------------------------===//

static unique_ptr<TableRef> OutdatedBindReplace(ClientContext &context, TableFunctionBindInput &input) {
	Inner inner(context);
	return make_uniq<SubqueryRef>(ParseSelect(OutdatedSQL(inner)));
}

static TableFunction OutdatedFunction() {
	TableFunction function("glossql_outdated", {}, nullptr);
	function.bind_replace = OutdatedBindReplace;
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
	// a second argument true (or 'all') takes the dataset's views too — a metric definition
	// is one — each bound at bind and skipped when it does not, as a screen skips a
	// metric definition that fails; 'views' takes the views alone, so that a read over the
	// metric definitions — a metric's series, a plan's rows — binds no table; a place (the
	// lineage kit's) takes the tables and views of the place, walked from the running
	// function's subject, else from the dataset
	bool views = false;
	bool only_views = false;
	bool placed = false;
	std::set<string> place;
	if (input.inputs.size() > 1 && !input.inputs[1].IsNull()) {
		if (input.inputs[1].type().id() == LogicalTypeId::VARCHAR) {
			auto mode = input.inputs[1].ToString();
			if (mode == "views") {
				views = only_views = true;
			} else if (mode == "all") {
				views = true;
			} else if (PlaceNames(inner).count(mode)) {
				views = placed = true;
				Value subject;
				if (!ClientConfig::GetConfig(context).GetUserVariable(string(RUN_SUBJECT), subject)) {
					ClientConfig::GetConfig(*inner.con.context).SetUserVariable(RUN_SUBJECT, Value(parts[0]));
				}
				auto rows = inner.Run(
				    "SELECT DISTINCT split_part(asset, '.', 2) FROM main.place(" + QuoteText(mode) +
				    ") WHERE main.asset_grain(asset) = 'TABLE' AND split_part(asset, '.', 1) = " + QuoteText(parts[0]));
				while (auto chunk = rows->Fetch()) {
					for (idx_t r = 0; r < chunk->size(); r++) {
						place.insert(chunk->GetValue(0, r).ToString());
					}
				}
			} else if (mode != "tables") {
				throw InvalidInputException("glossql: dataset_cells takes tables, views, all or a place, got '%s'",
				                            mode);
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
		if ((only_views && !column.view) || (placed && !place.count(column.table))) {
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
// for an edge across two tables the orphan keys (the three with the most rows,
// each with its rows; a pair of key columns as a JSON list; each value cut to
// its first 100 characters) and how many there are, and the temporal pairs: the first three date
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
		for (auto column : StringUtil::Split(key.substr(1, key.size() - 2), ',')) {
			StringUtil::Trim(column);
			result.key_columns.push_back(column);
		}
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

//! How many characters of an orphan key's value the examples keep; the counts are of the whole values
constexpr idx_t ORPHAN_KEY_LENGTH = 100;

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
	// the child keys no parent row carries, each with its rows: the orphans of an edge across tables,
	// grouped by the key columns and served as the three with the most rows, a pair as a JSON list and
	// each value cut to its first ORPHAN_KEY_LENGTH characters, and the count of keys
	sql += ", o AS (SELECT k1, k2, count(*) AS n FROM c WHERE NOT EXISTS (SELECT 1 FROM p WHERE p.k1 = c.k1 AND "
	       "p.k2 IS NOT DISTINCT FROM c.k2) GROUP BY k1, k2)";
	auto cut = [](const string &column) {
		return "left(" + column + ", " + to_string(ORPHAN_KEY_LENGTH) + ")";
	};
	string shown = two ? "to_json([" + cut("k1") + ", " + cut("k2") + "])::VARCHAR" : cut("k1");
	string join_orphans = "(SELECT coalesce(sum(n), 0)::BIGINT FROM o)";
	string orphan_keys = "(SELECT list({'key': " + shown + ", 'rows': n} ORDER BY n DESC, k1, k2)[1:3] FROM o)";
	string orphan_distinct = "(SELECT count(*) FROM o)";
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
		orphan_keys = "NULL::STRUCT(key VARCHAR, \"rows\" BIGINT)[]";
		orphan_distinct = "NULL::BIGINT";
	}
	sql += ", e AS (SELECT (SELECT count(*) FROM c) AS filled, " + orphans + " AS orphans, " + orphan_keys +
	       " AS orphan_keys, " + orphan_distinct + " AS orphan_distinct)";
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
	       " AS relationship, e.filled, e.orphans, e.orphan_keys, e.orphan_distinct, t.pair_seq, t.child_column, "
	       "t.parent_column, t.joined, t.precedes FROM e LEFT JOIN (SELECT * FROM t WHERE joined > 0) t ON true)";
	return sql;
}

//! Whether two sides name the same table and key columns, as DuckDB matches names
static bool SameSide(const EdgeSide &a, const EdgeSide &b) {
	if (!StringUtil::CIEquals(a.table, b.table) || a.key_columns.size() != b.key_columns.size()) {
		return false;
	}
	for (idx_t i = 0; i < a.key_columns.size(); i++) {
		if (!StringUtil::CIEquals(a.key_columns[i], b.key_columns[i])) {
			return false;
		}
	}
	return true;
}

static unique_ptr<TableRef> RelationshipChecksBindReplace(ClientContext &context, TableFunctionBindInput &input) {
	Inner inner(context);
	// a dataset's every declared relationship, or one relationship by its pair path: its two sides,
	// as the declaration names them
	auto subject = ResolveSubject(context, input.inputs[0].ToString());
	string wanted_op;
	for (auto candidate : {"<->", "->"}) {
		if (wanted_op.empty() && subject.find(" " + string(candidate) + " ") != string::npos) {
			wanted_op = candidate;
		}
	}
	bool one = !wanted_op.empty();
	auto dataset = one ? DatasetOf(subject) : subject;
	EdgeSide wanted_child;
	EdgeSide wanted_parent;
	if (one) {
		auto at = subject.find(" " + wanted_op + " ");
		wanted_child = ParseSide(context, dataset, subject.substr(0, at));
		wanted_parent = ParseSide(context, dataset, subject.substr(at + wanted_op.size() + 2));
	}
	vector<string> edges;
	auto rows =
	    inner.Run("SELECT left_side, op, right_side FROM " + inner.Store("relationships") +
		          " WHERE dataset = " + QuoteText(dataset) + " AND end_snapshot IS NULL ORDER BY begin_snapshot");
	while (auto chunk = rows->Fetch()) {
		for (idx_t i = 0; i < chunk->size(); i++) {
			auto left = chunk->GetValue(0, i).ToString();
			auto op = chunk->GetValue(1, i).ToString();
			auto right = chunk->GetValue(2, i).ToString();
			auto child = ParseSide(context, dataset, left);
			auto parent = ParseSide(context, dataset, right);
			if (one && (op != wanted_op || !SameSide(child, wanted_child) || !SameSide(parent, wanted_parent))) {
				continue;
			}
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
			            " NULL::DOUBLE AS orphan_rate, NULL::STRUCT(key VARCHAR, \"rows\" BIGINT)[] AS orphan_keys,"
			            " NULL::BIGINT AS orphan_distinct, NULL::BIGINT AS pair_seq, NULL::VARCHAR AS child_column,"
			            " NULL::VARCHAR AS parent_column, NULL::BIGINT AS joined, NULL::BIGINT AS precedes,"
			            " NULL::DOUBLE AS precedes_rate"));
	}
	string sql =
	    "SELECT true AS applicable, NULL::VARCHAR AS reason, seq, relationship, filled, orphans, CASE WHEN filled "
	    "= 0 THEN 0.0 ELSE orphans / filled::DOUBLE END AS orphan_rate, orphan_keys, orphan_distinct, pair_seq, "
	    "child_column, parent_column, "
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

//! Whether a kit's CREATE OR REPLACE of a macro or a view states what the catalog holds already: such
//! a statement is not run, so an open writes nothing for an entry it leaves as it is
static bool HeldAlready(ClientContext &context, SQLStatement &statement) {
	if (statement.type != StatementType::CREATE_STATEMENT) {
		return false;
	}
	auto &info = *statement.Cast<CreateStatement>().info;
	if (info.on_conflict != OnCreateConflict::REPLACE_ON_CONFLICT ||
	    (info.type != CatalogType::MACRO_ENTRY && info.type != CatalogType::TABLE_MACRO_ENTRY &&
	     info.type != CatalogType::VIEW_ENTRY)) {
		return false;
	}
	bool same = false;
	context.RunFunctionInTransaction([&]() {
		auto entry = Catalog::GetEntry(context, EntryLookupInfo(info.type, info.GetQualifiedName()),
		                               OnEntryNotFound::RETURN_NULL);
		if (!entry) {
			return;
		}
		auto held = entry->GetInfo()->ToString();
		auto stated = info.Copy();
		stated->GetQualifiedNameMutable() = entry->GetInfo()->GetQualifiedName();
		stated->on_conflict = entry->GetInfo()->on_conflict;
		auto text = stated->ToString();
		if (text == held) {
			same = true;
			return;
		}
		// the lake stores the entry as text and parses it again when it loads it, and DuckDB's text of a
		// VALUES list under an alias does not parse back to the same text: compare once more after that
		// round trip
		auto options = ParserOptions::Builtin();
		options.compiled_grammar = CompiledGrammar::DefaultGrammar();
		Parser parser(options);
		parser.ParseQuery(text);
		if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::CREATE_STATEMENT) {
			return;
		}
		auto loaded = std::move(parser.statements[0]->Cast<CreateStatement>().info);
		loaded->GetQualifiedNameMutable() = stated->GetQualifiedName();
		loaded->on_conflict = stated->on_conflict;
		same = loaded->ToString() == held;
	});
	return same;
}

vector<pair<string, string>> RunKit(ClientContext &context, const string &name, const string &location,
                                    const string &script, bool in_lake) {
	vector<pair<string, string>> rows;
	auto hash = ScriptHash(script);
	Inner inner(context);
	inner.Run("SET active_grammar_extensions = ['glossql']");
	if (in_lake) {
		inner.Run("USE " + QuoteName(inner.ws.lake));
	}
	auto newest = inner.Scalar("SELECT hash FROM " + inner.Current("kits") + " WHERE name = " + QuoteText(name));
	bool current = !newest.IsNull() && newest.ToString() == hash;
	auto statements = inner.con.ExtractStatements(script);
	if (current && std::none_of(statements.begin(), statements.end(), [](const unique_ptr<SQLStatement> &statement) {
		    return statement->type == StatementType::LOAD_STATEMENT;
	    })) {
		rows.emplace_back(name, "current");
		return rows;
	}
	// every declaration the kit makes names it as its author
	inner.Run("SET VARIABLE glossql_declared_by = " + QuoteText("kit:" + name));
	for (auto &statement : statements) {
		auto head = FirstLine(statement->query);
		if (current && statement->type != StatementType::LOAD_STATEMENT) {
			continue;
		}
		if (HeldAlready(*inner.con.context, *statement)) {
			rows.emplace_back(head, "unchanged: no-op");
			continue;
		}
		auto result = inner.con.Query(std::move(statement));
		if (result->HasError()) {
			throw InvalidInputException("glossql: kit %s stopped at statement %d (%s): %s", name, rows.size() + 1, head,
			                            result->GetError());
		}
		string outcome = "ok";
		auto chunk = result->Fetch();
		if (chunk && chunk->ColumnCount() == 1 && chunk->size() == 1 &&
		    chunk->data[0].GetType().id() == LogicalTypeId::VARCHAR) {
			outcome = chunk->GetValue(0, 0).ToString();
		}
		rows.emplace_back(head, outcome);
	}
	if (current) {
		rows.emplace_back(name, "current");
	} else {
		auto count = rows.size();
		inner.Write(context, [&](CatalogWrite &write) {
			write.End("kits", "name = " + QuoteText(name));
			write.Insert("kits", QuoteText(name) + ", " + QuoteText(location) + ", " + to_string(count) + ", now(), " +
			                         QuoteText(hash));
		});
	}
	return rows;
}

static void KitExec(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<KitState>();
	if (!state.ran) {
		state.ran = true;
		auto path = data.bind_data->Cast<RunnerBindData>().inputs[0].ToString();
		state.rows = RunKit(context, KitName(path), path, ReadScript(context, path), false);
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
	loader.RegisterFunction(OutdatedFunction());
	loader.RegisterFunction(DatasetCellsFunctions());
	loader.RegisterFunction(RelationshipChecksFunction());
	loader.RegisterFunction(AdditivityCellsFunction());
}

} // namespace duckdb
