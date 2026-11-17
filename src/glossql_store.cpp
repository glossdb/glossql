// The glossql store: the workspace is an attached DuckLake, and the store
// relations are tables in its metadata catalog, glossql_<relation> beside the
// ducklake_* tables, written by the statements the grammar adds, read by
// GLOSSARY() and TESTS(), and ended by DELETE FROM glossary.
//
// A dataset is a schema of the lake (SPEC §3): DECLARE DATASET fin creates
// schema fin, DuckDB's own USE binds it, and an unprefixed subject resolves
// against the connection's default schema. A row is versioned as DuckLake
// versions its own: begin_snapshot is the lake snapshot the write that made
// it took, end_snapshot the one that replaced or removed it, and no row is
// deleted. The reads serve the current rows, human over agent over function;
// a test's detector judges the values at read (SPEC §7), and a value reads
// stale once its subject changed after the snapshot it was written at
// (SPEC §5.2), a join against the lake's column history.

#include "glossql_grammar.hpp"
#include "glossql_lineage_kit.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/catalog/catalog_search_path.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/common/enums/on_entry_not_found.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/function/pragma_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/peg/compiled_grammar.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/storage/object_cache.hpp"

#include <exception>
#include <functional>
#include <mutex>
#include <pthread.h>
#include <set>

#include <algorithm>
#include <functional>

namespace duckdb {

//===--------------------------------------------------------------------===//
// Names and subjects
//===--------------------------------------------------------------------===//

string QuoteName(const string &name) {
	return "\"" + StringUtil::Replace(name, "\"", "\"\"") + "\"";
}

string QuoteText(const string &text) {
	return "'" + StringUtil::Replace(text, "'", "''") + "'";
}

vector<string> SubjectParts(const string &subject) {
	vector<string> parts;
	auto first = subject.find('.');
	if (first == string::npos) {
		parts.push_back(subject);
		return parts;
	}
	parts.push_back(subject.substr(0, first));
	auto second = subject.find('.', first + 1);
	if (second == string::npos) {
		parts.push_back(subject.substr(first + 1));
		return parts;
	}
	parts.push_back(subject.substr(first + 1, second - first - 1));
	parts.push_back(subject.substr(second + 1));
	return parts;
}

string CurrentDataset(ClientContext &context) {
	auto &entry = ClientData::Get(context).catalog_search_path->GetDefault();
	return entry.GetSchema().GetIdentifierName();
}

static bool SchemaExists(ClientContext &context, const string &name) {
	auto &catalog = Catalog::GetCatalog(context, Identifier(LakeName(context)));
	return catalog.GetSchema(context, Identifier(name), OnEntryNotFound::RETURN_NULL) != nullptr;
}

//! Whether a name is a declared source, a workspace row (SPEC §5.1); read
//! through the typed view, which holds once PRAGMA glossql ran
static bool SourceExists(ClientContext &context, const string &name) {
	Connection con(*context.db);
	auto result = con.Query("SELECT count(*) FROM " + QuoteName(LakeName(context)) +
	                        ".main.sources WHERE name = " + QuoteText(name));
	if (result->HasError()) {
		return false;
	}
	auto chunk = result->Fetch();
	return chunk && chunk->size() == 1 && chunk->GetValue(0, 0).GetValue<int64_t>() > 0;
}

//! A relation of the lake by name, a loaded table or a view — a metric definition's
//! (SPEC §5.2) or DuckDB's own — since a view counts as a table (SPEC §3) and
//! its columns are subjects like a table's; null when neither holds
optional_ptr<CatalogEntry> RelationEntry(ClientContext &context, const string &schema, const string &table) {
	EntryLookupInfo lookup(CatalogType::TABLE_ENTRY,
	                       QualifiedName(Identifier(LakeName(context)), Identifier(schema), Identifier(table)));
	return Catalog::GetEntry(context, lookup, OnEntryNotFound::RETURN_NULL);
}

//! A relation's columns as the catalog spells them: a table's own; a view's
//! aliases over the names its query binds to, the view bound here if it was not
static vector<string> RelationColumns(ClientContext &context, CatalogEntry &entry) {
	vector<string> names;
	if (entry.type == CatalogType::VIEW_ENTRY) {
		auto &view = entry.Cast<ViewCatalogEntry>();
		view.BindView(context);
		auto bound = view.GetColumnInfo();
		for (idx_t i = 0; bound && i < bound->names.size(); i++) {
			names.push_back((i < view.aliases.size() ? view.aliases[i] : bound->names[i]).GetIdentifierName());
		}
		return names;
	}
	for (auto &column : entry.Cast<TableCatalogEntry>().GetColumns().Logical()) {
		names.push_back(column.Name().GetIdentifierName());
	}
	return names;
}

//! The column of a relation that a spelling names, matched as DuckDB matches
//! names, case-insensitively; empty when none does
static string ColumnOf(ClientContext &context, CatalogEntry &entry, const string &column) {
	for (auto &name : RelationColumns(context, entry)) {
		if (StringUtil::CIEquals(name, column)) {
			return name;
		}
	}
	return string();
}

vector<RelationColumn> DatasetColumns(ClientContext &context, const string &dataset, const string &table, bool views) {
	vector<RelationColumn> result;
	auto &catalog = Catalog::GetCatalog(context, Identifier(LakeName(context)));
	auto schema = catalog.GetSchema(context, Identifier(dataset), OnEntryNotFound::RETURN_NULL);
	if (!schema) {
		return result;
	}
	vector<std::reference_wrapper<CatalogEntry>> relations;
	schema->Scan(context, CatalogType::TABLE_ENTRY, [&](CatalogEntry &entry) {
		if (table.empty() || StringUtil::CIEquals(entry.name.GetIdentifierName(), table)) {
			relations.push_back(entry);
		}
	});
	std::sort(relations.begin(), relations.end(),
	          [](const std::reference_wrapper<CatalogEntry> &a, const std::reference_wrapper<CatalogEntry> &b) {
		          return a.get().name.GetIdentifierName() < b.get().name.GetIdentifierName();
	          });
	for (auto &relation : relations) {
		auto &entry = relation.get();
		auto name = entry.name.GetIdentifierName();
		idx_t ordinal = 0;
		if (entry.type == CatalogType::VIEW_ENTRY) {
			// main holds the kits' reads, no dataset's relations: binding one of
			// those views runs the glossary reads whose binds list these columns
			// again, under the bind DuckDB holds for the view
			if (!views || StringUtil::CIEquals(dataset, "main")) {
				continue;
			}
			auto &view = entry.Cast<ViewCatalogEntry>();
			shared_ptr<ViewColumnInfo> bound;
			try {
				view.BindView(context);
				bound = view.GetColumnInfo();
			} catch (std::exception &) {
				continue;
			}
			for (idx_t i = 0; bound && i < bound->names.size(); i++) {
				auto column = (i < view.aliases.size() ? view.aliases[i] : bound->names[i]).GetIdentifierName();
				result.push_back({name, column, bound->types[i].ToString(), ++ordinal, true});
			}
			continue;
		}
		if (entry.type != CatalogType::TABLE_ENTRY) {
			continue;
		}
		for (auto &column : entry.Cast<TableCatalogEntry>().GetColumns().Logical()) {
			result.push_back({name, column.Name().GetIdentifierName(), column.Type().ToString(), ++ordinal, false});
		}
	}
	return result;
}

static bool TableExists(ClientContext &context, const string &schema, const string &table) {
	return RelationEntry(context, schema, table) != nullptr;
}

static bool ColumnExists(ClientContext &context, const string &schema, const string &table, const string &column) {
	auto entry = RelationEntry(context, schema, table);
	// a tuple key names columns of the table, never a table of a dataset
	return entry && (StringUtil::StartsWith(column, "(") || !ColumnOf(context, *entry, column).empty());
}

//! A loaded table or a view and its column in the catalog's own spelling; a
//! path that names nothing stored holds as given, for the admission to refuse
static string Canonical(ClientContext &context, const string &schema, const string &table, const string &column) {
	auto entry = RelationEntry(context, schema, table);
	auto tail = column.empty() ? "" : "." + column;
	if (!entry) {
		return schema + "." + table + tail;
	}
	auto name = schema + "." + entry->name.GetIdentifierName();
	if (column.empty() || StringUtil::StartsWith(column, "(")) {
		return name + tail;
	}
	auto canonical = ColumnOf(context, *entry, column);
	return canonical.empty() ? name + tail : name + "." + canonical;
}

//! dataset | source | dataset.table | dataset.table.column, a tuple key allowed
//! at the end. Resolving a resolved path is a no-op: a three-part path whose
//! head is a dataset holds, so a subject may pass through twice (an
//! extraction hands its resolved subject to subject_column)
static string ResolvePath(ClientContext &context, const string &path) {
	auto dataset = CurrentDataset(context);
	auto first = path.find('.');
	if (first == string::npos) {
		// one name is a dataset or a declared source, the workspace's own rows
		// (SPEC §5.1), else a table of the USE'd dataset
		return SchemaExists(context, path) || SourceExists(context, path) ? path : dataset + "." + path;
	}
	// a dataset's and a table's name carry no dot; a column's may (SPEC §4), so the
	// first two dots are the separators and the column is all that follows
	auto head = path.substr(0, first);
	auto rest = path.substr(first + 1);
	auto second = rest.find('.');
	if (second != string::npos && SchemaExists(context, head)) {
		return Canonical(context, head, rest.substr(0, second), rest.substr(second + 1));
	}
	// two names: table.column of the USE'd dataset, or dataset.table. A stored
	// table of the USE'd dataset wins over a dataset of the same name (SPEC §3)
	// when it holds the column; a dataset holding the table wins otherwise
	if (TableExists(context, dataset, head) &&
	    (ColumnExists(context, dataset, head, rest) || !SchemaExists(context, head))) {
		return Canonical(context, dataset, head, rest);
	}
	if (second == string::npos && SchemaExists(context, head)) {
		return Canonical(context, head, rest, "");
	}
	return dataset + "." + path;
}

string ResolveSubject(ClientContext &context, const string &subject) {
	for (auto op : {" <-> ", " -> "}) {
		auto pos = subject.find(op);
		if (pos != string::npos) {
			return ResolvePath(context, subject.substr(0, pos)) + op +
			       ResolvePath(context, subject.substr(pos + strlen(op)));
		}
	}
	return ResolvePath(context, subject);
}

string DatasetOf(ClientContext &context, const string &subject) {
	auto head = DatasetOf(subject);
	// a bare source is written and read in the USE'd dataset: the deposit one
	// onboarding makes, which every dataset reads (SPEC §5.1)
	return head != subject || SchemaExists(context, head) ? head : CurrentDataset(context);
}

//! Parses plain SQL with the stock grammar, so no glossql rule captures it, into one SELECT
unique_ptr<SelectStatement> ParseSelect(const string &sql) {
	auto options = ParserOptions::Builtin();
	options.identifier_case_mode = IdentifierCaseMode::LOWERCASE;
	options.compiled_grammar = CompiledGrammar::DefaultGrammar();
	Parser parser(options);
	parser.ParseQuery(sql);
	if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::SELECT_STATEMENT) {
		throw InternalException("glossql: expected one SELECT, got: %s", sql);
	}
	return unique_ptr_cast<SQLStatement, SelectStatement>(std::move(parser.statements[0]));
}

//===--------------------------------------------------------------------===//
// glossql_init: the store's DDL, run by PRAGMA glossql
//===--------------------------------------------------------------------===//

//! The store relations: glossql_<relation> in the lake's metadata catalog,
//! text and numbers only, each row between its begin and end snapshot. A
//! declaration's SET pairs are stored as one JSON object, `settings`. The
//! lake's schema `main` reads the current rows of each as a typed view: JSON
//! bodies, lists from their JSON text. <relation>_history reads every row,
//! each with its end: what a read that compares with an earlier row reads
struct StoreRelation {
	const char *name;
	const char *columns;
	const char *view;
};

static const StoreRelation STORE_RELATIONS[] = {
    {"datasets",
	 "name VARCHAR NOT NULL, settings VARCHAR NOT NULL, declared_at TIMESTAMP NOT NULL, declared_by VARCHAR",
	 "begin_snapshot, name, settings::JSON AS settings, declared_at, declared_by"},
    {"kits",
	 "name VARCHAR NOT NULL, location VARCHAR NOT NULL, statements INTEGER NOT NULL, loaded_at TIMESTAMP NOT NULL, "
	 "hash VARCHAR",
	 "* EXCLUDE (end_snapshot)"},
    {"sources", "name VARCHAR NOT NULL, settings VARCHAR NOT NULL, declared_at TIMESTAMP NOT NULL, declared_by VARCHAR",
	 "begin_snapshot, name, settings::JSON AS settings, declared_at, declared_by"},
    {"recipes",
	 "dataset VARCHAR NOT NULL, table_name VARCHAR NOT NULL, source VARCHAR NOT NULL, settings VARCHAR NOT NULL, "
	 "sql VARCHAR NOT NULL, declared_at TIMESTAMP NOT NULL, declared_by VARCHAR",
	 "begin_snapshot, dataset, table_name, source, settings::JSON AS settings, sql, declared_at, declared_by"},
    // snapshot_id is the snapshot the load itself made; begin_snapshot is its row's
    {"imports",
	 "dataset VARCHAR NOT NULL, table_name VARCHAR NOT NULL, mode VARCHAR NOT NULL, rows BIGINT NOT NULL, "
	 "dropped_rows_count BIGINT, lineage VARCHAR NOT NULL, snapshot_id BIGINT, imported_at TIMESTAMP NOT NULL, "
	 "source_rows BIGINT, changed_rows BIGINT, fingerprint VARCHAR, identical BOOLEAN",
	 "begin_snapshot, dataset, table_name, mode, rows, dropped_rows_count, lineage::JSON AS lineage, snapshot_id, "
	 "imported_at, source_rows, changed_rows, fingerprint, identical"},
    {"aspects",
	 "name VARCHAR NOT NULL, kind VARCHAR NOT NULL, grains VARCHAR NOT NULL, condition VARCHAR, "
	 "schema VARCHAR NOT NULL, declared_at TIMESTAMP NOT NULL, declared_by VARCHAR",
	 "begin_snapshot, name, kind, from_json(grains, '[\"VARCHAR\"]') AS grains, condition::JSON AS condition, "
	 "schema::JSON AS schema, declared_at, declared_by"},
    {"glossary",
	 "dataset VARCHAR NOT NULL, subject VARCHAR NOT NULL, aspect VARCHAR NOT NULL, actor_kind VARCHAR NOT NULL, "
	 "actor_id VARCHAR, body VARCHAR NOT NULL, written_at TIMESTAMP NOT NULL",
	 "begin_snapshot, dataset, subject, aspect, actor_kind, actor_id, body::JSON AS body, written_at"},
    {"relationships",
	 "dataset VARCHAR NOT NULL, left_side VARCHAR NOT NULL, op VARCHAR NOT NULL, right_side VARCHAR NOT NULL, "
	 "declared_at TIMESTAMP NOT NULL, declared_by VARCHAR",
	 "* EXCLUDE (end_snapshot)"},
    // reads: what the body reads, taken from its text at the declaration and again where it moved
    // (BodyReads)
    {"functions",
	 "name VARCHAR NOT NULL, scope VARCHAR NOT NULL, script VARCHAR NOT NULL, returns VARCHAR, reads VARCHAR, "
	 "declared_at TIMESTAMP NOT NULL, declared_by VARCHAR",
	 "begin_snapshot, name, scope, script, returns, reads::JSON AS reads, declared_at, declared_by"},
    // read_at: the lake's newest snapshot when the computation began to read; changed_at: the
    // snapshot its value and error last changed at, kept by a computation that returns the same
    {"measurements",
	 "dataset VARCHAR NOT NULL, function VARCHAR NOT NULL, subject VARCHAR NOT NULL, aspect VARCHAR NOT NULL, "
	 "value VARCHAR, computed_at TIMESTAMP NOT NULL, error VARCHAR, read_at BIGINT NOT NULL, changed_at BIGINT NOT "
	 "NULL",
	 "begin_snapshot, dataset, function, subject, aspect, value::JSON AS value, computed_at, error, read_at, "
	 "changed_at"},
    // one row per node a stored result read (SPEC §6): the rows begin with their result, whose
    // begin_snapshot they share, and end with it
    {"inputs", "asset VARCHAR NOT NULL, what VARCHAR NOT NULL", "begin_snapshot, asset, what"},
    // one row per lineage edge a stored result holds, a view's lineage the relations and columns it
    // reads: the rows begin and end with their result, whose begin_snapshot they share
    {"edges", "\"from\" VARCHAR NOT NULL, \"to\" VARCHAR NOT NULL, kind VARCHAR NOT NULL",
	 "begin_snapshot, \"from\", \"to\", kind"},
    {"tests",
	 "name VARCHAR NOT NULL, aspect VARCHAR NOT NULL, actors VARCHAR NOT NULL, detector VARCHAR, threshold DOUBLE, "
	 "declared_at TIMESTAMP NOT NULL, declared_by VARCHAR",
	 "begin_snapshot, name, aspect, from_json(actors, '[\"VARCHAR\"]') AS actors, detector, threshold, declared_at, "
	 "declared_by"},
};

//! A relation's current rows: those no write has ended
static string Newest(const Workspace &ws, const string &relation) {
	return "(SELECT * FROM " + ws.Store(relation) + " WHERE end_snapshot IS NULL)";
}

static string AnnotationsOverSQL(const Workspace &ws, const string &filter);

struct InitState : public GlobalTableFunctionState {
	bool done = false;
};

static unique_ptr<FunctionData> InitBind(ClientContext &context, TableFunctionBindInput &input,
                                         vector<LogicalType> &return_types, vector<Identifier> &names) {
	return_types.push_back(LogicalType::VARCHAR);
	names.emplace_back("outcome");
	return nullptr;
}

static unique_ptr<GlobalTableFunctionState> InitInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<InitState>();
}

//! glossql_init(): the store tables where missing, the lake's views over
//! them, and the lineage kit, whose read functions are the language's (SPEC
//! §6), where the workspace does not hold its text. A view names the metadata
//! catalog, so a view made under another alias is made again
static void InitExec(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<InitState>();
	if (state.done) {
		return;
	}
	state.done = true;
	Inner inner(context);
	auto &ws = inner.ws;
	for (auto &relation : STORE_RELATIONS) {
		inner.Run("CREATE TABLE IF NOT EXISTS " + ws.Store(relation.name) +
		          " (begin_snapshot BIGINT NOT NULL, end_snapshot BIGINT, " + relation.columns + ")");
	}
	idx_t views = 0;
	// a view's text names the metadata catalog, quoted or bare as the lake re-serializes it:
	// a view made under another alias is made again
	auto view = [&](const string &name, const string &sql) {
		auto current =
		    inner.Scalar("SELECT count(*) FROM duckdb_views() WHERE database_name = " + QuoteText(ws.lake) +
			             " AND schema_name = 'main' AND view_name = " + QuoteText(name) + " AND (contains(sql, " +
			             QuoteText(ws.meta + ".") + ") OR contains(sql, " + QuoteText(QuoteName(ws.meta) + ".") + "))");
		if (current.GetValue<int64_t>() > 0) {
			return;
		}
		inner.Run("CREATE OR REPLACE VIEW " + QuoteName(ws.lake) + ".main." + QuoteName(name) + " AS " + sql);
		views++;
	};
	for (auto &relation : STORE_RELATIONS) {
		auto columns = "SELECT " + string(relation.view);
		view(relation.name, columns + " FROM " + ws.Store(relation.name) + " WHERE end_snapshot IS NULL");
		// a view over every column names them all, the end among them
		auto every =
		    string(relation.view) == "* EXCLUDE (end_snapshot)" ? string("SELECT *") : columns + ", end_snapshot";
		view(string(relation.name) + "_history", every + " FROM " + ws.Store(relation.name));
	}
	RunKit(context, "lineage", "glossql", LINEAGE_KIT, true);
	output.data[0].Append(Value("workspace " + ws.lake + ": the store is " + ws.meta + "." + ws.meta_schema +
	                            ".glossql_*, read as the views of " + ws.lake + ".main (" + to_string(views) +
	                            " views made)"));
}

static TableFunction GlossInitFunction() {
	return TableFunction("glossql_init", {}, InitExec, InitBind, InitInit);
}

//! PRAGMA glossql: the one line a connection needs. The grammar becomes active
//! for this connection, beside any grammar already active, and the store is
//! made where missing
static string GlossQuery(ClientContext &context, const FunctionParameters &parameters) {
	auto active = ClientConfig::GetConfig(context).active_grammar_extensions;
	if (std::find(active.begin(), active.end(), "glossql") == active.end()) {
		active.push_back("glossql");
		vector<Value> names;
		for (auto &name : active) {
			names.emplace_back(name);
		}
		ActiveGrammarExtensionsSetting::SetLocal(context, Value::LIST(LogicalType::VARCHAR, names));
	}
	return "FROM glossql_init()";
}

//===--------------------------------------------------------------------===//
// glossql_write(table, values...): appends one row to a store relation, in
// the statement's own transaction. The actor rides the connection (SPEC §1):
// SET VARIABLE glossql_actor = 'human' is the kind, which supersession keys
// on; SET VARIABLE glossql_actor_id = 'who' is the writer, for the catalog
//===--------------------------------------------------------------------===//

struct GlossWriteBindData : public FunctionData {
	string table;
	vector<Value> row;

	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<GlossWriteBindData>();
		result->table = table;
		result->row = row;
		return result;
	}
	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<GlossWriteBindData>();
		return table == other.table && row == other.row;
	}
};

struct GlossWriteState : public GlobalTableFunctionState {
	bool done = false;
};

static unique_ptr<FunctionData> GlossWriteBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result = make_uniq<GlossWriteBindData>();
	result->table = input.inputs[0].ToString();
	for (idx_t i = 1; i < input.inputs.size(); i++) {
		result->row.push_back(input.inputs[i]);
	}
	return_types.push_back(LogicalType::VARCHAR);
	names.emplace_back("outcome");
	return result;
}

static unique_ptr<GlobalTableFunctionState> GlossWriteInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<GlossWriteState>();
}

static Value Variable(ClientContext &context, const char *name) {
	auto &variables = ClientConfig::GetConfig(context).user_variables;
	auto entry = variables.find(Identifier(name));
	if (entry == variables.end() || entry->second.IsNull()) {
		return Value(LogicalType::VARCHAR);
	}
	return Value(entry->second.ToString());
}

//! The actor's kind: agent unless the wrapper said otherwise
static string ActorKindOf(ClientContext &context) {
	auto kind = Variable(context, "glossql_actor");
	return kind.IsNull() ? string("agent") : kind.ToString();
}

//! Who the actor is, NULL when the wrapper set none
static Value ActorIdOf(ClientContext &context) {
	return Variable(context, "glossql_actor_id");
}

//! Who declares, for the declaration relations' declared_by: the kit while
//! glossql_kit runs one (glossql_declared_by on its connection), else the
//! actor as kind:id, or the kind alone where the wrapper set no id
string DeclaredBy(ClientContext &context) {
	auto kit = Variable(context, "glossql_declared_by");
	if (!kit.IsNull()) {
		return kit.ToString();
	}
	auto id = ActorIdOf(context);
	return ActorKindOf(context) + (id.IsNull() ? string() : ":" + id.ToString());
}

//! An aspect as its newest row holds
struct AspectRow {
	string name;
	string schema;
	string kind;
	vector<string> grains;
	//! the x-kind the schema carries: 'definition' marks a kit's metric definition schema
	string flavor;
};

static AspectRow LookupAspect(Inner &inner, const string &name) {
	auto row = inner.Row("SELECT schema, kind, from_json(grains, '[\"VARCHAR\"]'), schema::JSON->>'x-kind' FROM " +
	                     inner.Store("aspects") + " WHERE name = " + QuoteText(name) + " AND end_snapshot IS NULL");
	if (row.empty()) {
		throw InvalidInputException("glossql: aspect '%s' is not declared", name);
	}
	AspectRow result;
	result.name = name;
	result.schema = row[0].ToString();
	result.kind = row[1].ToString();
	for (auto &grain : ListValue::GetChildren(row[2])) {
		result.grains.push_back(grain.ToString());
	}
	result.flavor = row[3].IsNull() ? "" : row[3].ToString();
	return result;
}

//! The metric definition schemas a kit declared: every aspect flavored `x-kind:
//! metric definition`, newest declaration per name. A VIEW body meets each of them
//! after the metric definition contract; a workspace without one runs on the contract
static vector<pair<string, string>> DefinitionSchemas(Inner &inner) {
	vector<pair<string, string>> result;
	auto rows = inner.Run("SELECT name, schema FROM " + inner.Current("aspects") +
	                      " WHERE schema::JSON->>'x-kind' = 'definition' ORDER BY name");
	while (auto chunk = rows->Fetch()) {
		for (idx_t i = 0; i < chunk->size(); i++) {
			result.emplace_back(chunk->GetValue(0, i).ToString(), chunk->GetValue(1, i).ToString());
		}
	}
	return result;
}

static bool HasGrain(const AspectRow &aspect, const string &grain) {
	return std::find(aspect.grains.begin(), aspect.grains.end(), grain) != aspect.grains.end();
}

//! Admission of a gloss (SPEC §5.2, §7.1): the aspect is declared and not a
//! MEASUREMENT; the body conforms to the aspect's schema (FACT) or the
//! metric definition contract and the kit's metric definition schemas (VIEW); a TABLE or COLUMN grain takes stored
//! subjects only; a test's BY admits the actor's kind
static AspectRow AdmitGloss(ClientContext &context, Inner &inner, const string &aspect_name, const string &subject,
                            const string &body, const string &actor) {
	auto aspect = LookupAspect(inner, aspect_name);
	if (aspect.kind == "MEASUREMENT") {
		throw InvalidInputException("glossql: aspect '%s' is a MEASUREMENT: never glossed, its value is its function's",
		                            aspect_name);
	}
	if (aspect.flavor == "definition") {
		throw InvalidInputException(
		    "glossql: aspect '%s' is a definition schema: never glossed, it validates every VIEW gloss", aspect_name);
	}
	// A VIEW body meets the metric definition contract (sql or stopped, assumptions
	// disclosed), then every metric definition schema a kit declared; a FACT body meets
	// the aspect's own WITH schema
	if (aspect.kind == "VIEW") {
		auto error = JsonSchemaCheck(DEFINITION_CONTRACT, body);
		if (!error.empty()) {
			throw InvalidInputException("glossql: the body of %s does not meet the definition contract: %s",
			                            aspect_name, error);
		}
		for (auto &schema : DefinitionSchemas(inner)) {
			error = JsonSchemaCheck(schema.second, body);
			if (!error.empty()) {
				throw InvalidInputException("glossql: the body of %s does not conform to the definition schema %s: %s",
				                            aspect_name, schema.first, error);
			}
		}
	} else {
		auto error = JsonSchemaCheck(aspect.schema, body);
		if (!error.empty()) {
			throw InvalidInputException("glossql: the body of %s does not conform to the aspect's schema: %s",
			                            aspect_name, error);
		}
	}
	auto parts = SubjectParts(subject);
	bool pair = subject.find(" -> ") != string::npos || subject.find(" <-> ") != string::npos;
	if (aspect.kind == "VIEW" && (parts.size() != 1 || pair)) {
		throw InvalidInputException("glossql: a VIEW aspect takes the dataset as its subject, not %s", subject);
	}
	// the grain gate (SPEC §5.1): the subject's class is one the aspect declared. A
	// bare name is a source when one is declared by it, a dataset when a schema is,
	// and either when both are: the declaration says which is meant
	if (!aspect.grains.empty()) {
		bool is_source =
		    !pair && parts.size() == 1 &&
		    inner.Scalar("SELECT count(*) FROM " + inner.Current("sources") + " WHERE name = " + QuoteText(subject))
		            .GetValue<int64_t>() > 0;
		string grain = pair                ? "RELATIONSHIP"
		               : parts.size() >= 3 ? "COLUMN"
		               : parts.size() == 2 ? "TABLE"
		               : is_source         ? "SOURCE"
		                                   : "DATASET";
		// an asset of a kind a kit declares is of that kind, which no grain names
		if (!pair && parts.size() == 2 && !KindNameTaken(inner, parts[0], parts[1]).empty()) {
			grain = StringUtil::Upper(inner
			                              .Scalar("SELECT max(kind) FROM glossql_kind_assets(" + QuoteText(parts[0]) +
			                                      ") WHERE lower(asset) = lower(" + QuoteText(subject) + ")")
			                              .ToString());
		}
		bool admitted =
		    HasGrain(aspect, grain) || (is_source && SchemaExists(context, subject) && HasGrain(aspect, "DATASET"));
		if (!admitted) {
			throw InvalidInputException("glossql: aspect '%s' is declared ON %s; %s is a %s", aspect_name,
			                            StringUtil::Join(aspect.grains, ", "), subject, StringUtil::Lower(grain));
		}
	}
	if (!pair && parts.size() == 2 && HasGrain(aspect, "TABLE") && !RelationEntry(context, parts[0], parts[1])) {
		throw InvalidInputException("glossql: %s names no table of %s", subject, parts[0]);
	}
	if (!pair && parts.size() == 3 && HasGrain(aspect, "COLUMN")) {
		auto entry = RelationEntry(context, parts[0], parts[1]);
		if (!entry || ColumnOf(context, *entry, parts[2]).empty()) {
			throw InvalidInputException("glossql: %s names no column of %s.%s", subject, parts[0], parts[1]);
		}
	}
	// only declared relationships exist (SPEC §4): a pair path is a subject once declared
	if (pair && HasGrain(aspect, "RELATIONSHIP")) {
		auto declared = inner.Scalar("SELECT count(*) FROM " + inner.Current("relationships") +
		                             " WHERE left_side || ' ' || op || ' ' || right_side = " + QuoteText(subject));
		if (declared.GetValue<int64_t>() == 0) {
			throw InvalidInputException("glossql: %s is not a declared relationship", subject);
		}
	}
	auto actor_kinds = inner.Scalar("SELECT from_json(actors, '[\"VARCHAR\"]') FROM " + inner.Current("tests") +
	                                " WHERE aspect = " + QuoteText(aspect_name));
	if (actor_kinds.IsNull()) {
		return aspect;
	}
	auto admitted = ListValue::GetChildren(actor_kinds);
	if (admitted.empty()) {
		return aspect;
	}
	vector<string> names;
	for (auto &actor_kind : admitted) {
		names.push_back(actor_kind.ToString());
		if (StringUtil::Lower(actor_kind.ToString()) == actor) {
			return aspect;
		}
	}
	throw InvalidInputException("glossql: the test on '%s' admits %s, not %s", aspect_name,
	                            StringUtil::Join(names, ", "), actor);
}

//! A relationship endpoint is a column, or a column tuple, of a loaded table
//! (SPEC §4): naming a table or column the dataset does not have is refused
static void AdmitEndpoint(ClientContext &context, const string &endpoint) {
	auto parts = SubjectParts(endpoint);
	if (parts.size() != 3) {
		throw InvalidInputException("glossql: a relationship endpoint is [dataset.]table.key, not %s", endpoint);
	}
	auto entry = RelationEntry(context, parts[0], parts[1]);
	if (!entry) {
		throw InvalidInputException("glossql: %s names no table of %s", endpoint, parts[0]);
	}
	vector<string> columns;
	auto &key = parts[2];
	if (key.size() > 2 && key.front() == '(' && key.back() == ')') {
		columns = StringUtil::Split(key.substr(1, key.size() - 2), ", ");
	} else {
		columns.push_back(key);
	}
	for (auto &column : columns) {
		if (ColumnOf(context, *entry, column).empty()) {
			throw InvalidInputException("glossql: %s names no column %s of %s.%s", endpoint, column, parts[0],
			                            parts[1]);
		}
	}
}

//! A metric definition is a view in the catalog (SPEC §5.2): the dataset's schema holds
//! a view under the metric's name carrying the serving definition's sql. A
//! definition is refused before anything is written when its sql does not bind
//! or its name is a loaded table's. The view's lineage is planned there, as the
//! write will leave the lake, and stored with it (DefineView)
static ViewLineageResult AdmitDefinition(Inner &inner, const string &dataset, const string &metric,
                                         const string &body) {
	auto loaded =
	    inner.Scalar("SELECT count(*) FROM duckdb_tables() WHERE database_name = " + QuoteText(inner.ws.lake) +
		             " AND schema_name = " + QuoteText(dataset) + " AND table_name = " + QuoteText(metric));
	if (loaded.GetValue<int64_t>() > 0) {
		throw InvalidInputException("glossql: a definition on a loaded table's name is refused: %s.%s", dataset,
		                            metric);
	}
	ViewLineageResult lineage;
	auto sql = JsonStringField(body, "sql");
	if (sql.empty()) {
		return lineage;
	}
	// bound as the lake binds a view, in a transaction of the lake that is rolled back
	inner.Run("BEGIN");
	auto bound = inner.con.Query("CREATE OR REPLACE VIEW " + inner.Table(dataset, metric) + " AS " + sql);
	if (!bound->HasError()) {
		try {
			lineage = PlanViewLineage(inner, dataset, metric);
		} catch (std::exception &) {
			inner.con.Query("ROLLBACK");
			throw;
		}
	}
	inner.con.Query("ROLLBACK");
	if (bound->HasError()) {
		ErrorData error(bound->GetErrorObject());
		throw InvalidInputException("glossql: %s", error.Message());
	}
	return lineage;
}

//! The view of a metric follows the serving definition, a person's over an
//! agent's, a stop ending it: its DuckLake row is written in the catalog write
//! that wrote the definition, as DuckLake writes one, so the view and the
//! glossary are one commit. The same sql again keeps the view and its version:
//! the annotations and measurements on its columns stay current. The view's
//! lineage is stored in the same write where it was planned for the sql that
//! now serves (`admitted`), read at this write; a view ended takes its lineage
//! with it
static void DefineView(CatalogWrite &write, const string &dataset, const string &metric,
                       const ViewLineageResult &admitted, const string &admitted_sql) {
	auto &inner = write.inner;
	auto &ws = inner.ws;
	auto serving =
	    inner.Scalar("SELECT body FROM " + inner.Current("glossary") + " WHERE dataset = " + QuoteText(dataset) +
		             " AND subject = " + QuoteText(dataset) + " AND aspect = " + QuoteText(metric) +
		             " AND actor_kind IN ('human', 'agent')"
		             " ORDER BY CASE actor_kind WHEN 'human' THEN 0 ELSE 1 END LIMIT 1");
	auto sql = serving.IsNull() ? string() : JsonStringField(serving.ToString(), "sql");
	auto schema_id = inner.Scalar("SELECT schema_id FROM " + ws.Meta("schema") +
	                              " WHERE schema_name = " + QuoteText(dataset) + " AND end_snapshot IS NULL");
	if (schema_id.IsNull()) {
		throw InvalidInputException("glossql: dataset '%s' is no schema of the lake", dataset);
	}
	auto view = inner.Row("SELECT view_id, sql FROM " + ws.Meta("view") + " WHERE schema_id = " + schema_id.ToString() +
	                      " AND view_name = " + QuoteText(metric) + " AND end_snapshot IS NULL");
	// the lineage read the annotation that defines the view: written again, it is stored again, and the same
	// value keeps the snapshot it changed at
	auto lineage = !admitted.view.empty() && sql == admitted_sql;
	if (!view.empty() && view[1].ToString() == sql) {
		if (lineage) {
			StoreViewLineage(write, dataset, admitted, write.snapshot);
		}
		return;
	}
	if (!view.empty()) {
		write.Run("UPDATE " + ws.Meta("view") + " SET end_snapshot = " + to_string(write.snapshot) +
		          " WHERE view_id = " + view[0].ToString() + " AND end_snapshot IS NULL");
		write.Changed("dropped_view:" + view[0].ToString());
	}
	if (sql.empty()) {
		RetireViewLineage(write, dataset, dataset + "." + metric);
		return;
	}
	write.Run("INSERT INTO " + ws.Meta("view") + " VALUES (" + to_string(write.NewCatalogId()) + ", uuid(), " +
	          to_string(write.snapshot) + ", NULL, " + schema_id.ToString() + ", " + QuoteText(metric) +
	          ", 'duckdb', " + QuoteText(sql) + ", '')");
	write.Changed("created_view:" + QuoteName(dataset) + "." + QuoteName(metric));
	if (lineage) {
		StoreViewLineage(write, dataset, admitted, write.snapshot);
	}
}

//! Admission of an aspect (SPEC §5.1): identical content is a no-op, a change
//! under glosses is refused, a WHEN names a declared sibling whose enum holds
//! the literal. Returns false for the no-op.
static bool AdmitAspect(Inner &inner, const vector<Value> &row) {
	auto name = row[0].ToString();
	auto current = inner.Row("SELECT schema, kind, grains, condition FROM " + inner.Current("aspects") +
	                         " WHERE name = " + QuoteText(name));
	if (!current.empty()) {
		// compared as the store holds them: text, the grains as their JSON list
		vector<Value> incoming {row[1], row[2], Value(JsonList(row[3])), row[4]};
		bool same = true;
		for (idx_t i = 0; i < 4; i++) {
			auto &before = current[i];
			auto &after = incoming[i];
			same = same && (before.IsNull() || after.IsNull() ? before.IsNull() == after.IsNull()
			                                                  : before.ToString() == after.ToString());
		}
		if (same) {
			return false;
		}
		auto glossed =
		    inner.Scalar("SELECT count(*) FROM " + inner.Current("glossary") + " WHERE aspect = " + QuoteText(name));
		if (glossed.GetValue<int64_t>() > 0) {
			throw InvalidInputException("glossql: aspect '%s' has glosses: delete them before changing it", name);
		}
	}
	if (JsonStringField(row[1].ToString(), "x-kind") == "definition") {
		// existing bodies never silently stop matching their schema (SPEC §5.1):
		// every current VIEW value meets the schema before it is declared
		auto rows = inner.Run("SELECT aspect, subject, body FROM " + inner.Current("glossary") +
		                      " WHERE aspect IN (SELECT name FROM " + inner.Current("aspects") +
		                      " WHERE kind = 'VIEW') ORDER BY begin_snapshot");
		while (auto chunk = rows->Fetch()) {
			for (idx_t i = 0; i < chunk->size(); i++) {
				auto error = JsonSchemaCheck(row[1].ToString(), chunk->GetValue(2, i).ToString());
				if (!error.empty()) {
					throw InvalidInputException("glossql: definition schema '%s' would strand the %s gloss on %s: %s",
					                            name, chunk->GetValue(0, i).ToString(),
					                            chunk->GetValue(1, i).ToString(), error);
				}
			}
		}
	}
	if (!row[4].IsNull()) {
		auto condition = row[4].ToString();
		if (JsonStringField(condition, "aspect").empty()) {
			// WHEN TYPE narrows columns by their data type: the grain is COLUMN alone
			if (JsonList(row[3]) != "[\"COLUMN\"]") {
				throw InvalidInputException("glossql: WHEN TYPE narrows columns; aspect '%s' is declared ON %s", name,
				                            JsonList(row[3]));
			}
		} else {
			auto sibling = LookupAspect(inner, JsonStringField(condition, "aspect"));
			auto literal = JsonStringField(condition, "value");
			auto error = JsonSchemaValueEnumCheck(sibling.schema, literal);
			if (!error.empty()) {
				throw InvalidInputException("glossql: WHEN %s = %s: %s", sibling.name, literal, error);
			}
		}
	}
	return true;
}

string KindNameTaken(Inner &inner, const string &dataset, const string &relation) {
	auto d = QuoteText(dataset);
	auto named =
	    relation.empty()
	        ? "lower(split_part(k.asset, '.', 2)) IN (SELECT lower(table_name) FROM glossql_columns(" + d + "))"
	        : "lower(k.asset) = lower(" + QuoteText(dataset + "." + relation) + ")";
	auto taken = inner.Scalar("SELECT min(k.asset) FROM glossql_kind_assets(" + d +
	                          ") k WHERE k.node_what <> 'definition' AND " + named);
	return taken.IsNull() ? string() : taken.ToString();
}

//! A source-grain row: its subject names a declared source and its aspect
//! declares ON SOURCE. Such a row reads and supersedes workspace-wide (SPEC
//! §5.1); a grainless aspect on a source name stays dataset-scoped
static string SourceGrainSQL(const Workspace &ws) {
	return "(subject IN (SELECT name FROM " + ws.Store("sources") + ") AND aspect IN (SELECT name FROM " +
	       Newest(ws, "aspects") + " WHERE list_contains(from_json(grains, '[\"VARCHAR\"]'), 'SOURCE')))";
}

static void GlossWriteExec(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<GlossWriteState>();
	if (state.done) {
		return;
	}
	state.done = true;
	auto &bind = data.bind_data->Cast<GlossWriteBindData>();

	// a JSON body or schema is admitted through the json extension's cast, which refuses malformed text
	auto json = [&](const Value &value) {
		return value.CastAs(context, LogicalType::JSON()).ToString();
	};
	auto subject = [&](const Value &value) {
		return ResolveSubject(context, value.ToString());
	};

	Inner inner(context);
	// a definition is bound before the write, and its lineage planned: the lake binds it, and the write
	// touches the catalogue alone
	ViewLineageResult admitted;
	string admitted_sql;
	if (bind.table == "glossary") {
		auto resolved = subject(bind.row[0]);
		auto kind =
		    inner.Scalar("SELECT kind FROM " + inner.Current("aspects") + " WHERE name = " + bind.row[1].ToSQLString());
		if (!kind.IsNull() && kind.ToString() == "VIEW" && resolved.find('.') == string::npos) {
			admitted = AdmitDefinition(inner, resolved, bind.row[1].ToString(), json(bind.row[2]));
			admitted_sql = JsonStringField(json(bind.row[2]), "sql");
		}
	}
	string outcome;
	inner.Write(context, [&](CatalogWrite &write) {
		outcome.clear();
		if (bind.table == "aspects") {
			if (!AdmitAspect(inner, bind.row)) {
				outcome = "aspect " + bind.row[0].ToString() + " unchanged: no-op";
				return;
			}
			// (name, schema, kind, grains, condition) from the statement; the schema last in the row, being the long
			// one
			write.End("aspects", "name = " + bind.row[0].ToSQLString());
			write.Insert("aspects", bind.row[0].ToSQLString() + ", " + bind.row[2].ToSQLString() + ", " +
			                            QuoteText(JsonList(bind.row[3])) + ", " + bind.row[4].ToSQLString() + ", " +
			                            QuoteText(json(bind.row[1])) + ", now(), " + QuoteText(DeclaredBy(context)));
			outcome = "declared aspect " + bind.row[0].ToString();
		} else if (bind.table == "sources") {
			// identity is content (SPEC §3): an unchanged declaration is a no-op, so a script replays silently
			auto current = inner.Row("SELECT settings FROM " + inner.Current("sources") +
			                         " WHERE name = " + bind.row[0].ToSQLString());
			if (!current.empty() && current[0].ToString() == json(bind.row[1])) {
				outcome = "source " + bind.row[0].ToString() + " unchanged: no-op";
				return;
			}
			write.End("sources", "name = " + bind.row[0].ToSQLString());
			write.Insert("sources", bind.row[0].ToSQLString() + ", " + QuoteText(json(bind.row[1])) + ", now(), " +
			                            QuoteText(DeclaredBy(context)));
			outcome = "declared source " + bind.row[0].ToString();
		} else if (bind.table == "relationships") {
			auto left = subject(bind.row[0]);
			auto right = subject(bind.row[2]);
			auto spelled = left + " " + bind.row[1].ToString() + " " + right;
			auto current = inner.Scalar(
			    "SELECT count(*) FROM " + inner.Current("relationships") + " WHERE left_side = " + QuoteText(left) +
			    " AND op = " + bind.row[1].ToSQLString() + " AND right_side = " + QuoteText(right));
			if (current.GetValue<int64_t>() > 0) {
				outcome = "relationship " + spelled + " unchanged: no-op";
				return;
			}
			AdmitEndpoint(context, left);
			AdmitEndpoint(context, right);
			write.Insert("relationships", QuoteText(DatasetOf(left)) + ", " + QuoteText(left) + ", " +
			                                  bind.row[1].ToSQLString() + ", " + QuoteText(right) + ", now(), " +
			                                  QuoteText(DeclaredBy(context)));
			outcome = "declared relationship " + spelled;
		} else if (bind.table == "glossary") {
			// (subject, aspect) from the statement, the actor from the connection, then the body
			auto actor = ActorKindOf(context);
			auto resolved = subject(bind.row[0]);
			auto body = json(bind.row[2]);
			auto aspect = AdmitGloss(context, inner, bind.row[1].ToString(), resolved, body, actor);
			auto dataset = DatasetOf(context, resolved);
			// a value is superseded per (subject, aspect, actor kind) (SPEC §5.2), within its dataset;
			// a source-grain row is superseded workspace-wide (SPEC §5.1)
			auto source_grain =
			    inner.Scalar("SELECT " + SourceGrainSQL(inner.ws) + " FROM (SELECT " + QuoteText(resolved) +
				             " AS subject, " + QuoteText(aspect.name) + " AS aspect)");
			auto held = "subject = " + QuoteText(resolved) + " AND aspect = " + QuoteText(aspect.name) +
			            " AND actor_kind = " + QuoteText(actor) +
			            (source_grain.GetValue<bool>() ? string() : " AND dataset = " + QuoteText(dataset));
			// the same body by the same actor on an annotation that is not stale writes nothing
			auto same = inner.Scalar("SELECT g.body = " + QuoteText(body) + " AND g.actor_id IS NOT DISTINCT FROM " +
			                         ActorIdOf(context).ToSQLString() + " AND NOT a.stale FROM (SELECT * FROM " +
			                         inner.Current("glossary") + " WHERE " + held + ") g JOIN (" +
			                         AnnotationsOverSQL(inner.ws, held) + ") a ON a.ord = g.begin_snapshot");
			if (!same.IsNull() && same.GetValue<bool>()) {
				outcome = "gloss " + bind.row[1].ToString() + " on " + resolved + " unchanged: no-op";
				return;
			}
			write.End("glossary", held);
			write.Insert("glossary", QuoteText(dataset) + ", " + QuoteText(resolved) + ", " + QuoteText(aspect.name) +
			                             ", " + QuoteText(actor) + ", " + ActorIdOf(context).ToSQLString() + ", " +
			                             QuoteText(body) + ", now()");
			if (aspect.kind == "VIEW") {
				DefineView(write, resolved, aspect.name, admitted, admitted_sql);
			}
			// an asset of a kind a kit declares and a relation of its dataset do not share a name
			auto taken = KindNameTaken(inner, dataset, "");
			if (!taken.empty()) {
				throw InvalidInputException("glossql: asset_name_taken: %s names a relation of %s and an asset a kit "
				                            "declares; name one otherwise",
				                            taken, dataset);
			}
			outcome = "glossed " + bind.row[1].ToString() + " on " + resolved + " as " + actor;
		} else {
			throw InternalException("glossql: no write to '%s'", bind.table);
		}
	});
	output.data[0].Append(Value(outcome));
}

//! glossql_write(table, v1, ..., vN): one overload per store relation width
TableFunction GlossWriteFunction(idx_t width) {
	vector<LogicalType> arguments {LogicalType::VARCHAR};
	for (idx_t i = 0; i < width; i++) {
		arguments.push_back(LogicalType::ANY);
	}
	return TableFunction("glossql_write", arguments, GlossWriteExec, GlossWriteBind, GlossWriteInit);
}

static TableFunctionSet GlossWriteFunctions() {
	TableFunctionSet set("glossql_write");
	for (idx_t width = 2; width <= 6; width++) {
		set.AddFunction(GlossWriteFunction(width));
	}
	return set;
}

//===--------------------------------------------------------------------===//
// DELETE FROM [lake.][main.]glossary [alias] [WHERE ...]: SPEC §5.2's
// removal. The lake reads the store's current rows as views, and a removal
// ends the rows it selects: they leave the views and stay in the store's
// table, as a row DuckLake removes stays in its own. Its result is a
// DELETE's: the rows removed
//===--------------------------------------------------------------------===//

struct GlossDeleteBindData : public FunctionData {
	//! the lake named, the alias and the WHERE as SQL text, each NULL for none
	Value lake;
	Value alias;
	Value where;

	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<GlossDeleteBindData>();
		result->lake = lake;
		result->alias = alias;
		result->where = where;
		return result;
	}
	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<GlossDeleteBindData>();
		return lake == other.lake && alias == other.alias && where == other.where;
	}
};

static unique_ptr<FunctionData> GlossDeleteBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result = make_uniq<GlossDeleteBindData>();
	result->lake = input.inputs[0];
	result->alias = input.inputs[1];
	result->where = input.inputs[2];
	return_types.push_back(LogicalType::BIGINT);
	names.emplace_back("Count");
	return result;
}

static void GlossDeleteExec(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<GlossWriteState>();
	if (state.done) {
		return;
	}
	state.done = true;
	auto &bind = data.bind_data->Cast<GlossDeleteBindData>();
	Inner inner(context);
	// the lake named is this connection's workspace, whose store it is
	if (!bind.lake.IsNull() && !StringUtil::CIEquals(bind.lake.ToString(), inner.ws.lake)) {
		throw InvalidInputException("glossql: the workspace is %s, not %s", inner.ws.lake, bind.lake.ToString());
	}
	// the table carries the view's name as its alias, so a WHERE written against the view binds
	auto alias = bind.alias.IsNull() ? string("glossary") : bind.alias.ToString();
	auto where = bind.where.IsNull() ? string() : " AND (" + bind.where.ToString() + ")";
	int64_t count = 0;
	inner.Write(context, [&](CatalogWrite &write) {
		// a WHERE the store refuses is the statement's refusal, never a lost race
		count = inner
		            .Scalar("SELECT count(*) FROM " + inner.Store("glossary") + " AS " + QuoteName(alias) + " WHERE " +
		                    QuoteName(alias) + ".end_snapshot IS NULL" + where)
		            .GetValue<int64_t>();
		if (count == 0) {
			return;
		}
		write.Run("UPDATE " + inner.Store("glossary") + " AS " + QuoteName(alias) + " SET end_snapshot = " +
		          to_string(write.snapshot) + " WHERE " + QuoteName(alias) + ".end_snapshot IS NULL" + where);
	});
	output.data[0].Append(Value::BIGINT(count));
}

TableFunction GlossDeleteFunction() {
	return TableFunction("glossql_delete", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                     GlossDeleteExec, GlossDeleteBind, GlossWriteInit);
}

//===--------------------------------------------------------------------===//
// The plan: every glossql statement is one scan of its table function
//===--------------------------------------------------------------------===//

static ParserExtensionPlanResult GlossPlan(ParserExtensionInfo *info, ClientContext &context,
                                           unique_ptr<ParserExtensionParseData> parse_data) {
	auto &data = parse_data->Cast<GlossStatementData>();
	ParserExtensionPlanResult result;
	bool modifies = true;
	auto modification = DatabaseModificationType::INSERT_DATA;
	result.return_type = StatementReturnType::QUERY_RESULT;
	if (data.kind == "write") {
		result.function = GlossWriteFunction(data.row.size());
		result.parameters.push_back(Value(data.table));
	} else if (data.kind == "delete") {
		// a DELETE's statement: its one row is the count a client reads as the rows changed
		result.function = GlossDeleteFunction();
		modification = DatabaseModificationType::DELETE_DATA;
		result.return_type = StatementReturnType::CHANGED_ROWS;
	} else if (data.kind == "dataset") {
		result.function = GlossDatasetFunction();
	} else if (data.kind == "recipe") {
		result.function = GlossRecipeFunction();
	} else if (data.kind == "import") {
		result.function = GlossImportFunction();
	} else if (data.kind == "preview") {
		result.function = GlossPreviewFunction();
		modifies = false;
	} else if (data.kind == "function") {
		result.function = GlossFunctionFunction();
	} else if (data.kind == "test") {
		result.function = GlossTestFunction();
	} else {
		throw InternalException("glossql: unknown statement kind '%s'", data.kind);
	}
	for (auto &value : data.row) {
		result.parameters.push_back(value);
	}
	result.requires_valid_transaction = true;
	if (modifies) {
		StatementProperties properties;
		properties.RegisterDBModify(Catalog::GetCatalog(context, Identifier()), context, modification);
		result.modified_databases = properties.modified_databases;
	}
	return result;
}

ParserExtension GlossParserExtension() {
	ParserExtension extension;
	extension.plan_function = GlossPlan;
	return extension;
}

//===--------------------------------------------------------------------===//
// The reads: GLOSSARY() and TESTS(), planned as subqueries over the store
// (bind_replace), so they compose with any SQL. A dataset's rows — the values
// with their staleness, the detectors' results, the collapse — are computed
// on the second connection from the tests and detectors as they are now,
// once per store version, and kept by the instance; a bind serves them from
// there, narrowed to its subject and aspect in SQL. The stale rule is planned
// once per version, not once per bind.
//===--------------------------------------------------------------------===//

struct ReadScope {
	//! the dataset the read runs in
	string dataset;
	//! the scope is that whole dataset: the sweep
	bool whole = false;
	//! the rows in scope, over columns dataset, subject and aspect
	string filter;
	//! the same over subject alone
	string subjects;
	Workspace ws;
};

//! A read's scope, resolved from the catalog alone: the dataset, whether the
//! read sweeps it, and the subject filter a narrowed read applies. The rows'
//! filter and the workspace come with WholeOf, where a connection is at hand
static ReadScope ScopeOf(ClientContext &context, const Value &subject) {
	auto scope = subject.IsNull() ? CurrentDataset(context) : ResolveSubject(context, subject.ToString());
	ReadScope result;
	result.dataset = DatasetOf(context, scope);
	result.whole = scope == result.dataset;
	// a subject serves itself and what lies under it; no subject sweeps the USE'd dataset (SPEC §5.3)
	result.subjects = "(subject = " + QuoteText(scope) + " OR subject LIKE " + QuoteText(scope + ".%") + ")";
	return result;
}

static string AspectFilter(const Value &aspect) {
	return aspect.IsNull() ? string() : " AND aspect = " + QuoteText(aspect.ToString());
}

//! The lake's column history: one row per column version, by schema and table
//! name, live from begin_snapshot to end_snapshot, the end exclusive
static string ColumnsSQL(const Workspace &ws) {
	return "SELECT s.schema_name, t.table_name, c.column_name, c.column_type, c.begin_snapshot, c.end_snapshot FROM " +
	       ws.Meta("column") + " c JOIN " + ws.Meta("table") +
	       " t ON t.table_id = c.table_id AND c.begin_snapshot >= t.begin_snapshot"
	       " AND (t.end_snapshot IS NULL OR c.begin_snapshot < t.end_snapshot) JOIN " +
	       ws.Meta("schema") + " s ON s.schema_id = t.schema_id WHERE c.parent_column IS NULL";
}

//! The lake's view history: one row per definition, by schema and view name,
//! live from begin_snapshot to end_snapshot, the end exclusive
static string ViewsSQL(const Workspace &ws) {
	return "SELECT s.schema_name, v.view_name, v.begin_snapshot, v.end_snapshot FROM " + ws.Meta("view") + " v JOIN " +
	       ws.Meta("schema") + " s ON s.schema_id = v.schema_id";
}

//! The loads, for the re-derivation rule: each with the lineage it recorded
static string LoadsSQL(const Workspace &ws) {
	return "SELECT dataset, table_name, lineage, snapshot_id, begin_snapshot FROM " + ws.Store("imports");
}

//===--------------------------------------------------------------------===//
// The check (SPEC §6): the lake's snapshot id is the only version. A node a
// result read changed at the latest snapshot one of its rows begins or ends
// at, and rows are ended, never deleted; a result is out of date when a node
// it read changed after the snapshot it read at
//===--------------------------------------------------------------------===//

//! When each node a check asks about changed, (asset, what, dataset, function, changed_at), as
//! docs/specification/tables/inputs.md names the nodes. `wanted` names a relation (asset, what) in
//! scope: only those nodes are read, each from the rows of its own. dataset and function are NULL but for
//! a stored result: a result read is the reader's dataset's, and a function's own results are no input
//! of its own. A name is matched as the lake matches names, whatever its case
static string NodeChangesSQL(const Workspace &ws, const string &wanted) {
	string sql = R"(WITH nc_want AS (SELECT DISTINCT lower(asset) AS asset, what FROM $wanted),
nc_names AS (SELECT DISTINCT asset FROM nc_want),
nc_schema AS (SELECT DISTINCT schema_id, schema_name FROM $schema),
nc_tables AS (SELECT lower(s.schema_name || '.' || t.table_name) AS asset, t.table_id, t.begin_snapshot, t.end_snapshot
              FROM $table t JOIN nc_schema s USING (schema_id)
              WHERE lower(s.schema_name || '.' || t.table_name) IN (SELECT asset FROM nc_names)
                 OR lower(s.schema_name) IN (SELECT asset FROM nc_want WHERE what = 'assets')),
nc_views AS (SELECT lower(s.schema_name || '.' || v.view_name) AS asset, lower(s.schema_name) AS dataset, v.view_name,
                    v.begin_snapshot, v.end_snapshot
             FROM $view v JOIN nc_schema s USING (schema_id)
             WHERE lower(s.schema_name || '.' || v.view_name) IN (SELECT asset FROM nc_names)
                OR lower(s.schema_name) IN (SELECT asset FROM nc_names)),
-- a view's definition: the annotation that defines it, its row in the lake, its property's row
nc_defined AS (SELECT lower(subject || '.' || aspect) AS asset, begin_snapshot, end_snapshot FROM $glossary
               WHERE NOT contains(subject, '.') AND lower(subject || '.' || aspect) IN (SELECT asset FROM nc_names)
                 AND aspect IN (SELECT name FROM $aspects WHERE kind = 'VIEW')),
-- a table's data changed at a snapshot whose change text names its id
nc_log AS (SELECT snapshot_id, unnest(regexp_extract_all(changes_made, '[a-z_]*(insert|delete|alter)[a-z_]*:([0-9]+)', 2))::BIGINT
                  AS table_id
           FROM $snapshot_changes WHERE changes_made IS NOT NULL),
nc AS (
  SELECT lower(subject) AS asset, 'annotation ' || aspect AS what, NULL AS dataset, NULL AS function,
         greatest(begin_snapshot, coalesce(end_snapshot, 0)) AS changed_at FROM $glossary
  WHERE lower(subject) IN (SELECT asset FROM nc_names)
  -- a stored result: the current row's changed_at, else the last row's end
  UNION ALL SELECT lower(subject), w, dataset, function,
         coalesce(max(changed_at) FILTER (WHERE end_snapshot IS NULL), max(end_snapshot))
  FROM $measurements, unnest(['result ' || function, 'value ' || aspect]) u(w)
  WHERE lower(subject) IN (SELECT asset FROM nc_names) GROUP BY ALL
  UNION ALL SELECT asset, 'definition', NULL, NULL, greatest(begin_snapshot, coalesce(end_snapshot, 0)) FROM nc_tables
  UNION ALL SELECT t.asset, 'definition', NULL, NULL, greatest(c.begin_snapshot, coalesce(c.end_snapshot, 0))
  FROM nc_tables t JOIN $column c ON c.table_id = t.table_id
  -- a load whose recipe derived the columns otherwise than the load before it
  UNION ALL SELECT asset, 'definition', NULL, NULL, snapshot_id FROM (
    SELECT lower(dataset || '.' || table_name) AS asset, snapshot_id, lineage,
           lag(lineage) OVER (PARTITION BY dataset, table_name ORDER BY begin_snapshot) AS before FROM $imports
    WHERE lower(dataset || '.' || table_name) IN (SELECT asset FROM nc_names))
  WHERE before IS DISTINCT FROM lineage AND before IS NOT NULL
  UNION ALL SELECT asset, 'definition', NULL, NULL, greatest(begin_snapshot, coalesce(end_snapshot, 0))
  FROM (SELECT asset, begin_snapshot, end_snapshot FROM nc_views UNION ALL FROM nc_defined)
  UNION ALL SELECT v.asset, 'definition', NULL, NULL, greatest(a.begin_snapshot, coalesce(a.end_snapshot, 0))
  FROM (SELECT asset FROM nc_views UNION SELECT asset FROM nc_defined) v
  JOIN $aspects a ON lower(a.name) = substr(v.asset, strpos(v.asset, '.') + 1)
  UNION ALL SELECT lower(dataset || '.' || table_name), 'data', NULL, NULL, snapshot_id FROM $imports
  WHERE lower(dataset || '.' || table_name) IN (SELECT asset FROM nc_names)
  UNION ALL SELECT asset, 'data', NULL, NULL, greatest(begin_snapshot, coalesce(end_snapshot, 0)) FROM nc_tables
  UNION ALL SELECT t.asset, 'data', NULL, NULL, l.snapshot_id FROM nc_tables t JOIN nc_log l USING (table_id)
  UNION ALL SELECT lower(dataset || '.' || table_name), 'recipe', NULL, NULL,
         greatest(begin_snapshot, coalesce(end_snapshot, 0)) FROM $recipes
  UNION ALL SELECT lower(name), 'property', NULL, NULL, greatest(begin_snapshot, coalesce(end_snapshot, 0)) FROM $aspects
  UNION ALL SELECT lower(aspect), 'property', NULL, NULL, greatest(begin_snapshot, coalesce(end_snapshot, 0)) FROM $tests
  UNION ALL SELECT lower(name), 'function', NULL, NULL, greatest(begin_snapshot, coalesce(end_snapshot, 0)) FROM $functions
  UNION ALL SELECT lower(s.schema_name || '.' || m.macro_name), 'macro', NULL, NULL,
         greatest(m.begin_snapshot, coalesce(m.end_snapshot, 0)) FROM $macro m JOIN nc_schema s USING (schema_id)
  UNION ALL SELECT lower(split_part(side, '.', 1) || '.' || split_part(side, '.', 2)), 'relationships', NULL, NULL,
         greatest(begin_snapshot, coalesce(end_snapshot, 0)) FROM $relationships, unnest([left_side, right_side]) u(side)
  UNION ALL SELECT lower(dataset), 'relationships', NULL, NULL, greatest(begin_snapshot, coalesce(end_snapshot, 0))
  FROM $relationships
  UNION ALL SELECT split_part(asset, '.', 1), 'assets', NULL, NULL, greatest(begin_snapshot, coalesce(end_snapshot, 0))
  FROM (SELECT asset, begin_snapshot, end_snapshot FROM nc_tables UNION ALL SELECT asset, begin_snapshot, end_snapshot FROM nc_views)
  UNION ALL SELECT split_part(t.asset, '.', 1), 'assets', NULL, NULL, greatest(c.begin_snapshot, coalesce(c.end_snapshot, 0))
  FROM nc_tables t JOIN $column c ON c.table_id = t.table_id
  -- a view of a kind added to or removed from a dataset; a view defined again is the same member
  UNION ALL SELECT e.dataset, 'views ' || coalesce(json_extract_string(a.schema, '$.x-kind'), 'metric'), NULL, NULL, e.at
  FROM (SELECT dataset, view_name, begin_snapshot AS at FROM nc_views v WHERE NOT EXISTS
          (SELECT 1 FROM nc_views w WHERE w.asset = v.asset AND w.end_snapshot = v.begin_snapshot)
        UNION ALL SELECT dataset, view_name, end_snapshot FROM nc_views v WHERE end_snapshot IS NOT NULL AND NOT EXISTS
          (SELECT 1 FROM nc_views w WHERE w.asset = v.asset AND w.begin_snapshot = v.end_snapshot)) e
  JOIN $aspects a ON a.kind = 'VIEW' AND a.name = e.view_name AND a.begin_snapshot <= e.at
   AND (a.end_snapshot IS NULL OR a.end_snapshot >= e.at)
  UNION ALL SELECT lower(json_extract_string(schema, '$.x-kind')), 'properties', NULL, NULL,
         greatest(begin_snapshot, coalesce(end_snapshot, 0)) FROM $aspects WHERE json_extract_string(schema, '$.x-kind') IS NOT NULL)
SELECT asset, what, dataset, function, max(changed_at) AS changed_at FROM nc
WHERE (asset, what) IN (SELECT asset, what FROM nc_want) GROUP BY ALL)";
	for (auto &relation :
	     {"glossary", "measurements", "imports", "recipes", "aspects", "tests", "functions", "relationships"}) {
		sql = StringUtil::Replace(sql, "$" + string(relation), ws.Store(relation));
	}
	for (auto &table : {"snapshot_changes", "schema", "table", "column", "view", "macro"}) {
		sql = StringUtil::Replace(sql, "$" + string(table), ws.Meta(table));
	}
	return StringUtil::Replace(sql, "$wanted", wanted);
}

//! The current results that are out of date, with why: (id, dataset, function, subject, asset, what,
//! reason), id the snapshot that stored it. A node it read changed after the snapshot it read at
//! (changed), or a result it read is out of date or possibly so (possibly): it reads as it read until that
//! result is computed again, which keeps its changed_at when its value is the same. Asked on demand: `roots` empty asks
//! about every current result; else it is a condition on the measurements relation, and the check walks only the
//! results it selects and those they read, through their inputs
static string CheckRowsSQL(const Workspace &ws, const string &roots) {
	string sql = R"(WITH RECURSIVE ck_all AS (SELECT begin_snapshot AS id, dataset, function, subject, aspect, read_at
                                FROM $measurements WHERE end_snapshot IS NULL),
ck_edge AS (SELECT begin_snapshot AS id, asset, what FROM $inputs WHERE end_snapshot IS NULL),
ck_scope(id) AS ($scope),
ck_cur AS (SELECT * FROM ck_all WHERE id IN (SELECT id FROM ck_scope)),
ck_in AS (SELECT * FROM ck_edge WHERE id IN (SELECT id FROM ck_scope)),
-- a result read, through a read of its function's results or of the values of its property
ck_reads AS (SELECT r.id AS reader, q.id AS read, i.asset, i.what FROM ck_in i JOIN ck_cur r USING (id)
             JOIN ck_cur q ON q.dataset = r.dataset AND q.subject = i.asset
              AND (i.what = 'result ' || q.function OR (i.what = 'value ' || q.aspect AND q.function <> r.function))),
ck_node AS ($nodes),
ck_changed AS (SELECT c.id, i.asset, i.what FROM ck_cur c JOIN ck_in i USING (id) JOIN ck_node n
               ON n.asset = lower(i.asset) AND n.what = i.what AND (n.dataset IS NULL OR n.dataset = c.dataset)
              AND n.function IS DISTINCT FROM c.function WHERE n.changed_at > c.read_at),
ck_out(id) AS (SELECT id FROM ck_changed UNION SELECT r.reader FROM ck_reads r JOIN ck_out o ON o.id = r.read)
SELECT c.id, c.dataset, c.function, c.subject, x.asset, x.what, 'changed' AS reason FROM ck_changed x JOIN ck_cur c USING (id)
UNION SELECT c.id, c.dataset, c.function, c.subject, r.asset, r.what, 'possibly' FROM ck_reads r
JOIN ck_out o ON o.id = r.read JOIN ck_cur c ON c.id = r.reader)";
	auto scope = roots.empty() ? string("SELECT id FROM ck_all")
	                           : "SELECT begin_snapshot FROM $measurements WHERE end_snapshot IS NULL AND (" + roots +
	                                 ") UNION SELECT q.id FROM ck_scope s JOIN ck_edge i ON i.id = s.id JOIN ck_all r "
	                                 "ON r.id = s.id JOIN ck_all q"
	                                 " ON q.dataset = r.dataset AND q.subject = i.asset AND (i.what = 'result ' ||"
	                                 " q.function OR (i.what = 'value ' || q.aspect AND q.function <> r.function))";
	sql = StringUtil::Replace(sql, "$scope", scope);
	sql = StringUtil::Replace(sql, "$nodes", NodeChangesSQL(ws, "ck_in"));
	sql = StringUtil::Replace(sql, "$measurements", ws.Store("measurements"));
	return StringUtil::Replace(sql, "$inputs", ws.Store("inputs"));
}

string CurrentResultsSQL(Inner &inner, const string &filter) {
	return "SELECT m.*, k.id IS NULL AS current, coalesce(NOT k.changed, false) AS possibly FROM " +
	       inner.ws.Store("measurements") + " m LEFT JOIN (SELECT id, bool_or(reason = 'changed') AS changed FROM (" +
	       CheckRowsSQL(inner.ws, filter) +
	       ") GROUP BY id) k ON k.id = m.begin_snapshot WHERE m.end_snapshot IS NULL AND (" + filter + ")";
}

string OutdatedSQL(Inner &inner) {
	return "SELECT DISTINCT dataset, function, subject, asset, what, reason FROM (" + CheckRowsSQL(inner.ws, "") + ")";
}

//! The annotations in scope, each with whether its subject changed after the
//! snapshot it was written at (SPEC §5.2, §3): a column retyped or dropped, a
//! table one of whose columns was, or a column re-derived, the lineage behind
//! it in the newest load differing from the one in the load that held at the
//! write; rows arriving under the same shape and derivation do not age it. A
//! view, or a column of one, changed when the definition that held at the
//! write was replaced or dropped. A subject none of whose rows at the write
//! are left, an expiry of the lake's snapshots having removed the ended ones,
//! changed. Dataset subjects and pair paths never do.
//! Each fact is one join over the annotations, keyed by the snapshot a row
//! began at, which no two rows share: the rule plans as joins, where the same
//! rule as subqueries per row took the planner a tenth of a second
static string AnnotationsOverSQL(const Workspace &ws, const string &filter) {
	// the column is all that follows the second dot: its name may hold one (SPEC §4)
	string g = "g AS (SELECT dataset, subject, aspect, actor_kind, body, written_at, begin_snapshot AS id,"
	           " split_part(subject, '.', 1) AS sch, split_part(subject, '.', 2) AS tbl,"
	           " substr(subject, length(split_part(subject, '.', 1)) + length(split_part(subject, '.', 2)) + 3) AS col,"
	           " CASE WHEN subject LIKE '% -> %' OR subject LIKE '% <-> %' THEN 0"
	           " ELSE len(string_split(subject, '.')) END AS depth FROM " +
	           ws.Store("glossary") + " WHERE end_snapshot IS NULL AND " + filter + ")";
	string live_then = "c.begin_snapshot <= g.id AND (c.end_snapshot IS NULL OR c.end_snapshot > g.id)";
	// a column's type at the write, and now
	string col_then = "col_then AS (SELECT g.id, min(c.column_type) AS type FROM g JOIN cols c ON c.schema_name = g.sch"
	                  " AND c.table_name = g.tbl AND c.column_name = g.col AND " +
	                  live_then + " WHERE g.depth >= 3 GROUP BY g.id)";
	string col_now = "col_now AS (SELECT schema_name, table_name, column_name, min(column_type) AS type FROM cols"
	                 " WHERE end_snapshot IS NULL GROUP BY ALL)";
	// a table: a column that was live at the write is no longer there under its name and type
	string tbl_then = "tbl_then AS (SELECT g.id, bool_or(n.column_name IS NULL) AS changed FROM g JOIN cols c"
	                  " ON c.schema_name = g.sch AND c.table_name = g.tbl AND " +
	                  live_then +
	                  " LEFT JOIN cols n ON n.schema_name = c.schema_name AND n.table_name = c.table_name"
	                  " AND n.column_name = c.column_name AND n.column_type = c.column_type AND n.end_snapshot IS NULL"
	                  " WHERE g.depth = 2 GROUP BY g.id)";
	// the lineage of the load that held at the write, and of the newest load
	string load_then = "load_then AS (SELECT g.id, arg_max(i.lineage, (i.snapshot_id, i.begin_snapshot)) AS lineage"
	                   " FROM g JOIN loads i ON i.dataset = g.sch AND i.table_name = g.tbl AND i.snapshot_id <= g.id"
	                   " WHERE g.depth >= 2 GROUP BY g.id)";
	string load_now =
	    "load_now AS (SELECT dataset, table_name, arg_max(lineage, (snapshot_id, begin_snapshot)) AS lineage"
	    " FROM loads GROUP BY ALL)";
	// a view's shape is its definition: the one that held at the write, and whether it is still the live one
	string view_then = "view_then AS (SELECT g.id, bool_or(c.end_snapshot IS NULL) AS still FROM g JOIN views c"
	                   " ON c.schema_name = g.sch AND c.view_name = g.tbl AND " +
	                   live_then + " WHERE g.depth >= 2 GROUP BY g.id)";
	auto derivation = [](const string &lineage, const string &key) {
		return "json_extract_string(" + lineage + "::JSON, '$.\"' || " + key + " || '\"')";
	};
	string loaded_twice = "lt.lineage IS NOT NULL AND ln.lineage IS NOT NULL";
	// a subject with no row of the lake's catalog live at the write is one whose rows a later write
	// ended and an expiry of the lake's snapshots removed: it changed
	string column_stale = "(CASE WHEN ct.type IS NULL THEN vt.id IS NULL ELSE cn.type IS DISTINCT FROM ct.type END)";
	string column_rederived = "(" + loaded_twice + " AND " + derivation("lt.lineage", "g.col") + " IS DISTINCT FROM " +
	                          derivation("ln.lineage", "g.col") + ")";
	string table_rederived = "(" + loaded_twice +
	                         " AND coalesce(list_bool_or(list_transform(json_keys(lt.lineage::JSON),"
	                         " lambda k: " +
	                         derivation("lt.lineage", "k") + " IS DISTINCT FROM " + derivation("ln.lineage", "k") +
	                         ")), false))";
	string view_stale = "(vt.id IS NOT NULL AND NOT vt.still)";
	string stale = "CASE WHEN g.depth >= 3 THEN " + column_stale + " OR " + column_rederived + " OR " + view_stale +
	               " WHEN g.depth = 2 THEN coalesce(tt.changed, vt.id IS NULL) OR " + table_rederived + " OR " +
	               view_stale + " ELSE false END";
	return "WITH cols AS (" + ColumnsSQL(ws) + "), views AS (" + ViewsSQL(ws) + "), loads AS (" + LoadsSQL(ws) + "), " +
	       g + ", " + col_then + ", " + col_now + ", " + tbl_then + ", " + load_then + ", " + load_now + ", " +
	       view_then +
	       " SELECT g.subject, g.aspect, g.actor_kind AS actor, g.body::JSON AS body, g.written_at, coalesce(" + stale +
	       ", false) AS stale, g.id AS ord, NULL::VARCHAR AS error, g.dataset, false AS possibly FROM g"
	       " LEFT JOIN col_then ct ON ct.id = g.id"
	       " LEFT JOIN col_now cn ON cn.schema_name = g.sch AND cn.table_name = g.tbl AND cn.column_name = g.col"
	       " LEFT JOIN tbl_then tt ON tt.id = g.id"
	       " LEFT JOIN load_then lt ON lt.id = g.id"
	       " LEFT JOIN load_now ln ON ln.dataset = g.sch AND ln.table_name = g.tbl"
	       " LEFT JOIN view_then vt ON vt.id = g.id";
}

static string AnnotationsSQL(const ReadScope &scope, const string &aspect_filter) {
	return AnnotationsOverSQL(scope.ws, scope.filter + aspect_filter);
}

//! Every value in scope, the current one per actor kind: the glossary's agent
//! and human rows, and the function values the measurements hold, a view's
//! lineage none of them, which no property declares; each with
//! its staleness, `ord`, the snapshot it was written at, and the error of a
//! computation that failed, whose row holds no value
static string ValuesSQL(Inner &inner, const ReadScope &scope, const string &aspect_filter) {
	return "(" + AnnotationsSQL(scope, aspect_filter) +
	       ") UNION ALL SELECT subject, aspect, 'function' AS actor, value::JSON AS body, computed_at AS written_at,"
	       " NOT current AND NOT possibly AS stale, begin_snapshot AS ord, error, dataset, possibly FROM (" +
	       CurrentResultsSQL(inner, scope.filter + aspect_filter + " AND function <> 'lineage'") + ")";
}

struct TestRow {
	string name;
	string aspect;
	string detector;
	Value threshold;
	Value script;
	vector<string> grains;
	//! the aspect's WHEN: required only while the sibling's winning value carries the value,
	//! or only on columns of the named data types (a JSON list)
	Value condition_aspect;
	Value condition_value;
	Value condition_types;
};

//! The tests as they are now, with their detector's script and their aspect's grains and condition
static vector<TestRow> Tests(Inner &inner, const Value &aspect) {
	vector<TestRow> result;
	auto rows = inner.Run(
	    "SELECT w.name, w.aspect, w.detector, w.threshold, (SELECT f.script FROM " + inner.Store("functions") +
	    " f WHERE f.name = w.detector AND f.returns IS NULL AND f.end_snapshot IS NULL"
	    " ORDER BY f.begin_snapshot DESC LIMIT 1) AS script, "
	    "from_json(a.grains, '[\"VARCHAR\"]'), a.condition::JSON->>'aspect',"
	    " a.condition::JSON->>'value', a.condition::JSON->'type' FROM " +
	    Newest(inner.ws, "tests") + " w LEFT JOIN " + Newest(inner.ws, "aspects") + " a ON a.name = w.aspect" +
	    (aspect.IsNull() ? string() : " WHERE w.aspect = " + QuoteText(aspect.ToString())) + " ORDER BY w.aspect");
	while (auto chunk = rows->Fetch()) {
		for (idx_t i = 0; i < chunk->size(); i++) {
			TestRow test;
			test.name = chunk->GetValue(0, i).ToString();
			test.aspect = chunk->GetValue(1, i).ToString();
			auto detector = chunk->GetValue(2, i);
			test.detector = detector.IsNull() ? string() : detector.ToString();
			test.threshold = chunk->GetValue(3, i);
			test.script = chunk->GetValue(4, i);
			auto grains = chunk->GetValue(5, i);
			if (!grains.IsNull()) {
				for (auto &grain : ListValue::GetChildren(grains)) {
					test.grains.push_back(grain.ToString());
				}
			}
			test.condition_aspect = chunk->GetValue(6, i);
			test.condition_value = chunk->GetValue(7, i);
			test.condition_types = chunk->GetValue(8, i);
			result.push_back(std::move(test));
		}
	}
	return result;
}

static constexpr const char *TESTS_COLUMNS = "subject, aspect, test, state, score, computed_at, current, error";

//! The result `error` (SPEC §7.2): one row per subject holding values, score 0,
//! the failure text; the detector's failure is its test's result, never the read's
//! `current` on a result (SPEC §7.2): every function value the result read
//! is current; true where none annotated
static string CurrentSQL(const string &values) {
	return "coalesce((SELECT bool_and(c.current) FROM (" + values +
	       ") c WHERE c.subject = d.subject AND c.actor_kind = 'function'), true) AS current";
}

static string ErrorResultSQL(const string &head, const string &values, const string &failure) {
	return "SELECT DISTINCT d.subject, " + head +
	       "'error' AS state, 0.0::DOUBLE AS score, now()::TIMESTAMP AS computed_at, " + CurrentSQL(values) + ", " +
	       QuoteText(failure) + " AS error FROM (" + values + ") d";
}

static bool InTestsSchema(const Value &subject, const Value &state, const Value &score) {
	if (subject.IsNull() || state.IsNull() || score.IsNull()) {
		return false;
	}
	auto colour = state.ToString();
	if (colour != "green" && colour != "yellow" && colour != "orange" && colour != "red") {
		return false;
	}
	auto value = score.GetValue<double>();
	return value >= 0.0 && value <= 1.0;
}

//! The results in scope: each test's detector runs now, at read, over its
//! values on the second connection, and its rows serve in the standard test-results
//! schema (SPEC §7.2); a detector that cannot answer, or answers outside the
//! schema, is the result `error`
//! The values of a read's scope, every aspect, as the hoisted relation every
//! part of the read selects from: planned once, where each part once planned
//! the stale rule's joins again
static constexpr const char *HOISTED = "annotations_all";

//! A test's values as a detector reads them, from the hoisted relation
static string TestValuesSQL(const string &relation, const string &aspect) {
	return "SELECT subject, aspect, actor AS actor_kind, body, NOT stale AND NOT possibly AS current FROM " + relation +
	       " WHERE aspect = " + QuoteText(aspect);
}

//! The values of a scope, read once into a table of the fill's own connection:
//! the detectors and every part of the read select from it. A failed
//! computation is no value
static constexpr const char *STORED_VALUES = "glossql_values";

//! The values of a whole dataset as kept (glossql_cached 'values'): every row ValuesSQL serves, a
//! failed computation's too, computed once per store version for the raw read, the collapsed read
//! and the tests, which each fill on a connection of their own
static string KeptValuesSQL(const ReadScope &whole) {
	return "SELECT * FROM glossql_cached('values', " + QuoteText(whole.dataset) + ", NULL)";
}

static void StoreValues(Inner &inner, const ReadScope &whole) {
	auto made = inner.con.Query("CREATE OR REPLACE TEMP TABLE " + string(STORED_VALUES) + " AS SELECT * FROM (" +
	                            KeptValuesSQL(whole) + ") WHERE error IS NULL");
	if (made->HasError()) {
		throw InvalidInputException("glossql: the values of %s could not be read: %s", whole.dataset,
		                            FirstLine(made->GetError()));
	}
}

static string ResultsSQL(Inner &inner, const vector<TestRow> &tests, const ReadScope &scope) {
	vector<string> parts;
	// the detectors run at bind on the inner connection, over the scope's values
	// stored there as a temporary table (StoreValues)
	for (auto &test : tests) {
		if (test.detector.empty()) {
			continue;
		}
		auto values = TestValuesSQL(HOISTED, test.aspect);
		auto head = "'" + test.aspect + "' AS aspect, '" + test.name + "' AS test, ";
		if (test.script.IsNull()) {
			parts.push_back(ErrorResultSQL(head, values, "detector " + test.detector + " is not a declared function"));
			continue;
		}
		auto threshold = test.threshold.IsNull() ? string("NULL") : test.threshold.ToSQLString();
		auto script = ReplacePlaceholder(test.script.ToString(), "threshold", threshold);
		// the script is a derived table under the values CTE, so a body that opens
		// with its own WITH composes
		auto result =
		    inner.con.Query("SELECT d.subject::VARCHAR, d.state::VARCHAR, d.score::DOUBLE FROM (WITH annotations AS (" +
			                TestValuesSQL(STORED_VALUES, test.aspect) + ") SELECT * FROM (" + script + ") AS q) AS d");
		if (result->HasError()) {
			parts.push_back(ErrorResultSQL(head, values, FirstLine(result->GetError())));
			continue;
		}
		vector<string> rows;
		string failure;
		while (auto chunk = result->Fetch()) {
			for (idx_t i = 0; i < chunk->size() && failure.empty(); i++) {
				auto subject = chunk->GetValue(0, i);
				auto state = chunk->GetValue(1, i);
				auto score = chunk->GetValue(2, i);
				if (!InTestsSchema(subject, state, score)) {
					failure = "detector " + test.detector + " served (" + subject.ToString() + ", " + state.ToString() +
					          ", " + score.ToString() + "), outside the test-results schema";
					break;
				}
				// the score travels as text: a double round-trips through its shortest decimal form
				rows.push_back("(" + subject.ToSQLString() + ", " + state.ToSQLString() + ", " +
				               QuoteText(score.ToString()) + ")");
			}
		}
		if (!failure.empty()) {
			parts.push_back(ErrorResultSQL(head, values, failure));
		} else if (!rows.empty()) {
			parts.push_back("SELECT d.subject, " + head +
			                "d.state, d.score::DOUBLE AS score, now()::TIMESTAMP AS computed_at, " +
			                CurrentSQL(values) + ", NULL::VARCHAR AS error FROM (VALUES " +
			                StringUtil::Join(rows, ", ") + ") AS d(subject, state, score)");
		}
	}
	if (parts.empty()) {
		return "SELECT NULL::VARCHAR AS subject, NULL::VARCHAR AS aspect, NULL::VARCHAR AS test, "
		       "NULL::VARCHAR AS state, NULL::DOUBLE AS score, NULL::TIMESTAMP AS computed_at, "
		       "NULL::BOOLEAN AS current, NULL::VARCHAR AS error WHERE false";
	}
	return StringUtil::Join(parts, " UNION ALL ");
}

//! The subjects each tested aspect is required on, by the aspect's grain
//! (SPEC §5.1): what the read discloses as missing when nobody annotated
static string MissingSQL(const vector<TestRow> &tests, const ReadScope &scope) {
	vector<string> parts;
	for (auto &test : tests) {
		auto grains = test.grains;
		if (grains.empty()) {
			grains = {"DATASET", "TABLE", "COLUMN", "RELATIONSHIP", "SOURCE"};
		}
		auto aspect = ", " + QuoteText(test.aspect) + " AS aspect";
		auto dataset = QuoteText(scope.dataset);
		auto columns = " FROM glossql_columns(" + dataset + ")";
		vector<string> missing;
		for (auto &grain : grains) {
			if (grain == "COLUMN") {
				missing.push_back("SELECT " + dataset + " || '.' || table_name || '.' || column_name AS subject" +
				                  aspect + columns);
			} else if (grain == "TABLE") {
				missing.push_back("SELECT DISTINCT " + dataset + " || '.' || table_name AS subject" + aspect + columns);
			} else if (grain == "DATASET") {
				missing.push_back("SELECT " + dataset + " AS subject" + aspect);
			} else if (grain == "RELATIONSHIP") {
				missing.push_back("SELECT left_side || ' ' || op || ' ' || right_side AS subject" + aspect + " FROM " +
				                  scope.ws.Store("relationships") + " WHERE dataset = " + dataset);
			} else if (grain == "SOURCE") {
				missing.push_back("SELECT DISTINCT name AS subject" + aspect + " FROM " + scope.ws.Store("sources"));
			}
		}
		if (missing.empty()) {
			continue;
		}
		auto part = "SELECT subject, aspect FROM (" + StringUtil::Join(missing, " UNION ALL ") + ")";
		if (!test.condition_aspect.IsNull()) {
			// required only where the sibling's winning value, human over agent, carries the literal (SPEC §5.1)
			part += " WHERE subject IN (SELECT subject FROM (SELECT subject, body FROM " + string(HOISTED) +
			        " WHERE aspect = " + QuoteText(test.condition_aspect.ToString()) +
			        " AND actor IN ('human', 'agent') QUALIFY row_number() OVER (PARTITION BY subject"
			        " ORDER BY CASE actor WHEN 'human' THEN 0 ELSE 1 END, ord DESC) = 1)"
			        " WHERE body->>'value' = " +
			        QuoteText(test.condition_value.ToString()) + ")";
		} else if (!test.condition_types.IsNull()) {
			// required only on columns of the named types: the base type, DECIMAL for DECIMAL(18,2) (SPEC §5.1)
			part += " WHERE subject IN (SELECT " + dataset + " || '.' || table_name || '.' || column_name" + columns +
			        " WHERE upper(split_part(column_type, '(', 1)) IN (SELECT unnest(from_json(" +
			        QuoteText(test.condition_types.ToString()) + ", '[\"VARCHAR\"]'))))";
		}
		parts.push_back(part);
	}
	if (parts.empty()) {
		return "SELECT NULL::VARCHAR AS subject, NULL::VARCHAR AS aspect WHERE false";
	}
	// a narrowed read keeps to its subject; the dataset's sweep takes every declared source (SPEC §5.1)
	return "SELECT DISTINCT subject, aspect FROM (" + StringUtil::Join(parts, " UNION ALL ") + ")" +
	       (scope.whole ? string() : " WHERE " + scope.subjects);
}

//! The collapsed read: one row per (subject, aspect) — the precedence pick,
//! withheld while two actors' values are current and the test's score exceeds its
//! threshold; one value is never in conflict, it is served with its test state
//! (SPEC §5.3); every gap visible
//! The tests of a whole dataset as kept (glossql_cached 'tests'): the detectors run once per store version,
//! for the tests read and for the collapse, which reads each test's state
static string KeptTestsSQL(const ReadScope &whole) {
	return "glossql_cached('tests', " + QuoteText(whole.dataset) + ", NULL)";
}

static string CollapsedSQL(Inner &inner, const vector<TestRow> &tests, const ReadScope &scope, const Value &aspect) {
	auto aspect_filter = AspectFilter(aspect);
	auto missing = MissingSQL(tests, scope);
	if (!aspect.IsNull()) {
		missing = "SELECT * FROM (" + missing + ") WHERE aspect = " + QuoteText(aspect.ToString());
	}
	// an error is not a judgment (SPEC §7.2): it never withholds, and any other result outranks it;
	// a stale value is served and marked (SPEC §5.3)
	return "WITH " + string(HOISTED) + " AS (SELECT * FROM " + string(STORED_VALUES) +
	       "), annotations AS (SELECT * FROM " + string(HOISTED) + " WHERE true" + aspect_filter +
	       "), picked AS (SELECT subject, aspect, body AS value, stale, possibly FROM annotations QUALIFY row_number() "
	       "OVER ("
	       "PARTITION BY subject, aspect ORDER BY CASE actor WHEN 'human' THEN 0 WHEN 'agent' THEN 1 ELSE 2 END,"
	       " ord DESC) = 1), results AS (SELECT subject, aspect, test, state, score FROM " +
	       KeptTestsSQL(scope) +
	       "), judged AS (SELECT v.subject, v.aspect, v.state, v.score, w.threshold FROM results v LEFT JOIN " +
	       Newest(scope.ws, "tests") +
	       " w ON w.name = v.test QUALIFY row_number() OVER (PARTITION BY v.subject, v.aspect"
	       " ORDER BY v.state = 'error', v.score DESC) = 1), annotation_counts AS (SELECT subject, aspect,"
	       " count(DISTINCT actor) AS n FROM annotations GROUP BY subject, aspect), missing AS (" +
	       missing +
	       ") SELECT * FROM ("
	       "SELECT p.subject, p.aspect, CASE WHEN j.score > j.threshold AND v.n > 1 THEN NULL ELSE p.value END AS "
	       "value,"
	       " j.state AS test_state, j.score, CASE WHEN j.score > j.threshold AND v.n > 1 THEN 'conflict' WHEN p.stale "
	       "THEN 'stale'"
	       " WHEN p.possibly THEN 'possibly' ELSE 'current' END AS state"
	       " FROM picked p LEFT JOIN judged j ON j.subject = p.subject AND j.aspect = p.aspect"
	       " LEFT JOIN annotation_counts v ON v.subject = p.subject AND v.aspect = p.aspect"
	       " UNION ALL "
	       "SELECT o.subject, o.aspect, NULL::JSON AS value, NULL::VARCHAR AS test_state, NULL::DOUBLE AS score,"
	       " 'missing' AS state FROM missing o WHERE NOT EXISTS"
	       " (SELECT 1 FROM picked p WHERE p.subject = o.subject AND p.aspect = o.aspect)"
	       ") ORDER BY subject, aspect";
}

//! The raw read: one row per value — (subject, aspect, kind, test, actor, body, written_at, current,
//! error); a failed computation is a row with its error and no body
static string RawSQL(const ReadScope &whole) {
	return "SELECT s.subject, s.aspect, a.kind, w.name AS test, s.actor, s.body, s.written_at,"
	       " NOT s.stale AND NOT s.possibly AS current, s.error FROM (" +
	       KeptValuesSQL(whole) + ") s LEFT JOIN " + Newest(whole.ws, "aspects") +
	       " a ON a.name = s.aspect LEFT JOIN " + Newest(whole.ws, "tests") +
	       " w ON w.aspect = s.aspect ORDER BY s.subject, s.aspect, s.actor";
}

//===--------------------------------------------------------------------===//
// glossql_columns(subject[, views]): every column of a dataset's tables, or of
// one table, and of its views unless views is false — (table_name,
// column_name, column_type, ordinal_position, table_type). What the reads,
// the kits and the cells read a dataset's shape from: the dataset's own
// relations by name, where information_schema binds every view of every
// catalog for its columns
//===--------------------------------------------------------------------===//

struct ColumnsBindData : public FunctionData {
	string dataset;
	string table;
	bool views = true;

	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<ColumnsBindData>();
		result->dataset = dataset;
		result->table = table;
		result->views = views;
		return result;
	}
	bool Equals(const FunctionData &other) const override {
		auto &o = other.Cast<ColumnsBindData>();
		return dataset == o.dataset && table == o.table && views == o.views;
	}
};

struct ColumnsState : public GlobalTableFunctionState {
	vector<RelationColumn> rows;
	idx_t next = 0;
};

static unique_ptr<FunctionData> ColumnsBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto subject = ResolveSubject(context, input.inputs[0].ToString());
	auto parts = StringUtil::Split(subject, '.');
	if (parts.empty() || parts.size() > 2) {
		throw InvalidInputException("glossql: glossql_columns takes a dataset or dataset.table, got '%s'", subject);
	}
	auto result = make_uniq<ColumnsBindData>();
	result->dataset = parts[0];
	result->table = parts.size() == 2 ? parts[1] : string();
	result->views = input.inputs.size() < 2 || input.inputs[1].IsNull() || input.inputs[1].GetValue<bool>();
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
	                LogicalType::VARCHAR};
	names = {Identifier("table_name"), Identifier("column_name"), Identifier("column_type"),
	         Identifier("ordinal_position"), Identifier("table_type")};
	return result;
}

static unique_ptr<GlobalTableFunctionState> ColumnsInit(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind = input.bind_data->Cast<ColumnsBindData>();
	auto state = make_uniq<ColumnsState>();
	state->rows = DatasetColumns(context, bind.dataset, bind.table, bind.views);
	return state;
}

static void ColumnsExec(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<ColumnsState>();
	idx_t count = 0;
	while (state.next < state.rows.size() && count < STANDARD_VECTOR_SIZE) {
		auto &row = state.rows[state.next++];
		output.data[0].Append(Value(row.table));
		output.data[1].Append(Value(row.column));
		output.data[2].Append(Value(row.type));
		output.data[3].Append(Value::BIGINT(NumericCast<int64_t>(row.ordinal)));
		output.data[4].Append(Value(row.view ? "VIEW" : "BASE TABLE"));
		count++;
	}
	output.CheckCardinality(count);
}

static TableFunctionSet GlossColumnsFunctions() {
	TableFunctionSet set("glossql_columns");
	set.AddFunction(TableFunction("glossql_columns", {LogicalType::VARCHAR}, ColumnsExec, ColumnsBind, ColumnsInit));
	set.AddFunction(TableFunction("glossql_columns", {LogicalType::VARCHAR, LogicalType::BOOLEAN}, ColumnsExec,
	                              ColumnsBind, ColumnsInit));
	return set;
}

//===--------------------------------------------------------------------===//
// The read cache: the instance's reads, by lake, kind and dataset, each with
// the store version it was computed at. An entry of DuckDB's object cache, its
// bytes reserved in the buffer pool: memory_limit counts it, and pressure
// evicts it, after which a bind computes it again. Every connection shares
// it; a write or a delete on any connection moves the version, which every
// bind compares first. A dropped dataset's rows stay until evicted.
//===--------------------------------------------------------------------===//

//! One read of a dataset as kept: immutable once built, replaced whole
struct CachedRead : public ObjectCacheEntry {
	string version;
	vector<Identifier> names;
	vector<LogicalType> types;
	shared_ptr<ColumnDataCollection> rows;

	static string ObjectType() {
		return "glossql_read";
	}
	string GetObjectType() override {
		return ObjectType();
	}
	optional_idx GetEstimatedCacheMemory() const override {
		return optional_idx(rows ? rows->SizeInBytes() : 0);
	}
};

//===--------------------------------------------------------------------===//
// The kinds of asset a kit declares (SPEC §4): a kit brings table macros in
// the lake's main named lineage_assets_<kind>, lineage_edges_<kind> and
// lineage_places_<kind>, each taking the dataset, and the lineage reads them
// through glossql_kind_assets(), glossql_kind_edges() and glossql_kind_places():
// their rows unioned by name. The core names no kind
//===--------------------------------------------------------------------===//

struct KindStem {
	const char *function;
	const char *stem;
	//! the columns, as the empty relation where no kit declares the stem
	const char *empty;
};

static const KindStem KIND_STEMS[] = {
    {"glossql_kind_assets", "lineage_assets_",
	 "SELECT NULL::VARCHAR AS asset, NULL::VARCHAR AS kind, NULL::VARCHAR AS node_asset, NULL::VARCHAR AS node_what"},
    {"glossql_kind_edges", "lineage_edges_",
	 "SELECT NULL::VARCHAR AS \"from\", NULL::VARCHAR AS \"to\", NULL::VARCHAR AS kind"},
    {"glossql_kind_places", "lineage_places_",
	 "SELECT NULL::VARCHAR AS subject, NULL::VARCHAR AS \"where\", NULL::VARCHAR AS asset"}};

//! The stem's rows for a dataset, or for every declared dataset where it is NULL. Asked with NULL, a
//! places macro is asked with the empty dataset alone: a row with no subject names a place it adds
static unique_ptr<TableRef> KindBindReplace(ClientContext &context, TableFunctionBindInput &input,
                                            const KindStem *kind) {
	auto lake = LakeName(context);
	vector<string> macros;
	auto &catalog = Catalog::GetCatalog(context, Identifier(lake));
	auto schema = catalog.GetSchema(context, Identifier("main"), OnEntryNotFound::RETURN_NULL);
	if (schema) {
		schema->Scan(context, CatalogType::TABLE_MACRO_ENTRY, [&](CatalogEntry &entry) {
			auto macro = entry.name.GetIdentifierName();
			if (StringUtil::StartsWith(macro, kind->stem)) {
				macros.push_back(macro);
			}
		});
	}
	std::sort(macros.begin(), macros.end());
	// a read that names one place reads the macros that declare it alone: those with a row of no subject
	// that names it
	bool narrowed = string(kind->stem) == "lineage_places_" && input.inputs.size() > 1 && !input.inputs[1].IsNull();
	if (narrowed) {
		Inner inner(context);
		vector<string> declaring;
		for (auto &macro : macros) {
			auto declared =
			    inner.con.Query("SELECT count(*) FROM " + QuoteName(lake) + ".main." + QuoteName(macro) +
				                "('') WHERE subject IS NULL AND \"where\" = " + QuoteText(input.inputs[1].ToString()));
			if (!declared->HasError()) {
				auto chunk = declared->Fetch();
				if (chunk && chunk->size() == 1 && chunk->GetValue(0, 0).GetValue<int64_t>() > 0) {
					declaring.push_back(macro);
				}
			}
		}
		macros = declaring;
	}
	vector<string> datasets;
	if (!input.inputs[0].IsNull()) {
		datasets.push_back(QuoteText(input.inputs[0].ToString()));
	} else if (string(kind->stem) == "lineage_places_") {
		datasets.push_back("''");
	} else {
		Connection con(*context.db);
		auto rows = con.Query("SELECT name FROM " + QuoteName(lake) + ".main.datasets ORDER BY name");
		while (!rows->HasError()) {
			auto chunk = rows->Fetch();
			if (!chunk || chunk->size() == 0) {
				break;
			}
			for (idx_t r = 0; r < chunk->size(); r++) {
				datasets.push_back(QuoteText(chunk->GetValue(0, r).ToString()));
			}
		}
	}
	// the assets of one kind are its macro's alone
	if (string(kind->stem) == "lineage_assets_" && input.inputs.size() > 1 && !input.inputs[1].IsNull()) {
		auto named = string(kind->stem) + input.inputs[1].ToString();
		vector<string> owners;
		for (auto &macro : macros) {
			if (StringUtil::CIEquals(macro, named)) {
				owners.push_back(macro);
			}
		}
		macros = owners;
	}
	// where no kit names the place, one empty row, so that the read can say the where names no place
	if (narrowed && macros.empty()) {
		return make_uniq<SubqueryRef>(ParseSelect(string(kind->empty)));
	}
	vector<string> parts;
	for (auto &macro : macros) {
		for (auto &dataset : datasets) {
			parts.push_back("SELECT * FROM " + QuoteName(lake) + ".main." + QuoteName(macro) + "(" + dataset + ")");
		}
	}
	auto sql = parts.empty() ? string(kind->empty) + " WHERE false"
	                         : "SELECT * FROM (" + string(kind->empty) + " WHERE false UNION ALL BY NAME " +
	                               StringUtil::Join(parts, " UNION ALL BY NAME ") + ")";
	return make_uniq<SubqueryRef>(ParseSelect(sql));
}

template <idx_t STEM>
static unique_ptr<TableRef> KindBind(ClientContext &context, TableFunctionBindInput &input) {
	return KindBindReplace(context, input, &KIND_STEMS[STEM]);
}

static TableFunctionSet GlossKindFunction(idx_t stem, table_function_bind_replace_t bind) {
	TableFunctionSet set(KIND_STEMS[stem].function);
	TableFunction function(KIND_STEMS[stem].function, {LogicalType::VARCHAR}, nullptr);
	function.bind_replace = bind;
	set.AddFunction(function);
	// the places a kit adds, narrowed to the macros that add the where named; the assets of one kind
	if (string(KIND_STEMS[stem].stem) != "lineage_edges_") {
		TableFunction narrowed(KIND_STEMS[stem].function, {LogicalType::VARCHAR, LogicalType::VARCHAR}, nullptr);
		narrowed.bind_replace = bind;
		set.AddFunction(narrowed);
	}
	return set;
}

//! The fill mark on the second connection (ClientContextState) while it
//! computes a read: a gloss read bound meanwhile is the shape of a dataset
//! view glossql_columns binds, never a read run, and binds as an empty relation
//! of the read's columns — a fill never recurses. A read bind opens one
//! connection, for the version check and the fill, closed with the bind
struct ReadFill : public ClientContextState {
	bool filling = false;

	static ReadFill &Of(ClientContext &context) {
		return *context.registered_state->GetOrCreate<ReadFill>("glossql_read_fill");
	}
};

//! A read is kept per lake: two workspaces attached to one instance can hold
//! a dataset of one name at one version each
static string CacheKey(const string &lake, const string &kind, const string &dataset) {
	return "glossql_read|" + lake + "|" + kind + "|" + dataset;
}

//! The store's version: the lake's current snapshot. A write to the store
//! takes a snapshot, a removal too, and so does a load: everything a read's
//! rows depend on, through the stale rule, the parts, the tests and the
//! detectors, moves it
static string StoreVersionSQL(const Workspace &ws) {
	return "SELECT (SELECT id FROM ducklake_current_snapshot(" + QuoteText(ws.lake) + "))::VARCHAR";
}

//! The whole dataset a scope lies in: what a cache entry holds, and what a
//! detector judges over, whatever the read's subject — the dataset's own
//! rows, and the source-grain rows every dataset reads
static ReadScope WholeOf(const ReadScope &scope, const Workspace &ws) {
	auto whole = scope;
	whole.whole = true;
	whole.ws = ws;
	whole.filter = "(dataset = " + QuoteText(scope.dataset) + " OR " + SourceGrainSQL(ws) + ")";
	return whole;
}

//! The SQL of one kind of read over a whole dataset
static string ReadSQL(Inner &inner, const string &kind, const ReadScope &whole) {
	if (kind == "values") {
		return "SELECT * FROM (" + ValuesSQL(inner, whole, "") + ")";
	}
	if (kind == "raw") {
		return RawSQL(whole);
	}
	if (kind == "collapsed") {
		StoreValues(inner, whole);
		return CollapsedSQL(inner, Tests(inner, Value()), whole, Value());
	}
	if (kind == "tests") {
		StoreValues(inner, whole);
		return "WITH " + string(HOISTED) + " AS (SELECT * FROM " + string(STORED_VALUES) + ") SELECT " +
		       string(TESTS_COLUMNS) + " FROM (" + ResultsSQL(inner, Tests(inner, Value()), whole) + ")";
	}
	throw InvalidInputException("glossql_cached serves a read: collapsed, raw, tests or values, not '%s'", kind);
}

//! The stack a fill runs on. A fill is planned inside the bind of the read that composes it, and a
//! kept read composes others, each planned in the bind of the one before; the bind may itself run
//! on a worker thread of the engine, whose stack is the platform's default (512 KB on macOS)
static constexpr size_t FILL_STACK = static_cast<size_t>(64) * 1024 * 1024;

struct FillWork {
	std::function<void()> work;
	std::exception_ptr raised;
};

static void *RunFill(void *argument) {
	auto &fill = *static_cast<FillWork *>(argument);
	try {
		fill.work();
	} catch (...) {
		fill.raised = std::current_exception();
	}
	return nullptr;
}

//! Runs work on a thread of its own with the fill's stack and waits for it; what it raises is raised here
static void OnFillStack(std::function<void()> work) {
	FillWork fill {std::move(work), nullptr};
	pthread_attr_t attributes;
	pthread_attr_init(&attributes);
	pthread_attr_setstacksize(&attributes, FILL_STACK);
	pthread_t thread;
	auto failed = pthread_create(&thread, &attributes, RunFill, &fill);
	pthread_attr_destroy(&attributes);
	if (failed) {
		fill.work();
		return;
	}
	pthread_join(thread, nullptr);
	if (fill.raised) {
		std::rethrow_exception(fill.raised);
	}
}

//! The rows of a statement run on the inner connection, kept under a key at a version
static shared_ptr<CachedRead> Keep(ClientContext &context, Inner &inner, const string &key, const string &version,
                                   const string &sql) {
	auto read = make_shared_ptr<CachedRead>();
	OnFillStack([&]() {
		auto result = inner.Run(sql);
		read->rows = make_shared_ptr<ColumnDataCollection>(Allocator::Get(context), result->GetTypes());
		while (auto chunk = result->Fetch()) {
			read->rows->Append(*chunk);
		}
		read->names = result->GetNames();
		read->types = result->GetTypes();
	});
	read->version = version;
	ObjectCache::GetObjectCache(context).Put(key, read);
	return read;
}

//! One kind of read over a whole dataset: as kept while the store's version
//! holds, else computed on the inner connection and kept
static shared_ptr<CachedRead> Cached(ClientContext &context, Inner &inner, const string &kind, const ReadScope &whole) {
	auto key = CacheKey(inner.ws.lake, kind, whole.dataset);
	ReadFill::Of(*inner.con.context).filling = true;
	auto version = inner.Scalar(StoreVersionSQL(inner.ws)).ToString();
	auto kept = ObjectCache::GetObjectCache(context).Get<CachedRead>(key);
	if (kept && kept->version == version) {
		return kept;
	}
	return Keep(context, inner, key, version, ReadSQL(inner, kind, whole));
}

//! glossql_cached(kind, dataset, aspect): a dataset's read as kept, scanned; the
//! reads narrow it in SQL
struct CachedBindData : public FunctionData {
	shared_ptr<CachedRead> read;

	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<CachedBindData>();
		result->read = read;
		return result;
	}
	bool Equals(const FunctionData &other) const override {
		return read == other.Cast<CachedBindData>().read;
	}
};

struct CachedScanState : public GlobalTableFunctionState {
	shared_ptr<ColumnDataCollection> rows;
	ColumnDataScanState scan;
};

static unique_ptr<FunctionData> CachedBind(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<Identifier> &names) {
	Inner inner(context);
	auto kind = input.inputs[0].ToString();
	auto result = make_uniq<CachedBindData>();
	result->read = Cached(context, inner, kind, WholeOf(ScopeOf(context, input.inputs[1]), inner.ws));
	return_types = result->read->types;
	names = result->read->names;
	return result;
}

static unique_ptr<GlobalTableFunctionState> CachedInit(ClientContext &context, TableFunctionInitInput &input) {
	auto state = make_uniq<CachedScanState>();
	state->rows = input.bind_data->Cast<CachedBindData>().read->rows;
	state->rows->InitializeScan(state->scan);
	return state;
}

static void CachedExec(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<CachedScanState>();
	state.rows->Scan(state.scan, output);
}

static TableFunction GlossCachedFunction() {
	return TableFunction("glossql_cached", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                     CachedExec, CachedBind, CachedInit);
}

static string StoreVersion(Inner &inner) {
	return inner.Scalar(StoreVersionSQL(inner.ws)).ToString();
}

//===--------------------------------------------------------------------===//
// A kit's read kept: the rows of a table macro of the lake's main schema, with
// no arguments, over the caller's dataset, computed once per store version on a
// connection of its own and kept beside the reads. A read that several reads
// compose (the views' summary, the tests on the views) is then planned and run
// once per write, and each read that composes it scans the rows. The rows are
// the read's at the store version: a write, a load, a definition and a
// removal move it. A macro is declared at open, before any read keeps it.
//===--------------------------------------------------------------------===//

static string KeptReadKey(ClientContext &context, const string &read) {
	return "glossql_kept|" + LakeName(context) + "|" + CurrentDataset(context) + "|" + read;
}

//! The kept reads being computed, on any connection. A read that binds a kept read while it is being
//! computed (a view's lineage binds every view of its schema, a composed read among them) binds the
//! macro itself, so a fill never waits on itself
struct KeptNow {
	std::mutex lock;
	std::set<string> keys;

	static KeptNow &Get() {
		static KeptNow now;
		return now;
	}
};

struct Keeping {
	string key;
	explicit Keeping(string key_p) : key(std::move(key_p)) {
		auto &now = KeptNow::Get();
		std::lock_guard<std::mutex> guard(now.lock);
		now.keys.insert(key);
	}
	~Keeping() {
		auto &now = KeptNow::Get();
		std::lock_guard<std::mutex> guard(now.lock);
		now.keys.erase(key);
	}
	Keeping(const Keeping &) = delete;
	Keeping &operator=(const Keeping &) = delete;
};

static bool BeingKept(const string &key) {
	auto &now = KeptNow::Get();
	std::lock_guard<std::mutex> guard(now.lock);
	return now.keys.count(key) > 0;
}

//! glossql_kept(read): the rows of main.<read>() on the caller's dataset, in its order
static unique_ptr<FunctionData> KeptReadBind(ClientContext &context, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto read = input.inputs[0].ToString();
	Inner inner(context);
	auto key = KeptReadKey(context, read);
	auto version = StoreVersion(inner);
	auto kept = ObjectCache::GetObjectCache(context).Get<CachedRead>(key);
	if (!kept || kept->version != version) {
		Keeping mark(key);
		kept = Keep(context, inner, key, version,
		            "SELECT * FROM " + QuoteName(inner.ws.lake) + ".main." + QuoteName(read) + "()");
	}
	auto result = make_uniq<CachedBindData>();
	result->read = kept;
	return_types = kept->types;
	names = kept->names;
	return result;
}

//! While this connection fills a read, a kept read is the macro itself, whose gloss reads bind as their
//! shape: a fill never computes another read of its own dataset. While the read is being kept, it is the
//! macro too
static unique_ptr<TableRef> KeptReadBindReplace(ClientContext &context, TableFunctionBindInput &input) {
	if (!ReadFill::Of(context).filling && !BeingKept(KeptReadKey(context, input.inputs[0].ToString()))) {
		return nullptr;
	}
	return make_uniq<SubqueryRef>(ParseSelect("SELECT * FROM " + QuoteName(LakeName(context)) + ".main." +
	                                          QuoteName(input.inputs[0].ToString()) + "()"));
}

static TableFunction GlossKeptReadFunction() {
	TableFunction function("glossql_kept", {LogicalType::VARCHAR}, CachedExec, KeptReadBind, CachedInit);
	function.bind_replace = KeptReadBindReplace;
	return function;
}

//! The columns of each kind of read, as the empty relation a bind serves while
//! this connection fills a read
static const char *const COLLAPSED_SHAPE =
    "subject VARCHAR, aspect VARCHAR, value JSON, test_state VARCHAR, score DOUBLE, "
    "state VARCHAR";
static const char *const RAW_SHAPE = "subject VARCHAR, aspect VARCHAR, kind VARCHAR, test VARCHAR, actor VARCHAR, "
                                     "body JSON, written_at TIMESTAMP, current BOOLEAN, error VARCHAR";
static const char *const TESTS_SHAPE = "subject VARCHAR, aspect VARCHAR, test VARCHAR, state VARCHAR, "
                                       "score DOUBLE, computed_at TIMESTAMP, current BOOLEAN, error VARCHAR";

static unique_ptr<TableRef> Shape(const string &columns) {
	vector<string> parts;
	for (auto &column : StringUtil::Split(columns, ", ")) {
		auto space = column.find(' ');
		parts.push_back("NULL::" + column.substr(space + 1) + " AS " + column.substr(0, space));
	}
	return make_uniq<SubqueryRef>(ParseSelect("SELECT " + StringUtil::Join(parts, ", ") + " WHERE false"));
}

//! The narrowing a bind applies over the dataset's kept rows: to its subject and property
static string Narrowed(const string &kind, const ReadScope &scope, const Value &aspect, const string &order) {
	return "SELECT * FROM glossql_cached(" + QuoteText(kind) + ", " + QuoteText(scope.dataset) + ", " +
	       (aspect.IsNull() ? string("NULL") : QuoteText(aspect.ToString())) + ") WHERE true" +
	       (scope.whole ? string() : " AND " + scope.subjects) + AspectFilter(aspect) + " ORDER BY " + order;
}

static unique_ptr<TableRef> GlossGlossaryBindReplace(ClientContext &context, TableFunctionBindInput &input) {
	bool all = input.inputs[2].GetValue<bool>();
	if (ReadFill::Of(context).filling) {
		return Shape(all ? RAW_SHAPE : COLLAPSED_SHAPE);
	}
	auto scope = ScopeOf(context, input.inputs[0]);
	return make_uniq<SubqueryRef>(ParseSelect(Narrowed(all ? "raw" : "collapsed", scope, input.inputs[1],
	                                                   all ? "subject, aspect, actor" : "subject, aspect")));
}

static TableFunction GlossGlossaryFunction() {
	TableFunction function("glossql_glossary", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BOOLEAN},
	                       nullptr);
	function.bind_replace = GlossGlossaryBindReplace;
	return function;
}

static unique_ptr<TableRef> GlossTestsBindReplace(ClientContext &context, TableFunctionBindInput &input) {
	if (ReadFill::Of(context).filling) {
		return Shape(TESTS_SHAPE);
	}
	auto scope = ScopeOf(context, input.inputs[0]);
	return make_uniq<SubqueryRef>(ParseSelect(Narrowed("tests", scope, input.inputs[1], "subject, aspect, test")));
}

static TableFunction GlossTestsFunction() {
	TableFunction function("glossql_tests", {LogicalType::VARCHAR, LogicalType::VARCHAR}, nullptr);
	function.bind_replace = GlossTestsBindReplace;
	return function;
}

//===--------------------------------------------------------------------===//
// Registration
//===--------------------------------------------------------------------===//

void RegisterGlossStore(ExtensionLoader &loader) {
	loader.RegisterFunction(GlossInitFunction());
	loader.RegisterFunction(PragmaFunction::PragmaCall("glossql", GlossQuery, {}));
	loader.RegisterFunction(GlossWriteFunctions());
	loader.RegisterFunction(GlossDeleteFunction());
	loader.RegisterFunction(GlossGlossaryFunction());
	loader.RegisterFunction(GlossTestsFunction());
	loader.RegisterFunction(GlossCachedFunction());
	loader.RegisterFunction(GlossKeptReadFunction());
	loader.RegisterFunction(GlossColumnsFunctions());
	loader.RegisterFunction(GlossKindFunction(0, KindBind<0>));
	loader.RegisterFunction(GlossKindFunction(1, KindBind<1>));
	loader.RegisterFunction(GlossKindFunction(2, KindBind<2>));
}

} // namespace duckdb
