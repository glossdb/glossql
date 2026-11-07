// The glossql store: the workspace is an attached DuckLake, and the store
// relations are tables in its metadata catalog, glossql_<relation> beside the
// ducklake_* tables, written by the statements the grammar adds, read by
// GLOSSARY() and TESTS(), and deleted from by DELETE FROM glossary.
//
// A dataset is a schema of the lake (SPEC §3): DECLARE DATASET fin creates
// schema fin, DuckDB's own USE binds it, and an unprefixed subject resolves
// against the connection's default schema. History is rows — every statement
// appends; the reads collapse to the newest row per value, human over agent
// over function, a test's detector judges the values at read (SPEC §7), and
// a value reads stale once its subject changed after the snapshot it was
// written at (SPEC §5.2), a join against the lake's column history.

#include "glossql_grammar.hpp"

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
			if (!views) {
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
//! text and numbers only, a seq that orders the rows. A declaration's SET
//! pairs land as one JSON object, `settings`; `key` is the columns a
//! re-declaration supersedes by, empty for a relation that is history. The
//! lake's schema `glossql` reads each as a typed view: JSON bodies, lists from
//! their JSON text. `stamp` is the column that dates a row, the store's
//! version reading its newest
struct StoreRelation {
	const char *name;
	const char *columns;
	const char *key;
	const char *view;
	const char *stamp;
};

static const StoreRelation STORE_RELATIONS[] = {
    {"datasets", "name VARCHAR NOT NULL, settings VARCHAR NOT NULL, declared_at TIMESTAMP NOT NULL", "name",
	 "seq, name, settings::JSON AS settings, declared_at", "declared_at"},
    {"kits",
	 "name VARCHAR NOT NULL, location VARCHAR NOT NULL, statements INTEGER NOT NULL, loaded_at TIMESTAMP NOT NULL, "
	 "hash VARCHAR",
	 "", "*", "loaded_at"},
    {"sources", "name VARCHAR NOT NULL, settings VARCHAR NOT NULL, declared_at TIMESTAMP NOT NULL", "name",
	 "seq, name, settings::JSON AS settings, declared_at", "declared_at"},
    {"recipes",
	 "dataset VARCHAR NOT NULL, table_name VARCHAR NOT NULL, source VARCHAR NOT NULL, settings VARCHAR NOT NULL, "
	 "sql VARCHAR NOT NULL, declared_at TIMESTAMP NOT NULL",
	 "dataset, table_name", "seq, dataset, table_name, source, settings::JSON AS settings, sql, declared_at",
	 "declared_at"},
    {"imports",
	 "dataset VARCHAR NOT NULL, table_name VARCHAR NOT NULL, mode VARCHAR NOT NULL, rows BIGINT NOT NULL, "
	 "dropped_rows_count BIGINT, lineage VARCHAR NOT NULL, snapshot_id BIGINT, imported_at TIMESTAMP NOT NULL",
	 "", "seq, dataset, table_name, mode, rows, dropped_rows_count, lineage::JSON AS lineage, snapshot_id, imported_at",
	 "imported_at"},
    {"aspects",
	 "name VARCHAR NOT NULL, kind VARCHAR NOT NULL, grains VARCHAR NOT NULL, condition VARCHAR, "
	 "schema VARCHAR NOT NULL, declared_at TIMESTAMP NOT NULL",
	 "name",
	 "seq, name, kind, from_json(grains, '[\"VARCHAR\"]') AS grains, condition::JSON AS condition, "
	 "schema::JSON AS schema, declared_at",
	 "declared_at"},
    {"glossary",
	 "dataset VARCHAR NOT NULL, subject VARCHAR NOT NULL, aspect VARCHAR NOT NULL, actor_kind VARCHAR NOT NULL, "
	 "actor_id VARCHAR, body VARCHAR NOT NULL, snapshot_id BIGINT, written_at TIMESTAMP NOT NULL",
	 "", "seq, dataset, subject, aspect, actor_kind, actor_id, body::JSON AS body, snapshot_id, written_at",
	 "written_at"},
    {"relationships",
	 "dataset VARCHAR NOT NULL, left_side VARCHAR NOT NULL, op VARCHAR NOT NULL, right_side VARCHAR NOT NULL, "
	 "declared_at TIMESTAMP NOT NULL",
	 "", "*", "declared_at"},
    {"functions",
	 "name VARCHAR NOT NULL, scope VARCHAR NOT NULL, script VARCHAR NOT NULL, returns VARCHAR, "
	 "declared_at TIMESTAMP NOT NULL",
	 "name, scope", "*", "declared_at"},
    {"measurements",
	 "dataset VARCHAR NOT NULL, function VARCHAR NOT NULL, subject VARCHAR NOT NULL, aspect VARCHAR NOT NULL, "
	 "pin VARCHAR, value VARCHAR NOT NULL, computed_at TIMESTAMP NOT NULL, reads VARCHAR",
	 "", "seq, dataset, function, subject, aspect, pin, value::JSON AS value, computed_at, reads", "computed_at"},
    {"tests",
	 "name VARCHAR NOT NULL, aspect VARCHAR NOT NULL, actors VARCHAR NOT NULL, detector VARCHAR, threshold DOUBLE, "
	 "declared_at TIMESTAMP NOT NULL",
	 "aspect", "seq, name, aspect, from_json(actors, '[\"VARCHAR\"]') AS actors, detector, threshold, declared_at",
	 "declared_at"},
};

static const StoreRelation &RelationOf(const string &name) {
	for (auto &relation : STORE_RELATIONS) {
		if (relation.name == name) {
			return relation;
		}
	}
	throw InternalException("glossql: no store relation '%s'", name);
}

//! The newest row per key of a declaration relation: what holds after re-declarations
static string Newest(const Workspace &ws, const string &relation) {
	return "(SELECT * FROM " + ws.Store(relation) + " QUALIFY row_number() OVER (PARTITION BY " +
	       RelationOf(relation).key + " ORDER BY seq DESC) = 1)";
}

//===--------------------------------------------------------------------===//
// Pins and reads (SPEC §6): what a measurement read, at the version it read it
//===--------------------------------------------------------------------===//

//! The store's relations as the lake reads them, quoted, comma-separated: the
//! view names in main that are no leg of their own, since their table's seq is
static string StoreViewNames() {
	vector<string> names;
	for (auto &relation : STORE_RELATIONS) {
		names.push_back(QuoteText(relation.name));
	}
	return StringUtil::Join(names, ", ");
}

string StoreLegNames() {
	vector<string> names;
	for (auto &relation : STORE_RELATIONS) {
		if (string(relation.name) != "measurements") {
			names.push_back(QuoteText("main." + string(relation.name)));
		}
	}
	return StringUtil::Join(names, ", ");
}

string LegsSQL(const Workspace &ws) {
	// a table's version is the last snapshot that wrote, altered or dropped it: the
	// lake's own change log, where compaction is not a write
	string changes = "(SELECT snapshot_id, v AS ids FROM (SELECT snapshot_id, unnest(map_keys(changes)) AS k,"
	                 " unnest(map_values(changes)) AS v FROM ducklake_snapshots(" +
	                 QuoteText(ws.lake) +
	                 ")) WHERE k IN ('tables_inserted_into', 'tables_deleted_from', 'tables_altered',"
	                 " 'tables_dropped', 'inlined_insert', 'inlined_delete'))";
	string sql = "SELECT s.schema_name || '.' || t.table_name AS name, greatest(t.begin_snapshot, coalesce("
	             "(SELECT max(c.snapshot_id) FROM " +
	             changes + " c WHERE list_contains(c.ids, t.table_id::VARCHAR)), 0))::VARCHAR AS version FROM " +
	             ws.Meta("table") + " t JOIN " + ws.Meta("schema") +
	             " s ON s.schema_id = t.schema_id AND s.end_snapshot IS NULL WHERE t.end_snapshot IS NULL";
	// a view's version is the snapshot that made or replaced it: a metric definition's
	// re-gloss with other sql is a new definition, and what was measured over
	// the view's columns moves with it. The store's own views are no leg
	sql += " UNION ALL SELECT s.schema_name || '.' || v.view_name, v.begin_snapshot::VARCHAR FROM " + ws.Meta("view") +
	       " v JOIN " + ws.Meta("schema") +
	       " s ON s.schema_id = v.schema_id AND s.end_snapshot IS NULL WHERE v.end_snapshot IS NULL"
	       " AND NOT (s.schema_name = 'main' AND v.view_name IN (" +
	       StoreViewNames() + "))";
	for (auto &relation : STORE_RELATIONS) {
		if (string(relation.name) == "measurements") {
			continue; // an output, never an input
		}
		sql += " UNION ALL SELECT 'main." + string(relation.name) + "', coalesce(max(seq)::VARCHAR, '-') FROM " +
		       ws.Store(relation.name);
	}
	return sql;
}

string StandsSQL(const Workspace &ws, const string &m) {
	// the names the row's reads select: its list; for ** (and a row without one)
	// every leg of its own pin and the dataset's every table; nothing for ''
	string parts = "string_split(coalesce(" + m + ".pin, ''), ',')";
	string name_of = "regexp_replace(p, ':[^:]*$', '')";
	string names = "(CASE WHEN " + m + ".reads IS NULL OR " + m + ".reads = '**' THEN list_append(list_transform(" +
	               parts + ", lambda p: " + name_of + "), '*') WHEN " + m +
	               ".reads = '' THEN []::VARCHAR[] ELSE string_split(" + m + ".reads, ',') END)";
	// a leg is selected by its name, or by * when it is one of the dataset's tables
	auto selected = [&](const string &name) {
		return "(list_contains(" + names + ", " + name + ") OR (list_contains(" + names + ", '*') AND starts_with(" +
		       name + ", " + m + ".dataset || '.')))";
	};
	string then_view = "coalesce(list_aggregate(list_sort(list_filter(" + parts + ", lambda p: " + selected(name_of) +
	                   ")), 'string_agg', ','), '')";
	string now_view = "coalesce((SELECT string_agg(l.name || ':' || l.version, ',' ORDER BY l.name || ':' || l.version)"
	                  " FROM legs l WHERE " +
	                  selected("l.name") + "), '')";
	// a measurement of an older body is never served: the function's newest declaration precedes it
	string declared = "coalesce(" + m + ".computed_at >= (SELECT max(f.declared_at) FROM " + ws.Store("functions") +
	                  " f WHERE f.name = " + m + ".function AND f.scope IN (" + m + ".dataset, 'GLOBAL')), true)";
	return "(" + then_view + " = " + now_view + " AND " + declared + ")";
}

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

//! glossql_init(): the store tables where missing, and the lake's views over
//! them. A view names the metadata catalog, so a view made under another
//! alias is made again
static void InitExec(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<InitState>();
	if (state.done) {
		return;
	}
	state.done = true;
	Inner inner(context);
	auto &ws = inner.ws;
	for (auto &relation : STORE_RELATIONS) {
		inner.Run("CREATE TABLE IF NOT EXISTS " + ws.Store(relation.name) + " (seq BIGINT NOT NULL, " +
		          relation.columns + ")");
	}
	// a store made before a kit carried its text's hash: the column added where it is missing
	inner.Run("ALTER TABLE " + ws.Store("kits") + " ADD COLUMN IF NOT EXISTS hash VARCHAR");
	idx_t views = 0;
	for (auto &relation : STORE_RELATIONS) {
		// the view's text names the metadata catalog, quoted or bare as the lake re-serializes it
		auto standing = inner.Scalar("SELECT count(*) FROM duckdb_views() WHERE database_name = " + QuoteText(ws.lake) +
		                             " AND schema_name = 'main' AND view_name = " + QuoteText(relation.name) +
		                             " AND (contains(sql, " + QuoteText(ws.meta + ".") + ") OR contains(sql, " +
		                             QuoteText(QuoteName(ws.meta) + ".") + "))");
		if (standing.GetValue<int64_t>() > 0) {
			continue;
		}
		inner.Run("CREATE OR REPLACE VIEW " + QuoteName(ws.lake) + ".main." + QuoteName(relation.name) + " AS SELECT " +
		          relation.view + " FROM " + ws.Store(relation.name));
		views++;
	}
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
		return std::move(result);
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
	return std::move(result);
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

//! An aspect as its newest row holds
struct AspectRow {
	string name;
	string schema;
	string kind;
	vector<string> grains;
	//! the x-kind the schema carries: 'grounding' marks a kit's metric definition schema
	string flavor;
};

static AspectRow LookupAspect(Inner &inner, const string &name) {
	auto row = inner.Row("SELECT schema, kind, from_json(grains, '[\"VARCHAR\"]'), schema::JSON->>'x-kind' FROM " +
	                     inner.Store("aspects") + " WHERE name = " + QuoteText(name) + " ORDER BY seq DESC LIMIT 1");
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
static vector<pair<string, string>> GroundingSchemas(Inner &inner) {
	vector<pair<string, string>> result;
	auto rows = inner.Run("SELECT name, schema FROM " + inner.Store("aspects") +
	                      " WHERE schema::JSON->>'x-kind' = 'grounding'"
	                      " QUALIFY row_number() OVER (PARTITION BY name ORDER BY seq DESC) = 1 ORDER BY name");
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
	if (aspect.flavor == "grounding") {
		throw InvalidInputException(
		    "glossql: aspect '%s' is a grounding schema: never glossed, it validates every VIEW gloss", aspect_name);
	}
	// A VIEW body meets the metric definition contract (sql or stopped, assumptions
	// disclosed), then every metric definition schema a kit declared; a FACT body meets
	// the aspect's own WITH schema
	if (aspect.kind == "VIEW") {
		auto error = JsonSchemaCheck(GROUNDING_CONTRACT, body);
		if (!error.empty()) {
			throw InvalidInputException("glossql: the body of %s does not meet the grounding contract: %s", aspect_name,
			                            error);
		}
		for (auto &schema : GroundingSchemas(inner)) {
			error = JsonSchemaCheck(schema.second, body);
			if (!error.empty()) {
				throw InvalidInputException("glossql: the body of %s does not conform to the grounding schema %s: %s",
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
		    inner.Scalar("SELECT count(*) FROM " + inner.Store("sources") + " WHERE name = " + QuoteText(subject))
		            .GetValue<int64_t>() > 0;
		string grain = pair                ? "RELATIONSHIP"
		               : parts.size() >= 3 ? "COLUMN"
		               : parts.size() == 2 ? "TABLE"
		               : is_source         ? "SOURCE"
		                                   : "DATASET";
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
		auto declared = inner.Scalar("SELECT count(*) FROM " + inner.Store("relationships") +
		                             " WHERE left_side || ' ' || op || ' ' || right_side = " + QuoteText(subject));
		if (declared.GetValue<int64_t>() == 0) {
			throw InvalidInputException("glossql: %s is not a declared relationship", subject);
		}
	}
	auto speakers = inner.Scalar("SELECT from_json(actors, '[\"VARCHAR\"]') FROM " + inner.Store("tests") +
	                             " WHERE aspect = " + QuoteText(aspect_name) + " ORDER BY seq DESC LIMIT 1");
	if (speakers.IsNull()) {
		return aspect;
	}
	auto admitted = ListValue::GetChildren(speakers);
	if (admitted.empty()) {
		return aspect;
	}
	vector<string> names;
	for (auto &speaker : admitted) {
		names.push_back(speaker.ToString());
		if (StringUtil::Lower(speaker.ToString()) == actor) {
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
//! a view under the metric's name carrying the serving metric definition's sql; a stop
//! ends it
static void Ground(Inner &inner, const string &dataset, const string &metric, const string &body) {
	auto landed =
	    inner.Scalar("SELECT count(*) FROM duckdb_tables() WHERE database_name = " + QuoteText(inner.ws.lake) +
		             " AND schema_name = " + QuoteText(dataset) + " AND table_name = " + QuoteText(metric));
	if (landed.GetValue<int64_t>() > 0) {
		throw InvalidInputException("glossql: a grounding on a landed table's name is refused: %s.%s", dataset, metric);
	}
	auto sql = JsonStringField(body, "sql");
	if (sql.empty()) {
		inner.Run("DROP VIEW IF EXISTS " + inner.Table(dataset, metric));
		return;
	}
	// the same sql again — an assumption or an axis re-glossed — keeps the view
	// and its version: the glosses and measurements on its columns stand
	auto standing = inner.Scalar("SELECT json_extract_string(body, '$.sql') FROM " + inner.Store("glossary") +
	                             " WHERE dataset = " + QuoteText(dataset) + " AND subject = " + QuoteText(dataset) +
	                             " AND aspect = " + QuoteText(metric) + " ORDER BY seq DESC LIMIT 1");
	auto stands = inner.Scalar("SELECT count(*) FROM " + inner.ws.Meta("view") + " v JOIN " + inner.ws.Meta("schema") +
	                           " s ON s.schema_id = v.schema_id WHERE s.schema_name = " + QuoteText(dataset) +
	                           " AND v.view_name = " + QuoteText(metric) + " AND v.end_snapshot IS NULL");
	if (!standing.IsNull() && standing.ToString() == sql && stands.GetValue<int64_t>() > 0) {
		return;
	}
	inner.Run("CREATE OR REPLACE VIEW " + inner.Table(dataset, metric) + " AS " + sql);
}

//! Admission of an aspect (SPEC §5.1): identical content is a no-op, a change
//! under glosses is refused, a WHEN names a declared sibling whose enum holds
//! the literal. Returns false for the no-op.
static bool AdmitAspect(Inner &inner, const vector<Value> &row) {
	auto name = row[0].ToString();
	auto standing = inner.Row("SELECT schema, kind, grains, condition FROM " + inner.Store("aspects") +
	                          " WHERE name = " + QuoteText(name) + " ORDER BY seq DESC LIMIT 1");
	if (!standing.empty()) {
		// compared as the store holds them: text, the grains as their JSON list
		vector<Value> incoming {row[1], row[2], Value(JsonList(row[3])), row[4]};
		bool same = true;
		for (idx_t i = 0; i < 4; i++) {
			auto &before = standing[i];
			auto &after = incoming[i];
			same = same && (before.IsNull() || after.IsNull() ? before.IsNull() == after.IsNull()
			                                                  : before.ToString() == after.ToString());
		}
		if (same) {
			return false;
		}
		auto glossed =
		    inner.Scalar("SELECT count(*) FROM " + inner.Store("glossary") + " WHERE aspect = " + QuoteText(name));
		if (glossed.GetValue<int64_t>() > 0) {
			throw InvalidInputException("glossql: aspect '%s' has glosses: delete them before changing it", name);
		}
	}
	if (JsonStringField(row[1].ToString(), "x-kind") == "grounding") {
		// existing bodies never silently stop matching their schema (SPEC §5.1):
		// every current VIEW value meets the schema before it is declared
		auto rows = inner.Run("SELECT aspect, subject, body FROM " + inner.Store("glossary") +
		                      " WHERE aspect IN (SELECT name FROM " + inner.Store("aspects") +
		                      " WHERE kind = 'VIEW') QUALIFY row_number() OVER (PARTITION BY subject, aspect,"
		                      " actor_kind ORDER BY seq DESC) = 1 ORDER BY seq");
		while (auto chunk = rows->Fetch()) {
			for (idx_t i = 0; i < chunk->size(); i++) {
				auto error = JsonSchemaCheck(row[1].ToString(), chunk->GetValue(2, i).ToString());
				if (!error.empty()) {
					throw InvalidInputException("glossql: grounding schema '%s' would strand the %s gloss on %s: %s",
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

	string outcome;
	Inner inner(context);
	if (bind.table == "aspects" && !AdmitAspect(inner, bind.row)) {
		output.data[0].Append(Value("aspect " + bind.row[0].ToString() + " unchanged: no-op"));
		return;
	}

	// identity is content (SPEC §3): an unchanged declaration is a no-op, so a script replays silently
	if (bind.table == "sources") {
		auto standing = inner.Row("SELECT settings FROM " + inner.Store("sources") +
		                          " WHERE name = " + bind.row[0].ToSQLString() + " ORDER BY seq DESC LIMIT 1");
		if (!standing.empty() && standing[0].ToString() == json(bind.row[1])) {
			output.data[0].Append(Value("source " + bind.row[0].ToString() + " unchanged: no-op"));
			return;
		}
	}
	if (bind.table == "relationships") {
		auto left = subject(bind.row[0]);
		auto right = subject(bind.row[2]);
		auto standing = inner.Scalar(
		    "SELECT count(*) FROM " + inner.Store("relationships") + " WHERE left_side = " + QuoteText(left) +
		    " AND op = " + bind.row[1].ToSQLString() + " AND right_side = " + QuoteText(right));
		if (standing.GetValue<int64_t>() > 0) {
			output.data[0].Append(
			    Value("relationship " + left + " " + bind.row[1].ToString() + " " + right + " unchanged: no-op"));
			return;
		}
	}

	vector<string> values;
	if (bind.table == "glossary") {
		// (subject, aspect) from the statement, the actor from the connection, then the body
		auto actor = ActorKindOf(context);
		auto resolved = subject(bind.row[0]);
		auto body = json(bind.row[2]);
		auto aspect = AdmitGloss(context, inner, bind.row[1].ToString(), resolved, body, actor);
		// the catalog snapshot at write time (SPEC §5.2): a column or table carries it, a dataset or pair path none
		bool pair = resolved.find(" -> ") != string::npos || resolved.find(" <-> ") != string::npos;
		auto snapshot = !pair && resolved.find('.') != string::npos ? inner.Snapshot() : Value();
		values = {
		    QuoteText(DatasetOf(context, resolved)), QuoteText(resolved), bind.row[1].ToSQLString(), QuoteText(actor),
		    ActorIdOf(context).ToSQLString(),        QuoteText(body),     snapshot.ToSQLString()};
		if (aspect.kind == "VIEW") {
			Ground(inner, resolved, aspect.name, body);
		}
		outcome = "glossed " + bind.row[1].ToString() + " on " + resolved + " as " + actor;
	} else if (bind.table == "aspects") {
		// (name, schema, kind, grains, condition) from the statement; the schema last in the row, being the long one
		values = {bind.row[0].ToSQLString(), bind.row[2].ToSQLString(), QuoteText(JsonList(bind.row[3])),
		          bind.row[4].ToSQLString(), QuoteText(json(bind.row[1]))};
		outcome = "declared aspect " + bind.row[0].ToString();
	} else if (bind.table == "relationships") {
		auto left = subject(bind.row[0]);
		auto right = subject(bind.row[2]);
		AdmitEndpoint(context, left);
		AdmitEndpoint(context, right);
		values = {QuoteText(DatasetOf(left)), QuoteText(left), bind.row[1].ToSQLString(), QuoteText(right)};
		outcome = "declared relationship " + left + " " + bind.row[1].ToString() + " " + right;
	} else if (bind.table == "sources") {
		values = {bind.row[0].ToSQLString(), QuoteText(json(bind.row[1]))};
		outcome = "declared source " + bind.row[0].ToString();
	} else {
		throw InternalException("glossql: no write to '%s'", bind.table);
	}
	inner.Run("INSERT INTO " + inner.Store(bind.table) + " VALUES (" + inner.Seq(bind.table) + ", " +
	          StringUtil::Join(values, ", ") + ", now())");

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
// removal. The lake reads the store as views, and the engine deletes through
// a table only, so the statement runs on the store's table, where every write
// goes. Its result is a DELETE's: the rows removed
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
		return std::move(result);
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
	return std::move(result);
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
	auto count = inner.Scalar("DELETE FROM " + inner.Store("glossary") + " AS " + QuoteName(alias) +
	                          (bind.where.IsNull() ? "" : " WHERE " + bind.where.ToString()));
	output.data[0].Append(Value::BIGINT(count.GetValue<int64_t>()));
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
// on the second connection from the tests and detectors as they stand,
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

//! A source-grain row: its subject names a declared source and its aspect
//! declares ON SOURCE. Such a row reads and supersedes workspace-wide (SPEC
//! §5.1); a grainless aspect on a source name stays dataset-scoped
static string SourceGrainSQL(const Workspace &ws) {
	return "(subject IN (SELECT name FROM " + ws.Store("sources") + ") AND aspect IN (SELECT name FROM " +
	       Newest(ws, "aspects") + " WHERE list_contains(from_json(grains, '[\"VARCHAR\"]'), 'SOURCE')))";
}

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
static string LandingsSQL(const Workspace &ws) {
	return "SELECT dataset, table_name, lineage, snapshot_id, seq FROM " + ws.Store("imports");
}

//! Whether a glossary row's subject changed after the snapshot it was written
//! at (SPEC §5.2, §3): a column retyped or dropped, a table one of whose
//! columns was, or a column re-derived, the lineage behind it in the newest
//! load differing from the one in the load that held at the write;
//! rows arriving under the same shape and derivation do not age it. A view, or
//! a column of one, changed when the definition that held at the write was
//! replaced or dropped. Dataset subjects and pair paths never do
static string StaleSQL() {
	string at = " WHERE schema_name = split_part(g.subject, '.', 1) AND table_name = split_part(g.subject, '.', 2)";
	// the column is all that follows the second dot: its name may hold one (SPEC §4)
	string column_of =
	    "substr(g.subject, length(split_part(g.subject, '.', 1)) + length(split_part(g.subject, '.', 2)) + 3)";
	string column = at + " AND column_name = " + column_of;
	string then_live =
	    " AND begin_snapshot <= g.snapshot_id AND (end_snapshot IS NULL OR end_snapshot > g.snapshot_id)";
	string then_type = "(SELECT column_type FROM cols" + column + then_live + " LIMIT 1)";
	string now_type = "(SELECT column_type FROM cols" + column + " AND end_snapshot IS NULL LIMIT 1)";
	string column_stale =
	    then_type + " IS NOT NULL AND (" + now_type + " IS NULL OR " + now_type + " <> " + then_type + ")";
	string table_stale =
	    "EXISTS (SELECT 1 FROM cols a WHERE a.schema_name = split_part(g.subject, '.', 1)"
	    " AND a.table_name = split_part(g.subject, '.', 2) AND a.begin_snapshot <= g.snapshot_id"
	    " AND (a.end_snapshot IS NULL OR a.end_snapshot > g.snapshot_id)"
	    " AND NOT EXISTS (SELECT 1 FROM cols b WHERE b.schema_name = a.schema_name AND b.table_name = a.table_name"
	    " AND b.column_name = a.column_name AND b.column_type = a.column_type AND b.end_snapshot IS NULL))";
	// re-derivation: the lineage of the load that held at the write against the newest load's
	string landed = " FROM landings i WHERE i.dataset = split_part(g.subject, '.', 1)"
	                " AND i.table_name = split_part(g.subject, '.', 2)";
	string then_lineage = "(SELECT i.lineage" + landed +
	                      " AND i.snapshot_id <= g.snapshot_id ORDER BY i.snapshot_id DESC, i.seq DESC LIMIT 1)::JSON";
	string now_lineage = "(SELECT i.lineage" + landed + " ORDER BY i.snapshot_id DESC, i.seq DESC LIMIT 1)::JSON";
	auto derivation = [](const string &lineage, const string &key) {
		return "json_extract_string(" + lineage + ", '$.\"' || " + key + " || '\"')";
	};
	// a view's shape is its definition: stale once the one that held at the write is no longer the live one
	string view_at = " FROM views v WHERE v.schema_name = split_part(g.subject, '.', 1)"
	                 " AND v.view_name = split_part(g.subject, '.', 2) AND v.begin_snapshot <= g.snapshot_id";
	string view_stale = "(EXISTS (SELECT 1" + view_at +
	                    " AND (v.end_snapshot IS NULL OR v.end_snapshot > g.snapshot_id)) AND NOT EXISTS (SELECT 1" +
	                    view_at + " AND v.end_snapshot IS NULL))";
	string landed_twice = then_lineage + " IS NOT NULL AND " + now_lineage + " IS NOT NULL";
	string column_rederived = "(" + landed_twice + " AND " + derivation(then_lineage, column_of) +
	                          " IS DISTINCT FROM " + derivation(now_lineage, column_of) + ")";
	string table_rederived = "(" + landed_twice + " AND EXISTS (SELECT 1 FROM (SELECT unnest(json_keys(" +
	                         then_lineage + ")) AS k) WHERE " + derivation(then_lineage, "k") + " IS DISTINCT FROM " +
	                         derivation(now_lineage, "k") + "))";
	return "CASE WHEN g.snapshot_id IS NULL OR g.subject LIKE '% -> %' OR g.subject LIKE '% <-> %' THEN false"
	       " WHEN len(string_split(g.subject, '.')) >= 3 THEN (" +
	       column_stale + ") OR " + column_rederived + " OR " + view_stale +
	       " WHEN len(string_split(g.subject, '.')) = 2 THEN " + table_stale + " OR " + table_rederived + " OR " +
	       view_stale + " ELSE false END";
}

//! Every value in scope, newest per speaker: the glossary's agent and human
//! rows, and the function values the measurements hold; each with its
//! staleness
static string SlotsSQL(const ReadScope &scope, const string &aspect_filter) {
	return "(WITH cols AS (" + ColumnsSQL(scope.ws) + "), views AS (" + ViewsSQL(scope.ws) + "), landings AS (" +
	       LandingsSQL(scope.ws) + ") SELECT subject, aspect, actor_kind AS actor, body::JSON AS body, written_at, " +
	       StaleSQL() + " AS stale FROM " + scope.ws.Store("glossary") + " g WHERE " + scope.filter + aspect_filter +
	       " QUALIFY row_number() OVER (PARTITION BY subject, aspect, actor_kind ORDER BY seq DESC) = 1)"
	       " UNION ALL "
	       "(WITH legs AS (" +
	       LegsSQL(scope.ws) +
	       ") SELECT m.subject, m.aspect, 'function' AS actor, m.value::JSON AS body, m.computed_at AS written_at,"
	       " NOT " +
	       StandsSQL(scope.ws, "m") + " AS stale FROM " + scope.ws.Store("measurements") + " m WHERE " + scope.filter +
	       aspect_filter +
	       " QUALIFY row_number() OVER (PARTITION BY m.subject, m.aspect, m.function ORDER BY m.seq DESC) = 1)";
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

//! The tests as they stand, with their detector's script and their aspect's grains and condition
static vector<TestRow> Tests(Inner &inner, const Value &aspect) {
	vector<TestRow> result;
	auto rows = inner.Run(
	    "SELECT w.name, w.aspect, w.detector, w.threshold, (SELECT f.script FROM " + inner.Store("functions") +
	    " f WHERE f.name = w.detector AND f.returns IS NULL ORDER BY f.seq DESC LIMIT 1) AS script, "
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

static constexpr const char *TESTS_COLUMNS = "subject, aspect, test, band, score, computed_at, current, error";

//! The result `error` (SPEC §7.2): one row per subject holding values, score 0,
//! the failure text; the detector's failure is its test's result, never the read's
//! `current` on a result (SPEC §7.2): every function value the result read
//! holds at the read's pin; true where none spoke
static string CurrentSQL(const string &slots) {
	return "coalesce((SELECT bool_and(c.current) FROM (" + slots +
	       ") c WHERE c.subject = d.subject AND c.speaker = 'function'), true) AS current";
}

static string ErrorVerdictSQL(const string &head, const string &slots, const string &failure) {
	return "SELECT DISTINCT d.subject, " + head +
	       "'error' AS band, 0.0::DOUBLE AS score, now()::TIMESTAMP AS computed_at, " + CurrentSQL(slots) + ", " +
	       QuoteText(failure) + " AS error FROM (" + slots + ") d";
}

static bool InTestsSchema(const Value &subject, const Value &band, const Value &score) {
	if (subject.IsNull() || band.IsNull() || score.IsNull()) {
		return false;
	}
	auto colour = band.ToString();
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
static constexpr const char *HOISTED = "slots_all";

//! A test's values as a detector reads them, from the hoisted relation
static string TestSlotsSQL(const string &relation, const string &aspect) {
	return "SELECT subject, aspect, actor AS speaker, body, NOT stale AS current FROM " + relation +
	       " WHERE aspect = " + QuoteText(aspect);
}

static string VerdictsSQL(Inner &inner, const vector<TestRow> &tests, const ReadScope &scope) {
	vector<string> parts;
	// the detectors run at bind on the inner connection, over the scope's values
	// stored there once as a temporary table
	bool landed = false;
	for (auto &test : tests) {
		if (test.detector.empty()) {
			continue;
		}
		auto slots = TestSlotsSQL(HOISTED, test.aspect);
		auto head = "'" + test.aspect + "' AS aspect, '" + test.name + "' AS test, ";
		if (test.script.IsNull()) {
			parts.push_back(
			    ErrorVerdictSQL(head, slots, "detector " + test.detector + " is not a declared function"));
			continue;
		}
		if (!landed) {
			auto made = inner.con.Query("CREATE OR REPLACE TEMP TABLE glossql_slots AS " + SlotsSQL(scope, ""));
			if (made->HasError()) {
				throw InvalidInputException("glossql: the slots of %s could not be read: %s", scope.dataset,
				                            FirstLine(made->GetError()));
			}
			landed = true;
		}
		auto threshold = test.threshold.IsNull() ? string("NULL") : test.threshold.ToSQLString();
		auto script = ReplacePlaceholder(test.script.ToString(), "threshold", threshold);
		// the script is a derived table under the values CTE, so a body that opens
		// with its own WITH composes
		auto result = inner.con.Query(
		    "SELECT d.subject::VARCHAR, d.band::VARCHAR, d.score::DOUBLE FROM (WITH slots AS (" +
		    TestSlotsSQL("glossql_slots", test.aspect) + ") SELECT * FROM (" + script + ") AS q) AS d");
		if (result->HasError()) {
			parts.push_back(ErrorVerdictSQL(head, slots, FirstLine(result->GetError())));
			continue;
		}
		vector<string> rows;
		string failure;
		while (auto chunk = result->Fetch()) {
			for (idx_t i = 0; i < chunk->size() && failure.empty(); i++) {
				auto subject = chunk->GetValue(0, i);
				auto band = chunk->GetValue(1, i);
				auto score = chunk->GetValue(2, i);
				if (!InTestsSchema(subject, band, score)) {
					failure = "detector " + test.detector + " served (" + subject.ToString() + ", " +
					          band.ToString() + ", " + score.ToString() + "), outside the test-results schema";
					break;
				}
				// the score travels as text: a double round-trips through its shortest decimal form
				rows.push_back("(" + subject.ToSQLString() + ", " + band.ToSQLString() + ", " +
				               QuoteText(score.ToString()) + ")");
			}
		}
		if (!failure.empty()) {
			parts.push_back(ErrorVerdictSQL(head, slots, failure));
		} else if (!rows.empty()) {
			parts.push_back("SELECT d.subject, " + head +
			                "d.band, d.score::DOUBLE AS score, now()::TIMESTAMP AS computed_at, " + CurrentSQL(slots) +
			                ", NULL::VARCHAR AS error FROM (VALUES " + StringUtil::Join(rows, ", ") +
			                ") AS d(subject, band, score)");
		}
	}
	if (parts.empty()) {
		return "SELECT NULL::VARCHAR AS subject, NULL::VARCHAR AS aspect, NULL::VARCHAR AS test, "
		       "NULL::VARCHAR AS band, NULL::DOUBLE AS score, NULL::TIMESTAMP AS computed_at, "
		       "NULL::BOOLEAN AS current, NULL::VARCHAR AS error WHERE false";
	}
	return StringUtil::Join(parts, " UNION ALL ");
}

//! The subjects each tested aspect is required on, by the aspect's grain
//! (SPEC §5.1): what the read discloses as unassessed when nobody spoke
static string OwedSQL(const vector<TestRow> &tests, const ReadScope &scope) {
	vector<string> parts;
	for (auto &test : tests) {
		auto grains = test.grains;
		if (grains.empty()) {
			grains = {"DATASET", "TABLE", "COLUMN", "RELATIONSHIP", "SOURCE"};
		}
		auto aspect = ", " + QuoteText(test.aspect) + " AS aspect";
		auto dataset = QuoteText(scope.dataset);
		auto columns = " FROM glossql_columns(" + dataset + ")";
		vector<string> owed;
		for (auto &grain : grains) {
			if (grain == "COLUMN") {
				owed.push_back("SELECT " + dataset + " || '.' || table_name || '.' || column_name AS subject" + aspect +
				               columns);
			} else if (grain == "TABLE") {
				owed.push_back("SELECT DISTINCT " + dataset + " || '.' || table_name AS subject" + aspect + columns);
			} else if (grain == "DATASET") {
				owed.push_back("SELECT " + dataset + " AS subject" + aspect);
			} else if (grain == "RELATIONSHIP") {
				owed.push_back("SELECT left_side || ' ' || op || ' ' || right_side AS subject" + aspect + " FROM " +
				               scope.ws.Store("relationships") + " WHERE dataset = " + dataset);
			} else if (grain == "SOURCE") {
				owed.push_back("SELECT DISTINCT name AS subject" + aspect + " FROM " + scope.ws.Store("sources"));
			}
		}
		if (owed.empty()) {
			continue;
		}
		auto part = "SELECT subject, aspect FROM (" + StringUtil::Join(owed, " UNION ALL ") + ")";
		if (!test.condition_aspect.IsNull()) {
			// required only where the sibling's winning value, human over agent, carries the literal (SPEC §5.1)
			part += " WHERE subject IN (SELECT subject FROM (SELECT subject, body FROM " + string(HOISTED) +
			        " WHERE aspect = " + QuoteText(test.condition_aspect.ToString()) +
			        " AND actor IN ('human', 'agent') QUALIFY row_number() OVER (PARTITION BY subject"
			        " ORDER BY CASE actor WHEN 'human' THEN 0 ELSE 1 END, written_at DESC) = 1)"
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
//! withheld while the test's score exceeds its threshold; every gap visible
static string CollapsedSQL(Inner &inner, const vector<TestRow> &tests, const ReadScope &scope,
                           const Value &aspect) {
	auto aspect_filter = AspectFilter(aspect);
	auto owed = OwedSQL(tests, scope);
	if (!aspect.IsNull()) {
		owed = "SELECT * FROM (" + owed + ") WHERE aspect = " + QuoteText(aspect.ToString());
	}
	// an error is not a judgment (SPEC §7.2): it never withholds, and any other result outranks it;
	// a stale value is served and marked (SPEC §5.3)
	return "WITH " + string(HOISTED) + " AS MATERIALIZED (" + SlotsSQL(scope, "") + "), slots AS (SELECT * FROM " +
	       string(HOISTED) + " WHERE true" + aspect_filter +
	       "), picked AS (SELECT subject, aspect, body AS value, stale FROM slots QUALIFY row_number() OVER ("
	       "PARTITION BY subject, aspect ORDER BY CASE actor WHEN 'human' THEN 0 WHEN 'agent' THEN 1 ELSE 2 END,"
	       " written_at DESC) = 1), verdicts AS (" +
	       VerdictsSQL(inner, tests, scope) +
	       "), judged AS (SELECT v.subject, v.aspect, v.band, v.score, w.threshold FROM verdicts v LEFT JOIN " +
	       Newest(scope.ws, "tests") +
	       " w ON w.name = v.test QUALIFY row_number() OVER (PARTITION BY v.subject, v.aspect"
	       " ORDER BY v.band = 'error', v.score DESC) = 1), owed AS (" +
	       owed +
	       ") SELECT * FROM ("
	       "SELECT p.subject, p.aspect, CASE WHEN j.score > j.threshold THEN NULL ELSE p.value END AS value,"
	       " j.band, j.score, CASE WHEN j.score > j.threshold THEN 'contested' WHEN p.stale THEN 'stale'"
	       " ELSE 'current' END AS state"
	       " FROM picked p LEFT JOIN judged j ON j.subject = p.subject AND j.aspect = p.aspect"
	       " UNION ALL "
	       "SELECT o.subject, o.aspect, NULL::JSON AS value, NULL::VARCHAR AS band, NULL::DOUBLE AS score,"
	       " 'unassessed' AS state FROM owed o WHERE NOT EXISTS"
	       " (SELECT 1 FROM picked p WHERE p.subject = o.subject AND p.aspect = o.aspect)"
	       ") ORDER BY subject, aspect";
}

//! The raw read: one row per value — (subject, aspect, kind, test, actor, body, written_at, current)
static string RawSQL(const ReadScope &scope, const Value &aspect) {
	return "SELECT s.subject, s.aspect, a.kind, w.name AS test, s.actor, s.body, s.written_at,"
	       " NOT s.stale AS current FROM (" +
	       SlotsSQL(scope, AspectFilter(aspect)) + ") s LEFT JOIN " + Newest(scope.ws, "aspects") +
	       " a ON a.name = s.aspect LEFT JOIN " + Newest(scope.ws, "tests") +
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
		return std::move(result);
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
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> ColumnsInit(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind = input.bind_data->Cast<ColumnsBindData>();
	auto state = make_uniq<ColumnsState>();
	state->rows = DatasetColumns(context, bind.dataset, bind.table, bind.views);
	return std::move(state);
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

//! The store's version: the lake's current snapshot and, per store relation,
//! the highest seq, the row count and the newest stamp — everything a read's
//! rows depend on, through the stale rule, the legs, the tests and the
//! detectors. A write raises the seq; a delete lowers the count, and a row
//! written after one is stamped later than the row whose seq it takes over
static string StoreVersionSQL(const Workspace &ws) {
	string sql = "SELECT (SELECT id FROM ducklake_current_snapshot(" + QuoteText(ws.lake) + "))::VARCHAR";
	for (auto &relation : STORE_RELATIONS) {
		sql += " || ':' || (SELECT coalesce(max(seq), 0)::VARCHAR || '/' || count(*)::VARCHAR || '/' || coalesce(max(" +
		       string(relation.stamp) + ")::VARCHAR, '') FROM " + ws.Store(relation.name) + ")";
	}
	return sql;
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
	if (kind == "raw") {
		return RawSQL(whole, Value());
	}
	if (kind == "collapsed") {
		return CollapsedSQL(inner, Tests(inner, Value()), whole, Value());
	}
	if (kind == "tests") {
		return "WITH " + string(HOISTED) + " AS MATERIALIZED (" + SlotsSQL(whole, "") + ") SELECT " +
		       string(TESTS_COLUMNS) + " FROM (" + VerdictsSQL(inner, Tests(inner, Value()), whole) + ")";
	}
	throw InvalidInputException("glossql_cached serves a read: collapsed, raw or tests, not '%s'", kind);
}

//! One kind of read over a whole dataset: as kept while the store's version
//! holds, else computed on the inner connection and kept
static shared_ptr<CachedRead> Cached(ClientContext &context, Inner &inner, const string &kind, const ReadScope &whole) {
	auto &cache = ObjectCache::GetObjectCache(context);
	auto key = CacheKey(inner.ws.lake, kind, whole.dataset);
	ReadFill::Of(*inner.con.context).filling = true;
	auto version = inner.Scalar(StoreVersionSQL(inner.ws)).ToString();
	auto kept = cache.Get<CachedRead>(key);
	if (kept && kept->version == version) {
		return kept;
	}
	auto result = inner.Run(ReadSQL(inner, kind, whole));
	auto read = make_shared_ptr<CachedRead>();
	read->rows = make_shared_ptr<ColumnDataCollection>(Allocator::Get(context), result->GetTypes());
	while (auto chunk = result->Fetch()) {
		read->rows->Append(*chunk);
	}
	read->names = result->GetNames();
	read->types = result->GetTypes();
	read->version = version;
	cache.Put(key, read);
	return read;
}

//! glossql_cached(kind, dataset): a dataset's read as kept, scanned; the reads
//! narrow it in SQL
struct CachedBindData : public FunctionData {
	shared_ptr<CachedRead> read;

	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<CachedBindData>();
		result->read = read;
		return std::move(result);
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
	auto whole = WholeOf(ScopeOf(context, input.inputs[1]), inner.ws);
	auto result = make_uniq<CachedBindData>();
	result->read = Cached(context, inner, input.inputs[0].ToString(), whole);
	return_types = result->read->types;
	names = result->read->names;
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> CachedInit(ClientContext &context, TableFunctionInitInput &input) {
	auto state = make_uniq<CachedScanState>();
	state->rows = input.bind_data->Cast<CachedBindData>().read->rows;
	state->rows->InitializeScan(state->scan);
	return std::move(state);
}

static void CachedExec(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<CachedScanState>();
	state.rows->Scan(state.scan, output);
}

static TableFunction GlossCachedFunction() {
	return TableFunction("glossql_cached", {LogicalType::VARCHAR, LogicalType::VARCHAR}, CachedExec, CachedBind,
	                     CachedInit);
}

string StoreVersion(Inner &inner) {
	return inner.Scalar(StoreVersionSQL(inner.ws)).ToString();
}

//===--------------------------------------------------------------------===//
// A view kept: a dataset's view, its rows computed once per store version and
// kept in the object cache beside the reads, so that a cells read over the
// dataset's views — a metric's series, a plan's rows — scans what holds
// instead of computing every view from its tables on each call.
// The version is the reads' own: any write or delete moves it. A view's rows are a
// fraction of its tables' (a metric's series), so the entry is small, and
// the buffer pool counts and evicts it like a read's
//===--------------------------------------------------------------------===//

static string ViewKey(const string &lake, const string &dataset, const string &view) {
	return "glossql_view|" + lake + "|" + dataset + "|" + view;
}

//! glossql_kept_view(dataset, view, version): the view's rows as kept at that
//! version, else computed on the inner connection and kept. The version is
//! the caller's, read once for every view a cells read binds
static unique_ptr<FunctionData> KeptViewBind(ClientContext &context, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto dataset = input.inputs[0].ToString();
	auto view = input.inputs[1].ToString();
	auto version = input.inputs[2].ToString();
	auto &cache = ObjectCache::GetObjectCache(context);
	auto key = ViewKey(LakeName(context), dataset, view);
	auto kept = cache.Get<CachedRead>(key);
	if (!kept || kept->version != version) {
		Inner inner(context);
		auto result = inner.Run("SELECT * FROM " + inner.Table(dataset, view));
		auto read = make_shared_ptr<CachedRead>();
		read->rows = make_shared_ptr<ColumnDataCollection>(Allocator::Get(context), result->GetTypes());
		while (auto chunk = result->Fetch()) {
			read->rows->Append(*chunk);
		}
		read->names = result->GetNames();
		read->types = result->GetTypes();
		read->version = version;
		cache.Put(key, read);
		kept = std::move(read);
	}
	auto result = make_uniq<CachedBindData>();
	result->read = kept;
	return_types = kept->types;
	names = kept->names;
	return std::move(result);
}

static TableFunction GlossKeptViewFunction() {
	return TableFunction("glossql_kept_view", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                     CachedExec, KeptViewBind, CachedInit);
}

//! The columns of each kind of read, as the empty relation a bind serves while
//! this connection fills a read
static const char *const COLLAPSED_SHAPE = "subject VARCHAR, aspect VARCHAR, value JSON, band VARCHAR, score DOUBLE, "
                                           "state VARCHAR";
static const char *const RAW_SHAPE = "subject VARCHAR, aspect VARCHAR, kind VARCHAR, test VARCHAR, actor VARCHAR, "
                                     "body JSON, written_at TIMESTAMP, current BOOLEAN";
static const char *const TESTS_SHAPE = "subject VARCHAR, aspect VARCHAR, test VARCHAR, band VARCHAR, "
                                        "score DOUBLE, computed_at TIMESTAMP, current BOOLEAN, error VARCHAR";

static unique_ptr<TableRef> Shape(const string &columns) {
	vector<string> parts;
	for (auto &column : StringUtil::Split(columns, ", ")) {
		auto space = column.find(' ');
		parts.push_back("NULL::" + column.substr(space + 1) + " AS " + column.substr(0, space));
	}
	return make_uniq<SubqueryRef>(ParseSelect("SELECT " + StringUtil::Join(parts, ", ") + " WHERE false"));
}

//! The narrowing a bind applies over the dataset's kept rows
static string Narrowed(const string &kind, const ReadScope &scope, const Value &aspect, const string &order) {
	return "SELECT * FROM glossql_cached(" + QuoteText(kind) + ", " + QuoteText(scope.dataset) + ") WHERE true" +
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
	loader.RegisterFunction(GlossKeptViewFunction());
	loader.RegisterFunction(GlossColumnsFunctions());
}

} // namespace duckdb
