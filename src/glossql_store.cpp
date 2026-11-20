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
// deleted. The reads serve the current rows, human over agent over function,
// and each test's stored result (SPEC §7); a value reads stale once the
// definition of its subject changed after the snapshot it was written at
// (SPEC §5.2), a join against the lake's catalog history.

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
#include <mutex>
#include <unordered_map>
#include <functional>

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
	try {
		auto &inner = Inner::Of(context);
		return !inner
		            .Row("SELECT 1 FROM " + QuoteName(inner.ws.lake) + ".main.sources WHERE name = " + QuoteText(name) +
		                 " LIMIT 1")
		            .empty();
	} catch (std::exception &) {
		// no store, no sources
		return false;
	}
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
    {"properties",
	 "name VARCHAR NOT NULL, kind VARCHAR NOT NULL, grains VARCHAR NOT NULL, condition VARCHAR, "
	 "schema VARCHAR NOT NULL, declared_at TIMESTAMP NOT NULL, declared_by VARCHAR",
	 "begin_snapshot, name, kind, from_json(grains, '[\"VARCHAR\"]') AS grains, condition::JSON AS condition, "
	 "schema::JSON AS schema, declared_at, declared_by"},
    {"glossary",
	 "dataset VARCHAR NOT NULL, subject VARCHAR NOT NULL, property VARCHAR NOT NULL, actor_kind VARCHAR NOT NULL, "
	 "actor_id VARCHAR, body VARCHAR NOT NULL, written_at TIMESTAMP NOT NULL",
	 "begin_snapshot, dataset, subject, property, actor_kind, actor_id, body::JSON AS body, written_at"},
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
    // id: the result's own, as one write may store many; read_at: the lake's newest snapshot when the
    // computation began to read; changed_at: the snapshot its value and error last changed at, kept by a
    // computation that returns the same
    {"results",
	 "id BIGINT NOT NULL, dataset VARCHAR NOT NULL, function VARCHAR NOT NULL, subject VARCHAR NOT NULL, property "
	 "VARCHAR NOT NULL, value VARCHAR, computed_at TIMESTAMP NOT NULL, error VARCHAR, read_at BIGINT NOT NULL, "
	 "changed_at BIGINT NOT NULL",
	 "begin_snapshot, id, dataset, function, subject, property, value::JSON AS value, computed_at, error, read_at, "
	 "changed_at"},
    // one row per node a stored result read (SPEC §6), by the result's id: the rows begin with their
    // result and end with it
    {"inputs", "result BIGINT NOT NULL, asset VARCHAR NOT NULL, what VARCHAR NOT NULL",
	 "begin_snapshot, result, asset, what"},
    // one row per lineage edge a stored result holds, a view's lineage the relations and columns it
    // reads, by the result's id: the rows begin and end with their result
    {"edges", "result BIGINT NOT NULL, \"from\" VARCHAR NOT NULL, \"to\" VARCHAR NOT NULL, kind VARCHAR NOT NULL",
	 "begin_snapshot, result, \"from\", \"to\", kind"},
    {"tests",
	 "name VARCHAR NOT NULL, property VARCHAR NOT NULL, actors VARCHAR NOT NULL, detector VARCHAR, threshold DOUBLE, "
	 "declared_at TIMESTAMP NOT NULL, declared_by VARCHAR",
	 "begin_snapshot, name, property, from_json(actors, '[\"VARCHAR\"]') AS actors, detector, threshold, declared_at, "
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

//! A property as its newest row holds
struct PropertyRow {
	string name;
	string schema;
	string kind;
	vector<string> grains;
};

static PropertyRow LookupProperty(Inner &inner, const string &name) {
	auto row = inner.Row("SELECT schema, kind, from_json(grains, '[\"VARCHAR\"]') FROM " + inner.Store("properties") +
	                     " WHERE name = " + QuoteText(name) + " AND end_snapshot IS NULL");
	if (row.empty()) {
		throw InvalidInputException("glossql: property '%s' is not declared", name);
	}
	PropertyRow result;
	result.name = name;
	result.schema = row[0].ToString();
	result.kind = row[1].ToString();
	for (auto &grain : ListValue::GetChildren(row[2])) {
		result.grains.push_back(grain.ToString());
	}
	return result;
}

static bool HasGrain(const PropertyRow &property, const string &grain) {
	return std::find(property.grains.begin(), property.grains.end(), grain) != property.grains.end();
}

//! Admission of a gloss (SPEC §5.2, §7.1): the property is declared and not a
//! MEASUREMENT; the body conforms to the property's schema (FACT) or the
//! metric definition contract (VIEW); a TABLE or COLUMN grain takes stored
//! subjects only; a test's BY admits the actor's kind
static PropertyRow AdmitGloss(ClientContext &context, Inner &inner, const string &property_name, const string &subject,
                              const string &body, const string &actor) {
	auto property = LookupProperty(inner, property_name);
	if (property.kind == "MEASUREMENT") {
		throw InvalidInputException(
		    "glossql: property '%s' is a MEASUREMENT: never glossed, its value is its function's", property_name);
	}
	// A VIEW body meets the metric definition contract (sql or stopped, assumptions
	// disclosed); a FACT body meets the property's own WITH schema
	if (property.kind == "VIEW") {
		auto error = JsonSchemaCheck(DEFINITION_CONTRACT, body);
		if (!error.empty()) {
			throw InvalidInputException("glossql: the body of %s does not meet the definition contract: %s",
			                            property_name, error);
		}
	} else {
		auto error = JsonSchemaCheck(property.schema, body);
		if (!error.empty()) {
			throw InvalidInputException("glossql: the body of %s does not conform to the property's schema: %s",
			                            property_name, error);
		}
	}
	auto parts = SubjectParts(subject);
	bool pair = subject.find(" -> ") != string::npos || subject.find(" <-> ") != string::npos;
	if (property.kind == "VIEW" && (parts.size() != 1 || pair)) {
		throw InvalidInputException("glossql: a VIEW property takes the dataset as its subject, not %s", subject);
	}
	// the grain gate (SPEC §5.1): the subject's class is one the property declared. A
	// bare name is a source when one is declared by it, a dataset when a schema is,
	// and either when both are: the declaration says which is meant
	if (!property.grains.empty()) {
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
		    HasGrain(property, grain) || (is_source && SchemaExists(context, subject) && HasGrain(property, "DATASET"));
		if (!admitted) {
			throw InvalidInputException("glossql: property '%s' is declared ON %s; %s is a %s", property_name,
			                            StringUtil::Join(property.grains, ", "), subject, StringUtil::Lower(grain));
		}
	}
	if (!pair && parts.size() == 2 && HasGrain(property, "TABLE") && !RelationEntry(context, parts[0], parts[1])) {
		throw InvalidInputException("glossql: %s names no table of %s", subject, parts[0]);
	}
	if (!pair && parts.size() == 3 && HasGrain(property, "COLUMN")) {
		auto entry = RelationEntry(context, parts[0], parts[1]);
		if (!entry || ColumnOf(context, *entry, parts[2]).empty()) {
			throw InvalidInputException("glossql: %s names no column of %s.%s", subject, parts[0], parts[1]);
		}
	}
	// only declared relationships exist (SPEC §4): a pair path is a subject once declared
	if (pair && HasGrain(property, "RELATIONSHIP")) {
		auto declared = inner.Scalar("SELECT count(*) FROM " + inner.Current("relationships") +
		                             " WHERE left_side || ' ' || op || ' ' || right_side = " + QuoteText(subject));
		if (declared.GetValue<int64_t>() == 0) {
			throw InvalidInputException("glossql: %s is not a declared relationship", subject);
		}
	}
	auto actor_kinds = inner.Scalar("SELECT from_json(actors, '[\"VARCHAR\"]') FROM " + inner.Current("tests") +
	                                " WHERE property = " + QuoteText(property_name));
	if (actor_kinds.IsNull()) {
		return property;
	}
	auto admitted = ListValue::GetChildren(actor_kinds);
	if (admitted.empty()) {
		return property;
	}
	vector<string> names;
	for (auto &actor_kind : admitted) {
		names.push_back(actor_kind.ToString());
		if (StringUtil::Lower(actor_kind.ToString()) == actor) {
			return property;
		}
	}
	throw InvalidInputException("glossql: the test on '%s' admits %s, not %s", property_name,
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
//! the annotations and results on its columns stay current. The view's
//! lineage is stored in the same write where it was planned for the sql that
//! now serves (`admitted`), read at this write; a view ended takes its lineage
//! with it
static void DefineView(CatalogWrite &write, const string &dataset, const string &metric,
                       const ViewLineageResult &admitted, const string &admitted_sql) {
	auto &inner = write.inner;
	auto &ws = inner.ws;
	auto serving =
	    inner.Scalar("SELECT body FROM " + inner.Current("glossary") + " WHERE dataset = " + QuoteText(dataset) +
		             " AND subject = " + QuoteText(dataset) + " AND property = " + QuoteText(metric) +
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

//! Admission of a property (SPEC §5.1): identical content is a no-op, a change
//! under glosses is refused, a WHEN names a declared sibling whose enum holds
//! the literal. Returns false for the no-op.
static bool AdmitProperty(Inner &inner, const vector<Value> &row) {
	auto name = row[0].ToString();
	auto current = inner.Row("SELECT schema, kind, grains, condition FROM " + inner.Current("properties") +
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
		    inner.Scalar("SELECT count(*) FROM " + inner.Current("glossary") + " WHERE property = " + QuoteText(name));
		if (glossed.GetValue<int64_t>() > 0) {
			throw InvalidInputException("glossql: property '%s' has glosses: delete them before changing it", name);
		}
	}
	if (!row[4].IsNull()) {
		auto condition = row[4].ToString();
		if (JsonStringField(condition, "property").empty()) {
			// WHEN TYPE narrows columns by their data type: the grain is COLUMN alone
			if (JsonList(row[3]) != "[\"COLUMN\"]") {
				throw InvalidInputException("glossql: WHEN TYPE narrows columns; property '%s' is declared ON %s", name,
				                            JsonList(row[3]));
			}
		} else {
			auto sibling = LookupProperty(inner, JsonStringField(condition, "property"));
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

//! The sources and properties the reads look names up in, one row of lists: every source declared (names),
//! the properties that declare ON SOURCE now (props), every property ever declared of kind VIEW (views)
static string SourcesSQL(const Workspace &ws) {
	return "gl_src AS MATERIALIZED (SELECT coalesce((SELECT list(name) FROM " + ws.Store("sources") +
	       "), []) AS names, coalesce((SELECT list(name) FROM " + ws.Store("properties") +
	       " WHERE end_snapshot IS NULL AND list_contains(from_json(grains, '[\"VARCHAR\"]'), 'SOURCE')), []) AS "
	       "props, coalesce((SELECT list(name) FROM " +
	       ws.Store("properties") + " WHERE kind = 'VIEW'), []) AS views)";
}

//! A source-grain row: its subject names a declared source and its property
//! declares ON SOURCE. Such a row reads and supersedes workspace-wide (SPEC
//! §5.1); a grainless property on a source name stays dataset-scoped. Over gl_src (SourcesSQL)
static string SourceGrainSQL() {
	return "(list_contains((SELECT names FROM gl_src), subject) AND list_contains((SELECT props FROM gl_src), "
	       "property))";
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
	// touches the catalogue alone. A name no property declares, on a dataset, is a view definition: the
	// write declares its property (SPEC §5.2)
	ViewLineageResult admitted;
	string admitted_sql;
	bool defines = false;
	if (bind.table == "glossary") {
		auto resolved = subject(bind.row[0]);
		auto kind = inner.Scalar("SELECT kind FROM " + inner.Current("properties") +
		                         " WHERE name = " + bind.row[1].ToSQLString());
		defines = resolved.find('.') == string::npos && resolved.find(' ') == string::npos &&
		          (kind.IsNull() ? SchemaExists(context, resolved) : kind.ToString() == "VIEW");
		if (defines) {
			admitted = AdmitDefinition(inner, resolved, bind.row[1].ToString(), json(bind.row[2]));
			admitted_sql = JsonStringField(json(bind.row[2]), "sql");
		}
	}
	string outcome;
	inner.Write(context, [&](CatalogWrite &write) {
		outcome.clear();
		if (bind.table == "properties") {
			if (!AdmitProperty(inner, bind.row)) {
				outcome = "property " + bind.row[0].ToString() + " unchanged: no-op";
				return;
			}
			// (name, schema, kind, grains, condition) from the statement; the schema last in the row, being the long
			// one
			write.End("properties", "name = " + bind.row[0].ToSQLString());
			write.Insert("properties", bind.row[0].ToSQLString() + ", " + bind.row[2].ToSQLString() + ", " +
			                               QuoteText(JsonList(bind.row[3])) + ", " + bind.row[4].ToSQLString() + ", " +
			                               QuoteText(json(bind.row[1])) + ", now(), " + QuoteText(DeclaredBy(context)));
			outcome = "declared property " + bind.row[0].ToString();
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
			// (subject, property) from the statement, the actor from the connection, then the body
			auto actor = ActorKindOf(context);
			auto resolved = subject(bind.row[0]);
			auto body = json(bind.row[2]);
			// a definition of a name no property declares declares it, by its actor, in this write
			auto declared = inner.Scalar("SELECT count(*) FROM " + inner.Current("properties") +
			                             " WHERE name = " + bind.row[1].ToSQLString());
			if (defines && declared.GetValue<int64_t>() == 0) {
				write.Insert("properties", bind.row[1].ToSQLString() + ", 'VIEW', '[]', NULL, '{}', now(), " +
				                               QuoteText(DeclaredBy(context)));
			}
			auto property = AdmitGloss(context, inner, bind.row[1].ToString(), resolved, body, actor);
			auto dataset = DatasetOf(context, resolved);
			// a value is superseded per (subject, property, actor kind) (SPEC §5.2), within its dataset;
			// a source-grain row is superseded workspace-wide (SPEC §5.1)
			auto source_grain =
			    inner.Scalar("WITH " + SourcesSQL(inner.ws) + " SELECT " + SourceGrainSQL() + " FROM (SELECT " +
				             QuoteText(resolved) + " AS subject, " + QuoteText(property.name) + " AS property)");
			auto held = "subject = " + QuoteText(resolved) + " AND property = " + QuoteText(property.name) +
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
			write.Insert("glossary", QuoteText(dataset) + ", " + QuoteText(resolved) + ", " + QuoteText(property.name) +
			                             ", " + QuoteText(actor) + ", " + ActorIdOf(context).ToSQLString() + ", " +
			                             QuoteText(body) + ", now()");
			if (property.kind == "VIEW") {
				DefineView(write, resolved, property.name, admitted, admitted_sql);
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
// (bind_replace), so they compose with any SQL. They read stored rows: the
// annotations, the results, and each test's stored result (SPEC §7.2).
// The marks are planned once per statement: an annotation is stale once the
// definition of its subject changed after the snapshot it was written at, a
// stored result is out of date once a node it read changed after it read
// (SPEC §6). Nothing is computed or kept between statements.
//===--------------------------------------------------------------------===//

struct ReadScope {
	//! the dataset the read runs in
	string dataset;
	//! the scope is that whole dataset: the sweep
	bool whole = false;
	//! the subjects in scope, over a column subject
	string subjects;
};

//! A read's scope, resolved from the catalog alone: the dataset, whether the
//! read sweeps it, and the subject filter a narrowed read applies
static ReadScope ScopeOf(ClientContext &context, const Value &subject) {
	auto scope = subject.IsNull() ? CurrentDataset(context) : ResolveSubject(context, subject.ToString());
	ReadScope result;
	result.dataset = DatasetOf(context, scope);
	result.whole = scope == result.dataset;
	// a subject serves itself and what lies under it; no subject sweeps the USE'd dataset (SPEC §5.3)
	result.subjects = "(subject = " + QuoteText(scope) + " OR subject LIKE " + QuoteText(scope + ".%") + ")";
	return result;
}

static string PropertyFilter(const Value &property) {
	return property.IsNull() ? string() : " AND property = " + QuoteText(property.ToString());
}

//! The rows a read of a scope serves, over columns dataset, subject and property: the dataset's own and the
//! source-grain rows every dataset reads, narrowed to the scope's subjects and to a property; over gl_src
//! (SourcesSQL)
static string RowsFilter(const ReadScope &scope, const Value &property) {
	return "(dataset = " + QuoteText(scope.dataset) + " OR " + SourceGrainSQL() + ")" +
	       (scope.whole ? string() : " AND " + scope.subjects) + PropertyFilter(property);
}

//===--------------------------------------------------------------------===//
// The check (SPEC §6): the lake's snapshot id is the only version. A node a
// result read changed at the latest snapshot one of its rows begins or ends
// at, and rows are ended, never deleted; a result is out of date when a node
// it read changed after the snapshot it read at
//===--------------------------------------------------------------------===//

//! One arm of the relation of changes (ChangesSQL): over the rows `from`, the key of the node a row
//! changed, what of the node changed and the snapshot it changed at; read by the check, by the stale mark
//! of annotations, or by both. A key or a snapshot that is NULL is no change
struct ChangeArm {
	const char *from;
	const char *key;
	const char *what;
	const char *at;
	bool checked;
	bool marked;
};

//! The nodes as docs/specification/tables/inputs.md names them, a name matched as the lake matches names,
//! whatever its case. `vcol:` and a view's path keys the definition of the view's columns, its SQL
static const ChangeArm CHANGE_ARMS[] = {
    // the lake's tables, views and macros, every row
    {"gl_rel", "CASE WHEN kind <> 'm' THEN asset END", "'definition'", "g", true, true},
    {"gl_rel", "CASE WHEN kind = 'v' THEN 'vcol:' || asset END", "'definition'", "g", true, true},
    {"gl_rel", "CASE WHEN kind = 't' THEN asset END", "'data'", "g", true, false},
    {"gl_rel", "CASE WHEN kind <> 'm' THEN ds END", "'assets'", "g", true, false},
    {"gl_rel", "CASE WHEN kind = 'm' THEN asset END", "'macro'", "g", true, false},
    // a table column's definition: its rows in the lake (created, retyped, dropped). What is written about a
    // table holds while its columns do: the stale mark reads where a column's row ended, and a column added
    // leaves it as it was (SPEC §5.2); a result reads the table's every column row, so a column added is a
    // member added to what it read
    {"gl_cols", "CASE WHEN parent_column IS NULL THEN asset END", "'definition'", "g", true, true},
    {"gl_cols", "rel", "'definition'", "e", false, true},
    {"gl_cols", "rel", "'definition'", "g", true, false},
    {"gl_cols", "split_part(rel, '.', 1)", "'assets'", "g", true, false},
    // a load: the table's data, and its definition where the recipe derived the columns otherwise than the
    // load before it; for the stale mark, a column the load before derived otherwise
    {"gl_loads", "asset", "'data'", "snapshot_id", true, false},
    {"gl_loads", "asset", "'definition'",
	 "CASE WHEN before IS NOT NULL AND before IS DISTINCT FROM lineage THEN snapshot_id END", true, false},
    {"gl_loads", "asset", "'definition'",
	 "CASE WHEN before IS NOT NULL AND coalesce(list_bool_or(list_transform(json_keys(before::JSON), lambda k: "
	 "json_extract_string(before::JSON, '$.\"' || k || '\"') IS DISTINCT FROM json_extract_string(lineage::JSON, "
	 "'$.\"' || k || '\"'))), false) THEN snapshot_id END",
	 false, true},
    // an annotation, and a view's definition: the annotation that defines it
    {"$glossary", "lower(subject)", "'annotation ' || property", "$at", true, false},
    {"$glossary",
	 "CASE WHEN NOT contains(subject, '.') AND list_contains((SELECT views FROM gl_src), property) THEN lower(subject "
	 "|| '.' || property) END",
	 "'definition'", "$at", true, true},
    // the views of a kind in a dataset: a definition of that kind written or ended, the kind its body names
    {"$glossary",
	 "CASE WHEN NOT contains(subject, '.') AND list_contains((SELECT views FROM gl_src), property) AND "
	 "json_extract_string(body, '$.kind') IS NOT NULL THEN lower(subject) END",
	 "'views ' || json_extract_string(body, '$.kind')", "$at", true, false},
    {"$properties", "lower(name)", "'property'", "$at", true, false},
    {"$tests", "lower(property)", "'property'", "$at", true, false},
    {"$functions", "lower(name)", "'function'", "$at", true, false},
    {"$recipes", "lower(dataset || '.' || table_name)", "'recipe'", "$at", true, false},
    {"$relationships", "lower(split_part(left_side, '.', 1) || '.' || split_part(left_side, '.', 2))",
	 "'relationships'", "$at", true, false},
    {"$relationships", "lower(split_part(right_side, '.', 1) || '.' || split_part(right_side, '.', 2))",
	 "'relationships'", "$at", true, false},
    {"$relationships", "lower(dataset)", "'relationships'", "$at", true, false},
    // a table's data changed at a snapshot whose change text names its id
    {"(SELECT snapshot_id, unnest(regexp_extract_all(changes_made, '[a-z_]*(insert|delete|alter)[a-z_]*:([0-9]+)', "
	 "2))::BIGINT AS id FROM $snapshot_changes WHERE changes_made IS NOT NULL) l JOIN gl_rel t ON t.kind = 't' AND "
	 "t.id = l.id",
	 "t.asset", "'data'", "l.snapshot_id", true, false},
    // a load whose recipe derived the column otherwise than the load before it
    {"(SELECT k.asset, l.snapshot_id, json_extract_string(l.lineage::JSON, '$.\"' || k.column_name || '\"') AS "
	 "derived, json_extract_string(l.before_ci::JSON, '$.\"' || k.column_name || '\"') AS before FROM (SELECT "
	 "DISTINCT asset, rel, column_name FROM gl_cols WHERE parent_column IS NULL) k JOIN gl_loads l ON l.asset = "
	 "k.rel) WHERE before IS DISTINCT FROM derived AND before IS NOT NULL",
	 "asset", "'definition'", "snapshot_id", true, true},
    // a view's definition: its property's rows
    {"(SELECT asset FROM gl_rel WHERE kind = 'v' UNION SELECT lower(subject || '.' || property) FROM $glossary WHERE "
	 "NOT contains(subject, '.') AND list_contains((SELECT views FROM gl_src), property)) v JOIN $properties a ON "
	 "lower(a.name) = substr(v.asset, strpos(v.asset, '.') + 1)",
	 "v.asset", "'definition'", "greatest(a.begin_snapshot, coalesce(a.end_snapshot, 0))", true, true},
};

//! When each node changed, one relation: gl_chg (key, what, dataset, function, at, checked, marked), one
//! row per catalog row that changed a node, `checked` where the check reads it, `marked` where the stale
//! mark of annotations does. dataset and function are NULL but for a stored result: a result read is the
//! reader's dataset's, and a function's own results are no input of its own. With gl_src before it, as CTEs.
//! `checked` false plans the stale mark's rows alone
static string ChangesSQL(const Workspace &ws, bool checked) {
	string arms;
	for (idx_t i = 0; i < sizeof(CHANGE_ARMS) / sizeof(CHANGE_ARMS[0]);) {
		auto from = string(CHANGE_ARMS[i].from);
		vector<string> items;
		for (; i < sizeof(CHANGE_ARMS) / sizeof(CHANGE_ARMS[0]) && from == CHANGE_ARMS[i].from; i++) {
			auto &arm = CHANGE_ARMS[i];
			if (checked || arm.marked) {
				items.push_back("{'key': " + string(arm.key) + ", 'what': " + arm.what + ", 'at': " + arm.at +
				                ", 'checked': " + (arm.checked ? "true" : "false") +
				                ", 'marked': " + (arm.marked ? "true" : "false") + "}");
			}
		}
		if (!items.empty()) {
			arms += string(arms.empty() ? "" : " UNION ALL ") + "SELECT unnest([" + StringUtil::Join(items, ", ") +
			        "]) AS gl_k FROM " + from;
		}
	}
	auto sql =
	    SourcesSQL(ws) +
	    ", gl_rel AS MATERIALIZED (SELECT r.kind, r.id, lower(s.schema_name) AS ds, lower(s.schema_name || '.' || "
	    "r.name) AS asset, r.name, r.begin_snapshot AS b, r.end_snapshot AS e, greatest(r.begin_snapshot, "
	    "coalesce(r.end_snapshot, 0)) AS g FROM (SELECT 't' AS kind, table_id AS id, schema_id, table_name AS name, "
	    "begin_snapshot, end_snapshot FROM $table UNION ALL SELECT 'v', view_id, schema_id, view_name, "
	    "begin_snapshot, end_snapshot FROM $view UNION ALL SELECT 'm', macro_id, schema_id, macro_name, "
	    "begin_snapshot, end_snapshot FROM $macro) r JOIN (SELECT DISTINCT schema_id, schema_name FROM $schema) s "
	    "USING (schema_id)),"
	    " gl_cols AS MATERIALIZED (SELECT t.asset AS rel, lower(t.asset || '.' || c.column_name) AS asset, "
	    "c.column_name, c.parent_column, c.end_snapshot AS e, greatest(c.begin_snapshot, coalesce(c.end_snapshot, 0)) "
	    "AS g FROM $column c JOIN gl_rel t ON t.kind = 't' AND t.id = c.table_id),"
	    // each load with the lineage of the load before it of the same table: before by its name as written,
	    // before_ci by its path in lower case
	    " gl_loads AS MATERIALIZED (SELECT asset, r.s AS snapshot_id, r.l AS lineage, list_filter(ls[1:r.i - 1], "
	    "lambda x: x.ds = r.ds AND x.tn = r.tn)[-1].l AS before, CASE WHEN r.i > 1 THEN ls[r.i - 1].l END AS "
	    "before_ci FROM (SELECT asset, ls, unnest(list_transform(ls, lambda x, i: struct_insert(x, i := i))) AS r "
	    "FROM (SELECT lower(dataset || '.' || table_name) AS asset, list({'s': snapshot_id, 'l': lineage, 'ds': "
	    "dataset, 'tn': table_name} ORDER BY begin_snapshot) AS ls FROM $imports GROUP BY 1))),"
	    " gl_chg AS MATERIALIZED (SELECT gl_k.key, gl_k.what, NULL::VARCHAR AS dataset, NULL::VARCHAR AS function, "
	    "gl_k.at, gl_k.checked, gl_k.marked FROM (" +
	    arms + ") WHERE gl_k.key IS NOT NULL AND gl_k.at IS NOT NULL" +
	    // a stored result: the current row's changed_at, else the last row's end
	    (checked ? " UNION ALL SELECT lower(subject), gl_r, dataset, function, coalesce(max(changed_at) FILTER "
		           "(WHERE end_snapshot IS NULL), max(end_snapshot)), true, false FROM $results, "
		           "unnest(['result ' || function, 'value ' || property]) u(gl_r) GROUP BY ALL"
		         : "") +
	    ")";
	sql = StringUtil::Replace(sql, "$at", "greatest(begin_snapshot, coalesce(end_snapshot, 0))");
	for (auto &relation :
	     {"glossary", "results", "imports", "recipes", "properties", "tests", "functions", "relationships"}) {
		sql = StringUtil::Replace(sql, "$" + string(relation), ws.Store(relation));
	}
	for (auto &table : {"snapshot_changes", "schema", "table", "column", "view", "macro"}) {
		sql = StringUtil::Replace(sql, "$" + string(table), ws.Meta(table));
	}
	return sql;
}

//! The keys of the changes to a definition node `node` (lower case): its own, and for a column its view's
//! SQL, which defines a view's columns
static string DefinitionKeysSQL(const string &node) {
	return "CASE WHEN len(string_split(" + node + ", '.')) >= 3 THEN [" + node + ", 'vcol:' || split_part(" + node +
	       ", '.', 1) || '.' || split_part(" + node + ", '.', 2)] ELSE [" + node + "] END";
}

//! The check of the current stored results in the datasets `roots` selects, a condition on the
//! results relation (empty: every result), as CTEs over gl_chg (ChangesSQL): ck_cur the results,
//! `root` where `roots` selects one; ck_changed (id, asset, what) a node it read changed after the snapshot
//! it read at; ck_reads (reader, read, asset, what) a result read, through a read of its function's results
//! or of the values of its property; ck_out the results out of date or possibly so, which read as they read
//! until the result read is computed again, which keeps its changed_at when its value is the same; ck_marks
//! each result with `changed` and `out`. A result reads within its dataset, so the datasets bound the walk
static string CheckSQL(const Workspace &ws, const string &roots) {
	auto results = ws.Store("results");
	return "ck_cur AS MATERIALIZED (SELECT id, dataset, function, subject, property, read_at, value, computed_at, "
	       "error, begin_snapshot, " +
	       (roots.empty() ? string("true") : "(" + roots + ")") + " AS root FROM " + results +
	       " WHERE end_snapshot IS NULL" +
	       (roots.empty() ? string()
	                      : " AND dataset IN (SELECT dataset FROM " + results + " WHERE end_snapshot IS NULL AND (" +
	                            roots + "))") +
	       "), ck_in AS MATERIALIZED (SELECT i.result AS id, i.asset, i.what, c.dataset, c.function, c.read_at FROM " +
	       ws.Store("inputs") +
	       " i JOIN ck_cur c ON c.id = i.result WHERE i.end_snapshot IS NULL),"
	       " ck_changed AS MATERIALIZED (SELECT DISTINCT i.id, i.asset, i.what FROM (SELECT *, unnest(CASE WHEN what = "
	       "'definition' THEN " +
	       DefinitionKeysSQL("lower(asset)") +
	       " ELSE [lower(asset)] END) AS key FROM ck_in) i JOIN gl_chg n ON n.key = i.key AND n.what = i.what"
	       " WHERE n.checked AND n.at > i.read_at AND (n.dataset IS NULL OR n.dataset = i.dataset)"
	       " AND n.function IS DISTINCT FROM i.function),"
	       " ck_reads AS (SELECT i.id AS reader, q.id AS read, i.asset, i.what FROM ck_in i JOIN (SELECT id, dataset, "
	       "subject, function, unnest(['result ' || function, 'value ' || property]) AS key FROM ck_cur) q"
	       " ON q.dataset = i.dataset AND q.subject = i.asset AND q.key = i.what"
	       " WHERE starts_with(i.what, 'result ') OR q.function <> i.function),"
	       " ck_out(id) AS (SELECT id FROM ck_changed UNION SELECT r.reader FROM ck_reads r JOIN ck_out o ON o.id = "
	       "r.read),"
	       " ck_marks AS (SELECT c.*, x.id IS NOT NULL AS changed, o.id IS NOT NULL AS out FROM ck_cur c"
	       " LEFT JOIN (SELECT DISTINCT id FROM ck_out) o ON o.id = c.id"
	       " LEFT JOIN (SELECT DISTINCT id FROM ck_changed) x ON x.id = c.id)";
}

string CurrentResultsSQL(const Workspace &ws, const string &filter) {
	return "WITH RECURSIVE " + ChangesSQL(ws, true) + ", " + CheckSQL(ws, filter) +
	       " SELECT m.*, NOT k.out AS current, k.out AND NOT k.changed AS possibly FROM (SELECT * FROM " +
	       ws.Store("results") + " WHERE end_snapshot IS NULL AND (" + filter + ")) m JOIN ck_marks k ON k.id = m.id";
}

string OutdatedSQL(Inner &inner) {
	return "WITH RECURSIVE " + ChangesSQL(inner.ws, true) + ", " + CheckSQL(inner.ws, "") +
	       " SELECT c.dataset, c.function, c.subject, x.asset, x.what, 'changed' AS reason FROM ck_changed x"
	       " JOIN ck_cur c USING (id) UNION SELECT c.dataset, c.function, c.subject, r.asset, r.what, 'possibly'"
	       " FROM ck_reads r JOIN ck_out o ON o.id = r.read JOIN ck_cur c ON c.id = r.reader";
}

//! The lake's current views and table columns by their path, lower case, one row of lists: gl_now
static string NowSQL(const Workspace &ws) {
	auto schemas = "(SELECT DISTINCT schema_id, schema_name FROM " + ws.Meta("schema") + " WHERE end_snapshot IS NULL)";
	return "gl_now AS MATERIALIZED (SELECT coalesce((SELECT list(lower(s.schema_name || '.' || v.view_name)) FROM " +
	       ws.Meta("view") + " v JOIN " + schemas +
	       " s USING (schema_id) WHERE v.end_snapshot IS NULL), []) AS views, coalesce((SELECT "
	       "list(lower(s.schema_name "
	       "|| '.' || t.table_name || '.' || c.column_name)) FROM " +
	       ws.Meta("column") + " c JOIN " + ws.Meta("table") +
	       " t ON t.table_id = c.table_id AND t.end_snapshot IS NULL JOIN " + schemas +
	       " s ON s.schema_id = t.schema_id WHERE c.end_snapshot IS NULL AND c.parent_column IS NULL), []) AS cols)";
}

//! The node an annotation's staleness reads for its subject, as SQL over the subject expression and gl_now
//! (NowSQL): the `definition` of a table or a view, and of a column, whose rows are a table column's own or a
//! view's SQL; a column no relation holds now reads its relation's; NULL for a dataset, a source and a
//! relationship (SPEC §5.2)
static string DefinitionNodeSQL(const string &subject) {
	auto relation = "split_part(" + subject + ", '.', 1) || '.' || split_part(" + subject + ", '.', 2)";
	return "CASE WHEN " + subject + " LIKE '% -> %' OR " + subject + " LIKE '% <-> %' OR NOT contains(" + subject +
	       ", '.') THEN NULL WHEN len(string_split(" + subject + ", '.')) = 2 THEN " + subject +
	       " WHEN list_contains((SELECT views FROM gl_now), lower(" + relation +
	       ")) OR list_contains((SELECT cols FROM gl_now), lower(" + subject + ")) THEN " + subject + " ELSE " +
	       relation + " END";
}

//! The annotations `filter` selects, as CTEs over gl_chg (ChangesSQL), an: each with whether it is stale
//! (SPEC §5.2): the definition of its subject (DefinitionNodeSQL) changed after the snapshot it was written
//! at, or no row of the lake's catalog holds that definition any more. A dataset, a source and a
//! relationship never are. (subject, property, actor, body, written_at, stale, ord, error, dataset, possibly),
//! ord the snapshot it was written at
static string AnnotationsSQL(const Workspace &ws, const string &filter) {
	return NowSQL(ws) +
	       ", an_g AS MATERIALIZED (SELECT dataset, subject, property, actor_kind, body, written_at, begin_snapshot AS "
	       "id, " +
	       DefinitionNodeSQL("subject") + " AS node FROM " + ws.Store("glossary") + " WHERE end_snapshot IS NULL AND " +
	       filter + "), an_n AS (SELECT w.node, max(c.at) AS at FROM (SELECT node, unnest(" +
	       DefinitionKeysSQL("node") +
	       ") AS key FROM (SELECT DISTINCT lower(node) AS node FROM an_g WHERE node IS NOT NULL)) w"
	       " JOIN gl_chg c ON c.key = w.key AND c.what = 'definition' AND c.marked GROUP BY w.node),"
	       " an AS (SELECT g.subject, g.property, g.actor_kind AS actor, g.body::JSON AS body, g.written_at,"
	       " g.node IS NOT NULL AND coalesce(n.at > g.id, true) AS stale, g.id AS ord, NULL::VARCHAR AS error,"
	       " g.dataset, false AS possibly FROM an_g g LEFT JOIN an_n n ON n.node = lower(g.node))";
}

static string AnnotationsOverSQL(const Workspace &ws, const string &filter) {
	return "WITH " + ChangesSQL(ws, false) + ", " + AnnotationsSQL(ws, filter) + " SELECT * FROM an";
}

//! Every value `filter` selects, the current one per actor kind, as CTEs: gl_all, the glossary's agent and
//! human rows and the function values the results hold, a view's lineage and a test's result none of
//! them; each with its staleness, `ord`, the snapshot it was written at, and the error of a computation that
//! failed, whose row holds no value. `checked` false where no current stored result holds a value
//! `filter` selects: the check is not planned
static string ValuesSQL(const Workspace &ws, const string &filter, bool checked) {
	auto roots = filter + " AND property NOT IN ('lineage', 'test')";
	return ChangesSQL(ws, checked) + ", " + AnnotationsSQL(ws, filter) + ", " +
	       (checked
	            ? CheckSQL(ws, roots) +
	                  ", gl_values AS (SELECT subject, property, 'function' AS actor, value::JSON AS body, computed_at"
	                  " AS written_at, changed AS stale, begin_snapshot AS ord, error, dataset, out AND NOT changed"
	                  " AS possibly FROM ck_marks WHERE root)"
				: string("gl_values AS (SELECT * FROM an WHERE false)")) +
	       ", gl_all AS (SELECT * FROM an UNION ALL SELECT * FROM gl_values)";
}

//! The stored results of the tests in scope, a condition on the results relation
static string TestsFilter(const Workspace &ws, const ReadScope &scope, const Value &property) {
	return "dataset = " + QuoteText(scope.dataset) + " AND property = 'test'" +
	       (scope.whole ? string() : " AND " + scope.subjects) +
	       (property.IsNull() ? string()
	                          : " AND function IN (SELECT name FROM " + Newest(ws, "tests") +
	                                " WHERE property = " + QuoteText(property.ToString()) + ")");
}

//! A test's stored result as its judgment: state and score, an error the state `error` with score 0
static constexpr const char *JUDGMENT = "CASE WHEN m.error IS NOT NULL THEN 'error' ELSE m.value::JSON->>'state' "
                                        "END AS state, CASE WHEN m.error IS NOT NULL THEN 0.0 ELSE "
                                        "(m.value::JSON->>'score')::DOUBLE END AS score";
//! A test's stored result that holds a judgment: a subject its detector returned no row for holds none
static constexpr const char *JUDGED = "(m.error IS NOT NULL OR json_type(m.value::JSON) = 'OBJECT')";

//! The tests' results among stored results with their marks, in the standard test-results schema
//! (SPEC §7.2): (subject, property, test, state, score, computed_at, current, error), `current` whether the
//! stored result is current (SPEC §6)
static string TestResultsSQL(const Workspace &ws, const ReadScope &scope, const Value &property) {
	return "SELECT m.subject, t.property, m.function AS test, " + string(JUDGMENT) +
	       ", m.computed_at, m.current, m.error FROM (" + CurrentResultsSQL(ws, TestsFilter(ws, scope, property)) +
	       ") m JOIN " + Newest(ws, "tests") + " t ON t.name = m.function AND t.detector IS NOT NULL WHERE " + JUDGED;
}

//! The subjects each tested property is required on, by the property's grain (SPEC §5.1), as CTEs over gl_src:
//! gl_missing (subject, property), what the collapsed read discloses as missing when nobody annotated. A
//! narrowed read keeps to its subject; the dataset's sweep takes every declared source
static string MissingSQL(const Workspace &ws, const ReadScope &scope, const Value &property) {
	auto d = QuoteText(scope.dataset);
	return "ms_tests AS (SELECT w.property, coalesce(from_json(a.grains, '[\"VARCHAR\"]'), []::VARCHAR[]) AS "
	       "grains, a.condition::JSON->>'property' AS c_property, a.condition::JSON->>'value' AS c_value,"
	       " from_json(a.condition::JSON->'type', '[\"VARCHAR\"]') AS c_types FROM " +
	       Newest(ws, "tests") + " w LEFT JOIN " + Newest(ws, "properties") + " a ON a.name = w.property" +
	       (property.IsNull() ? string() : " WHERE w.property = " + QuoteText(property.ToString())) +
	       "), ms_cols AS MATERIALIZED (SELECT table_name, column_name, column_type FROM glossql_columns(" + d +
	       ")), ms_cand AS (SELECT " + d +
	       " || '.' || table_name || '.' || column_name AS subject, 'COLUMN' AS grain,"
	       " upper(split_part(column_type, '(', 1)) AS base FROM ms_cols UNION ALL SELECT DISTINCT " +
	       d + " || '.' || table_name, 'TABLE', NULL FROM ms_cols UNION ALL SELECT " + d +
	       ", 'DATASET', NULL UNION ALL SELECT left_side || ' ' || op || ' ' || right_side, 'RELATIONSHIP', NULL "
	       "FROM " +
	       ws.Store("relationships") + " WHERE dataset = " + d +
	       " UNION ALL SELECT DISTINCT name, 'SOURCE', NULL FROM " + ws.Store("sources") +
	       // required only where the sibling's winning value, human over agent, carries the literal (SPEC §5.1)
	       "), ms_sibling AS (SELECT subject, property, arg_min(body, {'r': CASE actor_kind WHEN 'human' THEN 0 ELSE 1 "
	       "END, 'o': -begin_snapshot}) AS body FROM " +
	       ws.Store("glossary") +
	       " WHERE end_snapshot IS NULL AND actor_kind IN ('human', 'agent') AND (dataset = " + d + " OR " +
	       SourceGrainSQL() +
	       ") AND list_contains((SELECT list(c_property) FROM ms_tests), property) GROUP BY subject, property),"
	       " gl_missing AS (SELECT DISTINCT c.subject, t.property FROM ms_tests t JOIN ms_cand c"
	       " ON len(t.grains) = 0 OR list_contains(t.grains, c.grain)"
	       " LEFT JOIN ms_sibling s ON s.subject = c.subject AND s.property = t.c_property"
	       " WHERE (t.c_property IS NULL OR s.body::JSON->>'value' = t.c_value)"
	       // required only on columns of the named types: the base type, DECIMAL for DECIMAL(18,2) (SPEC §5.1)
	       " AND (t.c_types IS NULL OR (c.grain = 'COLUMN' AND list_contains(t.c_types, c.base)))" +
	       (scope.whole ? string() : " AND " + StringUtil::Replace(scope.subjects, "subject", "c.subject")) + ")";
}

//! The collapsed read: one row per (subject, property) — the precedence pick,
//! withheld while two actors' values are current and the test's stored score
//! exceeds its threshold; one value is never in conflict, it is served with
//! its test state (SPEC §5.3); every gap visible
static string CollapsedSQL(const Workspace &ws, const ReadScope &scope, const Value &property, bool checked) {
	// an error is not a judgment (SPEC §7.2): it never withholds, and any other result outranks it, the
	// highest score first; a stale value is served and marked (SPEC §5.3). A test's state and score are read
	// as stored: the collapsed read serves no test's mark
	return "WITH RECURSIVE " + ValuesSQL(ws, RowsFilter(scope, property), checked) +
	       ", gl_picked AS (SELECT subject, property, p.value, p.stale, p.possibly, gl_actors FROM (SELECT subject, "
	       "property, count(DISTINCT actor) AS gl_actors, arg_min({'value': body::JSON, 'stale': stale, 'possibly': "
	       "possibly}, {'r': CASE actor WHEN 'human' THEN 0 WHEN 'agent' THEN 1 ELSE 2 END, 'o': -ord}) AS p"
	       " FROM gl_all WHERE error IS NULL GROUP BY subject, property)),"
	       " gl_judged AS (SELECT subject, property, j.state, j.score, j.threshold FROM (SELECT subject, property,"
	       " arg_min({'state': state, 'score': score, 'threshold': threshold}, {'e': CASE WHEN state = 'error' THEN 1"
	       " WHEN state IS NULL THEN 2 ELSE 0 END, 's': coalesce(-score, 'infinity'::DOUBLE)}) AS j FROM (SELECT"
	       " m.subject, t.property, " +
	       JUDGMENT + ", t.threshold FROM (SELECT * FROM " + ws.Store("results") + " WHERE end_snapshot IS NULL AND " +
	       TestsFilter(ws, scope, Value()) + ") m JOIN " + Newest(ws, "tests") +
	       " t ON t.name = m.function AND t.detector IS NOT NULL" +
	       (property.IsNull() ? string() : " AND t.property = " + QuoteText(property.ToString())) + " WHERE " + JUDGED +
	       ") GROUP BY subject, property)), " + MissingSQL(ws, scope, property) +
	       " SELECT * FROM (SELECT p.subject, p.property,"
	       " CASE WHEN j.score > j.threshold AND p.gl_actors > 1 THEN NULL ELSE p.value END AS value,"
	       " j.state AS test_state, j.score, CASE WHEN j.score > j.threshold AND p.gl_actors > 1"
	       " THEN 'conflict' WHEN p.stale THEN 'stale' WHEN p.possibly THEN 'possibly' ELSE 'current' END AS state"
	       " FROM gl_picked p LEFT JOIN gl_judged j ON j.subject = p.subject AND j.property = p.property"
	       " UNION ALL SELECT o.subject, o.property, NULL::JSON AS value, NULL::VARCHAR AS test_state,"
	       " NULL::DOUBLE AS score, 'missing' AS state FROM gl_missing o ANTI JOIN gl_picked q"
	       " ON q.subject = o.subject AND q.property = o.property) ORDER BY subject, property";
}

//! The raw read: one row per value — (subject, property, kind, test, actor, body, written_at, current,
//! error); a failed computation is a row with its error and no body
static string RawSQL(const Workspace &ws, const ReadScope &scope, const Value &property, bool checked) {
	return "WITH RECURSIVE " + ValuesSQL(ws, RowsFilter(scope, property), checked) +
	       " SELECT gl_s.subject, gl_s.property, gl_a.kind, gl_t.name AS test, gl_s.actor, gl_s.body::JSON AS body,"
	       " gl_s.written_at, NOT gl_s.stale AND NOT gl_s.possibly AS current, gl_s.error FROM gl_all gl_s LEFT JOIN " +
	       Newest(ws, "properties") + " gl_a ON gl_a.name = gl_s.property LEFT JOIN " + Newest(ws, "tests") +
	       " gl_t ON gl_t.property = gl_s.property ORDER BY gl_s.subject, gl_s.property, gl_s.actor";
}

//===--------------------------------------------------------------------===//
// A test's result (SPEC §7.2): SELECT <test>() FROM <subject> computes the
// test on each subject at or under the subject that holds a value of its
// property, where no result is stored or the stored one is not current, and
// stores one result per subject and test in one write: the detector's state
// and score, or the error it failed with, with the nodes it read. A stored
// result on a subject that holds no value any more retires.
//===--------------------------------------------------------------------===//

TestDeclaration DeclaredTest(Inner &inner, const string &name) {
	TestDeclaration test;
	auto row = inner.Row("SELECT w.name, w.property, w.detector, w.threshold, (SELECT f.script FROM " +
	                     inner.Store("functions") +
	                     " f WHERE f.name = w.detector AND f.returns IS NULL AND f.end_snapshot IS NULL"
	                     " ORDER BY f.begin_snapshot DESC LIMIT 1) FROM " +
	                     Newest(inner.ws, "tests") + " w WHERE w.name = " + QuoteText(name));
	if (row.empty()) {
		return test;
	}
	test.name = row[0].ToString();
	test.property = row[1].ToString();
	test.detector = row[2].IsNull() ? string() : row[2].ToString();
	test.threshold = row[3];
	test.script = row[4];
	return test;
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

//! The detector's rows over the values in the inner connection's table `values`: (subject, state, score) per
//! subject, or the failure that is every subject's result
static string RunDetector(ClientContext &context, Inner &inner, const TestDeclaration &test, const string &values,
                          std::map<string, pair<string, string>> &judged) {
	if (test.script.IsNull()) {
		return "detector " + test.detector + " is not a declared function";
	}
	auto across = CrossSubject(context, test.script.ToString());
	if (!across.empty()) {
		return "cross_subject: detector " + test.detector + " " + across;
	}
	auto threshold = test.threshold.IsNull() ? string("NULL") : test.threshold.ToSQLString();
	auto script = ReplacePlaceholder(test.script.ToString(), "threshold", threshold);
	// the script is a derived table under the values CTE, so a body that opens with its own WITH composes
	auto result = inner.con.Query("SELECT d.subject::VARCHAR, d.state::VARCHAR, d.score::DOUBLE FROM (WITH "
	                              "annotations AS (SELECT * FROM " +
	                              values + ") SELECT * FROM (" + script + ") AS q) AS d");
	if (result->HasError()) {
		return FirstLine(result->GetError());
	}
	while (auto chunk = result->Fetch()) {
		for (idx_t i = 0; i < chunk->size(); i++) {
			auto subject = chunk->GetValue(0, i);
			auto state = chunk->GetValue(1, i);
			auto score = chunk->GetValue(2, i);
			if (!InTestsSchema(subject, state, score)) {
				return "detector " + test.detector + " served (" + subject.ToString() + ", " + state.ToString() + ", " +
				       score.ToString() + "), outside the test-results schema";
			}
			judged[subject.ToString()] = {state.ToString(), score.ToString()};
		}
	}
	return string();
}

vector<vector<Value>> ComputeTest(ClientContext &context, Inner &inner, const TestDeclaration &test,
                                  const string &subject) {
	auto &ws = inner.ws;
	auto scope = ScopeOf(context, Value(subject));
	auto values = RowsFilter(scope, Value(test.property));
	auto stored = "dataset = " + QuoteText(scope.dataset) + " AND function = " + QuoteText(test.name) +
	              " AND property = 'test'" + (scope.whole ? string() : " AND " + scope.subjects);
	// the subjects that hold a value of the property, and the test's results stored in scope with whether each
	// is current
	auto rows =
	    inner.Run("WITH " + SourcesSQL(ws) + ", holding AS (SELECT DISTINCT subject FROM (SELECT subject FROM " +
		          inner.Current("glossary") + " WHERE " + values + " UNION ALL SELECT subject FROM " +
		          inner.Current("results") + " WHERE error IS NULL AND " + values +
		          ")), kept AS (SELECT subject, value, current FROM (" + CurrentResultsSQL(ws, stored) +
		          ")) SELECT coalesce(h.subject, k.subject), h.subject IS NOT NULL, k.subject IS NOT NULL,"
		          " coalesce(k.current, false), k.value FROM holding h FULL JOIN kept k ON k.subject ="
		          " h.subject ORDER BY 1");
	vector<string> due;
	vector<string> retire;
	vector<vector<Value>> served;
	while (auto chunk = rows->Fetch()) {
		for (idx_t r = 0; r < chunk->size(); r++) {
			auto name = chunk->GetValue(0, r).ToString();
			auto holds = chunk->GetValue(1, r).GetValue<bool>();
			auto kept = chunk->GetValue(2, r).GetValue<bool>();
			if (!holds) {
				retire.push_back(name);
			} else if (kept && chunk->GetValue(3, r).GetValue<bool>()) {
				served.push_back({Value(test.name), Value(name), Value(test.property), chunk->GetValue(4, r),
				                  Value::BOOLEAN(false)});
			} else {
				due.push_back(name);
			}
		}
	}
	if (due.empty() && retire.empty()) {
		return served;
	}
	// the snapshot it reads at, taken before the values are read: a change under it is a change after it
	auto read_at = inner.Scalar("SELECT max(snapshot_id) FROM " + ws.Meta("snapshot")).GetValue<int64_t>();
	vector<string> quoted;
	for (auto &name : due) {
		quoted.push_back("(" + QuoteText(name) + ")");
	}
	auto in_due = due.empty() ? string("false")
	                          : "subject IN (SELECT s FROM (VALUES " + StringUtil::Join(quoted, ", ") + ") v(s))";
	const string once = "__gloss_test_values";
	inner.Run(
	    "CREATE OR REPLACE TEMP TABLE " + once +
	    " AS SELECT subject, property, actor AS actor_kind, body, NOT stale AND NOT possibly AS current FROM (WITH "
	    "RECURSIVE " +
	    ValuesSQL(ws, values + " AND " + in_due, true) + " SELECT * FROM gl_all) WHERE error IS NULL");
	std::map<string, pair<string, string>> judged;
	auto failure = due.empty() ? string() : RunDetector(context, inner, test, once, judged);
	// a detector that failed ended the read it ran in, and the table with it
	inner.Run("DROP TABLE IF EXISTS " + once);
	if (context.IsInterrupted()) {
		throw InterruptException();
	}
	// the nodes each result read: the subject's values of the property, its definition, which its values'
	// staleness reads, the property's and the test's declaration, and the detector's
	std::map<string, string> definitions;
	if (!due.empty()) {
		auto nodes = inner.Run("WITH " + NowSQL(ws) + " SELECT s, " + DefinitionNodeSQL("s") + " FROM (VALUES " +
		                       StringUtil::Join(quoted, ", ") + ") v(s)");
		while (auto chunk = nodes->Fetch()) {
			for (idx_t r = 0; r < chunk->size(); r++) {
				auto node = chunk->GetValue(1, r);
				if (!node.IsNull()) {
					definitions[chunk->GetValue(0, r).ToString()] = node.ToString();
				}
			}
		}
	}
	vector<ResultRow> results;
	vector<vector<Value>> out;
	for (auto &name : due) {
		ResultRow row;
		row.subject = name;
		if (!failure.empty()) {
			row.error = Value(StringUtil::StartsWith(failure, "glossql: ") ? failure.substr(9) : failure);
		} else {
			auto found = judged.find(name);
			// a subject the detector returned no row for holds no judgment, stored as such
			row.value = found == judged.end() ? Value("null")
			                                  : Value("{\"state\":" + JsonString(found->second.first) +
			                                          ",\"score\":" + found->second.second + "}");
		}
		row.nodes = {
		    {name, "annotation " + test.property}, {name, "value " + test.property}, {test.property, "property"}};
		if (!test.detector.empty()) {
			row.nodes.emplace_back(test.detector, "function");
		}
		auto definition = definitions.find(name);
		if (definition != definitions.end()) {
			row.nodes.emplace_back(definition->second, "definition");
		}
		out.push_back({Value(test.name), Value(name), Value(test.property), row.error.IsNull() ? row.value : Value(),
		               Value::BOOLEAN(true)});
		results.push_back(std::move(row));
	}
	inner.Write(
	    context,
	    [&](CatalogWrite &write) { StoreResults(write, scope.dataset, test.name, "test", read_at, results, retire); },
	    true);
	out.insert(out.end(), served.begin(), served.end());
	std::sort(out.begin(), out.end(),
	          [](const vector<Value> &a, const vector<Value> &b) { return a[1].ToString() < b[1].ToString(); });
	return out;
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
	vector<string> datasets;
	if (!input.inputs[0].IsNull()) {
		datasets.push_back(QuoteText(input.inputs[0].ToString()));
	} else if (string(kind->stem) == "lineage_places_") {
		datasets.push_back("''");
	} else {
		try {
			auto rows = Inner::Of(context).Run("SELECT name FROM " + QuoteName(lake) + ".main.datasets ORDER BY name");
			while (auto chunk = rows->Fetch()) {
				for (idx_t r = 0; r < chunk->size(); r++) {
					datasets.push_back(QuoteText(chunk->GetValue(0, r).ToString()));
				}
			}
		} catch (std::exception &) {
			// no store, no datasets
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
	// the assets of one kind: its own macro's
	if (string(KIND_STEMS[stem].stem) == "lineage_assets_") {
		TableFunction narrowed(KIND_STEMS[stem].function, {LogicalType::VARCHAR, LogicalType::VARCHAR}, nullptr);
		narrowed.bind_replace = bind;
		set.AddFunction(narrowed);
	}
	return set;
}

//===--------------------------------------------------------------------===//
// glossql_glossary(subject, property, all) and glossql_tests(subject, property):
// GLOSSARY() and TESTS() as the grammar rewrites them. Each read's rows are
// computed when the statement runs, on the second connection, and scanned:
// planned there, apart from the statement that reads them, so a statement
// that composes many reads plans each one's marks once. Nothing is kept past
// the statement
//===--------------------------------------------------------------------===//

struct ReadBindData : public FunctionData {
	string sql;

	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<ReadBindData>();
		result->sql = sql;
		return result;
	}
	bool Equals(const FunctionData &other) const override {
		return sql == other.Cast<ReadBindData>().sql;
	}
};

struct ReadState : public GlobalTableFunctionState {
	shared_ptr<ColumnDataCollection> rows;
	ColumnDataScanState scan;
};

//! The columns of each read, `name TYPE` each
static const char *const COLLAPSED_SHAPE =
    "subject VARCHAR, property VARCHAR, value JSON, test_state VARCHAR, score DOUBLE, state VARCHAR";
static const char *const RAW_SHAPE = "subject VARCHAR, property VARCHAR, kind VARCHAR, test VARCHAR, actor VARCHAR, "
                                     "body JSON, written_at TIMESTAMP, current BOOLEAN, error VARCHAR";
static const char *const TESTS_SHAPE = "subject VARCHAR, property VARCHAR, test VARCHAR, state VARCHAR, "
                                       "score DOUBLE, computed_at TIMESTAMP, current BOOLEAN, error VARCHAR";

//! A read's bind: its columns from the shape, and its SQL cast to them
static unique_ptr<FunctionData> ReadBind(const string &shape, const string &sql, vector<LogicalType> &return_types,
                                         vector<Identifier> &names) {
	vector<string> casts;
	for (auto &column : StringUtil::Split(shape, ", ")) {
		auto space = column.find(' ');
		auto name = column.substr(0, space);
		auto type = column.substr(space + 1);
		names.emplace_back(name);
		return_types.push_back(type == "JSON"        ? LogicalType::JSON()
		                       : type == "DOUBLE"    ? LogicalType::DOUBLE
		                       : type == "TIMESTAMP" ? LogicalType::TIMESTAMP
		                       : type == "BOOLEAN"   ? LogicalType::BOOLEAN
		                                             : LogicalType::VARCHAR);
		casts.push_back(QuoteName(name) + "::" + type);
	}
	auto result = make_uniq<ReadBindData>();
	result->sql = "SELECT " + StringUtil::Join(casts, ", ") + " FROM (" + sql + ")";
	return std::move(result);
}

static unique_ptr<FunctionData> GlossaryBind(ClientContext &context, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types, vector<Identifier> &names) {
	bool all = input.inputs[2].GetValue<bool>();
	auto scope = ScopeOf(context, input.inputs[0]);
	auto ws = WorkspaceOf(context);
	auto &property = input.inputs[1];
	// a read of one property that no current stored result holds serves no function value: the check is
	// not planned
	bool checked = property.IsNull() ||
	               !Inner::Of(context)
	                    .Row("SELECT 1 FROM " + ws.Store("results") + " WHERE end_snapshot IS NULL AND property = " +
	                         QuoteText(property.ToString()) + " AND property NOT IN ('lineage', 'test') LIMIT 1")
	                    .empty();
	if (all) {
		return ReadBind(RAW_SHAPE, RawSQL(ws, scope, property, checked), return_types, names);
	}
	return ReadBind(COLLAPSED_SHAPE, CollapsedSQL(ws, scope, property, checked), return_types, names);
}

static unique_ptr<FunctionData> TestsBind(ClientContext &context, TableFunctionBindInput &input,
                                          vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto scope = ScopeOf(context, input.inputs[0]);
	return ReadBind(TESTS_SHAPE,
	                "SELECT * FROM (" + TestResultsSQL(WorkspaceOf(context), scope, input.inputs[1]) +
	                    ") ORDER BY subject, property, test",
	                return_types, names);
}

//! A read's rows, computed once per statement by their SQL (StatementState): a statement that composes the
//! same read twice (a summary read by two of the reads it composes) computes it once, and so does a
//! computation over many subjects, whose bodies read on the connection that holds its read
static unique_ptr<GlobalTableFunctionState> ReadInit(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind = input.bind_data->Cast<ReadBindData>();
	auto state = make_uniq<ReadState>();
	auto &statement = StatementState::Of(context);
	// a read the statement computed already, at its bind or in another read, on any of its connections
	auto &reads = statement.Root();
	{
		std::lock_guard<std::mutex> guard(reads.lock);
		auto found = reads.reads.find(bind.sql);
		if (found != reads.reads.end()) {
			state->rows = found->second;
		}
	}
	if (!state->rows) {
		// the pipelines that scan reads initialise on several threads: one runs on the statement's
		// connection, another at the same time on a connection of its own
		std::unique_lock<std::recursive_mutex> serial(statement.use, std::try_to_lock);
		unique_ptr<Inner> own;
		if (!serial.owns_lock()) {
			own = make_uniq<Inner>(context);
		}
		auto result = (own ? *own : Inner::Of(context)).Run(bind.sql);
		auto rows = make_shared_ptr<ColumnDataCollection>(Allocator::Get(context), result->GetTypes());
		while (auto chunk = result->Fetch()) {
			rows->Append(*chunk);
		}
		std::lock_guard<std::mutex> guard(reads.lock);
		state->rows = reads.reads.emplace(bind.sql, rows).first->second;
	}
	state->rows->InitializeScan(state->scan);
	return std::move(state);
}

static void ReadExec(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<ReadState>();
	state.rows->Scan(state.scan, output);
}

static TableFunction GlossGlossaryFunction() {
	return TableFunction("glossql_glossary", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BOOLEAN},
	                     ReadExec, GlossaryBind, ReadInit);
}

static TableFunction GlossTestsFunction() {
	return TableFunction("glossql_tests", {LogicalType::VARCHAR, LogicalType::VARCHAR}, ReadExec, TestsBind, ReadInit);
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
	loader.RegisterFunction(GlossColumnsFunctions());
	loader.RegisterFunction(GlossKindFunction(0, KindBind<0>));
	loader.RegisterFunction(GlossKindFunction(1, KindBind<1>));
	loader.RegisterFunction(GlossKindFunction(2, KindBind<2>));
}

} // namespace duckdb
