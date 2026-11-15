#pragma once

#include "duckdb.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/parser/grammar_extension.hpp"
#include "duckdb/parser/parser_extension.hpp"
#include "duckdb/parser/statement/select_statement.hpp"

#include <functional>
#include <map>
#include <set>

namespace duckdb {

//! The glossql grammar (glossql_grammar.cpp): activated per connection with
//! SET active_grammar_extensions = ['glossql']
class GlossGrammar final : public GrammarExtension {
public:
	GlossGrammar();
	vector<GrammarChange> GetChanges() const override;
};

//! What a glossql statement carries from the parser to the planner. `kind`
//! picks the table function that executes it (glossql_store.cpp,
//! glossql_runner.cpp); `row` is its positional input.
struct GlossStatementData : public ParserExtensionParseData {
	GlossStatementData(string kind_p, string table_p, vector<Value> row_p, string text_p)
	    : kind(std::move(kind_p)), table(std::move(table_p)), row(std::move(row_p)), text(std::move(text_p)) {
	}

	//! write | delete | dataset | recipe | preview | import | function | test
	string kind;
	//! the store relation, for a write or a delete
	string table;
	vector<Value> row;
	string text;

	unique_ptr<ParserExtensionParseData> Copy() const override {
		return make_uniq<GlossStatementData>(kind, table, row, text);
	}
	string ToString() const override {
		return text;
	}
};

//! The ParserExtension whose plan_function turns a GlossStatementData into a
//! scan of the executing table function. Never registered as a parser
//! fallback: the grammar builds the ExtensionStatement itself.
ParserExtension GlossParserExtension();

//! Registers the store (glossql_store.cpp): the glossql_init pragma, the
//! write and delete functions and the GLOSSARY() read
void RegisterGlossStore(ExtensionLoader &loader);
//! Registers the runner (glossql_runner.cpp): datasets, recipes, previews, imports
void RegisterGlossRunner(ExtensionLoader &loader);

//! A dataset is a schema of the workspace's lake (SPEC §3): the USE'd dataset
//! is the connection's default schema, and unprefixed subjects resolve against it
string CurrentDataset(ClientContext &context);
//! The attached DuckLake this connection works in: the only one, or the one
//! SET VARIABLE glossql_workspace names
string LakeName(ClientContext &context);
//! A JSON string literal, escaped
string JsonString(const string &text);
//! A list as JSON text, the portable form a store column holds
string JsonList(const vector<string> &items);
string JsonList(const Value &list);
//! The dataset a subject lies in: its first name; a pair path's is its left endpoint's
string DatasetOf(const string &subject);
//! The dataset a subject is written and read in: the above, or the USE'd dataset
//! for a bare source, a workspace row (SPEC §5.1)
string DatasetOf(ClientContext &context, const string &subject);
//! Resolves a subject or pair path to its dataset-qualified form
string ResolveSubject(ClientContext &context, const string &subject);
//! Who declares: kit:<name> while a kit runs, else the actor as kind:id (glossql_store.cpp)
string DeclaredBy(ClientContext &context);
//! A resolved subject's parts: the dataset, the table, and the column as all that
//! follows the second dot, since a column's name may hold a dot (SPEC §4)
vector<string> SubjectParts(const string &subject);
//! A relation of the lake by name, a loaded table or a view (SPEC §3: a view
//! counts as a table, so its columns are subjects like a table's); null when
//! neither holds
optional_ptr<CatalogEntry> RelationEntry(ClientContext &context, const string &schema, const string &table);
//! One column of a dataset's relation, as glossql_columns serves it
struct RelationColumn {
	string table;
	string column;
	string type;
	idx_t ordinal;
	bool view;
};
//! Every column of a dataset's tables, and of its views where `views`, by
//! relation name then position; one table's where `table` names it. A view
//! is bound here for its columns and left out when it does not bind. The
//! catalog is walked by name: no catalog-wide scan, so no view outside the
//! dataset is bound for it (glossql_store.cpp)
vector<RelationColumn> DatasetColumns(ClientContext &context, const string &dataset, const string &table, bool views);
//! Double-quotes a name for SQL text
string QuoteName(const string &name);
//! Single-quotes a string for SQL text
string QuoteText(const string &text);

//! The table functions the plan function dispatches to
TableFunction GlossWriteFunction(idx_t width);
TableFunction GlossDeleteFunction();
TableFunction GlossDatasetFunction();
TableFunction GlossRecipeFunction();
TableFunction GlossPreviewFunction();
TableFunction GlossImportFunction();
TableFunction GlossFunctionFunction();
TableFunction GlossTestFunction();
TableFunction GlossExtractFunction();

//! Registers json_schema_check (glossql_schema.cpp)
void RegisterGlossSchema(ExtensionLoader &loader);
//! Registers glossql_lineage (glossql_lineage.cpp)
void RegisterGlossLineage(ExtensionLoader &loader);
// glossql_service(route, body): one call to the model service the instance names (glossql_service.cpp)
void RegisterGlossService(ExtensionLoader &loader);

//! The derivation of each output column of a query, in output order: what
//! computes it, resolved through the bound plan down to the base scans;
//! `complete` is false when the walk could not follow a step (glossql_lineage.cpp)
struct QueryLineage {
	vector<pair<string, string>> columns;
	//! per column, how its value was computed where that decides how it adds up:
	//! "ratio", "average", "distinct", or empty
	vector<string> shapes;
	bool complete = true;
	//! every lake table the plan scans, as schema.table, joins and filters included
	std::set<std::pair<string, vector<string>>> scans;
	//! the pairs of scanned columns the plan's joins and filters equate, as
	//! schema.table.column, and per side whether the plan joins it one row per value
	std::map<std::pair<string, string>, std::pair<bool, bool>> joins;
	//! the mapping of a view: one row per output column, branch of its union and
	//! scanned column, filled where the view's own SQL is given
	struct Mapping {
		string column;
		idx_t branch;
		string derivation;
		//! schema.table and its column, empty where the derivation reads no lake table
		string table;
		string table_column;
		//! the name the view's SQL gives the scan, where it reads the table more than once
		string alias;
		//! the conjuncts of the view's joins and filters that read this scan alone
		string condition;
		vector<string> literals;
		bool complete;
	};
	vector<Mapping> mapping;
};
//! `view_sql`, the view's CREATE VIEW statement, asks for the mapping too
QueryLineage LineageOf(ClientContext &context, const string &sql, const string *view_sql = nullptr);
//! The lineage as one JSON object, column to derivation; a column the walk could
//! not follow carries a digest of the SQL, so it reads moved whenever the SQL did
string LineageJson(const QueryLineage &lineage, const string &sql);
//! Validates a JSON document against a JSON Schema: empty when it conforms, else the first violation
string JsonSchemaCheck(const string &schema, const string &document);
//! Empty, or why the literal is not a member of the enum the schema pins `value` to
string JsonSchemaValueEnumCheck(const string &schema, const string &literal);
//! A top-level string field of a JSON object, empty when absent
string JsonStringField(const string &document, const string &field);
//! The metric definition contract: sql or stopped, assumptions disclosed; every other key is a kit's
extern const char *const DEFINITION_CONTRACT;

//! Parses plain SQL with the stock grammar into one SELECT
unique_ptr<SelectStatement> ParseSelect(const string &sql);
//! Replaces $name with the replacement where $name holds as a whole word
string ReplacePlaceholder(const string &sql, const string &name, const string &replacement);
//! The first line of a text, trimmed
string FirstLine(const string &text);
//! Runs a kit's script under its name (glossql_kit): (statement, outcome) per statement it ran, or one
//! row (name, current) where the kits relation holds the same text and it has no LOAD to run; in_lake
//! runs it with the workspace's lake as the default catalog
vector<pair<string, string>> RunKit(ClientContext &context, const string &name, const string &location,
                                    const string &script, bool in_lake);

//! The workspace: one attached DuckLake. Its schemas are the datasets, its
//! metadata catalog holds the store relations as glossql_<relation> beside the
//! ducklake_* tables, text and numbers, and its schema `glossql` reads them as
//! typed views
struct Workspace {
	//! the lake's catalog
	string lake;
	//! the metadata catalog, and the schema in it that holds ducklake_snapshot
	string meta;
	string meta_schema;

	//! "meta"."schema"."glossql_<relation>"
	string Store(const string &relation) const;
	//! "meta"."schema"."ducklake_<table>"
	string Meta(const string &table) const;
	//! "lake"."dataset"."name"
	string Table(const string &dataset, const string &name) const;
};

class Inner;

//! What a stored result reads, by kind of read, per dataset: which of the kits' relations the
//! digests of those reads walk (ReadsNowSQL)
struct ReadNeeds {
	std::set<string> places;
	std::set<string> tables;
	std::set<string> definitions;
	std::set<string> lineage;
	std::set<string> annotations;
	//! the macros the functions' bodies follow, each with a digest of its definition now (MacroDigests)
	std::map<string, string> macros;

	//! A read of this kind (annotations, results, definition, data, lineage) of a result in the dataset
	void Add(const string &dataset, const string &read);
};
//! What a set of reads returns now, each as a digest (SPEC §6): CTE definitions to follow a WITH
//! over the caller's `wanted` (id, dataset, subject, function, aspect, read, what, where), ending
//! in now_versions (id, read, what, where, version) and now_results (id, read, what, where,
//! result), the stored results each read of results returned. A result stores these digests when
//! it is computed, and is current while its reads return the same (glossql_store.cpp)
string ReadsNowSQL(const Workspace &ws, const ReadNeeds &needs);
//! The macros the bodies of the current functions follow, or of one function's, as their reads name
//! them (functions.reads), each with a digest of its definition as the catalog holds it now; `gone`
//! where none does (glossql_runner.cpp)
std::map<string, string> MacroDigests(Inner &inner, const string &function);
//! An input's name, as the inputs relation stores it: read(what, where) for a read function's call
//! and for data; function and property for the declarations
string InputName(const string &read, const string &what, const string &where);
//! The current stored results, each row with `current`: each of its reads returns now what it
//! returned when it was computed, and no result it read is out of date (SPEC §6)
string CurrentResultsSQL(Inner &inner);
//! Why a stored result is out of date: (dataset, function, subject, input, reason), a read that
//! returns other rows now (reason `changed`), or a read of results that returned one out of date
//! (reason `outdated`)
string OutdatedSQL(Inner &inner);
//! The session variable that names the subject a function computes a result for: the
//! engine sets it on the connection of the run, and the read functions walk from it
constexpr const char *RUN_SUBJECT = "glossql_subject";

//! A second connection to the same database, for SQL run while the calling
//! connection's context lock is held (glossql_runner.cpp)
class Inner {
public:
	explicit Inner(ClientContext &context);

	//! Runs one statement; throws its error as InvalidInputException, a transaction conflict, an
	//! interrupt and the engine's own failures (fatal, internal, memory) as themselves
	unique_ptr<QueryResult> Run(const string &sql);
	//! The first column of the first row, or NULL for no rows
	Value Scalar(const string &sql);
	//! The first row, or empty for no rows
	vector<Value> Row(const string &sql);
	//! The store relation, in the workspace's metadata catalog
	string Store(const string &relation) const;
	//! The store relation's current rows, as a subquery: those no write has ended
	string Current(const string &relation) const;
	//! One catalog write. `body` reads and writes the store through the write it
	//! is handed; its rows, the lake snapshot whose id they carry and that
	//! snapshot's change row commit as one transaction of the catalogue database.
	//! A write that loses a race to another writer runs again, whole. Returns the
	//! snapshot id, or 0 for a body that wrote nothing. A body touches the store
	//! and the lake's metadata tables only: one transaction writes one database
	int64_t Write(ClientContext &context, const std::function<void(class CatalogWrite &)> &body);
	//! Whether a write that failed with `error`, having read the snapshot `head`, lost a
	//! race; `taking_snapshot` while it writes its snapshot row and commits
	bool Overtaken(const ErrorData &error, int64_t head, bool taking_snapshot);
	//! "lake"."schema"."name"
	string Table(const string &schema, const string &name) const;
	//! The lake's current snapshot id
	Value Snapshot();
	//! Whether a declared glossql function of that name serves the dataset: its own, then GLOBAL
	string FunctionSchema(const string &dataset, const string &name);

	Connection con;
	Workspace ws;
	//! whether a statement of the catalog write in progress failed (CatalogWrite::Run)
	bool catalog_failed = false;
};

//! A catalog write in progress (Inner::Write): the snapshot id its rows carry,
//! and the statements that write. A statement of these that fails raises the
//! catalog database's error; Inner::Write runs the write again if it lost a race
class CatalogWrite {
public:
	CatalogWrite(Inner &inner, int64_t snapshot, int64_t next_catalog_id)
	    : inner(inner), snapshot(snapshot), next_catalog_id(next_catalog_id) {
	}

	//! Runs one writing statement; returns the rows it changed
	int64_t Run(const string &sql);
	//! Ends the relation's current rows that `where` selects; returns how many
	int64_t End(const string &relation, const string &where);
	//! Appends a row that begins at this write: `values` are the relation's columns after the markers
	void Insert(const string &relation, const string &values);
	//! An id for a lake object this write creates
	int64_t NewCatalogId();
	//! Names a lake change this write makes itself, in DuckLake's change text
	void Changed(const string &entry);

	Inner &inner;
	int64_t snapshot;
	int64_t next_catalog_id;
	//! the lake changes this write makes itself: none for a write to the store alone
	vector<string> changes;
	bool wrote = false;
};

//! The store's version, that the reads and the kept views are kept under: the
//! lake's snapshot, which every catalog write and every load moves (glossql_store.cpp)
string StoreVersion(Inner &inner);

} // namespace duckdb
