#pragma once

#include "duckdb.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/parser/grammar_extension.hpp"
#include "duckdb/parser/parser_extension.hpp"
#include "duckdb/parser/statement/select_statement.hpp"

#include <functional>
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
};
QueryLineage LineageOf(ClientContext &context, const string &sql);
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

//! The parts a stored result can read whatever its subject, each at its version
//! now: (name, version, kind). Every live table of the lake as <schema>.<table>
//! at the last snapshot that wrote, altered or dropped it; every live view but
//! the store's own as <schema>.<view> at the snapshot that defined it; every
//! store relation but the results' as main.<relation> at its highest marker;
//! the lake's shape, and each dataset's as lake.shape/<dataset>. A set of parts is a part too, its version a digest of
//! its members: <dataset>.* the dataset's tables, <dataset>.** what a body whose reads could not be named may have
//! read. kind is table, view, store or set. The glossary, the results and the declarations about subjects are read
//! within a closure instead (ScopedRead) (glossql_store.cpp)
string PartsSQL(const Workspace &ws);
//! A bind names an input it read beside the plan, by the part's name: a table
//! function whose rows come from the catalog or the lake's shape leaves a plan
//! that cannot show them (glossql_runner.cpp)
void BindRead(ClientContext &context, const string &name);
//! A bind names one property it read: its annotations and the results stored under it
void BindProperty(ClientContext &context, const string &aspect);
//! The lake's shape as a part: its tables' columns and its views' definitions
constexpr const char *LAKE_SHAPE = "lake.shape";
//! The part a read names: `*` and `**` resolve to the dataset's sets; empty for
//! the function's own results, which are no input of its own
string InputName(const string &read, const string &dataset, const string &function);
//! The current stored results, each row with `current`: every part it read has
//! the version it read, the rows of its closure it read hold what they held, no
//! result it read is out of date, and its function has not been declared again
//! since (SPEC §6)
string CurrentResultsSQL(const Workspace &ws);

//! What a computation on a subject sees of the catalog (SPEC §6): the subject's
//! closure. A run holds it on its connection while it plans and runs, and every
//! read of the glossary, the results and the declarations about subjects
//! returns the rows within it and no other (glossql_sees, GLOSSARY(), TESTS()).
//! Members, as an inputs row stores them: d:<dataset> every row of the dataset,
//! s:<subject> a subject's rows, p:<subject>::<property> one property's rows on
//! a subject, r:<subject> the results stored on a subject
struct RunClosure {
	string dataset;
	bool whole = false;
	std::set<string> subjects;
	std::set<pair<string, string>> properties;
	std::set<string> results;
	//! the properties whose declarations and tests the run sees: those of the
	//! views its closure holds, and those its reads name
	std::set<string> named;

	//! The members as text, one per row the closure admits by
	vector<string> Members() const;
	//! Whether a row of a store relation is within the closure
	bool Sees(const string &relation, const string &dataset, const string &subject, const string &aspect) const;
};
//! The closure the connection's run holds, or none outside a run
shared_ptr<RunClosure> RunClosureOf(ClientContext &context);
void SetRunClosure(ClientContext &context, shared_ptr<RunClosure> closure);
//! Whether an input names rows of a closure: the glossary, the results and
//! the declarations about subjects, main.<relation>[/<key>|@<property>]
bool ScopedInput(const string &name);
//! The session variable that names the subject a function computes a result for: the
//! engine sets it on the connection of the run, and the read functions walk from it
constexpr const char *RUN_SUBJECT = "glossql_subject";
//! The input that stores a result's closure; its version is the members as a JSON list
constexpr const char *CLOSURE_INPUT = "closure";
//! The closure members (id, kind, subject, property) of a JSON list of members, per id
string MembersSQL(const string &lists);
//! The versions of scoped inputs now, (id, input, version), over the CTEs
//! `members` (id, kind, subject, property) and `reads` (id, dataset, function,
//! input) the caller defines: a digest of the rows of the closure each input
//! names, as they hold now. Also defines sc_rv (id, result, function, aspect):
//! the results each closure holds. Returns CTE definitions to follow a WITH
string ScopedSQL(const Workspace &ws);
//! The CTEs of the current results' scoped inputs as stored, and their versions
//! now (ScopedSQL): what the currency rule and glossql_parts() read
string HeldSQL(const Workspace &ws);

//! A second connection to the same database, for SQL run while the calling
//! connection's context lock is held (glossql_runner.cpp)
class Inner {
public:
	explicit Inner(ClientContext &context);

	//! Runs one statement; throws its error as InvalidInputException
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
