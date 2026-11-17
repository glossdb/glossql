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
	//! schema.table.column, the left side as the predicate writes it (a join's
	//! left input), per side whether the plan joins it one row per value, and
	//! the join or filter that equates them, numbered in the plan: the pairs of
	//! one number are that join's key
	struct JoinPair {
		string left;
		string right;
		bool left_unique;
		bool right_unique;
		idx_t join;
	};
	vector<JoinPair> joins;
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
		//! the equalities of the join that looks this scan up: its columns against
		//! the columns of the side it is joined to
		string lookup;
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

//! The current stored results `filter` selects, a condition on the measurements relation, each row with
//! `current`: no node it read changed after the snapshot it read at, and no result it read is out of
//! date (SPEC §6). The check walks only those results and what they read
string CurrentResultsSQL(Inner &inner, const string &filter);
//! Why a stored result is out of date: (dataset, function, subject, asset, what, reason), a node it
//! read that changed after it read (reason `changed`), or a result it read that is out of date (reason
//! `outdated`)
string OutdatedSQL(Inner &inner);
//! An asset of a kind a kit declares (glossql_kind_assets()) that a relation of the dataset names too, or
//! that `relation` would name; empty where none does (SPEC §4)
string KindNameTaken(Inner &inner, const string &dataset, const string &relation);
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
	//! The first of `count` ids for the results this write stores: past every id the store holds
	int64_t NewResultIds(idx_t count);
	//! Names a lake change this write makes itself, in DuckLake's change text
	void Changed(const string &entry);

	Inner &inner;
	int64_t snapshot;
	int64_t next_catalog_id;
	//! the next result id, read from the store at the first result this write stores
	int64_t next_result_id = -1;
	//! the lake changes this write makes itself: none for a write to the store alone
	vector<string> changes;
	bool wrote = false;
};

//! One result a write stores (StoreResults): its subject, its value as JSON text or its error, the nodes it
//! read and the lineage edges it holds from its subject, (to, kind)
struct ResultRow {
	string subject;
	Value value;
	Value error;
	vector<pair<string, string>> nodes;
	vector<pair<string, string>> edges;
};
//! Stores results of one function in one write, each read at `read_at`, with its inputs and edges, and
//! retires the stored results of the function on the subjects of `retire`. The result a row replaces
//! ends, its inputs and edges with it; the same value and error keep the snapshot they changed at
void StoreResults(CatalogWrite &write, const string &dataset, const string &function, const string &aspect,
                  int64_t read_at, const vector<ResultRow> &rows, const vector<string> &retire = {});

//! A view's lineage as its stored result holds it (glossql_lineage.cpp): the view's path as the catalog
//! spells it; the value, or the error where the view does not plan; the relations and columns it reads,
//! its edges; and the nodes the planning read, the view's definition and that of every relation and
//! macro its SQL names, through the views it reads
struct ViewLineageResult {
	string view;
	string value;
	string error;
	vector<string> reads;
	vector<pair<string, string>> nodes;
};
//! Plans one view of the lake as the inner connection's transaction sees it; not_a_view where none is
ViewLineageResult PlanViewLineage(Inner &inner, const string &dataset, const string &view);
//! Stores a view's lineage as the result of `lineage` on the view, read at `read_at`, with its inputs and
//! edges; the result it replaces ends, and the same value and error keep the snapshot they changed at
void StoreViewLineage(CatalogWrite &write, const string &dataset, const ViewLineageResult &lineage, int64_t read_at);
//! Ends the stored lineage of a view, its inputs and its edges; false where none is stored
bool RetireViewLineage(CatalogWrite &write, const string &dataset, const string &view);

} // namespace duckdb
