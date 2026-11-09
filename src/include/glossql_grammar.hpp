#pragma once

#include "duckdb.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/parser/grammar_extension.hpp"
#include "duckdb/parser/parser_extension.hpp"
#include "duckdb/parser/statement/select_statement.hpp"

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
	bool complete = true;
	//! every lake table the plan scans, as schema.table, joins and filters included
	std::set<std::pair<string, vector<string>>> scans;
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
extern const char *const GROUNDING_CONTRACT;

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

//! The legs a measurement can read, each at its version now: (name, version, kind) —
//! every live table of the lake as <schema>.<table> at the last snapshot that
//! wrote, altered or dropped it, every live view but the store's own as
//! <schema>.<view> at the snapshot that defined it, every store relation but
//! measurements as glossql.<relation> at its highest seq ('-' while empty); kind is
//! table, view or store. A measurement's pin is these parts joined, sorted,
//! name:version; glossql_store.cpp
string LegsSQL(const Workspace &ws);
//! The store's legs by name, quoted and comma-separated: 'main.aspects', ... (measurements is none)
string StoreLegNames();
//! Whether the measurement row `row` (pin, reads, dataset, function, computed_at)
//! still holds, as a SQL boolean over a CTE named legs (LegsSQL): the legs its
//! reads name look the same now as in its pin, and its function has not been
//! re-declared since
string StandsSQL(const Workspace &ws, const string &row);

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
	//! The next seq of a store relation, as SQL: rows are ordered by it
	string Seq(const string &relation) const;
	//! "lake"."schema"."name"
	string Table(const string &schema, const string &name) const;
	//! The lake's current snapshot id
	Value Snapshot();
	//! Whether a declared glossql function of that name serves the dataset: its own, then GLOBAL
	string FunctionSchema(const string &dataset, const string &name);

	Connection con;
	Workspace ws;
};

//! The store's version — the lake's snapshot and every store relation's
//! highest seq, row count and newest stamp — that the reads and the kept views
//! are kept under; any write or delete moves it (glossql_store.cpp)
string StoreVersion(Inner &inner);

} // namespace duckdb
