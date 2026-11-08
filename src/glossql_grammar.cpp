// The glossql grammar as a DuckDB 2.0 GrammarExtension.
//
// Every statement of ../glossql/grammar.ebnf is a rule prepended to
// `Statement`; its transform builds an ExtensionStatement whose plan is one
// scan of the table function that executes it (glossql_store.cpp,
// glossql_runner.cpp). The reads are rules prepended to `InnerTableRef`,
// transformed into stock table-function references. Subjects and pair paths
// are rules of their own, so the stock binder never sees `orders.amount` as
// a column reference; bodies are the native dollar-quoted string token; new
// words are unreserved keywords, still usable as identifiers.
//
// Syntax is grammar.ebnf; semantics are SPEC.md there. Host SQL (USE,
// SELECT, ...) is DuckDB's own; DELETE FROM glossary is a statement here,
// since the lake reads the store as views and the engine deletes through a
// table only. The store the statements write to is created by `PRAGMA glossql`.

#include "glossql_grammar.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/grammar_change.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/parser_extension.hpp"
#include "duckdb/parser/peg/ast/table_alias.hpp"
#include "duckdb/parser/peg/matcher.hpp"
#include "duckdb/parser/peg/parsed_grammar.hpp"
#include "duckdb/parser/peg/transformer/parse_result.hpp"
#include "duckdb/parser/peg/transformer/peg_transformer.hpp"
#include "duckdb/parser/statement/extension_statement.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

#include <set>

namespace duckdb {

//===--------------------------------------------------------------------===//
// Parse-tree helpers (the shapes the matchers produce for the grammar macros)
//===--------------------------------------------------------------------===//

static bool IsRule(const ParseResult &parse_result, const char *name) {
	return StringUtil::CIEquals(parse_result.Name(), name);
}

//! A rule `R <- A / B / C` yields a list holding one choice; this is the alternative that matched.
static ParseResult &GetChoice(ParseResult &parse_result) {
	auto &list = parse_result.Cast<ListParseResult>();
	return list.Child<ChoiceParseResult>(0).GetResult();
}

//! List(D) <- D (',' D)* ','?
static vector<reference<ParseResult>> ListItems(ParseResult &parse_result) {
	vector<reference<ParseResult>> result;
	auto &list = parse_result.Cast<ListParseResult>();
	result.push_back(list.GetChild(0));
	auto &rest = list.Child<OptionalParseResult>(1);
	if (rest.HasResult()) {
		for (auto &child : rest.GetResult().Cast<RepeatParseResult>().GetChildren()) {
			result.push_back(child.get().Cast<ListParseResult>().GetChild(1));
		}
	}
	return result;
}

//! Parens(D) <- '(' D ')'
static ParseResult &InParens(ParseResult &parse_result) {
	return parse_result.Cast<ListParseResult>().GetChild(1);
}

//! A rule whose body is one macro call (R <- Parens(List(X))) wraps the macro's list in a list of its own
static ParseResult &MacroBody(ParseResult &parse_result) {
	return parse_result.Cast<ListParseResult>().GetChild(0);
}

static string Ident(PEGTransformer &transformer, ParseResult &identifier) {
	return transformer.Transform<string>(identifier);
}

static string Text(PEGTransformer &transformer, ParseResult &string_literal) {
	return transformer.Transform<string>(string_literal);
}

//===--------------------------------------------------------------------===//
// Subjects: path | pair path (SPEC §4)
//===--------------------------------------------------------------------===//

//! GlossRelSide <- GlossRelPrefix+ GlossRelKey ; GlossRelPrefix <- Identifier '.'
//! GlossRelKey <- GlossRelTuple / Identifier ; GlossRelTuple <- Parens(List(Identifier))
static string RelSideText(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list = parse_result.Cast<ListParseResult>();
	auto &prefixes = list.Child<RepeatParseResult>(0);
	vector<string> parts;
	for (auto &prefix : prefixes.GetChildren()) {
		parts.push_back(Ident(transformer, prefix.get().Cast<ListParseResult>().GetChild(0)));
	}
	if (parts.size() > 2) {
		throw ParserException("a relationship endpoint is [dataset.]table.key, not more");
	}
	auto &key = GetChoice(list.GetChild(1));
	if (IsRule(key, "GlossRelTuple")) {
		vector<string> columns;
		for (auto &item : ListItems(InParens(MacroBody(key)))) {
			columns.push_back(Ident(transformer, item.get()));
		}
		if (columns.size() < 2) {
			throw ParserException("a composite key names at least two columns");
		}
		parts.push_back("(" + StringUtil::Join(columns, ", ") + ")");
	} else {
		parts.push_back(Ident(transformer, key));
	}
	return StringUtil::Join(parts, ".");
}

//! GlossRelOp <- GlossOneToOne / GlossManyToOne
static string RelOpText(ParseResult &parse_result) {
	auto &op = GetChoice(parse_result);
	if (IsRule(op, "GlossOneToOne")) {
		return "<->";
	}
	return "->";
}

//! GlossPairPath <- GlossRelSide GlossRelOp GlossRelSide
static string PairPathText(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list = parse_result.Cast<ListParseResult>();
	return RelSideText(transformer, list.GetChild(0)) + " " + RelOpText(list.GetChild(1)) + " " +
	       RelSideText(transformer, list.GetChild(2));
}

//! GlossPath <- Identifier GlossPathTail* ; GlossPathTail <- '.' Identifier
static vector<string> PathParts(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list = parse_result.Cast<ListParseResult>();
	vector<string> parts;
	parts.push_back(Ident(transformer, list.GetChild(0)));
	auto &tail = list.Child<OptionalParseResult>(1);
	if (tail.HasResult()) {
		for (auto &child : tail.GetResult().Cast<RepeatParseResult>().GetChildren()) {
			parts.push_back(Ident(transformer, child.get().Cast<ListParseResult>().GetChild(1)));
		}
	}
	return parts;
}

static string PathText(PEGTransformer &transformer, ParseResult &parse_result) {
	auto parts = PathParts(transformer, parse_result);
	if (parts.size() > 3) {
		throw ParserException("a subject is dataset, dataset.table or dataset.table.column, not more");
	}
	return StringUtil::Join(parts, ".");
}

//! GlossSubject <- GlossPairPath / GlossPath
static string SubjectText(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &subject = GetChoice(parse_result);
	if (IsRule(subject, "GlossPairPath")) {
		return PairPathText(transformer, subject);
	}
	return PathText(transformer, subject);
}

//===--------------------------------------------------------------------===//
// Settings: SET (key: value, ...) is stored as one JSON object, the pairs verbatim;
// which keys a statement takes is a JSON Schema the extension holds, checked
// here, at parse
//===--------------------------------------------------------------------===//

//! DECLARE SOURCE (SPEC §3): the kind (any word; relational_db is the one the engine acts on), where, and the
//! reference to the engine-held secret
static constexpr const char *SOURCE_SETTINGS =
    R"({"type": "object", "required": ["type", "location"], "additionalProperties": false, "properties": {
    "type": {"type": "string"}, "location": {"type": "string"},
    "via": {"type": "string"}}})";
//! DECLARE DATASET
static constexpr const char *DATASET_SETTINGS =
    R"({"type": "object", "additionalProperties": false, "properties": {"purpose": {"type": "string"}}})";
//! DECLARE RECIPE: the table's key, one column or 'a, b'
static constexpr const char *RECIPE_SETTINGS =
    R"({"type": "object", "additionalProperties": false, "properties": {"key": {"type": "string"}}})";

//! GlossPair <- Identifier ':' GlossPairValue ; GlossPairValue <- StringLiteral / NumberLiteral / Identifier
//! The value as JSON: a string or a name is a JSON string, a number holds as written
static pair<string, string> PairOf(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list = parse_result.Cast<ListParseResult>();
	auto key = StringUtil::Lower(Ident(transformer, list.GetChild(0)));
	auto &value = GetChoice(list.GetChild(2));
	if (IsRule(value, "StringLiteral")) {
		return {key, JsonString(Text(transformer, value))};
	}
	if (IsRule(value, "NumberLiteral")) {
		return {key, transformer.Transform<unique_ptr<ParsedExpression>>(value)->ToString()};
	}
	return {key, JsonString(Ident(transformer, value))};
}

//! GlossSettings <- 'SET' Parens(List(GlossPair)): the pairs as one JSON object that meets the
//! statement's schema
static string SettingsOf(PEGTransformer &transformer, ParseResult &parse_result, const char *schema,
                         const string &statement) {
	vector<string> members;
	std::set<string> seen;
	auto &list = parse_result.Cast<ListParseResult>();
	for (auto &item : ListItems(InParens(list.GetChild(1)))) {
		auto pair = PairOf(transformer, item.get());
		if (!seen.insert(pair.first).second) {
			throw ParserException("%s: setting '%s' is given twice", statement, pair.first);
		}
		members.push_back(JsonString(pair.first) + ":" + pair.second);
	}
	auto json = "{" + StringUtil::Join(members, ",") + "}";
	auto error = JsonSchemaCheck(schema, json);
	if (!error.empty()) {
		throw ParserException("%s: settings %s", statement, error);
	}
	return json;
}

//===--------------------------------------------------------------------===//
// Statements: each becomes an ExtensionStatement
//===--------------------------------------------------------------------===//

static ParserExtension &Extension() {
	static ParserExtension extension = GlossParserExtension();
	return extension;
}

static arena_ptr<TransformResultValue> MakeStatement(PEGTransformer &transformer, const string &kind,
                                                     const string &table, vector<Value> row, string text) {
	auto data = make_uniq<GlossStatementData>(kind, table, std::move(row), std::move(text));
	unique_ptr<SQLStatement> statement = make_uniq<ExtensionStatement>(Extension(), std::move(data));
	return transformer.MakeResult<unique_ptr<SQLStatement>>(std::move(statement));
}

static arena_ptr<TransformResultValue> MakeWrite(PEGTransformer &transformer, const string &table, vector<Value> row,
                                                 string text) {
	return MakeStatement(transformer, "write", table, std::move(row), std::move(text));
}

#define GLOSSQL_TRANSFORM(START, FINALIZE)                                                                             \
	static arena_ptr<TransformProcess> START(PEGTransformer &transformer, ParseResult &parse_result) {                 \
		return transformer.MakeProcess<FinalizeTransformProcess>(transformer, parse_result, FINALIZE);                 \
	}

//! GlossGlossStatement <- 'GLOSS' Identifier 'ON' GlossSubject 'AS' StringLiteral
static arena_ptr<TransformResultValue> FinalizeGloss(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list = parse_result.Cast<ListParseResult>();
	auto aspect = Ident(transformer, list.GetChild(1));
	auto subject = SubjectText(transformer, list.GetChild(3));
	auto body = Text(transformer, list.GetChild(5));
	return MakeWrite(transformer, "glossary", {Value(subject), Value(aspect), Value(body)},
	                 "GLOSS " + aspect + " ON " + subject);
}
GLOSSQL_TRANSFORM(StartGloss, FinalizeGloss)

//! GlossDeclareSource <- 'DECLARE' 'SOURCE' Identifier GlossSettings
static arena_ptr<TransformResultValue> FinalizeDeclareSource(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list = parse_result.Cast<ListParseResult>();
	auto name = Ident(transformer, list.GetChild(2));
	auto settings = SettingsOf(transformer, list.GetChild(3), SOURCE_SETTINGS, "DECLARE SOURCE");
	return MakeWrite(transformer, "sources", {Value(name), Value(settings)}, "DECLARE SOURCE " + name);
}
GLOSSQL_TRANSFORM(StartDeclareSource, FinalizeDeclareSource)

//! GlossDeclareDataset <- 'DECLARE' 'DATASET' Identifier GlossSettings
static arena_ptr<TransformResultValue> FinalizeDeclareDataset(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list = parse_result.Cast<ListParseResult>();
	auto name = Ident(transformer, list.GetChild(2));
	auto settings = SettingsOf(transformer, list.GetChild(3), DATASET_SETTINGS, "DECLARE DATASET");
	return MakeStatement(transformer, "dataset", "", {Value(name), Value(settings)}, "DECLARE DATASET " + name);
}
GLOSSQL_TRANSFORM(StartDeclareDataset, FinalizeDeclareDataset)

//! GlossDeclareRecipe <- 'DECLARE' 'RECIPE' Identifier 'ON' Identifier 'FROM' Identifier GlossSettings?
//!                         'AS' StringLiteral
static arena_ptr<TransformResultValue> FinalizeDeclareRecipe(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list = parse_result.Cast<ListParseResult>();
	auto name = Ident(transformer, list.GetChild(2));
	auto dataset = Ident(transformer, list.GetChild(4));
	auto source = Ident(transformer, list.GetChild(6));
	string settings = "{}";
	auto &settings_opt = list.Child<OptionalParseResult>(7);
	if (settings_opt.HasResult()) {
		settings = SettingsOf(transformer, settings_opt.GetResult(), RECIPE_SETTINGS, "DECLARE RECIPE");
	}
	auto sql = Text(transformer, list.GetChild(9));
	return MakeStatement(transformer, "recipe", "",
	                     {Value(name), Value(dataset), Value(source), Value(settings), Value(sql)},
	                     "DECLARE RECIPE " + name + " ON " + dataset + " FROM " + source);
}
GLOSSQL_TRANSFORM(StartDeclareRecipe, FinalizeDeclareRecipe)

//! GlossPreview <- 'PREVIEW' Identifier 'AS' StringLiteral
static arena_ptr<TransformResultValue> FinalizePreview(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list = parse_result.Cast<ListParseResult>();
	auto source = Ident(transformer, list.GetChild(1));
	auto sql = Text(transformer, list.GetChild(3));
	return MakeStatement(transformer, "preview", "", {Value(source), Value(sql)}, "PREVIEW " + source);
}
GLOSSQL_TRANSFORM(StartPreview, FinalizePreview)

//! GlossImport <- 'IMPORT' Identifier GlossPathTail?
static arena_ptr<TransformResultValue> FinalizeImport(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list = parse_result.Cast<ListParseResult>();
	auto first = Ident(transformer, list.GetChild(1));
	Value dataset(LogicalType::VARCHAR);
	string table = first;
	auto &tail = list.Child<OptionalParseResult>(2);
	if (tail.HasResult()) {
		dataset = Value(first);
		table = Ident(transformer, tail.GetResult().Cast<ListParseResult>().GetChild(1));
	}
	return MakeStatement(transformer, "import", "", {dataset, Value(table)},
	                     "IMPORT " + (dataset.IsNull() ? table : first + "." + table));
}
GLOSSQL_TRANSFORM(StartImport, FinalizeImport)

//! GlossDeclareFunction <- 'DECLARE' 'FUNCTION' Identifier 'FOR' GlossFunctionScope 'AS' StringLiteral
//!                           GlossReturns?
//! GlossFunctionScope <- GlossScopeGlobal / Identifier ; GlossScopeGlobal <- 'GLOBAL'
//! GlossReturns <- 'RETURNS' Identifier
static arena_ptr<TransformResultValue> FinalizeDeclareFunction(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list = parse_result.Cast<ListParseResult>();
	auto name = Ident(transformer, list.GetChild(2));
	auto &scope_choice = GetChoice(list.GetChild(4));
	auto scope = IsRule(scope_choice, "GlossScopeGlobal") ? string("GLOBAL") : Ident(transformer, scope_choice);
	auto script = Text(transformer, list.GetChild(6));
	Value returns(LogicalType::VARCHAR);
	auto &returns_opt = list.Child<OptionalParseResult>(7);
	if (returns_opt.HasResult()) {
		returns = Value(Ident(transformer, returns_opt.GetResult().Cast<ListParseResult>().GetChild(1)));
	}
	return MakeStatement(transformer, "function", "", {Value(name), Value(scope), Value(script), returns},
	                     "DECLARE FUNCTION " + name + " FOR " + scope);
}
GLOSSQL_TRANSFORM(StartDeclareFunction, FinalizeDeclareFunction)

//! The grammar's expression language has no lookahead, so the end of a
//! statement is a custom terminal: it succeeds without consuming when the
//! next token is a terminator or the end of the input.
class GlossEndMatcher : public AtomicMatcher {
public:
	static constexpr MatcherType TYPE = MatcherType::CUSTOM;

public:
	GlossEndMatcher() : AtomicMatcher(TYPE) {
	}

	MatcherResult MatchAtomic(MatchState &state) const override {
		auto current = state.token_iterator.Current();
		if (current && current->type != TokenType::TERMINATOR && current->type != TokenType::END_OF_INPUT &&
		    current->type != TokenType::END_OF_INPUT_AUTOCOMPLETE) {
			return MatcherResult::Failure();
		}
		optional_idx offset = current ? optional_idx(current->offset) : optional_idx();
		return state.AllocateParseResult<TokenParseResult>(string(), offset, optional_idx(0));
	}

	SuggestionType AddSuggestionInternal(MatchState &state) const override {
		return SuggestionType::MANDATORY;
	}

	string ToString() const override {
		return "GlossEnd";
	}
};

//! One word of the store's name, bare or quoted: a keyword literal sees a
//! quoted identifier with its quotes, and the lake's view may be spelled so
class GlossWordMatcher : public AtomicMatcher {
public:
	static constexpr MatcherType TYPE = MatcherType::CUSTOM;

public:
	explicit GlossWordMatcher(string word_p) : AtomicMatcher(TYPE), word(std::move(word_p)) {
	}

	MatcherResult MatchAtomic(MatchState &state) const override {
		auto token = state.token_iterator.Current();
		if (!token) {
			return MatcherResult::Failure();
		}
		auto &text = token->text;
		auto spelled = text;
		if (spelled.size() > 2 && (spelled.front() == '"' || spelled.front() == '`') &&
		    spelled.back() == spelled.front()) {
			spelled = spelled.substr(1, spelled.size() - 2);
		}
		if (!StringUtil::CIEquals(spelled, word)) {
			return MatcherResult::Failure();
		}
		optional_idx offset(token->offset);
		optional_idx length(token->length);
		state.token_iterator.Advance();
		state.UpdateMaxTokenIndex();
		return state.AllocateParseResult<TokenParseResult>(text, offset, length);
	}

	SuggestionType AddSuggestionInternal(MatchState &state) const override {
		return SuggestionType::MANDATORY;
	}

	string ToString() const override {
		return "'" + StringUtil::Upper(word) + "'";
	}

private:
	string word;
};

//! GlossDeleteGlossary <- 'DELETE' 'FROM' GlossGlossaryTarget TargetAlias? WhereClause? GlossEnd
//! GlossGlossaryTarget <- GlossGlossaryInLake / GlossGlossaryInSchema / GlossGlossaryWord
//! GlossGlossaryInLake <- Identifier '.' GlossSchemaWord '.' GlossGlossaryWord
//! GlossGlossaryInSchema <- GlossSchemaWord '.' GlossGlossaryWord
//! SPEC §5.2's removal: the lake reads the store as views, and the engine
//! deletes through a table only, so the statement runs on the store's table
//! (glossql_store.cpp). The alias and the WHERE ride as text; the lake named
//! is held against the workspace's at run
static arena_ptr<TransformResultValue> FinalizeDeleteGlossary(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list = parse_result.Cast<ListParseResult>();
	auto &target = GetChoice(list.GetChild(2));
	Value lake(LogicalType::VARCHAR);
	string spelled = "glossary";
	if (IsRule(target, "GlossGlossaryInLake")) {
		lake = Value(Ident(transformer, target.Cast<ListParseResult>().GetChild(0)));
		spelled = lake.ToString() + ".main.glossary";
	} else if (IsRule(target, "GlossGlossaryInSchema")) {
		spelled = "main.glossary";
	}
	Value alias(LogicalType::VARCHAR);
	auto &alias_opt = list.Child<OptionalParseResult>(3);
	if (alias_opt.HasResult()) {
		alias = Value(transformer.Transform<Identifier>(alias_opt.GetResult()).GetIdentifierName());
	}
	Value where(LogicalType::VARCHAR);
	auto &where_opt = list.Child<OptionalParseResult>(4);
	if (where_opt.HasResult()) {
		where = Value(transformer.Transform<unique_ptr<ParsedExpression>>(where_opt.GetResult())->ToString());
	}
	auto text = "DELETE FROM " + spelled + (alias.IsNull() ? "" : " AS " + alias.ToString()) +
	            (where.IsNull() ? "" : " WHERE " + where.ToString());
	return MakeStatement(transformer, "delete", "glossary", {lake, alias, where}, text);
}
GLOSSQL_TRANSFORM(StartDeleteGlossary, FinalizeDeleteGlossary)

//! GlossExtract <- 'SELECT' List(GlossCall) 'FROM' GlossSubject GlossEnd
//! GlossCall <- Identifier '(' ')'
//! Whether this is an extraction or the plain SQL it looks like is the binder's
//! call: the statement becomes SELECT * FROM glossql_select_or_extract(...)
static arena_ptr<TransformResultValue> FinalizeExtract(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list = parse_result.Cast<ListParseResult>();
	vector<unique_ptr<ParsedExpression>> calls;
	vector<string> names;
	for (auto &call : ListItems(list.GetChild(1))) {
		auto name = Ident(transformer, call.get().Cast<ListParseResult>().GetChild(0));
		calls.push_back(ConstantExpression::String(name));
		names.push_back(name + "()");
	}
	auto subject = SubjectText(transformer, list.GetChild(3));
	auto text = "SELECT " + StringUtil::Join(names, ", ") + " FROM " + subject;

	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(ConstantExpression::String(subject));
	children.push_back(make_uniq<FunctionExpression>(Identifier("list_value"), std::move(calls)));
	children.push_back(ConstantExpression::String(text));
	auto ref = make_uniq<TableFunctionRef>();
	ref->function = make_uniq<FunctionExpression>(Identifier("glossql_select_or_extract"), std::move(children));
	auto node = make_uniq<SelectNode>();
	node->select_list.push_back(make_uniq<StarExpression>());
	node->from_table = std::move(ref);
	auto select = make_uniq<SelectStatement>();
	select->node = std::move(node);
	unique_ptr<SQLStatement> statement = std::move(select);
	return transformer.MakeResult<unique_ptr<SQLStatement>>(std::move(statement));
}
GLOSSQL_TRANSFORM(StartExtract, FinalizeExtract)

//! GlossDeclareTest <- 'DECLARE' 'TEST' Identifier 'ON' Identifier GlossBy? GlossDetector?
//!                          GlossThreshold?
//! GlossBy <- 'BY' Parens(List(GlossSpeaker)) ; GlossSpeaker <- GlossSpeakerAgent / GlossSpeakerHuman
//! GlossDetector <- 'DETECTOR' Identifier ; GlossThreshold <- 'THRESHOLD' NumberLiteral
static arena_ptr<TransformResultValue> FinalizeDeclareTest(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list = parse_result.Cast<ListParseResult>();
	auto name = Ident(transformer, list.GetChild(2));
	auto aspect = Ident(transformer, list.GetChild(4));
	vector<Value> speakers;
	auto &by = list.Child<OptionalParseResult>(5);
	if (by.HasResult()) {
		for (auto &speaker : ListItems(InParens(by.GetResult().Cast<ListParseResult>().GetChild(1)))) {
			speakers.push_back(Value(IsRule(GetChoice(speaker.get()), "GlossSpeakerHuman") ? "HUMAN" : "AGENT"));
		}
	}
	Value detector(LogicalType::VARCHAR);
	auto &detector_opt = list.Child<OptionalParseResult>(6);
	if (detector_opt.HasResult()) {
		detector = Value(Ident(transformer, detector_opt.GetResult().Cast<ListParseResult>().GetChild(1)));
	}
	Value threshold(LogicalType::DOUBLE);
	auto &threshold_opt = list.Child<OptionalParseResult>(7);
	if (threshold_opt.HasResult()) {
		auto &number = threshold_opt.GetResult().Cast<ListParseResult>().GetChild(1);
		auto text = transformer.Transform<unique_ptr<ParsedExpression>>(number)->ToString();
		double value = std::stod(text);
		if (value < 0 || value > 1) {
			throw ParserException("DECLARE TEST: THRESHOLD is in 0..1, not %s", text);
		}
		threshold = Value::DOUBLE(value);
	}
	if (speakers.empty() && detector.IsNull()) {
		throw ParserException("DECLARE TEST names BY or DETECTOR, or both");
	}
	return MakeStatement(
	    transformer, "test", "",
	    {Value(name), Value(aspect), Value::LIST(LogicalType::VARCHAR, std::move(speakers)), detector, threshold},
	    "DECLARE TEST " + name + " ON " + aspect);
}
GLOSSQL_TRANSFORM(StartDeclareTest, FinalizeDeclareTest)

//! GlossAspectKind <- GlossKindMeasurement / GlossKindFact / GlossKindView
static string AspectKindText(ParseResult &parse_result) {
	auto &kind = GetChoice(parse_result);
	if (IsRule(kind, "GlossKindMeasurement")) {
		return "MEASUREMENT";
	}
	if (IsRule(kind, "GlossKindFact")) {
		return "FACT";
	}
	return "VIEW";
}

//! GlossGrain <- GlossGrainDataset / GlossGrainTable / GlossGrainColumn / GlossGrainRelationship /
//!                 GlossGrainSource
static string GrainText(ParseResult &parse_result) {
	auto &grain = GetChoice(parse_result);
	for (auto name : {"Dataset", "Table", "Column", "Relationship", "Source"}) {
		if (IsRule(grain, (string("GlossGrain") + name).c_str())) {
			return StringUtil::Upper(name);
		}
	}
	throw InternalException("unknown grain rule '%s'", grain.Name());
}

//! GlossDeclareAspect <- 'DECLARE' 'ASPECT' Identifier 'WITH' StringLiteral 'AS' GlossAspectKind
//!                         GlossGrainClause?
//! GlossGrainClause <- 'ON' List(GlossGrain) GlossWhen? ; GlossWhen <- 'WHEN' GlossWhenBody
//! GlossWhenBody <- GlossWhenType / GlossWhenAspect ; GlossWhenAspect <- Identifier '=' StringLiteral
//! GlossWhenType <- 'TYPE' GlossWhenTypes ; GlossWhenTypes <- GlossWhenTypeOne / GlossWhenTypeIn
//! GlossWhenTypeOne <- '=' StringLiteral ; GlossWhenTypeIn <- 'IN' '(' List(StringLiteral) ')'
static arena_ptr<TransformResultValue> FinalizeDeclareAspect(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list = parse_result.Cast<ListParseResult>();
	auto name = Ident(transformer, list.GetChild(2));
	auto schema = Text(transformer, list.GetChild(4));
	auto kind = AspectKindText(list.GetChild(6));
	vector<Value> grains;
	// the WHEN as JSON: the sibling aspect and the literal its winning value carries, or the
	// columns' data types, upper-cased as DuckDB names them
	Value condition(LogicalType::VARCHAR);
	auto &grain_clause = list.Child<OptionalParseResult>(7);
	if (grain_clause.HasResult()) {
		auto &clause = grain_clause.GetResult().Cast<ListParseResult>();
		for (auto &grain : ListItems(clause.GetChild(1))) {
			grains.push_back(Value(GrainText(grain.get())));
		}
		auto &when = clause.Child<OptionalParseResult>(2);
		if (when.HasResult()) {
			auto &body = GetChoice(when.GetResult().Cast<ListParseResult>().GetChild(1));
			if (IsRule(body, "GlossWhenType")) {
				auto &types = GetChoice(body.Cast<ListParseResult>().GetChild(1));
				vector<string> names;
				if (IsRule(types, "GlossWhenTypeOne")) {
					names.push_back(
					    JsonString(StringUtil::Upper(Text(transformer, types.Cast<ListParseResult>().GetChild(1)))));
				} else {
					for (auto &item : ListItems(types.Cast<ListParseResult>().GetChild(2))) {
						names.push_back(JsonString(StringUtil::Upper(Text(transformer, item.get()))));
					}
				}
				condition = Value("{\"type\":[" + StringUtil::Join(names, ",") + "]}");
			} else {
				auto &clause_when = body.Cast<ListParseResult>();
				condition = Value("{\"aspect\":" + JsonString(Ident(transformer, clause_when.GetChild(0))) +
				                  ",\"value\":" + JsonString(Text(transformer, clause_when.GetChild(2))) + "}");
			}
		}
	}
	if (kind == "VIEW" && !grains.empty()) {
		// SPEC §5.1: a metric is grounded on the dataset; a grain clause is refused
		throw ParserException("a VIEW aspect takes no grain clause: its grain is the dataset");
	}
	return MakeWrite(
	    transformer, "aspects",
	    {Value(name), Value(schema), Value(kind), Value::LIST(LogicalType::VARCHAR, std::move(grains)), condition},
	    "DECLARE ASPECT " + name);
}
GLOSSQL_TRANSFORM(StartDeclareAspect, FinalizeDeclareAspect)

//! GlossDeclareRelationship <- 'DECLARE' 'RELATIONSHIP' GlossRelSide GlossRelOp GlossRelSide
static arena_ptr<TransformResultValue> FinalizeDeclareRelationship(PEGTransformer &transformer,
                                                                   ParseResult &parse_result) {
	auto &list = parse_result.Cast<ListParseResult>();
	auto left = RelSideText(transformer, list.GetChild(2));
	auto op = RelOpText(list.GetChild(3));
	auto right = RelSideText(transformer, list.GetChild(4));
	return MakeWrite(transformer, "relationships", {Value(left), Value(op), Value(right)},
	                 "DECLARE RELATIONSHIP " + left + " " + op + " " + right);
}
GLOSSQL_TRANSFORM(StartDeclareRelationship, FinalizeDeclareRelationship)

//===--------------------------------------------------------------------===//
// Reads: GLOSSARY(...) becomes glossql_glossary(subject, aspect, all) and
// TESTS(...) becomes glossql_tests(subject, aspect) — stock table-function
// references, whose bind plans the read over the store
//===--------------------------------------------------------------------===//

static arena_ptr<TransformResultValue> ReadRef(PEGTransformer &transformer, const char *function,
                                               vector<unique_ptr<ParsedExpression>> children,
                                               OptionalParseResult &alias_opt) {
	auto ref = make_uniq<TableFunctionRef>();
	ref->function = make_uniq<FunctionExpression>(Identifier(function), std::move(children));
	if (alias_opt.HasResult()) {
		auto alias = transformer.Transform<TableAlias>(alias_opt.GetResult());
		ref->alias = alias.name;
		ref->column_name_alias = alias.column_name_alias;
	}
	unique_ptr<TableRef> result = std::move(ref);
	return transformer.MakeResult<unique_ptr<TableRef>>(std::move(result));
}

//! GlossGlossaryRead <- 'GLOSSARY' Parens(GlossGlossaryArgs?) TableAlias?
//! GlossGlossaryArgs <- GlossSubjectArgs / GlossAllFlag
//! GlossSubjectArgs <- GlossSubject GlossAspectNarrow? GlossAllTail?
//! GlossAspectNarrow <- '::' Identifier ; GlossAllTail <- ',' GlossAllFlag
//! GlossAllFlag <- 'ALL' '=>' 'TRUE'
static arena_ptr<TransformResultValue> FinalizeGlossaryRead(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list = parse_result.Cast<ListParseResult>();
	unique_ptr<ParsedExpression> subject = ConstantExpression::Null();
	unique_ptr<ParsedExpression> aspect = ConstantExpression::Null();
	bool all = false;

	auto &args_opt = InParens(list.GetChild(1)).Cast<OptionalParseResult>();
	if (args_opt.HasResult()) {
		auto &args = GetChoice(args_opt.GetResult());
		if (IsRule(args, "GlossAllFlag")) {
			all = true;
		} else {
			auto &subject_args = args.Cast<ListParseResult>();
			subject = ConstantExpression::String(SubjectText(transformer, subject_args.GetChild(0)));
			auto &narrow = subject_args.Child<OptionalParseResult>(1);
			if (narrow.HasResult()) {
				aspect = ConstantExpression::String(
				    Ident(transformer, narrow.GetResult().Cast<ListParseResult>().GetChild(1)));
			}
			all = subject_args.Child<OptionalParseResult>(2).HasResult();
		}
	}

	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(std::move(subject));
	children.push_back(std::move(aspect));
	children.push_back(ConstantExpression::Boolean(all));
	return ReadRef(transformer, "glossql_glossary", std::move(children), list.Child<OptionalParseResult>(2));
}
GLOSSQL_TRANSFORM(StartGlossaryRead, FinalizeGlossaryRead)

//! GlossTestsRead <- 'TESTS' Parens(GlossTestsArgs?) TableAlias?
//! GlossTestsArgs <- GlossSubject GlossAspectNarrow?
static arena_ptr<TransformResultValue> FinalizeTestsRead(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list = parse_result.Cast<ListParseResult>();
	unique_ptr<ParsedExpression> subject = ConstantExpression::Null();
	unique_ptr<ParsedExpression> aspect = ConstantExpression::Null();
	auto &args_opt = InParens(list.GetChild(1)).Cast<OptionalParseResult>();
	if (args_opt.HasResult()) {
		auto &args = args_opt.GetResult().Cast<ListParseResult>();
		subject = ConstantExpression::String(SubjectText(transformer, args.GetChild(0)));
		auto &narrow = args.Child<OptionalParseResult>(1);
		if (narrow.HasResult()) {
			aspect =
			    ConstantExpression::String(Ident(transformer, narrow.GetResult().Cast<ListParseResult>().GetChild(1)));
		}
	}
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(std::move(subject));
	children.push_back(std::move(aspect));
	return ReadRef(transformer, "glossql_tests", std::move(children), list.Child<OptionalParseResult>(2));
}
GLOSSQL_TRANSFORM(StartTestsRead, FinalizeTestsRead)

//===--------------------------------------------------------------------===//
// The grammar extension
//===--------------------------------------------------------------------===//

GlossGrammar::GlossGrammar()
    : GrammarExtension("glossql", "glossql: the context language over DuckDB — DECLARE, GLOSS, GLOSSARY()") {
}

vector<GrammarChange> GlossGrammar::GetChanges() const {
	vector<GrammarChange> changes;
	auto rule = [&](const char *definition) {
		changes.push_back(GrammarChange::AddRule(definition));
	};
	auto statement = [&](const char *definition, grammar_transform_process_function_t transform) {
		changes.push_back(GrammarChange::AddRule(definition, std::move(transform)));
		string name(definition);
		changes.push_back(GrammarChange::PrependChoice("Statement", name.substr(0, name.find(' '))));
	};

	// New words are unreserved: still usable as column and table names
	for (auto keyword :
	     {"'GLOSS'", "'GLOSSARY'", "'ASPECT'", "'RELATIONSHIP'", "'FACT'", "'MEASUREMENT'", "'DATASET'", "'RECIPE'",
	      "'PREVIEW'", "'TEST'", "'DETECTOR'", "'THRESHOLD'", "'AGENT'", "'HUMAN'", "'TESTS'"}) {
		changes.push_back(GrammarChange::AddChoice("UnreservedKeyword", keyword));
	}

	// Subjects
	rule("GlossSubject <- GlossPairPath / GlossPath");
	rule("GlossPath <- Identifier GlossPathTail*");
	rule("GlossPathTail <- '.' Identifier");
	rule("GlossPairPath <- GlossRelSide GlossRelOp GlossRelSide");
	rule("GlossRelSide <- GlossRelPrefix+ GlossRelKey");
	rule("GlossRelPrefix <- Identifier '.'");
	rule("GlossRelKey <- GlossRelTuple / Identifier");
	rule("GlossRelTuple <- Parens(List(Identifier))");
	rule("GlossRelOp <- GlossOneToOne / GlossManyToOne");
	rule("GlossManyToOne <- '->'");
	rule("GlossOneToOne <- '<->'");

	// Settings
	rule("GlossSettings <- 'SET' Parens(List(GlossPair))");
	rule("GlossPair <- Identifier ':' GlossPairValue");
	rule("GlossPairValue <- StringLiteral / NumberLiteral / Identifier");

	// Statements, tried before every stock statement
	statement("GlossGlossStatement <- 'GLOSS' Identifier 'ON' GlossSubject 'AS' StringLiteral", StartGloss);
	statement("GlossDeclareSource <- 'DECLARE' 'SOURCE' Identifier GlossSettings", StartDeclareSource);
	statement("GlossDeclareDataset <- 'DECLARE' 'DATASET' Identifier GlossSettings", StartDeclareDataset);
	statement("GlossDeclareRecipe <- 'DECLARE' 'RECIPE' Identifier 'ON' Identifier 'FROM' Identifier "
	          "GlossSettings? 'AS' StringLiteral",
	          StartDeclareRecipe);
	statement("GlossDeclareAspect <- 'DECLARE' 'ASPECT' Identifier 'WITH' StringLiteral 'AS' GlossAspectKind "
	          "GlossGrainClause?",
	          StartDeclareAspect);
	rule("GlossAspectKind <- GlossKindMeasurement / GlossKindFact / GlossKindView");
	rule("GlossKindMeasurement <- 'MEASUREMENT'");
	rule("GlossKindFact <- 'FACT'");
	rule("GlossKindView <- 'VIEW'");
	rule("GlossGrainClause <- 'ON' List(GlossGrain) GlossWhen?");
	rule("GlossWhen <- 'WHEN' GlossWhenBody");
	rule("GlossWhenBody <- GlossWhenType / GlossWhenAspect");
	rule("GlossWhenType <- 'TYPE' GlossWhenTypes");
	rule("GlossWhenTypes <- GlossWhenTypeOne / GlossWhenTypeIn");
	rule("GlossWhenTypeOne <- '=' StringLiteral");
	rule("GlossWhenTypeIn <- 'IN' '(' List(StringLiteral) ')'");
	rule("GlossWhenAspect <- Identifier '=' StringLiteral");
	rule("GlossGrain <- GlossGrainDataset / GlossGrainTable / GlossGrainColumn / GlossGrainRelationship / "
	     "GlossGrainSource");
	rule("GlossGrainDataset <- 'DATASET'");
	rule("GlossGrainTable <- 'TABLE'");
	rule("GlossGrainColumn <- 'COLUMN'");
	rule("GlossGrainRelationship <- 'RELATIONSHIP'");
	rule("GlossGrainSource <- 'SOURCE'");
	statement("GlossDeclareRelationship <- 'DECLARE' 'RELATIONSHIP' GlossRelSide GlossRelOp GlossRelSide",
	          StartDeclareRelationship);
	statement("GlossPreview <- 'PREVIEW' Identifier 'AS' StringLiteral", StartPreview);
	statement("GlossDeclareFunction <- 'DECLARE' 'FUNCTION' Identifier 'FOR' GlossFunctionScope 'AS' "
	          "StringLiteral GlossReturns?",
	          StartDeclareFunction);
	rule("GlossFunctionScope <- GlossScopeGlobal / Identifier");
	rule("GlossScopeGlobal <- 'GLOBAL'");
	rule("GlossReturns <- 'RETURNS' Identifier");
	// extraction is the whole statement: SELECT f(), g() FROM subject and nothing after it
	statement("GlossExtract <- 'SELECT' List(GlossCall) 'FROM' GlossSubject GlossEnd", StartExtract);
	rule("GlossCall <- Identifier '(' ')'");
	rule("GlossEnd <- ';'");
	changes.push_back(GrammarChange::AddTerminalRuleOverride(
	    "GlossEnd", [](const PEGKeywordHelper &) { return make_uniq<GlossEndMatcher>(); }));
	// IMPORT [dataset.]table, tried before the stock statements like the rest: an
	// appended choice can be shadowed by another extension's appended choice that
	// matches the bare keyword (the shell's expression statement takes IMPORT as a
	// column name and leaves the table unconsumed). The end matcher keeps DuckDB's
	// IMPORT DATABASE its own: a name followed by anything but the end is not ours
	// (the rule parser ignores a ! lookahead)
	statement("GlossImport <- 'IMPORT' Identifier GlossPathTail? GlossEnd", StartImport);
	// DELETE FROM [lake.][main.]glossary [alias] [WHERE ...]: the lake reads the
	// store as views, and the engine deletes through a table only, so the removal
	// (SPEC §5.2) is a statement here, run on the store's table. The name's words
	// are custom terminals, since a keyword literal sees a quoted identifier with
	// its quotes; any other DELETE stays DuckDB's own
	statement("GlossDeleteGlossary <- 'DELETE' 'FROM' GlossGlossaryTarget TargetAlias? WhereClause? GlossEnd",
	          StartDeleteGlossary);
	rule("GlossGlossaryTarget <- GlossGlossaryInLake / GlossGlossaryInSchema / GlossGlossaryWord");
	rule("GlossGlossaryInLake <- Identifier '.' GlossSchemaWord '.' GlossGlossaryWord");
	rule("GlossGlossaryInSchema <- GlossSchemaWord '.' GlossGlossaryWord");
	rule("GlossSchemaWord <- Identifier");
	rule("GlossGlossaryWord <- Identifier");
	changes.push_back(GrammarChange::AddTerminalRuleOverride(
	    "GlossSchemaWord", [](const PEGKeywordHelper &) { return make_uniq<GlossWordMatcher>("main"); }));
	changes.push_back(GrammarChange::AddTerminalRuleOverride(
	    "GlossGlossaryWord", [](const PEGKeywordHelper &) { return make_uniq<GlossWordMatcher>("glossary"); }));

	// Reads, tried before every stock table reference
	changes.push_back(GrammarChange::AddRule("GlossGlossaryRead <- 'GLOSSARY' Parens(GlossGlossaryArgs?) TableAlias?",
	                                         StartGlossaryRead));
	rule("GlossGlossaryArgs <- GlossSubjectArgs / GlossAllFlag");
	rule("GlossSubjectArgs <- GlossSubject GlossAspectNarrow? GlossAllTail?");
	rule("GlossAspectNarrow <- '::' Identifier");
	rule("GlossAllTail <- ',' GlossAllFlag");
	rule("GlossAllFlag <- 'ALL' '=>' 'TRUE'");
	changes.push_back(GrammarChange::PrependChoice("InnerTableRef", "GlossGlossaryRead"));
	changes.push_back(
	    GrammarChange::AddRule("GlossTestsRead <- 'TESTS' Parens(GlossTestsArgs?) TableAlias?", StartTestsRead));
	rule("GlossTestsArgs <- GlossSubject GlossAspectNarrow?");
	changes.push_back(GrammarChange::PrependChoice("InnerTableRef", "GlossTestsRead"));

	// Tests
	statement("GlossDeclareTest <- 'DECLARE' 'TEST' Identifier 'ON' Identifier GlossBy? GlossDetector? "
	          "GlossThreshold?",
	          StartDeclareTest);
	rule("GlossBy <- 'BY' Parens(List(GlossSpeaker))");
	rule("GlossSpeaker <- GlossSpeakerAgent / GlossSpeakerHuman");
	rule("GlossSpeakerAgent <- 'AGENT'");
	rule("GlossSpeakerHuman <- 'HUMAN'");
	rule("GlossDetector <- 'DETECTOR' Identifier");
	rule("GlossThreshold <- 'THRESHOLD' NumberLiteral");

	return changes;
}

} // namespace duckdb
