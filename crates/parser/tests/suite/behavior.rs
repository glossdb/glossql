//! Classification and rejection behavior the grammar fixes: what routes to
//! substrate (DataFusion's parser), what must fail, and the respelled
//! forms' error messages.

use glossql_parser::{Declaration, GlossqlParser, Statement, Subject};

fn single(src: &str) -> Statement {
    let mut statements = GlossqlParser::parse_sql(src).expect("must parse");
    assert_eq!(statements.len(), 1, "expected exactly one statement");
    statements.remove(0)
}

fn error(src: &str) -> String {
    GlossqlParser::parse_sql(src)
        .expect_err("must fail")
        .to_string()
}

// -- classification ------------------------------------------------------

#[test]
fn extract_is_recognized() {
    assert!(matches!(
        single("SELECT detect_relationships() FROM fin;"),
        Statement::Extract(_)
    ));
}

#[test]
fn select_with_projection_is_substrate() {
    assert!(matches!(
        single("SELECT subject, band FROM ATTEST(fin.trial_balance) WHERE band = 'red';"),
        Statement::Substrate(_)
    ));
}

#[test]
fn glossary_read_with_named_arg_is_substrate() {
    assert!(matches!(
        single("SELECT * FROM GLOSSARY(fin.orders.amount, all => true);"),
        Statement::Substrate(_)
    ));
}

#[test]
fn attest_on_one_to_one_pair_path_parses_as_substrate() {
    // Needs the postgres dialect: `<->` is a single TwoWayArrow token there.
    assert!(matches!(
        single("SELECT * FROM ATTEST(invoices.order_id <-> orders.id::fk_note);"),
        Statement::Substrate(_)
    ));
}

#[test]
fn mixed_call_and_column_select_is_substrate() {
    assert!(matches!(
        single("SELECT f(), col FROM t;"),
        Statement::Substrate(_)
    ));
}

#[test]
fn create_view_is_substrate() {
    assert!(matches!(
        single("CREATE VIEW v AS SELECT a FROM t;"),
        Statement::Substrate(_)
    ));
}

#[test]
fn delete_from_glossary_is_substrate() {
    assert!(matches!(
        single("DELETE FROM glossary WHERE subject = 'orders.amount' AND aspect = 'unit';"),
        Statement::Substrate(_)
    ));
}

// -- respelled forms -----------------------------------------------------

#[test]
fn bare_brace_body_is_rejected_with_guidance() {
    let e = error(r#"GLOSS unit ON orders.amount AS {"value": "EUR"};"#);
    assert!(e.contains("dollar-quoted"), "{e}");
}

#[test]
fn single_quoted_body_is_rejected() {
    let e = error(r#"GLOSS unit ON orders.amount AS '{"value": "EUR"}';"#);
    assert!(e.contains("dollar-quoted"), "{e}");
}

#[test]
fn invalid_json_in_dollar_body_is_an_error() {
    let e = error(r#"GLOSS unit ON orders.amount AS $${"value": }$$;"#);
    assert!(e.contains("invalid JSON body"), "{e}");
}

#[test]
fn non_object_json_body_is_an_error() {
    let e = error("GLOSS unit ON orders.amount AS $$[1, 2]$$;");
    assert!(e.contains("must be an object"), "{e}");
}

#[test]
fn tagged_dollar_body_parses() {
    assert!(matches!(
        single(r#"GLOSS note ON fin AS $json${"text": "a body containing $$"}$json$;"#),
        Statement::Gloss(_)
    ));
}

#[test]
fn recipe_tail_must_be_dollar_quoted() {
    let e = error("DECLARE RECIPE segments ON fin FROM crm AS SELECT id FROM t;");
    assert!(e.contains("dollar-quoted recipe SQL"), "{e}");
}

#[test]
fn calls_with_arguments_are_not_extractions() {
    // Settings are context, never call arguments: an argument-carrying call
    // is not an extraction — it falls through to substrate SQL, where
    // planning rejects it loudly.
    assert!(matches!(
        single("SELECT dso(days_in_period => 90) FROM fin;"),
        Statement::Substrate(_)
    ));
}

// -- rejections the grammar fixes ----------------------------------------

#[test]
fn declare_pattern_is_not_a_head() {
    // Fixture 13's rejected fork: patterns are FACT glosses, not a
    // declaration head.
    let e = error("DECLARE PATTERN '^a$' FOR TYPE;");
    assert!(e.contains("after DECLARE"), "{e}");
}

#[test]
fn four_segment_paths_are_rejected() {
    let e = error(r#"GLOSS a ON w.x.y.z AS $${"v": 1}$$;"#);
    assert!(e.contains("three segments"), "{e}");
}

#[test]
fn relationship_endpoint_needs_table_and_column() {
    let e = error("DECLARE RELATIONSHIP a -> b.c;");
    assert!(e.contains("table.column"), "{e}");
}

#[test]
fn aspect_kind_must_be_known() {
    let e = error(r#"DECLARE ASPECT a WITH $${"type": "object"}$$ AS OPINION;"#);
    assert!(e.contains("MEASUREMENT"), "{e}");
}

#[test]
fn trailing_input_after_a_statement_is_an_error() {
    let e = error("USE fin extra;");
    assert!(e.contains("end of statement"), "{e}");
}

// -- statement stream ----------------------------------------------------

#[test]
fn empty_statements_are_dropped() {
    assert_eq!(GlossqlParser::parse_sql("USE fin;;").unwrap().len(), 1);
}

#[test]
fn semicolons_inside_bodies_do_not_split() {
    assert_eq!(
        GlossqlParser::parse_sql(r#"GLOSS m ON t AS $${"value": "a; b"}$$;"#)
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn multi_statement_scripts_parse_in_order() {
    let statements = GlossqlParser::parse_sql(concat!(
        "USE fin;\n",
        r#"DECLARE ASPECT unit WITH $${"type": "object"}$$ AS FACT;"#,
        "\n",
        r#"GLOSS unit ON orders.amount AS $${"value": "EUR"}$$;"#,
        "\nSELECT profile() FROM fin.orders;\n",
        "SELECT * FROM GLOSSARY(fin.orders.amount);"
    ))
    .unwrap();
    assert_eq!(statements.len(), 5);
    assert!(matches!(statements[0], Statement::Use(_)));
    assert!(matches!(statements[1], Statement::Declare(_)));
    assert!(matches!(statements[2], Statement::Gloss(_)));
    assert!(matches!(statements[3], Statement::Extract(_)));
    assert!(matches!(statements[4], Statement::Substrate(_)));
}

// -- names ---------------------------------------------------------------

/// An unquoted name folds to lowercase, a double-quoted one keeps its
/// case — the planner's own rule, so what a declaration lands is what an
/// unquoted read reaches (SPEC.md §1).
#[test]
fn unquoted_names_fold_and_quoted_names_keep_case() {
    let Statement::Declare(decl) =
        single("DECLARE RECIPE AdsInfo ON Avito FROM Export AS $$select 1$$;")
    else {
        panic!("a declaration");
    };
    let Declaration::Recipe(recipe) = *decl else {
        panic!("a recipe");
    };
    assert_eq!(recipe.table.value, "adsinfo");
    assert_eq!(recipe.dataset.value, "avito");
    assert_eq!(recipe.source.value, "export");

    let Statement::Declare(decl) =
        single(r#"DECLARE RECIPE "AdsInfo" ON avito FROM export AS $$select 1$$;"#)
    else {
        panic!("a declaration");
    };
    let Declaration::Recipe(recipe) = *decl else {
        panic!("a recipe");
    };
    assert_eq!(recipe.table.value, "AdsInfo");

    let Statement::Gloss(gloss) =
        single(r#"GLOSS Unit ON Orders."Amount" AS $${"value": "EUR"}$$;"#)
    else {
        panic!("a gloss");
    };
    assert_eq!(gloss.aspect.value, "unit");
    let Subject::Path(path) = gloss.subject else {
        panic!("a path subject");
    };
    let segments: Vec<&str> = path.segments.iter().map(|s| s.value.as_str()).collect();
    assert_eq!(segments, ["orders", "Amount"]);
}

/// A body written `…};` — closed by the semicolon, never by `$$` — is
/// refused with the road out in both shapes it takes: alone in the
/// call, the tokenizer's unterminated region; followed by another such
/// body, the region running to the next `$$` and the body check seeing
/// the swallowed statement as text after the object.
#[test]
fn a_body_closed_by_the_semicolon_names_the_missing_dollar_quote() {
    let e = error(r#"GLOSS entity ON encounters AS $${"value": "visit"};"#);
    assert!(e.contains("Unterminated dollar-quoted"), "{e}");
    assert!(e.contains("closes with $$ before the semicolon"), "{e}");
    let e = error(
        "GLOSS entity ON encounters AS $${\"value\": \"visit\"};\n\
         GLOSS entity ON patients AS $${\"value\": \"person\"};",
    );
    assert!(e.contains("invalid JSON body"), "{e}");
    assert!(e.contains("text after the object"), "{e}");
    assert!(e.contains("closes with $$ before the semicolon"), "{e}");
    // A body that closes, followed by one that does not, refuses at
    // the second with the tokenizer's text.
    let e = error(
        "GLOSS entity ON encounters AS $${\"value\": \"visit\"}$$;\n\
         GLOSS entity ON patients AS $${\"value\": \"person\"};",
    );
    assert!(e.contains("Unterminated dollar-quoted"), "{e}");
}

#[test]
fn a_body_closed_by_mirroring_the_opener_names_the_close() {
    let e = error(r#"GLOSS entity ON encounters AS $${"value": "visit"}$${;"#);
    assert!(e.contains("found: {"), "{e}");
    assert!(e.contains("mirrors the opener"), "{e}");
    // Any other stray token at the statement's end passes as it came.
    let e = error(r#"GLOSS entity ON encounters AS $${"value": "visit"}$$ x;"#);
    assert!(e.contains("found: x") && !e.contains("mirrors"), "{e}");
}
