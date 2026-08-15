//! AST snapshots — one representative statement per glossql grammar form.
//! Substrate statements are DataFusion's AST and are covered by behavior
//! tests instead of snapshots.

use glossql_parser::GlossqlParser;

macro_rules! snap {
    ($name:ident, $src:expr) => {
        #[test]
        fn $name() {
            insta::assert_debug_snapshot!(GlossqlParser::parse_sql($src).expect("must parse"));
        }
    };
}

snap!(
    source_decl,
    "DECLARE SOURCE crm SET (type: relational_db, location: 'postgres://crm.internal/prod', via: crm_prod);"
);
snap!(
    recipe_decl,
    "DECLARE RECIPE segments ON fin FROM crm AS $$SELECT id, segment FROM customer_segments$$;"
);
snap!(
    dataset_decl_and_use,
    "DECLARE DATASET fin SET (purpose: 'working-capital analysis');\nUSE fin;"
);
snap!(
    relationship_decls,
    "DECLARE RELATIONSHIP orders.customer_id -> customers.id;\nDECLARE RELATIONSHIP invoices.order_id <-> orders.id;"
);
snap!(
    relationship_decl_composite,
    "DECLARE RELATIONSHIP master_txn.(business_id, account) -> coa.(business_id, account_name);"
);
snap!(
    gloss_on_composite_pair_path,
    r#"GLOSS meaning ON txn.(business_id, party) -> parties.(business_id, name) AS $${"value": "scoped reference"}$$;"#
);
snap!(
    aspect_decl_fact,
    r#"DECLARE ASPECT unit WITH $${"type": "object", "properties": {"value": {"type": "string"}}}$$ AS FACT;"#
);
snap!(
    aspect_decl_measurement,
    r#"DECLARE ASPECT min_max WITH $${"type": "object", "properties": {"min": {}, "max": {}}}$$ AS MEASUREMENT;"#
);
snap!(
    aspect_decl_with_grain,
    r#"DECLARE ASPECT meaning WITH $${"type": "object"}$$ AS FACT ON TABLE, COLUMN, RELATIONSHIP;"#
);
snap!(
    aspect_decl_source_grain,
    r#"DECLARE ASPECT conventions WITH $${"type": "object"}$$ AS FACT ON SOURCE;"#
);
snap!(
    aspect_decl_conditional_relevance,
    r#"DECLARE ASPECT behavior WITH $${"type": "object"}$$ AS FACT ON COLUMN WHEN role = 'measure';"#
);
snap!(
    gloss_fact,
    r#"GLOSS unit ON orders.amount AS $${"value": "EUR", "source_column": "currency_code"}$$;"#
);
snap!(
    gloss_on_pair_path,
    r#"GLOSS fk_note ON orders.customer_id -> customers.id AS $${"value": "2% orphaned rows"}$$;"#
);
snap!(
    gloss_body_with_escapes,
    r#"GLOSS type_patterns ON fin AS $${"expr": "STRPTIME(\"{col}\", '%d.%m.%Y')"}$$;"#
);
snap!(
    function_decl_accepts_aspects,
    "DECLARE FUNCTION outliers FOR GLOBAL AS $$let p = context.column_profile; #{iqr: fences(p)}$$ ACCEPTS (column_profile) RETURNS outlier_profile;"
);
snap!(
    function_decl_detector,
    "DECLARE FUNCTION slot_entropy FOR fin AS $$#{score: spread(context.slots)}$$;"
);
snap!(
    witness_decl_full,
    "DECLARE WITNESS behavior_w ON behavior BY (AGENT, HUMAN) DETECTOR behavior_entropy THRESHOLD 0.7;"
);
snap!(
    witness_decl_detector_only,
    "DECLARE WITNESS reconciliation_w ON reconciliation DETECTOR reconcile_bands THRESHOLD 0.5;"
);
snap!(
    extract_two_calls,
    "SELECT outliers(), profile() FROM fin.orders;"
);
