//! Store behavior the language fixes: admission by aspect kind (SPEC.md
//! §5.2), the witness speaker gate and detector eligibility (§7.1),
//! supersession per (subject, aspect, actor kind), the provisional collapse
//! policy (§5.3), and cache semantics (§6).

use glossql_glossary::{Actor, ActorKind, Error, ReadContext, Scope, Store};
use glossql_parser::{Declaration, Gloss, GlossqlParser, Statement};

fn decl(sql: &str) -> Declaration {
    match GlossqlParser::parse_sql(sql)
        .expect("declaration parses")
        .remove(0)
    {
        Statement::Declare(d) => *d,
        other => panic!("not a declaration: {other:?}"),
    }
}

fn gloss(sql: &str) -> Gloss {
    match GlossqlParser::parse_sql(sql)
        .expect("gloss parses")
        .remove(0)
    {
        Statement::Gloss(g) => g,
        other => panic!("not a gloss: {other:?}"),
    }
}

async fn store() -> Store {
    let store = Store::open_memory().await.unwrap();
    let Declaration::Aspect(unit) = decl(
        r#"DECLARE ASPECT unit WITH $${
            "type": "object",
            "required": ["value"],
            "properties": {"value": {"type": "string"}},
            "additionalProperties": false
        }$$ AS FACT;"#,
    ) else {
        unreachable!()
    };
    store.declare_aspect(&unit).await.unwrap();
    store
}

fn agent() -> Actor {
    Actor {
        kind: ActorKind::Agent,
        id: "agent-1".into(),
    }
}

fn human() -> Actor {
    Actor {
        kind: ActorKind::Human,
        id: "philipp".into(),
    }
}

async fn write(store: &Store, actor: &Actor, statement: &str) -> Result<(), Error> {
    let g = gloss(statement);
    store
        .gloss(
            "fin",
            actor,
            &g.aspect.value,
            "orders.amount",
            &g.body,
            None,
        )
        .await
}

// -- admission by aspect kind --------------------------------------------

#[tokio::test]
async fn unknown_aspect_is_rejected() {
    let s = store().await;
    let e = write(
        &s,
        &agent(),
        r#"GLOSS nope ON orders.amount AS $${"value": "x"}$$;"#,
    )
    .await
    .unwrap_err();
    assert!(matches!(e, Error::Unknown { what: "aspect", .. }), "{e}");
}

#[tokio::test]
async fn fact_body_must_match_the_with_schema() {
    let s = store().await;
    let e = write(
        &s,
        &agent(),
        r#"GLOSS unit ON orders.amount AS $${"wrong": 1}$$;"#,
    )
    .await
    .unwrap_err();
    assert!(matches!(e, Error::BodyRejected { .. }), "{e}");
    write(
        &s,
        &agent(),
        r#"GLOSS unit ON orders.amount AS $${"value": "EUR"}$$;"#,
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn query_gloss_validates_against_the_grounding_schema() {
    let s = store().await;
    let Declaration::Aspect(revenue) = decl(
        r#"DECLARE ASPECT revenue WITH $${"title": "revenue", "x-kind": "measure"}$$ AS QUERY;"#,
    ) else {
        unreachable!()
    };
    s.declare_aspect(&revenue).await.unwrap();
    let e = write(
        &s,
        &agent(),
        r#"GLOSS revenue ON orders.amount AS $${"prose": "no sql"}$$;"#,
    )
    .await
    .unwrap_err();
    assert!(matches!(e, Error::BodyRejected { .. }), "{e}");
    write(
        &s,
        &agent(),
        r#"GLOSS revenue ON orders.amount AS $${"sql": "SELECT amount FROM orders"}$$;"#,
    )
    .await
    .unwrap();
    // The authored stock marker (ruled 2026-08-11 with the band walk;
    // the schema learned it after the monitoring evaluation caught the
    // rejection): "stock"/"flow" admitted, anything else refused.
    write(
        &s,
        &agent(),
        r#"GLOSS revenue ON orders.amount AS $${"sql": "SELECT amount FROM orders", "behavior": "stock"}$$;"#,
    )
    .await
    .unwrap();
    let e = write(
        &s,
        &agent(),
        r#"GLOSS revenue ON orders.amount AS $${"sql": "SELECT amount FROM orders", "behavior": "level"}$$;"#,
    )
    .await
    .unwrap_err();
    assert!(matches!(e, Error::BodyRejected { .. }), "{e}");
}

#[tokio::test]
async fn measurement_aspects_are_never_glossed() {
    let s = store().await;
    let Declaration::Aspect(m) =
        decl(r#"DECLARE ASPECT min_max WITH $${"type": "object"}$$ AS MEASUREMENT;"#)
    else {
        unreachable!()
    };
    s.declare_aspect(&m).await.unwrap();
    let e = write(
        &s,
        &agent(),
        r#"GLOSS min_max ON orders.amount AS $${"min": 1}$$;"#,
    )
    .await
    .unwrap_err();
    assert!(matches!(e, Error::MeasurementGloss(_)), "{e}");
}

// -- the witness speaker gate --------------------------------------------

#[tokio::test]
async fn witness_by_list_gates_actor_kinds() {
    let s = store().await;
    let Declaration::Witness(w) = decl("DECLARE WITNESS unit_w ON unit BY (HUMAN);") else {
        unreachable!()
    };
    s.declare_witness(&w).await.unwrap();
    let e = write(
        &s,
        &agent(),
        r#"GLOSS unit ON orders.amount AS $${"value": "EUR"}$$;"#,
    )
    .await
    .unwrap_err();
    assert!(matches!(e, Error::SpeakerNotAdmitted { .. }), "{e}");
    write(
        &s,
        &human(),
        r#"GLOSS unit ON orders.amount AS $${"value": "EUR"}$$;"#,
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn measurement_aspects_take_one_producer_and_no_speaker_gate() {
    let s = store().await;
    let Declaration::Aspect(m) =
        decl(r#"DECLARE ASPECT min_max WITH $${"type": "object"}$$ AS MEASUREMENT;"#)
    else {
        unreachable!()
    };
    s.declare_aspect(&m).await.unwrap();
    let Declaration::Function(f) =
        decl("DECLARE FUNCTION profile_min_max FOR fin FROM 'p.rhai' RETURNS min_max;")
    else {
        unreachable!()
    };
    s.declare_function(&f).await.unwrap();

    // One producer per MEASUREMENT aspect — a second function is refused.
    let Declaration::Function(rival) =
        decl("DECLARE FUNCTION other_min_max FOR fin FROM 'o.rhai' RETURNS min_max;")
    else {
        unreachable!()
    };
    let e = s.declare_function(&rival).await.unwrap_err();
    assert!(matches!(e, Error::MeasurementProducerTaken { .. }), "{e}");

    // Nobody glosses a measurement, so BY is refused on its witness.
    let Declaration::Witness(bad) = decl("DECLARE WITNESS m_w ON min_max BY (AGENT);") else {
        unreachable!()
    };
    let e = s.declare_witness(&bad).await.unwrap_err();
    assert!(matches!(e, Error::MeasurementWitnessSpeakers(_)), "{e}");
}

#[tokio::test]
async fn a_detector_is_a_function_without_returns() {
    let s = store().await;
    let Declaration::Function(f) =
        decl("DECLARE FUNCTION vibes FOR fin FROM 'v.rhai' RETURNS unit;")
    else {
        unreachable!()
    };
    s.declare_function(&f).await.unwrap();
    // A function that RETURNS an aspect is a voice, never a detector.
    let Declaration::Witness(w) =
        decl("DECLARE WITNESS unit_w ON unit BY (AGENT, HUMAN) DETECTOR vibes;")
    else {
        unreachable!()
    };
    let e = s.declare_witness(&w).await.unwrap_err();
    assert!(matches!(e, Error::DetectorNotEligible { .. }), "{e}");

    // A witness naming neither BY nor DETECTOR declares nothing.
    let Declaration::Witness(empty) = decl("DECLARE WITNESS unit_w ON unit;") else {
        unreachable!()
    };
    let e = s.declare_witness(&empty).await.unwrap_err();
    assert!(matches!(e, Error::WitnessNamesNothing(_)), "{e}");
}

#[tokio::test]
async fn threshold_outside_unit_interval_is_rejected() {
    let s = store().await;
    let Declaration::Witness(w) =
        decl("DECLARE WITNESS unit_w ON unit BY (AGENT, HUMAN) THRESHOLD 1.7;")
    else {
        unreachable!()
    };
    assert!(s.declare_witness(&w).await.is_err());
}

#[tokio::test]
async fn redeclaring_an_aspect_is_content_idempotent_but_refused_once_glossed() {
    let s = store().await;
    // Same content, different whitespace: a no-op, not a replace.
    let Declaration::Aspect(same) = decl(
        r#"DECLARE ASPECT unit WITH $${"type":"object","required":["value"],"properties":{"value":{"type":"string"}},"additionalProperties":false}$$ AS FACT;"#,
    ) else {
        unreachable!()
    };
    s.declare_aspect(&same).await.unwrap();

    // Changing it is fine while nothing is glossed under it…
    let Declaration::Aspect(changed) =
        decl(r#"DECLARE ASPECT unit WITH $${"type": "object"}$$ AS FACT;"#)
    else {
        unreachable!()
    };
    s.declare_aspect(&changed).await.unwrap();

    // …and refused once something is.
    write(
        &s,
        &agent(),
        r#"GLOSS unit ON orders.amount AS $${"value": "EUR"}$$;"#,
    )
    .await
    .unwrap();
    let Declaration::Aspect(again) =
        decl(r#"DECLARE ASPECT unit WITH $${"type": "object", "properties": {}}$$ AS FACT;"#)
    else {
        unreachable!()
    };
    let e = s.declare_aspect(&again).await.unwrap_err();
    assert!(matches!(e, Error::AspectInUse { .. }), "{e}");
}

// -- supersession and collapse -------------------------------------------

#[tokio::test]
async fn supersession_is_per_subject_aspect_actor_kind() {
    let s = store().await;
    write(
        &s,
        &agent(),
        r#"GLOSS unit ON orders.amount AS $${"value": "USD"}$$;"#,
    )
    .await
    .unwrap();
    write(
        &s,
        &agent(),
        r#"GLOSS unit ON orders.amount AS $${"value": "EUR"}$$;"#,
    )
    .await
    .unwrap();
    let rows = s
        .raw_read("fin", &Scope::Subject("orders.amount".into()), None)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "agent slot holds one current value");
    assert!(
        rows[0].body.contains("EUR"),
        "latest wins: {}",
        rows[0].body
    );

    write(
        &s,
        &human(),
        r#"GLOSS unit ON orders.amount AS $${"value": "CHF"}$$;"#,
    )
    .await
    .unwrap();
    let rows = s
        .raw_read("fin", &Scope::Subject("orders.amount".into()), None)
        .await
        .unwrap();
    assert_eq!(rows.len(), 2, "human slot is separate");
}

#[tokio::test]
async fn collapse_serves_by_precedence_human_over_agent() {
    let s = store().await;
    let ctx = ReadContext::default();
    write(
        &s,
        &agent(),
        r#"GLOSS unit ON orders.amount AS $${"value": "EUR"}$$;"#,
    )
    .await
    .unwrap();
    let rows = s
        .collapsed_read("fin", &Scope::Subject("orders.amount".into()), None, &ctx)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(rows[0].value.as_deref().unwrap().contains("EUR"));
    assert_eq!(rows[0].state, "current");

    write(
        &s,
        &human(),
        r#"GLOSS unit ON orders.amount AS $${"value": "USD"}$$;"#,
    )
    .await
    .unwrap();
    let rows = s
        .collapsed_read("fin", &Scope::Subject("orders.amount".into()), None, &ctx)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(
        rows[0].value.as_deref().unwrap().contains("USD"),
        "the human slot outranks the agent slot (ruled 2026-08-04)"
    );
}

// -- functions and the cache ---------------------------------------------

#[tokio::test]
async fn accepts_names_must_be_declared_aspects() {
    let s = store().await;
    let Declaration::Function(good) =
        decl(r#"DECLARE FUNCTION f FOR fin FROM 'f.rhai' ACCEPTS (unit);"#)
    else {
        unreachable!()
    };
    s.declare_function(&good).await.unwrap();
    let row = s.function("f", Some("fin")).await.unwrap().unwrap();
    assert_eq!(row.accepts, vec!["unit"]);

    let Declaration::Function(bad) =
        decl(r#"DECLARE FUNCTION g FOR fin FROM 'g.rhai' ACCEPTS (nope);"#)
    else {
        unreachable!()
    };
    let e = s.declare_function(&bad).await.unwrap_err();
    assert!(matches!(e, Error::Unknown { what: "aspect", .. }), "{e}");
}

#[tokio::test]
async fn function_scope_gates_visibility() {
    let s = store().await;
    let Declaration::Function(f) = decl(r#"DECLARE FUNCTION profile FOR fin FROM 'p.rhai';"#)
    else {
        unreachable!()
    };
    s.declare_function(&f).await.unwrap();
    assert!(s.function("profile", Some("fin")).await.unwrap().is_some());
    assert!(s.function("profile", Some("crm")).await.unwrap().is_none());
}

#[tokio::test]
async fn cache_serves_the_latest_row_per_subject_and_function() {
    let s = store().await;
    s.cache_put("fin", "orders", "profile", None, r#"{"n": 1}"#, None)
        .await
        .unwrap();
    s.cache_put("fin", "orders", "profile", None, r#"{"n": 2}"#, None)
        .await
        .unwrap();
    let row = s
        .cache_get("fin", "orders", "profile", None)
        .await
        .unwrap()
        .unwrap();
    assert!(row.body.contains('2'));
}

#[tokio::test]
async fn forwarded_deletes_only_touch_the_two_relations() {
    let s = store().await;
    write(
        &s,
        &agent(),
        r#"GLOSS unit ON orders.amount AS $${"value": "EUR"}$$;"#,
    )
    .await
    .unwrap();
    let n = s
        .forward_delete(
            "glossary",
            "DELETE FROM glossary WHERE subject = 'orders.amount' AND aspect = 'unit'",
        )
        .await
        .unwrap();
    assert_eq!(n, 1);
    let e = s
        .forward_delete("aspects", "DELETE FROM aspects")
        .await
        .unwrap_err();
    assert!(matches!(e, Error::ForwardRejected(_)), "{e}");
}

#[tokio::test]
async fn a_glossary_delete_invalidates_detector_verdicts() {
    let s = store().await;
    // One detector (no RETURNS), one extraction function (RETURNS) —
    // only the detector's cache is a verdict about slots.
    let Declaration::Function(det) = decl("DECLARE FUNCTION entropy FOR fin FROM 'e.rhai';") else {
        unreachable!()
    };
    s.declare_function(&det).await.unwrap();
    let Declaration::Function(prod) =
        decl("DECLARE FUNCTION vibes FOR fin FROM 'v.rhai' RETURNS unit;")
    else {
        unreachable!()
    };
    s.declare_function(&prod).await.unwrap();

    write(
        &s,
        &agent(),
        r#"GLOSS unit ON orders.amount AS $${"value": "EUR"}$$;"#,
    )
    .await
    .unwrap();
    s.cache_put(
        "fin",
        "orders.amount",
        "entropy",
        Some("unit_w"),
        r#"{"band": "red"}"#,
        None,
    )
    .await
    .unwrap();
    s.cache_put("fin", "orders.amount", "vibes", None, r#"{"n": 1}"#, None)
        .await
        .unwrap();

    // Striking the slot invalidates the verdict about it: deletion makes
    // the slot set smaller, never newer, so timestamp freshness alone
    // would keep serving a verdict about slots that no longer exist.
    s.forward_delete(
        "glossary",
        "DELETE FROM glossary WHERE subject = 'orders.amount' AND actor_kind = 'agent'",
    )
    .await
    .unwrap();
    assert!(
        s.cache_get("fin", "orders.amount", "entropy", Some("unit_w"))
            .await
            .unwrap()
            .is_none(),
        "the detector verdict is gone — it recomputes at the next read"
    );
    assert!(
        s.cache_get("fin", "orders.amount", "vibes", None)
            .await
            .unwrap()
            .is_some(),
        "extraction caches answer to ACCEPTS invalidation, not slot deletion"
    );
}

#[tokio::test]
async fn declaration_relations_are_invalidation_edges() {
    let s = store().await;
    // ACCEPTS admits the wired declaration relations (ruled 2026-08-05):
    // no context arrives, but a write to the relation kills the cache
    // dataset-wide — an edge can change any subject's evidence.
    let Declaration::Function(dep) = decl(
        "DECLARE FUNCTION evidence FOR fin FROM 'ev.rhai' \
         ACCEPTS (relationships, imports) RETURNS unit;",
    ) else {
        unreachable!()
    };
    s.declare_function(&dep).await.unwrap();
    let Declaration::Function(other) = decl("DECLARE FUNCTION vibes FOR fin FROM 'v.rhai';") else {
        unreachable!()
    };
    s.declare_function(&other).await.unwrap();

    s.cache_put(
        "fin",
        "orders.amount",
        "evidence",
        None,
        r#"{"applicable": false}"#,
        None,
    )
    .await
    .unwrap();
    s.cache_put("fin", "orders.amount", "vibes", None, r#"{"n": 1}"#, None)
        .await
        .unwrap();

    s.declare_relationship("fin", "orders.customer_id", "->", "customers.id")
        .await
        .unwrap();
    assert!(
        s.cache_get("fin", "orders.amount", "evidence", None)
            .await
            .unwrap()
            .is_none(),
        "a declared edge kills the accepting function's cache"
    );

    s.cache_put(
        "fin",
        "orders.amount",
        "evidence",
        None,
        r#"{"applicable": false}"#,
        None,
    )
    .await
    .unwrap();
    s.import_put("fin", "payments", 10, 10, "{}").await.unwrap();
    assert!(
        s.cache_get("fin", "orders.amount", "evidence", None)
            .await
            .unwrap()
            .is_none(),
        "a landed import kills it the same way"
    );

    assert!(
        s.cache_get("fin", "orders.amount", "vibes", None)
            .await
            .unwrap()
            .is_some(),
        "a function without the edge keeps its cache"
    );

    // An unwired relation name is refused as an unknown aspect — a
    // silent no-op edge would be worse than an error.
    let Declaration::Function(bad) =
        decl("DECLARE FUNCTION w FOR fin FROM 'w.rhai' ACCEPTS (witnesses);")
    else {
        unreachable!()
    };
    let e = s.declare_function(&bad).await.unwrap_err();
    assert!(matches!(e, Error::Unknown { what: "aspect", .. }), "{e}");
}

// -- recipe admission (SPEC.md §3) ----------------------------------------

#[tokio::test]
async fn recipe_redeclare_is_content_idempotent_and_change_is_refused() {
    use glossql_glossary::RecipeAdmission;

    let s = store().await;
    for setup in [
        "DECLARE DATASET fin SET (purpose: 'test');",
        "DECLARE SOURCE erp SET (type: parquet, location: 'lake/erp');",
    ] {
        match decl(setup) {
            Declaration::Dataset(d) => s.declare_dataset(&d).await.unwrap(),
            Declaration::Source(d) => s.declare_source(&d).await.unwrap(),
            other => panic!("unexpected: {other:?}"),
        }
    }
    let recipe = |sql: &str| match decl(sql) {
        Declaration::Recipe(r) => r,
        other => panic!("not a recipe: {other:?}"),
    };
    // The session's two steps: decide what the declaration does, land it,
    // then record it. Nothing lands here (no lake), so the record follows
    // the decision directly.
    async fn declare(s: &Store, d: &glossql_parser::RecipeDecl) -> RecipeAdmission {
        let admission = s.recipe_admission(d).await.unwrap();
        s.put_recipe(d).await.unwrap();
        admission
    }

    let v1 = recipe(
        "DECLARE RECIPE orders ON fin FROM erp AS $$SELECT * FROM read_parquet('orders/*.parquet')$$;",
    );
    assert_eq!(declare(&s, &v1).await, RecipeAdmission::Created);
    assert_eq!(declare(&s, &v1).await, RecipeAdmission::Unchanged);

    // A changed SQL supersedes (ruled 2026-08-06) — the row updates and
    // the session re-lands on `Replaced`.
    let v2 = recipe(
        "DECLARE RECIPE orders ON fin FROM erp AS $$SELECT * FROM read_parquet('orders_v2/*.parquet')$$;",
    );
    assert_eq!(declare(&s, &v2).await, RecipeAdmission::Replaced);
    let row = s.recipe("fin", "orders").await.unwrap().unwrap();
    assert!(row.sql.contains("orders_v2"), "{}", row.sql);
    // The new spelling is now the unchanged one.
    assert_eq!(declare(&s, &v2).await, RecipeAdmission::Unchanged);
}

// -- writes invalidate (project lead, 2026-08-04) --------------------------

#[tokio::test]
async fn a_gloss_invalidates_the_caches_of_functions_accepting_its_aspect() {
    let s = store().await;
    let Declaration::Function(f) =
        decl(r#"DECLARE FUNCTION conv FOR GLOBAL FROM 'conv.rhai' ACCEPTS (unit);"#)
    else {
        unreachable!()
    };
    s.declare_function(&f).await.unwrap();
    s.cache_put("fin", "orders.amount", "conv", None, "{}", None)
        .await
        .unwrap();
    s.cache_put("fin", "invoices.total", "conv", None, "{}", None)
        .await
        .unwrap();

    // At or under the subject: the other table's row survives.
    write(
        &s,
        &agent(),
        r#"GLOSS unit ON orders.amount AS $${"value": "EUR"}$$;"#,
    )
    .await
    .unwrap();
    assert!(
        s.cache_get("fin", "orders.amount", "conv", None)
            .await
            .unwrap()
            .is_none(),
        "the dependent evidence died with the write"
    );
    assert!(
        s.cache_get("fin", "invoices.total", "conv", None)
            .await
            .unwrap()
            .is_some()
    );

    // A dataset-level gloss sweeps the dataset.
    let g = gloss(r#"GLOSS unit ON fin AS $${"value": "EUR"}$$;"#);
    s.gloss("fin", &agent(), "unit", "fin", &g.body, None)
        .await
        .unwrap();
    assert!(
        s.cache_get("fin", "invoices.total", "conv", None)
            .await
            .unwrap()
            .is_none()
    );
}

// -- aspect grain (ruled 2026-08-05) --------------------------------------

#[tokio::test]
async fn grain_gates_glosses_and_bounds_disclosure() {
    let s = store().await;
    let Declaration::Aspect(role) = decl(
        r#"DECLARE ASPECT role WITH $${
            "type": "object", "properties": {"value": {"type": "string"}}
        }$$ AS FACT ON COLUMN;"#,
    ) else {
        unreachable!()
    };
    s.declare_aspect(&role).await.unwrap();
    // A witness puts the aspect on the disclosure grid.
    let Declaration::Witness(w) = decl("DECLARE WITNESS role_w ON role BY (AGENT, HUMAN);") else {
        unreachable!()
    };
    s.declare_witness(&w).await.unwrap();

    // In-grain lands; a table subject is refused with the grain named.
    let g = gloss(r#"GLOSS role ON orders.amount AS $${"value": "measure"}$$;"#);
    s.gloss("fin", &agent(), "role", "orders.amount", &g.body, None)
        .await
        .unwrap();
    let e = s
        .gloss("fin", &agent(), "role", "orders", &g.body, None)
        .await
        .unwrap_err();
    assert!(
        matches!(e, Error::GrainRefused { grain: "table", .. }),
        "{e}"
    );

    // Disclosure stays within grain: the unglossed column is a visible
    // absence, the table never shows a role row at all.
    let ctx = ReadContext {
        universe: vec!["orders".into(), "orders.amount".into(), "orders.qty".into()],
        ..Default::default()
    };
    let rows = s
        .collapsed_read("fin", &Scope::Dataset, None, &ctx)
        .await
        .unwrap();
    let states: Vec<(String, String)> = rows
        .iter()
        .filter(|r| r.aspect == "role")
        .map(|r| (r.subject.clone(), r.state.clone()))
        .collect();
    assert!(
        states.contains(&("orders.amount".into(), "current".into())),
        "{states:?}"
    );
    assert!(
        states.contains(&("orders.qty".into(), "unassessed".into())),
        "{states:?}"
    );
    assert!(
        !states.iter().any(|(subject, _)| subject == "orders"),
        "{states:?}"
    );
}

// -- what the adversarial review found (2026-08-06) ------------------------

#[tokio::test]
async fn a_forwarded_delete_refuses_a_statement_sequence() {
    // The store executes forwarded text verbatim, and SQLite runs every
    // `;`-separated statement in it. The caller normalizes literals; this
    // is the check where the execution happens, so it holds whatever the
    // caller did.
    let s = store().await;
    let e = s
        .forward_delete(
            "cache",
            "DELETE FROM cache WHERE subject = 'x'; DROP TABLE glossary",
        )
        .await
        .unwrap_err();
    assert!(matches!(e, Error::ForwardUnsafe { .. }), "{e}");
    let e = s
        .forward_delete("cache", "DELETE FROM cache WHERE subject = $q$x$q$")
        .await
        .unwrap_err();
    assert!(matches!(e, Error::ForwardUnsafe { .. }), "{e}");
    // A `;` inside a quoted literal is data, not a separator.
    s.forward_delete("cache", "DELETE FROM cache WHERE subject = 'a;b'")
        .await
        .expect("quoted semicolons are values");
}

#[tokio::test]
async fn a_subject_is_data_in_the_scope_predicate_not_a_pattern() {
    // `_` is LIKE's single-character wildcard, so `order_items` used to
    // sweep `orderxitems` — and its cached evidence with it.
    let s = store().await;
    s.cache_put("fin", "order_items.qty", "profile", None, "{}", None)
        .await
        .unwrap();
    s.cache_put("fin", "orderxitems.qty", "profile", None, "{}", None)
        .await
        .unwrap();
    s.invalidate_table_evidence("fin", "order_items")
        .await
        .unwrap();
    assert!(
        s.cache_get("fin", "order_items.qty", "profile", None)
            .await
            .unwrap()
            .is_none(),
        "the named table's evidence is cleared"
    );
    assert!(
        s.cache_get("fin", "orderxitems.qty", "profile", None)
            .await
            .unwrap()
            .is_some(),
        "a neighbour whose name differs by one character keeps its evidence"
    );
}

#[tokio::test]
async fn a_table_cannot_take_a_store_relation_name() {
    let s = store().await;
    let Declaration::Dataset(ds) = decl("DECLARE DATASET fin SET (purpose: 'test');") else {
        unreachable!()
    };
    s.declare_dataset(&ds).await.unwrap();
    let Declaration::Source(src) = decl("DECLARE SOURCE erp SET (type: csv, location: 'lake');")
    else {
        unreachable!()
    };
    s.declare_source(&src).await.unwrap();
    let Declaration::Recipe(r) =
        decl("DECLARE RECIPE imports ON fin FROM erp AS $$SELECT * FROM read_csv('i.csv')$$;")
    else {
        unreachable!()
    };
    let e = s.recipe_admission(&r).await.unwrap_err();
    assert!(matches!(e, Error::ReservedTableName(_)), "{e}");
}

#[tokio::test]
async fn a_function_cannot_accept_the_aspect_it_returns() {
    let s = store().await;
    let Declaration::Function(f) =
        decl("DECLARE FUNCTION refine FOR fin FROM 'r.rhai' ACCEPTS (unit) RETURNS unit;")
    else {
        unreachable!()
    };
    let e = s.declare_function(&f).await.unwrap_err();
    assert!(matches!(e, Error::SelfAccepting { .. }), "{e}");
}

#[tokio::test]
async fn an_aspect_with_cached_values_under_it_does_not_re_declare() {
    // Glosses were counted, function values were not — and a MEASUREMENT
    // aspect can only hold the latter, so its schema could change under
    // values validated against the old one.
    let s = store().await;
    let Declaration::Aspect(a) =
        decl(r#"DECLARE ASPECT profile_stats WITH $${"type": "object"}$$ AS MEASUREMENT;"#)
    else {
        unreachable!()
    };
    s.declare_aspect(&a).await.unwrap();
    let Declaration::Function(f) =
        decl("DECLARE FUNCTION profile FOR fin FROM 'p.rhai' RETURNS profile_stats;")
    else {
        unreachable!()
    };
    s.declare_function(&f).await.unwrap();
    s.cache_put("fin", "orders.amount", "profile", None, r#"{"n": 1}"#, None)
        .await
        .unwrap();

    let Declaration::Aspect(stricter) = decl(
        r#"DECLARE ASPECT profile_stats WITH $${"type": "object", "required": ["n"]}$$ AS MEASUREMENT;"#,
    ) else {
        unreachable!()
    };
    let e = s.declare_aspect(&stricter).await.unwrap_err();
    assert!(matches!(e, Error::AspectValued { .. }), "{e}");
}

// -- what the adversarial review found (2026-08-12) ------------------------

#[tokio::test]
async fn a_glossary_delete_invalidates_accepting_functions_like_a_write() {
    let s = store().await;
    // `conv` RETURNS so its cache rows are values, not detector verdicts —
    // only the ACCEPTS edge can strike them here.
    let Declaration::Aspect(converted) =
        decl(r#"DECLARE ASPECT converted WITH $${"type": "object"}$$ AS MEASUREMENT;"#)
    else {
        unreachable!()
    };
    s.declare_aspect(&converted).await.unwrap();
    let Declaration::Function(f) =
        decl("DECLARE FUNCTION conv FOR GLOBAL FROM 'conv.rhai' ACCEPTS (unit) RETURNS converted;")
    else {
        unreachable!()
    };
    s.declare_function(&f).await.unwrap();
    write(
        &s,
        &agent(),
        r#"GLOSS unit ON orders.amount AS $${"value": "EUR"}$$;"#,
    )
    .await
    .unwrap();
    s.cache_put("fin", "orders.amount", "conv", None, "{}", None)
        .await
        .unwrap();
    // `whatif` rows are ordinary cache rows outside the functions table;
    // their reads replay QUERY groundings, so a glossary strike stales them.
    s.cache_put("fin", "fin", "whatif", None, r#"{"bands": []}"#, None)
        .await
        .unwrap();
    s.cache_put("crm", "leads.score", "conv", None, "{}", None)
        .await
        .unwrap();
    s.cache_put("crm", "crm", "whatif", None, r#"{"bands": []}"#, None)
        .await
        .unwrap();

    s.forward_delete("glossary", "DELETE FROM glossary WHERE aspect = 'unit'")
        .await
        .unwrap();
    assert!(
        s.cache_get("fin", "orders.amount", "conv", None)
            .await
            .unwrap()
            .is_none(),
        "the dependent evidence died with the strike, as it would with a write"
    );
    assert!(
        s.cache_get("fin", "fin", "whatif", None)
            .await
            .unwrap()
            .is_none(),
        "whatif reads replay what the delete just changed"
    );
    assert!(
        s.cache_get("crm", "leads.score", "conv", None)
            .await
            .unwrap()
            .is_some(),
        "the sweep is keyed by the doomed rows' dataset, not the world"
    );
    assert!(
        s.cache_get("crm", "crm", "whatif", None)
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn an_unextractable_delete_predicate_sweeps_the_cache_blunt() {
    let s = store().await;
    write(
        &s,
        &agent(),
        r#"GLOSS unit ON orders.amount AS $${"value": "EUR"}$$;"#,
    )
    .await
    .unwrap();
    s.cache_put("crm", "leads.score", "profile", None, "{}", None)
        .await
        .unwrap();
    // A LIMIT anywhere outside a literal — even in a subquery, where the
    // predicate would still be exact — hides which rows die; correctness
    // over precision, the whole cache goes.
    s.forward_delete(
        "glossary",
        "DELETE FROM glossary WHERE id IN (SELECT id FROM glossary LIMIT 1)",
    )
    .await
    .unwrap();
    assert!(
        s.cache_get("crm", "leads.score", "profile", None)
            .await
            .unwrap()
            .is_none(),
        "unknown targets sweep everything"
    );
}

#[tokio::test]
async fn a_cache_delete_of_a_voice_drops_the_verdicts_over_its_aspect() {
    let s = store().await;
    // A voice into `unit` (RETURNS), a detector, and the witness pairing
    // them.
    let Declaration::Function(voice) =
        decl("DECLARE FUNCTION vibes FOR fin FROM 'v.rhai' RETURNS unit;")
    else {
        unreachable!()
    };
    s.declare_function(&voice).await.unwrap();
    let Declaration::Function(det) = decl("DECLARE FUNCTION entropy FOR fin FROM 'e.rhai';") else {
        unreachable!()
    };
    s.declare_function(&det).await.unwrap();
    let Declaration::Witness(w) =
        decl("DECLARE WITNESS unit_w ON unit BY (AGENT, HUMAN) DETECTOR entropy THRESHOLD 0.5;")
    else {
        unreachable!()
    };
    s.declare_witness(&w).await.unwrap();

    s.cache_put(
        "fin",
        "orders.amount",
        "vibes",
        None,
        r#"{"value": "EUR"}"#,
        None,
    )
    .await
    .unwrap();
    s.cache_put(
        "fin",
        "orders.amount",
        "entropy",
        Some("unit_w"),
        r#"{"band": "red", "score": 0.9}"#,
        None,
    )
    .await
    .unwrap();
    s.cache_put(
        "fin",
        "invoices.total",
        "entropy",
        Some("unit_w"),
        r#"{"band": "green", "score": 0.1}"#,
        None,
    )
    .await
    .unwrap();

    // Striking the disputed voice makes the slot set older, never newer —
    // a cached contested verdict would pass the freshness check and keep
    // withholding a value the dispute no longer touches.
    s.forward_delete("cache", "DELETE FROM cache WHERE function = 'vibes'")
        .await
        .unwrap();
    assert!(
        s.cache_get("fin", "orders.amount", "entropy", Some("unit_w"))
            .await
            .unwrap()
            .is_none(),
        "the verdict over the struck voice's slot recomputes at the next read"
    );
    assert!(
        s.cache_get("fin", "invoices.total", "entropy", Some("unit_w"))
            .await
            .unwrap()
            .is_some(),
        "verdicts on untouched subjects stand"
    );
}

#[tokio::test]
async fn each_verdict_is_judged_against_its_own_witness_threshold() {
    let s = store().await;
    let ctx = ReadContext::default();
    // Two witnesses on one aspect: cross-wiring compared w_b's score
    // against w_a's threshold, so a crossing verdict never contested.
    for f in [
        "DECLARE FUNCTION det_a FOR fin FROM 'a.rhai';",
        "DECLARE FUNCTION det_b FOR fin FROM 'b.rhai';",
    ] {
        let Declaration::Function(d) = decl(f) else {
            unreachable!()
        };
        s.declare_function(&d).await.unwrap();
    }
    for w in [
        "DECLARE WITNESS w_a ON unit BY (AGENT, HUMAN) DETECTOR det_a THRESHOLD 0.9;",
        "DECLARE WITNESS w_b ON unit BY (AGENT, HUMAN) DETECTOR det_b THRESHOLD 0.5;",
    ] {
        let Declaration::Witness(d) = decl(w) else {
            unreachable!()
        };
        s.declare_witness(&d).await.unwrap();
    }
    write(
        &s,
        &agent(),
        r#"GLOSS unit ON orders.amount AS $${"value": "EUR"}$$;"#,
    )
    .await
    .unwrap();
    // w_b's 0.7 crosses its own 0.5 but not w_a's 0.9 — contested: any
    // witness's own-threshold crossing withholds (interim; the plural-
    // witness ruling is still owed).
    s.cache_put(
        "fin",
        "orders.amount",
        "det_a",
        Some("w_a"),
        r#"{"band": "green", "score": 0.2}"#,
        None,
    )
    .await
    .unwrap();
    s.cache_put(
        "fin",
        "orders.amount",
        "det_b",
        Some("w_b"),
        r#"{"band": "red", "score": 0.7}"#,
        None,
    )
    .await
    .unwrap();
    let rows = s
        .collapsed_read("fin", &Scope::Subject("orders.amount".into()), None, &ctx)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].state, "contested", "{:?}", rows[0]);
    assert!(rows[0].value.is_none());
    assert_eq!(
        rows[0].score,
        Some(0.7),
        "the crossing verdict rides the row"
    );

    // 0.6 crosses the neighbour's 0.5 but nobody's own threshold — served.
    let g = gloss(r#"GLOSS unit ON invoices.total AS $${"value": "USD"}$$;"#);
    s.gloss("fin", &agent(), "unit", "invoices.total", &g.body, None)
        .await
        .unwrap();
    s.cache_put(
        "fin",
        "invoices.total",
        "det_a",
        Some("w_a"),
        r#"{"band": "yellow", "score": 0.6}"#,
        None,
    )
    .await
    .unwrap();
    s.cache_put(
        "fin",
        "invoices.total",
        "det_b",
        Some("w_b"),
        r#"{"band": "green", "score": 0.3}"#,
        None,
    )
    .await
    .unwrap();
    let rows = s
        .collapsed_read("fin", &Scope::Subject("invoices.total".into()), None, &ctx)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].state, "current", "{:?}", rows[0]);
    assert!(rows[0].value.as_deref().unwrap().contains("USD"));
    assert_eq!(
        rows[0].score,
        Some(0.6),
        "uncontested, the first witness's verdict (name order) rides the row"
    );
}

#[tokio::test]
async fn imports_relation_serves_unknown_not_negative_on_fan_out() {
    let s = store().await;
    // A fan-out join lands more rows than the source scan counted; the
    // statement outcome saturates to 0, and the relation must not answer
    // with a negative count — the difference is unknown.
    s.import_put("fin", "orders", 10, 25, "{}").await.unwrap();
    s.import_put("fin", "orders", 10, 7, "{}").await.unwrap();
    let rows = s.relation_rows("imports").await.unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][4], None, "landed > source: unknown, not -15");
    assert_eq!(rows[1][4].as_deref(), Some("3"));
}

#[tokio::test]
async fn source_grain_slots_read_and_supersede_workspace_wide() {
    // Ruled 2026-08-12 (the source-conventions proposal, fork B): an
    // aspect ON SOURCE speaks to declared source names, and its slots
    // collapse across datasets — the deposit the next dataset reads.
    let s = store().await;
    for d in [
        "DECLARE DATASET glos SET (purpose: 'p');",
        "DECLARE DATASET fin2 SET (purpose: 'p');",
    ] {
        let Declaration::Dataset(ds) = decl(d) else {
            unreachable!()
        };
        s.declare_dataset(&ds).await.unwrap();
    }
    let Declaration::Source(src) =
        decl("DECLARE SOURCE glos_erp SET (type: parquet, location: 'lake');")
    else {
        unreachable!()
    };
    s.declare_source(&src).await.unwrap();
    let Declaration::Aspect(a) =
        decl(r#"DECLARE ASPECT conventions WITH $${"type": "object"}$$ AS FACT ON SOURCE;"#)
    else {
        unreachable!()
    };
    s.declare_aspect(&a).await.unwrap();

    let g = gloss(r#"GLOSS conventions ON glos_erp AS $${"placeholder_date": "1900-01-01"}$$;"#);
    // A bare name that is not a declared source stays table-shaped and
    // is refused at SOURCE grain.
    let e = s
        .gloss("glos", &agent(), "conventions", "orders", &g.body, None)
        .await
        .unwrap_err();
    assert!(matches!(e, Error::GrainRefused { .. }), "{e}");

    // Spoken while glos was the dataset; served in fin2 unchanged.
    s.gloss("glos", &agent(), "conventions", "glos_erp", &g.body, None)
        .await
        .unwrap();
    let rows = s
        .collapsed_read(
            "fin2",
            &Scope::Subject("glos_erp".into()),
            None,
            &ReadContext::default(),
        )
        .await
        .unwrap();
    let row = rows
        .iter()
        .find(|r| r.aspect == "conventions")
        .expect("the deposit reads from the other dataset");
    assert_eq!(row.state, "current");

    // Superseded from fin2; the newest slot wins back in glos too —
    // supersession over (subject, aspect, actor kind) ignores the
    // dataset at source grain.
    let g2 = gloss(r#"GLOSS conventions ON glos_erp AS $${"placeholder_date": "cured"}$$;"#);
    s.gloss("fin2", &agent(), "conventions", "glos_erp", &g2.body, None)
        .await
        .unwrap();
    let rows = s
        .collapsed_read(
            "glos",
            &Scope::Subject("glos_erp".into()),
            None,
            &ReadContext::default(),
        )
        .await
        .unwrap();
    let row = rows.iter().find(|r| r.aspect == "conventions").unwrap();
    assert!(
        row.value.as_ref().unwrap().to_string().contains("cured"),
        "{row:?}"
    );
}

#[tokio::test]
async fn an_unspoken_source_aspect_is_owed_on_every_declared_source() {
    // Disclosure at SOURCE grain: a witnessed conventions aspect nobody
    // spoke to shows as an unassessed row on the declared source, in
    // whichever dataset the read runs (ruled 2026-08-12).
    let s = store().await;
    let Declaration::Dataset(ds) = decl("DECLARE DATASET fin SET (purpose: 'p');") else {
        unreachable!()
    };
    s.declare_dataset(&ds).await.unwrap();
    let Declaration::Source(src) =
        decl("DECLARE SOURCE glos_erp SET (type: parquet, location: 'lake');")
    else {
        unreachable!()
    };
    s.declare_source(&src).await.unwrap();
    let Declaration::Aspect(a) =
        decl(r#"DECLARE ASPECT conventions WITH $${"type": "object"}$$ AS FACT ON SOURCE;"#)
    else {
        unreachable!()
    };
    s.declare_aspect(&a).await.unwrap();
    let Declaration::Witness(w) = decl("DECLARE WITNESS conventions_w ON conventions BY (AGENT, HUMAN);")
    else {
        unreachable!()
    };
    s.declare_witness(&w).await.unwrap();

    let rows = s
        .collapsed_read("fin", &Scope::Dataset, None, &ReadContext::default())
        .await
        .unwrap();
    assert!(
        rows.iter().any(|r| r.subject == "glos_erp"
            && r.aspect == "conventions"
            && r.state == "unassessed"),
        "{rows:?}"
    );
}
