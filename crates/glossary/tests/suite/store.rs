//! Store behavior the language fixes: admission by aspect kind (SPEC.md
//! §5.2), the witness speaker gate and detector eligibility (§7.1),
//! supersession per (subject, aspect, actor kind), the provisional collapse
//! policy (§5.3), and measurement semantics (§6).

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

async fn store() -> (tempfile::TempDir, Store) {
    let dir = tempfile::tempdir().unwrap();
    let lake = glossql_catalog::Lake::open(
        &dir.path().join("catalog.sqlite"),
        &dir.path().join("warehouse"),
    )
    .await
    .unwrap();
    let store = Store::open(lake).await.unwrap();
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
    (dir, store)
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

/// The read's context — built after the writes it should see, exactly
/// as a statement's pre-pass builds it.
async fn rctx(store: &Store) -> ReadContext {
    store
        .read_context("fin", vec![], Default::default())
        .await
        .unwrap()
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
    let (_dir, s) = store().await;
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
    let (_dir, s) = store().await;
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
    let (_dir, s) = store().await;
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
    // A stop in place of the SQL (SPEC.md §5.2): admitted with its
    // reason; both at once is neither.
    write(
        &s,
        &agent(),
        r#"GLOSS revenue ON orders.amount AS $${"stopped": "amount never landed"}$$;"#,
    )
    .await
    .unwrap();
    let e = write(
        &s,
        &agent(),
        r#"GLOSS revenue ON orders.amount AS $${"sql": "SELECT 1", "stopped": "both"}$$;"#,
    )
    .await
    .unwrap_err();
    assert!(matches!(e, Error::BodyRejected { .. }), "{e}");
    // The authored stock marker:
    // "stock"/"flow" admitted, anything else refused.
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
    // The declared grain: an array of served column names — admitted;
    // an empty array declares nothing and is refused.
    write(
        &s,
        &agent(),
        r#"GLOSS revenue ON orders.amount AS $${"sql": "SELECT amount FROM orders", "grain": ["date", "account_id"]}$$;"#,
    )
    .await
    .unwrap();
    let e = write(
        &s,
        &agent(),
        r#"GLOSS revenue ON orders.amount AS $${"sql": "SELECT amount FROM orders", "grain": []}$$;"#,
    )
    .await
    .unwrap_err();
    assert!(matches!(e, Error::BodyRejected { .. }), "{e}");
}

#[tokio::test]
async fn measurement_aspects_are_never_glossed() {
    let (_dir, s) = store().await;
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
    let (_dir, s) = store().await;
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
    let (_dir, s) = store().await;
    let Declaration::Aspect(m) =
        decl(r#"DECLARE ASPECT min_max WITH $${"type": "object"}$$ AS MEASUREMENT;"#)
    else {
        unreachable!()
    };
    s.declare_aspect(&m).await.unwrap();
    let Declaration::Function(f) =
        decl("DECLARE FUNCTION profile_min_max FOR fin AS $$#{}$$ RETURNS min_max;")
    else {
        unreachable!()
    };
    s.declare_function(&f).await.unwrap();

    // One producer per MEASUREMENT aspect — a second function is refused.
    let Declaration::Function(rival) =
        decl("DECLARE FUNCTION other_min_max FOR fin AS $$#{}$$ RETURNS min_max;")
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
    let (_dir, s) = store().await;
    let Declaration::Function(f) = decl("DECLARE FUNCTION vibes FOR fin AS $$#{}$$ RETURNS unit;")
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
    let (_dir, s) = store().await;
    let Declaration::Witness(w) =
        decl("DECLARE WITNESS unit_w ON unit BY (AGENT, HUMAN) THRESHOLD 1.7;")
    else {
        unreachable!()
    };
    assert!(s.declare_witness(&w).await.is_err());
}

#[tokio::test]
async fn redeclaring_an_aspect_is_content_idempotent_but_refused_once_glossed() {
    let (_dir, s) = store().await;
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
    let (_dir, s) = store().await;
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
    let rows = Store::raw_read(
        "fin",
        &Scope::Subject("orders.amount".into()),
        None,
        &rctx(&s).await,
    );
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
    let rows = Store::raw_read(
        "fin",
        &Scope::Subject("orders.amount".into()),
        None,
        &rctx(&s).await,
    );
    assert_eq!(rows.len(), 2, "human slot is separate");
}

#[tokio::test]
async fn collapse_serves_by_precedence_human_over_agent() {
    let (_dir, s) = store().await;
    let verdicts = Default::default();
    write(
        &s,
        &agent(),
        r#"GLOSS unit ON orders.amount AS $${"value": "EUR"}$$;"#,
    )
    .await
    .unwrap();
    let rows = Store::collapsed_read(
        "fin",
        &Scope::Subject("orders.amount".into()),
        None,
        &rctx(&s).await,
        &verdicts,
    );
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
    let rows = Store::collapsed_read(
        "fin",
        &Scope::Subject("orders.amount".into()),
        None,
        &rctx(&s).await,
        &verdicts,
    );
    assert_eq!(rows.len(), 1);
    assert!(
        rows[0].value.as_deref().unwrap().contains("USD"),
        "the human slot outranks the agent slot"
    );
}

// -- functions and measurements --------------------------------------------

#[tokio::test]
async fn function_scope_gates_visibility() {
    let (_dir, s) = store().await;
    let Declaration::Function(f) = decl(r#"DECLARE FUNCTION profile FOR fin AS $$#{}$$;"#) else {
        unreachable!()
    };
    s.declare_function(&f).await.unwrap();
    assert!(s.function("profile", Some("fin")).await.unwrap().is_some());
    assert!(s.function("profile", Some("crm")).await.unwrap().is_none());
}

#[tokio::test]
async fn measurements_serve_the_latest_row_at_a_pin_and_miss_at_another() {
    let (_dir, s) = store().await;
    let pin = s.pin("fin", &Default::default()).await.unwrap();
    // `**`: the reads were not enumerable, so the row stands only at
    // its exact pin.
    s.measurement_put(
        "fin",
        "profile",
        "orders",
        "stats",
        &pin,
        r#"{"n": 1}"#,
        "**",
    )
    .await
    .unwrap();
    s.measurement_put(
        "fin",
        "profile",
        "orders",
        "stats",
        &pin,
        r#"{"n": 2}"#,
        "**",
    )
    .await
    .unwrap();
    let ctx = rctx(&s).await;
    let row = Store::measurement_in(&ctx, "fin", "orders", "profile").unwrap();
    assert!(row.body.contains('2'), "latest write at the pin wins");

    // Any input moving makes a new pin: a miss, never an invalidation.
    let moved = s
        .read_context(
            "fin",
            vec![],
            std::collections::HashMap::from([("orders".into(), 7)]),
        )
        .await
        .unwrap();
    assert!(Store::measurement_in(&moved, "fin", "orders", "profile").is_none());
}

/// The currency rule (SPEC.md §7): a measurement that recorded what it
/// read stands while those legs are unchanged — a write to anything
/// else, data or store, is not its staleness.
#[tokio::test]
async fn a_measurement_stands_while_what_it_read_is_unchanged() {
    let (_dir, s) = store().await;
    let at = |orders: i64, payments: i64| {
        std::collections::HashMap::from([
            ("orders".to_string(), orders),
            ("payments".to_string(), payments),
        ])
    };
    let pin = s.pin("fin", &at(7, 3)).await.unwrap();
    // The recorded reads: the orders leg alone, by name — extraction
    // records a body that scanned one table.
    s.measurement_put(
        "fin",
        "profile",
        "orders",
        "stats",
        &pin,
        r#"{"n": 1}"#,
        "fin.orders",
    )
    .await
    .unwrap();

    // payments moves: not a leg it read, the row stands.
    let ctx = s.read_context("fin", vec![], at(7, 4)).await.unwrap();
    assert!(
        Store::measurement_in(&ctx, "fin", "orders", "profile").is_some(),
        "a write it cannot see is not its staleness"
    );

    // A store write — a new function — moves a store leg; the
    // measurement read data alone and still stands.
    let Declaration::Function(f) = decl(r#"DECLARE FUNCTION noise FOR fin AS $$#{}$$;"#) else {
        unreachable!()
    };
    s.declare_function(&f).await.unwrap();
    let ctx = s.read_context("fin", vec![], at(7, 4)).await.unwrap();
    assert!(
        Store::measurement_in(&ctx, "fin", "orders", "profile").is_some(),
        "a store write it cannot see is not its staleness"
    );

    // orders moves: the one leg it read, and the row no longer stands.
    let ctx = s.read_context("fin", vec![], at(8, 4)).await.unwrap();
    assert!(
        Store::measurement_in(&ctx, "fin", "orders", "profile").is_none(),
        "its own input moved"
    );
}

#[tokio::test]
async fn the_strike_is_parked_and_says_so() {
    // The substrate cannot commit a row removal until
    // iceberg-rust 0.11, so `DELETE FROM glossary` refuses by name —
    // and anything but the glossary refuses as ever.
    let (_dir, s) = store().await;
    let e = s.forward_delete("glossary").await.unwrap_err();
    assert!(matches!(e, Error::StrikeParked), "{e}");
    assert!(e.to_string().contains("delete write path"), "{e}");
    let e = s.forward_delete("aspects").await.unwrap_err();
    assert!(matches!(e, Error::ForwardRejected(_)), "{e}");
}

#[tokio::test]
async fn grain_gates_glosses_and_bounds_disclosure() {
    let (_dir, s) = store().await;
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
    let ctx = s
        .read_context(
            "fin",
            vec!["orders".into(), "orders.amount".into(), "orders.qty".into()],
            Default::default(),
        )
        .await
        .unwrap();
    let rows = Store::collapsed_read("fin", &Scope::Dataset, None, &ctx, &Default::default());
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

// -- conditional relevance -------------------------------------------------

#[tokio::test]
async fn a_condition_narrows_what_a_subject_owes() {
    let (_dir, s) = store().await;
    let Declaration::Aspect(role) = decl(
        r#"DECLARE ASPECT role WITH $${
            "type": "object", "properties": {"value": {"enum": ["key", "measure"]}}
        }$$ AS FACT ON COLUMN;"#,
    ) else {
        unreachable!()
    };
    s.declare_aspect(&role).await.unwrap();
    let Declaration::Aspect(behavior) = decl(
        r#"DECLARE ASPECT behavior WITH $${
            "type": "object", "properties": {"value": {"type": "string"}}
        }$$ AS FACT ON COLUMN WHEN role = 'measure';"#,
    ) else {
        unreachable!()
    };
    s.declare_aspect(&behavior).await.unwrap();
    for statement in [
        "DECLARE WITNESS role_w ON role BY (AGENT, HUMAN);",
        "DECLARE WITNESS behavior_w ON behavior BY (AGENT, HUMAN);",
    ] {
        let Declaration::Witness(w) = decl(statement) else {
            unreachable!()
        };
        s.declare_witness(&w).await.unwrap();
    }
    let uni = || {
        vec![
            "orders.amount".to_string(),
            "orders.customer_id".to_string(),
        ]
    };

    // Before any role is spoken, no column owes behavior — role is the
    // whole backlog.
    let rows = Store::collapsed_read(
        "fin",
        &Scope::Dataset,
        None,
        &s.read_context("fin", uni(), Default::default())
            .await
            .unwrap(),
        &Default::default(),
    );
    assert!(!rows.iter().any(|r| r.aspect == "behavior"), "{rows:?}");

    // role lands — measure on amount, key on customer_id — and behavior
    // is owed on the measure alone.
    for (subject, body) in [
        (
            "orders.amount",
            r#"GLOSS role ON orders.amount AS $${"value": "measure"}$$;"#,
        ),
        (
            "orders.customer_id",
            r#"GLOSS role ON orders.customer_id AS $${"value": "key"}$$;"#,
        ),
    ] {
        let g = gloss(body);
        s.gloss("fin", &agent(), "role", subject, &g.body, None)
            .await
            .unwrap();
    }
    let rows = Store::collapsed_read(
        "fin",
        &Scope::Dataset,
        None,
        &s.read_context("fin", uni(), Default::default())
            .await
            .unwrap(),
        &Default::default(),
    );
    let behavior_states: Vec<(String, String)> = rows
        .iter()
        .filter(|r| r.aspect == "behavior")
        .map(|r| (r.subject.clone(), r.state.clone()))
        .collect();
    assert_eq!(
        behavior_states,
        vec![("orders.amount".into(), "unassessed".into())],
        "{rows:?}"
    );

    // A spoken slot outside its condition still serves — the condition
    // bounds disclosure, never writes.
    let g = gloss(r#"GLOSS behavior ON orders.customer_id AS $${"value": "none"}$$;"#);
    s.gloss(
        "fin",
        &agent(),
        "behavior",
        "orders.customer_id",
        &g.body,
        None,
    )
    .await
    .unwrap();
    let rows = Store::collapsed_read(
        "fin",
        &Scope::Dataset,
        None,
        &s.read_context("fin", uni(), Default::default())
            .await
            .unwrap(),
        &Default::default(),
    );
    assert!(
        rows.iter().any(|r| r.aspect == "behavior"
            && r.subject == "orders.customer_id"
            && r.state == "current"),
        "{rows:?}"
    );
}

#[tokio::test]
async fn a_condition_is_validated_at_declare() {
    let (_dir, s) = store().await;
    // Referencing an undeclared aspect is refused.
    let Declaration::Aspect(unanchored) = decl(
        r#"DECLARE ASPECT currency WITH $${"type": "object"}$$ AS FACT ON COLUMN WHEN role = 'measure';"#,
    ) else {
        unreachable!()
    };
    let e = s.declare_aspect(&unanchored).await.unwrap_err();
    assert!(matches!(e, Error::BadCondition { .. }), "{e}");

    // The literal itself is not judged: a value no slot ever carries
    // makes a condition that never holds, the same as for any schema
    // shape without an enum.
    let Declaration::Aspect(role) = decl(
        r#"DECLARE ASPECT role WITH $${
            "type": "object", "properties": {"value": {"enum": ["key", "measure"]}}
        }$$ AS FACT ON COLUMN;"#,
    ) else {
        unreachable!()
    };
    s.declare_aspect(&role).await.unwrap();

    // The spelled-right condition lands; identical redeclaration stays
    // a no-op.
    let Declaration::Aspect(currency) = decl(
        r#"DECLARE ASPECT currency WITH $${"type": "object"}$$ AS FACT ON COLUMN WHEN role = 'measure';"#,
    ) else {
        unreachable!()
    };
    s.declare_aspect(&currency).await.unwrap();
    s.declare_aspect(&currency).await.unwrap();
}

#[tokio::test]
async fn a_subject_is_data_in_the_scope_predicate_not_a_pattern() {
    // `_` is LIKE's single-character wildcard, so `order_items` used to
    // sweep `orderxitems` with it.
    let (_dir, s) = store().await;
    for subject in ["order_items.qty", "orderxitems.qty"] {
        let g = gloss(r#"GLOSS unit ON x AS $${"value": "EUR"}$$;"#);
        s.gloss("fin", &agent(), "unit", subject, &g.body, None)
            .await
            .unwrap();
    }
    let rows = Store::raw_read(
        "fin",
        &Scope::Subject("order_items".into()),
        None,
        &rctx(&s).await,
    );
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(rows[0].subject, "order_items.qty");
    let scope = Scope::Subject("order_items".into());
    assert!(scope.admits("order_items.qty"));
    assert!(
        !scope.admits("orderxitems.qty"),
        "the in-memory twin holds the same line"
    );
}

#[tokio::test]
async fn a_table_cannot_take_a_store_relation_name() {
    let (_dir, s) = store().await;
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
async fn each_verdict_is_judged_against_its_own_witness_threshold() {
    use glossql_glossary::{Verdict, Verdicts};
    let (_dir, s) = store().await;
    let at = |witness: &str, band: &str, score: f64, threshold: f64| Verdict {
        witness: witness.into(),
        band: band.into(),
        score,
        threshold: Some(threshold),
        computed_at: "t".into(),
        current: true,
        error: None,
    };
    write(
        &s,
        &agent(),
        r#"GLOSS unit ON orders.amount AS $${"value": "EUR"}$$;"#,
    )
    .await
    .unwrap();
    // w_b's 0.7 crosses its own 0.5 but not w_a's 0.9 — the crossing
    // is judged against the RIGHT threshold (the cross-wiring
    // regression this test exists for). With one voice it shows as a
    // red band beside the served value (contested
    // needs voices that can differ); a second voice below turns the
    // same crossing into a contest.
    let mut verdicts = Verdicts::default();
    verdicts.insert(
        ("orders.amount".into(), "unit".into()),
        vec![at("w_a", "green", 0.2, 0.9), at("w_b", "red", 0.7, 0.5)],
    );
    let rows = Store::collapsed_read(
        "fin",
        &Scope::Subject("orders.amount".into()),
        None,
        &rctx(&s).await,
        &verdicts,
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].state, "current", "{:?}", rows[0]);
    assert_eq!(rows[0].band.as_deref(), Some("red"), "{:?}", rows[0]);
    assert!(rows[0].value.is_some(), "one voice serves, crossing or not");
    assert_eq!(
        rows[0].score,
        Some(0.7),
        "the crossing verdict rides the row"
    );

    // A second voice on the same slot: now the crossing contests and
    // the value is withheld.
    write(
        &s,
        &human(),
        r#"GLOSS unit ON orders.amount AS $${"value": "CHF"}$$;"#,
    )
    .await
    .unwrap();
    let rows = Store::collapsed_read(
        "fin",
        &Scope::Subject("orders.amount".into()),
        None,
        &rctx(&s).await,
        &verdicts,
    );
    assert_eq!(rows[0].state, "contested", "{:?}", rows[0]);
    assert!(rows[0].value.is_none());

    // 0.6 crosses the neighbour's 0.5 but nobody's own threshold — served.
    let g = gloss(r#"GLOSS unit ON invoices.total AS $${"value": "USD"}$$;"#);
    s.gloss("fin", &agent(), "unit", "invoices.total", &g.body, None)
        .await
        .unwrap();
    let mut verdicts = Verdicts::default();
    verdicts.insert(
        ("invoices.total".into(), "unit".into()),
        vec![at("w_a", "yellow", 0.6, 0.9), at("w_b", "green", 0.3, 0.5)],
    );
    let rows = Store::collapsed_read(
        "fin",
        &Scope::Subject("invoices.total".into()),
        None,
        &rctx(&s).await,
        &verdicts,
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].state, "current", "{:?}", rows[0]);
    assert!(rows[0].value.as_deref().unwrap().contains("USD"));
    assert_eq!(
        rows[0].score,
        Some(0.6),
        "uncontested, the first witness's verdict rides the row"
    );
}

#[tokio::test]
async fn the_band_withholds_a_value_and_the_raw_score_never_does() {
    use glossql_glossary::{Verdict, Verdicts};
    // A detector may band against something other than its witness
    // THRESHOLD — `rate_tolerance` bands on the authored tolerance and
    // falls back to the THRESHOLD only where no expectation stands. The
    // collapse must not re-derive a crossing from score against that
    // same THRESHOLD: it would judge the score a second time against
    // the line the detector deliberately replaced, and the slot read
    // green while its value was withheld.
    let (_dir, s) = store().await;
    let at = |witness: &str, band: &str, score: f64, threshold: f64| Verdict {
        witness: witness.into(),
        band: band.into(),
        score,
        threshold: Some(threshold),
        computed_at: "t".into(),
        current: true,
        error: None,
    };
    for (actor, value) in [(agent(), "EUR"), (human(), "CHF")] {
        write(
            &s,
            &actor,
            &format!(r#"GLOSS unit ON orders.amount AS $${{"value": "{value}"}}$$;"#),
        )
        .await
        .unwrap();
    }

    // Two voices, so a crossing could contest. The score sits far above
    // the witness threshold and the detector still says green.
    let mut verdicts = Verdicts::default();
    verdicts.insert(
        ("orders.amount".into(), "unit".into()),
        vec![at("w_a", "green", 0.9, 0.1)],
    );
    let rows = Store::collapsed_read(
        "fin",
        &Scope::Subject("orders.amount".into()),
        None,
        &rctx(&s).await,
        &verdicts,
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].state, "current",
        "green never withholds: {:?}",
        rows[0]
    );
    assert!(rows[0].value.is_some(), "{:?}", rows[0]);

    // And red withholds on the band alone, with the score below the
    // threshold it would have been measured against.
    let mut verdicts = Verdicts::default();
    verdicts.insert(
        ("orders.amount".into(), "unit".into()),
        vec![at("w_a", "red", 0.01, 0.9)],
    );
    let rows = Store::collapsed_read(
        "fin",
        &Scope::Subject("orders.amount".into()),
        None,
        &rctx(&s).await,
        &verdicts,
    );
    assert_eq!(rows[0].state, "contested", "{:?}", rows[0]);
    assert!(rows[0].value.is_none(), "{:?}", rows[0]);
}

#[tokio::test]
async fn source_grain_slots_read_and_supersede_workspace_wide() {
    // An
    // aspect ON SOURCE speaks to declared source names, and its slots
    // collapse across datasets — the deposit the next dataset reads.
    let (_dir, s) = store().await;
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
    let rows = Store::collapsed_read(
        "fin2",
        &Scope::Subject("glos_erp".into()),
        None,
        &rctx(&s).await,
        &Default::default(),
    );
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
    let rows = Store::collapsed_read(
        "glos",
        &Scope::Subject("glos_erp".into()),
        None,
        &rctx(&s).await,
        &Default::default(),
    );
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
    // whichever dataset the read runs.
    let (_dir, s) = store().await;
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
    let Declaration::Witness(w) =
        decl("DECLARE WITNESS conventions_w ON conventions BY (AGENT, HUMAN);")
    else {
        unreachable!()
    };
    s.declare_witness(&w).await.unwrap();

    let rows = Store::collapsed_read(
        "fin",
        &Scope::Dataset,
        None,
        &rctx(&s).await,
        &Default::default(),
    );
    assert!(
        rows.iter().any(|r| r.subject == "glos_erp"
            && r.aspect == "conventions"
            && r.state == "unassessed"),
        "{rows:?}"
    );
}

#[tokio::test]
async fn a_source_subject_is_refused_outside_source_grain() {
    // `GLOSS entity ON erp` — a table-grain aspect,
    // a source subject — must refuse: accepted, the unassessed grid
    // carries rows that can never legitimately be filled. A source name is
    // SOURCE grain, never table grain: table-grain writes refuse it and
    // the backlog stays clean.
    let (_dir, s) = store().await;
    let Declaration::Dataset(ds) = decl("DECLARE DATASET fin SET (purpose: 'p');") else {
        unreachable!()
    };
    s.declare_dataset(&ds).await.unwrap();
    let Declaration::Source(src) =
        decl("DECLARE SOURCE erp SET (type: parquet, location: 'lake');")
    else {
        unreachable!()
    };
    s.declare_source(&src).await.unwrap();
    let Declaration::Aspect(a) =
        decl(r#"DECLARE ASPECT entity WITH $${"type": "object"}$$ AS FACT ON TABLE;"#)
    else {
        unreachable!()
    };
    s.declare_aspect(&a).await.unwrap();
    let Declaration::Witness(w) = decl("DECLARE WITNESS entity_w ON entity BY (AGENT, HUMAN);")
    else {
        unreachable!()
    };
    s.declare_witness(&w).await.unwrap();

    let g = gloss(r#"GLOSS entity ON erp AS $${"value": "not a table"}$$;"#);
    let e = s
        .gloss("fin", &agent(), "entity", "erp", &g.body, None)
        .await
        .unwrap_err();
    assert!(
        matches!(
            e,
            Error::GrainRefused {
                grain: "source",
                ..
            }
        ),
        "{e}"
    );

    // And the disclosure agrees: the table owes an entity row, the
    // source never does.
    let ctx = s
        .read_context("fin", vec!["orders".into()], Default::default())
        .await
        .unwrap();
    let rows = Store::collapsed_read("fin", &Scope::Dataset, None, &ctx, &Default::default());
    assert!(
        rows.iter()
            .any(|r| r.subject == "orders" && r.aspect == "entity" && r.state == "unassessed"),
        "{rows:?}"
    );
    assert!(
        !rows
            .iter()
            .any(|r| r.subject == "erp" && r.aspect == "entity"),
        "{rows:?}"
    );
}

#[tokio::test]
async fn a_source_named_after_its_dataset_is_still_a_source() {
    // Nothing forbids naming a source for the dataset it feeds, and the
    // backlog has always read such a name as SOURCE grain. Admission
    // used to disagree — it tested `subject == dataset` first and
    // resolved the name to DATASET grain, so a source-grain aspect
    // refused the only subject that could ever carry it, with no way to
    // rename around it.
    let (_dir, s) = store().await;
    let Declaration::Dataset(ds) = decl("DECLARE DATASET erp SET (purpose: 'p');") else {
        unreachable!()
    };
    s.declare_dataset(&ds).await.unwrap();
    let Declaration::Source(src) =
        decl("DECLARE SOURCE erp SET (type: parquet, location: 'lake');")
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
    let Declaration::Witness(w) =
        decl("DECLARE WITNESS conventions_w ON conventions BY (AGENT, HUMAN);")
    else {
        unreachable!()
    };
    s.declare_witness(&w).await.unwrap();

    let g = gloss(r#"GLOSS conventions ON erp AS $${"value": "ISO dates"}$$;"#);
    s.gloss("erp", &agent(), "conventions", "erp", &g.body, None)
        .await
        .expect("a source keeps source grain even where it spells its dataset");
}

#[tokio::test]
async fn a_dataset_stays_glossable_when_a_source_shares_its_name() {
    // The other half of the collision. A source may carry its dataset's
    // name, and admission resolved such a name to SOURCE and stopped —
    // so `GLOSS cube ON f1`, a DATASET-grain aspect on the dataset's own
    // spelling, was refused with no way to reach it. Nothing about the
    // subject decides this: the aspect's declaration says which reading
    // is meant, and the collapsed read agrees, keying a row source-grain
    // only when its aspect is source-grain too.
    let (_dir, s) = store().await;
    let Declaration::Dataset(ds) = decl("DECLARE DATASET f1 SET (purpose: 'p');") else {
        unreachable!()
    };
    s.declare_dataset(&ds).await.unwrap();
    let Declaration::Source(src) = decl("DECLARE SOURCE f1 SET (type: parquet, location: 'lake');")
    else {
        unreachable!()
    };
    s.declare_source(&src).await.unwrap();
    let Declaration::Aspect(a) =
        decl(r#"DECLARE ASPECT topic WITH $${"type": "object"}$$ AS FACT ON DATASET;"#)
    else {
        unreachable!()
    };
    s.declare_aspect(&a).await.unwrap();
    let Declaration::Witness(w) = decl("DECLARE WITNESS topic_w ON topic BY (AGENT, HUMAN);")
    else {
        unreachable!()
    };
    s.declare_witness(&w).await.unwrap();

    let g = gloss(r#"GLOSS topic ON f1 AS $${"value": "race results"}$$;"#);
    s.gloss("f1", &agent(), "topic", "f1", &g.body, None)
        .await
        .expect("a dataset stays reachable when a source spells its name");

    // And the backlog agrees with admission, in both directions: the row
    // it takes is disclosed, the reading it refuses is not owed.
    let ctx = s
        .read_context("f1", vec![], Default::default())
        .await
        .unwrap();
    let rows = Store::collapsed_read("f1", &Scope::Dataset, None, &ctx, &Default::default());
    assert!(
        rows.iter()
            .any(|r| r.subject == "f1" && r.aspect == "topic" && r.state == "current"),
        "{rows:?}"
    );

    // A grain neither reading supports still refuses, and names both.
    let Declaration::Aspect(a) =
        decl(r#"DECLARE ASPECT entity WITH $${"type": "object"}$$ AS FACT ON TABLE;"#)
    else {
        unreachable!()
    };
    s.declare_aspect(&a).await.unwrap();
    let g = gloss(r#"GLOSS entity ON f1 AS $${"value": "not a table"}$$;"#);
    let e = s
        .gloss("f1", &agent(), "entity", "f1", &g.body, None)
        .await
        .unwrap_err();
    assert!(
        matches!(
            e,
            Error::GrainRefused {
                grain: "source or dataset",
                ..
            }
        ),
        "{e}"
    );
}

/// The store holds what it resolved under the version it read it at,
/// and a write moves that version. This is the whole cache: there is no
/// freshness check on the read path and no invalidation call at the
/// write — a commit drops the head, the next read walks a new one, and
/// a moved version simply misses.
///
/// Asserted on identity, not contents: the six relations ride behind
/// `Arc`s, so a served-from-cache context shares their pointers and a
/// rebuilt one cannot.
#[tokio::test]
async fn a_read_context_is_held_until_a_write_moves_the_version() {
    let (_dir, store) = store().await;
    let first = rctx(&store).await;
    let again = rctx(&store).await;
    assert_eq!(
        first.version, again.version,
        "no write stands between these reads"
    );
    assert!(
        std::sync::Arc::ptr_eq(&first.glossary, &again.glossary),
        "the second read rebuilt a store nothing had moved"
    );

    write(
        &store,
        &agent(),
        "GLOSS unit ON orders.amount AS $${\"value\":\"eur\"}$$;",
    )
    .await
    .unwrap();

    let after = rctx(&store).await;
    assert_ne!(
        first.version, after.version,
        "a landed gloss left the store's version where it was"
    );
    assert!(
        !std::sync::Arc::ptr_eq(&first.glossary, &after.glossary),
        "a read after a write served the rows from before it"
    );
    assert_eq!(after.glossary.len(), first.glossary.len() + 1);
}

/// An idempotent re-declare writes nothing, so it must not move the
/// version either — otherwise every restart would rebuild every context
/// for no change at all.
#[tokio::test]
async fn a_re_declare_that_writes_nothing_moves_nothing() {
    let (_dir, store) = store().await;
    let before = rctx(&store).await;
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
    let after = rctx(&store).await;
    assert_eq!(before.version, after.version);
    assert!(std::sync::Arc::ptr_eq(&before.aspects, &after.aspects));
}

/// A batched sequence lands one append per touched relation, and its
/// cross-references resolve against the batch's own view: the function
/// RETURNS an aspect that is itself still buffered when the function is
/// declared.
#[tokio::test]
async fn a_batch_lands_one_append_per_relation() {
    let (_dir, store) = store().await; // one landed append on `aspects`
    store.batch_begin();
    let Declaration::Aspect(depth) = decl(
        r#"DECLARE ASPECT depth WITH $${
            "type": "object",
            "required": ["value"],
            "properties": {"value": {"type": "number"}},
            "additionalProperties": false
        }$$ AS MEASUREMENT;"#,
    ) else {
        unreachable!()
    };
    store.declare_aspect(&depth).await.unwrap();
    let Declaration::Aspect(width) = decl(
        r#"DECLARE ASPECT width WITH $${
            "type": "object",
            "required": ["value"],
            "properties": {"value": {"type": "number"}},
            "additionalProperties": false
        }$$ AS MEASUREMENT;"#,
    ) else {
        unreachable!()
    };
    store.declare_aspect(&width).await.unwrap();
    let Declaration::Function(probe) =
        decl("DECLARE FUNCTION probe FOR GLOBAL AS $$#{}$$ RETURNS depth;")
    else {
        unreachable!()
    };
    store.declare_function(&probe).await.unwrap();
    // An identical re-declare of what already stands buffers nothing.
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
    store.batch_flush().await.unwrap();

    assert!(store.aspect("depth").await.unwrap().is_some());
    assert!(store.aspect("width").await.unwrap().is_some());
    assert!(store.function("probe", None).await.unwrap().is_some());
    let mut appends = std::collections::HashMap::new();
    for landing in store.lake().landings("glossql").await.unwrap() {
        *appends.entry(landing.table).or_insert(0) += 1;
    }
    assert_eq!(
        appends.get("aspects"),
        Some(&2),
        "the fixture's landing plus one for the whole batch: {appends:?}"
    );
    assert_eq!(appends.get("functions"), Some(&1), "{appends:?}");
}

/// A batch carrying two rows to one supersession key lands, and the
/// later row wins: inside one landing `_row_id` orders the rows, the
/// way the sequence number orders landings.
#[tokio::test]
async fn a_batch_carrying_two_rows_to_one_key_lands_the_later_one() {
    let (_dir, store) = store().await;
    store.batch_begin();
    let Declaration::Aspect(first) = decl(
        r#"DECLARE ASPECT gap WITH $${
            "type": "object",
            "required": ["value"],
            "properties": {"value": {"type": "number"}},
            "additionalProperties": false
        }$$ AS MEASUREMENT;"#,
    ) else {
        unreachable!()
    };
    store.declare_aspect(&first).await.unwrap();
    let Declaration::Aspect(second) = decl(
        r#"DECLARE ASPECT gap WITH $${
            "type": "object",
            "required": ["value"],
            "properties": {"value": {"type": "string"}},
            "additionalProperties": false
        }$$ AS MEASUREMENT;"#,
    ) else {
        unreachable!()
    };
    store.declare_aspect(&second).await.unwrap();
    store.batch_flush().await.unwrap();
    let (schema, _, _) = store
        .aspect("gap")
        .await
        .unwrap()
        .expect("the batch landed");
    assert_eq!(
        schema["properties"]["value"]["type"], "string",
        "the later row of the batch is the current one: {schema}"
    );
    // The batch is closed: the store writes directly again, and the
    // direct write supersedes the batch's.
    store.declare_aspect(&first).await.unwrap();
    let (schema, _, _) = store.aspect("gap").await.unwrap().expect("declared");
    assert_eq!(schema["properties"]["value"]["type"], "number", "{schema}");
}

/// A channel's batch is its own: two channels buffering the same
/// relation each see committed state plus their own rows, and a flush
/// on one lands for every channel.
#[tokio::test]
async fn a_channels_batch_is_its_own() {
    let (_dir, store) = store().await;
    let aspect = |name: &str| {
        let Declaration::Aspect(a) = decl(&format!(
            r#"DECLARE ASPECT {name} WITH $${{"type": "object"}}$$ AS FACT;"#
        )) else {
            unreachable!()
        };
        a
    };
    let (a, b) = (store.channel(), store.channel());
    a.batch_begin();
    b.batch_begin();
    a.declare_aspect(&aspect("from_a")).await.unwrap();
    b.declare_aspect(&aspect("from_b")).await.unwrap();
    assert!(a.aspect("from_a").await.unwrap().is_some());
    assert!(
        a.aspect("from_b").await.unwrap().is_none(),
        "b's buffer is not a's"
    );
    assert!(b.aspect("from_a").await.unwrap().is_none());
    b.batch_flush().await.unwrap();
    assert!(
        a.aspect("from_b").await.unwrap().is_some(),
        "a landed row reaches every channel"
    );
    assert!(
        store.aspect("from_a").await.unwrap().is_none(),
        "still buffered on a"
    );
    a.batch_flush().await.unwrap();
    assert!(store.aspect("from_a").await.unwrap().is_some());
}
