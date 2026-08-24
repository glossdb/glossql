//! Concurrent writers against one store relation.
//!
//! Every gloss appends to the same Iceberg table, and an Iceberg commit
//! is optimistic: the catalog's conditional update refuses whichever of
//! two writers read the same metadata second. That refusal is the format
//! working — the loser has lost nothing but its turn — so a writer that
//! reports it to the caller is reporting contention as failure. These
//! tests hold the line that concurrent writers all land, and that what
//! they wrote is all readable afterwards.

use std::sync::Arc;

use glossql_glossary::{Actor, ActorKind, Store};
use glossql_parser::{Declaration, GlossqlParser, Statement};

fn decl(sql: &str) -> Declaration {
    match GlossqlParser::parse_sql(sql)
        .expect("declaration parses")
        .remove(0)
    {
        Statement::Declare(d) => *d,
        other => panic!("not a declaration: {other:?}"),
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
    // Setup writes of its own contend too; the counts below are taken
    // after this returns, so they belong to the writers under test.
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

fn body(value: &str) -> glossql_parser::JsonBody {
    match GlossqlParser::parse_sql(&format!(
        "GLOSS unit ON orders.amount AS $${{\"value\": \"{value}\"}}$$;"
    ))
    .expect("gloss parses")
    .remove(0)
    {
        Statement::Gloss(g) => g.body,
        other => panic!("not a gloss: {other:?}"),
    }
}

/// Distinct subjects, so nothing supersedes anything: every write must
/// be readable at the end. They contend all the same, because a store
/// relation is one Iceberg table and a gloss is one commit to it.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_writers_all_land() {
    const WRITERS: usize = 24;
    let (_dir, store) = store().await;
    let store = Arc::new(store);

    let started = std::time::Instant::now();
    let conflicts_before = store.lake().conflict_count();
    let mut writing = Vec::with_capacity(WRITERS);
    for n in 0..WRITERS {
        let store = Arc::clone(&store);
        writing.push(tokio::spawn(async move {
            let actor = Actor {
                kind: ActorKind::Agent,
                id: format!("agent-{n}"),
            };
            store
                .gloss(
                    "fin",
                    &actor,
                    "unit",
                    &format!("orders.c{n}"),
                    &body("EUR"),
                    None,
                )
                .await
        }));
    }
    let mut refused = Vec::new();
    for (n, task) in writing.into_iter().enumerate() {
        if let Err(e) = task.await.expect("the writer task did not panic") {
            refused.push(format!("writer {n}: {e}"));
        }
    }
    let elapsed = started.elapsed();
    // Writers contend here — created without the retry arrangement the
    // store relations carry, seventeen of these twenty-four are refused.
    // The exhausted count is in the message rather than an assertion of
    // its own: a writer that runs out of retries is a refusal, so this
    // fires first either way, and what it needs to say is which of the
    // two it was.
    let exhausted = store.lake().conflict_count() - conflicts_before;
    assert!(
        refused.is_empty(),
        "{} of {WRITERS} concurrent writers were refused, {exhausted} of them for \
         running out of retries — if that is most of them, the commit properties the \
         relations are created with are too small for this contention:\n{}",
        refused.len(),
        refused.join("\n")
    );

    // What landed is what reads back — a retry that dropped a write
    // would pass the assertion above and fail this one.
    let rows = store.relation_rows("glossary").await.unwrap();
    assert_eq!(
        rows.len(),
        WRITERS,
        "every concurrent write is one row in the relation"
    );
    println!(
        "BENCH concurrent_writers n={WRITERS} wall_ms={}",
        elapsed.as_millis()
    );
}

/// The same subject from every writer: one slot, and the reads collapse
/// to a single winner. What must not happen is a writer being refused —
/// supersession is a read, so racing to write the same slot is legal.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_writers_on_one_slot_all_land() {
    const WRITERS: usize = 12;
    let (_dir, store) = store().await;
    let store = Arc::new(store);

    let mut writing = Vec::with_capacity(WRITERS);
    for n in 0..WRITERS {
        let store = Arc::clone(&store);
        writing.push(tokio::spawn(async move {
            let actor = Actor {
                kind: ActorKind::Agent,
                id: format!("agent-{n}"),
            };
            store
                .gloss("fin", &actor, "unit", "orders.amount", &body("EUR"), None)
                .await
        }));
    }
    for (n, task) in writing.into_iter().enumerate() {
        task.await
            .expect("the writer task did not panic")
            .unwrap_or_else(|e| panic!("writer {n} was refused: {e}"));
    }
    let rows = store.relation_rows("glossary").await.unwrap();
    assert_eq!(rows.len(), WRITERS, "history keeps every write");
}
