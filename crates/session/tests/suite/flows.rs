//! Statement flows end-to-end through the router: the shapes fixtures 01–10
//! and 13 are built from, executed in memory against `:memory:` stores. Data
//! tables are injected via `register_table` where a flow needs them —
//! recipes materialize real ones at M3.

use std::sync::Arc;

use datafusion::arrow::array::{
    Array, Float64Array, Int32Array, ListBuilder, RecordBatch, StringBuilder,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::datasource::MemTable;
use glossql_glossary::{Actor, ActorKind, Store};
use glossql_session::{Outcome, Session};

async fn session_with(actor_kind: ActorKind, id: &str, store: &Store) -> Session {
    Session::new(
        store.clone(),
        Actor {
            kind: actor_kind,
            id: id.into(),
        },
    )
    .expect("session builds")
}

async fn agent_session() -> (tempfile::TempDir, Session) {
    let (dir, store) = scratch_store().await;
    let session = session_with(ActorKind::Agent, "agent-1", &store).await;
    (dir, session)
}

async fn run(session: &Session, sql: &str) -> Vec<Outcome> {
    session
        .execute(sql)
        .await
        .unwrap_or_else(|e| panic!("`{sql}` failed: {e}"))
}

/// Land `orders(id, customer_id, amount)` and `customers(id)` as
/// fixture tables — what a relationship endpoint or a column-grain
/// gloss must find.
async fn land_orders_and_customers(session: &Session) {
    let orders = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("customer_id", DataType::Int32, false),
            Field::new("amount", DataType::Float64, false),
        ])),
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(Int32Array::from(vec![7, 7])),
            Arc::new(Float64Array::from(vec![10.0, 32.5])),
        ],
    )
    .unwrap();
    let customers = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
        vec![Arc::new(Int32Array::from(vec![7]))],
    )
    .unwrap();
    for (name, batch) in [("orders", orders), ("customers", customers)] {
        let schema = batch.schema();
        session
            .register_table(
                name,
                Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
            )
            .await
            .unwrap();
    }
}

async fn table(session: &Session, sql: &str) -> String {
    let outcomes = run(session, sql).await;
    let Some(Outcome::Rows(batches)) = outcomes.into_iter().next_back() else {
        panic!("`{sql}` produced no rows");
    };
    pretty_format_batches(&batches).unwrap().to_string()
}

const SETUP: &str = r#"
DECLARE DATASET fin SET (purpose: 'working-capital analysis');
USE fin;
DECLARE ASPECT unit WITH $${
  "type": "object", "required": ["value"],
  "properties": {"value": {"type": "string"}, "source_column": {"type": "string"}},
  "additionalProperties": false
}$$ AS FACT;
"#;

/// The engine's `information_schema` is on: one read serves the bound
/// dataset's tables and every column of them — what an agent reaches
/// for before its first DESCRIBE.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn information_schema_serves_the_mounted_schema_in_one_read() {
    let (_dir, session) = agent_session().await;
    run(&session, SETUP).await;
    land_orders_and_customers(&session).await;
    let tables = table(
        &session,
        "SELECT table_schema, table_name FROM information_schema.tables \
         WHERE table_name IN ('orders', 'customers') ORDER BY table_name;",
    )
    .await;
    assert!(
        tables.contains("customers") && tables.contains("orders"),
        "{tables}"
    );
    let columns = table(
        &session,
        "SELECT table_name, column_name, data_type FROM information_schema.columns \
         WHERE table_name = 'orders' ORDER BY ordinal_position;",
    )
    .await;
    assert!(
        columns.contains("customer_id") && columns.contains("amount"),
        "{columns}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gloss_then_read_collapsed_and_raw() {
    let (_dir, session) = agent_session().await;
    run(&session, SETUP).await;
    run(
        &session,
        r#"GLOSS unit ON orders.amount AS $${"value": "EUR"}$$;"#,
    )
    .await;

    // Unprefixed and dataset-prefixed spellings resolve to the same subject.
    let collapsed = table(
        &session,
        "SELECT subject, aspect, value FROM GLOSSARY(fin.orders.amount);",
    )
    .await;
    insta::assert_snapshot!(collapsed, @r#"
    +---------------+--------+------------------+
    | subject       | aspect | value            |
    +---------------+--------+------------------+
    | orders.amount | unit   | {"value": "EUR"} |
    +---------------+--------+------------------+
    "#);

    // `kind` is the aspect's kind; who spoke is `actor` (SPEC.md §5.3).
    let raw = table(
        &session,
        "SELECT subject, aspect, kind, actor, body FROM GLOSSARY(orders.amount, all => true);",
    )
    .await;
    insta::assert_snapshot!(raw, @r#"
    +---------------+--------+------+---------+------------------+
    | subject       | aspect | kind | actor   | body             |
    +---------------+--------+------+---------+------------------+
    | orders.amount | unit   | fact | agent-1 | {"value": "EUR"} |
    +---------------+--------+------+---------+------------------+
    "#);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_human_slot_outranks_the_agent_slot_in_collapse() {
    let (_dir, store) = scratch_store().await;
    let agent = session_with(ActorKind::Agent, "agent-1", &store).await;
    run(&agent, SETUP).await;
    run(
        &agent,
        r#"GLOSS unit ON orders.amount AS $${"value": "EUR"}$$;"#,
    )
    .await;

    let human = session_with(ActorKind::Human, "philipp", &store).await;
    run(&human, "USE fin;").await;
    run(
        &human,
        r#"GLOSS unit ON orders.amount AS $${"value": "USD"}$$;"#,
    )
    .await;

    // Precedence: human > agent > function; no detector
    // on this aspect, so nothing withholds the value, and the state says it
    // is current.
    let collapsed = table(
        &agent,
        "SELECT subject, aspect, value, state FROM GLOSSARY(orders.amount);",
    )
    .await;
    insta::assert_snapshot!(collapsed, @r#"
    +---------------+--------+------------------+---------+
    | subject       | aspect | value            | state   |
    +---------------+--------+------------------+---------+
    | orders.amount | unit   | {"value": "USD"} | current |
    +---------------+--------+------------------+---------+
    "#);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn extraction_computes_once_then_serves_the_pin() {
    let (_dir, session) = agent_session().await;
    run(&session, SETUP).await;
    run(
        &session,
        r#"DECLARE ASPECT outlier_rows WITH $${"type": "object",
             "required": ["rows"], "properties": {"rows": {"type": "array"}}}$$ AS MEASUREMENT;
           DECLARE FUNCTION outliers FOR fin AS $$SELECT [1] AS rows, 1 AS n$$
           RETURNS outlier_rows;
           DECLARE ASPECT counted_rows WITH $${"type": "object",
             "required": ["rows"], "properties": {"rows": {"type": "array"}}}$$ AS MEASUREMENT;
           DECLARE FUNCTION counted FOR fin
           AS $$SELECT [1] AS rows, count(*) AS n FROM glossary$$
           RETURNS counted_rows;"#,
    )
    .await;

    // Each compute lands one `measurements` row; a repeat serves the
    // landed row and lands nothing.
    let computes = || async { table(&session, "SELECT count(*) AS n FROM measurements;").await };
    run(&session, "SELECT outliers() FROM fin;").await;
    assert!(computes().await.contains("| 1"), "one landing");
    run(&session, "SELECT outliers() FROM fin;").await;
    assert!(
        computes().await.contains("| 1"),
        "the second run serves the standing measurement"
    );
    run(&session, "SELECT counted() FROM fin;").await;
    assert!(computes().await.contains("| 2"), "a second function lands");

    // Currency is what a measurement READ. A gloss moves the glossary
    // leg: `counted` read it and recomputes; `outliers` read nothing
    // and its answer cannot have changed, so it still serves. No
    // sweep, only a miss — and only where an input actually moved.
    run(&session, r#"GLOSS unit ON fin AS $${"value": "x"}$$;"#).await;
    run(&session, "SELECT outliers() FROM fin;").await;
    assert!(
        computes().await.contains("| 2"),
        "a body that read nothing always stands"
    );
    run(&session, "SELECT counted() FROM fin;").await;
    assert!(computes().await.contains("| 3"), "the leg it read moved");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stale_voice_is_owed_a_re_measure_until_it_runs_again() {
    let (_dir, session) = agent_session().await;
    run(&session, SETUP).await;
    run(
        &session,
        r#"DECLARE ASPECT tie WITH $${"type": "object",
             "required": ["n"], "properties": {"n": {"type": "number"}}}$$ AS MEASUREMENT;
           DECLARE FUNCTION tie_check FOR fin
           AS $$SELECT count(*) AS n, 'measured' AS note FROM glossary$$
           RETURNS tie;
           SELECT tie_check() FROM fin;"#,
    )
    .await;

    // The voice landed at this pin: nothing stands stale, nothing owed.
    let owed = || async { table(&session, "SELECT kind, subject FROM owed;").await };
    assert!(
        !owed().await.contains("re-measure"),
        "a voice at the current pin owes nothing"
    );

    // Any write moves the leg the check read; the voice serves marked,
    // and the debt derives — nobody declares it.
    run(&session, r#"GLOSS unit ON fin AS $${"value": "x"}$$;"#).await;
    let waiting = owed().await;
    assert!(waiting.contains("re-measure"), "{waiting}");
    assert!(waiting.contains("tie_check"), "{waiting}");

    // The act resolves it: the re-run lands the voice at this pin and
    // the row derives away.
    session.remeasure().await.unwrap();
    assert!(
        !owed().await.contains("re-measure"),
        "the re-run voice stands at the pin"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn attest_serves_detector_outputs_in_the_fixed_shape() {
    let (_dir, session) = agent_session().await;
    run(&session, SETUP).await;
    run(
        &session,
        r#"
        DECLARE ASPECT reconciliation WITH $${"type": "object"}$$ AS MEASUREMENT;
        DECLARE FUNCTION tb_check FOR fin AS $$SELECT 0.4 AS delta, 'measured' AS note$$
          RETURNS reconciliation;
        DECLARE FUNCTION tb_bands FOR fin AS
          $$SELECT DISTINCT subject, 'red' AS band, 0.9 AS score FROM slots$$;
        DECLARE WITNESS tb_w ON reconciliation DETECTOR tb_bands THRESHOLD 0.7;
        SELECT tb_check() FROM fin.trial_balance;
        "#,
    )
    .await;

    let attest = table(
        &session,
        "SELECT subject, aspect, witness, band, score FROM ATTEST(fin.trial_balance) WHERE band = 'red';",
    )
    .await;
    insta::assert_snapshot!(attest, @r"
    +---------------+----------------+---------+------+-------+
    | subject       | aspect         | witness | band | score |
    +---------------+----------------+---------+------+-------+
    | trial_balance | reconciliation | tb_w    | red  | 0.9   |
    +---------------+----------------+---------+------+-------+
    ");

    // The sweep form: no subject, the USE'd dataset.
    let sweep = table(&session, "SELECT subject, band FROM ATTEST();").await;
    assert!(sweep.contains("trial_balance"), "{sweep}");

    // `subject::aspect` narrows to one declared aspect.
    let narrowed = table(
        &session,
        "SELECT subject, band FROM ATTEST(fin.trial_balance::reconciliation);",
    )
    .await;
    assert!(narrowed.contains("red"), "{narrowed}");
    let e = session
        .execute("SELECT * FROM ATTEST(fin.trial_balance::nope);")
        .await
        .unwrap_err();
    assert!(e.to_string().contains("aspect"), "{e}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_broken_detector_serves_its_failure_and_spares_the_read() {
    let (_dir, session) = agent_session().await;
    run(&session, SETUP).await;
    // Two witnesses: a healthy one, and one whose detector indexes
    // `body` as a struct while an empty-object body keeps the column
    // text (nothing shreds) — the shape trigger that needs no
    // detector bug.
    run(
        &session,
        r#"
        DECLARE ASPECT reconciliation WITH $${"type": "object"}$$ AS MEASUREMENT;
        DECLARE FUNCTION tb_check FOR fin AS $$SELECT 0.4 AS delta, 'measured' AS note$$
          RETURNS reconciliation;
        DECLARE FUNCTION tb_bands FOR fin AS
          $$SELECT DISTINCT subject, 'red' AS band, 0.9 AS score FROM slots$$;
        DECLARE WITNESS tb_w ON reconciliation DETECTOR tb_bands THRESHOLD 0.7;
        DECLARE ASPECT note WITH $${"type": "object"}$$ AS FACT;
        DECLARE FUNCTION note_bands FOR fin AS
          $$SELECT subject, body['x'] AS band, 1.0 AS score FROM slots$$;
        DECLARE WITNESS note_w ON note BY (AGENT, HUMAN) DETECTOR note_bands THRESHOLD 0.7;
        SELECT tb_check() FROM fin.trial_balance;
        GLOSS note ON trial_balance AS $${}$$;
        "#,
    )
    .await;

    // The read serves: the healthy witness answers, the broken one's
    // failure is its verdict — band `error`, the text in `error`.
    let attest = table(
        &session,
        "SELECT witness, band, error FROM ATTEST(fin.trial_balance) ORDER BY witness;",
    )
    .await;
    assert!(
        attest.contains("note_w") && attest.contains("| error"),
        "{attest}"
    );
    assert!(attest.contains("note_bands"), "{attest}");
    assert!(
        attest.contains("tb_w") && attest.contains("| red"),
        "{attest}"
    );

    // An error is never a judgment: nothing is withheld — the note
    // serves, the band beside it.
    let collapsed = table(
        &session,
        "SELECT value, band, state FROM GLOSSARY(trial_balance) WHERE aspect = 'note';",
    )
    .await;
    assert!(collapsed.contains("{}"), "{collapsed}");
    assert!(collapsed.contains("error"), "{collapsed}");
    assert!(!collapsed.contains("contested"), "{collapsed}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dataset_scoped_detector_fails_as_a_verdict_on_other_datasets() {
    let (_dir, store) = scratch_store().await;
    let fin = session_with(ActorKind::Agent, "agent-1", &store).await;
    run(&fin, SETUP).await;
    run(
        &fin,
        r#"
        DECLARE ASPECT sanity WITH $${"type": "object"}$$ AS FACT;
        DECLARE FUNCTION sane FOR fin AS
          $$SELECT DISTINCT subject, 'green' AS band, 0.1 AS score FROM slots$$;
        DECLARE WITNESS sanity_w ON sanity BY (AGENT, HUMAN) DETECTOR sane THRESHOLD 0.7;
        GLOSS sanity ON orders AS $${"ok": true}$$;
        "#,
    )
    .await;
    let home = table(&fin, "SELECT band, error FROM ATTEST(fin.orders);").await;
    assert!(home.contains("green"), "{home}");

    // The witness is workspace-global; its detector carries `FOR fin`.
    // Read from another dataset holding slots on the aspect, the
    // unresolved detector is the verdict there — never a refusal.
    let ops = session_with(ActorKind::Agent, "agent-1", &store).await;
    run(
        &ops,
        r#"DECLARE DATASET ops SET (purpose: 'the other side');
           USE ops;
           GLOSS sanity ON payments AS $${"ok": true}$$;"#,
    )
    .await;
    let away = table(
        &ops,
        "SELECT witness, band, error FROM ATTEST(ops.payments);",
    )
    .await;
    assert!(
        away.contains("sanity_w") && away.contains("| error"),
        "{away}"
    );
    assert!(away.contains("sane"), "{away}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_glossary_is_a_plain_readable_relation_and_the_strike_is_parked() {
    let (_dir, session) = agent_session().await;
    run(&session, SETUP).await;
    run(
        &session,
        r#"GLOSS unit ON orders.amount AS $${"value": "EUR"}$$;"#,
    )
    .await;

    let rows = table(
        &session,
        "SELECT subject, aspect, actor_kind FROM glossary;",
    )
    .await;
    insta::assert_snapshot!(rows, @r"
    +---------------+--------+------------+
    | subject       | aspect | actor_kind |
    +---------------+--------+------------+
    | orders.amount | unit   | agent      |
    +---------------+--------+------------+
    ");

    // The strike routes, and refuses by name until iceberg-rust
    // can remove rows.
    let e = session
        .execute("DELETE FROM glossary WHERE subject = 'orders.amount' AND aspect = 'unit';")
        .await
        .unwrap_err();
    assert!(e.to_string().contains("parked"), "{e}");
    assert!(e.to_string().contains("delete write path"), "{e}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn substrate_sql_runs_against_registered_tables() {
    let (_dir, session) = agent_session().await;
    run(&session, SETUP).await;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("amount", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(Float64Array::from(vec![10.0, 32.5])),
        ],
    )
    .unwrap();
    session
        .register_table(
            "orders",
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
        )
        .await
        .unwrap();

    let rows = table(
        &session,
        "SELECT id, amount FROM orders WHERE amount > 20 ORDER BY id;",
    )
    .await;
    insta::assert_snapshot!(rows, @r"
    +----+--------+
    | id | amount |
    +----+--------+
    | 2  | 32.5   |
    +----+--------+
    ");

    // The allowlist: schema-altering SQL is
    // refused at the door — tables come from recipes.
    let err = session
        .execute("CREATE VIEW big_orders AS SELECT id FROM orders;")
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("not open for CREATE VIEW"),
        "{err}"
    );

    // DESCRIBE and EXPLAIN are reads, so they pass —
    // otherwise the only way to see a landed schema is burning a
    // diagnostic re-landing and reading arrow_typeof off a row.
    let described = table(&session, "DESCRIBE orders;").await;
    assert!(
        described.contains("amount") && described.contains("Float64"),
        "{described}"
    );
    let explained = table(&session, "EXPLAIN SELECT id FROM orders;").await;
    assert!(explained.contains("plan"), "{explained}");

    // EXPLAIN carries a statement of its own — the allowlist repeats
    // inside it rather than being walked around.
    for (sneak, refused_as) in [
        (
            "EXPLAIN INSERT INTO orders VALUES (3, 1.0);",
            "EXPLAIN INSERT",
        ),
        ("EXPLAIN SELECT 1 AS a INTO scratch;", "SELECT INTO"),
    ] {
        let e = session.execute(sneak).await.unwrap_err();
        assert!(e.to_string().contains(refused_as), "`{sneak}`: {e}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metric_metadata_reads_via_aspect_narrowing() {
    let (_dir, session) = agent_session().await;
    run(&session, SETUP).await;
    // A metric is a QUERY aspect declared on the dataset (fixture 03): its
    // metadata and SQL are one narrowed read away.
    run(
        &session,
        r#"
        DECLARE ASPECT dso WITH $${"title": "Days Sales Outstanding", "x-kind": "metric"}$$ AS QUERY;
        GLOSS dso ON fin AS $${"sql": "SELECT (sum(ar) / sum(rev)) * 30 FROM monthly_balances"}$$;
        "#,
    )
    .await;

    let narrowed = table(
        &session,
        "SELECT subject, aspect, value FROM GLOSSARY(fin::dso);",
    )
    .await;
    insta::assert_snapshot!(narrowed, @r#"
    +---------+--------+-------------------------------------------------------------------+
    | subject | aspect | value                                                             |
    +---------+--------+-------------------------------------------------------------------+
    | fin     | dso    | {"sql": "SELECT (sum(ar) / sum(rev)) * 30 FROM monthly_balances"} |
    +---------+--------+-------------------------------------------------------------------+
    "#);

    // The bare aspect name is a guided error, not a silent empty table.
    let e = session
        .execute("SELECT * FROM GLOSSARY(dso);")
        .await
        .unwrap_err();
    assert!(e.to_string().contains("subject::dso"), "{e}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reads_without_a_dataset_in_use_fail_loudly() {
    let (_dir, session) = agent_session().await;
    let e = session
        .execute("SELECT * FROM GLOSSARY();")
        .await
        .unwrap_err();
    assert!(e.to_string().contains("USE"), "{e}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gloss_on_a_pair_path_lands_under_the_relationship_subject() {
    let (_dir, session) = agent_session().await;
    run(&session, SETUP).await;
    land_orders_and_customers(&session).await;
    run(
        &session,
        r#"
        DECLARE RELATIONSHIP orders.customer_id -> customers.id;
        DECLARE ASPECT fk_note WITH $${"type": "object"}$$ AS FACT;
        GLOSS fk_note ON orders.customer_id -> customers.id AS $${"value": "2% orphaned"}$$;
        "#,
    )
    .await;
    let rows = table(
        &session,
        "SELECT subject, aspect FROM GLOSSARY(orders.customer_id -> customers.id);",
    )
    .await;
    insta::assert_snapshot!(rows, @r"
    +------------------------------------+---------+
    | subject                            | aspect  |
    +------------------------------------+---------+
    | orders.customer_id -> customers.id | fk_note |
    +------------------------------------+---------+
    ");

    // Sweeping a table picks up relationships it participates in — from
    // either side; the far endpoint's own context stays out.
    let swept = table(&session, "SELECT subject, aspect FROM GLOSSARY(orders);").await;
    assert!(swept.contains("customer_id -> customers.id"), "{swept}");
    let other_side = table(&session, "SELECT subject, aspect FROM GLOSSARY(customers);").await;
    assert!(
        other_side.contains("customer_id -> customers.id"),
        "{other_side}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_declarations_read_as_plain_relations() {
    let (_dir, session) = agent_session().await;
    run(&session, SETUP).await;
    run(
        &session,
        r#"
        DECLARE SOURCE erp SET (type: parquet, location: 'lake/erp');
        DECLARE ASPECT column_profile WITH $${"type": "object"}$$ AS MEASUREMENT;
        DECLARE ASPECT outlier_profile WITH $${"type": "object"}$$ AS MEASUREMENT;
        DECLARE FUNCTION outliers FOR GLOBAL AS $$#{}$$
          RETURNS outlier_profile;
        DECLARE FUNCTION checker FOR fin AS $$#{}$$;
        DECLARE WITNESS unit_w ON unit BY (AGENT, HUMAN);
        "#,
    )
    .await;

    // An agent lists what exists instead of being told: the declaration
    // relations answer like glossary/cache/imports do.
    let functions = table(
        &session,
        "SELECT name, scope, returns FROM functions ORDER BY name;",
    )
    .await;
    insta::assert_snapshot!(functions, @r#"
    +----------+--------+-----------------+
    | name     | scope  | returns         |
    +----------+--------+-----------------+
    | checker  | fin    |                 |
    | outliers | GLOBAL | outlier_profile |
    +----------+--------+-----------------+
    "#);

    let aspects = table(&session, "SELECT name, kind FROM aspects ORDER BY name;").await;
    insta::assert_snapshot!(aspects, @r"
    +-----------------+-------------+
    | name            | kind        |
    +-----------------+-------------+
    | column_profile  | measurement |
    | outlier_profile | measurement |
    | unit            | fact        |
    +-----------------+-------------+
    ");

    let witnesses = table(
        &session,
        "SELECT name, aspect, speakers, detector FROM witnesses;",
    )
    .await;
    assert!(witnesses.contains("unit_w"), "{witnesses}");

    let sources = table(&session, "SELECT name FROM sources;").await;
    assert!(sources.contains("erp"), "{sources}");

    // A session's first question — what datasets exist — has an answer
    // (otherwise USE-and-find-out is the only way).
    let datasets = table(&session, "SELECT name FROM datasets;").await;
    assert!(datasets.contains("fin"), "{datasets}");

    // The relations compose like any table — a WHERE clause is a sweep.
    let global = table(
        &session,
        "SELECT name FROM functions WHERE scope = 'GLOBAL';",
    )
    .await;
    assert!(
        global.contains("outliers") && !global.contains("checker"),
        "{global}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_validation_adjudicates_the_expectation_beside_the_check_voice() {
    // Fixture 16 §5 (fixture 04's ruled shape, exercised): the authored
    // expectation is a FACT gloss, the check is a function VOICE on the
    // same aspect, and the detector bands across both slots. The
    // detector's own query is the proof it saw both: green only when
    // two slots carry an outcome and exactly one carries the voice's
    // measured imbalance.
    let (_dir, session) = agent_session().await;
    run(&session, SETUP).await;
    // The aspect is ON TABLE, so its subject must be landed.
    let trial_balance = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "account",
            DataType::Int32,
            false,
        )])),
        vec![Arc::new(Int32Array::from(vec![1000]))],
    )
    .unwrap();
    session
        .register_table(
            "trial_balance",
            Arc::new(MemTable::try_new(trial_balance.schema(), vec![vec![trial_balance]]).unwrap()),
        )
        .await
        .unwrap();
    run(
        &session,
        r##"
        DECLARE ASPECT journal_balanced WITH $${
          "type": "object", "required": ["outcome"],
          "properties": {"outcome": {"type": "string"}, "tolerance": {"type": "number"}}
        }$$ AS FACT ON TABLE;
        GLOSS journal_balanced ON fin.trial_balance AS $${"outcome": "debits equal credits, exactly", "tolerance": 0.0}$$;
        DECLARE FUNCTION journal_check FOR fin AS
          $$SELECT 'measured: debits equal credits' AS outcome, 0.0 AS imbalance$$
          RETURNS journal_balanced;
        DECLARE FUNCTION framework_bands FOR fin AS $$
          SELECT subject,
                 CASE WHEN count(*) FILTER (WHERE body['outcome'] IS NOT NULL) = 2
                       AND count(body['imbalance']) = 1
                      THEN 'green' ELSE 'red' END AS band,
                 0.0 AS score
          FROM slots GROUP BY subject
        $$;
        DECLARE WITNESS journal_w ON journal_balanced BY (AGENT, HUMAN)
          DETECTOR framework_bands THRESHOLD 0.5;
        SELECT journal_check() FROM fin.trial_balance;
        "##,
    )
    .await;

    let attest = table(
        &session,
        "SELECT band, score FROM ATTEST(fin.trial_balance::journal_balanced);",
    )
    .await;
    assert!(attest.contains("green"), "{attest}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_grounding_admits_and_serves_its_sql_back() {
    // Fixture 16 §2: a concept grounds as a grain-free extract; the read
    // serves the SQL, running it is the reader's act.
    let (_dir, session) = agent_session().await;
    run(&session, SETUP).await;
    run(
        &session,
        r##"
        DECLARE ASPECT revenue WITH $${"title": "Revenue"}$$ AS QUERY ON DATASET;
        GLOSS revenue ON fin AS $${
          "sql": "SELECT e.date, l.credit - l.debit AS value FROM journal_lines l JOIN journal_entries e ON l.entry_id = e.entry_id",
          "assumptions": [{"assumption": "grain-preserving join", "basis": "relationship glosses"}]
        }$$;
        "##,
    )
    .await;
    let served = table(
        &session,
        "SELECT value FROM GLOSSARY(fin::revenue) WHERE state = 'current';",
    )
    .await;
    assert!(served.contains("l.credit - l.debit"), "{served}");

    // The standard grounding schema is the gate: a body without `sql`
    // is refused however plausible it looks.
    let e = session
        .execute(r#"GLOSS revenue ON fin AS $${"query": "SELECT 1"}$$;"#)
        .await
        .unwrap_err();
    assert!(e.to_string().contains("grounding"), "{e}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_serve_door_runs_the_current_grounding() {
    // Fixture 16 §6; `read.` is the one generic serving prefix over
    // every QUERY gloss:
    // `read.<aspect>()` expands the collapsed current QUERY grounding as
    // an ordinary relation — the reader composes around it, the pinned
    // definition is what runs.
    let (_dir, store) = scratch_store().await;
    let agent = session_with(ActorKind::Agent, "agent-1", &store).await;
    run(&agent, SETUP).await;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("amount", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(Float64Array::from(vec![10.0, 32.5])),
        ],
    )
    .unwrap();
    agent
        .register_table(
            "orders",
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
        )
        .await
        .unwrap();

    run(
        &agent,
        r##"
        DECLARE ASPECT revenue WITH $${"title": "Revenue"}$$ AS QUERY ON DATASET;
        DECLARE ASPECT doubled WITH $${"title": "Doubled"}$$ AS QUERY ON DATASET;
        GLOSS revenue ON fin AS $${"sql": "SELECT amount AS value FROM orders"}$$;
        GLOSS doubled ON fin AS $${"sql": "SELECT value * 2 AS value FROM read.revenue()"}$$;
        "##,
    )
    .await;

    let total = table(&agent, "SELECT sum(value) AS total FROM read.revenue();").await;
    assert!(total.contains("42.5"), "{total}");
    // Filters ride WHERE, composed around the expansion.
    let filtered = table(
        &agent,
        "SELECT sum(value) AS total FROM read.revenue() WHERE value > 20;",
    )
    .await;
    assert!(filtered.contains("32.5"), "{filtered}");
    // A recorded evaluation composes from sibling metrics — nesting is
    // the formula composition, done by the engine.
    let doubled = table(&agent, "SELECT sum(value) AS total FROM read.doubled();").await;
    assert!(doubled.contains("85"), "{doubled}");

    // The human pin supersedes, and propagates through composition.
    let human = session_with(ActorKind::Human, "philipp", &store).await;
    run(
        &human,
        r##"USE fin;
        GLOSS revenue ON fin AS $${"sql": "SELECT amount * 2 AS value FROM orders"}$$;"##,
    )
    .await;
    let pinned = table(&agent, "SELECT sum(value) AS total FROM read.revenue();").await;
    assert!(pinned.contains("85"), "{pinned}");
    let repinned = table(&agent, "SELECT sum(value) AS total FROM read.doubled();").await;
    assert!(repinned.contains("170"), "{repinned}");

    // A grounding that reaches itself errors naming the loop.
    run(
        &agent,
        r##"
        DECLARE ASPECT looping WITH $${"title": "Loop"}$$ AS QUERY ON DATASET;
        GLOSS looping ON fin AS $${"sql": "SELECT value FROM read.looping()"}$$;
        "##,
    )
    .await;
    let e = agent
        .execute("SELECT * FROM read.looping();")
        .await
        .unwrap_err();
    // Keys are door-prefixed since the guard covers whatif./misfit.
    // too — a mixed cycle names each door on the path.
    assert!(
        e.to_string()
            .contains("read cycle: read.looping -> read.looping"),
        "{e}"
    );

    // The refusals name what the reader should do instead.
    for (sql, said) in [
        ("SELECT * FROM read.nothing();", "no aspect"),
        ("SELECT * FROM read.unit();", "GLOSSARY"),
        ("SELECT * FROM read.revenue(1);", "takes no arguments"),
    ] {
        let e = agent.execute(sql).await.unwrap_err();
        assert!(e.to_string().contains(said), "`{sql}`: {e}");
    }
    run(
        &agent,
        r#"DECLARE ASPECT dso WITH $${"title": "DSO"}$$ AS QUERY ON DATASET;"#,
    )
    .await;
    let e = agent
        .execute("SELECT * FROM read.dso();")
        .await
        .unwrap_err();
    assert!(e.to_string().contains("no current grounding"), "{e}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn witnesses_sharing_a_detector_hold_their_own_verdicts() {
    // The defect class: a verdict cached under (subject,
    // function), so the first witness to compute answered for every other
    // witness on the same detector — and sharing one is the shipped idiom
    // (role, behavior and unit all band through `slot_entropy`). Here
    // `alpha` is contested and `beta` is not; each must say so itself.
    let (_dir, store) = scratch_store().await;
    let agent = session_with(ActorKind::Agent, "agent-1", &store).await;
    run(&agent, SETUP).await;
    land_orders_and_customers(&agent).await;
    // A detector that actually reads its slots: one slot agrees with
    // itself, two disagree. What it answers therefore depends on the
    // witness it was called for — which is the point of this test.
    run(
        &agent,
        r#"
        DECLARE ASPECT alpha WITH $${"type": "object"}$$ AS FACT ON COLUMN;
        DECLARE ASPECT beta WITH $${"type": "object"}$$ AS FACT ON COLUMN;
        DECLARE FUNCTION slot_bands FOR fin AS $$
          SELECT subject,
                 CASE WHEN count(*) > 1 THEN 'red' ELSE 'green' END AS band,
                 CASE WHEN count(*) > 1 THEN 1.0 ELSE 0.0 END AS score
          FROM slots GROUP BY subject
        $$;
        DECLARE WITNESS alpha_w ON alpha BY (AGENT, HUMAN) DETECTOR slot_bands THRESHOLD 0.5;
        DECLARE WITNESS beta_w ON beta BY (AGENT, HUMAN) DETECTOR slot_bands THRESHOLD 0.5;
        GLOSS alpha ON orders.amount AS $${"reading": "agent's"}$$;
        GLOSS beta ON orders.amount AS $${"reading": "uncontested"}$$;
        "#,
    )
    .await;

    // The human disputes `alpha` only.
    let human = session_with(ActorKind::Human, "philipp", &store).await;
    run(&human, "USE fin;").await;
    run(
        &human,
        r#"GLOSS alpha ON orders.amount AS $${"reading": "human's, and different"}$$;"#,
    )
    .await;

    let attested = table(
        &agent,
        "SELECT aspect, witness, band, score FROM ATTEST(orders.amount) ORDER BY aspect;",
    )
    .await;
    insta::assert_snapshot!(attested, @r"
    +--------+---------+-------+-------+
    | aspect | witness | band  | score |
    +--------+---------+-------+-------+
    | alpha  | alpha_w | red   | 1.0   |
    | beta   | beta_w  | green | 0.0   |
    +--------+---------+-------+-------+
    ");

    // And the collapse follows the verdict that belongs to each aspect:
    // the disputed value is withheld, the undisputed one is served.
    let collapsed = table(
        &agent,
        "SELECT aspect, state, value FROM GLOSSARY(orders.amount) ORDER BY aspect;",
    )
    .await;
    insta::assert_snapshot!(collapsed, @r#"
    +--------+-----------+----------------------------+
    | aspect | state     | value                      |
    +--------+-----------+----------------------------+
    | alpha  | contested |                            |
    | beta   | current   | {"reading": "uncontested"} |
    +--------+-----------+----------------------------+
    "#);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn select_into_is_not_a_way_to_make_a_table() {
    // `SELECT … INTO t` parses as a Query and plans as CREATE MEMORY
    // TABLE, so no check keyed on the statement variant can see it. The
    // plan is what is verified, and it is verified before anything runs —
    // minting happens at execution. Tables come from recipes.
    let (_dir, session) = agent_session().await;
    run(&session, SETUP).await;
    let e = session
        .execute("SELECT 1 AS a INTO scratch;")
        .await
        .unwrap_err();
    assert!(e.to_string().contains("SELECT INTO"), "{e}");
    assert!(
        session.execute("SELECT * FROM scratch;").await.is_err(),
        "nothing was created"
    );

    // A CTE is a query like any other and may carry the spelling; the
    // plan check reaches it because the CTE is part of the plan.
    let e = session
        .execute("WITH x AS (SELECT 1 AS a INTO sneak_cte) SELECT * FROM x;")
        .await
        .unwrap_err();
    assert!(e.to_string().contains("SELECT INTO"), "{e}");

    // A derived table in FROM nests its CreateMemoryTable inside the plan
    // (datafusion-sql query.rs). Refused by name like the others: the
    // verification walks subqueries, so every nesting is one walk rather
    // than one arm per spelling.
    let e = session
        .execute("SELECT * FROM (SELECT 1 AS a INTO sneak_sub) t;")
        .await
        .unwrap_err();
    assert!(e.to_string().contains("SELECT INTO"), "{e}");
    for made in ["SELECT * FROM sneak_cte;", "SELECT * FROM sneak_sub;"] {
        assert!(
            session.execute(made).await.is_err(),
            "nothing was created: {made}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn select_into_is_refused_on_the_streaming_path_too() {
    // The streaming door plans through the same funnel as the execute
    // path, so it inherits the same refusal — where a check per entry
    // point had to be repeated at each.
    let (_dir, session) = agent_session().await;
    run(&session, SETUP).await;
    let e = session
        .query_stream("SELECT 1 AS a INTO scratch_stream;")
        .await
        .err()
        .expect("refused");
    assert!(e.to_string().contains("SELECT INTO"), "{e}");
    assert!(
        session
            .execute("SELECT * FROM scratch_stream;")
            .await
            .is_err(),
        "nothing was created"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_self_referential_frame_errors_instead_of_recursing() {
    // The `read.` door has guarded expansion cycles since it landed; the
    // `whatif.` and `misfit.` doors re-enter the planner through the SQL
    // they replay and without a guard a self-referential body recurses
    // to stack overflow.
    let (_dir, session) = agent_session().await;
    run(&session, SETUP).await;
    run(
        &session,
        r#"DECLARE ASPECT selfframe WITH $${"type": "object"}$$ AS QUERY ON DATASET;
           GLOSS selfframe ON fin AS $${"sql": "SELECT * FROM misfit.selfframe()", "assumptions": []}$$;"#,
    )
    .await;
    let e = session
        .execute("SELECT * FROM misfit.selfframe();")
        .await
        .unwrap_err();
    assert!(e.to_string().contains("read cycle"), "{e}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_source_conventions_gloss_reads_from_another_dataset() {
    // `AS FACT ON SOURCE`: a declared source's name
    // is a subject, and its slots serve in every dataset — the deposit
    // the next dataset reads before probing.
    let (_dir, session) = agent_session().await;
    run(&session, SETUP).await;
    run(
        &session,
        r#"DECLARE SOURCE glos_erp SET (type: parquet, location: 'lake/erp');
           DECLARE ASPECT conventions WITH $${"type": "object"}$$ AS FACT ON SOURCE;
           GLOSS conventions ON glos_erp AS $${"placeholder_date": "1900-01-01"}$$;
           DECLARE DATASET fin2 SET (purpose: 'second dataset, same workspace');
           USE fin2;"#,
    )
    .await;
    let served = table(
        &session,
        "SELECT subject, state, value FROM GLOSSARY(glos_erp) WHERE aspect = 'conventions';",
    )
    .await;
    assert!(served.contains("current"), "{served}");
    assert!(served.contains("1900-01-01"), "{served}");

    // The grain gate holds: a table-shaped subject that names no source
    // is refused.
    let e = session
        .execute(r#"GLOSS conventions ON orders AS $${"placeholder_date": "x"}$$;"#)
        .await
        .unwrap_err();
    assert!(e.to_string().contains("ON SOURCE"), "{e}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_cube_reads_compute_under_the_declared_cube_aspect() {
    let (_dir, session) = agent_session().await;
    run(
        &session,
        "DECLARE DATASET fin SET (purpose: 'metrics'); USE fin;",
    )
    .await;

    // No grounding, no rows and no facts — honest absence; nothing
    // computes, nothing lands (the cube is a query result, never a
    // measurement). The scripts suite holds the cube's own semantics.
    let empty = table(
        &session,
        "SELECT count(*) AS n FROM metric_series(grain => 'month');",
    )
    .await;
    assert!(empty.contains("| 0"), "{empty}");
    let facts = table(&session, "SELECT count(*) AS n FROM metric_axes();").await;
    assert!(facts.contains("| 0"), "{facts}");

    // The `cube` aspect is the declared contract a cube computes under
    // — the floor and the ladder; the first metric that needs building
    // without it is refused with the road out (the kit ships it).
    run(
        &session,
        r#"DECLARE ASPECT dso WITH $${"title": "DSO"}$$ AS QUERY ON DATASET;
           GLOSS dso ON fin AS $${"sql": "SELECT 1 AS v"}$$;"#,
    )
    .await;
    let e = session
        .execute("SELECT count(*) FROM metric_series();")
        .await
        .unwrap_err();
    assert!(e.to_string().contains("no `cube` aspect"), "{e}");
    run(
        &session,
        r#"DECLARE ASPECT cube WITH $${"type": "object", "properties": {
             "resolution": {"default": "day"},
             "windows": {"type": "object", "properties": {"month": {"default": "48 months"}}}}}$$
           AS FACT ON DATASET;"#,
    )
    .await;
    // A grounding that cannot serve abstains in its fact row, with
    // the reason, and serves no cells.
    let facts = table(
        &session,
        "SELECT metric, applicable, reason FROM metric_axes();",
    )
    .await;
    assert!(
        facts.contains("dso") && facts.contains("false") && facts.contains("no value column"),
        "{facts}"
    );
    let empty = table(&session, "SELECT count(*) AS n FROM metric_series();").await;
    assert!(empty.contains("| 0"), "{empty}");

    // The one argument is the grain; filters ride WHERE.
    let e = session
        .execute("SELECT * FROM metric_series('revenue');")
        .await
        .unwrap_err();
    assert!(e.to_string().contains("grain =>"), "{e}");
    let e = session
        .execute("SELECT * FROM metric_series(grain => 'fortnight');")
        .await
        .unwrap_err();
    assert!(
        e.to_string()
            .contains("one of minute, hour, day, week, month, quarter, year"),
        "{e}"
    );
    let e = session
        .execute("SELECT * FROM metric_axes('revenue');")
        .await
        .unwrap_err();
    assert!(e.to_string().contains("no arguments"), "{e}");
}

/// A grounding's write answers with its fact from the channel that can
/// judge it. Bound to nothing — a dataset-grain gloss resolves its
/// subject without a `USE` — the grounding's table names would not
/// resolve here, so the row abstains and names the `USE` that judges
/// it; the gloss lands either way.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_grounding_written_unbound_lands_and_names_where_it_is_judged() {
    let (_dir, session) = agent_session().await;
    run(
        &session,
        r#"DECLARE DATASET fin SET (purpose: 'metrics');
           DECLARE ASPECT dso WITH $${"title": "DSO"}$$ AS QUERY ON DATASET;"#,
    )
    .await;
    let row = table(
        &session,
        r#"GLOSS dso ON fin AS $${"sql": "SELECT d AS date, 1.0 AS value FROM t"}$$;"#,
    )
    .await;
    assert!(row.contains("| dso "), "{row}");
    assert!(
        row.contains("| false ") && row.contains("bound to no dataset") && row.contains("USE fin;"),
        "{row}"
    );
    let landed = table(
        &session,
        "SELECT count(*) AS n FROM glossary WHERE aspect = 'dso';",
    )
    .await;
    assert!(landed.contains("| 1 "), "{landed}");
}

/// A measurement is a query (stage 5, §7e): the skill's own
/// quick-validation flow, end to end — declare the aspect and a
/// SQL-bodied function, extract, read the landed value back.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_measurement_body_is_sql() {
    let (_dir, session) = agent_session().await;
    run(&session, SETUP).await;
    let schema = Arc::new(Schema::new(vec![
        Field::new("billed", DataType::Float64, true),
        Field::new("settled", DataType::Float64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Float64Array::from(vec![100.0, 200.0, 50.0])),
            Arc::new(Float64Array::from(vec![100.0, 150.0, 50.0])),
        ],
    )
    .unwrap();
    session
        .register_table(
            "settlements",
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
        )
        .await
        .unwrap();

    run(
        &session,
        r#"DECLARE ASPECT ar_check WITH $${"type": "object",
             "required": ["outcome", "breach_rate"],
             "properties": {"outcome": {"type": "string"},
                            "breach_rate": {"type": "number"}}}$$ AS MEASUREMENT;
           DECLARE FUNCTION ar_settles_in_full FOR fin AS $$
             SELECT
               'a receipt settles its invoice in full' AS outcome,
               CASE WHEN count(*) = 0 THEN 0.0
                    ELSE CAST(count(*) FILTER (WHERE settled < billed) AS DOUBLE) / count(*)
               END AS breach_rate
             FROM settlements
           $$ RETURNS ar_check;"#,
    )
    .await;
    // The outcome says whether the call computed or served: the first
    // run computes; a repeat at the same pin serves the recorded row,
    // its `computed_at` the first run's.
    let first = table(&session, "SELECT ar_settles_in_full() FROM settlements;").await;
    assert!(
        first.contains("| true ") && first.contains("computed"),
        "{first}"
    );
    let again = table(&session, "SELECT ar_settles_in_full() FROM settlements;").await;
    assert!(again.contains("| false "), "{again}");

    let read = table(
        &session,
        "SELECT aspect, value FROM GLOSSARY(settlements::ar_check);",
    )
    .await;
    assert!(
        read.contains("0.3333333333333333") && read.contains("settles its invoice"),
        "{read}"
    );
}

/// `subject_column($subject)` — the column-grain primitive: the body is
/// declared once, the subject varies per extraction, and a one-column
/// one-row result lands as the bare value.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_subject_binds_through_the_column_door() {
    let (_dir, session) = agent_session().await;
    run(&session, SETUP).await;
    let schema = Arc::new(Schema::new(vec![Field::new(
        "billed",
        DataType::Float64,
        true,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Float64Array::from(vec![
            Some(100.0),
            None,
            Some(50.0),
        ]))],
    )
    .unwrap();
    session
        .register_table(
            "settlements",
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
        )
        .await
        .unwrap();

    run(
        &session,
        r#"DECLARE ASPECT filled WITH $${"type": "integer"}$$ AS MEASUREMENT;
           DECLARE FUNCTION filled_count FOR fin AS $$
             SELECT count(v) FROM subject_column($subject)
           $$ RETURNS filled;
           SELECT filled_count() FROM settlements.billed;"#,
    )
    .await;
    let read = table(
        &session,
        "SELECT subject, value FROM GLOSSARY(settlements.billed::filled);",
    )
    .await;
    assert!(read.contains("| 2"), "the two non-null cells: {read}");

    // A malformed argument is refused with the door's own sentence.
    let e = session
        .execute("SELECT * FROM subject_column(billed);")
        .await
        .unwrap_err();
    assert!(e.to_string().contains("one quoted subject"), "{e}");
}

/// A CTE shadows a same-named table — SQL's precedence. The planner seam
/// runs before DataFusion's own CTE lookup, so without declining these
/// names the pin arm (a landed table) and the batch arm (a store
/// relation) would both capture them silently.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cte_shadows_a_same_named_table() {
    let (_dir, session) = agent_session().await;
    run(&session, SETUP).await;
    let schema = Arc::new(Schema::new(vec![Field::new(
        "amount",
        DataType::Float64,
        true,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Float64Array::from(vec![1.0, 2.0]))],
    )
    .unwrap();
    session
        .register_table(
            "cells",
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
        )
        .await
        .unwrap();

    // The landed `cells` has no `marker`; only the CTE serves this.
    let read = table(
        &session,
        "WITH cells AS (SELECT 42 AS marker) SELECT marker FROM cells;",
    )
    .await;
    assert!(read.contains("42"), "{read}");

    // Same precedence over a store relation.
    let read = table(
        &session,
        "WITH functions AS (SELECT 7 AS seven) SELECT seven FROM functions;",
    )
    .await;
    assert!(read.contains("| 7"), "{read}");

    // And under a spelling the engine folds. An unquoted identifier is
    // case-insensitive to DataFusion, so `CELLS` and `cells` are one
    // name to the planner; a seam that decides shadowing by its own fold
    // would decline one spelling and capture the other, and the capture
    // is silent — the pinned table answers with plausible rows.
    let read = table(
        &session,
        "WITH CELLS AS (SELECT 43 AS marker) SELECT marker FROM cells;",
    )
    .await;
    assert!(read.contains("43"), "{read}");
    let read = table(
        &session,
        "WITH cells AS (SELECT 44 AS marker) SELECT marker FROM CELLS;",
    )
    .await;
    assert!(read.contains("44"), "{read}");
}

/// A shipped read answers to the name however it is cased, because
/// DataFusion folds an unquoted identifier and the language's own words
/// are read the same way as any other.
///
/// Two seams have to agree on it: the pre-pass resolves the door and
/// keys the plan by this name, and the planner looks the plan up by the
/// name it reads. When only one of them folded, the pre-pass never saw
/// `GLOSSARY` as a door, and planning refused a name it had itself
/// declined to resolve.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_shipped_read_answers_however_its_name_is_cased() {
    let (_dir, session) = agent_session().await;
    run(&session, SETUP).await;
    for spelling in ["glossary", "GLOSSARY", "Glossary"] {
        let sql = format!("SELECT subject FROM {spelling} LIMIT 1;");
        let read = table(&session, &sql).await;
        assert!(read.contains("subject"), "{spelling}: {read}");
    }
    // Quoted is exact, which is also DataFusion's rule: this one is not
    // the shipped read and there is no such table.
    let e = session
        .execute("SELECT subject FROM \"GLOSSARY\" LIMIT 1;")
        .await
        .unwrap_err();
    assert!(
        e.to_string().contains("GLOSSARY"),
        "a quoted name is a different name: {e}"
    );
}

/// A measurement landed on one channel is visible on every other the
/// moment it commits. The pin covers inputs only, so a landing moves no
/// pin — a cached context checked by pin alone would keep serving the
/// view from before the landing on every channel but the one that
/// computed it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_landing_reaches_a_channel_that_already_read() {
    let (_dir, store) = scratch_store().await;
    let agent = session_with(ActorKind::Agent, "agent-1", &store).await;
    let human = session_with(ActorKind::Human, "phil", &store).await;
    run(&agent, SETUP).await;
    let schema = Arc::new(Schema::new(vec![
        Field::new("billed", DataType::Float64, true),
        Field::new("settled", DataType::Float64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Float64Array::from(vec![100.0, 200.0, 50.0])),
            Arc::new(Float64Array::from(vec![100.0, 150.0, 50.0])),
        ],
    )
    .unwrap();
    agent
        .register_table(
            "settlements",
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
        )
        .await
        .unwrap();
    run(
        &agent,
        r#"DECLARE ASPECT ar_check WITH $${"type": "object",
             "required": ["outcome", "breach_rate"],
             "properties": {"outcome": {"type": "string"},
                            "breach_rate": {"type": "number"}}}$$ AS MEASUREMENT;
           DECLARE FUNCTION ar_settles_in_full FOR fin AS $$
             SELECT 'short receipts counted' AS outcome,
               CAST(count(*) FILTER (WHERE settled < billed) AS DOUBLE) / count(*) AS breach_rate
             FROM settlements
           $$ RETURNS ar_check;"#,
    )
    .await;

    // The other channel reads first, so its context caches at the
    // current pin — before anything is measured.
    run(&human, "USE fin;").await;
    let before = table(
        &human,
        "SELECT aspect, state FROM GLOSSARY(settlements::ar_check);",
    )
    .await;
    assert!(before.contains("unassessed"), "{before}");

    // The agent's channel lands the measurement; the pin does not move.
    run(&agent, "SELECT ar_settles_in_full() FROM settlements;").await;

    let after = table(
        &human,
        "SELECT aspect, value FROM GLOSSARY(settlements::ar_check);",
    )
    .await;
    assert!(
        after.contains("0.3333333333333333"),
        "the landing must reach the channel that cached first: {after}"
    );
}

/// A store over its own throwaway lake; hold the dir for the test's life.
async fn scratch_store() -> (tempfile::TempDir, Store) {
    let dir = tempfile::tempdir().unwrap();
    let lake = glossql_catalog::Lake::open(
        &dir.path().join("catalog.sqlite"),
        &dir.path().join("warehouse"),
    )
    .await
    .unwrap();
    let store = Store::open(lake).await.unwrap();
    (dir, store)
}

/// A landing that failed leaves its shape behind, and the shape is
/// still offered for assessment.
///
/// `land` creates the table through the mounted schema and only then
/// commits the batches, so a commit that refuses leaves a table the
/// catalog holds with a schema and no snapshot. Nothing else in the
/// language produces one — every other route writes as it creates.
///
/// It is where the two halves of the catalog walk part company: the pin
/// wants a snapshot and finds none, so the table adds no part to it,
/// while the grid wants the columns and finds all of them. A walk that
/// carried the snapshot's absence into the columns — or dropped the
/// table for having no snapshot — reports nothing here.
///
/// What it does not hold is which schema the columns come from. The
/// static provider reads `current_schema()` when there is no snapshot to
/// resolve, so the two agree on exactly this shape; they diverge only
/// after a commit that changes a schema without landing, which nothing
/// in the language does today.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_landing_that_failed_still_offers_its_columns_for_assessment() {
    let (_dir, session) = agent_session().await;
    run(&session, SETUP).await;

    // No rows, so the commit has no data file to write and refuses —
    // after the table itself was created.
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("amount", DataType::Float64, false),
    ]));
    let refused = session
        .register_table(
            "invoices",
            Arc::new(
                MemTable::try_new(
                    Arc::clone(&schema),
                    vec![vec![RecordBatch::new_empty(schema)]],
                )
                .unwrap(),
            ),
        )
        .await
        .expect_err("an empty landing has no data file to commit");
    assert!(
        refused.to_string().contains("manifest"),
        "the refusal should come from the commit, not the create: {refused}"
    );

    // Only a witnessed aspect carries a backlog — an unassessed row is
    // an aspect someone is expected to speak to.
    run(
        &session,
        "DECLARE WITNESS unit_w ON unit BY (AGENT, HUMAN);",
    )
    .await;

    let grid = table(
        &session,
        "SELECT subject, aspect, state FROM GLOSSARY(invoices) ORDER BY subject;",
    )
    .await;
    for subject in ["invoices", "invoices.id", "invoices.amount"] {
        assert!(
            grid.contains(subject),
            "`{subject}` should stand unassessed, got:\n{grid}"
        );
    }
    assert!(grid.contains("unassessed"), "{grid}");
}

/// The catalog is walked once per statement, however many plans the
/// statement builds under it.
///
/// A walk loads every table of the dataset and parses its metadata, and
/// the doors build plans freely: `whatif`, `misfit`, `search` and the
/// cube each resolve their own SQL, and each resolution used to start a
/// fresh walk. The count is what says the statement pays for one.
///
/// The second half is the constraint that keeps it honest: the walk is
/// the statement's, not the sequence's. A statement that lands must be
/// visible to the next one, so the count rises again rather than being
/// served from what the landing invalidated.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_statement_walks_the_catalog_once_however_many_plans_it_builds() {
    // The store's lake, kept so the walks can be counted — the session
    // below is built on the same one, and the counter is shared.
    let (_dir, store) = scratch_store().await;
    let lake = store.lake();
    let session = session_with(ActorKind::Agent, "agent-1", &store).await;
    run(&session, SETUP).await;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("amount", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(Float64Array::from(vec![10.0, 32.5])),
        ],
    )
    .unwrap();
    session
        .register_table(
            "orders",
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
        )
        .await
        .unwrap();

    // One statement that reads a door and scans a table: the pre-pass
    // pins the tables, the door derives its context, and before the
    // walk was held both of those went to the catalog on their own.
    let before = lake.walk_count();
    run(
        &session,
        "SELECT count(*) FROM orders; SELECT subject FROM GLOSSARY(orders);",
    )
    .await;
    let after = lake.walk_count();
    assert_eq!(
        after - before,
        2,
        "two statements, one walk each — got {} walks",
        after - before
    );

    // And a single statement doing both stays at one.
    let before = lake.walk_count();
    run(
        &session,
        "SELECT (SELECT count(*) FROM orders) AS n, (SELECT count(*) FROM GLOSSARY(orders)) AS g;",
    )
    .await;
    assert_eq!(lake.walk_count() - before, 1, "one statement, one walk");
}

/// A source's slots are workspace rows: the conventions read runs
/// with nothing bound — the state a fresh channel, and every call on
/// the MCP door, is in — and serves what an earlier onboarding banked.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_source_conventions_gloss_reads_with_no_dataset_bound() {
    let (_dir, store) = scratch_store().await;
    let bound = session_with(ActorKind::Agent, "agent-1", &store).await;
    run(&bound, SETUP).await;
    run(
        &bound,
        r#"DECLARE SOURCE glos_erp SET (type: parquet, location: 'lake/erp');
           DECLARE ASPECT conventions WITH $${"type": "object"}$$ AS FACT ON SOURCE;
           GLOSS conventions ON glos_erp AS $${"placeholder_date": "1900-01-01"}$$;"#,
    )
    .await;

    let unbound = session_with(ActorKind::Agent, "agent-1", &store).await;
    let served = table(
        &unbound,
        "SELECT subject, state, value FROM GLOSSARY(glos_erp) WHERE aspect = 'conventions';",
    )
    .await;
    assert!(served.contains("current"), "{served}");
    assert!(served.contains("1900-01-01"), "{served}");
    // The verdict read over the same subject runs too — nothing to
    // adjudicate, nothing refused.
    run(&unbound, "SELECT subject, band FROM ATTEST(glos_erp);").await;
}

/// A source's files are readable from inside the language: the
/// listing runs through the engine's object store under the source's
/// location, subdirectories included, so a recipe author learns the
/// names through the same door that lands them. A relational source
/// has none and says so.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_sources_files_list_from_inside_the_language() {
    let (dir, session) = agent_session().await;
    let root = dir.path().join("exports");
    std::fs::create_dir_all(root.join("sub")).unwrap();
    std::fs::write(root.join("a.csv"), "x\n1\n").unwrap();
    std::fs::write(root.join("sub/b.csv"), "y\n22\n").unwrap();
    run(
        &session,
        &format!(
            "DECLARE SOURCE files SET (type: csv, location: '{}');",
            root.display()
        ),
    )
    .await;
    let listed = table(
        &session,
        "SELECT path, size FROM source_files('files') ORDER BY path;",
    )
    .await;
    assert!(
        listed.contains("| a.csv ") && listed.contains("| sub/b.csv "),
        "{listed}"
    );
    assert!(
        listed.contains("| 4 ") && listed.contains("| 5 "),
        "{listed}"
    );

    run(
        &session,
        "DECLARE SOURCE crm SET (type: relational_db, location: 'postgres://crm/prod', \
         driver: 'adbc_driver_postgresql');",
    )
    .await;
    let e = session
        .execute("SELECT path FROM source_files('crm');")
        .await
        .unwrap_err();
    assert!(e.to_string().contains("relational"), "{e}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_relationship_endpoint_must_be_a_landed_column() {
    let (_dir, session) = agent_session().await;
    run(&session, SETUP).await;
    land_orders_and_customers(&session).await;

    // The refusal names the road out: the table's columns, or the
    // dataset's tables.
    let e = session
        .execute("DECLARE RELATIONSHIP orders.nope -> customers.id;")
        .await
        .unwrap_err()
        .to_string();
    assert!(e.contains("`orders` has no column `nope`"), "{e}");
    assert!(e.contains("columns: id, customer_id, amount"), "{e}");
    let e = session
        .execute("DECLARE RELATIONSHIP orders.customer_id -> parties.id;")
        .await
        .unwrap_err()
        .to_string();
    assert!(
        e.contains("`parties` is not a landed table in `fin`"),
        "{e}"
    );
    assert!(e.contains("tables: customers, orders"), "{e}");
    // A tuple endpoint checks every column of the tuple.
    let e = session
        .execute("DECLARE RELATIONSHIP orders.(customer_id, region) -> customers.(id, region);")
        .await
        .unwrap_err()
        .to_string();
    assert!(e.contains("`orders` has no column `region`"), "{e}");

    run(
        &session,
        "DECLARE RELATIONSHIP orders.customer_id -> customers.id;",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_table_or_column_grain_gloss_must_find_its_subject() {
    let (_dir, session) = agent_session().await;
    run(&session, SETUP).await;
    land_orders_and_customers(&session).await;
    run(
        &session,
        r#"DECLARE ASPECT role WITH $${"type": "object"}$$ AS FACT ON COLUMN;
           DECLARE ASPECT entity WITH $${"type": "object"}$$ AS FACT ON TABLE, COLUMN;"#,
    )
    .await;

    let e = session
        .execute(r#"GLOSS role ON orders.amt AS $${"value": "measure"}$$;"#)
        .await
        .unwrap_err()
        .to_string();
    assert!(e.contains("`orders` has no column `amt`"), "{e}");
    let e = session
        .execute(r#"GLOSS entity ON invoices AS $${"value": "x"}$$;"#)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        e.contains("`invoices` is not a landed table in `fin`"),
        "{e}"
    );
    run(
        &session,
        r#"GLOSS role ON orders.amount AS $${"value": "measure"}$$;
           GLOSS entity ON orders AS $${"value": "order"}$$;"#,
    )
    .await;

    // No grain clause claims nothing about the schema: the subject is
    // an address (an app's `app.page` rides the column shape), and
    // stands unchecked.
    run(
        &session,
        r#"GLOSS unit ON nowhere.at_all AS $${"value": "x"}$$;"#,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_missing_table_at_the_door_names_the_roads_out() {
    let (_dir, session) = agent_session().await;
    run(&session, SETUP).await;
    land_orders_and_customers(&session).await;

    let e = session
        .execute("SELECT * FROM tables;")
        .await
        .unwrap_err()
        .to_string();
    assert!(e.contains("not found"), "{e}");
    assert!(e.contains("tables in `fin`: customers, orders"), "{e}");
    assert!(
        e.contains("the store's relations: glossary, imports"),
        "{e}"
    );

    // Without a dataset in use the road out is USE.
    let (_dir, bare) = agent_session().await;
    let e = bare
        .execute("SELECT * FROM orders;")
        .await
        .unwrap_err()
        .to_string();
    assert!(e.contains("USE one first"), "{e}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_source_type_outside_the_vocabulary_is_refused_at_declare() {
    let (_dir, session) = agent_session().await;
    let e = session
        .execute("DECLARE SOURCE files SET (type: excel, location: '/nowhere');")
        .await
        .unwrap_err()
        .to_string();
    assert!(e.contains("unknown type `excel`"), "{e}");
    assert!(e.contains("relational_db, parquet, csv, json"), "{e}");
    // A bare name folds (SPEC.md §1): `CSV` is `csv`.
    session
        .execute("DECLARE SOURCE upper SET (type: CSV, location: '/nowhere');")
        .await
        .expect("an unquoted type name folds into the vocabulary");
    let e = session
        .execute("DECLARE SOURCE files SET (location: '/nowhere');")
        .await
        .unwrap_err()
        .to_string();
    assert!(e.contains("missing `type`"), "{e}");
    run(
        &session,
        "DECLARE SOURCE files SET (type: csv, location: '/nowhere');",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_probe_naming_a_table_at_a_file_source_lists_the_files() {
    let (dir, session) = agent_session().await;
    let root = dir.path().join("field");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("work_orders.csv"), "id\n1\n").unwrap();
    std::fs::write(root.join("sites.csv"), "id\n1\n").unwrap();
    run(
        &session,
        &format!(
            "DECLARE SOURCE field SET (type: csv, location: '{}');",
            root.display()
        ),
    )
    .await;

    // A table name at a file source: the engine's text stays, the
    // files and the read call that names one follow.
    let e = session
        .execute("PROBE field AS $$SELECT * FROM work_orders$$;")
        .await
        .unwrap_err()
        .to_string();
    assert!(e.starts_with("probe failed: "), "{e}");
    assert!(e.contains("not found"), "{e}");
    assert!(e.contains("read_csv("), "{e}");
    assert!(
        e.contains("files present: sites.csv, work_orders.csv"),
        "{e}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_json_arrow_read_leaves_the_engine_as_text() {
    let (_dir, session) = agent_session().await;
    run(&session, SETUP).await;
    let outcomes = run(
        &session,
        r#"SELECT '{"a": 1, "b": "x"}' -> 'a' AS a, '{"a": 1, "b": "x"}' -> 'b' AS b;"#,
    )
    .await;
    let Some(Outcome::Rows(batches)) = outcomes.into_iter().next_back() else {
        panic!("no rows");
    };
    // The union the JSON functions return never reaches a consumer:
    // arrow's JSON writer refuses it at the doors.
    for field in batches[0].schema().fields() {
        assert!(
            !matches!(field.data_type(), DataType::Union(..)),
            "{field:?}"
        );
    }
    let shown = pretty_format_batches(&batches).unwrap().to_string();
    assert!(shown.contains("| 1 ") && shown.contains("\"x\""), "{shown}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_filter_on_a_list_column_stays_with_the_engine() {
    // iceberg evaluates a pushed predicate through field accessors,
    // which primitive fields alone have: `IS NULL` on a list column
    // pushed to the scan fails at its first poll. The pin declines the
    // pushdown, so the filter runs in the engine over the scanned rows.
    let (_dir, session) = agent_session().await;
    run(&session, SETUP).await;
    let mut category = ListBuilder::new(StringBuilder::new());
    category.values().append_value("tools");
    category.append(true);
    category.append(false);
    category.values().append_value("a");
    category.values().append_value("b");
    category.append(true);
    let category = category.finish();
    let product = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("category", category.data_type().clone(), true),
        ])),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(category),
        ],
    )
    .unwrap();
    session
        .register_table(
            "product",
            Arc::new(MemTable::try_new(product.schema(), vec![vec![product]]).unwrap()),
        )
        .await
        .unwrap();

    let none = table(
        &session,
        "SELECT count(*) AS n FROM product WHERE category IS NULL;",
    )
    .await;
    assert!(none.contains("| 1 "), "{none}");
    let some = table(
        &session,
        "SELECT id FROM product WHERE category IS NOT NULL ORDER BY id;",
    )
    .await;
    assert!(
        some.contains("| 1 ") && some.contains("| 3 ") && !some.contains("| 2 "),
        "{some}"
    );
    // A primitive filter beside it is unaffected.
    let both = table(
        &session,
        "SELECT count(*) AS n FROM product WHERE category IS NOT NULL AND id > 1;",
    )
    .await;
    assert!(both.contains("| 1 "), "{both}");
}

/// A grounding stopped by its author (SPEC.md §5.2): `stopped` in
/// place of `sql`. The write answers with the author's own reason,
/// the read door and the cube refuse with it, `metric_surfaces`
/// serves it, the assumptions ride it into the round — and a human
/// `sql` over it serves.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stopped_grounding_serves_its_reason_and_no_number() {
    let (_dir, store) = scratch_store().await;
    let agent = session_with(ActorKind::Agent, "agent-1", &store).await;
    run(
        &agent,
        r#"DECLARE DATASET fin SET (purpose: 'metrics'); USE fin;
           DECLARE ASPECT cube WITH $${"type": "object", "properties": {
             "resolution": {"default": "day"},
             "windows": {"type": "object", "properties": {"month": {"default": "48 months"}}}}}$$
           AS FACT ON DATASET;
           DECLARE ASPECT utilization WITH $${"title": "Utilization"}$$ AS QUERY ON DATASET;"#,
    )
    .await;
    let written = table(
        &agent,
        r#"GLOSS utilization ON fin AS $${
             "stopped": "capacity never landed: the roster export carries no hours column",
             "assumptions": [{"dimension": "definition", "key": "capacity-source",
               "assumption": "capacity is the roster's planned hours", "basis": "convention",
               "confidence": 0.6}]}$$;"#,
    )
    .await;
    assert!(
        written.contains("false") && written.contains("stopped: capacity never landed"),
        "{written}"
    );
    let e = agent
        .execute("SELECT * FROM read.utilization();")
        .await
        .unwrap_err();
    assert!(
        e.to_string().contains("stopped — capacity never landed"),
        "{e}"
    );
    let facts = table(
        &agent,
        "SELECT metric, applicable, reason FROM metric_axes();",
    )
    .await;
    assert!(facts.contains("stopped: capacity never landed"), "{facts}");
    let surfaces = table(
        &agent,
        "SELECT name, grounded, stopped FROM metric_surfaces;",
    )
    .await;
    assert!(
        surfaces.contains("utilization")
            && surfaces.contains("false")
            && surfaces.contains("capacity never landed"),
        "{surfaces}"
    );
    let asked = table(&agent, "SELECT aspect, key, conf FROM open_questions;").await;
    assert!(asked.contains("capacity-source"), "{asked}");

    // The human's `sql` outranks the agent's stop: served, not stopped.
    let human = session_with(ActorKind::Human, "philipp", &store).await;
    run(
        &human,
        r#"USE fin; GLOSS utilization ON fin AS $${"sql": "SELECT 1 AS value"}$$;"#,
    )
    .await;
    let surfaces = table(
        &agent,
        "SELECT name, grounded, stopped FROM metric_surfaces;",
    )
    .await;
    assert!(
        surfaces.contains("true") && !surfaces.contains("capacity never landed"),
        "{surfaces}"
    );
}

/// `DESCRIBE` reaches every name a read can plan — a store relation
/// and a shipped read here, which the engine's own DESCRIBE cannot
/// see — in the engine's DESCRIBE shape; an unknown name is the
/// planner's refusal, naming it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_reaches_every_readable_name() {
    let (_dir, session) = agent_session().await;
    run(
        &session,
        "DECLARE DATASET fin SET (purpose: 'metrics'); USE fin;",
    )
    .await;
    let aspects = table(&session, "DESCRIBE aspects;").await;
    for column in [
        "column_name",
        "data_type",
        "is_nullable",
        "name",
        "kind",
        "grains",
        "condition",
        "schema",
        "Utf8",
    ] {
        assert!(aspects.contains(column), "{aspects}");
    }
    let next = table(&session, "DESCRIBE workspace_next;").await;
    assert!(next.contains("surface") && next.contains("open"), "{next}");
    let e = session.execute("DESCRIBE nothing_here;").await.unwrap_err();
    assert!(e.to_string().contains("nothing_here"), "{e}");
}

/// `SHOW TABLES` is the bound dataset's landed tables, and nothing
/// without a `USE`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn show_tables_lists_the_bound_dataset() {
    let (_dir, session) = agent_session().await;
    let e = session.execute("SHOW TABLES;").await.unwrap_err();
    assert!(matches!(e, glossql_session::SessionError::NoDataset), "{e}");
    run(
        &session,
        "DECLARE DATASET fin SET (purpose: 'metrics'); USE fin;",
    )
    .await;
    land_orders_and_customers(&session).await;
    let tables = table(&session, "SHOW TABLES;").await;
    assert!(
        tables.contains("orders") && tables.contains("customers") && tables.contains("fin"),
        "{tables}"
    );
}
