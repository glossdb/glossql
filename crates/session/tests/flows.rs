//! Statement flows end-to-end through the router: the shapes fixtures 01–10
//! and 13 are built from, executed in memory against `:memory:` stores. Data
//! tables are injected via `register_table` where a flow needs them —
//! recipes materialize real ones at M3.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use datafusion::arrow::array::{Float64Array, Int32Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::datasource::MemTable;
use glossql_glossary::{Actor, ActorKind, FunctionRow, Store};
use glossql_session::{FunctionRuntime, Outcome, Session};
use serde_json::{Value, json};

#[derive(Debug, Default)]
struct Fake {
    invocations: AtomicUsize,
    last_context: Mutex<Option<Value>>,
}

impl FunctionRuntime for Fake {
    fn invoke(
        &self,
        function: &FunctionRow,
        _: &str,
        context: &Value,
    ) -> Result<Value, String> {
        self.invocations.fetch_add(1, Ordering::SeqCst);
        *self.last_context.lock().unwrap() = Some(context.clone());
        Ok(match function.name.as_str() {
            "tb_check" => json!({"delta": 0.4}),
            "tb_bands" => json!({
                "subject": "trial_balance", "aspect": "reconciliation",
                "witness": "tb_w", "band": "red", "score": 0.9,
                "computed_at": "2026-08-04T00:00:00Z"
            }),
            // A voice validates against the aspect it speaks (fixture 06's
            // respelling): the check's verdict is an `outcome` too, with
            // its measurement beside it.
            "journal_check" => {
                json!({"outcome": "measured: debits equal credits", "imbalance": 0.0})
            }
            "framework_bands" => json!({
                "subject": "trial_balance", "aspect": "journal_balanced",
                "witness": "journal_w", "band": "green", "score": 0.0,
                "computed_at": "2026-08-06T00:00:00Z"
            }),
            "outliers" => json!({"rows": [1]}),
            // A canned cube: one flow metric with a served dimension and
            // a disclosed rival — the script's own logic is the scripts
            // suite's business; here the contract is the serving read.
            "metric_cube" => json!({
                "applicable": true,
                "caps": {"dims": 2, "members": 24, "months": 24},
                "metrics": [{
                    "metric": "revenue", "applicable": true, "behavior": "flow",
                    "dims": ["region"], "alternative": "all invoiced",
                    "rows": [
                        {"dimension": "", "member": "", "period": "2026-01", "value": 100.0},
                        {"dimension": "", "member": "", "period": "2026-02", "value": 130.0},
                        {"dimension": "region", "member": "EMEA", "period": "2026-01", "value": 60.0},
                        {"dimension": "region", "member": "EMEA", "period": "2026-02", "value": 70.0},
                        {"dimension": "region", "member": "AMER", "period": "2026-01", "value": 40.0},
                        {"dimension": "region", "member": "AMER", "period": "2026-02", "value": 60.0},
                        {"dimension": "alternative", "member": "all invoiced", "period": "2026-01", "value": 90.0},
                        {"dimension": "alternative", "member": "all invoiced", "period": "2026-02", "value": 95.0}
                    ]
                }]
            }),
            // A detector that actually reads its context: one slot agrees
            // with itself, two disagree. What it answers therefore depends
            // on the witness it was called for — which is the point of the
            // shared-detector test below.
            "slot_bands" => {
                let slots = context["slots"].as_array().map_or(0, Vec::len);
                let (band, score) = if slots > 1 {
                    ("red", 1.0)
                } else {
                    ("green", 0.0)
                };
                json!({
                    "subject": context["subject"], "aspect": context["aspect"],
                    "witness": context["witness"], "band": band, "score": score,
                    "computed_at": "2026-08-06T00:00:00Z"
                })
            }
            _ => json!({"ok": true}),
        })
    }
}

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

async fn agent_session() -> (Session, Arc<Fake>) {
    let store = Store::open_memory().await.unwrap();
    let fake = Arc::new(Fake::default());
    let session = session_with(ActorKind::Agent, "agent-1", &store)
        .await
        .with_runtime(fake.clone());
    (session, fake)
}

async fn run(session: &Session, sql: &str) -> Vec<Outcome> {
    session
        .execute(sql)
        .await
        .unwrap_or_else(|e| panic!("`{sql}` failed: {e}"))
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gloss_then_read_collapsed_and_raw() {
    let (session, _) = agent_session().await;
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
    let store = Store::open_memory().await.unwrap();
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

    // Precedence (ruled 2026-08-04): human > agent > function; no detector
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
    let (session, fake) = agent_session().await;
    run(&session, SETUP).await;
    run(
        &session,
        r#"DECLARE ASPECT outlier_rows WITH $${"type": "object",
             "required": ["rows"], "properties": {"rows": {"type": "array"}}}$$ AS MEASUREMENT;
           DECLARE FUNCTION outliers FOR fin AS $$#{}$$
           RETURNS outlier_rows;"#,
    )
    .await;

    run(&session, "SELECT outliers() FROM fin;").await;
    assert_eq!(fake.invocations.load(Ordering::SeqCst), 1);
    run(&session, "SELECT outliers() FROM fin;").await;
    assert_eq!(
        fake.invocations.load(Ordering::SeqCst),
        1,
        "the second run serves the measurement at the same pin"
    );

    // Any input moving makes a new pin — a gloss moves the glossary head,
    // so the next extraction recomputes. No sweep, only a miss.
    run(
        &session,
        r#"GLOSS unit ON fin AS $${"value": "x"}$$;"#,
    )
    .await;
    run(&session, "SELECT outliers() FROM fin;").await;
    assert_eq!(fake.invocations.load(Ordering::SeqCst), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn context_arrives_from_the_accepts_aspects() {
    let (session, fake) = agent_session().await;
    run(&session, SETUP).await;
    // Fixture 13's model: config is context — glossed on the dataset,
    // named by ACCEPTS, handed to the script by the server.
    run(
        &session,
        r##"
        DECLARE ASPECT null_values WITH $${"type": "object"}$$ AS FACT;
        GLOSS null_values ON fin AS $${"values": ["#N/A", "TBD"]}$$;
        DECLARE ASPECT inferred WITH $${"type": "object"}$$ AS MEASUREMENT;
        DECLARE FUNCTION infer_types FOR GLOBAL AS $$#{}$$
          ACCEPTS (null_values)
          RETURNS inferred;
        SELECT infer_types() FROM orders;
        "##,
    )
    .await;
    let context = fake.last_context.lock().unwrap().clone().unwrap();
    assert_eq!(
        context,
        json!({"null_values": {"values": ["#N/A", "TBD"]}}),
        "the dataset-level gloss reaches a table-subject run via the parent walk"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn accepts_must_name_declared_aspects() {
    let (session, _) = agent_session().await;
    run(&session, SETUP).await;
    let e = session
        .execute(r#"DECLARE FUNCTION f FOR fin AS $$#{}$$ ACCEPTS (nope);"#)
        .await
        .unwrap_err();
    assert!(e.to_string().contains("aspect"), "{e}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn attest_serves_detector_outputs_in_the_fixed_shape() {
    let (session, _) = agent_session().await;
    run(&session, SETUP).await;
    run(
        &session,
        r#"
        DECLARE ASPECT reconciliation WITH $${"type": "object"}$$ AS MEASUREMENT;
        DECLARE FUNCTION tb_check FOR fin AS $$#{}$$
          RETURNS reconciliation;
        DECLARE FUNCTION tb_bands FOR fin AS $$#{}$$;
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
async fn the_glossary_is_a_plain_readable_relation_and_the_strike_is_parked() {
    let (session, _) = agent_session().await;
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

    // The strike routes, and refuses by name until iceberg-rust 0.11
    // can remove rows (ruled 2026-08-17).
    let e = session
        .execute("DELETE FROM glossary WHERE subject = 'orders.amount' AND aspect = 'unit';")
        .await
        .unwrap_err();
    assert!(e.to_string().contains("parked"), "{e}");
    assert!(e.to_string().contains("0.11"), "{e}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn substrate_sql_runs_against_registered_tables() {
    let (session, _) = agent_session().await;
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

    // The allowlist (project lead, 2026-08-04): schema-altering SQL is
    // refused at the door — tables come from recipes.
    let err = session
        .execute("CREATE VIEW big_orders AS SELECT id FROM orders;")
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("not open for CREATE VIEW"),
        "{err}"
    );

    // DESCRIBE and EXPLAIN are reads, so they pass (2026-08-07) —
    // before this, the only way to see a landed schema was burning a
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
    let (session, _) = agent_session().await;
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
    let (session, _) = agent_session().await;
    let e = session
        .execute("SELECT * FROM GLOSSARY();")
        .await
        .unwrap_err();
    assert!(e.to_string().contains("USE"), "{e}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gloss_on_a_pair_path_lands_under_the_relationship_subject() {
    let (session, _) = agent_session().await;
    run(&session, SETUP).await;
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
    let (session, _) = agent_session().await;
    run(&session, SETUP).await;
    run(
        &session,
        r#"
        DECLARE SOURCE erp SET (type: parquet, location: 'lake/erp');
        DECLARE ASPECT column_profile WITH $${"type": "object"}$$ AS MEASUREMENT;
        DECLARE ASPECT outlier_profile WITH $${"type": "object"}$$ AS MEASUREMENT;
        DECLARE FUNCTION outliers FOR GLOBAL AS $$#{}$$
          ACCEPTS (column_profile)
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
        "SELECT name, scope, accepts, returns FROM functions ORDER BY name;",
    )
    .await;
    insta::assert_snapshot!(functions, @r#"
    +----------+--------+--------------------+-----------------+
    | name     | scope  | accepts            | returns         |
    +----------+--------+--------------------+-----------------+
    | checker  | fin    |                    |                 |
    | outliers | GLOBAL | ["column_profile"] | outlier_profile |
    +----------+--------+--------------------+-----------------+
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
    // (2026-08-07; before this, USE-and-find-out was the only way).
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
    // same aspect, and the detector bands across both slots.
    let (session, fake) = agent_session().await;
    run(&session, SETUP).await;
    run(
        &session,
        r##"
        DECLARE ASPECT journal_balanced WITH $${
          "type": "object", "required": ["outcome"],
          "properties": {"outcome": {"type": "string"}, "tolerance": {"type": "number"}}
        }$$ AS FACT ON TABLE;
        GLOSS journal_balanced ON fin.trial_balance AS $${"outcome": "debits equal credits, exactly", "tolerance": 0.0}$$;
        DECLARE FUNCTION journal_check FOR fin AS $$#{}$$
 RETURNS journal_balanced;
        DECLARE FUNCTION framework_bands FOR fin AS $$#{}$$;
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

    // The detector saw both slots: the agent's authored expectation and
    // the check voice's measured result.
    let ctx = fake.last_context.lock().unwrap().clone().unwrap();
    let slots = ctx["slots"]
        .as_array()
        .unwrap_or_else(|| panic!("no slots in {ctx}"));
    assert_eq!(slots.len(), 2, "{ctx}");
    let all = ctx.to_string();
    assert!(
        all.contains("outcome") && all.contains("imbalance"),
        "{ctx}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_grounding_admits_and_serves_its_sql_back() {
    // Fixture 16 §2: a concept grounds as a grain-free extract; the read
    // serves the SQL, running it is the reader's act.
    let (session, _) = agent_session().await;
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
    // Fixture 16 §6, bound 2026-08-07; the door renamed to `read.`
    // 2026-08-11 (one generic serving prefix over every QUERY gloss):
    // `read.<aspect>()` expands the collapsed current QUERY grounding as
    // an ordinary relation — the reader composes around it, the pinned
    // definition is what runs.
    let store = Store::open_memory().await.unwrap();
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
    // too (2026-08-12) — a mixed cycle names each door on the path.
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
    // The defect found 2026-08-06: a verdict was cached under (subject,
    // function), so the first witness to compute answered for every other
    // witness on the same detector — and sharing one is the shipped idiom
    // (role, behavior and unit all band through `slot_entropy`). Here
    // `alpha` is contested and `beta` is not; each must say so itself.
    let store = Store::open_memory().await.unwrap();
    let fake = Arc::new(Fake::default());
    let agent = session_with(ActorKind::Agent, "agent-1", &store)
        .await
        .with_runtime(fake.clone());
    run(&agent, SETUP).await;
    run(
        &agent,
        r#"
        DECLARE ASPECT alpha WITH $${"type": "object"}$$ AS FACT ON COLUMN;
        DECLARE ASPECT beta WITH $${"type": "object"}$$ AS FACT ON COLUMN;
        DECLARE FUNCTION slot_bands FOR fin AS $$#{}$$;
        DECLARE WITNESS alpha_w ON alpha BY (AGENT, HUMAN) DETECTOR slot_bands THRESHOLD 0.5;
        DECLARE WITNESS beta_w ON beta BY (AGENT, HUMAN) DETECTOR slot_bands THRESHOLD 0.5;
        GLOSS alpha ON orders.amount AS $${"reading": "agent's"}$$;
        GLOSS beta ON orders.amount AS $${"reading": "uncontested"}$$;
        "#,
    )
    .await;

    // The human disputes `alpha` only.
    let human = session_with(ActorKind::Human, "philipp", &store)
        .await
        .with_runtime(fake.clone());
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
    // TABLE, so it walked through an allowlist keyed on the statement
    // variant (found 2026-08-06). Tables come from recipes.
    let (session, _) = agent_session().await;
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

    // The nested spellings walk past the allowlist — `selects_into` reads
    // the query's body, not its WITH clause — but the planner nests their
    // CreateMemoryTable inside the plan (datafusion-sql-53.1.0
    // query.rs:73), and nested DDL has no physical plan, so execution
    // refuses it. This pins that backstop across substrate upgrades: if a
    // refusal below ever turns into a success, the allowlist must learn
    // these spellings before the upgrade lands.
    for sneak in [
        "WITH x AS (SELECT 1 AS a INTO sneak_cte) SELECT * FROM x;",
        "SELECT * FROM (SELECT 1 AS a INTO sneak_sub) t;",
    ] {
        assert!(session.execute(sneak).await.is_err(), "{sneak}");
    }
    for made in ["SELECT * FROM sneak_cte;", "SELECT * FROM sneak_sub;"] {
        assert!(
            session.execute(made).await.is_err(),
            "nothing was created: {made}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn select_into_is_refused_on_the_streaming_path_too() {
    // The streaming door repeated the execute path's variant check but
    // not its `selects_into` guard, so the same spelling minted a table
    // there and materialized the whole source before the row cap
    // applied (found 2026-08-12).
    let (session, _) = agent_session().await;
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
    // they replay and had no guard, so a self-referential body recursed
    // to stack overflow (found 2026-08-12).
    let (session, _) = agent_session().await;
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
    // `AS FACT ON SOURCE` (ruled 2026-08-12): a declared source's name
    // is a subject, and its slots serve in every dataset — the deposit
    // the next dataset reads before probing.
    let (session, _) = agent_session().await;
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
async fn metric_series_serves_the_cached_cube() {
    let (session, _) = agent_session().await;
    run(
        &session,
        "DECLARE DATASET fin SET (purpose: 'metrics'); USE fin;",
    )
    .await;

    // Before the measurement runs the relation is empty — honest, not
    // an error; nothing computes at read.
    let empty = table(&session, "SELECT count(*) AS n FROM metric_series();").await;
    assert!(empty.contains("| 0"), "{empty}");

    run(
        &session,
        r#"DECLARE ASPECT metric_cube WITH $${"type": "object"}$$ AS MEASUREMENT ON DATASET;
           DECLARE FUNCTION metric_cube FOR GLOBAL AS $$#{}$$
             RETURNS metric_cube;
           SELECT metric_cube() FROM fin;"#,
    )
    .await;

    // The total series: dimension '' is the monthly total.
    let totals = table(
        &session,
        "SELECT period, value FROM metric_series() \
         WHERE metric = 'revenue' AND dimension = '' ORDER BY period;",
    )
    .await;
    assert!(totals.contains("2026-01") && totals.contains("100.0"), "{totals}");

    // Slices compose with plain SQL — the members sum back to the frame.
    let sliced = table(
        &session,
        "SELECT member, sum(value) AS v FROM metric_series() \
         WHERE metric = 'revenue' AND dimension = 'region' GROUP BY 1 ORDER BY 1;",
    )
    .await;
    assert!(sliced.contains("AMER") && sliced.contains("100.0"), "{sliced}");
    assert!(sliced.contains("EMEA") && sliced.contains("130.0"), "{sliced}");

    // The disclosed rival rides as its own dimension, named.
    let rival = table(
        &session,
        "SELECT member, value FROM metric_series() \
         WHERE dimension = 'alternative' ORDER BY period;",
    )
    .await;
    assert!(rival.contains("all invoiced") && rival.contains("95.0"), "{rival}");

    // Arguments are refused — filters ride WHERE.
    let e = session
        .execute("SELECT * FROM metric_series('revenue');")
        .await
        .unwrap_err();
    assert!(e.to_string().contains("no arguments"), "{e}");
}

/// A measurement is a query (stage 5, §7e): the skill's own
/// quick-validation flow, end to end — declare the aspect and a
/// SQL-bodied function, extract, read the landed value back. No script
/// runtime is involved: the Fake would panic on an unknown name, and
/// the assertion on `invocations` proves it was never asked.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_measurement_body_is_sql() {
    let (session, fake) = agent_session().await;
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
           $$ RETURNS ar_check;
           SELECT ar_settles_in_full() FROM settlements;"#,
    )
    .await;
    assert_eq!(
        fake.invocations.load(Ordering::SeqCst),
        0,
        "the engine ran the body; the script runtime was never asked"
    );

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
    let (session, _) = agent_session().await;
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
    let (session, _) = agent_session().await;
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
}

/// A measurement landed on one channel is visible on every other the
/// moment it commits. The pin covers inputs only, so a landing moves no
/// pin — a cached context checked by pin alone would keep serving the
/// view from before the landing on every channel but the one that
/// computed it (found 2026-08-18: the docket's charts stayed empty
/// while the agent's channel served the cube it had just landed).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_landing_reaches_a_channel_that_already_read() {
    let store = Store::open_memory().await.unwrap();
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
