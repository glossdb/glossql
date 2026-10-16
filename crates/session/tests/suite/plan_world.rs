//! The plan world (fixture 27): a plan is its own QUERY aspect, declared
//! `x-kind: plan` with `x-plan-of` naming its metric and grounded over a
//! landed budget table. The cube serves it under the metric it names,
//! `metric_series(world => 'plan', plan => '<name>')`, at its own
//! resolution or coarser, and in no other world; two editions stand side
//! by side; the declaration is refused where the tie does not hold.

use std::sync::Arc;

use datafusion::arrow::array::{Date32Array, Float64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::datasource::MemTable;
use glossql_glossary::{Actor, ActorKind, Store};
use glossql_session::{NoRuntime, Outcome, Session};

fn day(y: i32, m: u32, d: u32) -> i32 {
    let d = chrono::NaiveDate::from_ymd_opt(y, m, d).expect("a date");
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("the epoch");
    (d - epoch).num_days() as i32
}

/// The cube's settings, the temporal verdict a series is admitted on,
/// judged by a stub function, one metric and two editions of its plan.
fn setup() -> &'static str {
    r##"
DECLARE DATASET fin SET (purpose: 'the plan world');
USE fin;
DECLARE ASPECT cube WITH $${"type": "object", "properties": {
  "resolution": {"default": "month"},
  "windows": {"type": "object", "properties": {
    "day": {"default": "18 months"}, "month": {"default": "48 months"}, "year": {"default": "20 years"}}}}}$$
AS FACT ON DATASET;
DECLARE ASPECT temporal_profile WITH $${"type": "object", "required": ["applicable"],
  "properties": {"applicable": {"type": "boolean"}}}$$ AS MEASUREMENT ON COLUMN;
DECLARE FUNCTION judge_time FOR GLOBAL AS
  $$SELECT true AS applicable, 'month' AS granularity, named_struct('ratio', 1.0) AS completeness$$
  RETURNS temporal_profile;
DECLARE ASPECT expense WITH $${"title": "Operating expense", "x-kind": "measure"}$$ AS QUERY ON DATASET;
GLOSS expense ON fin AS $${"sql": "SELECT month, amount AS value FROM spend"}$$;
DECLARE ASPECT original_budget WITH $${
  "title": "Original budget", "x-kind": "plan", "x-plan-of": "expense"
}$$ AS QUERY ON DATASET;
DECLARE ASPECT revised_budget WITH $${
  "title": "Revised budget", "x-kind": "plan", "x-plan-of": "expense"
}$$ AS QUERY ON DATASET;
GLOSS original_budget ON fin AS $${"sql": "SELECT month, original AS value FROM budget"}$$;
GLOSS revised_budget ON fin AS $${"sql": "SELECT month, revised AS value FROM budget"}$$;
"##
}

/// Eighteen months, 2024-01 to 2025-06: spend of 100 rising by one a
/// month; a budget of 120 a month, revised to 130 from 2025-01.
async fn session() -> Session {
    let dir = tempfile::tempdir().expect("a scratch dir");
    let lake = glossql_catalog::Lake::open(
        &dir.path().join("catalog.sqlite"),
        &dir.path().join("warehouse"),
    )
    .await
    .expect("a lake");
    let store = Store::open(lake).await.expect("a store");
    let session = Session::new(
        store,
        Actor {
            kind: ActorKind::Agent,
            id: "agent-1".into(),
        },
    )
    .expect("a session")
    .with_runtime(Arc::new(NoRuntime));
    session.execute(setup()).await.expect("the setup lands");
    std::mem::forget(dir);
    let months: Vec<i32> = (0..18)
        .map(|i| day(2024 + i / 12, (i % 12 + 1) as u32, 1))
        .collect();
    let spend = Arc::new(Schema::new(vec![
        Field::new("month", DataType::Date32, false),
        Field::new("amount", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&spend),
        vec![
            Arc::new(Date32Array::from(months.clone())),
            Arc::new(Float64Array::from(
                (0..18).map(|i| 100.0 + i as f64).collect::<Vec<_>>(),
            )),
        ],
    )
    .expect("a batch");
    register(&session, "spend", spend, batch).await;
    let budget = Arc::new(Schema::new(vec![
        Field::new("month", DataType::Date32, false),
        Field::new("original", DataType::Float64, false),
        Field::new("revised", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&budget),
        vec![
            Arc::new(Date32Array::from(months)),
            Arc::new(Float64Array::from(vec![120.0; 18])),
            Arc::new(Float64Array::from(
                (0..18)
                    .map(|i| if i < 12 { 120.0 } else { 130.0 })
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .expect("a batch");
    register(&session, "budget", budget, batch).await;
    session
        .execute("SELECT judge_time() FROM spend.month; SELECT judge_time() FROM budget.month;")
        .await
        .expect("the cadence is judged");
    session
}

async fn register(session: &Session, name: &str, schema: Arc<Schema>, batch: RecordBatch) {
    session
        .register_table(
            name,
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).expect("a table")),
        )
        .await
        .expect("the table registers");
}

async fn rows(session: &Session, sql: &str) -> String {
    let outcomes = session.execute(sql).await.expect("the read serves");
    let Some(Outcome::Rows { batches, .. }) = outcomes.into_iter().next_back() else {
        panic!("no rows");
    };
    pretty_format_batches(&batches)
        .expect("printable")
        .to_string()
}

async fn refused(session: &Session, sql: &str) -> String {
    session.execute(sql).await.expect_err("refused").to_string()
}

/// The printed row naming `key`.
fn line<'a>(table: &'a str, key: &str) -> &'a str {
    table
        .lines()
        .find(|l| l.contains(key))
        .unwrap_or_else(|| panic!("no row with {key}:\n{table}"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_plan_declares_on_a_declared_metric_or_is_refused() {
    let session = session().await;
    for (decl, said) in [
        (
            r##"DECLARE ASPECT bad WITH $${"x-kind": "plan", "x-plan-of": "expense", "type": "object"}$$ AS FACT ON DATASET;"##,
            "a plan is a QUERY aspect",
        ),
        (
            r##"DECLARE ASPECT bad WITH $${"x-kind": "plan"}$$ AS QUERY ON DATASET;"##,
            "a plan names the metric it plans",
        ),
        (
            r##"DECLARE ASPECT bad WITH $${"x-kind": "plan", "x-plan-of": "nobody"}$$ AS QUERY ON DATASET;"##,
            "`nobody` is not a declared aspect",
        ),
        (
            r##"DECLARE ASPECT bad WITH $${"x-kind": "plan", "x-plan-of": "cube"}$$ AS QUERY ON DATASET;"##,
            "`cube` is a fact aspect",
        ),
        (
            r##"DECLARE ASPECT bad WITH $${"x-kind": "plan", "x-plan-of": "revised_budget"}$$ AS QUERY ON DATASET;"##,
            "`revised_budget` is itself a plan",
        ),
        (
            r##"DECLARE ASPECT bad WITH $${"x-kind": "measure", "x-plan-of": "expense"}$$ AS QUERY ON DATASET;"##,
            "`x-plan-of` rides `x-kind: plan`",
        ),
    ] {
        let e = refused(&session, decl).await;
        assert!(
            e.contains("DECLARE ASPECT bad") && e.contains(said),
            "{decl}\n{e}"
        );
    }
    let declared = rows(
        &session,
        "SELECT count(*) AS n FROM GLOSSARY(fin) WHERE aspect = 'bad';",
    )
    .await;
    assert!(declared.contains("| 0 "), "{declared}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_plan_world_serves_the_plan_under_its_metric_and_nowhere_else() {
    let session = session().await;

    // The actual world is the metrics' alone: the plans' heads are no
    // metric, and their rows never appear under their own names.
    let actual = rows(
        &session,
        "SELECT metric, count(*) AS n FROM metric_series() GROUP BY metric ORDER BY metric;",
    )
    .await;
    assert!(
        actual.contains("| expense |") && !actual.contains("budget"),
        "{actual}"
    );
    // The plan is a grounding like any other: a view in the catalog,
    // served by name.
    let served = rows(
        &session,
        "SELECT count(*) AS n, sum(value) AS total FROM read.revised_budget();",
    )
    .await;
    assert!(
        served.contains("| 18 ") && served.contains("2220.0"),
        "{served}"
    );
    // The fact row per grounding, the plans' naming the metric they plan.
    let axes = rows(
        &session,
        "SELECT metric, applicable, resolution, plan_of FROM metric_axes() ORDER BY metric;",
    )
    .await;
    for line in [
        "| expense         | true       | month      |         |",
        "| original_budget | true       | month      | expense |",
        "| revised_budget  | true       | month      | expense |",
    ] {
        assert!(axes.contains(line), "{axes}");
    }

    // The plan world: the plan's cells under the metric it names, at
    // the plan's own resolution — 120 a month through 2024, 130 from
    // 2025-01.
    let plan = rows(
        &session,
        "SELECT metric, world, period, value, behavior \
         FROM metric_series(world => 'plan', plan => 'revised_budget') ORDER BY period;",
    )
    .await;
    let lines: Vec<&str> = plan.lines().filter(|l| l.contains("| plan ")).collect();
    assert_eq!(lines.len(), 18, "{plan}");
    assert!(lines.iter().all(|l| l.starts_with("| expense ")), "{plan}");
    assert!(
        plan.contains("| 2024-01-01T00:00:00 | 120.0 | flow")
            && plan.contains("| 2025-01-01T00:00:00 | 130.0 | flow"),
        "{plan}"
    );
    // A coarser grain sums the plan as it sums a metric; a finer one
    // serves no rows.
    let years = rows(
        &session,
        "SELECT period, value FROM metric_series(world => 'plan', plan => 'revised_budget', grain => 'year') ORDER BY period;",
    )
    .await;
    assert!(line(&years, "2024-01-01").contains("| 1440.0 "), "{years}");
    assert!(line(&years, "2025-01-01").contains("| 780.0 "), "{years}");
    let days = rows(
        &session,
        "SELECT count(*) AS n FROM metric_series(world => 'plan', plan => 'revised_budget', grain => 'day');",
    )
    .await;
    assert!(days.contains("| 0 "), "{days}");

    // Two editions side by side: a join of two plan reads.
    let editions = rows(
        &session,
        "SELECT o.period, o.value AS original, r.value AS revised \
         FROM metric_series(world => 'plan', plan => 'original_budget') o \
         JOIN metric_series(world => 'plan', plan => 'revised_budget') r \
           ON o.metric = r.metric AND o.period = r.period \
         WHERE o.period = TIMESTAMP '2025-01-01';",
    )
    .await;
    let row = line(&editions, "2025-01-01");
    assert!(
        row.contains("| 120.0 ") && row.contains("| 130.0 "),
        "{editions}"
    );
    // The variance against the plan: a join of the actual and the plan
    // worlds at the plan's grain. 2024: spend 1266 against 1440.
    let variance = rows(
        &session,
        "SELECT a.period, a.value - p.value AS variance \
         FROM metric_series(grain => 'year') a \
         JOIN metric_series(world => 'plan', plan => 'revised_budget', grain => 'year') p \
           ON a.metric = p.metric AND a.period = p.period \
         WHERE a.metric = 'expense' AND a.dimension = '' AND p.dimension = '' ORDER BY a.period;",
    )
    .await;
    assert!(
        line(&variance, "2024-01-01").contains("| -174.0 "),
        "{variance}"
    );
    assert!(
        line(&variance, "2025-01-01").contains("| -93.0 "),
        "{variance}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_plan_read_is_refused_without_its_name_or_its_world() {
    let session = session().await;
    for (sql, said) in [
        (
            "SELECT * FROM metric_series(world => 'plan');",
            "a plan is read by name",
        ),
        (
            "SELECT * FROM metric_series(plan => 'revised_budget');",
            "read in the plan world",
        ),
        (
            "SELECT * FROM metric_series(world => 'scenario', plan => 'revised_budget');",
            "read in the plan world",
        ),
        (
            "SELECT * FROM metric_series(world => 'plan', scenario => 'x', plan => 'revised_budget');",
            "one name",
        ),
        (
            "SELECT * FROM metric_series(world => 'plan', plan => 'nobody');",
            "no aspect of that name",
        ),
        (
            "SELECT * FROM metric_series(world => 'plan', plan => 'expense');",
            "without `x-kind: plan`",
        ),
        (
            "SELECT * FROM metric_series(world => 'budget');",
            "a world is actual, forecast, scenario or plan",
        ),
    ] {
        let e = refused(&session, sql).await;
        assert!(e.contains(said), "{sql}\n{e}");
    }
    // A plan declared and not yet grounded is named as such.
    session
        .execute(r##"DECLARE ASPECT draft WITH $${"x-kind": "plan", "x-plan-of": "expense"}$$ AS QUERY ON DATASET;"##)
        .await
        .expect("declared");
    let e = refused(
        &session,
        "SELECT * FROM metric_series(world => 'plan', plan => 'draft');",
    )
    .await;
    assert!(e.contains("nothing serves it on `fin`"), "{e}");
}
