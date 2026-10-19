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
        (
            r##"DECLARE ASPECT bad WITH $${"x-kind": "measure", "x-covers": "year"}$$ AS QUERY ON DATASET;"##,
            "`x-covers` rides `x-kind: plan`",
        ),
        (
            r##"DECLARE ASPECT bad WITH $${"x-kind": "plan", "x-plan-of": "expense", "x-covers": "month"}$$ AS QUERY ON DATASET;"##,
            "`x-covers` names the grain a row's value is booked against, `year` or `quarter` — \"month\" is neither",
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

// -- the plan against the actual and the forecast ------------------------

/// The planning fixture: the same spend, a monthly budget landed
/// through 2025 — 120 a month in 2024, 130 from 2025-01 to 2025-06,
/// 110 after — and a yearly budget of 1440 for 2024 and 1560 for 2025,
/// judged at the year by the one cadence judge, which names the year
/// for the yearly tables; a headcount of 100 rising by one a month, a
/// stock, with a yearly target of 115 for 2024 and 120 for 2025. The
/// forecast service stubbed flat, the lever aspect declared as the kit
/// declares it.
fn planning_setup() -> &'static str {
    r##"
DECLARE DATASET fin SET (purpose: 'the planning goal');
USE fin;
DECLARE ASPECT cube WITH $${"type": "object", "properties": {
  "resolution": {"default": "month"},
  "windows": {"type": "object", "properties": {
    "day": {"default": "18 months"}, "month": {"default": "48 months"},
    "quarter": {"default": "10 years"}, "year": {"default": "20 years"}}}}}$$
AS FACT ON DATASET;
DECLARE ASPECT temporal_profile WITH $${"type": "object", "required": ["applicable"],
  "properties": {"applicable": {"type": "boolean"}}}$$ AS MEASUREMENT ON COLUMN;
DECLARE FUNCTION judge_time FOR GLOBAL AS
  $$SELECT true AS applicable,
           CASE WHEN CAST($subject AS VARCHAR) LIKE '%yearly%' OR CAST($subject AS VARCHAR) LIKE '%targets%'
                THEN 'year' ELSE 'month' END AS granularity,
           named_struct('ratio', 1.0) AS completeness$$
  RETURNS temporal_profile;
DECLARE ASPECT lever WITH $${"type": "object", "properties": {
  "value": {"type": "boolean"}, "grounds": {"type": "string"}}}$$ AS FACT ON COLUMN;
DECLARE ASPECT expense WITH $${"title": "Operating expense", "x-kind": "measure"}$$ AS QUERY ON DATASET;
GLOSS expense ON fin AS $${"sql": "SELECT month, amount AS value FROM spend"}$$;
DECLARE ASPECT revised_budget WITH $${
  "title": "Revised budget", "x-kind": "plan", "x-plan-of": "expense"
}$$ AS QUERY ON DATASET;
DECLARE ASPECT annual_budget WITH $${
  "title": "Annual budget", "x-kind": "plan", "x-plan-of": "expense"
}$$ AS QUERY ON DATASET;
GLOSS revised_budget ON fin AS $${"sql": "SELECT month, revised AS value FROM budget"}$$;
GLOSS annual_budget ON fin AS $${"sql": "SELECT year, planned AS value FROM yearly"}$$;
DECLARE ASPECT headcount WITH $${"title": "Headcount", "x-kind": "measure"}$$ AS QUERY ON DATASET;
GLOSS headcount ON fin AS $${"sql": "SELECT month, heads AS value FROM staff", "behavior": "stock"}$$;
DECLARE ASPECT headcount_target WITH $${
  "title": "Headcount target", "x-kind": "plan", "x-plan-of": "headcount"
}$$ AS QUERY ON DATASET;
GLOSS headcount_target ON fin AS $${"sql": "SELECT year, heads AS value FROM targets", "behavior": "stock"}$$;
"##
}

async fn planning_session(stub: Arc<crate::forecast::Flat>) -> Session {
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
    .with_runtime(stub);
    session
        .execute(planning_setup())
        .await
        .expect("the setup lands");
    std::mem::forget(dir);
    let months = |n: usize| -> Vec<i32> {
        (0..n)
            .map(|i| day(2024 + (i / 12) as i32, (i % 12 + 1) as u32, 1))
            .collect()
    };
    let spend = Arc::new(Schema::new(vec![
        Field::new("month", DataType::Date32, false),
        Field::new("amount", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&spend),
        vec![
            Arc::new(Date32Array::from(months(18))),
            Arc::new(Float64Array::from(
                (0..18).map(|i| 100.0 + i as f64).collect::<Vec<_>>(),
            )),
        ],
    )
    .expect("a batch");
    register(&session, "spend", spend, batch).await;
    let budget = Arc::new(Schema::new(vec![
        Field::new("month", DataType::Date32, false),
        Field::new("revised", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&budget),
        vec![
            Arc::new(Date32Array::from(months(24))),
            Arc::new(Float64Array::from(
                (0..24)
                    .map(|i| match i {
                        0..=11 => 120.0,
                        12..=17 => 130.0,
                        _ => 110.0,
                    })
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .expect("a batch");
    register(&session, "budget", budget, batch).await;
    let yearly = Arc::new(Schema::new(vec![
        Field::new("year", DataType::Date32, false),
        Field::new("planned", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&yearly),
        vec![
            Arc::new(Date32Array::from(vec![day(2024, 1, 1), day(2025, 1, 1)])),
            Arc::new(Float64Array::from(vec![1440.0, 1560.0])),
        ],
    )
    .expect("a batch");
    register(&session, "yearly", yearly, batch).await;
    let staff = Arc::new(Schema::new(vec![
        Field::new("month", DataType::Date32, false),
        Field::new("heads", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&staff),
        vec![
            Arc::new(Date32Array::from(months(18))),
            Arc::new(Float64Array::from(
                (0..18).map(|i| 100.0 + i as f64).collect::<Vec<_>>(),
            )),
        ],
    )
    .expect("a batch");
    register(&session, "staff", staff, batch).await;
    let targets = Arc::new(Schema::new(vec![
        Field::new("year", DataType::Date32, false),
        Field::new("heads", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&targets),
        vec![
            Arc::new(Date32Array::from(vec![day(2024, 1, 1), day(2025, 1, 1)])),
            Arc::new(Float64Array::from(vec![115.0, 120.0])),
        ],
    )
    .expect("a batch");
    register(&session, "targets", targets, batch).await;
    session
        .execute(
            "SELECT judge_time() FROM spend.month; SELECT judge_time() FROM budget.month; \
             SELECT judge_time() FROM yearly.year; SELECT judge_time() FROM staff.month; \
             SELECT judge_time() FROM targets.year;",
        )
        .await
        .expect("the cadences are judged");
    session
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_plan_landed_past_the_actuals_moves_no_edge() {
    // The budget runs to 2025-12 and the spend to 2025-06: the actual
    // world ends with the spend — eighteen cells, no zero to December —
    // while the plan's own cells reach the budget's end.
    let session = planning_session(Arc::new(crate::forecast::Flat::default())).await;
    let edge = rows(
        &session,
        "SELECT CAST(max(period) AS DATE) AS edge, count(*) AS n \
         FROM metric_series() WHERE metric = 'expense' AND dimension = '';",
    )
    .await;
    assert!(edge.contains("| 2025-06-01 | 18 "), "{edge}");
    let plan = rows(
        &session,
        "SELECT count(*) AS n FROM metric_series(world => 'plan', plan => 'revised_budget');",
    )
    .await;
    assert!(plan.contains("| 24 "), "{plan}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plan_points_serves_every_plan_against_the_actual_and_the_forecast() {
    let stub = Arc::new(crate::forecast::Flat::default());
    let session = planning_session(Arc::clone(&stub)).await;

    // Before any forecast: every month through the origin is landed
    // with its variance (actual minus plan), the months ahead stand
    // ahead, and the year rows — the plan's own for the yearly budget,
    // the monthly budget summed — are landed for 2024 and open for
    // 2025 with the actual to date.
    let points = rows(
        &session,
        "SELECT plan, grain, label, value, actual, variance, stands \
         FROM plan_points() ORDER BY plan, grain, period;",
    )
    .await;
    for want in [
        "| annual_budget    | year  | 2024    | 1440.0 | 1266.0 | -174.0   | landed |",
        "| annual_budget    | year  | 2025    | 1560.0 | 687.0  |          | open   |",
        "| revised_budget   | month | 2024-01 | 120.0  | 100.0  | -20.0    | landed |",
        "| revised_budget   | month | 2025-06 | 130.0  | 117.0  | -13.0    | landed |",
        "| revised_budget   | month | 2025-07 | 110.0  |        |          | ahead  |",
        "| revised_budget   | year  | 2024    | 1440.0 | 1266.0 | -174.0   | landed |",
        "| revised_budget   | year  | 2025    | 1440.0 | 687.0  |          | open   |",
        // A stock's year is its last level: 111 at the end of 2024
        // against a target of 115, 117 so far in 2025.
        "| headcount_target | year  | 2024    | 115.0  | 111.0  | -4.0     | landed |",
        "| headcount_target | year  | 2025    | 120.0  | 117.0  |          | open   |",
    ] {
        assert!(points.contains(want), "missing {want}\n{points}");
    }
    let n = points
        .lines()
        .filter(|l| l.contains("_budget") || l.contains("_target"))
        .count();
    assert_eq!(n, 2 + 24 + 2 + 2, "{points}");
    // No forecast and nothing owed: the plan goal is done.
    let goal = rows(
        &session,
        "SELECT state, why FROM next WHERE surface = 'plan';",
    )
    .await;
    assert!(goal.contains("| done "), "{goal}");

    // The forecast, flat at the last actual: 117 a month ahead with an
    // 80% band of ±8%, the year to go 702 with the same band. The
    // months ahead of the monthly budget sit inside their bands; the
    // yearly budget's 2025 lies above the band on the actual to date
    // plus the year to go, 1332.84 to 1445.16.
    session
        .execute(crate::forecast::forecast_kit())
        .await
        .expect("the measurement is declared");
    session
        .execute("SELECT metric_forecast() FROM fin;")
        .await
        .expect("the forecast lands");
    let points = rows(
        &session,
        "SELECT plan, grain, label, value, actual, round(low, 2) AS low, expected, round(high, 2) AS high, \
                stands, held_80, held_from, origin \
         FROM plan_points() WHERE stands NOT IN ('landed') ORDER BY plan, grain, period;",
    )
    .await;
    let year = line(&points, "| annual_budget    | year  | 2025");
    assert!(
        year.contains("| 1560.0 | 687.0  | 1332.84")
            && year.contains("| 1389.0")
            && year.contains("| 1445.16")
            && year.contains("| above  | 0.66    | service   | 2025-06-01 |"),
        "{points}"
    );
    let month = line(&points, "| revised_budget   | month | 2025-07");
    assert!(
        month.contains("| 110.0  |        | 107.64")
            && month.contains("| 117.0")
            && month.contains("| 126.36")
            && month.contains("| inside |")
            && month.contains("| 0.71    | service"),
        "{points}"
    );
    let monthly_year = line(&points, "| revised_budget   | year  | 2025");
    assert!(
        monthly_year.contains("| 1440.0 | 687.0") && monthly_year.contains("| inside |"),
        "{points}"
    );
    // A stock's year row takes December's band, nothing added: the flat
    // forecast's 117 at ±8%, six steps out, holds the target of 120.
    let target = line(&points, "| headcount_target | year  | 2025");
    assert!(
        target.contains("| 120.0  | 117.0  | 107.64")
            && target.contains("| 117.0")
            && target.contains("| 126.36")
            && target.contains("| inside | 0.76    | service   | 2025-06-01 |"),
        "{points}"
    );

    // The plan goal: the year the band does not hold is the gap. With
    // no scenario declared, the act is a scenario or the gap recorded
    // under the plan's key; with one, the comparison read and the
    // decision; recorded as the plan's assumption, the question stands
    // for the human and the goal is blocked.
    let goal = rows(
        &session,
        "SELECT state, act, say, why, statement, then FROM next WHERE surface = 'plan';",
    )
    .await;
    assert!(
        goal.contains("| next  | GLOSS scenario")
            && goal.contains("judge annual_budget at 2025: declare a scenario, or record the gap")
            && goal.contains("annual_budget at 2025 is 1560.0 and the forecast's 80% band for expense runs 1332.84 to 1445.16: the plan lies above it.")
            && goal.contains("\"key\": \"plan-2025\""),
        "{goal}"
    );
    session
        .execute(
            r##"GLOSS lever ON spend.amount AS $${"value": true, "grounds": "spend is set by the budget holders"}$$;
DECLARE ASPECT cut WITH $${"title": "Spend -10% from July", "x-kind": "scenario",
  "type": "object", "required": ["levers"], "properties": {"levers": {"type": "array"}}}$$ AS FACT ON DATASET;
GLOSS cut ON fin AS $${"levers": [{"column": "spend.amount",
  "factor": {"2025-07": 0.9, "2025-08": 0.9, "2025-09": 0.9, "2025-10": 0.9, "2025-11": 0.9, "2025-12": 0.9},
  "basis": "the savings plan"}]}$$;"##,
        )
        .await
        .expect("the lever and the scenario land");
    let goal = rows(
        &session,
        "SELECT state, act, say, statement FROM next WHERE surface = 'plan';",
    )
    .await;
    assert!(
        goal.contains("| next  | scenario_points")
            && goal.contains("read cut against annual_budget at 2025, and record the decision")
            && goal.contains("plan_points() WHERE plan = 'annual_budget' AND label = '2025'")
            && goal.contains("scenario_points(scenario => 'cut') WHERE metric = 'expense'"),
        "{goal}"
    );
    session
        .execute(
            r##"GLOSS annual_budget ON fin AS $${"sql": "SELECT year, planned AS value FROM yearly",
  "assumptions": [{"dimension": "definition", "key": "plan-2025",
    "assumption": "2025 is budgeted 1560 against an expected 1389; the cut closes a third of the gap",
    "basis": "plan_points() and the cut scenario", "confidence": 0.7}]}$$;"##,
        )
        .await
        .expect("the gap is recorded");
    let goal = rows(
        &session,
        "SELECT state, why FROM next WHERE surface = 'plan';",
    )
    .await;
    assert!(
        goal.contains("| blocked |")
            && goal.contains("annual_budget at 2025 lies above the forecast's band and the question stands for the human"),
        "{goal}"
    );
    let asked = rows(
        &session,
        "SELECT aspect, key FROM open_questions WHERE aspect = 'annual_budget';",
    )
    .await;
    assert!(asked.contains("| plan-2025 |"), "{asked}");
}

/// The planning fixture with two more plans on the spend: a schedule,
/// 125 a month from 2024-04 to 2025-09 — nine months of each calendar
/// year — and a budget booked against the year on the row of the month
/// it was authorized: 1440 on 2024-01, 1500 on 2025-01 and an
/// amendment of 60 on 2025-05, declared `x-covers: year`.
async fn coverage_session(stub: Arc<crate::forecast::Flat>) -> Session {
    let session = planning_session(stub).await;
    session
        .execute(
            r##"DECLARE ASPECT schedule WITH $${
  "title": "Service schedule", "x-kind": "plan", "x-plan-of": "expense"
}$$ AS QUERY ON DATASET;
GLOSS schedule ON fin AS $${"sql": "SELECT month, planned AS value FROM timetable"}$$;
DECLARE ASPECT booked_budget WITH $${
  "title": "Booked budget", "x-kind": "plan", "x-plan-of": "expense", "x-covers": "year"
}$$ AS QUERY ON DATASET;
GLOSS booked_budget ON fin AS $${"sql": "SELECT month, booked AS value FROM bookings"}$$;"##,
        )
        .await
        .expect("the two plans land");
    let timetable = Arc::new(Schema::new(vec![
        Field::new("month", DataType::Date32, false),
        Field::new("planned", DataType::Float64, false),
    ]));
    let months: Vec<i32> = (3..21)
        .map(|i| day(2024 + i / 12, (i % 12 + 1) as u32, 1))
        .collect();
    let batch = RecordBatch::try_new(
        Arc::clone(&timetable),
        vec![
            Arc::new(Date32Array::from(months)),
            Arc::new(Float64Array::from(vec![125.0; 18])),
        ],
    )
    .expect("a batch");
    register(&session, "timetable", timetable, batch).await;
    let bookings = Arc::new(Schema::new(vec![
        Field::new("month", DataType::Date32, false),
        Field::new("booked", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&bookings),
        vec![
            Arc::new(Date32Array::from(vec![
                day(2024, 1, 1),
                day(2025, 1, 1),
                day(2025, 5, 1),
            ])),
            Arc::new(Float64Array::from(vec![1440.0, 1500.0, 60.0])),
        ],
    )
    .expect("a batch");
    register(&session, "bookings", bookings, batch).await;
    session
        .execute(
            "SELECT judge_time() FROM timetable.month; SELECT judge_time() FROM bookings.month;",
        )
        .await
        .expect("the cadences are judged");
    session
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_plan_covering_part_of_a_year_is_judged_on_the_part_and_a_booked_one_on_the_year() {
    let stub = Arc::new(crate::forecast::Flat::default());
    let session = coverage_session(Arc::clone(&stub)).await;

    // The schedule covers nine months of each year. 2024's nine are
    // complete: the row is landed on the actual over April to December
    // (963 against 1125), never on the whole year's 1266. 2025's have
    // begun and not ended: partial, the actual to date over the
    // covered months, no band. The monthly budget covers its years
    // whole and says nothing.
    let points = rows(
        &session,
        "SELECT plan, grain, label, value, actual, variance, stands, covers \
         FROM plan_points() WHERE grain = 'year' ORDER BY plan, period;",
    )
    .await;
    let y2024 = line(&points, "| schedule         | year  | 2024");
    assert!(
        y2024.contains("| 1125.0 | 963.0 ")
            && y2024.contains("| -162.0 ")
            && y2024.contains("| landed  | 9 of 12 months |"),
        "{points}"
    );
    let y2025 = line(&points, "| schedule         | year  | 2025");
    assert!(
        y2025.contains("| 1125.0 | 687.0 ") && y2025.contains("| partial | 9 of 12 months |"),
        "{points}"
    );
    let whole = line(&points, "| revised_budget   | year  | 2025");
    assert!(
        whole.contains("| open    |") && !whole.contains(" of 12 "),
        "{points}"
    );
    // The booked budget reads at the year: 1440 for 2024, 1500 plus
    // the amendment for 2025, and no month row — a month's cell is
    // what was booked that month.
    let b2024 = line(&points, "| booked_budget    | year  | 2024");
    assert!(
        b2024.contains("| 1440.0 | 1266.0 | -174.0") && b2024.contains("| landed "),
        "{points}"
    );
    let b2025 = line(&points, "| booked_budget    | year  | 2025");
    assert!(
        b2025.contains("| 1560.0 | 687.0 ") && b2025.contains("| open "),
        "{points}"
    );
    let shape = rows(
        &session,
        "SELECT plan, count(*) AS n, count(*) FILTER (WHERE grain = 'month') AS months \
         FROM plan_points() WHERE plan IN ('schedule', 'booked_budget') GROUP BY plan ORDER BY plan;",
    )
    .await;
    assert!(
        line(&shape, "booked_budget").contains("| 2  | 0 ")
            && line(&shape, "schedule").contains("| 20 | 18 "),
        "{shape}"
    );

    // With the forecast: the booked year lies above the band on the
    // actual to date plus the year to go, as the yearly budget of the
    // same number does, and is a gap; the schedule's 2025 keeps no
    // band and is none. Its months ahead hold their own bands.
    session
        .execute(crate::forecast::forecast_kit())
        .await
        .expect("the measurement is declared");
    session
        .execute("SELECT metric_forecast() FROM fin;")
        .await
        .expect("the forecast lands");
    let points = rows(
        &session,
        "SELECT plan, grain, label, value, actual, round(low, 2) AS low, round(high, 2) AS high, stands, covers \
         FROM plan_points() WHERE label IN ('2025', '2025-07') ORDER BY plan, grain, period;",
    )
    .await;
    let booked = line(&points, "| booked_budget    | year  | 2025");
    assert!(
        booked.contains("| 1560.0 | 687.0  | 1332.84 | 1445.16 | above   |"),
        "{points}"
    );
    let part = line(&points, "| schedule         | year  | 2025");
    assert!(
        part.contains("| 1125.0 | 687.0  |         |         | partial | 9 of 12 months |"),
        "{points}"
    );
    let july = line(&points, "| schedule         | month | 2025-07");
    assert!(
        july.contains("| 125.0  |        | 107.64  | 126.36  | inside  |"),
        "{points}"
    );
    let gaps = rows(
        &session,
        "SELECT plan, label FROM plan_points() WHERE grain = 'year' AND stands IN ('below', 'above') ORDER BY plan;",
    )
    .await;
    assert!(
        gaps.contains("| annual_budget |")
            && gaps.contains("| booked_budget |")
            && !gaps.contains("schedule"),
        "{gaps}"
    );
}
