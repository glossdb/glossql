//! The band plane end to end: the shipped metric_bands body walked
//! over synthetic monthly series through a real session — the walk is
//! the metric_band_walk door and the body is SQL — and the band_breach
//! detector adjudicating the walk through its witness at the ATTEST
//! read. Live tests: the model answers from a running service, so they
//! run under `--ignored` when `GLOSSQL_E2E_FORECAST_URL` and
//! `GLOSSQL_E2E_MISFIT_URL` name one (the bearer in
//! `GLOSSQL_E2E_FORECAST_TOKEN` and `GLOSSQL_E2E_MISFIT_TOKEN`) and skip
//! with a message otherwise:
//! `cargo test -p glossql-scripts bands -- --ignored`. The numeric
//! fidelity of the model is the service's own suite; here the
//! contract is shape, ordering, and policy.

use std::path::Path;
use std::sync::Arc;

use datafusion::arrow::array::{Float64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use glossql_scripts::KernelRuntime;
use glossql_session::{FORECAST, FunctionRuntime, MISFIT, POINTS};
use serde_json::{Value, json};

/// The service under test, when the environment names one per point
/// (`GLOSSQL_E2E_<POINT>_URL`, the bearer in `_TOKEN`); None when it
/// names none, and a runtime missing a point refuses that point's test.
fn live() -> Option<Arc<KernelRuntime>> {
    let mut rt = KernelRuntime::native();
    let mut named = false;
    for point in POINTS {
        let variable = |suffix: &str| {
            std::env::var(format!(
                "GLOSSQL_E2E_{}_{suffix}",
                point.to_ascii_uppercase()
            ))
            .ok()
            .filter(|v| !v.trim().is_empty())
        };
        if let Some(url) = variable("URL") {
            let bearer = glossql_scripts::Bearer::from_env(variable("TOKEN").as_deref(), None);
            rt = rt
                .with_point(point, &url, bearer)
                .expect("a service address");
            named = true;
        }
    }
    named.then(|| Arc::new(rt))
}

/// Date32 day offsets for each month's first day, 2024-01 through
/// 2025-06 (2024 is a leap year); 19723 = 2024-01-01.
pub(super) const FIRSTS: [i32; 18] = [
    0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366, 397, 425, 456, 486, 517,
];

/// A session over a lake in `dir`, the walk declared from the shipped
/// body, `lines` and `levels` landed from the given rows; the temporal
/// verdict `judge_time` serves names a month cadence.
pub(super) async fn walk_session(
    dir: &Path,
    rt: Arc<KernelRuntime>,
    tables: Vec<(&str, Vec<i32>, Vec<f64>)>,
    glosses: &[&str],
) -> glossql_session::Session {
    walk_session_judged(dir, rt, tables, glosses, "month").await
}

/// [`walk_session`] with the cadence `judge_time` names — one aspect
/// has one producer, so the cadence is the fixture's, not a second
/// function's.
async fn walk_session_judged(
    dir: &Path,
    rt: Arc<KernelRuntime>,
    tables: Vec<(&str, Vec<i32>, Vec<f64>)>,
    glosses: &[&str],
    cadence: &str,
) -> glossql_session::Session {
    use datafusion::datasource::MemTable;
    use glossql_catalog::Lake;
    use glossql_glossary::{Actor, ActorKind, Store};

    let lake = Lake::open(&dir.join("catalog.db"), &dir.join("warehouse"))
        .await
        .unwrap();
    let store = Store::open(lake).await.unwrap();
    let session = glossql_session::Session::new(
        store,
        Actor {
            kind: ActorKind::Agent,
            id: "t".into(),
        },
    )
    .unwrap()
    .with_runtime(rt);
    session
        .execute("DECLARE DATASET fin SET (purpose: 'band walks'); USE fin;")
        .await
        .unwrap();
    for (name, dates, values) in tables {
        let schema = Arc::new(Schema::new(vec![
            Field::new("date", DataType::Date32, false),
            Field::new("value", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(datafusion::arrow::array::Date32Array::from(dates)),
                Arc::new(Float64Array::from(values)),
            ],
        )
        .unwrap();
        session
            .register_table(
                name,
                Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
            )
            .await
            .unwrap();
    }
    // The `next` read answers under the shipped `cube` aspect; the kit
    // declares it, so the fixture cuts that declaration from the kit.
    let kit = glossql_scripts::library::KIT;
    let cube_start = kit
        .find("DECLARE ASPECT cube")
        .expect("the kit ships the cube aspect");
    let cube_len = kit[cube_start..]
        .find("AS FACT ON DATASET;")
        .expect("the declaration closes")
        + "AS FACT ON DATASET;".len();
    let cube = &kit[cube_start..cube_start + cube_len];
    let declarations = glossql_scripts::library::splice(&format!(
        r#"{cube}
           DECLARE ASPECT metric_bands WITH $${{
             "type": "object", "required": ["applicable"],
             "properties": {{"applicable": {{"type": "boolean"}},
                            "metrics": {{"type": "array"}}}}}}$$ AS MEASUREMENT ON DATASET;
           DECLARE ASPECT temporal_profile WITH $${{
             "type": "object", "required": ["applicable"],
             "properties": {{"applicable": {{"type": "boolean"}}}}}}$$ AS MEASUREMENT ON COLUMN;
           DECLARE FUNCTION judge_time FOR GLOBAL AS
             $$SELECT true AS applicable, '{cadence}' AS granularity,
                      named_struct('ratio', 1.0) AS completeness$$
             RETURNS temporal_profile;
           DECLARE FUNCTION metric_bands FOR GLOBAL AS $$metric_bands.sql$$
             RETURNS metric_bands;
           DECLARE FUNCTION band_breach FOR GLOBAL AS $$band_breach.sql$$;
           DECLARE WITNESS bands_w ON metric_bands DETECTOR band_breach THRESHOLD 0.98;
           DECLARE ASPECT metric_forecast WITH $${{
             "type": "object", "required": ["applicable"],
             "properties": {{"applicable": {{"type": "boolean"}},
                            "metrics": {{"type": "array"}}}}}}$$ AS MEASUREMENT ON DATASET;
           DECLARE FUNCTION metric_forecast FOR GLOBAL AS $$metric_forecast.sql$$
             RETURNS metric_forecast;
           DECLARE WITNESS forecast_w ON metric_forecast;"#
    ))
    .expect("shipped body splices");
    session.execute(&declarations).await.unwrap();
    for g in glosses {
        session.execute(g).await.unwrap();
    }
    session
        .execute("SELECT metric_bands() FROM fin;")
        .await
        .unwrap();
    session
}

pub(super) async fn walked(session: &glossql_session::Session) -> Value {
    let outcomes = session
        .execute("SELECT value FROM GLOSSARY(fin::metric_bands) WHERE state = 'current';")
        .await
        .unwrap();
    let Some(glossql_session::Outcome::Rows { batches, .. }) = outcomes.last() else {
        panic!("a value row")
    };
    let batch = batches.iter().find(|b| b.num_rows() > 0).unwrap();
    let text = datafusion::arrow::util::display::array_value_to_string(batch.column(0), 0).unwrap();
    serde_json::from_str(&text).unwrap()
}

#[ignore = "needs a service: GLOSSQL_E2E_FORECAST_URL / GLOSSQL_E2E_MISFIT_URL"]
#[tokio::test(flavor = "multi_thread")]
async fn metric_bands_walks_and_reads_the_breach() {
    let Some(rt) = live().filter(|rt| rt.serves(FORECAST)) else {
        eprintln!("skipping: GLOSSQL_E2E_FORECAST_URL is not set");
        return;
    };
    let dir = tempfile::tempdir().unwrap();

    // A year and a half of a rising monthly flow with seasonality; the
    // last month is an obvious breach. The stock series is a rising
    // level whose last month collapses — a lower breach.
    let months = 18usize;
    let (mut flow, mut stock) = (Vec::new(), Vec::new());
    for i in 0..months {
        let seasonal = if i % 12 == 11 { 20.0 } else { 0.0 };
        flow.push(100.0 + 3.0 * i as f64 + seasonal);
        stock.push(1000.0 + 5.0 * i as f64);
    }
    *flow.last_mut().unwrap() = 500.0;
    *stock.last_mut().unwrap() = 200.0;
    let dates: Vec<i32> = FIRSTS.iter().map(|f| 19723 + f).collect();

    let session = walk_session(
        dir.path(),
        Arc::clone(&rt),
        vec![
            ("lines", dates.clone(), flow),
            ("levels", dates, stock),
        ],
        &[
            r#"DECLARE ASPECT revenue WITH $${"title": "Revenue"}$$ AS QUERY ON DATASET;"#,
            r#"DECLARE ASPECT inventory WITH $${"title": "Inventory"}$$ AS QUERY ON DATASET;"#,
            r#"DECLARE ASPECT share WITH $${"title": "Share"}$$ AS QUERY ON DATASET;"#,
            r#"GLOSS revenue ON fin AS $${"sql": "SELECT date, value FROM lines"}$$;"#,
            r#"GLOSS inventory ON fin AS $${"sql": "SELECT date, value FROM levels", "behavior": "stock"}$$;"#,
            // A ratio serves both halves; the month reads as the
            // division of their sums.
            r#"GLOSS share ON fin AS $${"sql": "SELECT date, value AS num, 2.0 AS den, value / 2.0 AS value FROM lines"}$$;"#,
            // One side profiled, one not: the walk anchors on the judged
            // column where a verdict stands and on the first date column
            // where none does. Both name the same column here, so the
            // series and the bands below are identical either way — what
            // differs is whether the measurement says it was judged.
            r#"SELECT judge_time() FROM lines.date;"#,
        ],
    )
    .await;
    let out = walked(&session).await;

    assert_eq!(out["applicable"], json!(true));
    let by_name = |name: &str| -> Value {
        out["metrics"]
            .as_array()
            .unwrap()
            .iter()
            .find(|m| m["metric"] == json!(name))
            .unwrap_or_else(|| panic!("metric {name} missing"))
            .clone()
    };

    // The axis the walk took, and whether a verdict named it. `revenue`
    // reads the profiled `lines.date`, `inventory` the unprofiled
    // `levels.date` — the cadence stays month for both, whatever the
    // axis.
    assert_eq!(by_name("revenue")["axis"], json!("date"));
    assert_eq!(by_name("revenue")["axis_judged"], json!(true));
    assert_eq!(by_name("inventory")["axis"], json!("date"));
    assert_eq!(by_name("inventory")["axis_judged"], json!(false));

    for (name, aggregation) in [
        ("revenue", "sum"),
        ("inventory", "latest-sum"),
        ("share", "ratio-of-sums"),
    ] {
        let metric = by_name(name);
        assert_eq!(metric["grain"], json!("month"));
        assert_eq!(metric["aggregation"], json!(aggregation), "{name}");
        let points = metric["points"].as_array().unwrap();
        assert_eq!(points.len(), 6, "the walk covers the last six months");
        for p in points {
            let (p05, p10, p50, p90, p95) = (
                p["p05"].as_f64().unwrap(),
                p["p10"].as_f64().unwrap(),
                p["p50"].as_f64().unwrap(),
                p["p90"].as_f64().unwrap(),
                p["p95"].as_f64().unwrap(),
            );
            assert!(p05 <= p10 && p10 <= p50 && p50 <= p90 && p90 <= p95);
            let pit = p["pit"].as_f64().unwrap();
            assert!((0.0..=1.0).contains(&pit));
        }
        let inside = &points[points.len() - 3];
        let pit = inside["pit"].as_f64().unwrap();
        assert!(
            (0.02..0.98).contains(&pit),
            "{name}: an in-pattern month, pit {pit}"
        );
    }

    // The manufactured breaches: the flow's 500 is an extreme upper
    // breach; the stock's collapse to 200 an extreme lower one.
    let flow_last = by_name("revenue")["points"][5]["pit"].as_f64().unwrap();
    assert!(flow_last > 0.95, "flow breach month, pit {flow_last}");
    let stock_last = by_name("inventory")["points"][5]["pit"].as_f64().unwrap();
    assert!(stock_last < 0.05, "stock breach month, pit {stock_last}");

    // The recorded walk as rows: one per metric per point, with the
    // displacement the detector scores — the breach is the newest
    // revenue point, and the read names it without re-running the
    // model.
    let points = session
        .execute(
            "SELECT metric, period, partial, displacement, current FROM band_points() \
             WHERE metric = 'revenue' ORDER BY period;",
        )
        .await
        .unwrap();
    let Some(glossql_session::Outcome::Rows { batches, .. }) = points.into_iter().next_back()
    else {
        panic!("no rows");
    };
    let shown = datafusion::arrow::util::pretty::pretty_format_batches(&batches)
        .unwrap()
        .to_string();
    let rows: Vec<&str> = shown
        .lines()
        .filter(|l| l.starts_with("| revenue"))
        .collect();
    assert_eq!(rows.len(), 6, "{shown}");
    assert!(
        rows[5].contains("2025-06") && rows[5].contains("| true"),
        "{shown}"
    );
    let newest: f64 = rows[5]
        .split('|')
        .map(str::trim)
        .filter_map(|c| c.parse::<f64>().ok())
        .next()
        .expect("a displacement");
    assert!(newest > 0.98, "{shown}");

    // The detector adjudicates the walk through its witness at the
    // ATTEST read: score is the worst displacement |2·pit − 1| across
    // each metric's latest walked point, raised to the number of
    // metrics scored — three here; band reads against the edges with
    // the witness's 0.98 red line.
    let worst = ["revenue", "inventory", "share"]
        .iter()
        .map(|name| {
            let pit = by_name(name)["points"][5]["pit"].as_f64().unwrap();
            (2.0 * pit - 1.0).abs()
        })
        .fold(0.0f64, f64::max)
        .powi(3);
    let outcomes = session
        .execute("SELECT band, score FROM ATTEST(fin::metric_bands);")
        .await
        .unwrap();
    let Some(glossql_session::Outcome::Rows { batches, .. }) = outcomes.last() else {
        panic!("attest rows")
    };
    let batch = batches
        .iter()
        .find(|b| b.num_rows() > 0)
        .expect("a verdict");
    let text = |c: usize| {
        datafusion::arrow::util::display::array_value_to_string(batch.column(c), 0).unwrap()
    };
    let (band, score) = (text(0), text(1).parse::<f64>().unwrap());
    assert!(
        (score - worst).abs() < 1e-9,
        "verdict score {score} against the walk's worst displacement {worst}"
    );
    let expected = match worst {
        w if w <= 0.8 => "green",
        w if w <= 0.9 => "yellow",
        w if w <= 0.98 => "orange",
        _ => "red",
    };
    assert_eq!(band, expected, "band at score {score}");

    // The bands goal names the breach: the metric whose newest complete
    // point sits furthest from its corridor, and that point's period —
    // read from the recorded walk, never a re-run.
    let outcomes = session
        .execute("SELECT say, statement FROM next WHERE surface = 'bands';")
        .await
        .unwrap();
    let Some(glossql_session::Outcome::Rows { batches, .. }) = outcomes.last() else {
        panic!("next rows")
    };
    let batch = batches.iter().find(|b| b.num_rows() > 0).expect("a row");
    let text = |c: usize| {
        datafusion::arrow::util::display::array_value_to_string(batch.column(c), 0).unwrap()
    };
    let (say, statement) = (text(0), text(1));
    let breached = ["revenue", "inventory"]
        .iter()
        .max_by(|a, b| {
            let d = |name: &str| {
                let pit = by_name(name)["points"][5]["pit"].as_f64().unwrap();
                (2.0 * pit - 1.0).abs()
            };
            d(a).total_cmp(&d(b))
        })
        .unwrap();
    assert_eq!(say, format!("judge {breached}, red at 2025-06"), "{say}");
    assert!(
        statement.contains(&format!("WHERE metric = '{breached}'")),
        "{statement}"
    );

    // The judgment recorded as an assumption under the band's key is
    // the question for the human, and the goal is blocked on it — and
    // the re-record owes the walk again, as only a grounding does.
    let body = if *breached == "revenue" {
        r#"{"sql": "SELECT date, value FROM lines", "assumptions": [{"dimension": "definition", "key": "band-2025-06", "assumption": "a shift: the June campaign", "basis": "the sales calendar", "confidence": 0.7}]}"#
    } else {
        r#"{"sql": "SELECT date, value FROM levels", "behavior": "stock", "assumptions": [{"dimension": "definition", "key": "band-2025-06", "assumption": "a shift: the warehouse move", "basis": "operations", "confidence": 0.7}]}"#
    };
    session
        .execute(&format!("GLOSS {breached} ON fin AS $${body}$$;"))
        .await
        .unwrap();
    let outcomes = session
        .execute("SELECT state, why FROM next WHERE surface = 'bands';")
        .await
        .unwrap();
    let Some(glossql_session::Outcome::Rows { batches, .. }) = outcomes.last() else {
        panic!("next rows")
    };
    let batch = batches.iter().find(|b| b.num_rows() > 0).expect("a row");
    let text = |c: usize| {
        datafusion::arrow::util::display::array_value_to_string(batch.column(c), 0).unwrap()
    };
    let (state, why) = (text(0), text(1));
    assert!(
        state == "next" && why.contains("run the walk again")
            || state == "blocked" && why.contains("the question stands"),
        "{state}: {why}"
    );
}

/// The multi-row stock regression: a stock's walk actuals must be
/// the month's latest-date SUM, not one arbitrary row. Three product
/// rows stand at each month's day 25 (sum 1750 + 30·m); a day-5 decoy
/// snapshot per month must be excluded by the latest-date rank.
#[ignore = "needs a service: GLOSSQL_E2E_FORECAST_URL / GLOSSQL_E2E_MISFIT_URL"]
#[tokio::test(flavor = "multi_thread")]
async fn the_stock_walk_sums_the_months_latest_snapshot() {
    let Some(rt) = live().filter(|rt| rt.serves(FORECAST)) else {
        eprintln!("skipping: GLOSSQL_E2E_FORECAST_URL is not set");
        return;
    };
    let dir = tempfile::tempdir().unwrap();

    let months = 18usize;
    let (mut dates, mut values) = (Vec::new(), Vec::new());
    for (m, &first) in FIRSTS.iter().enumerate().take(months) {
        let month_start = 19723 + first;
        for v in [1000.0, 500.0, 250.0] {
            dates.push(month_start + 24);
            values.push(v + 10.0 * m as f64);
        }
        dates.push(month_start + 4);
        values.push(9_000_000.0);
    }

    let session = walk_session(
        dir.path(),
        Arc::clone(&rt),
        vec![("levels", dates, values)],
        &[
            r#"DECLARE ASPECT inventory WITH $${"title": "Inventory"}$$ AS QUERY ON DATASET;"#,
            r#"GLOSS inventory ON fin AS $${"sql": "SELECT date, value FROM levels", "behavior": "stock"}$$;"#,
        ],
    )
    .await;
    let out = walked(&session).await;

    let metric = out["metrics"]
        .as_array()
        .unwrap()
        .iter()
        .find(|m| m["metric"] == json!("inventory"))
        .expect("inventory walked")
        .clone();
    assert_eq!(metric["aggregation"], json!("latest-sum"));
    let points = metric["points"].as_array().unwrap();
    assert_eq!(points.len(), 6, "the walk covers the last six months");
    for (i, p) in points.iter().enumerate() {
        let m = (months - 6 + i) as f64;
        let expected = 1750.0 + 30.0 * m;
        let actual = p["actual"].as_f64().unwrap();
        assert!(
            (actual - expected).abs() < 1e-6,
            "month index {m}: actual {actual}, expected the day-25 sum {expected}"
        );
    }
}

#[ignore = "needs a service: GLOSSQL_E2E_FORECAST_URL / GLOSSQL_E2E_MISFIT_URL"]
#[tokio::test(flavor = "multi_thread")]
async fn misfit_scores_rank_the_planted_violator_with_the_real_density() {
    // The misfit door's seam (fixture 20) against the real chain-rule
    // density: a frame whose columns cohere (y ≈ 2x, z = x + y) with
    // one planted row that betrays the relation while every marginal
    // value stays in range — the eval's shuffled-pairing shape. The
    // score's fidelity is the service's own business; here the
    // contract is the answer's shape and that the violator ranks
    // strangest.
    let Some(rt) = live() else {
        eprintln!("skipping: no GLOSSQL_E2E_<POINT>_URL is set");
        return;
    };
    if !rt.serves(MISFIT) {
        eprintln!("skipping: GLOSSQL_E2E_MISFIT_URL is not set");
        return;
    }

    let n = 40;
    let (mut xs, mut ys, mut zs) = (Vec::new(), Vec::new(), Vec::new());
    for i in 0..n {
        let a = 10.0 + (i as f64) * 37.0 % 90.0;
        let (a, b) = if i == 17 {
            (15.0, 170.0) // in-range marginals, impossible pairing
        } else {
            (a, 2.0 * a + (i as f64 % 5.0))
        };
        xs.push(a);
        ys.push(b);
        zs.push(a + b);
    }
    let frame = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("x", DataType::Float64, false),
            Field::new("y", DataType::Float64, false),
            Field::new("z", DataType::Float64, false),
        ])),
        vec![
            Arc::new(Float64Array::from(xs)),
            Arc::new(Float64Array::from(ys)),
            Arc::new(Float64Array::from(zs)),
        ],
    )
    .unwrap();
    let answered = rt
        .table(MISFIT, frame, json!({"cells": false}))
        .await
        .unwrap();
    assert_eq!(answered.answer["service"], json!("glossthesi"));
    assert_eq!(answered.table.num_rows(), n);
    let score = answered
        .table
        .column_by_name("score")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let worst = (0..n)
        .min_by(|a, b| score.value(*a).total_cmp(&score.value(*b)))
        .unwrap();
    assert_eq!(worst, 17, "the violator is the strangest row");
}

/// Rows land daily and the extract stops mid-month: the newest month
/// is partial. Scored, its short sum would read as a breach that is
/// only the calendar; the walk serves it marked, withholds its PIT,
/// and the detector scores the month before it.
#[ignore = "needs a service: GLOSSQL_E2E_FORECAST_URL / GLOSSQL_E2E_MISFIT_URL"]
#[tokio::test(flavor = "multi_thread")]
async fn a_partial_trailing_month_is_withheld_and_the_month_before_scores() {
    let Some(rt) = live().filter(|rt| rt.serves(FORECAST)) else {
        eprintln!("skipping: GLOSSQL_E2E_FORECAST_URL is not set");
        return;
    };
    let dir = tempfile::tempdir().unwrap();

    // Eighteen months of daily rows on a slow drift — never a repeated
    // monthly sum, so no month sits on the grid the walk withholds
    // for; the last month holds eleven days only (2025-06-01 through
    // 2025-06-11).
    let month_starts: Vec<i32> = FIRSTS.iter().copied().chain([547]).collect();
    let daily = |d: i32| 5.0 + 0.001 * d as f64;
    let (mut dates, mut values) = (Vec::new(), Vec::new());
    for i in 0..18 {
        let end = if i == 17 {
            month_starts[i] + 11
        } else {
            month_starts[i + 1]
        };
        for d in month_starts[i]..end {
            dates.push(19723 + d);
            values.push(daily(d));
        }
    }
    let partial_sum: f64 = (month_starts[17]..month_starts[17] + 11).map(daily).sum();

    let session = walk_session_judged(
        dir.path(),
        Arc::clone(&rt),
        vec![("lines", dates, values)],
        &[
            r#"DECLARE ASPECT spend WITH $${"title": "Spend"}$$ AS QUERY ON DATASET;"#,
            r#"GLOSS spend ON fin AS $${"sql": "SELECT date, value FROM lines"}$$;"#,
            r#"SELECT judge_time() FROM lines.date;"#,
        ],
        "day",
    )
    .await;
    let out = walked(&session).await;

    let metric = out["metrics"]
        .as_array()
        .unwrap()
        .iter()
        .find(|m| m["metric"] == json!("spend"))
        .expect("spend walked")
        .clone();
    let points = metric["points"].as_array().unwrap();
    assert_eq!(points.len(), 6, "the walk covers the last six months");
    let newest = &points[5];
    assert_eq!(newest["period"], json!("2025-06"));
    assert_eq!(newest["partial"], json!(true));
    assert!(newest["pit"].is_null(), "{newest}");
    assert_eq!(
        newest["withheld"],
        json!("partial: the extract ends 2025-06-11, before the period's last day 2025-06-30")
    );
    assert!((newest["actual"].as_f64().unwrap() - partial_sum).abs() < 1e-6);
    let before = &points[4];
    assert_eq!(before["period"], json!("2025-05"));
    assert_eq!(before["partial"], json!(false), "{before}");
    let pit = before["pit"].as_f64().expect("the complete month scores");

    // The detector reads the newest complete month: its score is that
    // month's displacement, not the partial one's.
    let verdict = session
        .execute("SELECT band, score FROM ATTEST(fin::metric_bands);")
        .await
        .unwrap();
    let Some(glossql_session::Outcome::Rows { batches, .. }) = verdict.into_iter().next_back()
    else {
        panic!("no verdict");
    };
    let shown = datafusion::arrow::util::pretty::pretty_format_batches(&batches)
        .unwrap()
        .to_string();
    let displacement = (2.0 * pit - 1.0).abs();
    let score: f64 = shown
        .lines()
        .filter(|l| l.starts_with("| ") && !l.contains("band"))
        .flat_map(|l| {
            l.split('|')
                .map(str::trim)
                .filter_map(|c| c.parse::<f64>().ok())
        })
        .next()
        .expect("a score");
    assert!(
        (score - displacement).abs() < 1e-6,
        "score {score} against the complete month's displacement {displacement}: {shown}"
    );
}

/// The forecast at an origin against the service: the cube's cells and
/// the periods ahead go as one table, and what lands is the model's
/// bands per step and span with the service's name on it.
#[ignore = "needs a service: GLOSSQL_E2E_FORECAST_URL"]
#[tokio::test(flavor = "multi_thread")]
async fn metric_forecast_lands_against_the_service() {
    let Some(rt) = live().filter(|rt| rt.serves(FORECAST)) else {
        eprintln!("skipping: GLOSSQL_E2E_FORECAST_URL is not set");
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    // A year and a half of a rising monthly flow with seasonality, and a
    // rising stock level.
    let months = 18usize;
    let (mut flow, mut stock) = (Vec::new(), Vec::new());
    for i in 0..months {
        let seasonal = if i % 12 == 11 { 20.0 } else { 0.0 };
        flow.push(100.0 + 3.0 * i as f64 + seasonal);
        stock.push(1000.0 + 5.0 * i as f64);
    }
    let dates: Vec<i32> = FIRSTS.iter().map(|f| 19723 + f).collect();
    let session = walk_session(
        dir.path(),
        Arc::clone(&rt),
        vec![
            ("lines", dates.clone(), flow),
            ("levels", dates, stock),
        ],
        &[
            r#"DECLARE ASPECT revenue WITH $${"title": "Revenue"}$$ AS QUERY ON DATASET;"#,
            r#"DECLARE ASPECT inventory WITH $${"title": "Inventory"}$$ AS QUERY ON DATASET;"#,
            r#"GLOSS revenue ON fin AS $${"sql": "SELECT date, value FROM lines"}$$;"#,
            r#"GLOSS inventory ON fin AS $${"sql": "SELECT date, value FROM levels", "behavior": "stock"}$$;"#,
            r#"SELECT judge_time() FROM lines.date;"#,
            r#"SELECT judge_time() FROM levels.date;"#,
        ],
    )
    .await;
    session
        .execute("SELECT metric_forecast() FROM fin;")
        .await
        .unwrap();
    let outcomes = session
        .execute(
            "SELECT metric, origin, step, span, to_go, period, p05, p10, p50, p90, p95, served_by \
             FROM forecast_points() ORDER BY metric, point_seq;",
        )
        .await
        .unwrap();
    let Some(glossql_session::Outcome::Rows { batches, .. }) = outcomes.last() else {
        panic!("rows")
    };
    let text = datafusion::arrow::util::pretty::pretty_format_batches(batches)
        .unwrap()
        .to_string();
    eprintln!("{text}");
    let rows: Vec<Vec<String>> = text
        .lines()
        .filter(|l| l.contains("2025-06-01"))
        .map(|l| {
            // Cells by position: an empty span or to_go stays a cell.
            l.trim()
                .trim_matches('|')
                .split('|')
                .map(|c| c.trim().to_string())
                .collect()
        })
        .collect();
    // The flow: twelve steps, the quarter and the year to go. The
    // stock: twelve steps and no total.
    assert_eq!(
        rows.iter().filter(|r| r[0] == "revenue").count(),
        14,
        "{text}"
    );
    assert_eq!(
        rows.iter().filter(|r| r[0] == "inventory").count(),
        12,
        "{text}"
    );
    for r in &rows {
        let q: Vec<f64> = r[6..11].iter().map(|c| c.parse().unwrap()).collect();
        assert!(
            q.windows(2).all(|w| w[0] <= w[1]),
            "ordered quantiles: {r:?}"
        );
        assert!(r[11].starts_with("glossthesi"), "{r:?}");
    }
    // The first month ahead continues the trend: the median within a
    // fifth of the line.
    let first = rows
        .iter()
        .find(|r| r[0] == "revenue" && r[2] == "1")
        .unwrap();
    assert_eq!(first[5], "2025-07-01");
    let p50: f64 = first[8].parse().unwrap();
    assert!(
        (124.0..=186.0).contains(&p50),
        "the trend sits near 154: {p50}"
    );
}
