//! The walk's PIT is withheld where the corridor cannot be read: a
//! series that repeats exact values has a resolution, and a corridor
//! narrower than it, with the actual inside it, is the kernel's noise
//! around a value the series takes exactly. A real move keeps its PIT
//! whatever the corridor's width.

use std::sync::Arc;

use datafusion::arrow::array::{Date32Array, Float64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::datasource::MemTable;
use glossql_glossary::{Actor, ActorKind, Store};
use glossql_session::{BandRead, FunctionRuntime, Outcome, Session};

/// A kernel whose corridor is a hair around the training median, and
/// whose PIT is 0.5 inside it, 0.01 below, 0.99 above — the mechanics
/// stand-in for a model that is very sure.
#[derive(Debug)]
struct ThinKernel;

#[glossql_session::async_trait]
impl FunctionRuntime for ThinKernel {
    fn carries_model(&self) -> bool {
        true
    }

    async fn band_points(
        &self,
        reads: &[BandRead],
        alphas: &[f64],
        _pit_history: Option<&[f64]>,
    ) -> Result<Vec<(Vec<f64>, f64)>, String> {
        let mut out = Vec::new();
        for read in reads {
            let mut sorted = read.train_y.clone();
            sorted.sort_by(f64::total_cmp);
            let p50 = sorted[sorted.len() / 2];
            let q: Vec<f64> = alphas.iter().map(|a| p50 + (a - 0.5) * 2.0e-6).collect();
            let pit = if read.actual < q[0] {
                0.01
            } else if read.actual > q[4] {
                0.99
            } else {
                0.5
            };
            out.push((q, pit));
        }
        Ok(out)
    }
}

/// Days since the epoch for the 15th of (year, month).
fn mid_month(year: i32, month: u32) -> i32 {
    let day = chrono::NaiveDate::from_ymd_opt(year, month, 15).expect("a civil date");
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("the epoch");
    (day - epoch).num_days() as i32
}

/// Eighteen monthly races: one win each over a field of 20 or 22 cars
/// — a win rate that takes exactly two values — and a takings column
/// that rises by an odd step every month.
fn races() -> (Arc<Schema>, RecordBatch) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("race_date", DataType::Date32, false),
        Field::new("wins", DataType::Float64, false),
        Field::new("starts", DataType::Float64, false),
        Field::new("takings", DataType::Float64, false),
    ]));
    let months: Vec<(i32, u32)> = (0..18)
        .map(|i| (2024 + i / 12, (i % 12 + 1) as u32))
        .collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Date32Array::from(
                months
                    .iter()
                    .map(|(y, m)| mid_month(*y, *m))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(vec![1.0; 18])),
            Arc::new(Float64Array::from(
                (0..18)
                    .map(|i| if i % 3 == 2 { 22.0 } else { 20.0 })
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                (0..18).map(|i| 100.0 + 3.7 * i as f64).collect::<Vec<_>>(),
            )),
        ],
    )
    .expect("a batch");
    (schema, batch)
}

const SETUP: &str = r##"
DECLARE DATASET fin SET (purpose: 'bands on exact values');
USE fin;
DECLARE ASPECT win_rate WITH $${"title": "Win rate"}$$ AS QUERY ON DATASET;
DECLARE ASPECT takings WITH $${"title": "Takings"}$$ AS QUERY ON DATASET;
GLOSS win_rate ON fin AS $${"sql": "SELECT race_date, wins / starts AS value, wins AS num, starts AS den FROM races"}$$;
GLOSS takings ON fin AS $${"sql": "SELECT race_date, takings AS value FROM races"}$$;
"##;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_corridor_below_the_series_resolution_withholds_the_pit() {
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
    .with_runtime(Arc::new(ThinKernel));
    session.execute(SETUP).await.expect("the setup lands");
    let (schema, batch) = races();
    session
        .register_table(
            "races",
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).expect("a table")),
        )
        .await
        .expect("the table registers");

    let outcomes = session
        .execute(
            "SELECT metric, period, pit, withheld FROM metric_band_walk('fin') \
             ORDER BY metric, point_seq;",
        )
        .await
        .expect("the walk serves");
    let Some(Outcome::Rows { batches, .. }) = outcomes.into_iter().next_back() else {
        panic!("the walk produced no rows");
    };
    let walked = pretty_format_batches(&batches)
        .expect("printable")
        .to_string();

    // The takings rise 3.7 a month: the actual sits well beyond a
    // hair-thin corridor, and that is a real move — the PIT stands.
    for line in walked.lines().filter(|l| l.contains("takings")) {
        assert!(line.contains("0.99"), "a real move keeps its PIT: {line}");
        assert!(!line.contains("corridor"), "{line}");
    }
    // The win rate takes exactly 1/20 and 1/22: its resolution is their
    // gap, the corridor is a hair, and every actual is one of the two
    // values — no PIT, and the reason names the resolution.
    let win_rate: Vec<&str> = walked.lines().filter(|l| l.contains("win_rate")).collect();
    assert_eq!(win_rate.len(), 6, "{walked}");
    for line in &win_rate {
        assert!(
            line.contains("narrower than the series' resolution"),
            "a PIT read against noise is withheld: {line}"
        );
        assert!(!line.contains("| 0.5 "), "no PIT is served: {line}");
    }
}

/// A session over one registered table, walking with the thin kernel.
async fn session_over(name: &str, schema: Arc<Schema>, batch: RecordBatch, setup: &str) -> Session {
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
    .with_runtime(Arc::new(ThinKernel));
    session.execute(setup).await.expect("the setup lands");
    session
        .register_table(
            name,
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).expect("a table")),
        )
        .await
        .expect("the table registers");
    // The scratch dir outlives the session's use of it in every test
    // below; leaking it is the simplest way to say so.
    std::mem::forget(dir);
    session
}

async fn walked(session: &Session, sql: &str) -> String {
    let outcomes = session.execute(sql).await.expect("the walk serves");
    let Some(Outcome::Rows { batches, .. }) = outcomes.into_iter().next_back() else {
        panic!("the walk produced no rows");
    };
    pretty_format_batches(&batches)
        .expect("printable")
        .to_string()
}

/// A NULL date is no period: the row that carries it buckets nowhere,
/// and the walk serves the dated months as if it were absent.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_null_date_is_no_period() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("race_date", DataType::Date32, true),
        Field::new("takings", DataType::Float64, false),
    ]));
    let mut dates: Vec<Option<i32>> = (0..18)
        .map(|i| Some(mid_month(2024 + i / 12, (i % 12 + 1) as u32)))
        .collect();
    dates.push(None);
    let mut takings: Vec<f64> = (0..18).map(|i| 100.0 + 3.7 * i as f64).collect();
    takings.push(1.0e6);
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Date32Array::from(dates)),
            Arc::new(Float64Array::from(takings)),
        ],
    )
    .expect("a batch");
    let session = session_over(
        "races",
        schema,
        batch,
        r##"
DECLARE DATASET fin SET (purpose: 'a null date');
USE fin;
DECLARE ASPECT takings WITH $${"title": "Takings"}$$ AS QUERY ON DATASET;
GLOSS takings ON fin AS $${"sql": "SELECT race_date, takings AS value FROM races"}$$;
"##,
    )
    .await;
    let shown = walked(
        &session,
        "SELECT metric, applicable, trained_on, period, actual FROM metric_band_walk('fin') \
         ORDER BY point_seq;",
    )
    .await;
    let points: Vec<&str> = shown.lines().filter(|l| l.contains("takings")).collect();
    assert_eq!(points.len(), 6, "{shown}");
    for line in &points {
        assert!(line.contains("| true "), "{line}");
        // Eighteen dated months trained on; the null-dated row is none.
        assert!(line.contains("| 18 "), "{line}");
        assert!(line.contains("| 2025-"), "{line}");
        assert!(
            !line.contains("1000000"),
            "the null-dated row is no period: {line}"
        );
    }
}

/// A series the engine refuses at execution abstains on its metric
/// with the engine's reason; the other metrics walk. A microsecond
/// timestamp past 2262 is one such series: the month bucketing runs
/// in nanoseconds and cannot hold it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_series_the_engine_refuses_abstains_on_its_metric_alone() {
    use datafusion::arrow::array::TimestampMicrosecondArray;
    use datafusion::arrow::datatypes::TimeUnit;
    let schema = Arc::new(Schema::new(vec![
        Field::new("race_date", DataType::Date32, false),
        Field::new(
            "gathered",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new("takings", DataType::Float64, false),
    ]));
    let days: Vec<i32> = (0..18)
        .map(|i| mid_month(2024 + i / 12, (i % 12 + 1) as u32))
        .collect();
    let mut micros: Vec<i64> = days
        .iter()
        .map(|d| i64::from(*d) * 86_400_000_000)
        .collect();
    // Year 2890 in microseconds: the token a text export carried.
    micros[3] = 29_050_531_200_000_000;
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Date32Array::from(days)),
            Arc::new(TimestampMicrosecondArray::from(micros)),
            Arc::new(Float64Array::from(
                (0..18).map(|i| 100.0 + 3.7 * i as f64).collect::<Vec<_>>(),
            )),
        ],
    )
    .expect("a batch");
    let session = session_over(
        "races",
        schema,
        batch,
        r##"
DECLARE DATASET fin SET (purpose: 'a refused series');
USE fin;
DECLARE ASPECT takings WITH $${"title": "Takings"}$$ AS QUERY ON DATASET;
DECLARE ASPECT gathered WITH $${"title": "Gathered"}$$ AS QUERY ON DATASET;
GLOSS takings ON fin AS $${"sql": "SELECT race_date, takings AS value FROM races"}$$;
GLOSS gathered ON fin AS $${"sql": "SELECT gathered, takings AS value FROM races"}$$;
"##,
    )
    .await;
    let shown = walked(
        &session,
        "SELECT metric, applicable, reason, period FROM metric_band_walk('fin') \
         ORDER BY metric, point_seq;",
    )
    .await;
    let refused: Vec<&str> = shown.lines().filter(|l| l.contains("| gathered")).collect();
    assert_eq!(refused.len(), 1, "{shown}");
    assert!(refused[0].contains("| false "), "{shown}");
    assert!(
        refused[0].contains("out of range"),
        "the engine's reason: {shown}"
    );
    let walked_: Vec<&str> = shown.lines().filter(|l| l.contains("| takings")).collect();
    assert_eq!(walked_.len(), 6, "the other metric walks: {shown}");
}

/// Rows land daily on an axis nobody judged, and the extract stops
/// mid-month: the newest month is partial by the extract's own shape,
/// and the walk withholds its PIT. The monthly-dated races above are
/// whole at their one row and never partial.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_partial_month_is_read_from_the_extract_not_the_judgment() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("day", DataType::Date32, false),
        Field::new("spend", DataType::Float64, false),
    ]));
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("the epoch");
    let (mut days, mut spend) = (Vec::new(), Vec::new());
    for i in 0..18 {
        let (y, m) = (2024 + i / 12, (i % 12 + 1) as u32);
        let first = chrono::NaiveDate::from_ymd_opt(y, m, 1).expect("a first");
        // The last month holds eleven days only.
        let last_day = if i == 17 { 11 } else { 28 };
        for d in 0..last_day {
            let date = first + chrono::Duration::days(d);
            days.push((date - epoch).num_days() as i32);
            spend.push(5.0 + 0.001 * (i * 31 + d as i32) as f64);
        }
    }
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Date32Array::from(days)),
            Arc::new(Float64Array::from(spend)),
        ],
    )
    .expect("a batch");
    let session = session_over(
        "lines",
        schema,
        batch,
        r##"
DECLARE DATASET fin SET (purpose: 'a partial month');
USE fin;
DECLARE ASPECT spend WITH $${"title": "Spend"}$$ AS QUERY ON DATASET;
GLOSS spend ON fin AS $${"sql": "SELECT day, spend AS value FROM lines"}$$;
"##,
    )
    .await;
    let shown = walked(
        &session,
        "SELECT metric, axis_judged, period, partial, pit, withheld FROM metric_band_walk('fin') \
         ORDER BY point_seq;",
    )
    .await;
    let points: Vec<&str> = shown.lines().filter(|l| l.contains("| spend")).collect();
    assert_eq!(points.len(), 6, "{shown}");
    for line in &points[..5] {
        assert!(
            line.contains("| false "),
            "an earlier month is whole: {line}"
        );
        assert!(!line.contains("partial:"), "{line}");
    }
    let newest = points[5];
    assert!(newest.contains("| 2025-06 "), "{newest}");
    assert!(
        newest.contains(
            "partial: the extract ends 2025-06-11, before the period's last day 2025-06-30"
        ),
        "{newest}"
    );
    // The axis is unjudged and the month is withheld anyway.
    assert!(newest.starts_with("| spend  | false"), "{newest}");
}
