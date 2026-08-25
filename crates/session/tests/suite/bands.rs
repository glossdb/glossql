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
use glossql_session::{FunctionRuntime, Matrix, Outcome, Session};

/// A kernel whose corridor is a hair around the training median, and
/// whose PIT is 0.5 inside it, 0.01 below, 0.99 above — the mechanics
/// stand-in for a model that is very sure.
#[derive(Debug)]
struct ThinKernel;

impl FunctionRuntime for ThinKernel {
    fn band_point(
        &self,
        _train: Matrix<'_>,
        train_y: &[f64],
        _test_x: &[f64],
        alphas: &[f64],
        actual: f64,
    ) -> Result<(Vec<f64>, f64), String> {
        let mut sorted = train_y.to_vec();
        sorted.sort_by(f64::total_cmp);
        let p50 = sorted[sorted.len() / 2];
        let q: Vec<f64> = alphas.iter().map(|a| p50 + (a - 0.5) * 2.0e-6).collect();
        let pit = if actual < q[0] {
            0.01
        } else if actual > q[4] {
            0.99
        } else {
            0.5
        };
        Ok((q, pit))
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
    let Some(Outcome::Rows(batches)) = outcomes.into_iter().next_back() else {
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
