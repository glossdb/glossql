//! The window ladder: a grain's series is served over the ladder's
//! rung for that grain, measured back from the data's edge, and a rung
//! of `n` periods is `n` whole buckets, the edge's bucket the last. The
//! cut is on the bucket, never on a cell's own date, so a day inside
//! the bucket before the window is left out at the month and at the
//! quarter.

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

/// A day floor and a rung at the day, the month and the quarter; one
/// flow judged daily by a stub.
fn setup() -> &'static str {
    r##"
DECLARE DATASET ops SET (purpose: 'the window ladder');
USE ops;
DECLARE ASPECT cube WITH $${"type": "object", "properties": {
  "resolution": {"default": "day"},
  "windows": {"type": "object", "properties": {
    "day": {"default": "10 days"}, "month": {"default": "3 months"}, "quarter": {"default": "6 months"}}}}}$$
AS FACT ON DATASET;
DECLARE ASPECT temporal_profile WITH $${"type": "object", "required": ["applicable"],
  "properties": {"applicable": {"type": "boolean"}}}$$ AS MEASUREMENT ON COLUMN;
DECLARE FUNCTION judge_time FOR GLOBAL AS
  $$SELECT true AS applicable, 'day' AS granularity, named_struct('ratio', 1.0) AS completeness$$
  RETURNS temporal_profile;
DECLARE ASPECT tickets WITH $${"title": "Tickets sold", "x-kind": "measure"}$$ AS QUERY ON DATASET;
GLOSS tickets ON ops AS $${"sql": "SELECT d, sold AS value FROM sales"}$$;
"##
}

/// One ticket a day from 2024-03-15 to 2024-12-15, 276 days.
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
    let first = day(2024, 3, 15);
    let days: Vec<i32> = (0..276).map(|i| first + i).collect();
    let schema = Arc::new(Schema::new(vec![
        Field::new("d", DataType::Date32, false),
        Field::new("sold", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Date32Array::from(days)),
            Arc::new(Float64Array::from(vec![1.0; 276])),
        ],
    )
    .expect("a batch");
    session
        .register_table(
            "sales",
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).expect("a table")),
        )
        .await
        .expect("the table registers");
    session
        .execute("SELECT judge_time() FROM sales.d;")
        .await
        .expect("the cadence is judged");
    session
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

/// The edge is 2024-12-15. The month rung of three months runs from
/// the edge's month less three, 2024-09-01, and serves the three whole
/// months after it: October, November and December, 31, 30 and 15
/// days — never a September of the days after the first. The quarter
/// rung of six months runs from 2024-04-01 and serves the third and
/// the fourth quarter, 92 and 76 days. The day rung serves the ten
/// days after 2024-12-05. The count beside the series agrees: of the
/// 276 days, 266 stand outside the day rung.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_windowed_series_serves_as_many_whole_buckets_as_its_rung() {
    let session = session().await;
    let shape = "SELECT count(*) AS n, min(period) AS first, sum(value) AS total \
                 FROM metric_series(grain => '{g}') WHERE dimension = ''";
    let month = rows(&session, &shape.replace("{g}", "month")).await;
    assert!(
        month.contains("| 3 | 2024-10-01T00:00:00 | 76.0  |"),
        "{month}"
    );
    let quarter = rows(&session, &shape.replace("{g}", "quarter")).await;
    assert!(
        quarter.contains("| 2 | 2024-07-01T00:00:00 | 168.0 |"),
        "{quarter}"
    );
    let days = rows(&session, &shape.replace("{g}", "day")).await;
    assert!(
        days.contains("| 10 | 2024-12-06T00:00:00 | 10.0  |"),
        "{days}"
    );
    let own = rows(
        &session,
        "SELECT count(*) AS n, min(period) AS first FROM metric_series() WHERE dimension = ''",
    )
    .await;
    assert!(own.contains("| 10 | 2024-12-06T00:00:00 |"), "{own}");
    let fact = rows(
        &session,
        "SELECT resolution, window, outside FROM metric_axes() WHERE metric = 'tickets'",
    )
    .await;
    assert!(
        fact.contains("| day        | 10 days | 266     |"),
        "{fact}"
    );
}
