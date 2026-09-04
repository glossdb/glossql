//! The cube over a real session, through its two reads — nothing
//! lands: `metric_series(grain => …)` serves the cells, `metric_axes()`
//! the fact row per metric. Grounded metrics with served dimension
//! columns, a disclosed rival, a marked stock and a ratio; the judged
//! verdicts the cube reads come from judge functions serving fixed
//! answers, so the tests exercise the cube's read policy, not the
//! shipped profilers. No model, no weights — the cube is plain SQL
//! policy over the judged surface, under the shipped `cube` aspect.

use std::sync::Arc;

use datafusion::arrow::array::{
    Date32Array, Float64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::datasource::MemTable;
use glossql_catalog::Lake;
use glossql_glossary::{Actor, ActorKind, Store};
use glossql_session::{CubeCache, Outcome, Session};

/// Date32 day offsets for each month's first day from 2024-01 (leap)
/// through 2026-06; 19723 = 2024-01-01.
const MONTH_STARTS: [i32; 30] = [
    0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, // 2024
    366, 397, 425, 456, 486, 517, 547, 578, 609, 639, 670, 700, // 2025
    731, 762, 790, 821, 851, 882, // 2026
];

/// The SHIPPED `cube` aspect, cut from the KPI kit the binary
/// bootstraps — the floor and the ladder a real workspace computes
/// under, defaults included.
fn shipped_cube_declaration() -> &'static str {
    let kit = glossql_scripts::library::KIT;
    let start = kit
        .find("DECLARE ASPECT cube")
        .expect("the kit ships the cube aspect");
    let len = kit[start..]
        .find("AS FACT ON DATASET;")
        .expect("the declaration closes")
        + "AS FACT ON DATASET;".len();
    &kit[start..start + len]
}

async fn cube_session(
    dir: &std::path::Path,
    tables: Vec<(&str, RecordBatch)>,
    glosses: &[&str],
) -> Session {
    cube_session_with(dir, tables, glosses, None).await
}

async fn cube_session_with(
    dir: &std::path::Path,
    tables: Vec<(&str, RecordBatch)>,
    glosses: &[&str],
    cache: Option<CubeCache>,
) -> Session {
    let lake = Lake::open(&dir.join("catalog.db"), &dir.join("warehouse"))
        .await
        .unwrap();
    let store = Store::open(lake).await.unwrap();
    cube_session_on(store, tables, glosses, cache).await
}

/// The agent's session over a store the caller holds — one store per
/// workspace, as the plane holds it: a second handle would carry its
/// own head and never see this one's writes.
async fn cube_session_on(
    store: Store,
    tables: Vec<(&str, RecordBatch)>,
    glosses: &[&str],
    cache: Option<CubeCache>,
) -> Session {
    let mut session = Session::new(
        store,
        Actor {
            kind: ActorKind::Agent,
            id: "t".into(),
        },
    )
    .unwrap();
    if let Some(cache) = cache {
        session = session.with_cube_cache(cache);
    }
    session
        .execute("DECLARE DATASET fin SET (purpose: 'the cube'); USE fin;")
        .await
        .unwrap();
    for (name, batch) in tables {
        let schema = batch.schema();
        session
            .register_table(
                name,
                Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
            )
            .await
            .unwrap();
    }
    // A measurement is never hand-glossed (SPEC.md §5.2) — the judged
    // verdicts the cube reads land through extractions. The judge
    // functions serve fixed verdicts keyed by subject: a named cadence
    // per table, irregular for an attribute date.
    let declarations = format!(
        r#"DECLARE ASPECT temporal_profile WITH $${{
             "type": "object", "required": ["applicable"],
             "properties": {{"applicable": {{"type": "boolean"}}}}}}$$ AS MEASUREMENT ON COLUMN;
           DECLARE ASPECT dimension_relevance WITH $${{
             "type": "object", "required": ["applicable"],
             "properties": {{"applicable": {{"type": "boolean"}}}}}}$$ AS MEASUREMENT ON COLUMN;
           {cube}
           DECLARE FUNCTION judge_time FOR GLOBAL AS
             $$SELECT true AS applicable,
                      CASE WHEN $subject LIKE '%.signed' THEN 'irregular'
                           WHEN $subject LIKE 'ticks.%' THEN 'minute'
                           WHEN $subject LIKE 'daily.%' THEN 'day'
                           WHEN $subject LIKE '%.to_date' THEN 'day'
                           ELSE 'month' END AS granularity,
                      CASE WHEN $subject LIKE '%.signed' THEN NULL
                           WHEN $subject LIKE '%.booked' THEN named_struct('ratio', 0.5)
                           ELSE named_struct('ratio', 1.0) END AS completeness$$
             RETURNS temporal_profile;
           DECLARE ASPECT dimension WITH $${{
             "type": "object", "required": ["value"],
             "properties": {{"value": {{"enum": ["primary", "supporting", "none"]}},
                            "grounds": {{"type": "string"}}}}}}$$ AS FACT ON COLUMN;
           DECLARE FUNCTION judge_axis FOR GLOBAL AS
             $$SELECT CASE WHEN $subject LIKE '%.label' THEN false ELSE true END AS applicable,
                      CASE WHEN $subject LIKE '%.label'
                           THEN 'near-key axis (distinct/filled >= 0.9)' END AS reason,
                      CASE WHEN $subject LIKE '%.note' THEN 0.9
                           ELSE 0.7 END AS relevance$$
             RETURNS dimension_relevance;
           DECLARE ASPECT behavior_evidence WITH $${{
             "type": "object", "required": ["applicable"],
             "properties": {{"applicable": {{"type": "boolean"}}}}}}$$ AS MEASUREMENT ON COLUMN;
           DECLARE FUNCTION judge_behavior FOR GLOBAL AS
             $$SELECT true AS applicable,
                      named_struct('verdict', CASE WHEN $subject LIKE '%.level'
                                                   THEN 'stock' ELSE 'flow' END) AS summary$$
             RETURNS behavior_evidence;"#,
        cube = shipped_cube_declaration()
    );
    session.execute(&declarations).await.unwrap();
    for g in glosses {
        session.execute(g).await.unwrap();
    }
    session
}

/// A human's session over the same store, bound to the dataset.
async fn human_on(store: Store) -> Session {
    let session = Session::new(
        store,
        Actor {
            kind: ActorKind::Human,
            id: "h".into(),
        },
    )
    .unwrap();
    session.execute("USE fin;").await.unwrap();
    session
}

/// Twelve months of results for three constructors: the key column,
/// a venue nobody judged, and the points.
fn results() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("date", DataType::Date32, false),
        Field::new("constructor_id", DataType::Utf8, false),
        Field::new("venue", DataType::Utf8, false),
        Field::new("points", DataType::Float64, false),
    ]));
    let (mut dates, mut cids, mut venues, mut points) =
        (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    for (m, start) in MONTH_STARTS.iter().take(12).enumerate() {
        for (i, cid) in ["c1", "c2", "c3"].iter().enumerate() {
            dates.push(19723 + start);
            cids.push(*cid);
            venues.push(if m % 2 == 0 { "street" } else { "circuit" });
            points.push(10.0 * (i + 1) as f64);
        }
    }
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Date32Array::from(dates)),
            Arc::new(StringArray::from(cids)),
            Arc::new(StringArray::from(venues)),
            Arc::new(Float64Array::from(points)),
        ],
    )
    .unwrap()
}

/// The constructors: one row per key, the label a near-key in its own
/// table by construction.
fn constructors() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("constructor_id", DataType::Utf8, false),
        Field::new("label", DataType::Utf8, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["c1", "c2", "c3"])),
            Arc::new(StringArray::from(vec!["Red", "Blue", "Green"])),
        ],
    )
    .unwrap()
}

const POINTS_BY_TEAM: &[&str] = &[
    r#"DECLARE ASPECT points WITH $${"title": "Points"}$$ AS QUERY ON DATASET;"#,
    r#"GLOSS points ON fin AS $${"sql": "SELECT r.date, c.label AS team, r.constructor_id AS cid, r.venue AS venue, r.points AS value FROM results r JOIN constructors c ON r.constructor_id = c.constructor_id"}$$;"#,
    "SELECT judge_time() FROM results.date;",
    "SELECT judge_axis() FROM constructors.label;",
    "SELECT judge_axis() FROM results.constructor_id;",
];

const AXES: &str = "SELECT dims, basis, admitted_by FROM metric_axes();";

/// The team is a label in its own table — a near-key there, never an
/// axis by its own verdict — and every metric could be sliced by
/// anything but the team. A declared edge reaches it from the results'
/// key column, whose verdict admits it; the fact row names that key as
/// the basis, and the cells slice by the label.
#[tokio::test(flavor = "multi_thread")]
async fn a_label_reached_through_a_declared_edge_is_an_axis_on_the_keys_verdict() {
    let dir = tempfile::tempdir().unwrap();
    let session = cube_session(
        dir.path(),
        vec![("results", results()), ("constructors", constructors())],
        POINTS_BY_TEAM,
    )
    .await;

    // No edge yet: the key admits itself, the label stays out — and
    // the fact row names it, with its abstention and the act that
    // admits it, beside the venue nobody judged.
    let before = grid(&session, AXES).await;
    assert!(
        before.contains("[cid] | [results.constructor_id] | [measurement]"),
        "{before}"
    );
    let gaps = grid(
        &session,
        "SELECT unadmitted, unadmitted_why FROM metric_axes();",
    )
    .await;
    assert!(gaps.contains("[team, venue]"), "{gaps}");
    assert!(
        gaps.contains(
            "dimension_relevance abstained on constructors.label (near-key axis \
             (distinct/filled >= 0.9)), and no declared relationship reaches it"
        ) && gaps.contains("no verdict on results.venue — run dimension_relevance()"),
        "{gaps}"
    );

    session
        .execute("DECLARE RELATIONSHIP results.constructor_id -> constructors.constructor_id;")
        .await
        .unwrap();
    let after = grid(&session, AXES).await;
    assert!(
        after.contains(
            "[cid, team] | [results.constructor_id, results.constructor_id] | [measurement, measurement]"
        ),
        "the label rides the key's verdict, named as its basis: {after}"
    );
    let gaps = grid(&session, "SELECT unadmitted FROM metric_axes();").await;
    assert!(
        gaps.contains("[venue]"),
        "only the unjudged venue stays out: {gaps}"
    );
    let members = grid(
        &session,
        "SELECT DISTINCT member FROM metric_series() WHERE dimension = 'team' ORDER BY member;",
    )
    .await;
    assert!(
        members.contains("Blue") && members.contains("Green") && members.contains("Red"),
        "{members}"
    );
}

/// The collapsed `dimension` gloss is the read policy over what was
/// measured: `primary` puts the axis first and the fact says whose
/// word that was, `none` closes it whatever the verdict, and a column
/// no verdict admits enters on a `primary` alone. The verdicts
/// themselves are untouched — a measurement is never glossed.
#[tokio::test(flavor = "multi_thread")]
async fn a_dimension_gloss_is_the_read_policy_over_the_measured_admission() {
    let dir = tempfile::tempdir().unwrap();
    let lake = Lake::open(
        &dir.path().join("catalog.db"),
        &dir.path().join("warehouse"),
    )
    .await
    .unwrap();
    let store = Store::open(lake).await.unwrap();
    let session = cube_session_on(
        store.clone(),
        vec![("results", results()), ("constructors", constructors())],
        POINTS_BY_TEAM,
        None,
    )
    .await;
    session
        .execute("DECLARE RELATIONSHIP results.constructor_id -> constructors.constructor_id;")
        .await
        .unwrap();
    let human = human_on(store).await;

    human
        .execute(r#"GLOSS dimension ON constructors.label AS $${"value": "primary", "grounds": "the team is what a reader slices by"}$$;"#)
        .await
        .unwrap();
    let axes = grid(&session, AXES).await;
    assert!(
        axes.contains(
            "[team, cid] | [results.constructor_id, results.constructor_id] | [human, measurement]"
        ),
        "primary leads, and the fact says a human put it there: {axes}"
    );

    human
        .execute(r#"GLOSS dimension ON constructors.label AS $${"value": "none"}$$;"#)
        .await
        .unwrap();
    let axes = grid(&session, AXES).await;
    assert!(
        axes.contains("[cid] | [results.constructor_id] | [measurement]"),
        "none closes the axis whatever was measured: {axes}"
    );
    let gaps = grid(&session, "SELECT unadmitted_why FROM metric_axes();").await;
    assert!(
        gaps.contains("closed by a dimension gloss on constructors.label (human: none)"),
        "{gaps}"
    );

    human
        .execute(r#"GLOSS dimension ON results.venue AS $${"value": "primary"}$$;"#)
        .await
        .unwrap();
    let axes = grid(&session, AXES).await;
    assert!(
        axes.contains(
            "[venue, cid] | [results.venue, results.constructor_id] | [human, measurement]"
        ),
        "a column no verdict admits enters on the gloss alone, first: {axes}"
    );
}

/// The write is where a grounding's shape is decided, so the write
/// answers with the fact row the cube will stand on — at the pin it
/// moved to, before any read. Nothing is refused: a grounding whose
/// SQL does not plan lands and abstains in its row with the engine's
/// refusal; one whose date column nobody judged names the profiler;
/// one whose slice columns nobody judged lists each with the act that
/// admits it, and the verb it will be summed by says it was a
/// default. The row is the plan stage — the member floor and the
/// bucketing are the scan's — and `metric_axes()` says the same.
#[tokio::test(flavor = "multi_thread")]
async fn a_grounding_write_answers_with_the_metrics_fact() {
    let dir = tempfile::tempdir().unwrap();
    let session = cube_session(
        dir.path(),
        vec![("results", results()), ("constructors", constructors())],
        &[r#"DECLARE ASPECT points WITH $${"title": "Points"}$$ AS QUERY ON DATASET;"#],
    )
    .await;
    let ground = |sql: &str| format!(r#"GLOSS points ON fin AS $${{"sql": "{sql}"}}$$;"#);
    const FRAME: &str = "SELECT r.date, r.constructor_id AS cid, r.venue AS venue, \
                         r.points AS value FROM results r";

    // Nobody judged the date column: the row abstains and names the
    // profiler — and the gloss has landed.
    let row = grid(&session, &ground(FRAME)).await;
    assert!(row.contains("| points "), "{row}");
    assert!(
        row.contains("| false ") && row.contains("no judged time column"),
        "{row}"
    );
    assert_eq!(
        cell(
            &session,
            "SELECT count(*) FROM glossary WHERE aspect = 'points';"
        )
        .await,
        "1"
    );

    // The date judged, the slice columns not: the row admits nothing
    // and names each column with the act that admits it; the verb is
    // a flow because nothing said otherwise, and the row says so.
    session
        .execute("SELECT judge_time() FROM results.date;")
        .await
        .unwrap();
    let row = grid(&session, &ground(FRAME)).await;
    assert!(row.contains("| true "), "{row}");
    assert!(
        row.contains("| flow ") && row.contains("| default "),
        "{row}"
    );
    assert!(row.contains("| [] "), "no axis admitted: {row}");
    assert!(row.contains("[cid, venue]"), "{row}");
    assert!(
        row.contains("no verdict on results.constructor_id — run dimension_relevance() over it")
            && row.contains("no verdict on results.venue — run dimension_relevance()"),
        "{row}"
    );

    // One column judged: it is an axis in the write's row and in the
    // read's, and the other is still named.
    session
        .execute("SELECT judge_axis() FROM results.constructor_id;")
        .await
        .unwrap();
    let row = grid(&session, &ground(FRAME)).await;
    assert!(
        row.contains("[cid]")
            && row.contains("[results.constructor_id]")
            && row.contains("[measurement]")
            && row.contains("[venue]"),
        "{row}"
    );
    let read = grid(
        &session,
        "SELECT dims, basis, admitted_by, unadmitted FROM metric_axes();",
    )
    .await;
    assert!(
        read.contains("[cid]")
            && read.contains("[results.constructor_id]")
            && read.contains("[measurement]")
            && read.contains("[venue]"),
        "the read says the same: {read}"
    );

    // A grounding the engine refuses lands too, and its row carries
    // the refusal rather than a read discovering it later.
    let row = grid(&session, &ground("SELECT nope AS value FROM results")).await;
    assert!(
        row.contains("| false ") && row.contains("not served") && row.contains("nope"),
        "{row}"
    );
    assert_eq!(
        cell(
            &session,
            "SELECT count(*) FROM glossary WHERE aspect = 'points';"
        )
        .await,
        "4"
    );
}

/// One read, rendered as text — for asserts over served relations.
async fn grid(session: &Session, sql: &str) -> String {
    let outcomes = session.execute(sql).await.unwrap();
    let Some(Outcome::Rows(batches)) = outcomes.last() else {
        panic!("rows")
    };
    datafusion::arrow::util::pretty::pretty_format_batches(batches)
        .unwrap()
        .to_string()
}

/// The first cell of a one-value read, as text.
async fn cell(session: &Session, sql: &str) -> String {
    let outcomes = session
        .execute(sql)
        .await
        .unwrap_or_else(|e| panic!("`{sql}`: {e}"));
    let Some(Outcome::Rows(batches)) = outcomes.last() else {
        panic!("rows")
    };
    let batch = batches
        .iter()
        .find(|b| b.num_rows() > 0)
        .unwrap_or_else(|| panic!("`{sql}` served no row"));
    datafusion::arrow::util::display::array_value_to_string(batch.column(0), 0).unwrap()
}

async fn number(session: &Session, sql: &str) -> f64 {
    let text = cell(session, sql).await;
    text.parse()
        .unwrap_or_else(|_| panic!("`{sql}` served `{text}`, not a number"))
}

fn near(got: f64, want: f64, what: &str) {
    assert!((got - want).abs() < 1e-9, "{what}: got {got}, want {want}");
}

fn dated(
    fields: Vec<Field>,
    dates: Vec<i32>,
    columns: Vec<Arc<dyn datafusion::arrow::array::Array>>,
) -> RecordBatch {
    let mut all: Vec<Field> = vec![Field::new("date", DataType::Date32, false)];
    all.extend(fields);
    let mut cols: Vec<Arc<dyn datafusion::arrow::array::Array>> =
        vec![Arc::new(Date32Array::from(dates))];
    cols.extend(columns);
    RecordBatch::try_new(Arc::new(Schema::new(all)), cols).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn the_cube_slices_windows_and_carries_the_rival() {
    let dir = tempfile::tempdir().unwrap();
    // 30 months of a flow: 15 rows per month (3 regions × 5 channels),
    // with a 40-distinct note column that exceeds the named-member cap
    // and must come back bucketed — top members plus 'other'.
    let (mut dates, mut values, mut regions, mut channels, mut notes) =
        (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new());
    for (i, start) in MONTH_STARTS.iter().enumerate() {
        let mut k = 0;
        for r in ["r1", "r2", "r3"] {
            for c in ["c1", "c2", "c3", "c4", "c5"] {
                dates.push(19723 + start);
                values.push(1.0);
                regions.push(r);
                channels.push(c);
                notes.push(format!("n{}", (i * 15 + k) % 40));
                k += 1;
            }
        }
    }
    let lines = dated(
        vec![
            Field::new("value", DataType::Float64, false),
            Field::new("region", DataType::Utf8, false),
            Field::new("channel", DataType::Utf8, false),
            Field::new("note", DataType::Utf8, false),
        ],
        dates,
        vec![
            Arc::new(Float64Array::from(values)),
            Arc::new(StringArray::from(regions)),
            Arc::new(StringArray::from(channels)),
            Arc::new(StringArray::from(notes)),
        ],
    );
    let alt = dated(
        vec![Field::new("value", DataType::Float64, false)],
        MONTH_STARTS.iter().map(|s| 19723 + s).collect(),
        vec![Arc::new(Float64Array::from(vec![90.0; 30]))],
    );
    let levels = dated(
        vec![Field::new("value", DataType::Float64, false)],
        MONTH_STARTS[..18].iter().map(|s| 19723 + s).collect(),
        vec![Arc::new(Float64Array::from(
            (0..18).map(|i| 1000.0 + 5.0 * i as f64).collect::<Vec<_>>(),
        ))],
    );

    let session = cube_session(
        dir.path(),
        vec![("lines", lines), ("alt", alt), ("levels", levels)],
        &[
            r#"DECLARE ASPECT revenue WITH $${"title": "Revenue"}$$ AS QUERY ON DATASET;"#,
            r#"DECLARE ASPECT inventory WITH $${"title": "Inventory"}$$ AS QUERY ON DATASET;"#,
            r#"GLOSS revenue ON fin AS $${
                 "sql": "SELECT date, value, region, channel, note FROM lines",
                 "assumptions": [
                     {"dimension": "definition",
                      "assumption": "revenue = invoiced less credit notes",
                      "alternative": "all invoiced",
                      "alternative_sql": "SELECT date, value FROM alt",
                      "confidence": 0.7}
                 ]}$$;"#,
            r#"GLOSS inventory ON fin AS $${
                 "sql": "SELECT date, value FROM levels", "behavior": "stock",
                 "assumptions": [
                     {"dimension": "definition",
                      "assumption": "book levels are the record",
                      "alternative": "the same levels",
                      "alternative_sql": "SELECT date, value FROM levels",
                      "tolerance": 0.01, "confidence": 0.8}
                 ]}$$;"#,
            // The judged axes land LAST: a measurement is keyed at the
            // statement pin, and every declaration moves it — a judge
            // run before the metric glosses would read as drift. The
            // rival's `alt` table deliberately carries no verdict — a
            // rival is authored, never admission-validated, and still
            // serves. The wide note column takes the higher relevance
            // so ordering is by judgment, never by member count.
            "SELECT judge_time() FROM lines.date;",
            "SELECT judge_time() FROM levels.date;",
            "SELECT judge_axis() FROM lines.note;",
            "SELECT judge_axis() FROM lines.region;",
            "SELECT judge_axis() FROM lines.channel;",
        ],
    )
    .await;

    // The fact row: admission by judged verdict — the 40-member note
    // column leads on relevance (and enters bucketed), the 0.7 tie
    // breaks by fewest members. A month cadence under the day floor is
    // month, and the month rung of the ladder is its window.
    let fact = |col: &'static str| {
        let session = &session;
        async move {
            cell(
                session,
                &format!("SELECT {col} FROM metric_axes() WHERE metric = 'revenue';"),
            )
            .await
        }
    };
    assert_eq!(fact("applicable").await, "true");
    assert_eq!(fact("judged_current").await, "true");
    assert_eq!(fact("behavior").await, "flow");
    // Where the verb came from. `revenue` carries no `behavior` key, so
    // it is a flow because nothing said otherwise — the common case,
    // and usually right; the fact says so rather than leaving it a
    // silent assumption. `inventory` is marked and the ratio metrics
    // never consult the marker at all.
    assert_eq!(fact("behavior_basis").await, "default");
    assert_eq!(fact("resolution").await, "month");
    assert_eq!(fact("window").await, "48 months");
    assert_eq!(
        fact("array_to_string(dims, ',')").await,
        "note,region,channel"
    );
    assert_eq!(fact("array_to_string(bucketed, ',')").await, "note");
    assert_eq!(fact("alternative").await, "all invoiced");
    // The measured disagreement: no authored tolerance, so the maximum
    // relative gap and its period — the coordinates, not two lines to
    // eyeball. Every month is 15.0 against the rival's 90.0.
    assert_eq!(
        fact("alternative_divergence").await,
        "max relative gap 0.8333 at 2024-01-01 over 30 shared periods"
    );

    // The cells: all 30 generated months fit under the 48-month rung;
    // the period is a typed timestamp, the bucket's start.
    let count = |filter: &'static str| {
        let session = &session;
        async move {
            number(
                session,
                &format!(
                    "SELECT count(*) FROM metric_series() WHERE metric = 'revenue' AND {filter};"
                ),
            )
            .await
        }
    };
    near(count("dimension = ''").await, 30.0, "total periods");
    assert_eq!(
        cell(
            &session,
            "SELECT min(period) FROM metric_series() WHERE metric = 'revenue' AND dimension = '';",
        )
        .await,
        "2024-01-01T00:00:00"
    );
    near(count("dimension = 'region'").await, 90.0, "region cells");
    near(count("dimension = 'channel'").await, 150.0, "channel cells");
    near(
        count("dimension = 'alternative'").await,
        30.0,
        "rival cells",
    );
    assert_eq!(
        cell(
            &session,
            "SELECT DISTINCT member FROM metric_series() \
             WHERE metric = 'revenue' AND dimension = 'alternative';",
        )
        .await,
        "all invoiced"
    );

    // The bucketed dimension: at most 24 members counting 'other', and
    // bucketing loses nothing — each month's note slices still sum to
    // the month's 15 rows.
    let members = number(
        &session,
        "SELECT count(DISTINCT member) FROM metric_series() \
         WHERE metric = 'revenue' AND dimension = 'note';",
    )
    .await;
    assert!(members <= 24.0, "{members} members");
    assert!(
        count("dimension = 'note' AND member = 'other'").await > 0.0,
        "the fold-in member exists"
    );
    assert_eq!(
        cell(
            &session,
            "SELECT bool_and(s = 15.0) FROM (SELECT period, sum(value) AS s FROM metric_series() \
             WHERE metric = 'revenue' AND dimension = 'note' GROUP BY period);",
        )
        .await,
        "true"
    );

    // The stock: no served dimensions, its own 18 months.
    assert_eq!(
        cell(
            &session,
            "SELECT behavior FROM metric_axes() WHERE metric = 'inventory';"
        )
        .await,
        "stock"
    );
    assert_eq!(
        cell(
            &session,
            "SELECT behavior_basis FROM metric_axes() WHERE metric = 'inventory';"
        )
        .await,
        "marked"
    );
    assert_eq!(
        cell(
            &session,
            "SELECT array_to_string(dims, ',') FROM metric_axes() WHERE metric = 'inventory';",
        )
        .await,
        ""
    );
    near(
        number(
            &session,
            "SELECT count(*) FROM metric_series() WHERE metric = 'inventory' AND dimension = '';",
        )
        .await,
        18.0,
        "stock periods",
    );
    // An agreeing rival with an authored tolerance: zero breaches is a
    // served answer, never silence.
    assert_eq!(
        cell(
            &session,
            "SELECT alternative_divergence FROM metric_axes() WHERE metric = 'inventory';",
        )
        .await,
        "0 of 18 shared periods differ beyond 0.01; max relative gap 0.0000 at 2024-01-01"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_stock_total_sums_the_months_latest_snapshot() {
    // Two products, snapshotted mid-month and again at month end:
    // the mid-month rows are superseded observations, the month-end
    // rows are the standing stock. January also proves the old
    // defect: one arbitrary month-end row (100 or 200) is wrong,
    // and summing everything (630) is wrong — the total is 300.
    let days = [
        ("2024-01-15", 10.0, "p1"),
        ("2024-01-15", 20.0, "p2"),
        ("2024-01-31", 100.0, "p1"),
        ("2024-01-31", 200.0, "p2"),
        ("2024-02-28", 110.0, "p1"),
        ("2024-02-28", 210.0, "p2"),
    ];
    let epoch = |d: &str| {
        let parts: Vec<i32> = d.split('-').map(|p| p.parse().unwrap()).collect();
        19723 + [0, 31][(parts[1] - 1) as usize] + parts[2] - 1
    };
    let levels = dated(
        vec![
            Field::new("value", DataType::Float64, false),
            Field::new("product", DataType::Utf8, false),
        ],
        days.iter().map(|(d, _, _)| epoch(d)).collect(),
        vec![
            Arc::new(Float64Array::from(
                days.iter().map(|(_, v, _)| *v).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                days.iter().map(|(_, _, p)| *p).collect::<Vec<_>>(),
            )),
        ],
    );

    let dir = tempfile::tempdir().unwrap();
    let session = cube_session(
        dir.path(),
        vec![("levels", levels)],
        &[
            r#"DECLARE ASPECT inventory WITH $${"title": "Inventory"}$$ AS QUERY ON DATASET;"#,
            r#"GLOSS inventory ON fin AS $${"sql": "SELECT date, value, product FROM levels", "behavior": "stock"}$$;"#,
            "SELECT judge_time() FROM levels.date;",
            "SELECT judge_axis() FROM levels.product;",
        ],
    )
    .await;

    assert_eq!(
        cell(
            &session,
            "SELECT behavior FROM metric_axes() WHERE metric = 'inventory';"
        )
        .await,
        "stock"
    );
    // product (2 members) is a served dimension on the real context
    assert_eq!(
        cell(
            &session,
            "SELECT array_to_string(dims, ',') FROM metric_axes() WHERE metric = 'inventory';",
        )
        .await,
        "product"
    );

    // the total: the month-end snapshot summed across products
    let total = grid(
        &session,
        "SELECT period, value FROM metric_series() \
         WHERE metric = 'inventory' AND dimension = '' ORDER BY period;",
    )
    .await;
    assert!(total.contains("2024-01-01T00:00:00 | 300.0"), "{total}");
    assert!(total.contains("2024-02-01T00:00:00 | 320.0"), "{total}");

    // the member series: each product's own latest observation
    let members = grid(
        &session,
        "SELECT member, period, value FROM metric_series() \
         WHERE metric = 'inventory' AND dimension = 'product' ORDER BY period, member;",
    )
    .await;
    for want in [
        "p1     | 2024-01-01T00:00:00 | 100.0",
        "p2     | 2024-01-01T00:00:00 | 200.0",
        "p1     | 2024-02-01T00:00:00 | 110.0",
        "p2     | 2024-02-01T00:00:00 | 210.0",
    ] {
        assert!(members.contains(want), "{want} in\n{members}");
    }

    // A coarser grain takes the bucket's LAST period for a stock: the
    // quarter stands at February's level, never the sum of the months.
    let quarter = grid(
        &session,
        "SELECT period, value, behavior FROM metric_series(grain => 'quarter') \
         WHERE metric = 'inventory' AND dimension = '';",
    )
    .await;
    assert!(
        quarter.contains("2024-01-01T00:00:00 | 320.0 | stock"),
        "{quarter}"
    );
}

/// The declared grain (SPEC §5.2, corpus fixture 21) is validated
/// where the frame is built: one row per key or the metric abstains,
/// because a frame that breaks its declared identity multiplies every
/// aggregating reader and nothing downstream can tell duplication from
/// multi-entity. A grain column the frame does not serve is caught at
/// the plan stage — the write's answer — and an undeclared grain is
/// served as the empty list, never refused.
#[tokio::test(flavor = "multi_thread")]
async fn the_declared_grain_gates_the_frame_one_row_per_key() {
    // Two accounts, two dates: distinct per (date, account), duplicated
    // per (date) — the shape that separates the declarations.
    let events = dated(
        vec![
            Field::new("value", DataType::Float64, false),
            Field::new("account", DataType::Utf8, false),
        ],
        vec![19723 + 14, 19723 + 14, 19723 + 30, 19723 + 30],
        vec![
            Arc::new(Float64Array::from(vec![10.0, 20.0, 100.0, 200.0])),
            Arc::new(StringArray::from(vec!["a1", "a2", "a1", "a2"])),
        ],
    );

    let dir = tempfile::tempdir().unwrap();
    let session = cube_session(
        dir.path(),
        vec![("events", events)],
        &[
            r#"DECLARE ASPECT held WITH $${"title": "Held"}$$ AS QUERY ON DATASET;"#,
            r#"GLOSS held ON fin AS $${"sql": "SELECT date, value, account FROM events", "behavior": "stock", "grain": ["date", "account"]}$$;"#,
            r#"DECLARE ASPECT broken WITH $${"title": "Broken"}$$ AS QUERY ON DATASET;"#,
            r#"GLOSS broken ON fin AS $${"sql": "SELECT date, value FROM events", "behavior": "stock", "grain": ["date"]}$$;"#,
            r#"DECLARE ASPECT unserved WITH $${"title": "Unserved"}$$ AS QUERY ON DATASET;"#,
            r#"GLOSS unserved ON fin AS $${"sql": "SELECT date, value FROM events", "grain": ["account"]}$$;"#,
            r#"DECLARE ASPECT undeclared WITH $${"title": "Undeclared"}$$ AS QUERY ON DATASET;"#,
            r#"GLOSS undeclared ON fin AS $${"sql": "SELECT date, value FROM events"}$$;"#,
            "SELECT judge_time() FROM events.date;",
        ],
    )
    .await;

    // Declared and held: the metric serves, the declaration rides the
    // fact row, and the stock total stands at the latest date's rows.
    assert_eq!(
        cell(
            &session,
            "SELECT array_to_string(grain, ',') FROM metric_axes() WHERE metric = 'held';"
        )
        .await,
        "date,account"
    );
    let total = grid(
        &session,
        "SELECT period, value FROM metric_series() \
         WHERE metric = 'held' AND dimension = '' ORDER BY period;",
    )
    .await;
    assert!(total.contains("2024-01-01T00:00:00 | 300.0"), "{total}");

    // Declared and broken: the metric abstains, the reason naming the
    // columns and the counts.
    assert_eq!(
        cell(
            &session,
            "SELECT applicable FROM metric_axes() WHERE metric = 'broken';"
        )
        .await,
        "false"
    );
    let reason = cell(
        &session,
        "SELECT reason FROM metric_axes() WHERE metric = 'broken';",
    )
    .await;
    assert!(
        reason.contains("breaks its declared grain (date): 4 rows over 2 distinct keys"),
        "{reason}"
    );

    // A grain column the frame does not serve: caught at the plan
    // stage, so the write's answer carries it too.
    let reason = cell(
        &session,
        "SELECT reason FROM metric_axes() WHERE metric = 'unserved';",
    )
    .await;
    assert!(
        reason.contains("the declared grain names `account`, which the frame does not serve"),
        "{reason}"
    );

    // Undeclared: served, and the fact row says so with the empty list.
    assert_eq!(
        cell(
            &session,
            "SELECT applicable FROM metric_axes() WHERE metric = 'undeclared';"
        )
        .await,
        "true"
    );
    assert_eq!(
        cell(
            &session,
            "SELECT array_to_string(grain, ',') FROM metric_axes() WHERE metric = 'undeclared';"
        )
        .await,
        ""
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_ratio_totals_by_dividing_the_summed_halves_never_by_adding_ratios() {
    // The defect this exists for: a ratio is neither stock nor flow,
    // so it took the flow path and every sliced ratio reported the SUM
    // of its members. DSO for one month came back 928.3 days against a
    // true 75.6 — the grounding served segment x region, so twelve
    // member ratios were added together.
    //
    // Four cells a month, chosen so the right answer and the old wrong
    // one cannot be confused: summing the per-row ratios gives 0.65,
    // dividing the summed halves gives 1000/6000.
    let cells = [
        ("2024-01-31", 100.0, 1000.0, "A", "EMEA"),
        ("2024-01-31", 200.0, 1000.0, "A", "APAC"),
        ("2024-01-31", 300.0, 2000.0, "B", "EMEA"),
        ("2024-01-31", 400.0, 2000.0, "B", "APAC"),
        ("2024-02-29", 110.0, 1100.0, "A", "EMEA"),
        ("2024-02-29", 210.0, 1100.0, "A", "APAC"),
        ("2024-02-29", 310.0, 2100.0, "B", "EMEA"),
        ("2024-02-29", 410.0, 2100.0, "B", "APAC"),
    ];
    let epoch = |d: &str| {
        let parts: Vec<i32> = d.split('-').map(|p| p.parse().unwrap()).collect();
        19723 + [0, 31][(parts[1] - 1) as usize] + parts[2] - 1
    };
    let batch = dated(
        vec![
            Field::new("value", DataType::Float64, false),
            Field::new("num", DataType::Float64, false),
            Field::new("den", DataType::Float64, false),
            Field::new("segment", DataType::Utf8, false),
            Field::new("region", DataType::Utf8, false),
        ],
        cells.iter().map(|c| epoch(c.0)).collect(),
        vec![
            // `value` is each row's own ratio — what the old code summed.
            Arc::new(Float64Array::from(
                cells.iter().map(|c| c.1 / c.2).collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                cells.iter().map(|c| c.1).collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                cells.iter().map(|c| c.2).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                cells.iter().map(|c| c.3).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                cells.iter().map(|c| c.4).collect::<Vec<_>>(),
            )),
        ],
    );

    let dir = tempfile::tempdir().unwrap();
    let session = cube_session(
        dir.path(),
        vec![("cells", batch)],
        &[
            r#"DECLARE ASPECT dso WITH $${"title": "DSO"}$$ AS QUERY ON DATASET;"#,
            // The rival is a ratio too: it serves num and den, at twice
            // the numerator — so at every grain it reads as exactly
            // twice the chosen reading, which only holds if the rival's
            // cells carry their halves and re-derive the division.
            r#"GLOSS dso ON fin AS $${"sql": "SELECT date, value, num, den, segment, region FROM cells",
                 "assumptions": [
                     {"dimension": "definition", "assumption": "net of credit notes",
                      "alternative": "gross", "confidence": 0.6,
                      "alternative_sql": "SELECT date, value * 2 AS value, num * 2 AS num, den FROM cells"}
                 ]}$$;"#,
            "SELECT judge_time() FROM cells.date;",
            "SELECT judge_axis() FROM cells.segment;",
            "SELECT judge_axis() FROM cells.region;",
        ],
    )
    .await;

    assert_eq!(
        cell(
            &session,
            "SELECT behavior FROM metric_axes() WHERE metric = 'dso';"
        )
        .await,
        "ratio"
    );
    // A ratio never consults the marker: both halves served, the
    // division is the reading whatever `behavior` says.
    assert_eq!(
        cell(
            &session,
            "SELECT behavior_basis FROM metric_axes() WHERE metric = 'dso';"
        )
        .await,
        "ratio"
    );

    // The halves are measures, not axes: num/den must never be offered
    // as dimensions to slice along.
    let dims = cell(
        &session,
        "SELECT array_to_string(dims, ',') FROM metric_axes() WHERE metric = 'dso';",
    )
    .await;
    assert!(
        dims.contains("segment") && dims.contains("region"),
        "{dims}"
    );
    assert!(!dims.contains("num") && !dims.contains("den"), "{dims}");

    let at =
        |dimension: &'static str, member: &'static str, period: &'static str, col: &'static str| {
            let session = &session;
            async move {
                number(
                    session,
                    &format!(
                        "SELECT {col} FROM metric_series() WHERE metric = 'dso' \
                     AND dimension = '{dimension}' AND member = '{member}' \
                     AND period = TIMESTAMP '{period}';"
                    ),
                )
                .await
            }
        };

    // The total: sum(num)/sum(den), never the 0.65 that adding the four
    // per-row ratios would give.
    near(
        at("", "", "2024-01-01", "value").await,
        1000.0 / 6000.0,
        "january total",
    );
    near(
        at("", "", "2024-02-01", "value").await,
        1040.0 / 6400.0,
        "february total",
    );

    // Each member likewise divides its own summed halves — segment A is
    // 300/2000, not the 0.3 its two region rows add up to.
    near(
        at("segment", "A", "2024-01-01", "value").await,
        300.0 / 2000.0,
        "segment A",
    );
    near(
        at("segment", "B", "2024-01-01", "value").await,
        700.0 / 4000.0,
        "segment B",
    );
    near(
        at("region", "EMEA", "2024-01-01", "value").await,
        400.0 / 3000.0,
        "region EMEA",
    );
    near(
        at("region", "APAC", "2024-01-01", "value").await,
        600.0 / 3000.0,
        "region APAC",
    );

    // Every ratio cell carries its summed halves — a coarser grain
    // re-derives the division from them, never from the ratio values.
    near(at("", "", "2024-01-01", "num").await, 1000.0, "january num");
    near(at("", "", "2024-01-01", "den").await, 6000.0, "january den");
    near(
        at("segment", "A", "2024-01-01", "num").await,
        300.0,
        "segment A num",
    );
    near(
        at("segment", "A", "2024-01-01", "den").await,
        2000.0,
        "segment A den",
    );
    // The rival too, at its own verb.
    near(
        at("alternative", "gross", "2024-01-01", "num").await,
        2000.0,
        "rival num",
    );
    assert_eq!(
        cell(
            &session,
            "SELECT DISTINCT behavior FROM metric_series() WHERE metric = 'dso' AND dimension = 'alternative';",
        )
        .await,
        "ratio"
    );

    // The quarter re-derives by the verb: the two months' halves summed
    // then divided — for the total, for a member, and for the rival,
    // which reads as exactly twice the chosen reading.
    let quarter = |dimension: &'static str, member: &'static str, col: &'static str| {
        let session = &session;
        async move {
            number(
                session,
                &format!(
                    "SELECT {col} FROM metric_series(grain => 'quarter') WHERE metric = 'dso' \
                     AND dimension = '{dimension}' AND member = '{member}';"
                ),
            )
            .await
        }
    };
    near(quarter("", "", "value").await, 2040.0 / 12400.0, "Q1 total");
    near(quarter("", "", "num").await, 2040.0, "Q1 num");
    near(
        quarter("segment", "A", "value").await,
        620.0 / 4200.0,
        "Q1 segment A",
    );
    near(
        quarter("alternative", "gross", "value").await,
        4080.0 / 12400.0,
        "Q1 rival",
    );
    near(
        number(
            &session,
            "SELECT count(*) FROM metric_series(grain => 'quarter') WHERE metric = 'dso' AND dimension = '';",
        )
        .await,
        1.0,
        "one quarter",
    );

    // A frame's named param binds into the door argument before the
    // pre-pass — the app's frames ride `metric_series(grain => $grain)`.
    let mut values: std::collections::HashMap<String, datafusion::common::ScalarValue> =
        Default::default();
    values.insert(
        "grain".into(),
        datafusion::common::ScalarValue::Utf8(Some("quarter".into())),
    );
    let query = session
        .query_stream_with_params(
            "SELECT count(*) AS n FROM metric_series(grain => $grain) WHERE dimension = ''",
            Some(datafusion::common::ParamValues::from(values)),
        )
        .await
        .unwrap();
    let batches: Vec<_> = futures::StreamExt::collect::<Vec<_>>(query.stream)
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let n = datafusion::arrow::util::pretty::pretty_format_batches(&batches)
        .unwrap()
        .to_string();
    assert!(n.contains("| 1 "), "{n}");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_axes_come_from_judged_verdicts_never_from_the_datas_shape() {
    // The onboarding shape: the served frame leads with an attribute
    // date (all contracts signed in one month), a sparser date follows,
    // the event date comes last among the three; a low-cardinality
    // column rides along with no verdict landed. Schema order would
    // anchor the series on `signed` (one period instead of thirty) and
    // cardinality would admit `channel`; the judged verdicts pick
    // `date` — a named cadence ranks before an irregular verdict, and
    // among named cadences the higher completeness wins — and
    // `region` alone. Served alone, the irregular column anchors at
    // the floor.
    let (mut signed, mut booked, mut dates, mut values, mut regions, mut channels) = (
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    );
    for (i, start) in MONTH_STARTS.iter().enumerate() {
        for r in ["r1", "r2", "r3"] {
            signed.push(19723 + (i as i32 % 28));
            booked.push(19723 + (i as i32 % 28));
            dates.push(19723 + start);
            values.push(1.0);
            regions.push(r);
            channels.push(["c1", "c2", "c3", "c4", "c5"][i % 5]);
        }
    }
    let lines = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("signed", DataType::Date32, false),
            Field::new("booked", DataType::Date32, false),
            Field::new("date", DataType::Date32, false),
            Field::new("value", DataType::Float64, false),
            Field::new("region", DataType::Utf8, false),
            Field::new("channel", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(Date32Array::from(signed)),
            Arc::new(Date32Array::from(booked)),
            Arc::new(Date32Array::from(dates)),
            Arc::new(Float64Array::from(values)),
            Arc::new(StringArray::from(regions)),
            Arc::new(StringArray::from(channels)),
        ],
    )
    .unwrap();
    let bare = dated(
        vec![Field::new("value", DataType::Float64, false)],
        vec![19723],
        vec![Arc::new(Float64Array::from(vec![1.0]))],
    );

    let dir = tempfile::tempdir().unwrap();
    let session = cube_session(
        dir.path(),
        vec![("lines", lines), ("bare", bare)],
        &[
            r#"DECLARE ASPECT revenue WITH $${"title": "Revenue"}$$ AS QUERY ON DATASET;"#,
            r#"DECLARE ASPECT raw WITH $${"title": "Raw"}$$ AS QUERY ON DATASET;"#,
            r#"DECLARE ASPECT signings WITH $${"title": "Signings"}$$ AS QUERY ON DATASET;"#,
            r#"GLOSS revenue ON fin AS $${"sql": "SELECT signed, booked, date, value, region, channel FROM lines"}$$;"#,
            r#"GLOSS raw ON fin AS $${"sql": "SELECT date, value FROM bare"}$$;"#,
            r#"GLOSS signings ON fin AS $${"sql": "SELECT signed, value FROM lines"}$$;"#,
            // The attribute date carries a verdict too — irregular, no
            // completeness — proving a landed verdict without a named
            // cadence ranks below one, not merely an absent one.
            "SELECT judge_time() FROM lines.signed;",
            "SELECT judge_time() FROM lines.booked;",
            "SELECT judge_time() FROM lines.date;",
            "SELECT judge_axis() FROM lines.region;",
        ],
    )
    .await;

    assert_eq!(
        cell(
            &session,
            "SELECT applicable FROM metric_axes() WHERE metric = 'revenue';"
        )
        .await,
        "true"
    );
    // the judged axis, not the first temporal column: thirty monthly
    // periods, not one
    let span = grid(
        &session,
        "SELECT count(*) AS n, min(period) AS first, max(period) AS last FROM metric_series() \
         WHERE metric = 'revenue' AND dimension = '';",
    )
    .await;
    assert!(span.contains("| 30 "), "{span}");
    assert!(
        span.contains("2024-01-01T00:00:00") && span.contains("2026-06-01T00:00:00"),
        "{span}"
    );
    // the judged dimension alone — the unjudged column is a gap, not a
    // candidate
    assert_eq!(
        cell(
            &session,
            "SELECT array_to_string(dims, ',') FROM metric_axes() WHERE metric = 'revenue';",
        )
        .await,
        "region"
    );
    // — named as the gap it is, with the act that closes it; the
    // date columns are the time axis's candidates, never listed here
    let gap = grid(
        &session,
        "SELECT unadmitted, unadmitted_why FROM metric_axes() WHERE metric = 'revenue';",
    )
    .await;
    assert!(gap.contains("[channel]"), "{gap}");
    assert!(
        gap.contains("no verdict on lines.channel — run dimension_relevance() over it"),
        "{gap}"
    );

    // the irregular column alone anchors at the floor: day cells over
    // the day rung — twenty-eight signing days, not one month
    let floor = grid(
        &session,
        "SELECT resolution, window, (SELECT count(*) FROM metric_series() \
         WHERE metric = 'signings' AND dimension = '') AS n \
         FROM metric_axes() WHERE metric = 'signings';",
    )
    .await;
    assert!(
        floor.contains("| day ") && floor.contains("| 18 months ") && floor.contains("| 28 "),
        "{floor}"
    );

    // a frame whose date column carries no verdict abstains with the
    // road out — in the fact row, and with no cells
    assert_eq!(
        cell(
            &session,
            "SELECT applicable FROM metric_axes() WHERE metric = 'raw';"
        )
        .await,
        "false"
    );
    let reason = cell(
        &session,
        "SELECT reason FROM metric_axes() WHERE metric = 'raw';",
    )
    .await;
    assert!(reason.contains("no judged time column"), "{reason}");
    near(
        number(
            &session,
            "SELECT count(*) FROM metric_series() WHERE metric = 'raw';",
        )
        .await,
        0.0,
        "an abstaining metric serves no cells",
    );

    // a flow cell carries no halves — the keys exist only where a
    // division has to be re-derivable
    near(
        number(
            &session,
            "SELECT count(num) + count(den) FROM metric_series() WHERE metric = 'revenue';",
        )
        .await,
        0.0,
        "flow halves",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_resolution_is_the_coarser_of_cadence_and_floor_and_the_window_its_rung() {
    // Three cadences under the shipped ladder (floor day): a day table
    // of 730 days, a minute table of three days, a month table of 60
    // months. The day metric serves day cells over the day rung (18
    // months); the minute metric is held at the floor; the month metric
    // serves months over the month rung (48). A gloss then lowers the
    // floor to the hour and shortens the month rung — each resolution
    // follows its own rung, the others stand.
    let daily = dated(
        vec![Field::new("value", DataType::Float64, false)],
        (0..730).map(|i| 19723 + i).collect(),
        vec![Arc::new(Float64Array::from(vec![1.0; 730]))],
    );
    // 2026-03-01T00:00 .. 2026-03-03T23:59, one row per minute.
    let start_us: i64 = (19723 + 790) as i64 * 86_400 * 1_000_000;
    let ticks = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            Field::new("value", DataType::Float64, false),
        ])),
        vec![
            Arc::new(TimestampMicrosecondArray::from(
                (0..3 * 1440)
                    .map(|i| start_us + i * 60 * 1_000_000)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(vec![1.0; 3 * 1440])),
        ],
    )
    .unwrap();
    // 60 month starts from 2021-01-01 (18628) to 2025-12-01.
    let month_starts: Vec<i32> = {
        let mut out = Vec::new();
        let days_in = |y: i32, m: i32| -> i32 {
            match m {
                1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
                4 | 6 | 9 | 11 => 30,
                _ => {
                    if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 {
                        29
                    } else {
                        28
                    }
                }
            }
        };
        let mut d = 18628;
        for y in 2021..=2025 {
            for m in 1..=12 {
                out.push(d);
                d += days_in(y, m);
            }
        }
        out
    };
    let months = dated(
        vec![Field::new("value", DataType::Float64, false)],
        month_starts,
        vec![Arc::new(Float64Array::from(vec![1.0; 60]))],
    );

    let dir = tempfile::tempdir().unwrap();
    let session = cube_session(
        dir.path(),
        vec![("daily", daily), ("ticks", ticks), ("months", months)],
        &[
            r#"DECLARE ASPECT by_day WITH $${"title": "By day"}$$ AS QUERY ON DATASET;"#,
            r#"DECLARE ASPECT by_minute WITH $${"title": "By minute"}$$ AS QUERY ON DATASET;"#,
            r#"DECLARE ASPECT by_month WITH $${"title": "By month"}$$ AS QUERY ON DATASET;"#,
            r#"GLOSS by_day ON fin AS $${"sql": "SELECT date, value FROM daily"}$$;"#,
            r#"GLOSS by_minute ON fin AS $${"sql": "SELECT ts, value FROM ticks"}$$;"#,
            r#"GLOSS by_month ON fin AS $${"sql": "SELECT date, value FROM months"}$$;"#,
            "SELECT judge_time() FROM daily.date;",
            "SELECT judge_time() FROM ticks.ts;",
            "SELECT judge_time() FROM months.date;",
        ],
    )
    .await;

    let axes = grid(
        &session,
        "SELECT metric, resolution, window FROM metric_axes() ORDER BY metric;",
    )
    .await;
    assert!(
        axes.contains("by_day    | day        | 18 months"),
        "{axes}"
    );
    assert!(
        axes.contains("by_minute | day        | 18 months"),
        "{axes}"
    );
    assert!(
        axes.contains("by_month  | month      | 48 months"),
        "{axes}"
    );

    let cells = |metric: &'static str, grain: &'static str| {
        let session = &session;
        async move {
            let from = if grain.is_empty() {
                "metric_series()".to_string()
            } else {
                format!("metric_series(grain => '{grain}')")
            };
            number(
                session,
                &format!(
                    "SELECT count(*) FROM {from} WHERE metric = '{metric}' AND dimension = '';"
                ),
            )
            .await
        }
    };
    // The day rung from the data's edge: 2025-12-30 less 18 months is
    // 2024-06-30, and the buckets after it number 548.
    near(
        cells("by_day", "").await,
        548.0,
        "day cells under the day rung",
    );
    // Minutes held at the floor: three day cells of 1,440 minutes.
    near(
        cells("by_minute", "").await,
        3.0,
        "minute metric at the day floor",
    );
    assert_eq!(
        cell(
            &session,
            "SELECT min(value) = 1440.0 AND max(value) = 1440.0 FROM metric_series() WHERE metric = 'by_minute';",
        )
        .await,
        "true"
    );
    near(
        cells("by_month", "").await,
        48.0,
        "month cells under the month rung",
    );

    // A coarser grain is its own build over that grain's rung: the day
    // metric's 730 days are 24 months, all inside the month rung (48),
    // where its own day cells span the day rung's 18. Where both stand
    // the months equal the day cells summed month by month. A grain
    // finer than a metric's resolution serves nothing — honest absence.
    near(
        cells("by_day", "month").await,
        24.0,
        "the day metric at month grain",
    );
    assert_eq!(
        cell(
            &session,
            "SELECT bool_and(m.value = d.s) FROM metric_series(grain => 'month') m \
             JOIN (SELECT date_trunc('month', period) AS p, sum(value) AS s FROM metric_series() \
                   WHERE metric = 'by_day' AND dimension = '' GROUP BY 1) d ON d.p = m.period \
             WHERE m.metric = 'by_day' AND m.dimension = '';",
        )
        .await,
        "true"
    );
    near(
        cells("by_month", "day").await,
        0.0,
        "a month metric at day grain",
    );
    // The month metric's 60 months are five years under the year rung
    // (20), where its own cells hold the month rung's 48.
    near(
        cells("by_month", "year").await,
        5.0,
        "a month metric at year grain",
    );
    assert_eq!(
        cell(
            &session,
            "SELECT min(value) = 12.0 AND max(value) = 12.0 FROM metric_series(grain => 'year') WHERE metric = 'by_month';",
        )
        .await,
        "true"
    );

    // A gloss on the dataset overrides the floor and one rung: the
    // minute metric now stands at the hour under the hour rung (one
    // day back from its edge: 24 cells of 60), the month metric holds
    // 12, and the day metric is untouched. The gloss moves the pin and
    // the cube rebuilds under the new settings — but the verdicts read
    // the data, not this gloss, so they still stand and the fact rows
    // stay current (the currency rule: what a measurement READ).
    session
        .execute(r#"GLOSS cube ON fin AS $${"resolution": "hour", "windows": {"hour": "1 day", "month": "12 months"}}$$;"#)
        .await
        .unwrap();
    let axes = grid(
        &session,
        "SELECT metric, resolution, window, judged_current FROM metric_axes() ORDER BY metric;",
    )
    .await;
    assert!(
        axes.contains("by_minute | hour       | 1 day     | true"),
        "{axes}"
    );
    assert!(
        axes.contains("by_month  | month      | 12 months | true"),
        "{axes}"
    );
    near(
        cells("by_minute", "").await,
        24.0,
        "hourly cells under the hour rung",
    );
    assert_eq!(
        cell(
            &session,
            "SELECT min(value) = 60.0 AND max(value) = 60.0 FROM metric_series() WHERE metric = 'by_minute';",
        )
        .await,
        "true"
    );
    near(
        cells("by_month", "").await,
        12.0,
        "the shortened month rung",
    );
    near(cells("by_day", "").await, 548.0, "the day rung stands");

    // The floor reaches the top of the ladder: a season-grained dataset
    // stands at the year, under the year rung.
    session
        .execute(
            r#"GLOSS cube ON fin AS $${"resolution": "year", "windows": {"year": "3 years"}}$$;"#,
        )
        .await
        .unwrap();
    let axes = grid(
        &session,
        "SELECT metric, resolution, window FROM metric_axes() ORDER BY metric;",
    )
    .await;
    assert!(axes.contains("by_month  | year       | 3 years"), "{axes}");
    assert!(axes.contains("by_minute | year       | 3 years"), "{axes}");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_cache_builds_once_and_misses_when_the_surface_or_data_moves() {
    let batch = || {
        dated(
            vec![Field::new("value", DataType::Float64, false)],
            MONTH_STARTS.iter().map(|s| 19723 + s).collect(),
            vec![Arc::new(Float64Array::from(vec![1.0; 30]))],
        )
    };
    let glosses = [
        r#"DECLARE ASPECT a WITH $${"title": "A"}$$ AS QUERY ON DATASET;"#,
        r#"DECLARE ASPECT b WITH $${"title": "B"}$$ AS QUERY ON DATASET;"#,
        r#"GLOSS a ON fin AS $${"sql": "SELECT date, value FROM t"}$$;"#,
        r#"GLOSS b ON fin AS $${"sql": "SELECT date, value * 2 AS value FROM t"}$$;"#,
        "SELECT judge_time() FROM t.date;",
    ];

    // Two concurrent reads of one key share one build: the series and
    // the facts asked together cost two builds for two metrics, not
    // four; a repeat is a hit.
    let dir = tempfile::tempdir().unwrap();
    let cache = CubeCache::new(64);
    let session = cube_session_with(
        dir.path(),
        vec![("t", batch())],
        &glosses,
        Some(cache.clone()),
    )
    .await;
    let (series, facts) = tokio::join!(
        session.execute("SELECT count(*) FROM metric_series();"),
        session.execute("SELECT count(*) FROM metric_axes();")
    );
    series.unwrap();
    facts.unwrap();
    assert_eq!(
        cache.builds(),
        2,
        "one build per metric, shared by the two readers"
    );
    near(
        number(&session, "SELECT count(*) FROM metric_series();").await,
        60.0,
        "cells",
    );
    assert_eq!(cache.builds(), 2, "a repeat at the same pin is a hit");

    assert_eq!(
        cell(
            &session,
            "SELECT bool_and(judged_current) FROM metric_axes();"
        )
        .await,
        "true"
    );

    // A note gloss moves the store's version — and nothing a build
    // reads: the metric surface digests alike, the data legs stand,
    // and the next read is a hit. A ruling, a question's answer, a
    // check's landing cost the cube nothing.
    session
        .execute(r#"DECLARE ASPECT note WITH $${"type": "object"}$$ AS FACT ON DATASET; GLOSS note ON fin AS $${"t": 1}$$;"#)
        .await
        .unwrap();
    near(
        number(&session, "SELECT count(*) FROM metric_series();").await,
        60.0,
        "cells across an unrelated gloss",
    );
    assert_eq!(
        cache.builds(),
        2,
        "a write that reaches no build keeps every entry a hit"
    );
    assert_eq!(
        cell(
            &session,
            "SELECT bool_and(judged_current) FROM metric_axes();"
        )
        .await,
        "true"
    );

    // A grounding write IS the surface: the digest moves and every
    // metric misses at the next read. No dump, no invalidation — a
    // complete key.
    session
        .execute(r#"GLOSS a ON fin AS $${"sql": "SELECT date, value * 3 AS value FROM t"}$$;"#)
        .await
        .unwrap();
    near(
        number(&session, "SELECT count(*) FROM metric_series();").await,
        60.0,
        "cells at the new surface",
    );
    assert_eq!(
        cache.builds(),
        4,
        "a moved surface is a miss for every metric"
    );
    assert_eq!(
        cell(
            &session,
            "SELECT bool_and(judged_current) FROM metric_axes();"
        )
        .await,
        "true"
    );

    // Re-measure finds nothing: staleness is a moved leg of what a
    // measurement read, and the gloss moved none of them. Nothing
    // lands, the version holds, and the next read is a hit.
    assert_eq!(
        session.remeasure().await.unwrap(),
        0,
        "no measurement's own inputs moved"
    );
    near(
        number(&session, "SELECT count(*) FROM metric_series();").await,
        60.0,
        "cells again",
    );
    assert_eq!(
        cache.builds(),
        4,
        "nothing landed: the same surface and data serve"
    );
    assert_eq!(
        cell(
            &session,
            "SELECT bool_and(judged_current) FROM metric_axes();"
        )
        .await,
        "true"
    );

    // A cache with no room evicts what it builds: every read rebuilds.
    let dir = tempfile::tempdir().unwrap();
    let tiny = CubeCache::new(0);
    let session = cube_session_with(
        dir.path(),
        vec![("t", batch())],
        &glosses,
        Some(tiny.clone()),
    )
    .await;
    number(&session, "SELECT count(*) FROM metric_series();").await;
    number(&session, "SELECT count(*) FROM metric_series();").await;
    assert_eq!(tiny.builds(), 4, "nothing stays under a zero cap");
}

/// A read at a grain coarser than a metric's resolution is its own
/// entry: built once from the grounding over that grain's rung, a hit
/// after. The metric's own cells serve a read at its own resolution,
/// and a finer grain serves nothing; neither builds.
#[tokio::test(flavor = "multi_thread")]
async fn a_coarser_grain_is_its_own_entry_built_once() {
    let months = dated(
        vec![Field::new("value", DataType::Float64, false)],
        MONTH_STARTS.iter().map(|s| 19723 + s).collect(),
        vec![Arc::new(Float64Array::from(vec![1.0; 30]))],
    );
    let dir = tempfile::tempdir().unwrap();
    let cache = CubeCache::new(64);
    let session = cube_session_with(
        dir.path(),
        vec![("t", months)],
        &[
            r#"DECLARE ASPECT a WITH $${"title": "A"}$$ AS QUERY ON DATASET;"#,
            r#"DECLARE ASPECT b WITH $${"title": "B"}$$ AS QUERY ON DATASET;"#,
            r#"GLOSS a ON fin AS $${"sql": "SELECT date, value FROM t"}$$;"#,
            r#"GLOSS b ON fin AS $${"sql": "SELECT date, value * 2 AS value FROM t"}$$;"#,
            "SELECT judge_time() FROM t.date;",
        ],
        Some(cache.clone()),
    )
    .await;

    near(
        number(&session, "SELECT count(*) FROM metric_series();").await,
        60.0,
        "own cells",
    );
    assert_eq!(cache.builds(), 2, "one build per metric");

    // Thirty months are three years under the year rung, each year the
    // months it holds summed: 2024 and 2025 whole, 2026 half.
    near(
        number(
            &session,
            "SELECT count(*) FROM metric_series(grain => 'year');",
        )
        .await,
        6.0,
        "year cells",
    );
    assert_eq!(
        cache.builds(),
        4,
        "one more build per metric, at the asked grain"
    );
    let year = |metric: &'static str, period: &'static str| {
        let session = &session;
        async move {
            number(
                session,
                &format!(
                    "SELECT value FROM metric_series(grain => 'year') \
                     WHERE metric = '{metric}' AND dimension = '' AND period = '{period}';"
                ),
            )
            .await
        }
    };
    near(year("a", "2024-01-01T00:00:00").await, 12.0, "a whole year");
    near(year("a", "2026-01-01T00:00:00").await, 6.0, "the half year");
    near(
        year("b", "2025-01-01T00:00:00").await,
        24.0,
        "the doubled metric",
    );
    number(
        &session,
        "SELECT count(*) FROM metric_series(grain => 'year');",
    )
    .await;
    assert_eq!(cache.builds(), 4, "a repeat at the grain is a hit");

    near(
        number(
            &session,
            "SELECT count(*) FROM metric_series(grain => 'month');",
        )
        .await,
        60.0,
        "the own resolution asked by name is the own cells",
    );
    near(
        number(
            &session,
            "SELECT count(*) FROM metric_series(grain => 'day');",
        )
        .await,
        0.0,
        "a finer grain is honest absence",
    );
    assert_eq!(
        cache.builds(),
        4,
        "neither the own grain nor a finer one builds"
    );
    assert_eq!(cache.entries().await, 4, "two entries per metric");
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unmarked_metric_takes_the_verdict_on_the_column_it_sums() {
    // Two products, snapshotted mid-month and at month end. A stock
    // folds the month to its last standing (300); a flow sums every
    // row (330) — so the verb shows in the number, not only in the
    // fact row.
    let dir = tempfile::tempdir().unwrap();
    let jan = |d: i32| 19723 + d - 1;
    let levels = dated(
        vec![
            Field::new("level", DataType::Float64, false),
            Field::new("product", DataType::Utf8, false),
        ],
        vec![jan(15), jan(15), jan(31), jan(31)],
        vec![
            Arc::new(Float64Array::from(vec![10.0, 20.0, 100.0, 200.0])),
            Arc::new(StringArray::from(vec!["p1", "p2", "p1", "p2"])),
        ],
    );
    let lines = dated(
        vec![Field::new("amount", DataType::Float64, false)],
        vec![jan(15), jan(31)],
        vec![Arc::new(Float64Array::from(vec![1.0, 2.0]))],
    );
    // A column nobody measured, glossed a stock: the kit's vocabulary
    // read as the verb's policy.
    let stocks = dated(
        vec![Field::new("qty", DataType::Float64, false)],
        vec![jan(15), jan(31)],
        vec![Arc::new(Float64Array::from(vec![10.0, 100.0]))],
    );
    let session = cube_session(
        dir.path(),
        vec![("levels", levels), ("lines", lines), ("stocks", stocks)],
        &[
            r#"DECLARE ASPECT behavior WITH $${"type": "object", "required": ["value"],
                 "properties": {"value": {"enum": ["stock", "flow", "none"]},
                                "grounds": {"type": "string"}}}$$ AS FACT ON COLUMN;"#,
            r#"GLOSS behavior ON stocks.qty AS $${"value": "stock", "grounds": "a carried level"}$$;"#,
            r#"DECLARE ASPECT stocked WITH $${"title": "Stocked"}$$ AS QUERY ON DATASET;"#,
            r#"DECLARE ASPECT overruled WITH $${"title": "Overruled"}$$ AS QUERY ON DATASET;"#,
            r#"DECLARE ASPECT flowed WITH $${"title": "Flowed"}$$ AS QUERY ON DATASET;"#,
            r#"DECLARE ASPECT counted WITH $${"title": "Counted"}$$ AS QUERY ON DATASET;"#,
            r#"DECLARE ASPECT glossed WITH $${"title": "Glossed"}$$ AS QUERY ON DATASET;"#,
            // Unmarked, one sum of a column glossed a stock and never
            // measured.
            r#"GLOSS glossed ON fin AS $${"sql": "SELECT date, sum(qty) AS value FROM stocks GROUP BY date"}$$;"#,
            // Unmarked, one sum of a column judged a stock.
            r#"GLOSS stocked ON fin AS $${"sql": "SELECT date, sum(level) AS value FROM levels GROUP BY date"}$$;"#,
            // The same frame, marked: the grounding's own word wins.
            r#"GLOSS overruled ON fin AS $${"sql": "SELECT date, sum(level) AS value FROM levels GROUP BY date", "behavior": "flow"}$$;"#,
            // Unmarked, the value IS a column judged a flow.
            r#"GLOSS flowed ON fin AS $${"sql": "SELECT date, amount AS value FROM lines"}$$;"#,
            // Unmarked, a count: no column to read, so the default.
            r#"GLOSS counted ON fin AS $${"sql": "SELECT date, count(*) * 1.0 AS value FROM lines GROUP BY date"}$$;"#,
            "SELECT judge_time() FROM levels.date;",
            "SELECT judge_time() FROM lines.date;",
            "SELECT judge_time() FROM stocks.date;",
            "SELECT judge_behavior() FROM levels.level;",
            "SELECT judge_behavior() FROM lines.amount;",
        ],
    )
    .await;
    let verb = |metric: &'static str| {
        let session = &session;
        async move {
            cell(
                session,
                &format!(
                    "SELECT behavior || ':' || behavior_basis FROM metric_axes() \
                     WHERE metric = '{metric}';"
                ),
            )
            .await
        }
    };
    assert_eq!(verb("stocked").await, "stock:evidence");
    assert_eq!(verb("overruled").await, "flow:marked");
    assert_eq!(verb("flowed").await, "flow:evidence");
    assert_eq!(verb("counted").await, "flow:default");
    assert_eq!(verb("glossed").await, "stock:glossed");
    let total = |metric: &'static str| {
        let session = &session;
        async move {
            number(
                session,
                &format!(
                    "SELECT value FROM metric_series() WHERE metric = '{metric}' AND dimension = '';"
                ),
            )
            .await
        }
    };
    near(
        total("stocked").await,
        300.0,
        "the evidence's stock, at its last standing",
    );
    near(total("overruled").await, 330.0, "the marker's flow, summed");
    near(
        total("glossed").await,
        100.0,
        "the gloss's stock, at its last standing",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_door_with_nothing_to_say_serves_the_empty_relation() {
    // Every grounding here is a series, and no walk has landed: the
    // facts read and the recorded walk have no row to serve. Each is
    // the empty relation in its own shape — a frame that unions or
    // counts over it keeps serving — never a refusal.
    let dir = tempfile::tempdir().unwrap();
    let jan = |d: i32| 19723 + d - 1;
    let stocks = dated(
        vec![Field::new("qty", DataType::Float64, false)],
        vec![jan(15), jan(31)],
        vec![Arc::new(Float64Array::from(vec![10.0, 100.0]))],
    );
    let session = cube_session(
        dir.path(),
        vec![("stocks", stocks)],
        &[
            r#"DECLARE ASPECT series WITH $${"title": "Series"}$$ AS QUERY ON DATASET;"#,
            r#"GLOSS series ON fin AS $${"sql": "SELECT date, qty AS value FROM stocks"}$$;"#,
        ],
    )
    .await;
    assert_eq!(
        cell(&session, "SELECT count(*) FROM fact_values();").await,
        "0"
    );
    assert_eq!(
        cell(&session, "SELECT count(*) FROM band_points();").await,
        "0"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn fact_values_serves_the_declared_facts() {
    // The declaration is what makes a fact. Two aspects declare
    // `x-kind: fact`: one frame is a row with a value, the other many
    // rows, and the read says so. A relation, a series, and a
    // grounding that serves no date and declares no kind are not
    // listed — the last is charted by nothing, and the cube's reason
    // is where that shows.
    let dir = tempfile::tempdir().unwrap();
    let jan = |d: i32| 19723 + d - 1;
    let stocks = dated(
        vec![Field::new("qty", DataType::Float64, false)],
        vec![jan(15), jan(31)],
        vec![Arc::new(Float64Array::from(vec![10.0, 100.0]))],
    );
    let session = cube_session(
        dir.path(),
        vec![("stocks", stocks)],
        &[
            r#"DECLARE ASPECT on_hand WITH $${"title": "On hand", "x-kind": "fact"}$$ AS QUERY ON DATASET;"#,
            r#"DECLARE ASPECT each WITH $${"title": "Each", "x-kind": "fact"}$$ AS QUERY ON DATASET;"#,
            r#"DECLARE ASPECT bare WITH $${"title": "Bare", "x-kind": "relation"}$$ AS QUERY ON DATASET;"#,
            r#"DECLARE ASPECT series WITH $${"title": "Series"}$$ AS QUERY ON DATASET;"#,
            r#"DECLARE ASPECT undated WITH $${"title": "Undated"}$$ AS QUERY ON DATASET;"#,
            r#"GLOSS on_hand ON fin AS $${"sql": "SELECT sum(qty) AS value FROM stocks"}$$;"#,
            r#"GLOSS each ON fin AS $${"sql": "SELECT qty AS value FROM stocks"}$$;"#,
            r#"GLOSS bare ON fin AS $${"sql": "SELECT qty FROM stocks"}$$;"#,
            r#"GLOSS series ON fin AS $${"sql": "SELECT date, qty AS value FROM stocks"}$$;"#,
            r#"GLOSS undated ON fin AS $${"sql": "SELECT sum(qty) AS value FROM stocks"}$$;"#,
        ],
    )
    .await;
    let shown = grid(
        &session,
        "SELECT metric, value, reason FROM fact_values() ORDER BY metric;",
    )
    .await;
    assert!(
        shown.contains("| on_hand ") && shown.contains("| 110.0 "),
        "{shown}"
    );
    assert!(
        shown.contains("| each ") && shown.contains("more than one row"),
        "{shown}"
    );
    for not_a_fact in ["bare", "series", "undated"] {
        assert!(
            !shown.contains(&format!("| {not_a_fact} ")),
            "{not_a_fact} declares no fact: {shown}"
        );
    }
    let reason = cell(
        &session,
        "SELECT reason FROM metric_axes() WHERE metric = 'undated';",
    )
    .await;
    assert!(
        reason.starts_with("no judged time column"),
        "the cube's reason is where an undated measure shows: {reason}"
    );
}

/// A metric composed as one flow against another — two `read.` arms
/// under UNION ALL — carries the time axis its arms share: the
/// pre-pass expands the reads inline, and the provenance walk crosses
/// the union where every input lands on the same subject. Arms
/// landing on judged date columns of different tables still abstain
/// with the standing message.
#[tokio::test(flavor = "multi_thread")]
async fn a_union_of_reads_carries_the_time_axis_its_arms_share() {
    let dir = tempfile::tempdir().unwrap();
    let daily = dated(
        vec![Field::new("amount", DataType::Float64, false)],
        vec![19723, 19724],
        vec![Arc::new(Float64Array::from(vec![1.0, 2.0]))],
    );
    let session = cube_session(
        dir.path(),
        vec![("results", results()), ("daily", daily)],
        &[
            r#"DECLARE ASPECT gross WITH $${"title": "Gross"}$$ AS QUERY ON DATASET;"#,
            r#"DECLARE ASPECT rebates WITH $${"title": "Rebates"}$$ AS QUERY ON DATASET;"#,
            r#"DECLARE ASPECT net WITH $${"title": "Net"}$$ AS QUERY ON DATASET;"#,
            r#"DECLARE ASPECT mixed WITH $${"title": "Mixed"}$$ AS QUERY ON DATASET;"#,
            r#"GLOSS gross ON fin AS $${"sql": "SELECT date, points AS value FROM results"}$$;"#,
            r#"GLOSS rebates ON fin AS $${"sql": "SELECT date, points / 10.0 AS value FROM results"}$$;"#,
            r#"GLOSS net ON fin AS $${"sql": "SELECT date, value FROM read.gross() UNION ALL SELECT date, -value AS value FROM read.rebates()"}$$;"#,
            r#"GLOSS mixed ON fin AS $${"sql": "SELECT date, value FROM read.gross() UNION ALL SELECT date, amount AS value FROM daily"}$$;"#,
            "SELECT judge_time() FROM results.date;",
            "SELECT judge_time() FROM daily.date;",
        ],
    )
    .await;

    let axes = grid(
        &session,
        "SELECT metric, applicable, resolution, reason FROM metric_axes() \
         WHERE metric IN ('net', 'mixed') ORDER BY metric;",
    )
    .await;
    assert!(axes.contains("net") && axes.contains("month"), "{axes}");
    assert!(
        axes.contains("mixed") && axes.contains("no judged time column"),
        "{axes}"
    );

    // The union's cells fold as one flow: each month's gross minus a
    // tenth of it.
    let jan = grid(
        &session,
        "SELECT value FROM metric_series() WHERE metric = 'net' AND dimension = '' \
         ORDER BY period LIMIT 1;",
    )
    .await;
    assert!(jan.contains("54.0"), "{jan}");
}

/// A stock over an interval table — `+1` at the start date, `-1` at
/// the end date, unioned into one served date and summed over it —
/// traces through the union to both columns of the one table: the
/// axis stands when both are judged, at the coarser of their cadences
/// (month here, the end date judged at day); with one of them
/// unjudged it is a gap, as it is across tables.
#[tokio::test(flavor = "multi_thread")]
async fn an_interval_stock_traces_through_the_union_of_its_judged_columns() {
    // Three stints: two open on 2024-01-01, one closes 2024-02-01 as
    // another opens, the rest close 2024-03-01 and 2024-04-01 — a
    // headcount of 2, 2, 1, 0 at the month ends.
    let stints = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("from_date", DataType::Date32, false),
            Field::new("to_date", DataType::Date32, false),
        ])),
        vec![
            Arc::new(Date32Array::from(vec![19723, 19723, 19754])),
            Arc::new(Date32Array::from(vec![19783, 19754, 19814])),
        ],
    )
    .unwrap();
    let daily = dated(
        vec![Field::new("amount", DataType::Float64, false)],
        vec![19723, 19724],
        vec![Arc::new(Float64Array::from(vec![1.0, 2.0]))],
    );
    let body = |from: &str, to: &str| {
        format!(
            r#"GLOSS {from}_{to} ON fin AS $${{"sql": "WITH events AS (SELECT {from} AS date, 1 AS delta FROM stints UNION ALL SELECT {to} AS date, -1 AS delta FROM stints), daily AS (SELECT date, sum(delta) AS delta FROM events GROUP BY date) SELECT date, CAST(sum(delta) OVER (ORDER BY date) AS DOUBLE) AS value FROM daily", "behavior": "stock", "grain": ["date"]}}$$;"#
        )
    };
    let dir = tempfile::tempdir().unwrap();
    let session = cube_session(
        dir.path(),
        vec![("stints", stints), ("daily", daily)],
        &[
            r#"DECLARE ASPECT headcount WITH $${"title": "Headcount"}$$ AS QUERY ON DATASET;"#,
            r#"DECLARE ASPECT half WITH $${"title": "Half"}$$ AS QUERY ON DATASET;"#,
            r#"DECLARE ASPECT across WITH $${"title": "Across"}$$ AS QUERY ON DATASET;"#,
            &body("from_date", "to_date").replace("from_date_to_date", "headcount"),
            // The same shape with the end date unjudged, and one with
            // an arm on another table's judged date.
            &body("from_date", "to_date")
                .replace("from_date_to_date", "half")
                .replace("SELECT to_date AS date", "SELECT to_date + INTERVAL '0' DAY AS date"),
            r#"GLOSS across ON fin AS $${"sql": "SELECT from_date AS date, 1.0 AS value FROM stints UNION ALL SELECT date, amount AS value FROM daily", "behavior": "stock", "grain": ["date"]}$$;"#,
            "SELECT judge_time() FROM stints.from_date;",
            "SELECT judge_time() FROM stints.to_date;",
            "SELECT judge_time() FROM daily.date;",
        ],
    )
    .await;

    let axes = grid(
        &session,
        "SELECT metric, applicable, resolution, reason FROM metric_axes() \
         WHERE metric IN ('headcount', 'half', 'across') ORDER BY metric;",
    )
    .await;
    for (metric, applicable, resolution) in [
        ("headcount", "true", "month"),
        ("half", "false", ""),
        ("across", "false", ""),
    ] {
        let sql =
            |column: &str| format!("SELECT {column} FROM metric_axes() WHERE metric = '{metric}';");
        assert_eq!(
            cell(&session, &sql("applicable")).await,
            applicable,
            "{axes}"
        );
        assert_eq!(
            cell(&session, &sql("resolution")).await,
            resolution,
            "{axes}"
        );
    }
    assert!(axes.contains("no judged time column"), "{axes}");

    // The stock's months: the latest level in each — 2, 2, 1, 0.
    let months = grid(
        &session,
        "SELECT period, value FROM metric_series() \
         WHERE metric = 'headcount' AND dimension = '' ORDER BY period;",
    )
    .await;
    for expected in [
        "2024-01-01T00:00:00 | 2.0",
        "2024-03-01T00:00:00 | 1.0",
        "2024-04-01T00:00:00 | 0.0",
    ] {
        assert!(months.contains(expected), "{months}");
    }
}

/// A frame that scans a workspace relation binds its entry to the
/// version: the surface digest cannot see what such a frame reads, so
/// the entry serves only at the version it was built at, and any
/// write is a miss for it — and for it alone.
#[tokio::test(flavor = "multi_thread")]
async fn a_frame_over_a_workspace_relation_rebuilds_with_the_version() {
    let batch = dated(
        vec![Field::new("value", DataType::Float64, false)],
        MONTH_STARTS.iter().map(|s| 19723 + s).collect(),
        vec![Arc::new(Float64Array::from(vec![1.0; 30]))],
    );
    let dir = tempfile::tempdir().unwrap();
    let cache = CubeCache::new(64);
    let session = cube_session_with(
        dir.path(),
        vec![("t", batch)],
        &[
            r#"DECLARE ASPECT meta WITH $${"title": "Meta"}$$ AS QUERY ON DATASET;"#,
            r#"GLOSS meta ON fin AS $${"sql": "SELECT date, value * (SELECT count(*) FROM glossary) AS value FROM t"}$$;"#,
            "SELECT judge_time() FROM t.date;",
        ],
        Some(cache.clone()),
    )
    .await;

    number(&session, "SELECT count(*) FROM metric_series();").await;
    assert_eq!(cache.builds(), 1);
    number(&session, "SELECT count(*) FROM metric_series();").await;
    assert_eq!(cache.builds(), 1, "a repeat at the same version is a hit");

    // An unrelated gloss moves the version. The digest holds — but
    // this frame reads the glossary, and the glossary moved.
    session
        .execute(r#"DECLARE ASPECT note WITH $${"type": "object"}$$ AS FACT ON DATASET; GLOSS note ON fin AS $${"t": 1}$$;"#)
        .await
        .unwrap();
    number(&session, "SELECT count(*) FROM metric_series();").await;
    assert_eq!(
        cache.builds(),
        2,
        "a version-bound entry misses after any write"
    );
}
