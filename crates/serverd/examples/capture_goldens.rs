//! Stage 0: land a corpus as a workspace and capture what every shipped
//! function answers on it — the regression baseline the port is measured
//! against (`reports/2026-08-17-the-foundation.md` §6).
//!
//! A refusal or an abstention is behaviour too, so errors are captured
//! verbatim beside values. Volatile fields are stripped so a re-run
//! compares.
//!
//! Usage:
//!   capture_goldens <tables-dir> <dataset> <out-dir> [--functions a,b]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use std::sync::Arc;

use glossql_catalog::Lake;
use glossql_glossary::{Actor, ActorKind, Store};
use glossql_scripts::RhaiRuntime;
use glossql_session::{Outcome, Plane, Session};
use glossql_serverd::bootstrap;

/// Table name from a parquet file name: `event_attendees.parquet` →
/// `event_attendees`.
fn table_of(p: &Path) -> Option<(String, &'static str)> {
    let reader = match p.extension()?.to_str()? {
        "parquet" => ("parquet", "read_parquet"),
        "csv" => ("csv", "read_csv"),
        _ => return None,
    };
    Some((p.file_stem()?.to_str()?.to_string(), reader.1))
}

fn source_kind(files: &[PathBuf]) -> &'static str {
    if files.iter().any(|f| f.extension().is_some_and(|e| e == "csv")) {
        "csv"
    } else {
        "parquet"
    }
}

fn render(outcome: &Outcome) -> serde_json::Value {
    match outcome {
        Outcome::Done(s) => serde_json::json!({ "done": s }),
        Outcome::Affected(n) => serde_json::json!({ "affected": n }),
        Outcome::Rows(batches) => {
            let mut out = Vec::new();
            for b in batches.iter().filter(|b| b.num_rows() > 0) {
                let mut buf = Vec::new();
                let mut w = arrow_json::ArrayWriter::new(&mut buf);
                if w.write(b).is_ok() && w.finish().is_ok() {
                    if let Ok(v) = serde_json::from_slice::<serde_json::Value>(&buf) {
                        if let Some(a) = v.as_array() {
                            out.extend(a.iter().cloned());
                        }
                    }
                }
            }
            serde_json::Value::Array(out)
        }
    }
}

/// Sums over partitions do not associate: DataFusion may add the same
/// values in a different order run to run, and the last bit moves.
///
/// Rounding settles it only if the quantum is comfortably above the
/// noise, because a value sitting on a rounding boundary flips either
/// way. Twelve digits was not: booksql's larger sums carry a relative
/// noise near 3e-12, and two means flipped between runs
/// (`31996.2576049` / `31996.257605`). Eight digits leaves four orders
/// of margin, and a real change moves far more than 1 part in 1e8.
fn round_sig(x: f64, digits: i32) -> f64 {
    if x == 0.0 || !x.is_finite() {
        return x;
    }
    // A residual of 2e-17 means the two series matched exactly; its
    // significant digits ARE the summation noise, so rounding cannot
    // settle it. Below this floor a number is zero and says so.
    if x.abs() < 1e-12 {
        return 0.0;
    }
    let mag = x.abs().log10().floor();
    let factor = 10f64.powi(digits - 1 - mag as i32);
    (x * factor).round() / factor
}

/// Timestamps and snapshot ids move between runs and say nothing about
/// behaviour. Everything else is compared.
fn scrub(v: &mut serde_json::Value) {
    const VOLATILE: [&str; 5] = [
        "computed_at",
        "written_at",
        "imported_at",
        "snapshot_id",
        "committed_at",
    ];
    match v {
        serde_json::Value::Object(m) => {
            for k in VOLATILE {
                if m.contains_key(k) {
                    m.insert(k.into(), serde_json::Value::String("<volatile>".into()));
                }
            }
            for (_, val) in m.iter_mut() {
                scrub(val);
            }
        }
        serde_json::Value::Array(a) => a.iter_mut().for_each(scrub),
        serde_json::Value::Number(n) => {
            if let Some(f) = n.as_f64()
                && !n.is_i64()
                && !n.is_u64()
                && let Some(r) = serde_json::Number::from_f64(round_sig(f, 8))
            {
                *v = serde_json::Value::Number(r);
            }
        }
        // A body rides as a JSON *string*; normalise inside it too, or the
        // rounding never reaches the numbers that actually move.
        serde_json::Value::String(s) => {
            let trimmed = s.trim_start();
            if trimmed.starts_with('{') || trimmed.starts_with('[') {
                if let Ok(mut inner) = serde_json::from_str::<serde_json::Value>(s) {
                    scrub(&mut inner);
                    if let Ok(text) = serde_json::to_string(&inner) {
                        *s = text;
                    }
                }
            }
        }
        _ => {}
    }
}

/// Column names from a `LIMIT 0`: the PROBE rule ships one empty batch
/// carrying the schema, so this reads metadata and no rows.
async fn columns_of(session: &Session, table: &str) -> Vec<String> {
    match session.execute(&format!("SELECT * FROM \"{table}\" LIMIT 0;")).await {
        Ok(outcomes) => outcomes
            .iter()
            .find_map(|o| match o {
                Outcome::Rows(b) => b.first().map(|b| {
                    b.schema().fields().iter().map(|f| f.name().clone()).collect()
                }),
                _ => None,
            })
            .unwrap_or_default(),
        Err(e) => {
            println!("  schema of {table} failed: {e}");
            Vec::new()
        }
    }
}

async fn cell(session: &Session, sql: &str) -> Vec<Vec<String>> {
    let Ok(outcomes) = session.execute(sql).await else {
        return Vec::new();
    };
    let mut rows = Vec::new();
    for o in &outcomes {
        if let Outcome::Rows(batches) = o {
            for b in batches.iter().filter(|b| b.num_rows() > 0) {
                for i in 0..b.num_rows() {
                    let mut row = Vec::new();
                    for c in 0..b.num_columns() {
                        row.push(
                            datafusion::arrow::util::display::array_value_to_string(b.column(c), i)
                                .unwrap_or_default(),
                        );
                    }
                    rows.push(row);
                }
            }
        }
    }
    rows
}

/// One statement per `;` — but not inside a dollar-quoted body, where a
/// rhai script's own semicolons live, and not on a comment line, where
/// prose punctuation is not syntax.
fn split_statements(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut inside = false;
    for line in text.lines() {
        let comment = !inside && line.trim_start().starts_with("--");
        let mut chars = line.chars().peekable();
        while let Some(c) = chars.next() {
            if c == '$' && chars.peek() == Some(&'$') {
                chars.next();
                cur.push_str("$$");
                inside = !inside;
                continue;
            }
            cur.push(c);
            if c == ';' && !inside && !comment {
                out.push(std::mem::take(&mut cur));
            }
        }
        cur.push('\n');
    }
    out.push(cur);
    out
}

/// Run a `.glossql` setup file statement by statement, reporting what the
/// door refuses rather than stopping — a refusal is worth seeing.
async fn run_setup(session: &Session, path: &Path) -> usize {
    let text = std::fs::read_to_string(path).expect("setup file");
    let mut n = 0;
    for stmt in split_statements(&text) {
        let stmt = stmt.trim();
        if stmt.is_empty() || stmt.lines().all(|l| l.trim_start().starts_with("--")) {
            continue;
        }
        match session.execute(stmt).await {
            Ok(_) => n += 1,
            Err(e) => println!("  REFUSED setup: {e}"),
        }
    }
    println!("  ran {n} setup statement(s) from {}", path.display());
    n
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let mut args = std::env::args().skip(1);
    let tables = PathBuf::from(args.next().expect("usage: <tables-dir> <dataset> <out-dir>"));
    let dataset: String = args.next().expect("dataset name");
    let out = PathBuf::from(args.next().expect("out dir"));
    let mut only: Option<Vec<String>> = None;
    let mut setup: Option<PathBuf> = None;
    // A workspace that already carries glosses and groundings is captured
    // as it stands — re-landing it would throw away what makes it useful.
    let mut existing = false;
    for a in args {
        if let Some(v) = a.strip_prefix("--functions=") {
            only = Some(v.split(',').map(str::to_string).collect());
        } else if let Some(v) = a.strip_prefix("--setup=") {
            setup = Some(PathBuf::from(v));
        } else if a == "--existing" {
            existing = true;
        }
    }

    std::fs::create_dir_all(&out).unwrap();
    let ws = if existing { tables.clone() } else { out.join("ws") };
    if !existing {
        if ws.exists() {
            std::fs::remove_dir_all(&ws).unwrap();
        }
        std::fs::create_dir_all(&ws).unwrap();
    }

    let lake = Lake::open(&ws.join("catalog.sqlite"), &ws.join("warehouse"))
        .await
        .unwrap();
    let store = Store::open(lake.clone()).await.unwrap();
    // The same wiring `serverd` uses, so the workspace receives the
    // shipped library before anything is asked of it.
    let actor = Actor {
        kind: ActorKind::Agent,
        id: "goldens".into(),
    };
    let runtime = Arc::new(RhaiRuntime::new(ws.clone()));
    let plane = Plane::new(store.clone(), runtime);
    bootstrap(&store, &plane, actor.clone()).await.expect("bootstrap");
    let session = plane.session(actor).await.expect("session");

    // ---- land ----------------------------------------------------------
    let t0 = std::time::Instant::now();

    let mut files: Vec<PathBuf> = std::fs::read_dir(&tables)
        .expect("tables dir")
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| {
            p.extension()
                .is_some_and(|e| e == "parquet" || e == "csv")
        })
        .collect();
    files.sort();
    let kind = source_kind(&files);
    if existing {
        session.execute(&format!("USE {dataset};")).await.expect("use");
        // An existing workspace can need declarations too — fin's FK
        // truth was re-declared when `relationships` crossed to the lake.
        // Declaring is idempotent, so this runs on every capture rather
        // than being a one-off somebody has to remember.
        if let Some(path) = &setup {
            run_setup(&session, path).await;
        }
        let names = lake_tables(&session, &dataset).await;
        println!("existing workspace: {} tables", names.len());
        capture(&session, &dataset, names, &only, &out).await;
        return;
    }
    session
        .execute(&format!(
            "DECLARE DATASET {dataset} SET (purpose: 'golden corpus');\n\
             USE {dataset};\n\
             DECLARE SOURCE src SET (type: {kind}, location: '{}');",
            tables.display()
        ))
        .await
        .expect("dataset, source");

    let mut landed = Vec::new();
    for f in &files {
        let Some((name, reader)) = table_of(f) else { continue };
        let file = f.file_name().unwrap().to_str().unwrap();
        let sql = format!(
            "DECLARE RECIPE {name} ON {dataset} FROM src AS $$SELECT * FROM {reader}('{file}')$$;"
        );
        match session.execute(&sql).await {
            Ok(o) => {
                println!("  landed {name}: {}", o.last().map(render).unwrap_or_default());
                landed.push(name);
            }
            Err(e) => println!("  REFUSED {name}: {e}"),
        }
    }
    // RelBench ships the FK truth beside the tables; declaring it is what
    // turns on the borrowed-axis, shared-parent and coherence paths.
    let mut rels = 0usize;
    let schema_json = tables.parent().map(|p| p.join("schema.json"));
    if let Some(sj) = schema_json.filter(|p| p.exists())
        && let Ok(text) = std::fs::read_to_string(&sj)
        && let Ok(v) = serde_json::from_str::<serde_json::Value>(&text)
        && let Some(tables_map) = v.as_object()
    {
        for (child, spec) in tables_map {
            if !landed.contains(child) {
                continue;
            }
            let Some(fkeys) = spec.get("fkeys").and_then(|f| f.as_object()) else {
                continue;
            };
            for (col, parent) in fkeys {
                let Some(parent) = parent.as_str() else { continue };
                if !landed.iter().any(|t| t == parent) {
                    continue;
                }
                let pkey = tables_map
                    .get(parent)
                    .and_then(|p| p.get("pkey"))
                    .and_then(|p| p.as_str());
                let Some(pkey) = pkey else { continue };
                let sql = format!(
                    "DECLARE RELATIONSHIP {child}.\"{col}\" -> {parent}.\"{pkey}\";"
                );
                match session.execute(&sql).await {
                    Ok(_) => rels += 1,
                    Err(e) => println!("  REFUSED {child}.{col} -> {parent}.{pkey}: {e}"),
                }
            }
        }
        println!("  declared {rels} relationships from schema.json");
    }

    // A corpus that ships no FK truth declares it by hand.
    if let Some(path) = &setup {
        run_setup(&session, path).await;
    }

    println!(
        "landed {}/{} tables in {:.1}s",
        landed.len(),
        files.len(),
        t0.elapsed().as_secs_f64()
    );
    if landed.is_empty() {
        println!("nothing landed — stopping before the goldens");
        return;
    }
    capture(&session, &dataset, landed, &only, &out).await;
}

/// Table names in the dataset, for a workspace we did not land ourselves.
async fn lake_tables(session: &Session, dataset: &str) -> Vec<String> {
    cell(
        session,
        &format!(
            "SELECT DISTINCT table_name FROM imports WHERE dataset = '{dataset}' ORDER BY 1;"
        ),
    )
    .await
    .into_iter()
    .filter_map(|r| r.into_iter().next())
    .collect()
}

/// Run every shipped function over every subject of its grain and write
/// the answers — values and refusals alike.
async fn capture(
    session: &Session,
    dataset: &str,
    landed: Vec<String>,
    only: &Option<Vec<String>>,
    out: &Path,
) {
    // ---- the subject grid ----------------------------------------------
    let mut columns: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for t in &landed {
        columns.insert(t.clone(), columns_of(&session, t).await);
    }
    println!(
        "  {} tables, {} columns",
        columns.len(),
        columns.values().map(|c| c.len()).sum::<usize>()
    );

    // ---- functions and their grains -------------------------------------
    let fns = cell(
        &session,
        "SELECT f.name, f.returns, a.grains FROM functions f \
         LEFT JOIN aspects a ON a.name = f.returns ORDER BY f.name;",
    )
    .await;

    let mut goldens: BTreeMap<String, serde_json::Value> = BTreeMap::new();
    let mut calls = 0usize;
    let t0 = std::time::Instant::now();

    for row in &fns {
        let name = row[0].clone();
        if let Some(only) = &only
            && !only.contains(&name)
        {
            continue;
        }
        let grains = row.get(2).cloned().unwrap_or_default().to_lowercase();
        let mut subjects: Vec<String> = Vec::new();
        if grains.contains("dataset") || grains.is_empty() {
            subjects.push(dataset.to_string());
        }
        if grains.contains("table") {
            subjects.extend(landed.iter().cloned());
        }
        if grains.contains("column") {
            for (t, cs) in &columns {
                subjects.extend(cs.iter().map(|c| format!("{t}.{c}")));
            }
        }

        let mut per_subject = serde_json::Map::new();
        for s in subjects {
            let sql = format!("SELECT {name}() FROM {s};");
            calls += 1;
            let entry = match session.execute(&sql).await {
                Ok(o) => {
                    let mut v = o.last().map(render).unwrap_or(serde_json::Value::Null);
                    scrub(&mut v);
                    v
                }
                Err(e) => serde_json::json!({ "error": e.to_string() }),
            };
            per_subject.insert(s, entry);
        }
        println!(
            "  {name}: {} subject(s) [{grains}]",
            per_subject.len()
        );
        goldens.insert(name, serde_json::Value::Object(per_subject));
    }

    let dir = out.join("goldens");
    std::fs::create_dir_all(&dir).unwrap();
    for (name, v) in &goldens {
        let path = dir.join(format!("{name}.json"));
        std::fs::write(&path, serde_json::to_string_pretty(v).unwrap()).unwrap();
    }
    // Extraction serves a summary where a body carries one; the whole
    // value reads back from the store. Dump the measurements this
    // capture landed as the value golden — verdicts compute at read and
    // are covered by the outcome files.
    let measured = cell(
        &session,
        "SELECT subject, function, value FROM measurements ORDER BY 1, 2;",
    )
    .await;
    let mut values = serde_json::Map::new();
    for r in &measured {
        let key = format!("{}::{}", r[0], r[1]);
        let mut body: serde_json::Value =
            serde_json::from_str(&r[2]).unwrap_or(serde_json::Value::String(r[2].clone()));
        scrub(&mut body);
        values.insert(key, body);
    }
    std::fs::write(
        dir.join("_values.json"),
        serde_json::to_string_pretty(&serde_json::Value::Object(values)).unwrap(),
    )
    .unwrap();
    println!("  {} computed values dumped", measured.len());

    println!(
        "captured {} functions, {calls} calls, {:.1}s → {}",
        goldens.len(),
        t0.elapsed().as_secs_f64(),
        dir.display()
    );
}
