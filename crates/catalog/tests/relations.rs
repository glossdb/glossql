//! The store's relations on Iceberg v3 (stage 3): rows round-trip, and
//! the write order comes from the format rather than a column of ours.

use glossql_catalog::{IcebergRelations, Lake};
use glossql_glossary::Relations;

const RELATIONSHIPS: (&str, &[&str]) = (
    "relationships",
    &["dataset", "left_path", "op", "right_path"],
);

fn row(v: &[&str]) -> Vec<Option<String>> {
    v.iter().map(|s| Some((*s).to_string())).collect()
}

async fn open(dir: &std::path::Path) -> IcebergRelations {
    let lake = Lake::open(&dir.join("catalog.db"), &dir.join("warehouse"))
        .await
        .unwrap();
    IcebergRelations::open(lake, "fin_meta", &[RELATIONSHIPS])
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn rows_round_trip_and_an_empty_relation_is_empty() {
    let dir = tempfile::tempdir().unwrap();
    let rel = open(dir.path()).await;

    assert!(
        rel.scan("relationships").await.unwrap().is_empty(),
        "a relation nobody has written to serves no rows, and does not fail"
    );

    rel.append(
        "relationships",
        vec![
            row(&["fin", "orders.customer_id", "->", "customers.id"]),
            row(&["fin", "lines.order_id", "->", "orders.id"]),
        ],
    )
    .await
    .unwrap();

    let rows = rel.scan("relationships").await.unwrap();
    assert_eq!(rows.len(), 2);
    let mut paths: Vec<&str> = rows.iter().filter_map(|r| r.get(1)).collect();
    paths.sort();
    assert_eq!(paths, vec!["lines.order_id", "orders.customer_id"]);
    assert_eq!(rows[0].get(0), Some("fin"));
}

#[tokio::test(flavor = "multi_thread")]
async fn the_format_supplies_the_write_order() {
    let dir = tempfile::tempdir().unwrap();
    let rel = open(dir.path()).await;

    // Three commits, then two rows inside one.
    for i in 1..=3 {
        rel.append(
            "relationships",
            vec![row(&["fin", &format!("t{i}.k"), "->", "u.k"])],
        )
        .await
        .unwrap();
    }
    rel.append(
        "relationships",
        vec![
            row(&["fin", "batch.a", "->", "u.k"]),
            row(&["fin", "batch.b", "->", "u.k"]),
        ],
    )
    .await
    .unwrap();

    let rows = rel.scan("relationships").await.unwrap();
    assert_eq!(rows.len(), 5);

    // Nothing of ours minted these: the catalog assigned them at commit.
    let mut ordered: Vec<(i64, i64, &str)> = rows
        .iter()
        .map(|r| (r.seq.0, r.seq.1, r.get(1).unwrap()))
        .collect();
    ordered.sort();
    let names: Vec<&str> = ordered.iter().map(|(_, _, n)| *n).collect();
    assert_eq!(
        names,
        vec!["t1.k", "t2.k", "t3.k", "batch.a", "batch.b"],
        "commits order across, position orders within: (seq, pos) is total"
    );

    // Distinct sequence per commit; the batch shares one and splits on pos.
    let seqs: Vec<i64> = ordered.iter().map(|(s, ..)| *s).collect();
    assert!(seqs[0] < seqs[1] && seqs[1] < seqs[2], "{seqs:?}");
    assert_eq!(seqs[3], seqs[4], "one commit, one sequence number");
    assert_eq!((ordered[3].1, ordered[4].1), (0, 1), "position splits them");
}

#[tokio::test(flavor = "multi_thread")]
async fn supersession_is_the_rule_applied_over_history() {
    let dir = tempfile::tempdir().unwrap();
    let rel = open(dir.path()).await;

    // The same edge declared twice: the scan returns both, because a scan
    // returns history. Which one stands is `rules::latest_by`, not the
    // store — that is the whole point of the seam.
    rel.append("relationships", vec![row(&["fin", "a.k", "->", "b.k"])])
        .await
        .unwrap();
    rel.append("relationships", vec![row(&["fin", "a.k", "<->", "b.k"])])
        .await
        .unwrap();

    let rows = rel.scan("relationships").await.unwrap();
    assert_eq!(rows.len(), 2, "history, not the current view");

    let current = glossql_glossary::rules::latest_by(
        rows,
        |r| (r.get(0).map(str::to_string), r.get(1).map(str::to_string)),
        |r| r.seq,
    );
    assert_eq!(current.len(), 1);
    assert_eq!(current[0].get(2), Some("<->"), "the later write stands");
}
