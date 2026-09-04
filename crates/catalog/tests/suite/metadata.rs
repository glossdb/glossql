//! The store's relations on Iceberg v3 (stage 3): rows round-trip, the
//! write order comes from the format rather than a column of ours, and a
//! dataset-scoped relation is one table with `dataset` as a key column —
//! partitioned by the format, never routed by a layout of ours.

use glossql_catalog::{IcebergMetadata, Lake, RelationSpec};
use iceberg::{NamespaceIdent, TableIdent};

const RELATIONSHIPS: RelationSpec = RelationSpec {
    name: "relationships",
    columns: &["dataset", "left_path", "op", "right_path"],
    partition: &["dataset"],
};

fn row(v: &[&str]) -> Vec<Option<String>> {
    v.iter().map(|s| Some((*s).to_string())).collect()
}

async fn open(dir: &std::path::Path) -> (Lake, IcebergMetadata) {
    let lake = Lake::open(&dir.join("catalog.db"), &dir.join("warehouse"))
        .await
        .unwrap();
    let rel = IcebergMetadata::open(lake.clone(), "glossql", &[RELATIONSHIPS])
        .await
        .unwrap();
    (lake, rel)
}

#[tokio::test(flavor = "multi_thread")]
async fn rows_round_trip_and_an_empty_relation_is_empty() {
    let dir = tempfile::tempdir().unwrap();
    let (_lake, rel) = open(dir.path()).await;

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
async fn the_dataset_is_a_partition_not_a_namespace() {
    let dir = tempfile::tempdir().unwrap();
    let (lake, rel) = open(dir.path()).await;
    let warehouse = dir.path().join("warehouse");

    // One append, two datasets: one table, the dataset an explicit key
    // column, the physical split per dataset the format's own.
    rel.append(
        "relationships",
        vec![
            row(&["fin", "orders.customer_id", "->", "customers.id"]),
            row(&["books", "review.book_id", "->", "book.id"]),
        ],
    )
    .await
    .unwrap();

    let table = lake
        .catalog()
        .load_table(&TableIdent::new(
            NamespaceIdent::new("glossql".into()),
            "relationships".into(),
        ))
        .await
        .unwrap();
    let spec = table.metadata().default_partition_spec().clone();
    assert_eq!(spec.fields().len(), 1, "identity partition on the key");
    assert_eq!(spec.fields()[0].name, "dataset");
    let pinned = lake.pin_dataset("glossql").await.unwrap();
    let relationships = pinned
        .iter()
        .find(|p| p.name == "relationships")
        .expect("the relation was created above");
    assert_eq!(
        relationships.columns,
        vec!["dataset", "left_path", "op", "right_path"],
        "the dataset is an explicit key column, not a naming convention"
    );

    let mut rows = rel.scan("relationships").await.unwrap();
    rows.sort_by_key(|r| r.cells.clone());
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get(0), Some("books"));
    assert_eq!(rows[1].get(0), Some("fin"));

    // The declared spec is a claim; this is the claim honoured. One
    // append carrying two datasets lands as two files, one per dataset
    // value, in the directories the format names. A writer that emitted
    // one undivided file would satisfy every assertion above and none of
    // these — and a scan would then read every dataset's rows to serve
    // one.
    let mut partitions: Vec<String> = walk(&warehouse)
        .into_iter()
        .filter(|p| p.extension().is_some_and(|e| e == "parquet"))
        .filter_map(|p| {
            p.parent()?
                .file_name()
                .map(|d| d.to_string_lossy().into_owned())
        })
        .collect();
    partitions.sort();
    assert_eq!(
        partitions,
        vec!["dataset=books", "dataset=fin"],
        "one file per dataset, in the format's own partition directories"
    );
}

/// Every file under `dir`, recursively.
fn walk(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return out;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            out.extend(walk(&path));
        } else {
            out.push(path);
        }
    }
    out
}

#[tokio::test(flavor = "multi_thread")]
async fn the_format_supplies_the_write_order() {
    let dir = tempfile::tempdir().unwrap();
    let (_lake, rel) = open(dir.path()).await;

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
        "commits order across, the row id orders within: (seq, row_id) is total"
    );

    // Distinct sequence per commit; the batch shares one and splits on
    // the row id, which the commit assigned in order after every row
    // the earlier commits wrote.
    let seqs: Vec<i64> = ordered.iter().map(|(s, ..)| *s).collect();
    assert!(seqs[0] < seqs[1] && seqs[1] < seqs[2], "{seqs:?}");
    assert_eq!(seqs[3], seqs[4], "one commit, one sequence number");
    let ids: Vec<i64> = ordered.iter().map(|(_, id, _)| *id).collect();
    assert!(
        ids[2] < ids[3],
        "the batch's ids follow the earlier commits': {ids:?}"
    );
    assert_eq!(ids[4], ids[3] + 1, "the row id splits the batch: {ids:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_scan_returns_history_not_the_current_view() {
    let dir = tempfile::tempdir().unwrap();
    let (_lake, rel) = open(dir.path()).await;

    // The same edge written twice comes back twice. Collapsing it is a
    // rule, applied above this seam — see the glossary's own test.
    for _ in 0..2 {
        rel.append("relationships", vec![row(&["fin", "a.k", "->", "b.k"])])
            .await
            .unwrap();
    }
    assert_eq!(rel.scan("relationships").await.unwrap().len(), 2);
}
