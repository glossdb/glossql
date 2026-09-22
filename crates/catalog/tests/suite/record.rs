//! The store's relations on the catalog's database: rows round-trip,
//! the write order is the database's identity, a number column is
//! typed as one, and a scan hands back history.

use glossql_catalog::{Lake, Number, Record, RelationSpec};

const RELATIONSHIPS: RelationSpec = RelationSpec {
    name: "relationships",
    columns: &["dataset", "left_path", "op", "right_path"],
    numbers: &[],
};

const GLOSSARY: RelationSpec = RelationSpec {
    name: "glossary",
    columns: &["dataset", "subject", "body", "snapshot_id", "weight"],
    numbers: &[("snapshot_id", Number::Integer), ("weight", Number::Real)],
};

fn row(v: &[&str]) -> Vec<Option<String>> {
    v.iter().map(|s| Some((*s).to_string())).collect()
}

async fn open(dir: &std::path::Path) -> Record {
    let lake = Lake::open(&dir.join("catalog.db"), &dir.join("warehouse"))
        .await
        .unwrap();
    Record::open(lake.database(), &[RELATIONSHIPS, GLOSSARY])
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
    assert_eq!(
        rel.versions().await.unwrap(),
        vec![
            ("glossary".to_string(), None),
            ("relationships".to_string(), None)
        ],
        "an unwritten relation has no version"
    );

    rel.append(
        "relationships",
        vec![
            row(&["fin", "orders.customer_id", "->", "customers.id"]),
            row(&["books", "review.book_id", "->", "book.id"]),
        ],
    )
    .await
    .unwrap();

    let rows = rel.scan("relationships").await.unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get(0), Some("fin"));
    assert_eq!(rows[1].get(1), Some("review.book_id"));

    // The dataset is a key column: a read scoped to one selects its rows.
    let fin = rel
        .scan_where("relationships", "dataset", "fin")
        .await
        .unwrap();
    assert_eq!(fin.len(), 1);
    assert_eq!(fin[0].get(1), Some("orders.customer_id"));
    assert_eq!(
        rel.versions().await.unwrap()[1],
        ("relationships".to_string(), Some(2)),
        "the version is the highest seq"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_database_supplies_the_write_order() {
    let dir = tempfile::tempdir().unwrap();
    let rel = open(dir.path()).await;

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
    let names: Vec<&str> = rows.iter().map(|r| r.get(1).unwrap()).collect();
    assert_eq!(names, vec!["t1.k", "t2.k", "t3.k", "batch.a", "batch.b"]);
    let seqs: Vec<i64> = rows.iter().map(|r| r.seq).collect();
    assert!(
        seqs.windows(2).all(|w| w[0] < w[1]),
        "every row has its own place, inside one append as across them: {seqs:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_scan_returns_history_not_the_current_view() {
    let dir = tempfile::tempdir().unwrap();
    let rel = open(dir.path()).await;

    // The same edge written twice comes back twice. Collapsing it is a
    // rule, applied above this seam — see the glossary's own test.
    for _ in 0..2 {
        rel.append("relationships", vec![row(&["fin", "a.k", "->", "b.k"])])
            .await
            .unwrap();
    }
    assert_eq!(rel.scan("relationships").await.unwrap().len(), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_number_column_is_a_number_and_crosses_as_its_text() {
    let dir = tempfile::tempdir().unwrap();
    let rel = open(dir.path()).await;

    rel.append(
        "glossary",
        vec![
            vec![
                Some("fin".into()),
                Some("orders".into()),
                Some("{}".into()),
                Some("42".into()),
                Some("0.5".into()),
            ],
            vec![
                Some("fin".into()),
                Some("lines".into()),
                Some("{}".into()),
                None,
                None,
            ],
        ],
    )
    .await
    .unwrap();
    let rows = rel.scan("glossary").await.unwrap();
    assert_eq!(rows[0].get(3), Some("42"));
    assert_eq!(rows[0].get(4), Some("0.5"));
    assert_eq!(rows[1].get(3), None, "a missing number is NULL, not zero");
    // The other order too: a missing number first, then one — the
    // statement text differs by what is bound, so neither prepared
    // shape stands in for the other.
    rel.append(
        "glossary",
        vec![vec![
            Some("fin".into()),
            Some("items".into()),
            Some("{}".into()),
            Some("7".into()),
            Some("1".into()),
        ]],
    )
    .await
    .unwrap();
    assert_eq!(rel.scan("glossary").await.unwrap()[2].get(3), Some("7"));

    let refused = rel
        .append(
            "glossary",
            vec![vec![
                Some("fin".into()),
                Some("orders".into()),
                Some("{}".into()),
                Some("current".into()),
                None,
            ]],
        )
        .await;
    assert!(
        refused
            .unwrap_err()
            .to_string()
            .contains("`glossary.snapshot_id` is a number"),
        "text in a number column is the writer's error, named"
    );
    assert_eq!(
        rel.scan("glossary").await.unwrap().len(),
        3,
        "nothing landed"
    );
}
