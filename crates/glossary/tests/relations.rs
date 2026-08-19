//! Relations that have crossed to the lake, read back through the store.
//!
//! The catalog's own suite proves the seam (rows round-trip, `(seq, pos)`
//! orders writes, a scan returns history). This proves the half that is
//! ours: what the sqlite primary key used to do, the rule now does.

use glossql_glossary::Store;

async fn edges(store: &Store) -> Vec<Vec<Option<String>>> {
    store.relation_rows("relationships").await.unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_re_declared_edge_collapses_the_way_the_primary_key_did() {
    let (_dir, store) = scratch_store().await;

    for _ in 0..2 {
        store
            .declare_relationship("fin", "orders.customer_id", "->", "customers.id")
            .await
            .unwrap();
    }

    assert_eq!(
        edges(&store).await,
        vec![vec![
            Some("fin".into()),
            Some("orders.customer_id".into()),
            Some("->".into()),
            Some("customers.id".into()),
        ]],
        "sqlite had INSERT OR REPLACE on all four columns; an append \
         cannot, so the collapse is the rule over history"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_op_is_part_of_the_key_so_a_second_op_stands_beside_the_first() {
    let (_dir, store) = scratch_store().await;
    store
        .declare_relationship("fin", "a.k", "->", "b.k")
        .await
        .unwrap();
    store
        .declare_relationship("fin", "a.k", "<->", "b.k")
        .await
        .unwrap();

    let rows = edges(&store).await;
    assert_eq!(rows.len(), 2, "two edges, not one superseding the other");
    // Sorted by cells, so `->` sorts before `<->`.
    assert_eq!(rows[0][2], Some("->".into()));
    assert_eq!(rows[1][2], Some("<->".into()));
}

#[tokio::test(flavor = "multi_thread")]
async fn two_datasets_serve_as_one_relation() {
    let (_dir, store) = scratch_store().await;
    store
        .declare_relationship("fin", "orders.customer_id", "->", "customers.id")
        .await
        .unwrap();
    store
        .declare_relationship("books", "review.book_id", "->", "book.id")
        .await
        .unwrap();

    let rows = edges(&store).await;
    assert_eq!(rows.len(), 2, "each dataset's record, one served relation");
    assert_eq!(rows[0][0], Some("books".into()));
    assert_eq!(rows[1][0], Some("fin".into()));
}

#[tokio::test(flavor = "multi_thread")]
async fn the_dump_is_ordered_because_the_doors_compare_it() {
    let (_dir, store) = scratch_store().await;
    for (l, r) in [("z.k", "y.k"), ("a.k", "b.k"), ("m.k", "n.k")] {
        store.declare_relationship("fin", l, "->", r).await.unwrap();
    }
    let paths: Vec<_> = edges(&store)
        .await
        .into_iter()
        .map(|r| r[1].clone().unwrap())
        .collect();
    assert_eq!(
        paths,
        vec!["a.k", "m.k", "z.k"],
        "a scan hands back file order; the ORDER BY it replaced is ours to keep"
    );
}

/// A store over its own throwaway lake; hold the dir for the test's life.
async fn scratch_store() -> (tempfile::TempDir, Store) {
    let dir = tempfile::tempdir().unwrap();
    let lake = glossql_catalog::Lake::open(
        &dir.path().join("catalog.sqlite"),
        &dir.path().join("warehouse"),
    )
    .await
    .unwrap();
    let store = Store::open(lake).await.unwrap();
    (dir, store)
}
