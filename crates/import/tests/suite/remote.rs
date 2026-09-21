//! A file source in an object store: the same client the warehouse
//! reaches, the environment alone configuring. Against the dev rig's
//! Azurite (`dev/README.md`):
//!
//!     GLOSSQL_E2E_SOURCE=abfss://lake@devstoreaccount1.dfs.core.windows.net/sources/finance \
//!     AZURE_STORAGE_USE_EMULATOR=true \
//!       cargo test -p glossql-import live_source -- --ignored --nocapture

use datafusion::execution::runtime_env::RuntimeEnv;
use glossql_import::{SourceSpec, list_source, run_probe, run_recipe};
use object_store::ObjectStoreExt;
use serde_json::json;

/// One CSV put under the root through the seam itself, then read back
/// three ways — the listing, a recipe naming the file, a probe over a
/// glob — and a path out of the root refused before any store is asked.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs an object store: GLOSSQL_E2E_SOURCE"]
async fn live_source_in_an_object_store() {
    let Some(root) = std::env::var("GLOSSQL_E2E_SOURCE")
        .ok()
        .filter(|v| !v.is_empty())
    else {
        eprintln!("skipping: GLOSSQL_E2E_SOURCE is not set");
        return;
    };
    let (store, _) = glossql_catalog::storage::environment_store(&root).expect("a store");
    let url = url::Url::parse(&root).expect("a URL");
    let prefix = object_store::path::Path::from_url_path(url.path()).expect("a prefix");
    let key = prefix.join("2026").join("ledger.csv");
    store
        .put(&key, "id,amount\n1,10\n2,20\n".into())
        .await
        .expect("put");

    let spec =
        SourceSpec::from_settings("finance", &json!({"type": "csv", "location": root})).unwrap();

    let files = list_source(&spec).await.expect("a listing");
    assert!(
        files.iter().any(|f| f.path == "2026/ledger.csv"),
        "the listing names the file: {files:?}"
    );

    let (landed, batches) = run_recipe(
        &RuntimeEnv::default(),
        &spec,
        "SELECT id, amount FROM read_csv('2026/ledger.csv')",
    )
    .await
    .expect("a landing");
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 2);
    assert_eq!(
        landed.source_scans,
        vec![("2026/ledger.csv".to_string(), 2)]
    );

    let batches = run_probe(
        &RuntimeEnv::default(),
        &spec,
        "SELECT count(*) AS n FROM read_csv('2026/*.csv')",
        200,
    )
    .await
    .expect("a probe over a glob");
    let n =
        datafusion::arrow::util::display::array_value_to_string(batches[0].column(0), 0).unwrap();
    assert_eq!(n, "2");

    for out in ["../x.csv", "abfss://other@acme.dfs.core.windows.net/x.csv"] {
        let e = run_probe(
            &RuntimeEnv::default(),
            &spec,
            &format!("SELECT * FROM read_csv('{out}')"),
            200,
        )
        .await
        .unwrap_err();
        assert!(
            e.to_string()
                .contains("must stay under the source's location"),
            "{out}: {e}"
        );
    }
}
