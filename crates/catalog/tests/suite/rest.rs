//! The REST connection, against a stand-in catalog: what rides every
//! request (the bearer, the delegation header), and when a credential
//! is exchanged again.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use axum::Router;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::routing::{get, post};
use glossql_catalog::Lake;
use glossql_catalog::rest::{Auth, Connection};
use serde_json::json;

/// What the stand-in has seen: every Authorization value on a catalog
/// route, every delegation header, and how many exchanges the token
/// endpoint has served.
#[derive(Default)]
struct Seen {
    authorization: std::sync::Mutex<Vec<String>>,
    delegation: std::sync::Mutex<Vec<String>>,
    exchanges: AtomicU64,
    /// The `expires_in` the token endpoint states; `None` states none.
    expires_in: std::sync::Mutex<Option<u64>>,
}

impl Seen {
    fn record(&self, headers: &HeaderMap) {
        let text = |name: &str| {
            headers
                .get(name)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("<missing>")
                .to_string()
        };
        self.authorization
            .lock()
            .expect("seen lock")
            .push(text("authorization"));
        self.delegation
            .lock()
            .expect("seen lock")
            .push(text("x-iceberg-access-delegation"));
    }
}

/// A catalog that answers the handshake and an empty namespace list,
/// with a token endpoint beside it — enough surface for the client's
/// auth path to run whole.
async fn stand_in(seen: Arc<Seen>) -> String {
    let config = |State(seen): State<Arc<Seen>>, headers: HeaderMap| async move {
        seen.record(&headers);
        axum::Json(json!({"defaults": {}, "overrides": {}}))
    };
    let namespaces = |State(seen): State<Arc<Seen>>, headers: HeaderMap| async move {
        seen.record(&headers);
        axum::Json(json!({"namespaces": []}))
    };
    let tokens = |State(seen): State<Arc<Seen>>| async move {
        let n = seen.exchanges.fetch_add(1, Ordering::SeqCst) + 1;
        let expires_in = *seen.expires_in.lock().expect("seen lock");
        let mut token = json!({"access_token": format!("minted-{n}"), "token_type": "bearer"});
        if let Some(seconds) = expires_in {
            token["expires_in"] = json!(seconds);
        }
        axum::Json(token)
    };
    let app = Router::new()
        .route("/v1/config", get(config))
        .route("/v1/namespaces", get(namespaces))
        .route("/tokens", post(tokens))
        .with_state(seen);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("a loopback port");
    let addr = listener.local_addr().expect("a bound address");
    tokio::spawn(async move { axum::serve(listener, app).await.expect("serving") });
    format!("http://{addr}")
}

/// The same two doors as `lake.rs`, against a live REST catalog: run
/// by hand with a rig standing (SeaweedFS + Lakekeeper, or a hosted
/// catalog), the connection read from `GLOSSQL_E2E_CATALOG_URI`,
/// `_WAREHOUSE` and `_TOKEN`. A rig whose store vends nothing gets its
/// keys through `object_store`'s own environment conventions
/// (`AWS_ACCESS_KEY_ID`, `AWS_ENDPOINT`, …).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a live catalog; connection from GLOSSQL_E2E_CATALOG_*"]
async fn live_catalog_round_trip() {
    use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::catalog::CatalogProvider;
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;

    let var = |name: &str| std::env::var(name).ok().filter(|v| !v.is_empty());
    let lake = Lake::connect(Connection {
        uri: var("GLOSSQL_E2E_CATALOG_URI").expect("GLOSSQL_E2E_CATALOG_URI"),
        warehouse: var("GLOSSQL_E2E_CATALOG_WAREHOUSE").expect("GLOSSQL_E2E_CATALOG_WAREHOUSE"),
        auth: Auth::Token(var("GLOSSQL_E2E_CATALOG_TOKEN").expect("GLOSSQL_E2E_CATALOG_TOKEN")),
    })
    .await
    .expect("a live connection");

    // A fresh namespace per run, so re-runs never collide.
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("a clock")
        .as_millis();
    let dataset = format!("e2e_{stamp}");
    assert!(
        lake.ensure_namespace(&dataset, Default::default())
            .await
            .expect("a namespace")
    );

    let orders = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, true),
        Field::new("amount", DataType::Utf8, true),
    ]));
    let ctx = SessionContext::new();
    let provider = lake.provider().await.expect("a provider");
    let schema = provider.schema(&dataset).expect("the fresh namespace");
    ctx.catalog("datafusion")
        .expect("the default catalog")
        .register_schema(&dataset, Arc::clone(&schema))
        .expect("a mount");

    // CREATE through the provider, WRITE through the lake, READ
    // through SQL — the server's own three moves.
    let empty = RecordBatch::new_empty(Arc::clone(&orders));
    schema
        .register_table(
            "orders".into(),
            Arc::new(MemTable::try_new(Arc::clone(&orders), vec![vec![empty]]).expect("a shape")),
        )
        .expect("a create");
    let batch = RecordBatch::try_new(
        Arc::clone(&orders),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["12.50", "8.00", "99.90"])),
        ],
    )
    .expect("a batch");
    lake.append_batches(
        &dataset,
        "orders",
        std::slice::from_ref(&batch),
        HashMap::from([("glossql.source_rows".to_string(), "3".to_string())]),
    )
    .await
    .expect("a commit");

    let landings = lake.landings(&dataset).await.expect("landings");
    assert_eq!(landings.len(), 1);
    assert_eq!(
        landings[0].properties.get("glossql.source_rows"),
        Some(&"3".to_string()),
        "the fact rides the snapshot on this backend too"
    );
    let rows = ctx
        .sql(&format!("SELECT count(*) AS n FROM {dataset}.orders"))
        .await
        .expect("a plan")
        .collect()
        .await
        .expect("a read");
    assert_eq!(
        format!("{:?}", rows[0].column(0)),
        "PrimitiveArray<Int64>\n[\n  3,\n]"
    );
}

/// A static token is attached as it was given, on the handshake and on
/// every catalog request after it — and the delegation header rides
/// along, so a backend that vends storage credentials knows it may.
#[tokio::test]
async fn a_static_token_rides_every_request() {
    let seen = Arc::new(Seen::default());
    let base = stand_in(Arc::clone(&seen)).await;
    let lake = Lake::connect(Connection {
        uri: base,
        warehouse: "w1".into(),
        auth: Auth::Token("tok-static".into()),
    })
    .await
    .expect("a connection");
    assert_eq!(lake.namespaces().await.expect("a namespace list").len(), 0);

    let authorization = seen.authorization.lock().expect("seen lock").clone();
    assert!(!authorization.is_empty());
    assert!(
        authorization.iter().all(|a| a == "Bearer tok-static"),
        "{authorization:?}"
    );
    let delegation = seen.delegation.lock().expect("seen lock").clone();
    assert!(
        delegation.iter().all(|d| d == "vended-credentials"),
        "{delegation:?}"
    );
    assert_eq!(seen.exchanges.load(Ordering::SeqCst), 0);
}

/// A credential is exchanged once and the token reused while it is
/// fresh; a token the endpoint expires immediately is exchanged again
/// on the next request. The built-in manager would serve the first
/// token forever — this is the behavior the connection exists to
/// replace.
#[tokio::test]
async fn a_credential_is_exchanged_again_when_its_token_expires() {
    let seen = Arc::new(Seen::default());
    *seen.expires_in.lock().expect("seen lock") = None;
    let base = stand_in(Arc::clone(&seen)).await;
    let lake = Lake::connect(Connection {
        uri: base.clone(),
        warehouse: "w1".into(),
        auth: Auth::ClientCredentials {
            credential: "id:secret".into(),
            token_endpoint: format!("{base}/tokens"),
            scope: Some("catalog".into()),
        },
    })
    .await
    .expect("a connection");

    // No expiry stated: one exchange serves the handshake and both
    // reads.
    lake.namespaces().await.expect("a namespace list");
    lake.namespaces().await.expect("a namespace list");
    assert_eq!(seen.exchanges.load(Ordering::SeqCst), 1);
    {
        let authorization = seen.authorization.lock().expect("seen lock").clone();
        assert!(
            authorization.iter().all(|a| a == "Bearer minted-1"),
            "{authorization:?}"
        );
    }

    // Now the endpoint expires everything at once: every request is
    // stale by the time the next one asks, so each read exchanges.
    *seen.expires_in.lock().expect("seen lock") = Some(0);
    let lake = Lake::connect(Connection {
        uri: base.clone(),
        warehouse: "w1".into(),
        auth: Auth::ClientCredentials {
            credential: "id:secret".into(),
            token_endpoint: format!("{base}/tokens"),
            scope: None,
        },
    })
    .await
    .expect("a connection");
    let before = seen.exchanges.load(Ordering::SeqCst);
    lake.namespaces().await.expect("a namespace list");
    let between = seen.exchanges.load(Ordering::SeqCst);
    lake.namespaces().await.expect("a namespace list");
    let after = seen.exchanges.load(Ordering::SeqCst);
    assert!(
        before < between && between < after,
        "an expired token must be exchanged again ({before} → {between} → {after})"
    );
}
