//! The TLS door and the checked-in localhost pair: both PEM files
//! parse, the handshake completes, and the certificate answers for
//! `localhost` — proven with a client that trusts only the repo's own
//! certificate, so a regenerated pair that breaks any of it fails
//! here instead of at the desktop.

use axum::Router;
use axum::routing::get;

#[tokio::test(flavor = "multi_thread")]
async fn the_checked_in_pair_serves_https_for_localhost() {
    let manifest = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let cert = manifest.join("../../certs/localhost.pem");
    let key = manifest.join("../../certs/localhost-key.pem");
    let config = glossql_serverd::tls::config(&cert, &key).unwrap();

    let app = Router::new().route("/", get(|| async { "over tls" }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(glossql_serverd::tls::serve(listener, app, config));

    // The client trusts the repo certificate and nothing else, and
    // asks by the name the certificate answers for.
    let ca = reqwest::Certificate::from_pem(&std::fs::read(&cert).unwrap()).unwrap();
    let client = reqwest::Client::builder()
        .add_root_certificate(ca)
        .resolve("localhost", addr)
        .build()
        .unwrap();
    let body = client
        .get(format!("https://localhost:{}/", addr.port()))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .text()
        .await
        .unwrap();
    assert_eq!(body, "over tls");
}
