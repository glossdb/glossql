//! The doors over TLS — for the callers that require it: a desktop
//! MCP client connects to a remote server over https only.
//!
//! The pieces are the tree's own — tokio-rustls over the same rustls
//! reqwest already carries (the ring provider, so no second crypto
//! stack is built), hyper-util under axum — in the accept-loop shape
//! of axum's low-level-rustls example: the handshake runs in the
//! connection's task, never on the accept path. No serving crate on
//! top.
//!
//! The repo carries a self-signed localhost pair for testing
//! (`certs/`, regeneration in docs/reference/doors.md). A deployment
//! that terminates TLS at its edge simply does not pass the flags.

use std::path::Path;
use std::sync::Arc;

use axum::Router;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder;
use hyper_util::service::TowerToHyperService;
use rustls_pki_types::pem::PemObject as _;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::ServerConfig;
use tokio_rustls::rustls::crypto::ring;

/// The TLS arrangement from the two PEM files. Errors are strings for
/// the process edge, like the rest of the binary's.
pub fn config(cert: &Path, key: &Path) -> Result<Arc<ServerConfig>, String> {
    let at_cert = |e: rustls_pki_types::pem::Error| format!("--tls-cert {}: {e}", cert.display());
    let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(cert)
        .map_err(at_cert)?
        .collect::<Result<_, _>>()
        .map_err(at_cert)?;
    let key = PrivateKeyDer::from_pem_file(key)
        .map_err(|e| format!("--tls-key {}: {e}", key.display()))?;
    let mut config = ServerConfig::builder_with_provider(Arc::new(ring::default_provider()))
        .with_safe_default_protocol_versions()
        .map_err(|e| format!("tls: {e}"))?
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| format!("tls: {e}"))?;
    // Both HTTP versions on offer; the client's ALPN picks.
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Ok(Arc::new(config))
}

/// Serve the router over TLS. A handshake that fails is the caller's
/// problem — a plain-http probe, a scanner — and costs its own task,
/// never the accept loop.
pub async fn serve(
    listener: TcpListener,
    app: Router,
    config: Arc<ServerConfig>,
) -> std::io::Result<()> {
    let acceptor = TlsAcceptor::from(config);
    loop {
        let (stream, _) = listener.accept().await?;
        let acceptor = acceptor.clone();
        let service = TowerToHyperService::new(app.clone());
        tokio::spawn(async move {
            let Ok(stream) = acceptor.accept(stream).await else {
                return;
            };
            let _ = Builder::new(TokioExecutor::new())
                .serve_connection_with_upgrades(TokioIo::new(stream), service)
                .await;
        });
    }
}
