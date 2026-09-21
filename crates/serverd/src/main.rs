//! The server binary, `glossql`: open the workspace, verify who knocks,
//! serve the doors.

// An unwrap outside a test is a panic waiting for the row that has it;
// tests are exempt (clippy.toml).
#![warn(clippy::unwrap_used)]

mod config;

use std::future::IntoFuture;
use std::sync::Arc;

use config::{Catalog, Config, USAGE};
use glossql_catalog::Lake;
use glossql_glossary::{Actor, ActorKind, Store};
use glossql_scripts::KernelRuntime;
use glossql_serverd::{Access, Gate, INSECURE_DEV_MODE, Login, Plane, bootstrap, router};

fn main() {
    // Before parse: --version and --help answer and exit — a packaging
    // smoke test runs the first where no workspace or environment
    // stands.
    match std::env::args().nth(1).as_deref() {
        Some("--version") => {
            println!("glossql {}", env!("CARGO_PKG_VERSION"));
            return;
        }
        Some("--help" | "-h") => {
            println!("{USAGE}");
            return;
        }
        _ => {}
    }
    let flags = match config::parse(std::env::args()) {
        Ok(flags) => flags,
        Err(e) => {
            eprintln!("{e}\n{USAGE}");
            std::process::exit(2);
        }
    };
    // The process edge: an error is printed as its text, never in the
    // Debug form the runtime would give a Result returned from main.
    if let Err(e) = run(flags) {
        eprintln!("{e}");
        std::process::exit(1);
    }
}

fn run(flags: config::Flags) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // `.env` in the working directory, when the run has a workspace:
    // the laptop's shape keeps its arrangement in a file beside where
    // it starts, and a variable already set wins over the file. A
    // deployment has no workspace and reads no file — its environment
    // is injected, secrets included.
    if flags.workspace.is_some() {
        dotenvy::dotenv().ok();
    }
    let config = Config::read(flags, |name| std::env::var(name).ok())
        .map_err(|e| format!("{e}\n{USAGE}"))?;
    serve(config)
}

/// The runtime's whole life: built by the macro, dropped when the
/// server has stopped and the export's final flush is done — the
/// telemetry is installed inside it, since the gRPC export's channel
/// lives on the runtime it is built in, and flushed inside it for the
/// same reason.
#[tokio::main]
async fn serve(config: Config) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // After `.env`, so `GLOSSQL_LOG` and the export switch may come
    // from it; before anything opens, so the opening is on the record.
    let telemetry = glossql_serverd::telemetry::install()?;
    let served = doors(config).await;
    // The flush blocks, so it runs on the blocking pool while the
    // runtime — and the gRPC channel on it — is still alive.
    tokio::task::spawn_blocking(move || telemetry.shutdown())
        .await
        .map_err(|e| format!("the export's final flush: {e}"))?;
    served
}

/// Everything between the runtime's start and the server's stop.
async fn doors(config: Config) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // The native kernels, and the kernel service behind the model reads
    // when the environment names one (bodies ride their declarations,
    // fixture 24 — nothing of the runtime's lives in the workspace).
    let runtime = Arc::new(match &config.kernel {
        Some(kernel) => KernelRuntime::with_remote(&kernel.url, kernel.token.as_deref())?,
        None => KernelRuntime::native(),
    });
    match runtime.kernel_url() {
        Some(url) => tracing::info!(url, "kernel service"),
        None => tracing::info!("no kernel service — the model doors refuse by name"),
    }
    let store = Store::open(open_lake(config.catalog).await?).await?;

    let plane = Plane::new(store.clone(), runtime)
        .with_row_cap(config.row_cap)
        .with_cube_cache(config.cube_cache_mb)
        .with_memory_limit(config.memory_limit_mb)
        .with_spill_limit(config.spill_limit_mb);
    // A fresh workspace receives the shipped system before any door opens.
    bootstrap(
        &plane,
        Actor {
            kind: ActorKind::Human,
            id: glossql_serverd::BOOTSTRAP.into(),
        },
    )
    .await?;
    // The function listings read the registries the shipped system
    // declared into, so they follow the bootstrap.
    let listings = glossql_serverd::functions::pages(&plane).await?;
    let plane = Arc::new(plane.with_pages(glossql_serverd::skills::door_pages_with(listings)));

    // Who may speak: whoever the issuer says. Its keys are discovered
    // here, and a server that cannot reach them does not open. Under
    // the open switch there is nobody to ask, and the record says so
    // out loud.
    let access = match config.auth {
        Some(auth) => {
            let gate =
                Arc::new(Gate::discover(&auth.issuer, &config.audience, &auth.client_id).await?);
            tracing::info!(
                issuer = %gate.issuer(),
                audience = %config.audience,
                application = %auth.client_id,
                "verifying tokens"
            );
            Access::Gated(Arc::new(Login::new(gate, &auth.client_secret)?))
        }
        None => {
            tracing::warn!(
                actor = INSECURE_DEV_MODE,
                "GLOSSQL_INSECURE_OPEN — the doors are open, nobody is verified"
            );
            Access::Open
        }
    };

    let app = router(plane, config.doors, access);
    let listener = tokio::net::TcpListener::bind(&config.addr).await?;
    tracing::info!(
        addr = %config.addr,
        "glossql listening — / (datasets), /mcp, /<dataset>/query, /<dataset>/app"
    );
    // The first stop signal closes the listener and the idle
    // connections and lets what is in flight finish — a write mid-commit
    // lands rather than being cut. A second one ends the wait; a
    // platform's own bound is the kill that follows its grace period.
    let stopping = Arc::new(tokio::sync::Notify::new());
    let drain = {
        let stopping = Arc::clone(&stopping);
        async move {
            stop().await;
            tracing::info!("stopping: what is in flight finishes first");
            stopping.notify_one();
        }
    };
    tokio::select! {
        served = axum::serve(listener, app).with_graceful_shutdown(drain).into_future() => served?,
        () = async { stopping.notified().await; stop().await } => {
            tracing::info!("stopped before the drain finished");
        }
    }
    Ok(())
}

/// Open the one backend the run names; which one is on the record at
/// open. The SQL catalog's URI carries the credentials, so the record
/// gets its scheme and nothing more.
async fn open_lake(catalog: Catalog) -> Result<Lake, String> {
    match catalog {
        #[cfg(feature = "rest")]
        Catalog::Rest(connection) => {
            tracing::info!(
                uri = %connection.uri,
                warehouse = %connection.warehouse,
                "connecting the catalog"
            );
            Lake::connect(connection)
                .await
                .map_err(|e| format!("catalog connection: {e}"))
        }
        #[cfg(feature = "sql")]
        Catalog::Sql { catalog, warehouse } => {
            tracing::info!(
                catalog = catalog.split(':').next().unwrap_or("sql"),
                warehouse = %warehouse,
                "opening the catalog"
            );
            Lake::open_sql(&catalog, &warehouse)
                .await
                .map_err(|e| e.to_string())
        }
    }
}

/// SIGINT or SIGTERM — the terminal's Ctrl-C or the platform's stop.
/// Either ends the server through its own exit, so what is queued for
/// export is sent rather than lost with the process.
async fn stop() {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("SIGTERM can be listened for on any unix");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = terminate.recv() => {}
    }
}
