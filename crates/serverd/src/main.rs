//! The server binary, `glossql`: open the workspace, verify who knocks,
//! serve the doors.

// An unwrap outside a test is a panic waiting for the row that has it;
// tests are exempt (clippy.toml).
#![warn(clippy::unwrap_used)]

use std::future::IntoFuture;
use std::path::PathBuf;
use std::sync::Arc;

use glossql_catalog::Lake;
use glossql_glossary::{Actor, ActorKind, Store};
use glossql_scripts::KernelRuntime;
use glossql_serverd::{
    Access, DoorConfig, Gate, INSECURE_DEV_MODE, Login, Plane, bootstrap, router,
};

const USAGE: &str = "usage: glossql [--workspace <dir>] [--addr <ip:port>] \
[--row-cap <n>] [--cube-cache <megabytes>] [--memory-limit <megabytes>] \
[--tls-cert <pem> --tls-key <pem>] | glossql --version | glossql --help\n\
with --tls-cert and --tls-key the doors serve https — what a desktop \
MCP client requires; certs/ in the repo holds a self-signed localhost \
pair.\n\
--workspace holds apps/ and weights/, and — without a catalog \
connection — the catalog and warehouse themselves, which is why it is \
required then. With GLOSSQL_CATALOG_URI set (a REST catalog; data and \
metadata live behind it) it may be left unnamed: the working directory \
serves.\n\
the authorization arrangement is read from .env or the environment: \
GLOSSQL_ISSUER, GLOSSQL_CLIENT_ID, GLOSSQL_CLIENT_SECRET, [GLOSSQL_AUDIENCE] \
— or GLOSSQL_INSECURE_OPEN=true serves the doors without authentication, \
every caller recorded as insecure_dev_mode (the name is the warning); \
so is the catalog connection, when there is one: GLOSSQL_CATALOG_URI, \
GLOSSQL_CATALOG_WAREHOUSE and its authentication (see .env.example)";

struct Args {
    /// Optional at parse: whether a run can do without one is known
    /// only once the environment says where the catalog is —
    /// [`open_lake`] resolves it.
    workspace: Option<PathBuf>,
    addr: String,
    doors: DoorConfig,
    /// The process-wide byte budget for cubes, in megabytes.
    cube_cache_mb: u64,
    /// The engine's memory ceiling for the whole process, in megabytes.
    /// A separate budget from the cubes: the cube cache holds its bytes
    /// outside the engine, so a deployment is sized for the sum.
    memory_limit_mb: u64,
    /// The certificate and its key, both or neither: with them the
    /// doors serve https ([`glossql_serverd::tls`]).
    tls: Option<(PathBuf, PathBuf)>,
}

/// The authorization arrangement: one issuer, one registered
/// application, one audience. It comes from the environment as one
/// thing — `.env` beside the server or the variables set outright —
/// never from flags, where a secret would sit in a process list.
#[derive(Debug)]
struct Auth {
    /// The authorization server's issuer URL, from which its keys are
    /// discovered.
    issuer: String,
    /// This server's canonical URI — the audience every token must name
    /// (RFC 8707 §2). Defaults to the address the server listens on,
    /// under the scheme it serves.
    audience: String,
    /// The application registered at the issuer for this server, and
    /// its secret, which only the browser login uses.
    client_id: String,
    client_secret: String,
}

impl Auth {
    /// Read through `get`, so the reading is testable without touching
    /// the process environment.
    fn from(
        get: impl Fn(&str) -> Option<String>,
        addr: &str,
        scheme: &str,
    ) -> Result<Auth, String> {
        let required = |name: &str| {
            get(name).filter(|v| !v.trim().is_empty()).ok_or_else(|| {
                format!(
                    "{name} is not set — the authorization arrangement lives in .env \
                     (see .env.example); GLOSSQL_INSECURE_OPEN=true serves open instead, \
                     every caller insecure_dev_mode"
                )
            })
        };
        Ok(Auth {
            issuer: required("GLOSSQL_ISSUER")?,
            client_id: required("GLOSSQL_CLIENT_ID")?,
            client_secret: required("GLOSSQL_CLIENT_SECRET")?,
            audience: get("GLOSSQL_AUDIENCE")
                .filter(|v| !v.trim().is_empty())
                .unwrap_or_else(|| format!("{scheme}://{addr}")),
        })
    }
}

/// The explicit way to serve without the arrangement:
/// `GLOSSQL_INSECURE_OPEN=true` opens every door, every caller
/// recorded as [`INSECURE_DEV_MODE`]. Only the literal `true` counts —
/// the switch is a statement, never a fallback: anything else leaves
/// the gate required, and the refusal names the switch.
fn open(get: impl Fn(&str) -> Option<String>) -> bool {
    get("GLOSSQL_INSECURE_OPEN").is_some_and(|v| v.trim() == "true")
}

fn parse(mut argv: impl Iterator<Item = String>) -> Result<Args, String> {
    argv.next();
    let mut workspace = None;
    let mut addr = "127.0.0.1:8080".to_string();
    let mut doors = DoorConfig::default();
    let mut cube_cache_mb = glossql_session::DEFAULT_CUBE_CACHE_MB;
    let mut memory_limit_mb = glossql_session::DEFAULT_MEMORY_LIMIT_MB;
    let (mut tls_cert, mut tls_key) = (None, None);
    while let Some(flag) = argv.next() {
        let mut value = || argv.next().ok_or(format!("{flag} needs a value"));
        match flag.as_str() {
            "--workspace" => workspace = Some(PathBuf::from(value()?)),
            "--addr" => addr = value()?,
            "--tls-cert" => tls_cert = Some(PathBuf::from(value()?)),
            "--tls-key" => tls_key = Some(PathBuf::from(value()?)),
            "--row-cap" => {
                doors.row_cap = value()?.parse().map_err(|e| format!("--row-cap: {e}"))?;
            }
            "--cube-cache" => {
                cube_cache_mb = value()?.parse().map_err(|e| format!("--cube-cache: {e}"))?;
            }
            "--memory-limit" => {
                memory_limit_mb = value()?
                    .parse()
                    .map_err(|e| format!("--memory-limit: {e}"))?;
            }
            other => return Err(format!("unknown flag {other}")),
        }
    }
    let tls = match (tls_cert, tls_key) {
        (Some(cert), Some(key)) => Some((cert, key)),
        (None, None) => None,
        _ => {
            return Err("--tls-cert and --tls-key come together — \
                 one names the certificate, the other its key"
                .into());
        }
    };
    Ok(Args {
        workspace,
        addr,
        doors,
        cube_cache_mb,
        memory_limit_mb,
        tls,
    })
}

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
    let args = match parse(std::env::args()) {
        Ok(args) => args,
        Err(e) => {
            eprintln!("{e}\n{USAGE}");
            std::process::exit(2);
        }
    };
    // The process edge: an error is printed as its text, never in the
    // Debug form the runtime would give a Result returned from main.
    if let Err(e) = run(args) {
        eprintln!("{e}");
        std::process::exit(1);
    }
}

fn run(args: Args) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // `.env` in the working directory, when there is one; a variable
    // already set in the environment wins over it, which is how a
    // container configures the same server without a file.
    dotenvy::dotenv().ok();
    // After `.env`, so `GLOSSQL_LOG` and the export switch may come
    // from it; before anything opens, so the opening is on the record;
    // outside the runtime, so the final flush comes after it.
    let telemetry = glossql_serverd::telemetry::install()?;
    let served = serve(args);
    telemetry.shutdown();
    served
}

/// The runtime's whole life: built by the macro, dropped when the
/// server has stopped — before the export's final flush, on the main
/// thread.
#[tokio::main]
async fn serve(args: Args) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let scheme = if args.tls.is_some() { "https" } else { "http" };
    // The open switch is read where the arrangement would be, before
    // anything opens: a run is one or the other, and a misconfigured
    // arrangement still refuses rather than falling open.
    let auth = if open(|name| std::env::var(name).ok()) {
        None
    } else {
        Some(
            Auth::from(|name| std::env::var(name).ok(), &args.addr, scheme)
                .map_err(|e| format!("{e}\n{USAGE}"))?,
        )
    };
    let (lake, workspace) = open_lake(args.workspace.clone())
        .await
        .map_err(|e| format!("{e}\n{USAGE}"))?;
    let store = Store::open(lake).await?;
    // The runtime's root is the workspace — the band model's weights
    // live under it (bodies ride their declarations, fixture 24).
    let runtime = Arc::new(KernelRuntime::new(workspace.clone()));

    let plane = Arc::new(
        Plane::new(store.clone(), runtime)
            .with_pages(glossql_serverd::skills::door_pages())
            .with_row_cap(args.doors.row_cap)
            .with_cube_cache(args.cube_cache_mb)
            .with_memory_limit(args.memory_limit_mb),
    );
    // A fresh workspace receives the shipped system before any door opens.
    bootstrap(
        &plane,
        Actor {
            kind: ActorKind::Human,
            id: glossql_serverd::BOOTSTRAP.into(),
        },
    )
    .await?;

    // Who may speak: whoever the issuer says. Its keys are discovered
    // here, and a server that cannot reach them does not open. Under
    // the open switch there is nobody to ask, and the record says so
    // out loud.
    let access = match auth {
        Some(auth) => {
            let gate =
                Arc::new(Gate::discover(&auth.issuer, &auth.audience, &auth.client_id).await?);
            tracing::info!(
                issuer = %gate.issuer(),
                audience = %auth.audience,
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

    let app = router(plane, args.doors, workspace, access);
    let listener = tokio::net::TcpListener::bind(&args.addr).await?;
    tracing::info!(
        addr = %args.addr,
        scheme,
        "glossql listening — / (datasets), /mcp, /<dataset>/query, /<dataset>/app"
    );
    match &args.tls {
        Some((cert, key)) => {
            let config = glossql_serverd::tls::config(cert, key)?;
            tokio::select! {
                served = glossql_serverd::tls::serve(listener, app, config) => served?,
                () = stop() => tracing::info!("stopping"),
            }
        }
        None => {
            tokio::select! {
                served = axum::serve(listener, app).into_future() => served?,
                () = stop() => tracing::info!("stopping"),
            }
        }
    }
    Ok(())
}

/// The workspace data plane: the REST catalog when the environment
/// names one, the workspace directory's own SQLite catalog otherwise.
/// One backend serves a run; which one is on the record at open.
///
/// The directory comes back resolved with the lake, because what it
/// must hold depends on the backend: everything locally, only `apps/`
/// and `weights/` behind a REST catalog — where, unnamed, the working
/// directory serves.
async fn open_lake(workspace: Option<PathBuf>) -> Result<(Lake, PathBuf), String> {
    #[cfg(feature = "rest")]
    if let Some(connection) = catalog_from(|name| std::env::var(name).ok())? {
        tracing::info!(
            uri = %connection.uri,
            warehouse = %connection.warehouse,
            "connecting the catalog"
        );
        let lake = Lake::connect(connection)
            .await
            .map_err(|e| format!("catalog connection: {e}"))?;
        let workspace = match workspace {
            Some(dir) => dir,
            None => std::env::current_dir().map_err(|e| format!("working directory: {e}"))?,
        };
        return Ok((lake, workspace));
    }
    let workspace = workspace.ok_or(
        "--workspace is required without a catalog connection: \
         the directory holds the catalog and the warehouse themselves",
    )?;
    let lake = open_local(&workspace).await?;
    Ok((lake, workspace))
}

#[cfg(feature = "sql")]
async fn open_local(workspace: &std::path::Path) -> Result<Lake, String> {
    let warehouse = workspace.join("warehouse");
    std::fs::create_dir_all(&warehouse)
        .map_err(|e| format!("warehouse dir {}: {e}", warehouse.display()))?;
    Lake::open(&workspace.join("catalog.sqlite"), &warehouse)
        .await
        .map_err(|e| e.to_string())
}

#[cfg(not(feature = "sql"))]
async fn open_local(_workspace: &std::path::Path) -> Result<Lake, String> {
    Err("this build carries no local catalog — set GLOSSQL_CATALOG_URI (see .env.example)".into())
}

/// The catalog connection the environment describes, `None` without
/// `GLOSSQL_CATALOG_URI`. Read through `get` for the reason
/// [`Auth::from`] is: testable without touching the process
/// environment — and like the authorization arrangement it is never
/// flags, where a token would sit in a process list.
#[cfg(feature = "rest")]
fn catalog_from(
    get: impl Fn(&str) -> Option<String>,
) -> Result<Option<glossql_catalog::rest::Connection>, String> {
    use glossql_catalog::rest::{Auth as CatalogAuth, Connection};
    let var = |name: &str| get(name).filter(|v| !v.trim().is_empty());
    let Some(uri) = var("GLOSSQL_CATALOG_URI") else {
        return Ok(None);
    };
    let warehouse = var("GLOSSQL_CATALOG_WAREHOUSE").ok_or(
        "GLOSSQL_CATALOG_WAREHOUSE is not set — a REST catalog connection names its warehouse",
    )?;
    let auth = match (
        var("GLOSSQL_CATALOG_TOKEN"),
        var("GLOSSQL_CATALOG_CREDENTIAL"),
    ) {
        (Some(token), None) => CatalogAuth::Token(token),
        (None, Some(credential)) => CatalogAuth::ClientCredentials {
            credential,
            token_endpoint: var("GLOSSQL_CATALOG_TOKEN_ENDPOINT").ok_or(
                "GLOSSQL_CATALOG_TOKEN_ENDPOINT is not set — a credential is exchanged at its \
                 authorization server's token endpoint",
            )?,
            scope: var("GLOSSQL_CATALOG_SCOPE"),
        },
        (Some(_), Some(_)) => {
            return Err(
                "GLOSSQL_CATALOG_TOKEN and GLOSSQL_CATALOG_CREDENTIAL are both set — \
                 one of them authenticates the catalog connection"
                    .into(),
            );
        }
        (None, None) => {
            return Err(
                "neither GLOSSQL_CATALOG_TOKEN nor GLOSSQL_CATALOG_CREDENTIAL is set — \
                 the catalog connection has nothing to authenticate with"
                    .into(),
            );
        }
    };
    Ok(Some(Connection {
        uri,
        warehouse,
        auth,
    }))
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

#[cfg(test)]
mod tests {
    use super::{Auth, parse};

    fn argv(flags: &[&str]) -> Vec<String> {
        std::iter::once("glossql")
            .chain(flags.iter().copied())
            .map(str::to_string)
            .collect()
    }

    /// There is no open mode: without an issuer and a registered
    /// application there is nothing to verify against, and the server
    /// does not start. The refusal names the variable and where it
    /// lives, since the flags say nothing about it.
    #[test]
    fn the_arrangement_needs_an_issuer_and_an_application() {
        let env = |vars: &'static [(&str, &str)]| {
            move |name: &str| {
                vars.iter()
                    .find(|(k, _)| *k == name)
                    .map(|(_, v)| v.to_string())
            }
        };
        let none = Auth::from(env(&[]), "127.0.0.1:8080", "http").unwrap_err();
        assert!(
            none.contains("GLOSSQL_ISSUER") && none.contains(".env"),
            "{none}"
        );

        let issuer_only = Auth::from(
            env(&[("GLOSSQL_ISSUER", "https://issuer.test")]),
            "127.0.0.1:8080",
            "http",
        )
        .unwrap_err();
        assert!(issuer_only.contains("GLOSSQL_CLIENT_ID"), "{issuer_only}");

        let no_secret = Auth::from(
            env(&[
                ("GLOSSQL_ISSUER", "https://issuer.test"),
                ("GLOSSQL_CLIENT_ID", "app-1"),
            ]),
            "127.0.0.1:8080",
            "http",
        )
        .unwrap_err();
        assert!(no_secret.contains("GLOSSQL_CLIENT_SECRET"), "{no_secret}");

        let whole = Auth::from(
            env(&[
                ("GLOSSQL_ISSUER", "https://issuer.test"),
                ("GLOSSQL_CLIENT_ID", "app-1"),
                ("GLOSSQL_CLIENT_SECRET", "s3cret"),
            ]),
            "127.0.0.1:8080",
            "http",
        )
        .unwrap();
        assert_eq!(
            whole.audience, "http://127.0.0.1:8080",
            "the audience defaults to the address"
        );

        let named = Auth::from(
            env(&[
                ("GLOSSQL_ISSUER", "https://issuer.test"),
                ("GLOSSQL_CLIENT_ID", "app-1"),
                ("GLOSSQL_CLIENT_SECRET", "s3cret"),
                ("GLOSSQL_AUDIENCE", "https://glossql.example"),
            ]),
            "127.0.0.1:8080",
            "http",
        )
        .unwrap();
        assert_eq!(named.audience, "https://glossql.example");
    }

    /// The open switch is a statement, not a fallback: only the
    /// literal `true` opens, anything else keeps the gate required.
    #[test]
    fn only_the_literal_true_opens_the_doors() {
        let env = |v: Option<&'static str>| move |_: &str| v.map(str::to_string);
        assert!(super::open(env(Some("true"))));
        assert!(super::open(env(Some(" true "))), "whitespace is trimmed");
        assert!(!super::open(env(None)));
        assert!(!super::open(env(Some("1"))));
        assert!(!super::open(env(Some("TRUE"))));
        assert!(!super::open(env(Some("false"))));
    }

    /// The catalog connection comes from the environment whole, or not
    /// at all: no URI is the local catalog, a URI must name its
    /// warehouse and exactly one way to authenticate, and a credential
    /// must name where it is exchanged. Each refusal names the missing
    /// variable.
    #[cfg(feature = "rest")]
    #[test]
    fn the_catalog_connection_is_read_whole_or_not_at_all() {
        use glossql_catalog::rest::{Auth as CatalogAuth, Connection};

        let env = |vars: &'static [(&str, &str)]| {
            move |name: &str| {
                vars.iter()
                    .find(|(k, _)| *k == name)
                    .map(|(_, v)| v.to_string())
            }
        };
        // A connection holds auth material, so it carries no `Debug` to
        // unwrap through; a refusal is read off the error alone.
        let refusal =
            |r: Result<Option<Connection>, String>| r.err().expect("a refusal, not a connection");
        assert!(
            super::catalog_from(env(&[])).expect("readable").is_none(),
            "no URI is the local catalog"
        );

        let bare = super::catalog_from(env(&[("GLOSSQL_CATALOG_URI", "https://c.test")]));
        assert!(refusal(bare).contains("GLOSSQL_CATALOG_WAREHOUSE"));

        let unauthenticated = super::catalog_from(env(&[
            ("GLOSSQL_CATALOG_URI", "https://c.test"),
            ("GLOSSQL_CATALOG_WAREHOUSE", "w1"),
        ]));
        assert!(refusal(unauthenticated).contains("GLOSSQL_CATALOG_TOKEN"));

        let token = super::catalog_from(env(&[
            ("GLOSSQL_CATALOG_URI", "https://c.test"),
            ("GLOSSQL_CATALOG_WAREHOUSE", "w1"),
            ("GLOSSQL_CATALOG_TOKEN", "tok"),
        ]))
        .ok()
        .flatten()
        .expect("a connection");
        assert!(matches!(token.auth, CatalogAuth::Token(t) if t == "tok"));

        let endpointless = super::catalog_from(env(&[
            ("GLOSSQL_CATALOG_URI", "https://c.test"),
            ("GLOSSQL_CATALOG_WAREHOUSE", "w1"),
            ("GLOSSQL_CATALOG_CREDENTIAL", "id:secret"),
        ]));
        assert!(refusal(endpointless).contains("GLOSSQL_CATALOG_TOKEN_ENDPOINT"));

        let both = super::catalog_from(env(&[
            ("GLOSSQL_CATALOG_URI", "https://c.test"),
            ("GLOSSQL_CATALOG_WAREHOUSE", "w1"),
            ("GLOSSQL_CATALOG_TOKEN", "tok"),
            ("GLOSSQL_CATALOG_CREDENTIAL", "id:secret"),
        ]));
        assert!(refusal(both).contains("both set"));
    }

    /// TLS is two flags or none: a certificate without its key (or the
    /// reverse) is refused at parse, and under the pair the audience
    /// default carries the scheme actually served.
    #[test]
    fn the_tls_flags_come_together() {
        let lone = parse(argv(&["--tls-cert", "certs/localhost.pem"]).into_iter())
            .err()
            .expect("a lone certificate is refused");
        assert!(lone.contains("--tls-key"), "{lone}");
        let pair = parse(
            argv(&[
                "--tls-cert",
                "certs/localhost.pem",
                "--tls-key",
                "certs/localhost-key.pem",
            ])
            .into_iter(),
        )
        .unwrap();
        assert!(pair.tls.is_some());

        let https = Auth::from(
            |name: &str| {
                [
                    ("GLOSSQL_ISSUER", "https://issuer.test"),
                    ("GLOSSQL_CLIENT_ID", "app-1"),
                    ("GLOSSQL_CLIENT_SECRET", "s3cret"),
                ]
                .iter()
                .find(|(k, _)| *k == name)
                .map(|(_, v)| v.to_string())
            },
            "127.0.0.1:8443",
            "https",
        )
        .unwrap();
        assert_eq!(https.audience, "https://127.0.0.1:8443");
    }

    /// Two budgets, two flags, and neither borrows the other's default.
    /// The cube cache holds its bytes outside the engine, so a
    /// deployment is sized for the sum and the two numbers have to stay
    /// separately nameable.
    #[test]
    fn the_engine_ceiling_and_the_cube_cache_are_separate_numbers() {
        let default = parse(argv(&["--workspace", "/tmp/w"]).into_iter()).unwrap();
        assert_eq!(
            default.memory_limit_mb,
            glossql_session::DEFAULT_MEMORY_LIMIT_MB
        );
        assert_eq!(
            default.cube_cache_mb,
            glossql_session::DEFAULT_CUBE_CACHE_MB
        );

        let set =
            parse(argv(&["--workspace", "/tmp/w", "--memory-limit", "512"]).into_iter()).unwrap();
        assert_eq!(set.memory_limit_mb, 512);
        assert_eq!(
            set.cube_cache_mb,
            glossql_session::DEFAULT_CUBE_CACHE_MB,
            "naming one budget must not move the other"
        );
    }
}
