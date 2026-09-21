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
[--spill-limit <megabytes>] \
| glossql --version | glossql --help\n\
--workspace is the laptop's shape: the directory holding the catalog \
and the warehouse. GLOSSQL_CATALOG_SQL names the catalog on a Postgres server \
(postgres://…), GLOSSQL_WAREHOUSE the warehouse in an object store \
(s3://…, abfss://…), GLOSSQL_CATALOG_URI a REST catalog with both \
behind it; a deployment names them and runs without a directory.\n\
the band model behind the metric-bands walk, whatif. and misfit. is \
the kernel service named by GLOSSQL_TABICL_URL (its bearer in \
GLOSSQL_TABICL_TOKEN); unset, those three doors refuse by name and \
everything else serves.\n\
the authorization arrangement is read from .env or the environment: \
GLOSSQL_ISSUER, GLOSSQL_CLIENT_ID, GLOSSQL_CLIENT_SECRET, [GLOSSQL_AUDIENCE] \
— or GLOSSQL_INSECURE_OPEN=true serves the doors without authentication, \
every caller recorded as insecure_dev_mode (the name is the warning); \
so is the catalog connection, when there is one: GLOSSQL_CATALOG_URI, \
GLOSSQL_CATALOG_WAREHOUSE and its authentication (see .env.example)";

struct Args {
    /// The laptop's shape, and only that: the directory holding the
    /// catalog and the warehouse. A deployment names both in the
    /// environment and has no directory.
    workspace: Option<PathBuf>,
    addr: String,
    doors: DoorConfig,
    /// The process-wide byte budget for cubes, in megabytes.
    cube_cache_mb: u64,
    /// The engine's memory ceiling for the whole process, in megabytes.
    /// A separate budget from the cubes: the cube cache holds its bytes
    /// outside the engine, so a deployment is sized for the sum.
    memory_limit_mb: u64,
    /// The disk the engine may spill onto, in megabytes — the box's
    /// disk, a number of its own; unset, twice the memory ceiling.
    spill_limit_mb: Option<u64>,
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
    /// (RFC 8707 §2). Defaults to the address the server listens on.
    audience: String,
    /// The application registered at the issuer for this server, and
    /// its secret, which only the browser login uses.
    client_id: String,
    client_secret: String,
}

impl Auth {
    /// Read through `get`, so the reading is testable without touching
    /// the process environment.
    fn from(get: impl Fn(&str) -> Option<String>, addr: &str) -> Result<Auth, String> {
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
            audience: audience(&get, addr),
        })
    }
}

/// This server's own URI — `GLOSSQL_AUDIENCE`, the API identifier a
/// token must name and the host the world reaches the doors at —
/// defaulting to the bind address. Read under both arrangements: the
/// open switch verifies nobody, and the door still has to know its
/// name.
fn audience(get: &impl Fn(&str) -> Option<String>, addr: &str) -> String {
    get("GLOSSQL_AUDIENCE")
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| format!("http://{addr}"))
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
    let mut spill_limit_mb = None;
    while let Some(flag) = argv.next() {
        let mut value = || argv.next().ok_or(format!("{flag} needs a value"));
        match flag.as_str() {
            "--workspace" => workspace = Some(PathBuf::from(value()?)),
            "--addr" => addr = value()?,
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
            "--spill-limit" => {
                spill_limit_mb = Some(
                    value()?
                        .parse()
                        .map_err(|e| format!("--spill-limit: {e}"))?,
                );
            }
            other => return Err(format!("unknown flag {other}")),
        }
    }
    Ok(Args {
        workspace,
        addr,
        doors,
        cube_cache_mb,
        memory_limit_mb,
        spill_limit_mb,
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
    // `.env` in the working directory, when the run has a workspace:
    // the laptop's shape keeps its arrangement in a file beside where
    // it starts, and a variable already set wins over the file. A
    // deployment has no workspace and reads no file — its environment
    // is injected, secrets included.
    if args.workspace.is_some() {
        dotenvy::dotenv().ok();
    }
    serve(args)
}

/// The runtime's whole life: built by the macro, dropped when the
/// server has stopped and the export's final flush is done — the
/// telemetry is installed inside it, since the gRPC export's channel
/// lives on the runtime it is built in, and flushed inside it for the
/// same reason.
#[tokio::main]
async fn serve(args: Args) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // After `.env`, so `GLOSSQL_LOG` and the export switch may come
    // from it; before anything opens, so the opening is on the record.
    let telemetry = glossql_serverd::telemetry::install()?;
    let served = doors(args).await;
    // The flush blocks, so it runs on the blocking pool while the
    // runtime — and the gRPC channel on it — is still alive.
    tokio::task::spawn_blocking(move || telemetry.shutdown())
        .await
        .map_err(|e| format!("the export's final flush: {e}"))?;
    served
}

/// Everything between the runtime's start and the server's stop.
async fn doors(mut args: Args) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // The open switch is read where the arrangement would be, before
    // anything opens: a run is one or the other, and a misconfigured
    // arrangement still refuses rather than falling open.
    let auth = if open(|name| std::env::var(name).ok()) {
        None
    } else {
        Some(
            Auth::from(|name| std::env::var(name).ok(), &args.addr)
                .map_err(|e| format!("{e}\n{USAGE}"))?,
        )
    };
    let own_uri = match &auth {
        Some(auth) => auth.audience.clone(),
        None => audience(&|name| std::env::var(name).ok(), &args.addr),
    };
    args.doors.allowed_hosts = allowed_hosts(&own_uri);
    let lake = open_lake(args.workspace.as_deref())
        .await
        .map_err(|e| format!("{e}\n{USAGE}"))?;
    let store = Store::open(lake).await?;
    // The native kernels, and the kernel service behind the model reads
    // when the environment names one (bodies ride their declarations,
    // fixture 24 — nothing of the runtime's lives in the workspace).
    let runtime = Arc::new(kernel_from(|name| std::env::var(name).ok())?);
    match runtime.kernel_url() {
        Some(url) => tracing::info!(url, "kernel service"),
        None => tracing::info!("no kernel service — the model doors refuse by name"),
    }

    let plane = Plane::new(store.clone(), runtime)
        .with_row_cap(args.doors.row_cap)
        .with_cube_cache(args.cube_cache_mb)
        .with_memory_limit(args.memory_limit_mb)
        .with_spill_limit(args.spill_limit_mb);
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

    let app = router(plane, args.doors, access);
    let listener = tokio::net::TcpListener::bind(&args.addr).await?;
    tracing::info!(
        addr = %args.addr,
        "glossql listening — / (datasets), /mcp, /<dataset>/query, /<dataset>/app"
    );
    tokio::select! {
        served = axum::serve(listener, app).into_future() => served?,
        () = stop() => tracing::info!("stopping"),
    }
    Ok(())
}

/// Whose `Host` header the agent door answers: loopback — the
/// transport's DNS-rebinding guard, for a server on a laptop where a
/// browser could be steered at 127.0.0.1 — and the audience's host,
/// the name the world reaches this server by. A deployment names its
/// URL; one that does not answers its bind address alone, and the
/// transport says so to every real request.
fn allowed_hosts(audience: &str) -> Vec<String> {
    let mut hosts = DoorConfig::default().allowed_hosts;
    if let Some(host) = audience
        .parse::<axum::http::Uri>()
        .ok()
        .and_then(|uri| uri.host().map(str::to_string))
        && !hosts.contains(&host)
    {
        hosts.push(host);
    }
    hosts
}

/// The kernel service the environment names — `GLOSSQL_TABICL_URL`,
/// its bearer in `GLOSSQL_TABICL_TOKEN` — or the native kernels alone.
/// Read through `get` for the reason the other arrangements are:
/// testable without touching the process environment, never flags.
fn kernel_from(get: impl Fn(&str) -> Option<String>) -> Result<KernelRuntime, String> {
    let var = |name: &str| get(name).filter(|v| !v.trim().is_empty());
    match var("GLOSSQL_TABICL_URL") {
        Some(url) => KernelRuntime::with_remote(&url, var("GLOSSQL_TABICL_TOKEN").as_deref()),
        None => Ok(KernelRuntime::native()),
    }
}

/// The workspace data plane: the REST catalog when the environment
/// names one, the SQL catalog otherwise — on the Postgres server the
/// environment names, or the workspace directory's own SQLite file.
/// One backend serves a run; which one is on the record at open. A
/// laptop names a directory and it holds the catalog and the
/// warehouse; a deployment names both in the environment and has no
/// directory at all.
async fn open_lake(workspace: Option<&std::path::Path>) -> Result<Lake, String> {
    #[cfg(feature = "rest")]
    if let Some(connection) = catalog_from(|name| std::env::var(name).ok())? {
        tracing::info!(
            uri = %connection.uri,
            warehouse = %connection.warehouse,
            "connecting the catalog"
        );
        return Lake::connect(connection)
            .await
            .map_err(|e| format!("catalog connection: {e}"));
    }
    open_sql(workspace).await
}

/// What a run without a workspace directory is told when the
/// environment does not name the state either.
const NO_WORKSPACE: &str = "--workspace is required while the catalog or the warehouse lives \
in it: name both GLOSSQL_CATALOG_SQL and GLOSSQL_WAREHOUSE, or the directory";

/// The SQL catalog: on the Postgres server `GLOSSQL_CATALOG_SQL` names,
/// or the workspace directory's own SQLite file; the warehouse at the
/// location `GLOSSQL_WAREHOUSE` names in an object store, or under the
/// workspace directory. Without a directory the environment has to
/// name both. The catalog URI carries the credentials, so the record
/// gets its scheme and nothing more; the warehouse carries none.
#[cfg(feature = "sql")]
async fn open_sql(workspace: Option<&std::path::Path>) -> Result<Lake, String> {
    let var = |name: &str| std::env::var(name).ok().filter(|v| !v.trim().is_empty());
    let catalog = match (var("GLOSSQL_CATALOG_SQL"), workspace) {
        (Some(uri), _) => uri,
        (None, Some(dir)) => format!("sqlite:{}?mode=rwc", dir.join("catalog.sqlite").display()),
        (None, None) => return Err(NO_WORKSPACE.into()),
    };
    let warehouse = match (var("GLOSSQL_WAREHOUSE"), workspace) {
        (Some(location), _) => location,
        (None, Some(dir)) => dir.join("warehouse").display().to_string(),
        (None, None) => return Err(NO_WORKSPACE.into()),
    };
    tracing::info!(
        catalog = catalog.split(':').next().unwrap_or("sql"),
        warehouse = %warehouse,
        "opening the catalog"
    );
    Lake::open_sql(catalog.trim(), warehouse.trim())
        .await
        .map_err(|e| e.to_string())
}

#[cfg(not(feature = "sql"))]
async fn open_sql(_workspace: Option<&std::path::Path>) -> Result<Lake, String> {
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
    use super::{allowed_hosts, audience};

    fn argv(flags: &[&str]) -> Vec<String> {
        std::iter::once("glossql")
            .chain(flags.iter().copied())
            .map(str::to_string)
            .collect()
    }

    /// The audience's host joins the transport's loopback list; a bind
    /// address as the audience adds the bind address and nothing else.
    #[test]
    fn the_agent_door_answers_loopback_and_its_own_name() {
        let named = allowed_hosts("https://glossql-trial.example.azurecontainerapps.io");
        assert!(named.contains(&"glossql-trial.example.azurecontainerapps.io".to_string()));
        assert!(named.contains(&"127.0.0.1".to_string()) && named.len() == 4);
        let bound = allowed_hosts("http://0.0.0.0:8080");
        assert!(bound.contains(&"0.0.0.0".to_string()) && bound.len() == 4);
        assert_eq!(allowed_hosts("http://127.0.0.1:8080").len(), 3);
        let env = |name: &str| {
            (name == "GLOSSQL_AUDIENCE").then(|| "https://glossql.example".to_string())
        };
        assert_eq!(
            audience(&env, "0.0.0.0:8080"),
            "https://glossql.example"
        );
        assert_eq!(
            audience(&|_| None, "127.0.0.1:8080"),
            "http://127.0.0.1:8080"
        );
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
        let none = Auth::from(env(&[]), "127.0.0.1:8080").unwrap_err();
        assert!(
            none.contains("GLOSSQL_ISSUER") && none.contains(".env"),
            "{none}"
        );

        let issuer_only = Auth::from(
            env(&[("GLOSSQL_ISSUER", "https://issuer.test")]),
            "127.0.0.1:8080",
        )
        .unwrap_err();
        assert!(issuer_only.contains("GLOSSQL_CLIENT_ID"), "{issuer_only}");

        let no_secret = Auth::from(
            env(&[
                ("GLOSSQL_ISSUER", "https://issuer.test"),
                ("GLOSSQL_CLIENT_ID", "app-1"),
            ]),
            "127.0.0.1:8080",
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
        assert_eq!(
            set.spill_limit_mb, None,
            "the disk follows the ceiling until it is named"
        );
    }

    /// The disk is the box's own number: named on its own, and never
    /// derived from the memory ceiling once it is.
    #[test]
    fn the_spill_limit_is_a_number_of_its_own() {
        let set = parse(
            argv(&[
                "--workspace",
                "/tmp/w",
                "--memory-limit",
                "4096",
                "--spill-limit",
                "6144",
            ])
            .into_iter(),
        )
        .unwrap();
        assert_eq!(set.spill_limit_mb, Some(6144));
        assert_eq!(set.memory_limit_mb, 4096);
        let bad = parse(argv(&["--workspace", "/tmp/w", "--spill-limit", "six"]).into_iter());
        assert!(bad.err().is_some_and(|e| e.starts_with("--spill-limit:")));
    }
}
