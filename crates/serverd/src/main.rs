//! The serverd binary: open the workspace, verify who knocks, serve the
//! doors.

use std::path::PathBuf;
use std::sync::Arc;

use glossql_catalog::Lake;
use glossql_glossary::{Actor, ActorKind, Store};
use glossql_scripts::KernelRuntime;
use glossql_serverd::{DoorConfig, Gate, Plane, bootstrap, router};

const USAGE: &str = "usage: serverd --workspace <dir> [--addr <ip:port>] \
[--row-cap <n>] [--cube-cache <megabytes>] [--memory-limit <megabytes>]\n\
the authorization arrangement is read from .env or the environment: \
GLOSSQL_ISSUER, GLOSSQL_CLIENT_ID, GLOSSQL_CLIENT_SECRET, [GLOSSQL_AUDIENCE] \
(see .env.example)";

struct Args {
    workspace: PathBuf,
    addr: String,
    doors: DoorConfig,
    /// The process-wide byte budget for cubes, in megabytes.
    cube_cache_mb: u64,
    /// The engine's memory ceiling for the whole process, in megabytes.
    /// A separate budget from the cubes: the cube cache holds its bytes
    /// outside the engine, so a deployment is sized for the sum.
    memory_limit_mb: u64,
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
    /// The application registered at the issuer for this server.
    client_id: String,
}

impl Auth {
    /// Read through `get`, so the reading is testable without touching
    /// the process environment.
    fn from(get: impl Fn(&str) -> Option<String>, addr: &str) -> Result<Auth, String> {
        let required = |name: &str| {
            get(name).filter(|v| !v.trim().is_empty()).ok_or_else(|| {
                format!("{name} is not set — the authorization arrangement lives in .env (see .env.example)")
            })
        };
        Ok(Auth {
            issuer: required("GLOSSQL_ISSUER")?,
            client_id: required("GLOSSQL_CLIENT_ID")?,
            audience: get("GLOSSQL_AUDIENCE")
                .filter(|v| !v.trim().is_empty())
                .unwrap_or_else(|| format!("http://{addr}")),
        })
    }
}

fn parse(mut argv: impl Iterator<Item = String>) -> Result<Args, String> {
    argv.next();
    let mut workspace = None;
    let mut addr = "127.0.0.1:8080".to_string();
    let mut doors = DoorConfig::default();
    let mut cube_cache_mb = glossql_session::DEFAULT_CUBE_CACHE_MB;
    let mut memory_limit_mb = glossql_session::DEFAULT_MEMORY_LIMIT_MB;
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
            other => return Err(format!("unknown flag {other}")),
        }
    }
    Ok(Args {
        workspace: workspace.ok_or("--workspace is required")?,
        addr,
        doors,
        cube_cache_mb,
        memory_limit_mb,
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = parse(std::env::args()).map_err(|e| format!("{e}\n{USAGE}"))?;
    // `.env` in the working directory, when there is one; a variable
    // already set in the environment wins over it, which is how a
    // container configures the same server without a file.
    dotenvy::dotenv().ok();
    let auth = Auth::from(|name| std::env::var(name).ok(), &args.addr)
        .map_err(|e| format!("{e}\n{USAGE}"))?;
    let warehouse = args.workspace.join("warehouse");
    std::fs::create_dir_all(&warehouse)?;

    let lake = Lake::open(&args.workspace.join("catalog.sqlite"), &warehouse).await?;
    let store = Store::open(lake).await?;
    // The runtime's root is the workspace — the band model's weights
    // live under it (bodies ride their declarations, fixture 24).
    let runtime = Arc::new(KernelRuntime::new(args.workspace.clone()));

    let plane = Arc::new(
        Plane::new(store.clone(), runtime)
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
    // here, and a server that cannot reach them does not open.
    let gate = Arc::new(Gate::discover(&auth.issuer, &auth.audience, &auth.client_id).await?);
    println!(
        "glossql verifying {} tokens for {} (application {})",
        gate.issuer(),
        auth.audience,
        auth.client_id
    );

    let app = router(plane, args.doors, args.workspace.clone(), gate);
    let listener = tokio::net::TcpListener::bind(&args.addr).await?;
    println!(
        "serverd on {} — / (datasets), /mcp, /<dataset>/query, /<dataset>/app",
        args.addr
    );
    axum::serve(listener, app).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{Auth, parse};

    fn argv(flags: &[&str]) -> Vec<String> {
        std::iter::once("serverd")
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

        let whole = Auth::from(
            env(&[
                ("GLOSSQL_ISSUER", "https://issuer.test"),
                ("GLOSSQL_CLIENT_ID", "app-1"),
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
                ("GLOSSQL_AUDIENCE", "https://glossql.example"),
            ]),
            "127.0.0.1:8080",
        )
        .unwrap();
        assert_eq!(named.audience, "https://glossql.example");
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
