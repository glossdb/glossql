//! The serverd binary: open the workspace, verify who knocks, serve the
//! doors.

use std::path::PathBuf;
use std::sync::Arc;

use glossql_catalog::Lake;
use glossql_glossary::{Actor, ActorKind, Store};
use glossql_scripts::KernelRuntime;
use glossql_serverd::{DoorConfig, Gate, Plane, bootstrap, hand_out, router};

const USAGE: &str = "usage: serverd --workspace <dir> \
[--addr <ip:port>] [--agent <id>] [--row-cap <n>] \
[--cube-cache <megabytes>] [--require-token] \
[--issuer-key <public-key.pem> --issuer <iss>] [--audience <uri>]";

struct Args {
    workspace: PathBuf,
    addr: String,
    doors: DoorConfig,
    /// The process-wide byte budget for cubes, in megabytes.
    cube_cache_mb: u64,
    /// A configured issuer's public key. Without it the workspace mints
    /// and verifies with its own.
    issuer_key: Option<PathBuf>,
    issuer: Option<String>,
    /// This server's canonical URI — the audience every token must name
    /// (RFC 8707 §2). Defaults to the address it listens on.
    audience: Option<String>,
    /// Refuse a request that carries no token at all.
    require_token: bool,
}

fn parse(mut argv: std::env::Args) -> Result<Args, String> {
    argv.next();
    let mut workspace = None;
    let mut addr = "127.0.0.1:8080".to_string();
    let mut doors = DoorConfig::default();
    let mut cube_cache_mb = glossql_session::DEFAULT_CUBE_CACHE_MB;
    let mut issuer_key = None;
    let mut issuer = None;
    let mut audience = None;
    let mut require_token = false;
    while let Some(flag) = argv.next() {
        let mut value = || argv.next().ok_or(format!("{flag} needs a value"));
        match flag.as_str() {
            "--workspace" => workspace = Some(PathBuf::from(value()?)),
            "--addr" => addr = value()?,
            "--agent" => doors.agent = value()?,
            "--issuer-key" => issuer_key = Some(PathBuf::from(value()?)),
            "--issuer" => issuer = Some(value()?),
            "--audience" => audience = Some(value()?),
            "--require-token" => require_token = true,
            "--row-cap" => {
                doors.row_cap = value()?.parse().map_err(|e| format!("--row-cap: {e}"))?;
            }
            "--cube-cache" => {
                cube_cache_mb = value()?.parse().map_err(|e| format!("--cube-cache: {e}"))?;
            }
            other => return Err(format!("unknown flag {other}")),
        }
    }
    if issuer_key.is_some() != issuer.is_some() {
        return Err("--issuer-key and --issuer name one arrangement: give both or neither".into());
    }
    Ok(Args {
        workspace: workspace.ok_or("--workspace is required")?,
        addr,
        doors,
        cube_cache_mb,
        issuer_key,
        issuer,
        audience,
        require_token,
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = parse(std::env::args()).map_err(|e| format!("{e}\n{USAGE}"))?;
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
            .with_cube_cache(args.cube_cache_mb),
    );
    // A fresh workspace receives the shipped system before any door opens.
    bootstrap(
        &plane,
        Actor {
            kind: ActorKind::Human,
            id: glossql_serverd::HUMAN.into(),
        },
    )
    .await?;

    // Who may speak. A configured issuer mints elsewhere and this only
    // verifies; otherwise the workspace holds its own key and hands out
    // one token per actor kind.
    let audience = args
        .audience
        .unwrap_or_else(|| format!("http://{}", args.addr));
    let gate = match (&args.issuer_key, &args.issuer) {
        (Some(key), Some(issuer)) => Arc::new(Gate::issuer(
            key, issuer, &audience,
            // A server told where its tokens come from has no reason to
            // serve anyone who brings none.
            true,
        )?),
        _ => {
            let gate = Arc::new(Gate::local(&args.workspace, &audience, args.require_token)?);
            let handout = hand_out(
                &gate,
                &args.workspace,
                glossql_serverd::HUMAN,
                &args.doors.agent,
            )?;
            println!(
                "glossql tokens in {} — agent.jwt for an MCP client's headers",
                handout.dir.display()
            );
            println!(
                "  open http://{}/?token={} (the door swaps it for a cookie)",
                args.addr, handout.human
            );
            gate
        }
    };
    if !gate.require_token {
        println!(
            "  a request with no token is served as the anonymous human \
             (agent over /mcp) — --require-token to refuse it instead"
        );
    }

    let app = router(plane, args.doors, args.workspace.clone(), gate);
    let listener = tokio::net::TcpListener::bind(&args.addr).await?;
    println!(
        "serverd on {} — / (datasets), /<dataset>/mcp, /<dataset>/query, /<dataset>/app",
        args.addr
    );
    axum::serve(listener, app).await?;
    Ok(())
}
