//! The serverd binary: open the workspace, verify who knocks, serve the
//! doors.

use std::path::PathBuf;
use std::sync::Arc;

use glossql_catalog::Lake;
use glossql_glossary::{Actor, ActorKind, Store};
use glossql_scripts::KernelRuntime;
use glossql_serverd::{DoorConfig, Gate, Plane, bootstrap, router};

const USAGE: &str = "usage: serverd --workspace <dir> \
[--addr <ip:port>] [--row-cap <n>] \
[--cube-cache <megabytes>] [--require-token] \
[--public-key <key.pem> --issuer <iss>] [--audience <uri>]";

struct Args {
    workspace: PathBuf,
    addr: String,
    doors: DoorConfig,
    /// The process-wide byte budget for cubes, in megabytes.
    cube_cache_mb: u64,
    /// The public key tokens are verified against. Without it no token
    /// and verifies with its own.
    public_key: Option<PathBuf>,
    issuer: Option<String>,
    /// This server's canonical URI — the audience every token must name
    /// (RFC 8707 §2). Defaults to the address it listens on.
    audience: Option<String>,
    /// Refuse a request that carries no token at all.
    require_token: bool,
}

fn parse(mut argv: impl Iterator<Item = String>) -> Result<Args, String> {
    argv.next();
    let mut workspace = None;
    let mut addr = "127.0.0.1:8080".to_string();
    let mut doors = DoorConfig::default();
    let mut cube_cache_mb = glossql_session::DEFAULT_CUBE_CACHE_MB;
    let mut public_key = None;
    let mut issuer = None;
    let mut audience = None;
    let mut require_token = false;
    while let Some(flag) = argv.next() {
        let mut value = || argv.next().ok_or(format!("{flag} needs a value"));
        match flag.as_str() {
            "--workspace" => workspace = Some(PathBuf::from(value()?)),
            "--addr" => addr = value()?,
            "--public-key" => public_key = Some(PathBuf::from(value()?)),
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
    if public_key.is_some() != issuer.is_some() {
        return Err("--public-key and --issuer name one arrangement: give both or neither".into());
    }
    // Refusing a request needs something to verify the token against, so
    // this pair is an arrangement too. Accepted alone it would read as
    // "the door is shut" and leave it open.
    if require_token && public_key.is_none() {
        return Err(
            "--require-token needs --public-key and --issuer: there is nothing to verify \
             a token against without them"
                .into(),
        );
    }
    Ok(Args {
        workspace: workspace.ok_or("--workspace is required")?,
        addr,
        doors,
        cube_cache_mb,
        public_key,
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

    // Who may speak. Whoever holds the private half mints; this only
    // ever verifies. Without a public key there is nothing to verify
    // against and no gate at all — the doors write as they did before
    // tokens existed, which is how a fresh workspace is opened.
    let audience = args
        .audience
        .unwrap_or_else(|| format!("http://{}", args.addr));
    let gate = match (&args.public_key, &args.issuer) {
        (Some(key), Some(issuer)) => {
            let gate = Arc::new(Gate::issuer(key, issuer, &audience, args.require_token)?);
            println!(
                "glossql verifying {} tokens for {audience}",
                gate.minted_by()
            );
            if !gate.require_token {
                println!(
                    "  a request with no token is still served as the anonymous human \
                     (agent over /mcp) — --require-token to refuse it instead"
                );
            }
            Some(gate)
        }
        _ => {
            println!(
                "glossql open — no --public-key, so no token is verified and every \
                 request writes as the anonymous human (agent over /mcp).\n  \
                 Development tokens and the key that verifies them are in dev/."
            );
            None
        }
    };

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
    use super::parse;

    fn argv(flags: &[&str]) -> Vec<String> {
        std::iter::once("serverd")
            .chain(flags.iter().copied())
            .map(str::to_string)
            .collect()
    }

    /// `--require-token` alone reads as "the door is shut" and leaves it
    /// open: with no key there is no gate, so every request is served as
    /// the door's own default. A flag whose only effect is to mislead is
    /// refused at the boundary.
    #[test]
    fn require_token_without_a_key_is_refused() {
        let alone = parse(argv(&["--workspace", "/tmp/w", "--require-token"]).into_iter())
            .err()
            .expect("--require-token alone must not be accepted");
        assert!(alone.contains("--public-key"), "{alone}");

        let paired = parse(
            argv(&[
                "--workspace",
                "/tmp/w",
                "--require-token",
                "--public-key",
                "/tmp/k.pem",
                "--issuer",
                "glossql-dev",
            ])
            .into_iter(),
        );
        assert!(paired.is_ok(), "the pair stands together");
    }
}
