//! The serverd binary: open the workspace, serve the doors.

use std::path::PathBuf;
use std::sync::Arc;

use glossql_catalog::Lake;
use glossql_glossary::{Actor, ActorKind, Store};
use glossql_scripts::RhaiRuntime;
use glossql_serverd::{DoorConfig, Plane, bootstrap, router};

const USAGE: &str = "usage: serverd --workspace <dir> \
[--addr <ip:port>] [--agent <id>] [--row-cap <n>] [--elicit-probe]";

struct Args {
    workspace: PathBuf,
    addr: String,
    doors: DoorConfig,
}

fn parse(mut argv: std::env::Args) -> Result<Args, String> {
    argv.next();
    let mut workspace = None;
    let mut addr = "127.0.0.1:8080".to_string();
    let mut doors = DoorConfig::default();
    while let Some(flag) = argv.next() {
        let mut value = || argv.next().ok_or(format!("{flag} needs a value"));
        match flag.as_str() {
            "--workspace" => workspace = Some(PathBuf::from(value()?)),
            "--addr" => addr = value()?,
            "--agent" => doors.agent = value()?,
            "--row-cap" => {
                doors.row_cap = value()?.parse().map_err(|e| format!("--row-cap: {e}"))?;
            }
            "--elicit-probe" => doors.elicit_probe = true,
            other => return Err(format!("unknown flag {other}")),
        }
    }
    Ok(Args {
        workspace: workspace.ok_or("--workspace is required")?,
        addr,
        doors,
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = parse(std::env::args()).map_err(|e| format!("{e}\n{USAGE}"))?;
    let warehouse = args.workspace.join("warehouse");
    std::fs::create_dir_all(&warehouse)?;

    let store_url = format!(
        "sqlite://{}?mode=rwc",
        args.workspace.join("glossary.sqlite").display()
    );
    let store = Store::open(&store_url).await?;
    let lake = Lake::open(&args.workspace.join("catalog.sqlite"), &warehouse).await?;
    // The runtime's root is the workspace, so declarations reference
    // scripts the way the corpus spells them: 'functions/profile.rhai'.
    let runtime = Arc::new(RhaiRuntime::new(args.workspace.clone()));

    let plane =
        Arc::new(Plane::new(store.clone(), Some(lake), runtime).with_row_cap(args.doors.row_cap));
    // A fresh workspace receives the shipped system before any door opens.
    bootstrap(
        &store,
        &plane,
        &args.workspace,
        Actor {
            kind: ActorKind::Human,
            id: glossql_serverd::HUMAN.into(),
        },
    )
    .await?;
    let app = router(plane, args.doors, args.workspace.clone());
    let listener = tokio::net::TcpListener::bind(&args.addr).await?;
    println!(
        "serverd on {} — /mcp (agent door), /query (arrow door), /app (app door)",
        args.addr
    );
    axum::serve(listener, app).await?;
    Ok(())
}
