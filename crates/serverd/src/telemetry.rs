//! The process edge of instrumentation: one subscriber, installed at
//! startup. Everything below it emits `tracing` spans and events and
//! knows nothing about where they go.
//!
//! What is emitted, and at which level, is a policy: at `info` a call
//! is its actor, dataset, the digest and length of its text and the
//! spans of the work it caused — never the text itself. The text is a
//! `debug` event inside the call's span, because statement bodies and
//! groundings carry data. Refusals carry their reason and never the
//! token. Spans close with their busy and idle time, which is where
//! the durations come from.

use std::io::IsTerminal;

use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::format::FmtSpan;

/// The filter variable — `tracing`'s directive syntax. A bare level
/// (`debug`) is this server's crates at that level and the substrate
/// at `info`; directives (`glossql_session=debug,apache_avro=debug`)
/// are taken as written. `RUST_LOG` is honoured when it is unset;
/// `info` when neither is.
pub const FILTER_VAR: &str = "GLOSSQL_LOG";

/// The directives a bare level stands for. Targets match by prefix
/// (tracing-subscriber `filter/env/directive.rs`), so `glossql` is
/// every `glossql_*` crate and `serverd` the binary's own module.
fn directives(level: &str) -> String {
    format!("info,glossql={level},serverd={level}")
}

/// Install the subscriber: lines for a person when stdout is a
/// terminal, JSON otherwise. `log` records — DataFusion's, sqlx's, the
/// Avro reader's — are bridged into the same stream, which is why a
/// bare level stays ours: at `debug` the Avro reader alone writes ten
/// thousand lines per boot.
pub fn install() {
    let requested = std::env::var(FILTER_VAR)
        .or_else(|_| std::env::var("RUST_LOG"))
        .unwrap_or_else(|_| "info".into());
    let bare = !requested.contains(['=', ',']);
    let filter = if bare {
        EnvFilter::new(directives(requested.trim()))
    } else {
        EnvFilter::new(requested)
    };
    let builder = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_span_events(FmtSpan::CLOSE);
    if std::io::stdout().is_terminal() {
        builder.with_target(false).init();
    } else {
        builder.json().init();
    }
}
