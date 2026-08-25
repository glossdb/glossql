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

/// The filter variable — `tracing`'s directive syntax (`info`,
/// `debug`, `glossql_session=debug,info`). `RUST_LOG` is honoured when
/// it is unset; `info` when neither is.
pub const FILTER_VAR: &str = "GLOSSQL_LOG";

/// Install the subscriber: lines for a person when stdout is a
/// terminal, JSON otherwise. `log` records — DataFusion's, sqlx's —
/// are bridged into the same stream.
pub fn install() {
    let filter = EnvFilter::try_from_env(FILTER_VAR)
        .or_else(|_| EnvFilter::try_from_default_env())
        .unwrap_or_else(|_| EnvFilter::new("info"));
    let builder = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_span_events(FmtSpan::CLOSE);
    if std::io::stdout().is_terminal() {
        builder.with_target(false).init();
    } else {
        builder.json().init();
    }
}
