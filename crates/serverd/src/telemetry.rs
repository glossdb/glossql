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
//!
//! Where it goes: stdout always, and an OTLP collector when
//! [`ENDPOINT_VAR`] names one. The export is the OpenTelemetry SDK's
//! own pipeline — its batch processor runs on a thread of its own and
//! sends over HTTP from there (`opentelemetry_sdk`
//! `trace/span_processor.rs`), so the engine's runtime, whose workers
//! run the partitions, never carries an export. Events inside a span
//! travel with it; events outside every span — the startup lines —
//! are stdout's alone.

use std::io::IsTerminal;
use std::time::Duration;

use axum::http::{Request, Response};
use opentelemetry::KeyValue;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_http::HeaderExtractor;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

/// The filter variable — `tracing`'s directive syntax. A bare level
/// (`debug`) is this server's crates at that level, the substrate at
/// `info` and the MCP library at `warn`; directives
/// (`glossql_session=debug,apache_avro=debug`) are taken as written. `RUST_LOG` is honoured when it is unset;
/// `info` when neither is.
pub const FILTER_VAR: &str = "GLOSSQL_LOG";

/// The export switch, the SDK's own variable: where an OTLP collector
/// listens (`http://127.0.0.1:4318`; the SDK appends `/v1/traces`).
/// Unset, nothing is exported. The rest of the exporter's
/// configuration is the SDK's as well — `OTEL_EXPORTER_OTLP_HEADERS`
/// carries a hosted collector's credentials, `OTEL_RESOURCE_ATTRIBUTES`
/// what names a deployment beyond `service.name`, which is `glossql`.
pub const ENDPOINT_VAR: &str = "OTEL_EXPORTER_OTLP_ENDPOINT";

/// The directives a bare level stands for. Targets match by prefix
/// (tracing-subscriber `filter/env/directive.rs`), so `glossql` is
/// every `glossql_*` crate and `serverd` the binary's own module. The
/// MCP library narrates every request's service lifecycle at `info` —
/// initialized, stream terminated, finished — which the request span
/// already says; its `warn` is where it reports a problem.
fn directives(level: &str) -> String {
    format!("info,rmcp=warn,glossql={level},serverd={level}")
}

/// What `install` hands back: the export, to be drained once the
/// server has stopped.
pub struct Telemetry {
    traces: Option<SdkTracerProvider>,
}

impl Telemetry {
    /// Send what is still queued and stop the export thread, within
    /// the SDK's five seconds. For after the runtime, on the main
    /// thread: it blocks, and nothing produces spans any more.
    pub fn shutdown(self) {
        if let Some(traces) = self.traces
            && let Err(e) = traces.shutdown()
        {
            tracing::warn!(error = %e, "traces not flushed");
        }
    }
}

/// Install the subscriber: lines for a person when stdout is a
/// terminal, JSON otherwise, and the OTLP export when it is switched
/// on. `log` records — DataFusion's, sqlx's, the Avro reader's — are
/// bridged into the same stream, which is why a bare level stays
/// ours: at `debug` the Avro reader alone writes ten thousand lines
/// per boot. One filter gates both sinks: what is not on the record
/// is not exported either.
pub fn install() -> Result<Telemetry, String> {
    let requested = std::env::var(FILTER_VAR)
        .or_else(|_| std::env::var("RUST_LOG"))
        .unwrap_or_else(|_| "info".into());
    let bare = !requested.contains(['=', ',']);
    let filter = if bare {
        EnvFilter::new(directives(requested.trim()))
    } else {
        EnvFilter::new(requested)
    };
    let traces = traces().map_err(|e| format!("{ENDPOINT_VAR}: {e}"))?;
    let export = traces
        .as_ref()
        .map(|provider| tracing_opentelemetry::layer().with_tracer(provider.tracer("glossql")));
    let registry = tracing_subscriber::registry().with(filter).with(export);
    let lines = tracing_subscriber::fmt::layer().with_span_events(FmtSpan::CLOSE);
    if std::io::stdout().is_terminal() {
        registry.with(lines.with_target(false)).init();
    } else {
        registry.with(lines.json()).init();
    }
    if traces.is_some() {
        tracing::info!(
            endpoint = %std::env::var(ENDPOINT_VAR).unwrap_or_default(),
            "exporting traces"
        );
    }
    Ok(Telemetry { traces })
}

/// The export pipeline when [`ENDPOINT_VAR`] is set: the OTLP/HTTP
/// exporter with the SDK's own client and environment, batched, under
/// this service's name. The W3C trace context becomes the propagator,
/// so a `traceparent` a client sends is read at the door.
fn traces() -> Result<Option<SdkTracerProvider>, opentelemetry_otlp::ExporterBuildError> {
    if std::env::var_os(ENDPOINT_VAR).is_none() {
        return Ok(None);
    }
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .build()?;
    let resource = Resource::builder()
        .with_service_name("glossql")
        .with_attribute(KeyValue::new("service.version", env!("CARGO_PKG_VERSION")))
        .build();
    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());
    Ok(Some(
        SdkTracerProvider::builder()
            .with_resource(resource)
            .with_batch_exporter(exporter)
            .build(),
    ))
}

/// The span every door opens for a request — tower-http's `TraceLayer`
/// calls this. Method and path, never the query: the browser login's
/// callback carries its code there. A `traceparent` the client sent
/// makes this span a child of the client's; without the export layer
/// there is no parent to take, and that is not an error.
pub fn request_span<B>(request: &Request<B>) -> Span {
    let method = request.method();
    let path = request.uri().path();
    let span = tracing::info_span!(
        "request",
        otel.name = %format!("{method} {path}"),
        otel.kind = "server",
        method = %method,
        path = %path,
        status = tracing::field::Empty,
    );
    let parent = opentelemetry::global::get_text_map_propagator(|propagator| {
        propagator.extract(&HeaderExtractor(request.headers()))
    });
    let _ = span.set_parent(parent);
    span
}

/// The response's status onto the request's span; the span's close
/// carries the timing.
pub fn request_done<B>(response: &Response<B>, _latency: Duration, span: &Span) {
    span.record("status", response.status().as_u16());
}
