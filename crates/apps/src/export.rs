//! Downloads: a landed table or a served metric's relation as a file.
//! `GET /<dataset>/app/export/<name>.csv` or `.parquet`, where `<name>`
//! is a table or a metric of the dataset — a metric's grounding is a
//! view under its name; `read.<aspect>` names the same view. The file is the
//! relation as it stands at the read, encoded on the way out — the
//! same channel and plan as a frame, the same streaming shape as the
//! Arrow door, nothing stored twice. Parquet keeps the engine's types;
//! CSV is text. What a file cannot answer — a read of the caller's own,
//! a filter — the Arrow door does.
//!
//! The name is an identifier and nothing else: the plan is
//! `SELECT * FROM "<name>"` or `SELECT * FROM read.<aspect>()`, so a
//! name the dataset does not hold is the engine's refusal, with its
//! text, not a guess of ours.

use std::io::Write;
use std::sync::{Arc, Mutex};

use axum::body::{Body, Bytes};
use axum::extract::{Path, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use datafusion::arrow::csv;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::parquet::arrow::ArrowWriter;
use futures::{SinkExt, StreamExt, channel::mpsc};
use glossql_glossary::{Actor, ActorKind};

use crate::AppDoor;

pub async fn export(
    State(door): State<AppDoor>,
    Path((dataset, file)): Path<(String, String)>,
) -> Response {
    if let Some(missing) = crate::missing(&door, &dataset).await {
        return plain(StatusCode::NOT_FOUND, missing);
    }
    let Some((name, ext)) = file.rsplit_once('.') else {
        return plain(
            StatusCode::NOT_FOUND,
            format!("`{file}` names no format — `<name>.csv` or `<name>.parquet`"),
        );
    };
    let format = match ext {
        "csv" => Format::Csv,
        "parquet" => Format::Parquet,
        other => {
            return plain(
                StatusCode::NOT_FOUND,
                format!("no format `{other}` — csv or parquet"),
            );
        }
    };
    let Some(sql) = relation_sql(name) else {
        return plain(
            StatusCode::NOT_FOUND,
            format!("`{name}` is not a table's or a metric's name"),
        );
    };
    let actor = Actor {
        kind: ActorKind::Human,
        id: "app:export".into(),
    };
    let session = match door.plane.channel(actor, Some(&dataset)).await {
        Ok(session) => session,
        Err(e) => return plain(StatusCode::UNPROCESSABLE_ENTITY, e.to_string()),
    };
    match session.query_stream(&sql).await {
        // As the Arrow door: the first poll runs the plan, so a relation
        // the engine refuses is a refusal with its text, not a 200 and
        // an empty file.
        Ok(mut query) => match query.stream.next().await {
            Some(Err(e)) => plain(StatusCode::UNPROCESSABLE_ENTITY, e.to_string()),
            first => stream(query.stream, first, format, &format!("{name}.{ext}")),
        },
        Err(e) => plain(StatusCode::UNPROCESSABLE_ENTITY, e.to_string()),
    }
}

/// The plan for a name: a table quoted as spelled, or a grounding's
/// relation. Identifiers only — nothing else reaches the planner.
fn relation_sql(name: &str) -> Option<String> {
    let ident = |s: &str| {
        !s.is_empty()
            && !s.starts_with(|c: char| c.is_ascii_digit())
            && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
    };
    if let Some(aspect) = name.strip_prefix("read.") {
        return ident(aspect).then(|| format!("SELECT * FROM read.{aspect}()"));
    }
    ident(name).then(|| format!("SELECT * FROM \"{name}\""))
}

#[derive(Clone, Copy)]
enum Format {
    Csv,
    Parquet,
}

impl Format {
    fn mime(self) -> &'static str {
        match self {
            Format::Csv => "text/csv; charset=utf-8",
            Format::Parquet => "application/vnd.apache.parquet",
        }
    }
}

/// The encoders' sink: what they wrote since the last take, shipped
/// as one chunk. Both writers own their sink, so it is shared rather
/// than borrowed back.
#[derive(Clone, Default)]
struct Sink(Arc<Mutex<Vec<u8>>>);

impl Sink {
    fn take(&self) -> Vec<u8> {
        std::mem::take(&mut *self.0.lock().expect("sink lock"))
    }
}

impl Write for Sink {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().expect("sink lock").extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

enum Encoder {
    Csv(csv::Writer<Sink>),
    Parquet(ArrowWriter<Sink>),
}

/// Rows buffered before a row group closes: enough for the reader's
/// statistics to mean something, small enough that memory rides one
/// group rather than the file.
const ROW_GROUP: usize = 1 << 18;

impl Encoder {
    fn new(
        format: Format,
        schema: &datafusion::arrow::datatypes::SchemaRef,
        sink: Sink,
    ) -> Result<Self, String> {
        Ok(match format {
            Format::Csv => {
                let mut writer = csv::WriterBuilder::new().with_header(true).build(sink);
                // The header rides the first write, so an empty
                // relation still says what its columns were.
                writer
                    .write(&RecordBatch::new_empty(Arc::clone(schema)))
                    .map_err(|e| e.to_string())?;
                Encoder::Csv(writer)
            }
            Format::Parquet => Encoder::Parquet(
                ArrowWriter::try_new(sink, Arc::clone(schema), None).map_err(|e| e.to_string())?,
            ),
        })
    }

    fn write(&mut self, batch: &RecordBatch) -> Result<(), String> {
        match self {
            Encoder::Csv(w) => w.write(batch).map_err(|e| e.to_string()),
            Encoder::Parquet(w) => {
                w.write(batch).map_err(|e| e.to_string())?;
                if w.in_progress_rows() >= ROW_GROUP {
                    w.flush().map_err(|e| e.to_string())?;
                }
                Ok(())
            }
        }
    }

    fn finish(self) -> Result<(), String> {
        match self {
            Encoder::Csv(_) => Ok(()),
            Encoder::Parquet(w) => w.close().map(|_| ()).map_err(|e| e.to_string()),
        }
    }
}

/// Encode into a chunked body as batches arrive, `first` being the
/// batch the caller already polled — the Arrow door's shape
/// (crates/serverd/src/query.rs): the channel's capacity is the
/// backpressure, an error after bytes flowed ends the body without its
/// last chunk, a client that hangs up cancels the engine's work.
fn stream(
    mut batches: SendableRecordBatchStream,
    first: Option<Result<RecordBatch, DataFusionError>>,
    format: Format,
    filename: &str,
) -> Response {
    let schema = batches.schema();
    let (mut tx, rx) = mpsc::channel::<Result<Bytes, std::io::Error>>(2);
    tokio::spawn(async move {
        let sink = Sink::default();
        let mut encoder = match Encoder::new(format, &schema, sink.clone()) {
            Ok(encoder) => encoder,
            Err(e) => {
                let _ = tx.send(Err(std::io::Error::other(e))).await;
                return;
            }
        };
        let mut next = first;
        while let Some(batch) = next {
            let written = batch
                .map_err(|e| e.to_string())
                .and_then(|b| encoder.write(&b));
            if let Err(e) = written {
                tracing::warn!(error = %e, "export stream broke after bytes flowed");
                let _ = tx.send(Err(std::io::Error::other(e))).await;
                return;
            }
            if ship(&sink, &mut tx).await.is_err() {
                return;
            }
            next = batches.next().await;
        }
        if let Err(e) = encoder.finish() {
            let _ = tx.send(Err(std::io::Error::other(e))).await;
            return;
        }
        let _ = ship(&sink, &mut tx).await;
    });
    (
        [
            (header::CONTENT_TYPE, format.mime().to_string()),
            (
                header::CONTENT_DISPOSITION,
                format!("attachment; filename=\"{filename}\""),
            ),
            (header::CACHE_CONTROL, "no-store".to_string()),
        ],
        Body::from_stream(rx),
    )
        .into_response()
}

async fn ship(sink: &Sink, tx: &mut mpsc::Sender<Result<Bytes, std::io::Error>>) -> Result<(), ()> {
    let chunk = sink.take();
    if chunk.is_empty() {
        return Ok(());
    }
    tx.send(Ok(Bytes::from(chunk))).await.map_err(|_| ())
}

/// A download that cannot start answers as text: the link was opened
/// in a tab, and the reason is what the tab should show.
fn plain(status: StatusCode, text: String) -> Response {
    (status, [(header::CONTENT_TYPE, "text/plain")], text).into_response()
}
