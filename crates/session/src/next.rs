//! `next` — where the record stands toward each goal, and the one act
//! that moves it, as a statement to send.
//!
//! The goal is the agent's and the human's, never stored: it rides the
//! read as an argument, `next(surface => 'app')`, or the read serves
//! every surface. A surface is a goal a workspace is extended toward —
//! structure, metrics, slices, bands, checks, app, rulings — and its
//! route is a list of steps in `window.json`. A step is a condition
//! over a shipped read (a key, as the mechanical edges carry: some row
//! matches every predicate, or none does) and what holds when it does:
//! the goal is blocked and why, or one act with its statement, or the
//! goal is done. The first step whose condition holds decides. The
//! order of a route is the goal's preconditions and nothing more; an
//! act that is not a precondition of the goal is not a step.
//!
//! The statement is the imperative part. The door fills it from the
//! record — the dataset, the metric, the table its value comes from,
//! the standing body, the columns not yet served — and the agent
//! edits it and sends it, or does not. Hypermedia in the REST sense:
//! the representation carries the links and the forms, and the client
//! holds the goal.
//!
//! Reads run through the session's own pipeline (`whatif::build_plan`),
//! one per relation per call, only for the steps reached. A read that
//! refuses leaves its step undecided, never "done". Nothing here is
//! stored, ordered globally, or learned: the routes are data the eval
//! harness edits under its gate, and the suite holds every key to a
//! read and a column it serves.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use datafusion::arrow::array::{ArrayRef, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{
    Expr as SQLExpr, FunctionArg, FunctionArgExpr, Value as SQLValue,
};
use serde::Deserialize;
use serde_json::Value;

use crate::reads::Shared;
use crate::session::SessionError;

/// The graph, verbatim — `doc://window.json` serves the same bytes.
pub const GRAPH_JSON: &str = include_str!("../../../window.json");

#[derive(Deserialize)]
pub struct Graph {
    pub nodes: Vec<String>,
    pub edges: Vec<Edge>,
    #[serde(default)]
    pub surfaces: Vec<Surface>,
}

#[derive(Deserialize, Clone)]
pub struct Edge {
    pub from: String,
    pub to: String,
    #[serde(default)]
    pub relation: String,
    pub when: When,
    #[serde(default)]
    pub guidance: String,
    #[serde(default)]
    pub pitfalls: String,
    #[serde(default)]
    pub source: String,
}

#[derive(Deserialize, Clone, Default)]
pub struct When {
    #[serde(default)]
    pub text: String,
    #[serde(default)]
    pub key: Option<Keys>,
}

/// A condition over one read: some row matches every `where`
/// predicate — a value, `"empty"`, `"nonempty"`, `"nonzero"` — or no
/// row does when `none` is set.
#[derive(Deserialize, Clone)]
pub struct Key {
    pub read: String,
    #[serde(rename = "where", default)]
    pub conditions: std::collections::BTreeMap<String, Value>,
    #[serde(default)]
    pub none: bool,
}

/// One key, or several that must all hold.
#[derive(Deserialize, Clone)]
#[serde(untagged)]
pub enum Keys {
    One(Key),
    All(Vec<Key>),
}

impl Keys {
    pub fn each(&self) -> impl Iterator<Item = &Key> {
        match self {
            Keys::One(key) => std::slice::from_ref(key).iter(),
            Keys::All(keys) => keys.iter(),
        }
    }
}

/// A goal and its route.
#[derive(Deserialize)]
pub struct Surface {
    pub name: String,
    pub goal: String,
    pub route: Vec<Step>,
}

/// One step of a route: when its condition holds, the goal is blocked
/// (`blocked`), done (`done`), or one act is next (`act`, with its
/// `say`, `why`, `form` and `then`). A step without a condition holds.
#[derive(Deserialize)]
pub struct Step {
    #[serde(default)]
    pub when: Option<Keys>,
    #[serde(default)]
    pub blocked: Option<String>,
    #[serde(default)]
    pub done: Option<String>,
    #[serde(default)]
    pub act: Option<String>,
    #[serde(default)]
    pub say: Option<String>,
    #[serde(default)]
    pub why: Option<String>,
    #[serde(default)]
    pub form: Option<String>,
    #[serde(default)]
    pub then: Option<String>,
}

/// The embedded graph, parsed once.
pub fn graph() -> &'static Graph {
    static GRAPH: OnceLock<Graph> = OnceLock::new();
    GRAPH.get_or_init(|| {
        serde_json::from_str(GRAPH_JSON).expect("window.json parses; the suite holds it")
    })
}

impl Graph {
    /// The node spelled as the graph spells it, matched without case —
    /// `attest` in SQL is the graph's `ATTEST`.
    pub fn node_named(&self, name: &str) -> Option<&str> {
        self.nodes
            .iter()
            .map(String::as_str)
            .find(|n| n.eq_ignore_ascii_case(name))
    }

    /// The edges out of a node, in file order.
    pub fn out(&self, node: &str) -> Vec<&Edge> {
        self.edges.iter().filter(|e| e.from == node).collect()
    }

    pub fn surface(&self, name: &str) -> Option<&Surface> {
        self.surfaces.iter().find(|s| s.name == name)
    }
}

/// The slots a form may name, beside `{dataset}` and `{row.<field>}` /
/// `{row.<field>[0]}`: what the door derives from the record for the
/// matching row. The suite refuses a form naming any other.
pub const SLOTS: &[&str] = &[
    "dataset",
    "table",
    "body",
    "columns",
    "wanted_table",
    "metrics",
    "n",
    "app_form",
];

/// The SQL that reads a key's relation on the bound dataset: a door by
/// its call, a store relation narrowed to the dataset where it carries
/// one, a shipped read by its name.
pub fn read_sql(read: &str, dataset: &str) -> String {
    match read {
        "GLOSSARY" => "SELECT * FROM GLOSSARY()".to_string(),
        "ATTEST" => format!("SELECT * FROM ATTEST({dataset})"),
        name => {
            let lower = name.to_ascii_lowercase();
            let call = crate::reads::DOORS
                .iter()
                .any(|(door, syntax)| *door == lower && *syntax == format!("{door}()"));
            if call {
                format!("SELECT * FROM {lower}()")
            } else if glossql_glossary::relation_columns(&lower)
                .is_some_and(|columns| columns.contains(&"dataset"))
            {
                format!(
                    "SELECT * FROM {lower} WHERE dataset = '{}'",
                    dataset.replace('\'', "''")
                )
            } else {
                format!("SELECT * FROM {lower}")
            }
        }
    }
}

/// The first row matching every predicate of a key.
pub fn matching<'a>(key: &Key, rows: &'a [Value]) -> Option<&'a Value> {
    rows.iter().find(|row| {
        key.conditions
            .iter()
            .all(|(field, want)| predicate(row.get(field), want))
    })
}

/// Whether a key holds on the rows its read served.
pub fn holds(key: &Key, rows: &[Value]) -> bool {
    let any = matching(key, rows).is_some();
    if key.none { !any } else { any }
}

fn predicate(have: Option<&Value>, want: &Value) -> bool {
    match want.as_str() {
        Some("empty") => is_empty(have),
        Some("nonempty") => !is_empty(have),
        Some("nonzero") => have.and_then(Value::as_f64).is_some_and(|n| n != 0.0),
        _ => match have {
            None => false,
            Some(h) => {
                h == want
                    || match (h.as_str(), want.as_str()) {
                        (Some(a), Some(b)) => a.eq_ignore_ascii_case(b),
                        _ => false,
                    }
            }
        },
    }
}

fn is_empty(have: Option<&Value>) -> bool {
    match have {
        None | Some(Value::Null) => true,
        Some(Value::String(s)) => s.is_empty() || s == "[]",
        Some(Value::Array(a)) => a.is_empty(),
        Some(Value::Object(o)) => o.is_empty(),
        _ => false,
    }
}

/// One surface's answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Next {
    pub surface: String,
    /// `next`, `blocked` or `done`.
    pub state: &'static str,
    pub act: String,
    pub say: String,
    pub why: String,
    pub statement: String,
    pub then: String,
}

/// `next()` / `next(surface => '<surface>')` as rows.
pub(crate) async fn rows(
    shared: &Arc<Shared>,
    surface: Option<&str>,
) -> Result<RecordBatch, SessionError> {
    let answers = answer(shared, surface).await?;
    let column = |pick: fn(&Next) -> String| -> ArrayRef {
        Arc::new(StringArray::from(
            answers.iter().map(pick).collect::<Vec<String>>(),
        ))
    };
    let schema = Arc::new(Schema::new(
        ["surface", "state", "act", "say", "why", "statement", "then"]
            .into_iter()
            .map(|name| Field::new(name, DataType::Utf8, false))
            .collect::<Vec<_>>(),
    ));
    RecordBatch::try_new(
        schema,
        vec![
            column(|n| n.surface.clone()),
            column(|n| n.state.to_string()),
            column(|n| n.act.clone()),
            column(|n| n.say.clone()),
            column(|n| n.why.clone()),
            column(|n| n.statement.clone()),
            column(|n| n.then.clone()),
        ],
    )
    .map_err(|e| SessionError::Runtime(e.to_string()))
}

/// Every surface's answer, or one surface's, on the bound dataset.
pub(crate) async fn answer(
    shared: &Arc<Shared>,
    surface: Option<&str>,
) -> Result<Vec<Next>, SessionError> {
    let dataset = shared
        .dataset
        .read()
        .expect("state lock")
        .clone()
        .ok_or(SessionError::NoDataset)?;
    let graph = graph();
    let surfaces: Vec<&Surface> = match surface {
        Some(name) => vec![graph.surface(name).ok_or_else(|| {
            SessionError::BadSubject(format!(
                "next(surface => '{name}'): no such surface — one of {}",
                graph
                    .surfaces
                    .iter()
                    .map(|s| s.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ))
        })?],
        None => graph.surfaces.iter().collect(),
    };
    let mut record = Record {
        shared,
        ctx: shared.session_ctx(),
        dataset,
        reads: HashMap::new(),
    };
    let mut out = Vec::with_capacity(surfaces.len());
    for surface in surfaces {
        out.push(record.resolve(surface).await?);
    }
    Ok(out)
}

/// The `surface => '<name>'` argument of `next(...)`, if any.
pub(crate) fn surface_arg(args: &[FunctionArg]) -> Result<Option<String>, SessionError> {
    let refuse = || {
        SessionError::BadSubject(
            "next takes at most one argument: next() or next(surface => '<surface>')".into(),
        )
    };
    match args {
        [] => Ok(None),
        [
            FunctionArg::ExprNamed {
                name: SQLExpr::Identifier(n),
                arg: FunctionArgExpr::Expr(v),
                ..
            },
        ]
        | [
            FunctionArg::Named {
                name: n,
                arg: FunctionArgExpr::Expr(v),
                ..
            },
        ] if n.value.eq_ignore_ascii_case("surface") => match v {
            SQLExpr::Value(v) => match &v.value {
                SQLValue::SingleQuotedString(s) => Ok(Some(s.clone())),
                _ => Err(refuse()),
            },
            _ => Err(refuse()),
        },
        _ => Err(refuse()),
    }
}

/// The record as the routes read it: one read per relation per call,
/// through the session's own pipeline, a refusal remembered so a step
/// on it stays undecided.
struct Record<'a> {
    shared: &'a Arc<Shared>,
    ctx: SessionContext,
    dataset: String,
    reads: HashMap<String, Result<Vec<Value>, String>>,
}

impl Record<'_> {
    async fn sql(&self, sql: &str) -> Result<Vec<Value>, String> {
        let plan = Box::pin(crate::whatif::build_plan(self.shared, &self.ctx, sql))
            .await
            .map_err(|e| e.to_string())?;
        let batches = self
            .ctx
            .execute_logical_plan(plan)
            .await
            .map_err(|e| e.to_string())?
            .collect()
            .await
            .map_err(|e| e.to_string())?;
        rows_json(&batches)
    }

    async fn read(&mut self, name: &str) -> Result<&[Value], String> {
        if !self.reads.contains_key(name) {
            let rows = self.sql(&read_sql(name, &self.dataset)).await;
            if let Err(e) = &rows {
                tracing::debug!(read = name, error = %e, "next: a read refused");
            }
            self.reads.insert(name.to_string(), rows);
        }
        match &self.reads[name] {
            Ok(rows) => Ok(rows.as_slice()),
            Err(e) => Err(e.clone()),
        }
    }

    /// The first step whose condition holds decides.
    async fn resolve(&mut self, surface: &Surface) -> Result<Next, SessionError> {
        for step in &surface.route {
            let row = match &step.when {
                None => None,
                Some(keys) => {
                    let mut first: Option<Value> = None;
                    let mut all = true;
                    for (i, key) in keys.each().enumerate() {
                        let Ok(rows) = self.read(&key.read).await else {
                            all = false;
                            break;
                        };
                        let m = matching(key, rows);
                        if if key.none { m.is_some() } else { m.is_none() } {
                            all = false;
                            break;
                        }
                        if i == 0 {
                            first = m.cloned();
                        }
                    }
                    if !all {
                        continue;
                    }
                    first
                }
            };
            let mut next = Next {
                surface: surface.name.clone(),
                state: "done",
                act: String::new(),
                say: String::new(),
                why: String::new(),
                statement: String::new(),
                then: String::new(),
            };
            if let Some(reason) = &step.blocked {
                next.state = "blocked";
                next.why = self.fill(reason, row.as_ref()).await;
                return Ok(next);
            }
            if let Some(done) = &step.done {
                next.why = self.fill(done, row.as_ref()).await;
                return Ok(next);
            }
            if let Some(act) = &step.act {
                next.state = "next";
                next.act = act.clone();
                for (slot, template) in [
                    (&mut next.say, &step.say),
                    (&mut next.why, &step.why),
                    (&mut next.statement, &step.form),
                    (&mut next.then, &step.then),
                ] {
                    if let Some(template) = template {
                        *slot = self.fill(template, row.as_ref()).await;
                    }
                }
                return Ok(next);
            }
        }
        Ok(Next {
            surface: surface.name.clone(),
            state: "done",
            act: String::new(),
            say: String::new(),
            why: "the route has no step left".into(),
            statement: String::new(),
            then: String::new(),
        })
    }

    /// A template with its `{slots}` filled from the matching row and
    /// the record. A slot the record cannot fill stays as `<slot>`.
    async fn fill(&mut self, template: &str, row: Option<&Value>) -> String {
        let mut out = template.to_string();
        for name in slots_in(template) {
            let value = self.slot(&name, row).await;
            out = out.replace(
                &format!("{{{name}}}"),
                &value.unwrap_or(format!("<{name}>")),
            );
        }
        out
    }

    async fn slot(&mut self, name: &str, row: Option<&Value>) -> Option<String> {
        if name == "dataset" {
            return Some(self.dataset.clone());
        }
        if let Some(field) = name.strip_prefix("row.") {
            let (field, first) = match field.strip_suffix("[0]") {
                Some(f) => (f, true),
                None => (field, false),
            };
            let v = row?.get(field)?;
            return Some(match v {
                Value::Array(a) if first => a.first().map(text).unwrap_or_default(),
                Value::Array(a) => a.iter().map(text).collect::<Vec<_>>().join(", "),
                other => text(other),
            });
        }
        let metric = row
            .and_then(|r| r.get("metric"))
            .and_then(Value::as_str)
            .map(str::to_string);
        match name {
            "table" => self.table_of(metric.as_deref()?).await,
            "body" => self.body_of(metric.as_deref()?).await,
            "columns" => {
                let metric = metric?;
                let table = self.table_of(&metric).await?;
                self.unserved_columns(&metric, &table).await
            }
            "wanted_table" => {
                let over = row?.get("wanted_over")?.as_array()?.first()?.as_str()?;
                Some(table_part(over).to_string())
            }
            "metrics" => Some(self.applicable().await.join(", ")),
            "n" => Some(self.applicable().await.len().to_string()),
            "app_form" => Some(self.app_form().await),
            _ => None,
        }
    }

    /// The applicable metrics, by name.
    async fn applicable(&mut self) -> Vec<String> {
        let Ok(rows) = self.read("metric_axes").await else {
            return Vec::new();
        };
        rows.iter()
            .filter(|r| r.get("applicable") == Some(&Value::Bool(true)))
            .filter_map(|r| r.get("metric")?.as_str().map(str::to_string))
            .collect()
    }

    /// The table a metric's value comes from — its `value` field's
    /// source, else the first source it names.
    async fn table_of(&mut self, metric: &str) -> Option<String> {
        let rows = self.read("metric_sources").await.ok()?;
        let mine = rows
            .iter()
            .filter(|r| r.get("metric").and_then(Value::as_str) == Some(metric));
        let source = mine
            .clone()
            .find(|r| r.get("field").and_then(Value::as_str) == Some("value"))
            .or_else(|| mine.clone().next())
            .and_then(|r| r.get("source")?.as_str().map(str::to_string))?;
        Some(table_part(&source).to_string())
    }

    /// The standing grounding body of a metric, pretty-printed.
    async fn body_of(&mut self, metric: &str) -> Option<String> {
        let sql = format!(
            "SELECT value FROM GLOSSARY({}::{}) WHERE state = 'current'",
            self.dataset, metric
        );
        let rows = self.sql(&sql).await.ok()?;
        let raw = rows.first()?.get("value")?.as_str()?;
        Some(
            serde_json::from_str::<Value>(raw)
                .ok()
                .and_then(|v| serde_json::to_string_pretty(&v).ok())
                .unwrap_or_else(|| raw.to_string()),
        )
    }

    /// The columns of a table the metric does not serve — what a wider
    /// frame could add.
    async fn unserved_columns(&mut self, metric: &str, table: &str) -> Option<String> {
        let sql = format!(
            "SELECT column_name FROM information_schema.columns \
             WHERE table_schema = '{}' AND table_name = '{}' ORDER BY ordinal_position",
            self.dataset, table
        );
        let columns = self.sql(&sql).await.ok()?;
        let served: Vec<String> = self
            .read("metric_sources")
            .await
            .ok()?
            .iter()
            .filter(|r| r.get("metric").and_then(Value::as_str) == Some(metric))
            .filter_map(|r| {
                r.get("source")?
                    .as_str()
                    .map(|s| column_part(s).to_string())
            })
            .collect();
        let out: Vec<String> = columns
            .iter()
            .filter_map(|r| r.get("column_name")?.as_str().map(str::to_string))
            .filter(|c| !served.contains(c))
            .collect();
        Some(out.join(", "))
    }

    /// A first app over the applicable metrics: the manifest, one
    /// frame over the cube's monthly cells, one spec, one page.
    async fn app_form(&mut self) -> String {
        let metrics = self.applicable().await;
        let title = format!("{} review", self.dataset);
        let html = concat!(
            "{% extends \"shell.html\" %}\n",
            "{% import \"modules/tiles.html\" as tiles %}\n",
            "{% block main %}\n",
            "<div class=\"tiles\">\n",
            "  {{ tiles::chart(frame=\"frames/series\", spec=\"specs/trend.vl.json\", ",
            "title=\"By month\", chip=\"metric_series(grain => 'month')\") }}\n",
            "</div>\n",
            "{% endblock %}\n"
        );
        let spec = serde_json::json!({
            "$schema": "https://vega.github.io/schema/vega-lite/v6.json",
            "data": {"name": "frame"},
            "mark": "line",
            "encoding": {
                "x": {"field": "period", "type": "temporal"},
                "y": {"field": "value", "type": "quantitative"},
                "color": {"field": "metric", "type": "nominal"}
            }
        });
        let members = metrics
            .iter()
            .map(|m| format!("'{m}'"))
            .collect::<Vec<_>>()
            .join(", ");
        let frame = format!(
            "SELECT metric, period, value FROM metric_series(grain => 'month') \
             WHERE dimension = '' AND metric IN ({members}) ORDER BY metric, period"
        );
        format!(
            "GLOSS app ON review AS $${}$$;\n\
             GLOSS app_frame ON review.series AS $${}$$;\n\
             GLOSS app_spec ON review.trend AS $${}$$;\n\
             GLOSS app_page ON review.index AS $${}$$;",
            serde_json::json!({"title": title}),
            serde_json::json!({"sql": frame}),
            serde_json::json!({"spec": spec.to_string()}),
            serde_json::json!({"html": html}),
        )
    }
}

fn text(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// `table` of a `table.column` (or `dataset.table.column`) subject.
fn table_part(subject: &str) -> &str {
    let parts: Vec<&str> = subject.split('.').collect();
    match parts.as_slice() {
        [.., table, _] => table,
        [only] => only,
        [] => subject,
    }
}

fn column_part(subject: &str) -> &str {
    subject.rsplit('.').next().unwrap_or(subject)
}

/// The `{slot}` names a template uses.
pub fn slots_in(template: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut rest = template;
    while let Some(start) = rest.find('{') {
        let after = &rest[start + 1..];
        let Some(end) = after.find('}') else { break };
        let name = &after[..end];
        if !name.is_empty()
            && name
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '[' | ']'))
            && !out.contains(&name.to_string())
        {
            out.push(name.to_string());
        }
        rest = &after[end + 1..];
    }
    out
}

/// Batches as JSON rows, as the wire renders them.
fn rows_json(batches: &[RecordBatch]) -> Result<Vec<Value>, String> {
    if batches.is_empty() {
        return Ok(Vec::new());
    }
    let mut writer = arrow_json::ArrayWriter::new(Vec::new());
    let refs: Vec<&RecordBatch> = batches.iter().collect();
    writer.write_batches(&refs).map_err(|e| e.to_string())?;
    writer.finish().map_err(|e| e.to_string())?;
    let value: Value = serde_json::from_slice(&writer.into_inner()).map_err(|e| e.to_string())?;
    Ok(value.as_array().cloned().unwrap_or_default())
}
