//! `next` — where the record stands toward each goal, and the one act
//! that moves it, as a statement to send.
//!
//! The goal is the agent's and the human's, never stored: it rides the
//! read as an argument, `next(surface => 'app')`, or the read serves
//! every surface. A surface is a goal a workspace is extended toward —
//! structure, metrics, slices, bands, checks, app, rulings — and its
//! route is a list of steps in `window.json`. A step is a condition
//! over a shipped read (a key: some row matches every predicate, or
//! none does; `"null"` matches an absent field, `{"not": v}` anything
//! but `v`, a value may name a slot the record fills without a row —
//! `"{dataset}"`, `"{red_metric}"` — and a list field matches when one
//! member does) and what holds when it does — a step may also `need`
//! a slot the door can fill, and then it holds on the first matching
//! row the door can fill it for: the goal is blocked and why, or one
//! act with its statement, or the goal is done. The first step whose
//! condition holds decides. The order of a route is the goal's
//! preconditions and nothing more; an act that is not a precondition
//! of the goal is not a step.
//!
//! The statement is the imperative part. The door fills it from the
//! record — the dataset, the metric, the table its value comes from,
//! the standing body, the columns a verdict admits and the frame does
//! not serve, the metric and period a red band names — and the agent
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

use std::collections::{BTreeMap, HashMap};
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

/// The routes, and the vocabulary the localizer names an act by: the
/// statement kinds, the doors and reads, the acts the routes hand.
#[derive(Deserialize)]
pub struct Graph {
    pub nodes: Vec<String>,
    #[serde(default)]
    pub surfaces: Vec<Surface>,
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
    /// Slots the door must fill non-empty for the step to hold — a
    /// wider frame needs a column to add.
    #[serde(default)]
    pub needs: Vec<String>,
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
    "other_axes",
    "unjudged",
    "relevance_form",
    "unjudged_table",
    "wanted_table",
    "metrics",
    "n",
    "app_form",
    "red_metric",
    "red_period",
    "red_body",
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

/// The first row matching every predicate of a key; `"{dataset}"` as
/// a value is the bound dataset.
pub fn matching<'a>(key: &Key, rows: &'a [Value], dataset: &str) -> Option<&'a Value> {
    matching_all(key, rows, dataset).into_iter().next()
}

/// Every row matching every predicate of a key, in the read's order.
pub fn matching_all<'a>(key: &Key, rows: &'a [Value], dataset: &str) -> Vec<&'a Value> {
    rows.iter()
        .filter(|row| {
            key.conditions
                .iter()
                .all(|(field, want)| predicate(row.get(field), want, dataset))
        })
        .collect()
}

/// Whether a key holds on the rows its read served.
pub fn holds(key: &Key, rows: &[Value], dataset: &str) -> bool {
    let any = matching(key, rows, dataset).is_some();
    if key.none { !any } else { any }
}

fn predicate(have: Option<&Value>, want: &Value, dataset: &str) -> bool {
    // `{"not": v}`: anything but `v`, an absent field included.
    if let Some(not) = want.get("not") {
        return !predicate(have, not, dataset);
    }
    match want.as_str() {
        Some("{dataset}") => have.and_then(Value::as_str) == Some(dataset),
        Some("null") => have.is_none_or(Value::is_null),
        Some("empty") => is_empty(have),
        Some("nonempty") => !is_empty(have),
        Some("nonzero") => have.and_then(Value::as_f64).is_some_and(|n| n != 0.0),
        _ => match have {
            None => false,
            // A list field matches when one of its members does.
            Some(Value::Array(items)) if !want.is_array() => items.iter().any(|h| same(h, want)),
            Some(h) => same(h, want),
        },
    }
}

fn same(have: &Value, want: &Value) -> bool {
    have == want
        || matches!((have.as_str(), want.as_str()), (Some(a), Some(b)) if a.eq_ignore_ascii_case(b))
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
        candidates: HashMap::new(),
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
    candidates: HashMap<String, Option<Candidates>>,
}

/// What a wider frame over a metric's table could add, from the
/// record: the unserved columns a `dimension` gloss or an applicable
/// `dimension_relevance` verdict admits as an axis; the unserved
/// columns glossed `role` dimension that no verdict judged; and
/// whether nobody judged any unserved column at all.
#[derive(Clone, Default)]
struct Candidates {
    admitted: Vec<String>,
    unjudged: Vec<String>,
    unserved: Vec<String>,
    unjudged_table: bool,
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

    /// The first step whose condition holds decides — on the first
    /// matching row the door can fill its needs for.
    async fn resolve(&mut self, surface: &Surface) -> Result<Next, SessionError> {
        let dataset = self.dataset.clone();
        'steps: for step in &surface.route {
            // The rows the step may decide on: every match of its first
            // key while every key holds; one empty row without a
            // condition.
            let mut rows: Vec<Option<Value>> = vec![None];
            if let Some(keys) = &step.when {
                let mut first: Vec<Value> = Vec::new();
                for (i, key) in keys.each().enumerate() {
                    let key = Box::pin(self.bound(key)).await;
                    let Ok(read) = self.read(&key.read).await else {
                        continue 'steps;
                    };
                    let matches = matching_all(&key, read, &dataset);
                    if key.none == !matches.is_empty() {
                        continue 'steps;
                    }
                    if i == 0 {
                        first = matches.into_iter().cloned().collect();
                    }
                }
                if !first.is_empty() {
                    rows = first.into_iter().map(Some).collect();
                }
            }
            for row in rows {
                let mut needed = true;
                for need in &step.needs {
                    if self
                        .slot(need, row.as_ref())
                        .await
                        .is_none_or(|v| v.trim().is_empty())
                    {
                        needed = false;
                        break;
                    }
                }
                if needed {
                    return Ok(Box::pin(self.decide(surface, step, row.as_ref())).await);
                }
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

    /// A key with its slot-naming values filled — `"{dataset}"`,
    /// `"{red_metric}"` — from the record, without a row.
    async fn bound(&mut self, key: &Key) -> Key {
        let mut out = key.clone();
        for value in out.conditions.values_mut() {
            if let Some(text) = value.as_str()
                && text.contains('{')
            {
                *value = Value::String(Box::pin(self.fill(text, None)).await);
            }
        }
        out
    }

    /// What a step says once it holds on a row.
    async fn decide(&mut self, surface: &Surface, step: &Step, row: Option<&Value>) -> Next {
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
            next.why = self.fill(reason, row).await;
            return next;
        }
        if let Some(done) = &step.done {
            next.why = self.fill(done, row).await;
            return next;
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
                    *slot = self.fill(template, row).await;
                }
            }
        }
        next
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
            "columns" => Some(self.candidates(&metric?).await?.admitted.join(", ")),
            "other_axes" => Some(Box::pin(self.other_axes(&metric?)).await),
            "unjudged" => Some(self.candidates(&metric?).await?.unjudged.join(", ")),
            // The detector over each column glossed a dimension and not
            // judged; over every unserved column where nobody judged one.
            "relevance_form" => {
                let metric = metric?;
                let table = self.table_of(&metric).await?;
                let c = self.candidates(&metric).await?;
                let columns = if !c.unjudged.is_empty() {
                    c.unjudged
                } else if c.unjudged_table {
                    c.unserved
                } else {
                    Vec::new()
                };
                Some(
                    columns
                        .iter()
                        .map(|c| {
                            format!(
                                "SELECT dimension_relevance() FROM {}.{table}.{c}",
                                self.dataset
                            )
                        })
                        .collect::<Vec<_>>()
                        .join(";\n"),
                )
            }
            "unjudged_table" => {
                let metric = metric?;
                if self.candidates(&metric).await?.unjudged_table {
                    self.table_of(&metric).await
                } else {
                    Some(String::new())
                }
            }
            "red_metric" => self.worst_point().await.map(|(metric, _)| metric),
            "red_period" => self.worst_point().await.map(|(_, period)| period),
            "red_body" => {
                let (metric, period) = self.worst_point().await?;
                let mut body = self.body_value(&metric).await?;
                let stub = serde_json::json!({
                    "dimension": "definition",
                    "key": format!("band-{period}"),
                    "assumption": format!("<a shift at {period}, in the business's words — or the defect and its fix>"),
                    "basis": "<what it rests on>",
                    "confidence": 0.7
                });
                match body.get_mut("assumptions").and_then(Value::as_array_mut) {
                    Some(list) => list.push(stub),
                    None => {
                        body["assumptions"] = Value::Array(vec![stub]);
                    }
                }
                serde_json::to_string_pretty(&body).ok()
            }
            "wanted_table" => {
                let over = row?.get("wanted_over")?.as_array()?.first()?.as_str()?;
                Some(table_part(over).to_string())
            }
            "metrics" => Some(self.applicable().await.join(", ")),
            "n" => Some(self.applicable().await.len().to_string()),
            "app_form" => Some(Box::pin(self.app_form()).await),
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
        let body = self.body_value(metric).await?;
        serde_json::to_string_pretty(&body).ok()
    }

    /// The standing grounding body of a metric, as JSON.
    async fn body_value(&mut self, metric: &str) -> Option<Value> {
        let sql = format!(
            "SELECT value FROM GLOSSARY({}::{}) WHERE state = 'current'",
            self.dataset, metric
        );
        let rows = self.sql(&sql).await.ok()?;
        let raw = rows.first()?.get("value")?.as_str()?;
        serde_json::from_str::<Value>(raw).ok()
    }

    /// The other applicable metrics without an axis whose table has an
    /// admitted column to serve, each with its columns — so the line
    /// moves when any of them takes one, and holds on none.
    async fn other_axes(&mut self, metric: &str) -> String {
        let Ok(rows) = self.read("metric_axes").await else {
            return String::new();
        };
        let others: Vec<String> = rows
            .iter()
            .filter(|r| r.get("applicable") == Some(&Value::Bool(true)))
            .filter(|r| is_empty(r.get("dims")))
            .filter_map(|r| r.get("metric")?.as_str().map(str::to_string))
            .filter(|m| m != metric)
            .collect();
        let mut out = Vec::new();
        for other in others {
            if let Some(c) = self.candidates(&other).await
                && !c.admitted.is_empty()
            {
                out.push(format!("{other}: {}", c.admitted.join(", ")));
            }
        }
        if out.is_empty() {
            String::new()
        } else {
            format!(" — also {}", out.join("; "))
        }
    }

    /// The wider-frame candidates of a metric, computed once per call.
    async fn candidates(&mut self, metric: &str) -> Option<Candidates> {
        if let Some(c) = self.candidates.get(metric) {
            return c.clone();
        }
        let built = Box::pin(self.build_candidates(metric)).await;
        self.candidates.insert(metric.to_string(), built.clone());
        built
    }

    /// The unserved columns of the metric's table, by what admits
    /// them: a `dimension` gloss (`primary` before `supporting`;
    /// `none` closes the column), then an applicable
    /// `dimension_relevance` verdict by relevance — the cube's own
    /// admission rule, read from the same verdicts and glosses. The
    /// judgment is the detector's and the gloss's; the door lists.
    async fn build_candidates(&mut self, metric: &str) -> Option<Candidates> {
        let table = self.table_of(metric).await?;
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
        let unserved: Vec<String> = columns
            .iter()
            .filter_map(|r| r.get("column_name")?.as_str().map(str::to_string))
            .filter(|c| !served.contains(c))
            .collect();
        let rctx = Box::pin(self.shared.read_context()).await.ok()?;
        let relevance = crate::cube::judged_bodies(&rctx, &self.dataset, "dimension_relevance");
        let dimension = Box::pin(crate::search::current_fact_values(
            &rctx,
            &self.dataset,
            "dimension",
        ))
        .await
        .ok()?;
        let role = Box::pin(crate::search::current_fact_values(
            &rctx,
            &self.dataset,
            "role",
        ))
        .await
        .ok()?;
        let subject = |c: &str| format!("{table}.{c}");
        let word = |glosses: &HashMap<String, (Value, u8)>, c: &str| {
            glosses
                .get(&subject(c))
                .and_then(|(v, _)| v["value"].as_str())
                .map(str::to_string)
        };
        let judged =
            |c: &str| relevance.contains_key(&subject(c)) || dimension.contains_key(&subject(c));
        let mut admitted: Vec<(u8, f64, String)> = Vec::new();
        for c in &unserved {
            match word(&dimension, c).as_deref() {
                Some("primary") => admitted.push((0, 0.0, c.clone())),
                Some("supporting") => admitted.push((1, 0.0, c.clone())),
                Some(_) => {}
                None => {
                    if let Some(v) = relevance.get(&subject(c))
                        && v.body["applicable"] == Value::Bool(true)
                    {
                        let score = v.body["relevance"].as_f64().unwrap_or(0.0);
                        admitted.push((2, -score, c.clone()));
                    }
                }
            }
        }
        admitted.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.total_cmp(&b.1)));
        let unjudged = unserved
            .iter()
            .filter(|c| word(&role, c).as_deref() == Some("dimension") && !judged(c))
            .cloned()
            .collect();
        let unjudged_table = !unserved.is_empty() && unserved.iter().all(|c| !judged(c));
        Some(Candidates {
            admitted: admitted.into_iter().map(|(_, _, c)| c).collect(),
            unjudged,
            unserved,
            unjudged_table,
        })
    }

    /// The metric whose newest complete walked point sits furthest
    /// from its corridor's median, and that point's period — the
    /// point the bands detector scored. None where no walk stands.
    async fn worst_point(&mut self) -> Option<(String, String)> {
        let rows = self.read("band_points").await.ok()?;
        let mut by_metric: BTreeMap<&str, Vec<&Value>> = BTreeMap::new();
        for row in rows {
            if let (Some(metric), Some(_)) = (
                row.get("metric").and_then(Value::as_str),
                row.get("point_seq").and_then(Value::as_i64),
            ) {
                by_metric.entry(metric).or_default().push(row);
            }
        }
        let mut worst: Option<(f64, String, String)> = None;
        for (metric, mut points) in by_metric {
            points.sort_by_key(|p| p.get("point_seq").and_then(Value::as_i64));
            let partial = |p: &&Value| p.get("partial") == Some(&Value::Bool(true));
            let newest = match points.last() {
                Some(last) if partial(last) => points.iter().rev().nth(1).copied(),
                other => other.copied(),
            };
            let Some(point) = newest else { continue };
            let Some(displacement) = point.get("displacement").and_then(Value::as_f64) else {
                continue;
            };
            if worst.as_ref().is_none_or(|(d, _, _)| displacement > *d) {
                let period = point.get("period").and_then(Value::as_str).unwrap_or("");
                worst = Some((displacement, metric.to_string(), period.to_string()));
            }
        }
        worst.map(|(_, metric, period)| (metric, period))
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

/// The `{slot}` names a template uses — a name in braces, inside a
/// JSON body or not; a brace opening anything else is the body's own.
pub fn slots_in(template: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut rest = template;
    while let Some(start) = rest.find('{') {
        let after = &rest[start + 1..];
        let end = after
            .find(|c: char| !(c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '[' | ']')))
            .unwrap_or(after.len());
        if end > 0 && after[end..].starts_with('}') {
            let name = &after[..end];
            if !out.contains(&name.to_string()) {
                out.push(name.to_string());
            }
            rest = &after[end + 1..];
        } else {
            rest = after;
        }
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
