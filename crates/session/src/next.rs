//! `next` — where the record stands toward each goal, and the one act
//! that moves it, as a statement to send.
//!
//! The goal is the agent's and the human's, never stored: it rides the
//! read as an argument, `next(surface => 'app')`, or the read serves
//! every surface. A surface is a goal a workspace is extended toward —
//! structure, metrics, slices, bands, checks, app, rulings — and its
//! route is a list of steps in `window.json`. A step's condition is a
//! query over the record: the step holds when the query serves a row,
//! and every row it serves is a row the step may decide on, its
//! columns the `{row.<column>}` slots of the step's text. A condition
//! may name a slot as `$<slot>` — the red metric, its period — bound
//! as text before the query plans, and a step may `need` a slot the
//! door fills from the row; it then holds on the first row the door
//! can fill it for. What holds: the goal is blocked and why, or one
//! act with its statement, or the goal is done. The first step whose
//! condition holds decides. The order of a route is the goal's
//! preconditions and nothing more; an act that is not a precondition
//! of the goal is not a step.
//!
//! The statement is the imperative part. The door fills it from the
//! record through the slots — SQL in `window.json`, one read each,
//! `$metric` bound from the step's row and the dataset the session's
//! own through `current_dataset`, planned through the same pipeline
//! as every read; a slot that serves a JSON object is handed
//! pretty-printed. Two the door renders itself: the dataset, and the
//! standing body with the red band's question appended. Hypermedia in
//! the REST sense: the representation carries the links and the forms,
//! and the client holds the goal.
//!
//! Conditions and slots run through the session's own pipeline
//! (`whatif::build_plan`), once per query per call, only for the
//! steps reached. A query that refuses leaves its step undecided,
//! never "done". Nothing here is stored, ordered globally, or learned:
//! the routes are data, and the suite plans every condition and holds
//! every row field a step's text names to a column it serves.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, OnceLock};

use datafusion::arrow::array::{ArrayRef, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::{ParamValues, ScalarValue};
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

/// The routes, the slots their statements are filled from, and the
/// vocabulary the localizer names an act by: the statement kinds, the
/// doors and reads, the acts the routes hand.
#[derive(Deserialize)]
pub struct Graph {
    pub nodes: Vec<String>,
    /// A slot as one read: the first row's first column, as text —
    /// none where no row or a null comes back. `$metric` binds from
    /// the step's row; the dataset is `current_dataset`'s.
    #[serde(default)]
    pub slots: BTreeMap<String, String>,
    #[serde(default)]
    pub surfaces: Vec<Surface>,
}

/// A goal and its route.
#[derive(Deserialize)]
pub struct Surface {
    pub name: String,
    pub goal: String,
    pub route: Vec<Step>,
}

/// One step of a route: when its condition serves a row, the goal is
/// blocked (`blocked`), done (`done`), or one act is next (`act`, with
/// its `say`, `why`, `form` and `then`). A step without a condition
/// holds.
#[derive(Deserialize)]
pub struct Step {
    /// The condition: a query over the record, `$<slot>` bound from
    /// the record first. Absent, the step holds on one empty row.
    #[serde(default)]
    pub when: Option<String>,
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
    /// Slots the door must fill non-empty for the step to hold on a
    /// row — a wider frame needs a column to add.
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

/// The slots the door renders itself, beside the SQL slots of
/// `window.json`, `{dataset}` and `{row.<field>}` / `{row.<field>[0]}`:
/// the dataset, and the standing body with the band's question
/// appended. The suite refuses a form naming any other.
pub const SLOTS: &[&str] = &["dataset", "red_body"];

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
        conditions: HashMap::new(),
        slots: HashMap::new(),
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

/// The record as the routes read it: every condition and slot through
/// the session's own pipeline, once per query per call, a refusal
/// remembered so a step on it stays undecided.
struct Record<'a> {
    shared: &'a Arc<Shared>,
    ctx: SessionContext,
    dataset: String,
    /// The conditions served, by their SQL.
    conditions: HashMap<String, Result<Vec<Value>, String>>,
    /// The SQL slots served, by name and the metric they were bound to.
    slots: HashMap<(String, Option<String>), Option<String>>,
}

impl Record<'_> {
    /// A read with `$name` parameters bound as text before resolution
    /// — a slot's `$metric`, a condition's `$<slot>`; null where the
    /// slot served nothing.
    async fn sql_bound(
        &self,
        sql: &str,
        params: HashMap<String, Option<String>>,
    ) -> Result<Vec<Value>, String> {
        let values: HashMap<String, ScalarValue> = params
            .into_iter()
            .map(|(k, v)| (k, ScalarValue::Utf8(v)))
            .collect();
        let map = match ParamValues::from(values) {
            ParamValues::Map(map) => map,
            _ => HashMap::new(),
        };
        let plan = Box::pin(crate::whatif::build_plan_bound(
            self.shared,
            &self.ctx,
            sql,
            "the slot",
            &map,
        ))
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

    /// A step's condition: the rows its query serves, every `$<slot>`
    /// it names bound from the record first.
    async fn condition(&mut self, sql: &str) -> Result<Vec<Value>, String> {
        if !self.conditions.contains_key(sql) {
            let mut params = HashMap::new();
            for name in placeholders(sql) {
                let value = Box::pin(self.slot(&name, None)).await;
                params.insert(name, value);
            }
            let rows = self.sql_bound(sql, params).await;
            if let Err(e) = &rows {
                tracing::debug!(error = %e, sql, "next: a condition refused");
            }
            self.conditions.insert(sql.to_string(), rows);
        }
        self.conditions[sql].clone()
    }

    /// The first step whose condition holds decides — on the first
    /// row it served that the door can fill its needs for.
    async fn resolve(&mut self, surface: &Surface) -> Result<Next, SessionError> {
        for step in &surface.route {
            // The rows the step may decide on: what its condition
            // served; one empty row without a condition.
            let rows: Vec<Option<Value>> = match &step.when {
                None => vec![None],
                Some(sql) => match self.condition(sql).await {
                    Ok(rows) if rows.is_empty() => continue,
                    Ok(rows) => rows.into_iter().map(Some).collect(),
                    Err(_) => continue,
                },
            };
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

    /// A template with its `{slots}` filled from the row and the
    /// record. A slot the record cannot fill stays as `<slot>`.
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
        if let Some(sql) = graph().slots.get(name) {
            return Box::pin(self.sql_slot(name, sql, metric)).await;
        }
        match name {
            // The standing body of the red band's metric with the
            // question appended as an assumption to fill: the one slot
            // that edits a JSON body, and the door's own for it.
            "red_body" => {
                let metric = Box::pin(self.slot("red_metric", None)).await?;
                let period = Box::pin(self.slot("red_period", None)).await?;
                let row = serde_json::json!({ "metric": metric });
                let body = Box::pin(self.slot("body", Some(&row))).await?;
                let mut body: Value = serde_json::from_str(&body).ok()?;
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
            _ => None,
        }
    }

    /// A slot written as SQL: `$metric` bound from the row where the
    /// read names it, served once per binding; the first row's first
    /// column as text, none where no row or a null comes back, and
    /// none where the read refuses. A JSON object is handed
    /// pretty-printed — the standing body, for the author to edit.
    async fn sql_slot(&mut self, name: &str, sql: &str, metric: Option<String>) -> Option<String> {
        let bound = sql.contains("$metric");
        if bound && metric.is_none() {
            return None;
        }
        let key = (name.to_string(), if bound { metric.clone() } else { None });
        if let Some(served) = self.slots.get(&key) {
            return served.clone();
        }
        let mut params = HashMap::new();
        if let (true, Some(metric)) = (bound, metric) {
            params.insert("metric".to_string(), Some(metric));
        }
        let value = match self.sql_bound(sql, params).await {
            Ok(rows) => rows
                .first()
                .and_then(Value::as_object)
                .and_then(|row| row.values().next())
                .filter(|v| !v.is_null())
                .map(text)
                .map(|v| match serde_json::from_str::<Value>(&v) {
                    Ok(json @ Value::Object(_)) => serde_json::to_string_pretty(&json).unwrap_or(v),
                    _ => v,
                }),
            Err(e) => {
                tracing::debug!(slot = name, error = %e, "next: a slot refused");
                None
            }
        };
        self.slots.insert(key, value.clone());
        value
    }
}

fn text(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
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

/// The `$<slot>` names a condition binds from the record — a name
/// after a dollar sign, `$metric` excluded, which binds from the row.
pub fn placeholders(sql: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut rest = sql;
    while let Some(start) = rest.find('$') {
        let after = &rest[start + 1..];
        let end = after
            .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
            .unwrap_or(after.len());
        let name = &after[..end];
        if !name.is_empty()
            && !name.starts_with(|c: char| c.is_ascii_digit())
            && name != "metric"
            && !out.contains(&name.to_string())
        {
            out.push(name.to_string());
        }
        rest = &after[end..];
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
