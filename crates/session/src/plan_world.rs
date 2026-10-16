//! The plan world (fixture 27): a plan is its own QUERY aspect,
//! declared `x-kind: plan` with `x-plan-of` naming the metric it plans,
//! grounded by SQL over a landed table as a metric is. The cube builds
//! its head as it builds a metric's and serves its cells under the
//! metric it names, `metric_series(world => 'plan', plan => '<name>')`,
//! and in no other world: a plan is no metric of the actual world, no
//! series the walk or the forecast reads, no collision candidate. An
//! edition of a plan is an aspect of its own, so two editions stand
//! side by side and a comparison is a join of two plan reads.
//!
//! What this module holds is the tie: read from a declaration, checked
//! at `DECLARE`, and the refusal a read by an unknown name gets. The
//! cells are the cube's (`crate::cube`).

use std::sync::Arc;

use glossql_parser::{AspectDecl, AspectKind};
use serde_json::Value;

use crate::reads::Shared;
use crate::session::SessionError;

/// The metric a plan aspect's declaration names, where the schema
/// declares `x-kind: plan`: `x-plan-of` as a string. None for any other
/// aspect, and for a plan schema that names no metric — which
/// [`check_declaration`] refuses before it is declared.
pub(crate) fn plan_of(schema: &Value) -> Option<&str> {
    if schema["x-kind"].as_str() != Some("plan") {
        return None;
    }
    schema["x-plan-of"].as_str()
}

/// A plan declares as a QUERY aspect tied to a declared metric: refused
/// before the declaration lands where `x-kind: plan` rides another kind,
/// names no metric, names an aspect nobody declared, names a FACT or a
/// MEASUREMENT aspect, or names another plan. `x-plan-of` on an aspect
/// that is not a plan is refused too: the tie means one thing.
pub(crate) async fn check_declaration(
    shared: &Arc<Shared>,
    d: &AspectDecl,
) -> Result<(), SessionError> {
    let name = d.name.value.as_str();
    let schema = &d.schema.value;
    let bad = |detail: String| {
        SessionError::BadSubject(format!("DECLARE ASPECT {name}: {detail} (fixture 27)"))
    };
    let is_plan = schema["x-kind"].as_str() == Some("plan");
    let of = schema.get("x-plan-of");
    if !is_plan {
        if of.is_some() {
            return Err(bad(
                "`x-plan-of` rides `x-kind: plan` — a plan is a QUERY aspect declared with both"
                    .into(),
            ));
        }
        return Ok(());
    }
    if d.kind != AspectKind::Query {
        return Err(bad(
            "a plan is a QUERY aspect — `AS QUERY ON DATASET`; its numbers are a landed table \
             read by SQL, as a metric's are"
                .into(),
        ));
    }
    let Some(metric) = of.and_then(Value::as_str) else {
        return Err(bad(
            "a plan names the metric it plans — `\"x-plan-of\": \"<metric>\"` beside \
             `\"x-kind\": \"plan\"`"
                .into(),
        ));
    };
    match shared.store.aspect(metric).await? {
        None => Err(bad(format!(
            "`{metric}` is not a declared aspect — a plan is tied to a declared QUERY aspect; \
             `DECLARE ASPECT {metric} … AS QUERY ON DATASET` first"
        ))),
        Some((_, kind, _)) if kind != "query" => Err(bad(format!(
            "`{metric}` is a {kind} aspect — a plan is tied to a QUERY aspect, a metric"
        ))),
        Some((metric_schema, _, _)) if metric_schema["x-kind"].as_str() == Some("plan") => {
            Err(bad(format!(
                "`{metric}` is itself a plan — a plan is tied to a metric, never to another plan"
            )))
        }
        Some(_) => Ok(()),
    }
}

/// The refusal for `plan => '<name>'` naming no plan the cube holds:
/// nothing declared under the name, an aspect that is not a plan, or
/// a plan with no serving grounding on the dataset in use.
pub(crate) async fn unknown(shared: &Arc<Shared>, name: &str) -> SessionError {
    let bad = |detail: String| {
        SessionError::BadSubject(format!("metric_series(plan => '{name}'): {detail}"))
    };
    match shared.store.aspect(name).await {
        Err(e) => SessionError::from(e),
        Ok(None) => bad(
            "no aspect of that name is declared — a plan is a QUERY aspect declared with \
             `x-kind: plan` and `x-plan-of: <metric>` (fixture 27)"
                .into(),
        ),
        Ok(Some((schema, kind, _))) if kind != "query" || plan_of(&schema).is_none() => {
            bad(format!(
                "`{name}` is a {kind} aspect without `x-kind: plan` — a plan is a QUERY aspect \
                 declared with it and `x-plan-of: <metric>` (fixture 27)"
            ))
        }
        Ok(Some(_)) => {
            let dataset = shared
                .dataset
                .read()
                .expect("state lock")
                .clone()
                .unwrap_or_else(|| "<dataset>".into());
            bad(format!(
                "nothing serves it on `{dataset}` — `GLOSS {name} ON {dataset} AS \
                 $${{\"sql\": \"…\"}}$$` grounds the plan; a contested slot is withheld"
            ))
        }
    }
}
