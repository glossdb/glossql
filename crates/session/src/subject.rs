//! Subject resolution (SPEC.md §4): statements may spell subjects with or
//! without the dataset prefix; the store holds them dataset-relative. The
//! rules here turn spelled segments into (dataset, relative subject), using
//! the declared datasets and the `USE`'d one.

use glossql_glossary::{Scope, Store};

use crate::session::SessionError;

/// A resolved subject: the dataset it belongs to and the store's canonical
/// relative spelling (`fin` itself, `orders`, `orders.amount`,
/// `orders.customer_id -> customers.id`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Resolved {
    pub dataset: String,
    pub subject: String,
}

impl Resolved {
    /// The read scope this subject spans: naming the dataset itself sweeps
    /// it; anything else is the subject and its descendants.
    pub fn scope(&self) -> Scope {
        if self.subject == self.dataset {
            Scope::Dataset
        } else {
            Scope::Subject(self.subject.clone())
        }
    }
}

/// Resolve a 1–3 segment path. `use_dataset` is the session's `USE` state.
pub async fn resolve_path(
    store: &Store,
    use_dataset: Option<&str>,
    segments: &[String],
) -> Result<Resolved, SessionError> {
    let relative = |subject: String| -> Result<Resolved, SessionError> {
        let dataset = use_dataset.ok_or(SessionError::NoDataset)?;
        Ok(Resolved {
            dataset: dataset.to_string(),
            subject,
        })
    };
    match segments {
        [one] => {
            if store.dataset_exists(one).await? {
                Ok(Resolved {
                    dataset: one.clone(),
                    subject: one.clone(),
                })
            } else if use_dataset.is_none() && store.source_settings(one).await?.is_some() {
                // A declared source resolves workspace-wide — SOURCE-grain
                // slots serve in every dataset (ruled 2026-08-12), and
                // `DECLARE SOURCE` and `PROBE` run datasetless, so the
                // conventions read must too (found 2026-08-14: the one
                // read meant to be dataset-independent demanded a USE).
                Ok(Resolved {
                    dataset: one.clone(),
                    subject: one.clone(),
                })
            } else {
                relative(one.clone())
            }
        }
        [first, rest @ ..] if store.dataset_exists(first).await? => Ok(Resolved {
            dataset: first.clone(),
            subject: rest.join("."),
        }),
        [_, _] => relative(segments.join(".")),
        _ => Err(SessionError::BadSubject(format!(
            "`{}` — up to dataset.table.column, and `{}` is not a declared dataset",
            segments.join("."),
            segments[0]
        ))),
    }
}

/// Resolve one endpoint of a pair path: `[dataset.]table` segments plus the
/// key — one column, or ≥ 2 for a composite endpoint (the tuple is the key,
/// ruled 2026-08-05). Canonical spelling: `table.column` / `table.(a, b)`.
pub async fn resolve_endpoint(
    store: &Store,
    use_dataset: Option<&str>,
    table_segments: &[String],
    columns: &[String],
) -> Result<Resolved, SessionError> {
    let (dataset, table) = match table_segments {
        [table] => (
            use_dataset.ok_or(SessionError::NoDataset)?.to_string(),
            table.clone(),
        ),
        [dataset, table] if store.dataset_exists(dataset).await? => {
            (dataset.clone(), table.clone())
        }
        _ => {
            return Err(SessionError::BadSubject(format!(
                "relationship endpoint `{}` must be [dataset.]table.column or [dataset.]table.(a, b)",
                table_segments.join(".")
            )));
        }
    };
    let key = match columns {
        [] => {
            return Err(SessionError::BadSubject(format!(
                "relationship endpoint `{table}` names no key column"
            )));
        }
        [one] => one.clone(),
        many => format!("({})", many.join(", ")),
    };
    Ok(Resolved {
        dataset,
        subject: format!("{table}.{key}"),
    })
}

/// Resolve a textual `[dataset.]table.column` endpoint. Substrate SQL spells
/// no tuples — composite pairs are addressed by sweep plus WHERE on the
/// subject text.
pub async fn resolve_column_endpoint(
    store: &Store,
    use_dataset: Option<&str>,
    segments: &[String],
) -> Result<Resolved, SessionError> {
    match segments.split_last() {
        Some((column, table_segments)) if !table_segments.is_empty() => {
            resolve_endpoint(
                store,
                use_dataset,
                table_segments,
                std::slice::from_ref(column),
            )
            .await
        }
        _ => Err(SessionError::BadSubject(format!(
            "relationship endpoint `{}` must be table.column",
            segments.join(".")
        ))),
    }
}

/// Join two resolved endpoints into the canonical pair-path subject.
pub fn pair_subject(left: &Resolved, op: &str, right: &Resolved) -> String {
    format!("{} {op} {}", left.subject, right.subject)
}
