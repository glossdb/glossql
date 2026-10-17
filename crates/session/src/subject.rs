//! Subject resolution (SPEC.md §4): statements may spell subjects with or
//! without the dataset prefix; the store holds them dataset-relative. The
//! rules here turn spelled segments into (dataset, relative subject), using
//! the declared datasets and the `USE`'d one.

use datafusion::sql::planner::IdentNormalizer;
use datafusion::sql::sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, TableFactor, Value as SqlValue,
};
use glossql_glossary::{Scope, Store};

use crate::shared::SessionError;

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
///
/// A two-part path whose head names both a dataset and a landed table
/// of the `USE`'d dataset is `table.column` (SPEC.md §3): the `USE`'d
/// dataset is the nearer scope. The existence query runs only on that
/// collision — a head that names no dataset never pays it.
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
                // slots serve in every dataset, and
                // `DECLARE SOURCE` and `PROBE` run datasetless, so the
                // conventions read must too — the one read meant to be
                // dataset-independent must never demand a USE.
                Ok(Resolved {
                    dataset: one.clone(),
                    subject: one.clone(),
                })
            } else {
                relative(one.clone())
            }
        }
        [first, rest @ ..] if store.dataset_exists(first).await? => {
            if let [column] = rest
                && let Some(used) = use_dataset
                && store.table_exists(used, first).await?
            {
                return relative(format!("{first}.{column}"));
            }
            Ok(Resolved {
                dataset: first.clone(),
                subject: rest.join("."),
            })
        }
        [_, _] => relative(segments.join(".")),
        _ => Err(SessionError::BadSubject(format!(
            "`{}` — up to dataset.table.column, and `{}` is not a declared dataset",
            segments.join("."),
            segments[0]
        ))),
    }
}

/// Resolve one endpoint of a pair path: `[dataset.]table` segments plus the
/// key — one column, or ≥ 2 for a composite endpoint (the tuple is the
/// key). Canonical spelling: `table.column` / `table.(a, b)`.
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

/// A name as a quoted identifier, embedded quotes doubled as SQL wants
/// them — how every door spells a landed name inside the SQL it builds.
/// A landed column is whatever its export called it, a quote included.
pub(crate) fn qi(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Reads a factor as the `subject_column` door: `None` if it is
/// something else (including a bare `subject_column` table, which stays
/// a table), `Some(Err)` if it is the door with anything but one quoted
/// subject. Both sides of the seam call this — the pre-pass to collect,
/// the planner to serve or refuse — so the two cannot disagree on what
/// the door accepts.
pub(crate) fn subject_column_arg(
    idents: &IdentNormalizer,
    f: &TableFactor,
) -> Option<Result<String, SessionError>> {
    let TableFactor::Table {
        name,
        args: Some(a),
        ..
    } = f
    else {
        return None;
    };
    let [part] = name.0.as_slice() else {
        return None;
    };
    if !part
        .as_ident()
        .is_some_and(|i| idents.normalize(i.clone()) == "subject_column")
    {
        return None;
    }
    if let [FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(v)))] = a.args.as_slice()
        && let SqlValue::SingleQuotedString(subject) = &v.value
    {
        return Some(Ok(subject.clone()));
    }
    Some(Err(SessionError::BadSubject(
        "subject_column takes one quoted subject: subject_column('table.column')".into(),
    )))
}

#[cfg(test)]
mod tests {
    use super::qi;

    #[test]
    fn a_quote_inside_a_name_doubles() {
        assert_eq!(qi("amount"), "\"amount\"");
        assert_eq!(qi("net \"adj\""), "\"net \"\"adj\"\"\"");
    }
}
