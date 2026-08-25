//! The read library: derived reads shipped as SQL files, planned through
//! the same expansion `read.<aspect>()` uses (`reads::GlossqlReads::plan_sql`).
//!
//! A library read is a bare relation — `FROM open_questions` — so it
//! composes in SQL like any table, filters ride WHERE, and one file
//! serves every consumer: the MCP door reads it, an app frame renders
//! it, a skill names it. The alternative it replaces is what the code
//! did before: the same derivation written once as a Rust string
//! constant in the door, once as a `.sql` frame in the app, and once as
//! prose in a skill, so every correction cost three edits or drifted.
//!
//! Shipped reads are system knowledge and live here, in the binary.
//! Reads a workspace authors are QUERY glosses served by
//! `read.<aspect>()` — same expansion, same call posture, different
//! source of the SQL.

/// Every shipped read, by the name it answers to in `FROM`. Reads
/// build on reads — `open_questions` reads `ruling_entries` — which is
/// the point of expanding them through the planner rather than
/// executing them as strings.
pub(crate) const LIBRARY: &[(&str, &str)] = &[
    (
        "agent_assumptions",
        include_str!("../reads/agent_assumptions.sql"),
    ),
    ("owed", include_str!("../reads/owed.sql")),
    (
        "ruling_entries",
        include_str!("../reads/ruling_entries.sql"),
    ),
    (
        "open_questions",
        include_str!("../reads/open_questions.sql"),
    ),
    ("app_parts", include_str!("../reads/app_parts.sql")),
    (
        "workspace_next",
        include_str!("../reads/workspace_next.sql"),
    ),
    (
        "metric_surfaces",
        include_str!("../reads/metric_surfaces.sql"),
    ),
];

/// The SQL behind a shipped read, or `None` for a name we do not ship —
/// in which case the relation falls through to ordinary planning.
///
/// A name we do ship is RESERVED, and reserved harder than it looks: it
/// shadows a workspace table of the same name, as the store's relations
/// already do, and it also shadows a CTE an author declares in their own
/// query. The planner seam sees the raw `TableFactor` before default
/// planning and `RelationPlannerContext` (datafusion-expr-53.1.0
/// planner.rs:400) exposes no CTE scope, so there is nothing to defer
/// to — found the first time a shipped name met a frame's own
/// `WITH owed AS (…)`. Keep the shipped set small and its names
/// specific; a plain word like `owed` or `claims` is a name authors
/// reach for.
pub(crate) fn read_sql(name: &str) -> Option<&'static str> {
    LIBRARY
        .iter()
        .find(|(n, _)| *n == name)
        .map(|(_, sql)| *sql)
}

#[cfg(test)]
mod tests {
    /// Every shipped read is one query under the pre-pass's own rule —
    /// the shape a door serves. A read that only fails at first use is
    /// a read nobody finds until a run does.
    #[test]
    fn every_shipped_read_is_a_query() {
        for (name, sql) in super::LIBRARY {
            crate::prepass::parse(sql, name)
                .unwrap_or_else(|e| panic!("the shipped read `{name}`: {e}"));
        }
    }

    /// Names are unique and lowercase: the dispatch lowercases the
    /// relation before looking it up.
    #[test]
    fn shipped_names_are_lowercase_and_unique() {
        let mut seen: Vec<&str> = Vec::new();
        for (name, _) in super::LIBRARY {
            assert_eq!(*name, name.to_lowercase(), "`{name}` is not lowercase");
            assert!(!seen.contains(name), "`{name}` is shipped twice");
            seen.push(name);
        }
    }
}
