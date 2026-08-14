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
/// build on reads — `open_questions` and `ruling_conflicts` both read
/// `ruling_entries` — which is the point of expanding them through the
/// planner rather than executing them as strings.
pub(crate) const LIBRARY: &[(&str, &str)] = &[
    (
        "ruling_entries",
        include_str!("../reads/ruling_entries.sql"),
    ),
    (
        "ruling_conflicts",
        include_str!("../reads/ruling_conflicts.sql"),
    ),
    (
        "open_questions",
        include_str!("../reads/open_questions.sql"),
    ),
    ("app_parts", include_str!("../reads/app_parts.sql")),
];

/// The SQL behind a shipped read, or `None` for a name we do not ship —
/// in which case the relation falls through to ordinary planning. A
/// name we do ship is reserved: it shadows a workspace table called the
/// same thing, as the store's relations already do.
pub(crate) fn read_sql(name: &str) -> Option<&'static str> {
    LIBRARY
        .iter()
        .find(|(n, _)| *n == name)
        .map(|(_, sql)| *sql)
}

#[cfg(test)]
mod tests {
    use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
    use datafusion::sql::sqlparser::parser::Parser;

    /// Every shipped read is a single query — the shape `plan_sql`
    /// requires. A read that only fails at first use is a read nobody
    /// finds until a run does.
    #[test]
    fn every_shipped_read_is_a_query() {
        for (name, sql) in super::LIBRARY {
            Parser::new(&PostgreSqlDialect {})
                .try_with_sql(sql)
                .and_then(|mut p| p.parse_query())
                .unwrap_or_else(|e| panic!("the shipped read `{name}` does not parse: {e}"));
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
