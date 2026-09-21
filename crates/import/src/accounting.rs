//! Cell-level cast accounting: typing is authored, so a `try_*` that
//! fails lands a NULL cell in a kept row — invisible in row counts,
//! which is the import's blind spot. At the landing,
//! the one moment raw and typed values coexist, each cast in the
//! recipe's SELECT list is re-read as `input IS NOT NULL AND cast IS
//! NULL`: one companion aggregate for the counts, one grouped read per
//! failing `try_*` call site for its top tokens by frequency — per
//! SITE, not per column, so a projection that composes two casts over
//! two columns samples the values that actually failed, never the
//! other input's. The tokens are
//! derived from the data — **no sentinel vocabulary exists here, ruled:
//! none may** — and judging them is the agent's job, closed by an
//! authored recipe amendment.

use std::ops::ControlFlow;

use datafusion::sql::sqlparser::ast::{
    CastKind, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, SelectItem,
    SetExpr, Statement, visit_expressions,
};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;

/// One cast column's account: how many cells had a value the cast
/// nulled, and the most frequent of those values.
#[derive(Debug, Clone)]
pub struct CastCheck {
    pub column: String,
    pub failed: u64,
    /// `(token, count)`, most frequent first, capped at 8.
    pub tokens: Vec<(String, u64)>,
}

/// What the landing knows about its casts.
#[derive(Debug)]
pub enum CastAccounting {
    /// Every `try_*` in the SELECT list was accounted — possibly none.
    Checked(Vec<CastCheck>),
    /// The landing happened; the accounting could not. The note says why.
    Unchecked(String),
}

impl CastAccounting {
    pub fn to_json(&self) -> serde_json::Value {
        match self {
            CastAccounting::Checked(checks) => serde_json::json!({
                "checked": checks
                    .iter()
                    .map(|c| serde_json::json!({
                        "column": c.column,
                        "failed": c.failed,
                        "tokens": c.tokens,
                    }))
                    .collect::<Vec<_>>(),
            }),
            CastAccounting::Unchecked(note) => serde_json::json!({ "unchecked": note }),
        }
    }
}

/// A cast column the companion queries will account: the projection's
/// full expression (what lands) and every `try_*` call site within it
/// (what was there, per input), all rendered back to SQL.
pub(crate) struct Target {
    pub column: String,
    pub full: String,
    pub sites: Vec<Site>,
}

/// One `try_*` call site inside a projection: the try expression and
/// its input.
pub(crate) struct Site {
    pub try_expr: String,
    pub inner: String,
}

pub(crate) enum Plan {
    Checked {
        counts_sql: String,
        targets: Vec<Target>,
        /// The recipe's SELECT, kept for the per-column token reads.
        select: Box<datafusion::sql::sqlparser::ast::Select>,
    },
    Unchecked(String),
}

/// Read the recipe's shape and derive the companion queries. Flat
/// SELECTs only — a recipe that aggregates or unions has no per-row
/// cast to account, and saying so honestly beats guessing.
pub(crate) fn plan(sql: &str) -> Plan {
    let skip = |what: &str| Plan::Unchecked(format!("recipe shape ({what})"));
    let Ok(statements) = Parser::parse_sql(&GenericDialect {}, sql) else {
        return skip("did not re-parse");
    };
    let [Statement::Query(query)] = statements.as_slice() else {
        return skip("not a single query");
    };
    if query.with.is_some() {
        return skip("WITH");
    }
    let SetExpr::Select(select) = query.body.as_ref() else {
        return skip("set operation");
    };
    if select.distinct.is_some() || select.having.is_some() || select.top.is_some() {
        return skip("DISTINCT/HAVING/TOP");
    }
    match &select.group_by {
        GroupByExpr::Expressions(exprs, modifiers) if exprs.is_empty() && modifiers.is_empty() => {}
        _ => return skip("GROUP BY"),
    }

    let mut targets = Vec::new();
    for item in &select.projection {
        let (expr, column) = match item {
            SelectItem::ExprWithAlias { expr, alias } => (expr, alias.value.clone()),
            SelectItem::UnnamedExpr(expr) => (expr, expr.to_string()),
            _ => continue,
        };
        let sites = try_sites(expr);
        if !sites.is_empty() {
            targets.push(Target {
                column,
                full: expr.to_string(),
                sites,
            });
        }
    }
    if targets.is_empty() {
        return Plan::Checked {
            counts_sql: String::new(),
            targets,
            select: select.clone(),
        };
    }

    let mut counts = select.as_ref().clone();
    counts.projection = targets
        .iter()
        .map(|t| {
            let present = t
                .sites
                .iter()
                .map(|s| format!("({}) IS NOT NULL", s.inner))
                .collect::<Vec<_>>()
                .join(" OR ");
            parse_projection(&format!(
                "SUM(CASE WHEN ({present}) AND ({}) IS NULL THEN 1 ELSE 0 END)",
                t.full
            ))
        })
        .collect::<Result<_, _>>()
        .expect("companion projection parses: built from rendered exprs");
    Plan::Checked {
        counts_sql: counts.to_string(),
        targets,
        select: select.clone(),
    }
}

/// The token read for one call site of a failing column: the values
/// this site's cast nulled while the whole projection landed NULL,
/// most frequent first. The projection-NULL conjunct keeps the
/// COALESCE rule: a second format that catches the value is not a
/// failure, so its site samples nothing.
pub(crate) fn tokens_sql(
    select: &datafusion::sql::sqlparser::ast::Select,
    target: &Target,
    site: &Site,
) -> String {
    let mut q = select.clone();
    q.projection = vec![
        parse_projection(&format!("CAST(({}) AS VARCHAR)", site.inner))
            .expect("token projection parses"),
        parse_projection("COUNT(*)").expect("count projection parses"),
    ];
    let miss = format!(
        "({}) IS NOT NULL AND ({}) IS NULL AND ({}) IS NULL",
        site.inner, site.try_expr, target.full
    );
    let miss = Parser::new(&GenericDialect {})
        .try_with_sql(&miss)
        .and_then(|mut p| p.parse_expr())
        .expect("token predicate parses");
    q.selection = Some(match q.selection.take() {
        Some(existing) => Expr::BinaryOp {
            left: Box::new(Expr::Nested(Box::new(existing))),
            op: datafusion::sql::sqlparser::ast::BinaryOperator::And,
            right: Box::new(miss),
        },
        None => miss,
    });
    format!("{q} GROUP BY 1 ORDER BY 2 DESC, 1 LIMIT 8")
}

fn parse_projection(
    sql: &str,
) -> Result<SelectItem, datafusion::sql::sqlparser::parser::ParserError> {
    Ok(SelectItem::UnnamedExpr(
        Parser::new(&GenericDialect {})
            .try_with_sql(sql)?
            .parse_expr()?,
    ))
}

/// Every `try_*` call site in the expression, one per distinct input.
/// The pre-order walk meets an ancestor before its descendants, so a
/// try nested inside an earlier site's input drops in the filter, and
/// `COALESCE(try_to_date(x, a), try_to_date(x, b))` collapses to one
/// site over `x` — which keeps the fallback rule: the cell counts as
/// failed only when the whole projection landed NULL over a present
/// input, so a second format that catches it is not a failure. Two
/// sites over DIFFERENT inputs both survive, and each samples its own
/// input's tokens — the attribution the composite-expression bug was
/// about.
fn try_sites(expr: &Expr) -> Vec<Site> {
    let mut found: Vec<Site> = Vec::new();
    let _ = visit_expressions(expr, |e| {
        let inner = match e {
            Expr::Cast {
                kind: CastKind::TryCast | CastKind::SafeCast,
                expr,
                ..
            } => Some(expr.to_string()),
            Expr::Function(f)
                if f.name.0.last().and_then(|p| p.as_ident()).is_some_and(|i| {
                    let n = i.value.to_lowercase();
                    n == "try_to_date" || n == "try_to_timestamp"
                }) =>
            {
                let FunctionArguments::List(list) = &f.args else {
                    return ControlFlow::Continue(());
                };
                match list.args.first() {
                    Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(arg))) => Some(arg.to_string()),
                    _ => return ControlFlow::Continue(()),
                }
            }
            _ => None,
        };
        if let Some(inner) = inner {
            found.push(Site {
                try_expr: e.to_string(),
                inner,
            });
        }
        ControlFlow::<()>::Continue(())
    });
    let mut sites: Vec<Site> = Vec::new();
    for s in found {
        let kept = sites
            .iter()
            .any(|k| k.inner == s.inner || k.inner.contains(&s.try_expr));
        if !kept {
            sites.push(s);
        }
    }
    sites
}

/// The path or glob of every `read_*` call in `sql`, in the order the
/// statement names them — what a recipe scans, read from its text before
/// anything plans. A recipe that does not parse names none; the planner
/// refuses it in its own words.
pub(crate) fn file_scans(sql: &str) -> Vec<String> {
    use datafusion::sql::sqlparser::ast::{
        Expr, FunctionArg, FunctionArgExpr, TableFactor, Value, Visit, Visitor,
    };
    struct Scans(Vec<String>);
    impl Visitor for Scans {
        type Break = ();
        fn pre_visit_table_factor(&mut self, factor: &TableFactor) -> ControlFlow<()> {
            if let TableFactor::Table {
                name,
                args: Some(args),
                ..
            } = factor
                && crate::READERS
                    .iter()
                    .any(|(reader, _)| name.to_string().eq_ignore_ascii_case(reader))
                && let [FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(value)))] =
                    args.args.as_slice()
                && let Value::SingleQuotedString(path) = &value.value
            {
                self.0.push(path.clone());
            }
            ControlFlow::Continue(())
        }
    }
    let Ok(statements) = Parser::parse_sql(&GenericDialect {}, sql) else {
        return Vec::new();
    };
    let mut scans = Scans(Vec::new());
    let _ = statements.visit(&mut scans);
    scans.0
}

/// Whether appending the rows of new files alone yields the recipe's
/// result over all of them. It does when every landed row comes from one
/// source row of one scan and no row's value or presence depends on
/// another row — read from the engine's own plan, where an aggregate, a
/// window, a join, a limit or a set operation is a node and not a
/// spelling: only scans, projections, filters, aliases and sorts pass,
/// over exactly one scan.
pub(crate) fn appendable(plan: &datafusion::logical_expr::LogicalPlan) -> Result<(), String> {
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    use datafusion::logical_expr::LogicalPlan;
    let mut scans = 0usize;
    let mut other: Option<String> = None;
    let _ = plan.apply(|node| {
        match node {
            LogicalPlan::TableScan(_) => scans += 1,
            LogicalPlan::Projection(_) | LogicalPlan::Filter(_) | LogicalPlan::SubqueryAlias(_) => {
            }
            LogicalPlan::Sort(sort) if sort.fetch.is_none() => {}
            node => {
                other = Some(node.display().to_string());
                return Ok(TreeNodeRecursion::Stop);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    });
    let cannot = |what: String| {
        Err(format!(
            "the recipe {what}, so its result is not its rows file by file, and the lake \
             cannot yet replace a table's rows in one commit"
        ))
    };
    match (other, scans) {
        (Some(node), _) => cannot(format!("plans a `{node}`")),
        (None, 1) => Ok(()),
        (None, n) => cannot(format!("reads {n} relations")),
    }
}
