//! Cell-level cast accounting: typing is authored, so a `try_*` that
//! fails lands a NULL cell in a kept row — invisible in row counts,
//! which is the import's blind spot (ruled 2026-08-06). At the landing,
//! the one moment raw and typed values coexist, each cast in the
//! recipe's SELECT list is re-read as `input IS NOT NULL AND cast IS
//! NULL`: one companion aggregate for the counts, one grouped read per
//! failing column for its top tokens by frequency. The tokens are
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
/// full expression (what lands) and the try-node's input (what was
/// there), both rendered back to SQL.
pub(crate) struct Target {
    pub column: String,
    pub inner: String,
    pub full: String,
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
        if let Some(inner) = try_input(expr) {
            targets.push(Target {
                column,
                inner,
                full: expr.to_string(),
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
            parse_projection(&format!(
                "SUM(CASE WHEN ({}) IS NOT NULL AND ({}) IS NULL THEN 1 ELSE 0 END)",
                t.inner, t.full
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

/// The token read for one failing column: the cast's input values that
/// nulled, most frequent first.
pub(crate) fn tokens_sql(
    select: &datafusion::sql::sqlparser::ast::Select,
    target: &Target,
) -> String {
    let mut q = select.clone();
    q.projection = vec![
        parse_projection(&format!("CAST(({}) AS VARCHAR)", target.inner))
            .expect("token projection parses"),
        parse_projection("COUNT(*)").expect("count projection parses"),
    ];
    let miss = format!(
        "({}) IS NOT NULL AND ({}) IS NULL",
        target.inner, target.full
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

/// The input of the outermost `try_*` in the expression, if any — the
/// pre-order walk meets an ancestor before its descendants, so the
/// first hit is never inside another try. `COALESCE(try_to_date(x, a),
/// try_to_date(x, b))` accounts correctly through this: the cell counts
/// as failed only when the whole projection landed NULL over a present
/// input, so a second format that catches it is not a failure.
fn try_input(expr: &Expr) -> Option<String> {
    let mut found = None;
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
            found = Some(inner);
            return ControlFlow::Break(());
        }
        ControlFlow::<()>::Continue(())
    });
    found
}
