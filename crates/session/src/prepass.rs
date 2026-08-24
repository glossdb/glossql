//! The async pre-pass: resolve every door a statement names *before* the
//! planner runs, so planning is sync over an AST with nothing left to
//! fetch.
//!
//! This copies DataFusion's own shape: `statement_to_plan` walks a
//! statement for table references, awaits each one through the catalog
//! into a map, and only then runs the sync `SqlToRel`. The opposite
//! shape — a sync `RelationPlanner` reaching back into an async store
//! through `block_in_place` — costs three ways:
//!
//! - a blocked planner thread, which the guide names as the pitfall;
//! - re-entrancy, since expansion re-plans through the same context,
//!   which needs a `thread_local` stack to notice a cycle;
//! - one stack for the whole nesting, which datafusion 54 overflows.
//!
//! Resolution is depth-first and carries its path, so the cycle check is
//! the traversal rather than a mechanism. A door reached twice on
//! different branches is not on one path and resolves once.

use std::collections::{HashMap, HashSet};
use std::ops::ControlFlow;
use std::sync::Arc;

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::TableProvider;
use datafusion::datasource::provider_as_source;
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder, ident};
use datafusion::prelude::SessionContext;
use datafusion::sql::parser::Statement as DFStatement;
use datafusion::sql::planner::IdentNormalizer;
use datafusion::sql::resolve::resolve_table_references;
use datafusion::sql::sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, Query, Statement as SQLStatement, TableFactor,
    Value as SqlValue, VisitMut, VisitorMut,
};
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::sqlparser::tokenizer::Token;

use crate::reads::{Shared, served_grounding};
use crate::session::SessionError;

/// What a statement resolved before planning: the bound dataset's tables
/// pinned at one snapshot each, every SQL-bodied door as a plan, and
/// every compute door as a batch. Immutable once built and handed to a
/// planner that only reads it — planning fetches nothing, computes
/// nothing, and never re-enters.
#[derive(Debug, Default, Clone)]
pub(crate) struct Resolved {
    plans: HashMap<String, Arc<LogicalPlan>>,
    pins: HashMap<String, Arc<dyn TableProvider>>,
    batches: HashMap<String, RecordBatch>,
    ctes: HashSet<String>,
    /// Whether anything this statement resolved — at any expansion
    /// depth — reads the glossary: the relation itself, the GLOSSARY/
    /// ATTEST doors, a served grounding, or a compute door over slots.
    /// Derived here because resolution is the one place every name
    /// passes through; nothing curates a list of frames. The app door
    /// serves it as the frame class (`record`/`data`): a `record`
    /// frame can change under a glossary write
    /// (a ruling), a `data` frame provably cannot.
    record: bool,
}

impl Resolved {
    pub(crate) fn plan(&self, key: &str) -> Option<Arc<LogicalPlan>> {
        self.plans.get(key).cloned()
    }

    pub(crate) fn pin(&self, table: &str) -> Option<Arc<dyn TableProvider>> {
        self.pins.get(table).cloned()
    }

    /// The pinned tables' names — the bound dataset's tables as this
    /// statement sees them.
    pub(crate) fn tables(&self) -> Vec<String> {
        self.pins.keys().cloned().collect()
    }

    pub(crate) fn batch(&self, key: &str) -> Option<&RecordBatch> {
        self.batches.get(key)
    }

    /// Whether this factor is a name the statement binds as a CTE — see
    /// [`shadowed`].
    pub(crate) fn shadowed(&self, idents: &IdentNormalizer, f: &TableFactor) -> bool {
        shadowed(idents, &self.ctes, f)
    }

    /// Whether the statement reads the glossary anywhere in its
    /// expansion — the frame class, derived during resolution.
    pub(crate) fn touches_record(&self) -> bool {
        self.record
    }
}

/// Whether the factor is a bare reference to one of the statement's CTE
/// names — the planner leaves it alone so DataFusion's CTE lookup serves
/// it. Shipped read names stay reserved over both tables and CTEs.
fn shadowed(idents: &IdentNormalizer, ctes: &HashSet<String>, f: &TableFactor) -> bool {
    let TableFactor::Table {
        name, args: None, ..
    } = f
    else {
        return false;
    };
    let [part] = name.0.as_slice() else {
        return false;
    };
    let Some(ident) = part.as_ident() else {
        return false;
    };
    let name = idents.normalize(ident.clone());
    ctes.contains(&name) && crate::library::read_sql(&name).is_none()
}

/// A door reference this pass knows how to resolve ahead of planning:
/// something whose body is SQL. Compute doors build batches and are
/// stage 4's business.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Door {
    /// `read.<aspect>()` — a QUERY grounding, fetched from the store.
    Serve(String),
    /// A shipped read (`crates/session/reads/*.sql`) — SQL from the binary.
    Shipped(String),
    /// `subject_column('table.column')` — the named column of a pinned
    /// table, projected as `v`. The primitive a column-grain measurement
    /// body stands on: the body is declared once, the subject varies per
    /// extraction, so the body cannot name the column itself.
    /// No SQL behind it — the plan is built here.
    Column(String),
    /// `misfit.<frame>()` / `whatif.<scenario>()`. These still build their
    /// batch inside the planner (stage 4 moves them), so the pre-pass does
    /// not plan them — it walks the body they replay, for the path. That
    /// is the whole cycle guard: a frame whose SQL names its own door is a
    /// repeat on the path, where it used to be a stack overflow.
    Replay(&'static str, String),
}

impl Door {
    fn key(&self) -> String {
        match self {
            Door::Serve(a) => format!("read.{a}"),
            Door::Shipped(n) => format!("read:{n}"),
            Door::Column(s) => format!("subject_column:{s}"),
            Door::Replay(kind, n) => format!("{kind}.{n}"),
        }
    }

    fn what(&self) -> String {
        match self {
            Door::Serve(a) => format!("the grounding for `{a}` (read.{a}())"),
            Door::Shipped(n) => format!("the shipped read `{n}`"),
            Door::Column(s) => format!("the subject's column (subject_column('{s}'))"),
            Door::Replay(kind, n) => format!("the body `{kind}.{n}()` replays"),
        }
    }

    /// Whether the pre-pass plans it, or only walks it for the path.
    fn planned(&self) -> bool {
        !matches!(self, Door::Replay(..))
    }
}

/// Every SQL-bodied door named anywhere in the query — one total
/// traversal via sqlparser's derive-generated visitor, so a scalar
/// subquery in the SELECT list is covered like a FROM item. A
/// hand-written walker misses positions; this one cannot.
fn doors_in(idents: &IdentNormalizer, q: &mut Query) -> Vec<Door> {
    struct Collect<'a>(Vec<Door>, &'a IdentNormalizer);
    impl VisitorMut for Collect<'_> {
        type Break = ();
        fn pre_visit_table_factor(&mut self, f: &mut TableFactor) -> ControlFlow<()> {
            if let TableFactor::Table { name, args, .. } = f {
                // Normalized here and nowhere else: a door's key is built
                // from these parts and the planner rebuilds the same key
                // from the same name, so the two fold identically or the
                // lookup misses something that was resolved.
                let Some(parts) = name
                    .0
                    .iter()
                    .map(|p| p.as_ident().map(|i| self.1.normalize(i.clone())))
                    .collect::<Option<Vec<String>>>()
                else {
                    return ControlFlow::Continue(());
                };
                match parts.as_slice() {
                    [prefix, aspect] if prefix == "read" => {
                        self.0.push(Door::Serve(aspect.clone()));
                    }
                    [prefix, name] if prefix == "misfit" => {
                        self.0.push(Door::Replay("misfit", name.clone()));
                    }
                    [prefix, name] if prefix == "whatif" => {
                        self.0.push(Door::Replay("whatif", name.clone()));
                    }
                    // A bare name with no arguments, if we ship a read of
                    // that name. Anything else falls through untouched.
                    [name] if args.is_none() && crate::library::read_sql(name).is_some() => {
                        self.0.push(Door::Shipped(name.clone()));
                    }
                    _ => {
                        // The malformed-argument case is left uncollected
                        // on purpose: the planner meets the factor, calls
                        // the same reader, and reports it.
                        if let Some(Ok(subject)) = subject_column_arg(self.1, f) {
                            self.0.push(Door::Column(subject));
                        }
                    }
                }
            }
            ControlFlow::Continue(())
        }
    }
    let mut c = Collect(Vec::new(), idents);
    let _ = q.visit(&mut c);
    c.0.sort_by_key(Door::key);
    c.0.dedup();
    c.0
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

fn parse(sql: &str, what: &str) -> Result<Query, SessionError> {
    let mut parser = Parser::new(&PostgreSqlDialect {})
        .try_with_sql(sql)
        .map_err(|e| SessionError::BadSubject(format!("{what} does not parse: {e}")))?;
    let query = parser
        .parse_query()
        .map(|q| *q)
        .map_err(|e| SessionError::BadSubject(format!("{what} does not parse: {e}")))?;
    // `parse_query` stops at the end of one query and says nothing about
    // what follows, so a body of `SELECT 1; DROP TABLE t` planned as
    // `SELECT 1` and dropped the rest in silence. A door serves one
    // query; anything after it was authored and is not being run, which
    // the author has to be told rather than left to assume.
    //
    // Terminators first. A trailing `;` ends the query it follows and is
    // not a second one — `parse_query` leaves it because only the
    // multi-statement loop consumes it (sqlparser `parse_statements`).
    // Refusing it would refuse the ordinary way a body is written.
    while parser.consume_token(&Token::SemiColon) {}
    if parser.peek_token().token != Token::EOF {
        return Err(SessionError::BadSubject(format!(
            "{what} is more than one query — a door serves one, and \
             what follows it would not run"
        )));
    }
    Ok(query)
}

/// The body behind a door, fetched or embedded.
async fn body_of(shared: &Shared, door: &Door) -> Result<String, SessionError> {
    match door {
        Door::Serve(aspect) => served_grounding(shared, aspect).await,
        Door::Shipped(name) => crate::library::read_sql(name)
            .map(str::to_string)
            .ok_or_else(|| SessionError::BadSubject(format!("no shipped read `{name}`"))),
        // Both replay a declared grounding; a body that is not SQL names
        // no doors and walks to nothing.
        Door::Replay(_, name) => Ok(served_grounding(shared, name).await.unwrap_or_default()),
        Door::Column(_) => unreachable!("the column door resolves before any body is fetched"),
    }
}

/// A two-part relation whose head is a bound table and whose tail is
/// one of its columns can only be an extraction subject spelled inside
/// a read: the top-level form parses as its own statement (SPEC.md §6,
/// the compute act), and a read stays a read — the composable surface
/// over a measurement is GLOSSARY. Refused here with the road out,
/// where the engine would say "table not found" and mean it.
fn refuse_subject_relations(q: &mut Query, resolved: &Resolved) -> Result<(), SessionError> {
    for factor in factors_in(q) {
        let TableFactor::Table {
            name, args: None, ..
        } = &factor
        else {
            continue;
        };
        let [t, c] = name.0.as_slice() else {
            continue;
        };
        let (Some(t), Some(c)) = (t.as_ident(), c.as_ident()) else {
            continue;
        };
        let Some(pin) = resolved.pins.get(&t.value) else {
            continue;
        };
        if pin.schema().field_with_name(&c.value).is_ok() {
            return Err(SessionError::BadSubject(format!(
                "`{t}.{c}` is a subject, not a table: an extraction \
                 (SELECT fn() FROM {t}.{c}) is its own statement — run it, then \
                 compose its measurement through GLOSSARY({t}.{c}::aspect)",
                t = t.value,
                c = c.value
            )));
        }
    }
    Ok(())
}

/// Every table factor anywhere in the query, one total traversal.
fn factors_in(q: &mut Query) -> Vec<TableFactor> {
    struct Collect(Vec<TableFactor>);
    impl VisitorMut for Collect {
        type Break = ();
        fn pre_visit_table_factor(&mut self, f: &mut TableFactor) -> ControlFlow<()> {
            if matches!(f, TableFactor::Table { .. }) {
                self.0.push(f.clone());
            }
            ControlFlow::Continue(())
        }
    }
    let mut c = Collect(Vec::new());
    let _ = q.visit(&mut c);
    c.0
}

/// Evaluate every compute door the query names — the batches the sync
/// planner will serve as expansions. Keyed by the factor's own rendering,
/// which is what the planner sees again.
async fn compute_batches(
    shared: &Arc<Shared>,
    q: &mut Query,
    resolved: &mut Resolved,
) -> Result<(), SessionError> {
    let idents = shared.idents();
    for factor in factors_in(q) {
        let key = factor.to_string();
        if resolved.batches.contains_key(&key) || shadowed(&idents, &resolved.ctes, &factor) {
            continue;
        }
        if let Some(batch) = crate::reads::compute_batch(shared, &factor, resolved).await? {
            if crate::reads::reads_the_record(&idents, &factor) {
                resolved.record = true;
            }
            resolved.batches.insert(key, batch);
        }
    }
    Ok(())
}

async fn resolve_door(
    shared: &Arc<Shared>,
    ctx: &SessionContext,
    door: Door,
    path: &mut Vec<String>,
    done: &mut HashSet<String>,
    resolved: &mut Resolved,
) -> Result<(), SessionError> {
    let key = door.key();
    if path.iter().any(|p| p == &key) {
        // The path IS the error message; a set of everything expanded
        // would refuse a diamond, which is legitimate.
        return Err(SessionError::BadSubject(format!(
            "read cycle: {} -> {key}",
            path.join(" -> ")
        )));
    }
    if done.contains(&key) {
        return Ok(());
    }
    // A served grounding and a replayed body both come from the
    // glossary — the statement's answer can change under a glossary
    // write, whatever the body then reads.
    if matches!(door, Door::Serve(_) | Door::Replay(..)) {
        resolved.record = true;
    }
    // The column door has no SQL behind it: one projection of a pinned
    // table, aliased `v`, built right here.
    if let Door::Column(subject) = &door {
        let Some((table, column)) = subject.split_once('.') else {
            return Err(SessionError::BadSubject(format!(
                "subject_column wants 'table.column', got '{subject}'"
            )));
        };
        let provider = resolved.pins.get(table).cloned().ok_or_else(|| {
            SessionError::BadSubject(format!(
                "subject_column: no table `{table}` in the bound dataset"
            ))
        })?;
        let plan = LogicalPlanBuilder::scan(table, provider_as_source(provider), None)
            .and_then(|b| b.project(vec![ident(column).alias("v")]))
            .and_then(|b| b.build())
            .map_err(|e| SessionError::BadSubject(format!("subject_column('{subject}'): {e}")))?;
        done.insert(key.clone());
        resolved.plans.insert(key, Arc::new(plan));
        return Ok(());
    }
    let sql = body_of(shared, &door).await?;
    // A replayed body may not be SQL at all — a `whatif.` scenario is a
    // FACT carrying overrides. It names no doors, so there is nothing to
    // walk and nothing to refuse.
    let mut body = match parse(&sql, &door.what()) {
        Ok(q) => q,
        Err(e) if door.planned() => return Err(e),
        Err(_) => {
            done.insert(key);
            return Ok(());
        }
    };

    path.push(key.clone());
    for child in doors_in(&shared.idents(), &mut body) {
        Box::pin(resolve_door(shared, ctx, child, path, done, resolved)).await?;
    }
    path.pop();

    if !door.planned() {
        done.insert(key);
        return Ok(());
    }
    // Planned with everything it depends on already resolved — nested
    // doors, compute batches and pinned tables all in the map — so the
    // sync planner finds instead of fetching. The body is its own CTE
    // scope, not the outer statement's.
    Box::pin(compute_batches(shared, &mut body, resolved)).await?;
    let stmt = DFStatement::Statement(Box::new(SQLStatement::Query(Box::new(body))));
    let mut scoped = resolved.clone();
    scoped.ctes = resolve_table_references(&stmt, shared.normalize_idents)?
        .1
        .iter()
        .map(|c| c.table().to_string())
        .collect();
    let state = crate::reads::state_with(ctx, shared, scoped);
    let plan = state
        .statement_to_plan(stmt)
        .await
        .map_err(|e| SessionError::BadSubject(format!("not served: {}: {e}", door.what())))?;

    done.insert(key.clone());
    resolved.plans.insert(key, Arc::new(plan));
    Ok(())
}

/// Resolve everything the statement needs ahead of planning: the pin,
/// the doors, the compute batches.
pub(crate) async fn resolve(
    shared: &Arc<Shared>,
    ctx: &SessionContext,
    statement: &DFStatement,
) -> Result<Resolved, SessionError> {
    let DFStatement::Statement(inner) = statement else {
        return Ok(Resolved::default());
    };
    let SQLStatement::Query(q) = inner.as_ref() else {
        return Ok(Resolved::default());
    };
    let mut q = (**q).clone();
    let idents = shared.idents();
    // The CTE names come from DataFusion, not from a visitor of ours.
    // `resolve_table_references` returns them as its second element,
    // folded with the same `enable_ident_normalization` the planner will
    // use — which is the whole requirement, because these names decide
    // whether a factor declines the pin. Its breadth is the same as a
    // hand-rolled walk's: `all_ctes` is every CTE the statement defines
    // at any depth, so a name bound only in a subquery still declines
    // everywhere. That is the standing limit, not a regression — the
    // seam runs before DataFusion's own CTE lookup and has no scope to
    // ask about.
    let (_, ctes) = resolve_table_references(statement, shared.normalize_idents)?;
    let mut resolved = Resolved {
        pins: shared.statement_pins().await?,
        ctes: ctes.iter().map(|c| c.table().to_string()).collect(),
        ..Resolved::default()
    };
    refuse_subject_relations(&mut q, &resolved)?;
    let mut done = HashSet::new();
    let mut path = Vec::new();
    for door in doors_in(&idents, &mut q) {
        resolve_door(shared, ctx, door, &mut path, &mut done, &mut resolved).await?;
    }
    compute_batches(shared, &mut q, &mut resolved).await?;
    Ok(resolved)
}

/// Resolve the doors inside a body of SQL — for the callers that plan
/// authored SQL of their own (`whatif`, `misfit`).
pub(crate) async fn resolve_sql(
    shared: &Arc<Shared>,
    ctx: &SessionContext,
    sql: &str,
) -> Result<Resolved, SessionError> {
    let q = parse(sql, "the body")?;
    let stmt = DFStatement::Statement(Box::new(SQLStatement::Query(Box::new(q))));
    resolve(shared, ctx, &stmt).await
}

#[cfg(test)]
mod body_tests {
    //! What a door's body may be. One query, however it is punctuated;
    //! never two, because only the first would run.

    use super::*;

    fn refusal(sql: &str) -> Option<String> {
        parse(sql, "the body").err().map(|e| e.to_string())
    }

    #[test]
    fn one_query_stands_however_it_is_terminated() {
        for body in [
            "SELECT 1 AS v",
            "SELECT 1 AS v;",
            "SELECT 1 AS v;  ",
            "WITH c AS (SELECT 1 AS v) SELECT * FROM c;",
        ] {
            assert!(refusal(body).is_none(), "{body}: {:?}", refusal(body));
        }
    }

    #[test]
    fn a_second_query_is_refused_rather_than_dropped() {
        // The point of the check: `parse_query` returns the first query
        // and says nothing about the rest, so without this the second
        // statement is silently not run.
        for body in [
            "SELECT 1 AS v; SELECT 2 AS v",
            "SELECT 1 AS v; DROP TABLE t",
            "SELECT 1 AS v;; SELECT 2 AS v;",
        ] {
            let e = refusal(body).unwrap_or_else(|| panic!("{body} was admitted"));
            assert!(e.contains("more than one query"), "{body}: {e}");
            assert!(
                !e.contains("  "),
                "the refusal carries a run of spaces from a broken \
                 string literal: {e}"
            );
        }
    }
}
