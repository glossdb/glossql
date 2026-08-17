//! The async pre-pass: resolve every door a statement names *before* the
//! planner runs, so planning is sync over an AST with nothing left to
//! fetch.
//!
//! This is stage 2 of `reports/2026-08-17-the-foundation.md` §6, and it
//! copies DataFusion's own shape: `statement_to_plan` walks a statement
//! for table references, awaits each one through the catalog into a map,
//! and only then runs the sync `SqlToRel`. We did the opposite — a sync
//! `RelationPlanner` reaching back into an async store through
//! `block_in_place` — and paid for it three ways:
//!
//! - a blocked planner thread, which the guide names as the pitfall;
//! - re-entrancy, since expansion re-planned through the same context,
//!   which needed a `thread_local` stack to notice a cycle;
//! - one stack for the whole nesting, which datafusion 54 overflowed.
//!
//! Resolution is depth-first and carries its path, so the cycle check is
//! the traversal rather than a mechanism. A door reached twice on
//! different branches is not on one path and resolves once.

use std::collections::{HashMap, HashSet};
use std::ops::ControlFlow;
use std::sync::Arc;

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::TableProvider;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use datafusion::sql::parser::Statement as DFStatement;
use datafusion::sql::sqlparser::ast::{
    Query, Statement as SQLStatement, TableFactor, VisitMut, VisitorMut,
};
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion::sql::sqlparser::parser::Parser;

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
}

impl Resolved {
    pub(crate) fn plan(&self, key: &str) -> Option<Arc<LogicalPlan>> {
        self.plans.get(key).cloned()
    }

    pub(crate) fn pin(&self, table: &str) -> Option<Arc<dyn TableProvider>> {
        self.pins.get(table).cloned()
    }

    pub(crate) fn batch(&self, key: &str) -> Option<&RecordBatch> {
        self.batches.get(key)
    }
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
            Door::Replay(kind, n) => format!("{kind}.{n}"),
        }
    }

    fn what(&self) -> String {
        match self {
            Door::Serve(a) => format!("the grounding for `{a}` (read.{a}())"),
            Door::Shipped(n) => format!("the shipped read `{n}`"),
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
fn doors_in(q: &mut Query) -> Vec<Door> {
    struct Collect(Vec<Door>);
    impl VisitorMut for Collect {
        type Break = ();
        fn pre_visit_table_factor(&mut self, f: &mut TableFactor) -> ControlFlow<()> {
            if let TableFactor::Table { name, args, .. } = f {
                let parts: Vec<String> = name.0.iter().map(|p| p.to_string()).collect();
                match parts.as_slice() {
                    [prefix, aspect] if prefix.eq_ignore_ascii_case("read") => {
                        self.0.push(Door::Serve(aspect.clone()));
                    }
                    [prefix, name] if prefix.eq_ignore_ascii_case("misfit") => {
                        self.0.push(Door::Replay("misfit", name.clone()));
                    }
                    [prefix, name] if prefix.eq_ignore_ascii_case("whatif") => {
                        self.0.push(Door::Replay("whatif", name.clone()));
                    }
                    // A bare name with no arguments, if we ship a read of
                    // that name. Anything else falls through untouched.
                    [name] if args.is_none() && crate::library::read_sql(name).is_some() => {
                        self.0.push(Door::Shipped(name.clone()));
                    }
                    _ => {}
                }
            }
            ControlFlow::Continue(())
        }
    }
    let mut c = Collect(Vec::new());
    let _ = q.visit(&mut c);
    c.0.sort_by_key(Door::key);
    c.0.dedup();
    c.0
}

fn parse(sql: &str, what: &str) -> Result<Query, SessionError> {
    Parser::new(&PostgreSqlDialect {})
        .try_with_sql(sql)
        .and_then(|mut p| p.parse_query())
        .map(|q| *q)
        .map_err(|e| SessionError::BadSubject(format!("{what} does not parse: {e}")))
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
    }
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
    for factor in factors_in(q) {
        let key = factor.to_string();
        if resolved.batches.contains_key(&key) {
            continue;
        }
        if let Some(batch) = crate::reads::compute_batch(shared, &factor).await? {
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
    for child in doors_in(&mut body) {
        Box::pin(resolve_door(shared, ctx, child, path, done, resolved)).await?;
    }
    path.pop();

    if !door.planned() {
        done.insert(key);
        return Ok(());
    }
    // Planned with everything it depends on already resolved — nested
    // doors, compute batches and pinned tables all in the map — so the
    // sync planner finds instead of fetching.
    Box::pin(compute_batches(shared, &mut body, resolved)).await?;
    let state = crate::reads::state_with(ctx, shared, resolved.clone());
    let stmt = DFStatement::Statement(Box::new(SQLStatement::Query(Box::new(body))));
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
    let mut resolved = Resolved {
        pins: shared.statement_pins().await?,
        ..Resolved::default()
    };
    let mut done = HashSet::new();
    let mut path = Vec::new();
    for door in doors_in(&mut q) {
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
