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
//! - one stack for the whole nesting, which a deep nesting overflows.
//!
//! Resolution is depth-first and carries its path, so the cycle check is
//! the traversal rather than a mechanism. A door reached twice on
//! different branches is not on one path and resolves once.

use std::collections::{HashMap, HashSet};
use std::ops::ControlFlow;
use std::sync::Arc;

use datafusion::catalog::view::ViewTable;
use datafusion::datasource::provider_as_source;
use datafusion::logical_expr::{LogicalPlanBuilder, ident};
use datafusion::prelude::SessionContext;
use datafusion::sql::parser::Statement as DFStatement;
use datafusion::sql::planner::IdentNormalizer;
use datafusion::sql::resolve::resolve_table_references;
use datafusion::sql::sqlparser::ast::{
    Expr, Ident, Query, SelectItem, SetExpr, Statement as SQLStatement, TableFactor, VisitMut,
    VisitorMut,
};
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::sqlparser::tokenizer::Token;

use crate::reads::served_grounding;
use crate::shared::{Resolved, Served, SessionError, Shared};

/// A door reference this pass knows how to resolve ahead of planning:
/// something whose body is SQL. Compute doors build batches and are
/// the compute pass's ([`crate::reads::compute_batch`]).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Door {
    /// `read.<aspect>()`, or the aspect's bare name — a QUERY
    /// grounding: its definition, as the catalog's view under the
    /// metric's name carries it.
    Serve(String),
    /// A shipped read (`crates/session/reads/*.sql`) — SQL from the binary.
    Shipped(String),
    /// `subject_column('table.column')` — the named column of a pinned
    /// table, projected as `v`. The primitive a column-grain measurement
    /// body stands on: the body is declared once, the subject varies per
    /// extraction, so the body cannot name the column itself.
    /// No SQL behind it — the plan is built here.
    Column(String),
    /// `misfit.<frame>()`. The compute pass
    /// builds their batch, so this pass does not plan them — it walks
    /// the body they replay, for the path. That is the whole cycle
    /// guard: a frame whose SQL names its own door is a repeat on the
    /// path, never a stack overflow.
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
fn doors_in(idents: &IdentNormalizer, q: &mut Query, views: &HashMap<String, String>) -> Vec<Door> {
    struct Collect<'a>(Vec<Door>, &'a IdentNormalizer, &'a HashMap<String, String>);
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
                    // A bare name with no arguments, if we ship a read of
                    // that name. Anything else falls through untouched.
                    [name] if args.is_none() && crate::library::read_sql(name).is_some() => {
                        self.0.push(Door::Shipped(name.clone()));
                    }
                    // A grounding's view of the bound dataset, bare or
                    // under its dataset: the grounding, planned under
                    // the serve door's key.
                    [name] if args.is_none() && self.2.contains_key(name) => {
                        self.0.push(Door::Serve(name.clone()));
                    }
                    [_, name] if args.is_none() && self.2.contains_key(name) => {
                        self.0.push(Door::Serve(name.clone()));
                    }
                    _ => {
                        // The malformed-argument case is left uncollected
                        // on purpose: the planner meets the factor, calls
                        // the same reader, and reports it.
                        if let Some(Ok(subject)) = crate::subject::subject_column_arg(self.1, f) {
                            self.0.push(Door::Column(subject));
                        }
                    }
                }
            }
            ControlFlow::Continue(())
        }
    }
    let mut c = Collect(Vec::new(), idents, views);
    let _ = q.visit(&mut c);
    c.0.sort_by_key(Door::key);
    c.0.dedup();
    c.0
}

pub(crate) fn parse(sql: &str, what: &str) -> Result<Query, SessionError> {
    let mut parser = Parser::new(&PostgreSqlDialect {})
        .try_with_sql(sql)
        .map_err(|e| SessionError::shape(what, format!("{what} does not parse: {e}")))?;
    let query = parser
        .parse_query()
        .map(|q| *q)
        .map_err(|e| SessionError::shape(what, format!("{what} does not parse: {e}")))?;
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
        return Err(SessionError::shape(
            what,
            format!(
                "{what} is more than one query — a door serves one, and \
             what follows it would not run"
            ),
        ));
    }
    Ok(query)
}

/// The body behind a door: the catalog's view, the binary, or nothing.
async fn body_of(
    shared: &Shared,
    resolved: &Resolved,
    door: &Door,
) -> Result<String, SessionError> {
    match door {
        Door::Serve(aspect) => served_grounding(shared, &resolved.views, aspect).await,
        Door::Shipped(name) => crate::library::read_sql(name)
            .map(str::to_string)
            .ok_or_else(|| SessionError::unknown(name, format!("no shipped read `{name}`"))),
        // Both replay a declared grounding; a body that is not SQL names
        // no doors and walks to nothing.
        Door::Replay(_, name) => Ok(served_grounding(shared, &resolved.views, name)
            .await
            .unwrap_or_default()),
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
    for factor in &factors_in(q) {
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
            return Err(SessionError::shape(
                format!("{t}.{c}"),
                format!(
                    "`{t}.{c}` is a subject, not a table: an extraction \
                 (SELECT fn() FROM {t}.{c}) is its own statement — run it, then \
                 compose its measurement through GLOSSARY({t}.{c}::aspect)",
                    t = t.value,
                    c = c.value
                ),
            ));
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
    for factor in &factors_in(q) {
        let key = factor.to_string();
        if resolved.batches.contains_key(&key) || resolved.shadowed(&idents, factor) {
            continue;
        }
        if let Some(batch) = crate::doors::compute_batch(shared, factor, resolved).await? {
            if crate::doors::reads_the_record(&idents, factor) {
                resolved.record = true;
            }
            crate::doors::door_reads(&idents, factor, &mut resolved.reads);
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
        return Err(SessionError::shape(
            key.clone(),
            format!("read cycle: {} -> {key}", path.join(" -> ")),
        ));
    }
    if done.contains(&key) {
        return Ok(());
    }
    // A served grounding and a replayed body both come from the
    // glossary — the statement's answer can change under a glossary
    // write, whatever the body then reads. The body's own scans reach
    // the measurement record through the executed plan; the grounding
    // itself is a glossary read.
    if matches!(door, Door::Serve(_) | Door::Replay(..)) {
        resolved.record = true;
        resolved.reads.relations.insert("glossary".into());
        resolved.reads.relations.insert("aspects".into());
    }
    // The column door has no SQL behind it: one projection of a pinned
    // table, aliased `v`, built right here.
    if let Door::Column(subject) = &door {
        let Some((table, column)) = subject.split_once('.') else {
            return Err(SessionError::shape(
                subject,
                format!("subject_column wants 'table.column', got '{subject}'"),
            ));
        };
        let provider = resolved.pins.get(table).cloned().ok_or_else(|| {
            SessionError::unknown(
                table,
                format!("subject_column: no table `{table}` in the bound dataset"),
            )
        })?;
        let plan = LogicalPlanBuilder::scan(table, provider_as_source(provider), None)
            .and_then(|b| b.project(vec![ident(column).alias("v")]))
            .and_then(|b| b.build())
            .map_err(|e| {
                SessionError::unknown(subject, format!("subject_column('{subject}'): {e}"))
            })?;
        done.insert(key.clone());
        resolved.plans.insert(key, Arc::new(plan));
        return Ok(());
    }
    // A shipped read served from memory: the rows one run at this
    // version and pin produced, planned as a table. The bound dataset
    // keys it; unbound, the read expands like any other and refuses
    // wherever it would have.
    let bound = shared.dataset();
    if let Door::Shipped(name) = &door
        && crate::memo::memoized(name)
        && let Some(dataset) = bound
    {
        let memo = Box::pin(memo_shipped(
            shared, ctx, &door, &dataset, path, done, resolved,
        ))
        .await?;
        let provider = datafusion::datasource::MemTable::try_new(
            memo.served.schema.clone(),
            vec![memo.served.partitions.clone()],
        )?;
        let plan =
            LogicalPlanBuilder::scan(name.as_str(), provider_as_source(Arc::new(provider)), None)?
                .build()?;
        resolved.record |= memo.record;
        resolved.reads.extend(&memo.reads);
        done.insert(key.clone());
        resolved.plans.insert(key, Arc::new(plan));
        return Ok(());
    }
    expand(shared, ctx, door, path, done, resolved).await
}

/// One run of a memoized shipped read, or the entry a run at this key
/// already left. The run expands the read as any door is expanded —
/// into a scratch copy of the resolution, so a hit leaves nothing of
/// the body's own doors behind — and executes the plan to its rows.
async fn memo_shipped(
    shared: &Arc<Shared>,
    ctx: &SessionContext,
    door: &Door,
    dataset: &str,
    path: &[String],
    done: &HashSet<String>,
    resolved: &Resolved,
) -> Result<Arc<crate::memo::Memo>, SessionError> {
    let Door::Shipped(name) = door else {
        unreachable!("only a shipped read is memoized");
    };
    let rctx = shared.read_context_for(dataset).await?;
    let key = crate::memo::MemoKey {
        read: name.clone(),
        dataset: dataset.to_string(),
        version: rctx.version.clone(),
        pin: rctx.pin.text.clone(),
    };
    let span = tracing::info_span!("memo", read = %name);
    let cache = shared.shipped();
    tracing::Instrument::instrument(
        cache.get_or_run(key, || async {
            let mut scratch = resolved.clone();
            let mut path = path.to_vec();
            let mut done = done.clone();
            Box::pin(expand(
                shared,
                ctx,
                door.clone(),
                &mut path,
                &mut done,
                &mut scratch,
            ))
            .await?;
            let plan = scratch.plan(&door.key()).ok_or_else(|| {
                SessionError::shape(door.what(), format!("{} was not planned", door.what()))
            })?;
            let frame = ctx.execute_logical_plan((*plan).clone()).await?;
            let physical = crate::execution::physical(frame).await?;
            let schema = physical.schema();
            let partitions = datafusion::physical_plan::collect(physical, ctx.task_ctx()).await?;
            Ok(crate::memo::Memo {
                served: Served { schema, partitions },
                record: scratch.record,
                reads: scratch.reads,
            })
        }),
        span,
    )
    .await
}

/// The door expanded through its SQL body: nested doors resolved first,
/// then the body planned with everything it depends on in the map.
async fn expand(
    shared: &Arc<Shared>,
    ctx: &SessionContext,
    door: Door,
    path: &mut Vec<String>,
    done: &mut HashSet<String>,
    resolved: &mut Resolved,
) -> Result<(), SessionError> {
    let key = door.key();
    let sql = body_of(shared, resolved, &door).await?;
    // A replayed body that does not parse names no doors, so there is
    // nothing to walk; the door refuses it itself when it is read.
    let mut body = match parse(&sql, &door.what()) {
        Ok(q) => q,
        Err(e) if door.planned() => return Err(e),
        Err(_) => {
            done.insert(key);
            return Ok(());
        }
    };

    path.push(key.clone());
    let views = resolved.views.clone();
    for child in doors_in(&shared.idents(), &mut body, &views) {
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
    // scope, not the outer statement's; its columns are read by the
    // same spelling rule as the statement's.
    respell_columns(&mut body, resolved, &shared.idents())?;
    Box::pin(compute_batches(shared, &mut body, resolved)).await?;
    let stmt = DFStatement::Statement(Box::new(SQLStatement::Query(Box::new(body))));
    let mut scoped = resolved.clone();
    scoped.ctes = resolve_table_references(&stmt, shared.normalize_idents)?
        .1
        .iter()
        .map(|c| c.table().to_string())
        .filter(|c| crate::library::read_sql(c).is_none())
        .collect();
    let state = crate::reads::state_with(ctx, shared, scoped);
    let plan = state
        .statement_to_plan(stmt)
        .await
        .map_err(|e| SessionError::Engine {
            context: format!("not served: {}: ", door.what()),
            source: e,
        })?;

    done.insert(key.clone());
    match door {
        // A grounding is a view: DataFusion's own provider for a plan
        // served under a name, which the builder inlines wherever the
        // name is read.
        Door::Serve(name) => {
            resolved
                .served
                .insert(name, Arc::new(ViewTable::new(plan, Some(sql))));
        }
        _ => {
            resolved.plans.insert(key, Arc::new(plan));
        }
    }
    Ok(())
}

/// Resolve everything the statement needs ahead of planning: the pin,
/// the doors, the compute batches.
pub(crate) async fn resolve(
    shared: &Arc<Shared>,
    ctx: &SessionContext,
    statement: &mut DFStatement,
) -> Result<Resolved, SessionError> {
    if !matches!(statement, DFStatement::Statement(inner) if matches!(inner.as_ref(), SQLStatement::Query(_)))
    {
        return Ok(Resolved::default());
    }
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
    let (relations, ctes) = resolve_table_references(statement, shared.normalize_idents)?;
    // A statement that names only its dataset's own tables loads those;
    // any other reads the dataset whole.
    let pins = match shared.named_pins(&relations).await? {
        Some(named) => named,
        None => shared.statement_pins().await?,
    };
    // A shipped read's name stays reserved over a CTE of that name.
    let mut resolved = Resolved {
        pins,
        ctes: ctes
            .iter()
            .map(|c| c.table().to_string())
            .filter(|c| crate::library::read_sql(c).is_none())
            .collect(),
        views: shared.views().await?,
        ..Resolved::default()
    };
    let DFStatement::Statement(inner) = statement else {
        unreachable!("matched above");
    };
    let SQLStatement::Query(q) = inner.as_mut() else {
        unreachable!("matched above");
    };
    let q: &mut Query = q.as_mut();
    // The statement itself is what the planner reads, so a column
    // respelled here is the column it plans.
    respell_columns(q, &resolved, &idents)?;
    refuse_subject_relations(q, &resolved)?;
    let mut done = HashSet::new();
    let mut path = Vec::new();
    let views = resolved.views.clone();
    for door in doors_in(&idents, q, &views) {
        resolve_door(shared, ctx, door, &mut path, &mut done, &mut resolved).await?;
    }
    compute_batches(shared, q, &mut resolved).await?;
    Ok(resolved)
}

/// What the statement binds and reads, for [`respell_columns`]: which
/// names qualify a pinned table's columns (the table's own name and
/// each alias of it), which names the statement defines itself (CTEs,
/// alias column lists, SELECT aliases), and whether every relation it
/// reads is a pinned table or a CTE.
#[derive(Default)]
struct Scope {
    qualifiers: HashMap<String, String>,
    defined: HashSet<String>,
    only_pins: bool,
}

/// A column reached by the export's spelling (SPEC.md §1). A table is
/// named here, by the recipe that lands it, and folds as declared; a
/// column arrives spelled as the export spelled it, so an unquoted
/// name that folds to no column of the table but differs from exactly
/// one column's spelling only by case is rewritten to that spelling,
/// quoted, before the planner reads it. Two spellings that both match
/// are a refusal naming them. Nothing the statement defines itself is
/// touched, and an unqualified name is respelled only when every
/// relation the statement reads is a pinned table or a CTE — a shipped
/// read's `value` beside a landed `Value` stays the read's. The
/// planner's own miss ("No field named …") still answers a name that
/// matches nothing.
pub(crate) fn respell_columns(
    q: &mut Query,
    resolved: &Resolved,
    idents: &IdentNormalizer,
) -> Result<(), SessionError> {
    struct Survey<'a> {
        scope: Scope,
        idents: &'a IdentNormalizer,
        resolved: &'a Resolved,
    }
    impl Survey<'_> {
        fn define(&mut self, ident: &Ident) {
            self.scope
                .defined
                .insert(self.idents.normalize(ident.clone()));
        }
        fn aliases_in(&mut self, body: &SetExpr) {
            match body {
                SetExpr::Select(s) => {
                    for item in &s.projection {
                        if let SelectItem::ExprWithAlias { alias, .. } = item {
                            self.define(alias);
                        }
                    }
                }
                SetExpr::SetOperation { left, right, .. } => {
                    self.aliases_in(left);
                    self.aliases_in(right);
                }
                // A nested query is visited as one.
                _ => {}
            }
        }
    }
    impl VisitorMut for Survey<'_> {
        type Break = ();
        fn pre_visit_query(&mut self, q: &mut Query) -> ControlFlow<()> {
            if let Some(with) = &q.with {
                for cte in &with.cte_tables {
                    self.define(&cte.alias.name);
                    for c in &cte.alias.columns {
                        self.define(&c.name);
                    }
                }
            }
            self.aliases_in(&q.body);
            ControlFlow::Continue(())
        }
        fn pre_visit_table_factor(&mut self, f: &mut TableFactor) -> ControlFlow<()> {
            let cte = self.resolved.shadowed(self.idents, f);
            match f {
                TableFactor::Table {
                    name,
                    alias,
                    args: None,
                    ..
                } => {
                    let table = match name.0.as_slice() {
                        [part] => part.as_ident().map(|i| self.idents.normalize(i.clone())),
                        _ => None,
                    };
                    match table {
                        Some(_) if cte => {}
                        Some(t)
                            if crate::library::read_sql(&t).is_none()
                                && self.resolved.pins.contains_key(&t) =>
                        {
                            self.scope.qualifiers.insert(t.clone(), t.clone());
                            if let Some(a) = alias {
                                self.scope
                                    .qualifiers
                                    .insert(self.idents.normalize(a.name.clone()), t);
                            }
                        }
                        _ => self.scope.only_pins = false,
                    }
                    if let Some(a) = alias {
                        for c in &a.columns {
                            self.define(&c.name);
                        }
                    }
                }
                TableFactor::Derived { alias, .. } => {
                    if let Some(a) = alias {
                        for c in &a.columns {
                            self.define(&c.name);
                        }
                    }
                }
                _ => self.scope.only_pins = false,
            }
            ControlFlow::Continue(())
        }
    }
    let mut survey = Survey {
        scope: Scope {
            only_pins: true,
            ..Scope::default()
        },
        idents,
        resolved,
    };
    let _ = q.visit(&mut survey);
    let scope = survey.scope;
    if scope.qualifiers.is_empty() {
        return Ok(());
    }

    let columns: HashMap<String, Vec<String>> = scope
        .qualifiers
        .values()
        .filter_map(|t| {
            let pin = resolved.pins.get(t)?;
            let names = pin
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect();
            Some((t.clone(), names))
        })
        .collect();
    let all: Vec<String> = columns.values().flatten().cloned().collect();

    struct Respell<'a> {
        scope: &'a Scope,
        columns: &'a HashMap<String, Vec<String>>,
        all: &'a [String],
        idents: &'a IdentNormalizer,
    }
    impl Respell<'_> {
        fn respell(&self, id: &mut Ident, table: Option<&str>) -> ControlFlow<SessionError> {
            if id.quote_style.is_some() {
                return ControlFlow::Continue(());
            }
            let folded = self.idents.normalize(id.clone());
            let spellings: &[String] = match table {
                Some(t) => self.columns.get(t).map(Vec::as_slice).unwrap_or(&[]),
                None => self.all,
            };
            if spellings.contains(&folded)
                || (table.is_none() && self.scope.defined.contains(&folded))
            {
                return ControlFlow::Continue(());
            }
            let mut matches: Vec<&String> = spellings
                .iter()
                .filter(|s| s.eq_ignore_ascii_case(&id.value))
                .collect();
            matches.sort();
            matches.dedup();
            match matches.as_slice() {
                [] => ControlFlow::Continue(()),
                [one] => {
                    id.value = (*one).clone();
                    id.quote_style = Some('"');
                    ControlFlow::Continue(())
                }
                many => ControlFlow::Break(SessionError::AmbiguousName(format!(
                    "`{}` is no landed column as written, and {} differ from it only by case: {} — quote the one you mean",
                    id.value,
                    many.len(),
                    many.iter()
                        .map(|m| format!("\"{m}\""))
                        .collect::<Vec<_>>()
                        .join(", ")
                ))),
            }
        }
    }
    impl VisitorMut for Respell<'_> {
        type Break = SessionError;
        fn pre_visit_expr(&mut self, e: &mut Expr) -> ControlFlow<SessionError> {
            match e {
                Expr::Identifier(id) if self.scope.only_pins => self.respell(id, None),
                Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
                    let qualifier = self.idents.normalize(parts[0].clone());
                    match self.scope.qualifiers.get(&qualifier).cloned() {
                        Some(table) => self.respell(&mut parts[1], Some(&table)),
                        None => ControlFlow::Continue(()),
                    }
                }
                _ => ControlFlow::Continue(()),
            }
        }
    }
    let mut respell = Respell {
        scope: &scope,
        columns: &columns,
        all: &all,
        idents,
    };
    match q.visit(&mut respell) {
        ControlFlow::Break(e) => Err(e),
        ControlFlow::Continue(()) => Ok(()),
    }
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
