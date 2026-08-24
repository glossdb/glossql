//! The shared plane behind the doors: one store, one lake, one script
//! runtime, one cube cache. Everything that outlives a call lives here
//! or under the store; everything a call needs beyond that is built for
//! the call and dropped with it.
//!
//! Nothing is keyed by a caller or a connection. A channel's binding is
//! fixed at construction and `USE` selects which channel the statements
//! after it land on, so a channel never rebinds under a concurrent
//! reader. The dataset a call speaks to arrives with the call and `USE`
//! moves it for the rest of *that* call only — a pointer remembering
//! where someone last was would be the one piece of state a restart
//! could lose, and losing it loses whatever the caller said next.
//!
//! It lives in the session crate because every door needs it (serverd's
//! `/mcp` and `/query`, the app door's frame reads).

use std::sync::Arc;

use glossql_glossary::{Actor, Store};
use glossql_parser::{GlossqlParser, Statement};

use crate::cube::{CubeCache, DEFAULT_CUBE_CACHE_MB};
use crate::session::{FunctionRuntime, Outcome, Session, SessionError};

/// A caller a door has verified, carried in the request's extensions
/// so every door reads identity the same way. It lives here rather than
/// in one door because all three resolve it and the app door cannot see
/// the door crate that does the verifying.
///
/// Absent means no token was presented and the server did not insist —
/// each door then falls back to the identity it used before there were
/// tokens.
#[derive(Clone, Debug)]
pub struct Caller(pub Actor);

pub struct Plane {
    store: Store,
    runtime: Arc<dyn FunctionRuntime>,
    /// The cube cache every channel reads and fills — one set of
    /// entries for the process, bounded by `with_cube_cache`. Not the
    /// Store's: the Store is the record, and the cube is not.
    cube: CubeCache,
    row_cap: usize,
}

impl Plane {
    /// The store behind the plane — the doors compose the connect-time
    /// brief from it (counts only, no session).
    pub fn store(&self) -> &Store {
        &self.store
    }

    pub fn new(store: Store, runtime: Arc<dyn FunctionRuntime>) -> Self {
        Plane {
            store,
            runtime,
            cube: CubeCache::new(DEFAULT_CUBE_CACHE_MB),
            row_cap: usize::MAX,
        }
    }

    /// The cap the doors render at, pushed down so the engine is not asked
    /// for rows nobody will read.
    pub fn with_row_cap(mut self, cap: usize) -> Self {
        self.row_cap = cap;
        self
    }

    /// The process-wide byte budget for cubes, in megabytes (serverd's
    /// `--cube-cache`). The `cube` aspect bounds one cube; this bounds
    /// them all. Set before the first channel is built.
    pub fn with_cube_cache(mut self, megabytes: u64) -> Self {
        self.cube = CubeCache::new(megabytes);
        self
    }

    /// The cache itself — for the doors' instruments and the tests that
    /// count builds.
    pub fn cube_cache(&self) -> &CubeCache {
        &self.cube
    }

    /// The workspace's declared datasets — for doors that bind by
    /// convention: an app without a pinned dataset binds to the sole one.
    pub async fn datasets(&self) -> Result<Vec<String>, SessionError> {
        let rows = self.store.relation_rows("datasets").await?;
        Ok(rows
            .into_iter()
            .filter_map(|row| row.into_iter().next().flatten())
            .collect())
    }

    /// A channel onto a dataset, built for this call and dropped with
    /// it. Every door comes here with the dataset it was told;
    /// binding validates it, so an unknown one fails the channel rather
    /// than the query after it.
    ///
    /// Nothing is pooled. A channel is a DataFusion context, its
    /// function registry and the mounted catalog — measured at ~0.5 ms
    /// to build, because everything expensive behind it is held
    /// elsewhere: the lake's provider is shared, and the store's
    /// resolved rows are held by the store under its own version. There
    /// is no per-caller state left to keep, so there is no pool to
    /// bound, evict, or key.
    pub async fn channel(
        &self,
        actor: Actor,
        dataset: Option<&str>,
    ) -> Result<Arc<Session>, SessionError> {
        let session = Session::new(self.store.clone(), actor)?
            .with_row_cap(self.row_cap)
            .with_runtime(Arc::clone(&self.runtime))
            .with_cube_cache(self.cube.clone());
        if let Some(dataset) = dataset {
            session.bind(dataset).await?;
        }
        Ok(Arc::new(session))
    }

    /// The statement loop over an actor's channels. `dataset` is where
    /// the call arrives — the URL's first segment — and a `USE` inside
    /// it moves the statements after it onto another channel (an
    /// unknown dataset refuses there, and nothing has moved). The move
    /// lives as long as this call and no longer: the next call arrives
    /// on its own URL again.
    ///
    /// A failing statement stops the sequence, as it does inside a
    /// session.
    pub async fn execute(
        &self,
        actor: Actor,
        dataset: Option<&str>,
        sql: &str,
    ) -> Result<Vec<Outcome>, SessionError> {
        let statements = GlossqlParser::parse_sql(sql)?;
        let total = statements.len();
        // A refusal names its GLOBAL place in the call: runs between
        // `USE`s report local indices, rebased here on the outcomes
        // already standing — one outcome per completed statement, and
        // those outcomes ride the refusal.
        let rebase = |e: SessionError, mut standing: Vec<Outcome>| -> SessionError {
            if total <= 1 {
                return e;
            }
            let base = standing.len();
            let (local, source, mut landed) = match e {
                SessionError::Sequence {
                    index,
                    source,
                    landed,
                    ..
                } => (index, source, landed),
                other => (1, Box::new(other), Vec::new()),
            };
            standing.append(&mut landed);
            let index = base + local;
            SessionError::Sequence {
                index,
                total,
                context: crate::session::sequence_context(index, total),
                source,
                landed: standing,
            }
        };
        let mut outcomes = Vec::with_capacity(total);
        let mut run: Vec<Statement> = Vec::new();
        let mut on = dataset.map(str::to_string);
        for statement in statements {
            if let Statement::Use(u) = statement {
                if !run.is_empty() {
                    let session = self.channel(actor.clone(), on.as_deref()).await?;
                    match session.execute_statements(std::mem::take(&mut run)).await {
                        Ok(mut o) => outcomes.append(&mut o),
                        Err(e) => return Err(rebase(e, std::mem::take(&mut outcomes))),
                    }
                }
                let name = u.dataset.value;
                if let Err(e) = self.channel(actor.clone(), Some(&name)).await {
                    return Err(rebase(e, std::mem::take(&mut outcomes)));
                }
                on = Some(name.clone());
                outcomes.push(Outcome::Done(format!("USE {name}")));
            } else {
                run.push(statement);
            }
        }
        if !run.is_empty() {
            let session = self.channel(actor, on.as_deref()).await?;
            match session.execute_statements(run).await {
                Ok(mut o) => outcomes.append(&mut o),
                Err(e) => return Err(rebase(e, std::mem::take(&mut outcomes))),
            }
        }
        Ok(outcomes)
    }
}
