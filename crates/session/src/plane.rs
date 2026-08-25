//! The shared plane behind the doors: one store, one lake, one script
//! runtime, one cube cache, one engine runtime. Everything that outlives
//! a call lives here or under the store; everything a call needs beyond
//! that is built for the call and dropped with it.
//!
//! The engine runtime is here for that reason and not by preference. A
//! channel is built per call, so anything a channel builds for itself is
//! per-call — and a memory pool that is per-call is not a budget. What
//! bounds the process has to outlive the call, which means it lives
//! here and every channel is handed it.
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

use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};

use glossql_glossary::{Actor, Store};
use glossql_parser::{GlossqlParser, Statement};

use crate::cube::{CubeCache, DEFAULT_CUBE_CACHE_MB};
use crate::session::{FunctionRuntime, Outcome, Session, SessionError};

/// The engine's memory ceiling for the whole process, in megabytes
/// (serverd's `--memory-limit`).
///
/// Large but not the machine: a plan that would exceed it is refused by
/// name, which is the trade this default is chosen for — a container
/// killed for its memory says nothing about which query did it.
///
/// It does not cover the cube cache, which holds its own bytes outside
/// the engine under [`DEFAULT_CUBE_CACHE_MB`]. The two are separate
/// budgets and a deployment is sized for their sum.
pub const DEFAULT_MEMORY_LIMIT_MB: u64 = 4096;

/// The execution runtime every channel is built on.
///
/// Three decisions, and each is a decision rather than a default:
///
/// **A bounded pool.** DataFusion's own default is unbounded, so without
/// this nothing in the server has a memory ceiling at all. The pool
/// tracks its largest consumers, so the refusal names what was holding
/// the memory. DataFusion does not yet account for every operator, so
/// this is a ceiling on what the engine knows it is holding, not on the
/// process.
///
/// **No disk manager.** Nothing spills; a plan that outgrows the pool is
/// refused instead. Spilling is worth having when a temp directory is a
/// real disk, and in a container it is another way to run out of room —
/// with a refusal traded for a slower failure somewhere else.
///
/// **No file caches.** The three DataFusion keeps are on by default and
/// a limit of zero is how they are turned off. The one that decides it
/// is the list-files cache: its TTL defaults to infinite, and the
/// directories it would cache are the source globs a re-import is
/// re-reading precisely because they changed. A cache that cannot see a
/// new file is a wrong answer, not a slow one.
fn runtime_env(megabytes: u64) -> Arc<RuntimeEnv> {
    Arc::new(
        RuntimeEnvBuilder::new()
            .with_memory_limit((megabytes as usize) * 1024 * 1024, 1.0)
            .with_disk_manager_builder(
                DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled),
            )
            .with_metadata_cache_limit(0)
            .with_object_list_cache_limit(0)
            .with_file_statistics_cache_limit(0)
            // The two things `build` can refuse are a temp directory it
            // cannot make and a cache it cannot size. Disabled makes no
            // directory and a zero limit sizes nothing, so neither is
            // reachable from here.
            .build()
            .expect("a disabled disk manager and empty caches"),
    )
}

/// A caller the gate has verified, carried in the request's extensions
/// so every door reads identity the same way. It lives here rather than
/// in one door because all three resolve it and the app door cannot see
/// the door crate that does the verifying.
///
/// The id is the token's subject; the kind is the door's — the gate
/// stamps agent on the agent door and human on the others. Every door
/// is behind the gate, so a request that reaches one carries this.
#[derive(Clone, Debug)]
pub struct Caller(pub Actor);

pub struct Plane {
    store: Store,
    runtime: Arc<dyn FunctionRuntime>,
    /// The cube cache every channel reads and fills — one set of
    /// entries for the process, bounded by `with_cube_cache`. Not the
    /// Store's: the Store is the record, and the cube is not.
    cube: CubeCache,
    /// The engine runtime every channel is built on — one memory pool,
    /// one disk manager, one set of file caches for the process. It is
    /// held here and not built per channel because a channel is built
    /// per call: a pool each call carries its own of bounds one call.
    env: Arc<RuntimeEnv>,
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
            env: runtime_env(DEFAULT_MEMORY_LIMIT_MB),
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

    /// The engine's memory ceiling for the whole process, in megabytes
    /// (serverd's `--memory-limit`). Separate from the cube cache, which
    /// holds its bytes outside the engine. Set before the first channel
    /// is built — a channel keeps the runtime it was handed.
    pub fn with_memory_limit(mut self, megabytes: u64) -> Self {
        self.env = runtime_env(megabytes);
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
        let session = Session::on_runtime(self.store.clone(), actor, Arc::clone(&self.env))?
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
