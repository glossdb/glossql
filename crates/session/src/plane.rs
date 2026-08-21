//! The shared plane behind the doors: one store, one lake, one script
//! runtime, one cube cache, and the workspace's channels — sessions
//! keyed (actor, dataset). A channel's binding is fixed at
//! construction; `USE` selects which channel the statements after it
//! land on, it never rebinds a session — so channels serving concurrent
//! readers (the app door's frames) hold still under load.
//!
//! Nothing here is keyed by a connection. The dataset a call speaks to
//! arrives with the call — the URL's first segment on every door — and
//! `USE` moves it for the rest of *that* call only. The channels cache
//! is a pool of bound sessions, rebuildable at any moment and holding no
//! caller's intent; a pointer remembering where someone last was would
//! be the one piece of state a restart could lose, and losing it loses
//! whatever the caller said next.
//!
//! It lives in the session crate because every door needs it (serverd's
//! `/mcp` and `/query`, the app door's frame reads).

use std::sync::Arc;

use glossql_glossary::{Actor, Store};
use glossql_parser::{GlossqlParser, Statement};

use crate::cube::{CubeCache, DEFAULT_CUBE_CACHE_MB};
use crate::session::{FunctionRuntime, Outcome, Session, SessionError};

/// How many channels the pool holds before it evicts the least recently
/// used. Measured at ~84 kB of resident memory each — bound or unbound,
/// since the lake's provider is shared — so this is a ceiling of roughly
/// 90 MB.
///
/// It is a count rather than a byte budget because channels are uniform,
/// unlike cubes. The bound exists because the key's actor half is not
/// bounded by anything the server controls: with a token it is the
/// subject, but a door serving untokened calls takes the client's own
/// name for it, and a long-lived process would otherwise grow one
/// channel per name it was ever handed.
///
/// Evicting one costs a cold DataFusion context on the next call and
/// nothing else — a channel is a cache in front of the store, never a
/// record, and every read re-derives.
pub const MAX_CHANNELS: u64 = 1024;

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
    /// One session per (actor, dataset), built on first sight and held
    /// until it is the least recently used of [`MAX_CHANNELS`]; `None`
    /// is the unbound channel — a workspace-wide read that names no
    /// dataset. A caller holding an `Arc` keeps its session alive across
    /// its own eviction.
    channels: moka::future::Cache<(String, Option<String>), Arc<Session>>,
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
            channels: moka::future::Cache::builder()
                .max_capacity(MAX_CHANNELS)
                .eviction_policy(moka::policy::EvictionPolicy::lru())
                .build(),
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

    fn key(actor: &Actor) -> String {
        format!("{}:{}", actor.kind, actor.id)
    }

    /// The actor's channel for a dataset, built and bound on first
    /// sight. Every door comes here with the dataset its URL named;
    /// binding validates it, so an unknown one fails the channel rather
    /// than the query after it.
    ///
    /// Building is single-flight per key (`try_get_with`): concurrent
    /// first calls for one channel build it once and share it, and a
    /// build that refuses is not cached — the next call tries again
    /// rather than inheriting a stale refusal.
    pub async fn channel(
        &self,
        actor: Actor,
        dataset: Option<&str>,
    ) -> Result<Arc<Session>, SessionError> {
        let key = (Self::key(&actor), dataset.map(str::to_string));
        self.channels
            .try_get_with(key, async {
                let session = Session::new(self.store.clone(), actor)?
                    .with_row_cap(self.row_cap)
                    .with_runtime(Arc::clone(&self.runtime))
                    .with_cube_cache(self.cube.clone());
                if let Some(dataset) = dataset {
                    session.bind(dataset).await?;
                }
                Ok::<_, SessionError>(Arc::new(session))
            })
            .await
            // `try_get_with` hands the initializer's error back behind
            // an Arc, so every waiter on that flight sees the same one.
            .map_err(|e| SessionError::ChannelRefused(e.to_string()))
    }

    /// Channels standing right now, once moka's pending evictions have
    /// run — for the doors' instruments and the tests that count them.
    pub async fn channel_count(&self) -> u64 {
        self.channels.run_pending_tasks().await;
        self.channels.entry_count()
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
