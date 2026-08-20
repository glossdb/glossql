//! The shared plane behind the doors: one store, one lake, one script
//! runtime, one cube cache, and the workspace's channels — sessions
//! keyed (actor, dataset). The transports are stateless (the 2026-07-28 MCP
//! revision removed transport sessions), so continuity lives here. A
//! channel's binding is fixed at construction; `USE` selects which
//! channel an actor's statements land on, it never rebinds a session —
//! so channels serving concurrent readers (the app door's frames) hold
//! still under load. It lives in the session crate because every door
//! needs it (serverd's `/mcp` and `/query`, the app door's frame reads).

use std::collections::HashMap;
use std::sync::Arc;

use glossql_glossary::{Actor, Store};
use glossql_parser::{GlossqlParser, Statement};
use tokio::sync::RwLock;

use crate::cube::{CubeCache, DEFAULT_CUBE_CACHE_MB};
use crate::session::{FunctionRuntime, Outcome, Session, SessionError};

pub struct Plane {
    store: Store,
    runtime: Arc<dyn FunctionRuntime>,
    /// The cube cache every channel reads and fills — one set of
    /// entries for the process, bounded by `with_cube_cache`. Not the
    /// Store's: the Store is the record, and the cube is not.
    cube: CubeCache,
    /// One session per (actor, dataset), created on first sight and kept
    /// for the server's life; `None` is the actor's unbound channel —
    /// where they speak before any `USE`.
    channels: RwLock<HashMap<(String, Option<String>), Arc<Session>>>,
    /// The dataset each actor last `USE`d: which channel their next
    /// statement lands on.
    current: RwLock<HashMap<String, Option<String>>>,
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
            channels: RwLock::new(HashMap::new()),
            current: RwLock::new(HashMap::new()),
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

    /// The actor's channel for a dataset, created and bound on first
    /// sight. Doors that know their dataset (the app door reads it from
    /// `app.toml`) come straight here; binding validates the dataset, so
    /// an unknown one fails the channel, not the query after it.
    pub async fn channel(
        &self,
        actor: Actor,
        dataset: Option<&str>,
    ) -> Result<Arc<Session>, SessionError> {
        let key = (Self::key(&actor), dataset.map(str::to_string));
        if let Some(session) = self.channels.read().await.get(&key) {
            return Ok(Arc::clone(session));
        }
        let mut channels = self.channels.write().await;
        if let Some(session) = channels.get(&key) {
            return Ok(Arc::clone(session));
        }
        let session = Session::new(self.store.clone(), actor)?
            .with_row_cap(self.row_cap)
            .with_runtime(Arc::clone(&self.runtime))
            .with_cube_cache(self.cube.clone());
        let session = Arc::new(session);
        if let Some(dataset) = dataset {
            session.bind(dataset).await?;
        }
        channels.insert(key, Arc::clone(&session));
        Ok(session)
    }

    /// The channel the actor's `USE` pointer selects — the unbound one
    /// until they `USE` a dataset. Actor rides the connection (SPEC.md
    /// §1): the doors resolve who is speaking, this resolves where their
    /// statements land.
    pub async fn session(&self, actor: Actor) -> Result<Arc<Session>, SessionError> {
        let dataset = self
            .current
            .read()
            .await
            .get(&Self::key(&actor))
            .cloned()
            .flatten();
        self.channel(actor, dataset.as_deref()).await
    }

    /// The statement loop over an actor's channels: `USE` moves the
    /// pointer (building the channel if it is new — an unknown dataset
    /// refuses here and the pointer stays); everything between `USE`s
    /// runs on the channel the pointer names. A failing statement stops
    /// the sequence, as it does inside a session.
    pub async fn execute(&self, actor: Actor, sql: &str) -> Result<Vec<Outcome>, SessionError> {
        let statements = GlossqlParser::parse_sql(sql)?;
        let total = statements.len();
        // A refusal names its GLOBAL place in the call: runs between
        // `USE`s report local indices, rebased here on the outcomes
        // already standing — one outcome per completed statement.
        let rebase = |e: SessionError, base: usize| -> SessionError {
            if total <= 1 {
                return e;
            }
            let (local, source) = match e {
                SessionError::Sequence { index, source, .. } => (index, source),
                other => (1, Box::new(other)),
            };
            let index = base + local;
            SessionError::Sequence {
                index,
                total,
                context: crate::session::sequence_context(index, total),
                source,
            }
        };
        let mut outcomes = Vec::with_capacity(total);
        let mut run: Vec<Statement> = Vec::new();
        for statement in statements {
            if let Statement::Use(u) = statement {
                if !run.is_empty() {
                    let base = outcomes.len();
                    let session = self.session(actor.clone()).await?;
                    match session.execute_statements(std::mem::take(&mut run)).await {
                        Ok(mut o) => outcomes.append(&mut o),
                        Err(e) => return Err(rebase(e, base)),
                    }
                }
                let name = u.dataset.value;
                if let Err(e) = self.channel(actor.clone(), Some(&name)).await {
                    return Err(rebase(e, outcomes.len()));
                }
                self.current
                    .write()
                    .await
                    .insert(Self::key(&actor), Some(name.clone()));
                outcomes.push(Outcome::Done(format!("USE {name}")));
            } else {
                run.push(statement);
            }
        }
        if !run.is_empty() {
            let base = outcomes.len();
            let session = self.session(actor).await?;
            match session.execute_statements(run).await {
                Ok(mut o) => outcomes.append(&mut o),
                Err(e) => return Err(rebase(e, base)),
            }
        }
        Ok(outcomes)
    }
}
