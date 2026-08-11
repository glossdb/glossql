//! The shared plane behind the doors: one store, one lake, one script
//! runtime, and the workspace's channels — sessions keyed
//! (actor, dataset). The transports are stateless (the 2026-07-28 MCP
//! revision removed transport sessions), so continuity lives here. A
//! channel's binding is fixed at construction; `USE` selects which
//! channel an actor's statements land on, it never rebinds a session —
//! so channels serving concurrent readers (the app door's frames) hold
//! still under load. It lives in the session crate because every door
//! needs it (serverd's `/mcp` and `/query`, the app door's frame reads).

use std::collections::HashMap;
use std::sync::Arc;

use glossql_catalog::Lake;
use glossql_glossary::{Actor, Store};
use glossql_parser::{GlossqlParser, Statement};
use tokio::sync::RwLock;

use crate::session::{FunctionRuntime, Outcome, Session, SessionError};

pub struct Plane {
    store: Store,
    lake: Option<Lake>,
    runtime: Arc<dyn FunctionRuntime>,
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
    pub fn new(store: Store, lake: Option<Lake>, runtime: Arc<dyn FunctionRuntime>) -> Self {
        Plane {
            store,
            lake,
            runtime,
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
        let mut session = Session::new(self.store.clone(), actor)?
            .with_row_cap(self.row_cap)
            .with_runtime(Arc::clone(&self.runtime));
        if let Some(lake) = &self.lake {
            session = session.with_lake(lake.clone());
        }
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
        let mut outcomes = Vec::with_capacity(statements.len());
        let mut run: Vec<Statement> = Vec::new();
        for statement in statements {
            if let Statement::Use(u) = statement {
                if !run.is_empty() {
                    let session = self.session(actor.clone()).await?;
                    outcomes
                        .append(&mut session.execute_statements(std::mem::take(&mut run)).await?);
                }
                let name = u.dataset.value;
                self.channel(actor.clone(), Some(&name)).await?;
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
            let session = self.session(actor).await?;
            outcomes.append(&mut session.execute_statements(run).await?);
        }
        Ok(outcomes)
    }
}
