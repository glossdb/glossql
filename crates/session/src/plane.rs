//! The shared plane behind the doors: one store, one lake, one script
//! runtime, and the workspace's sessions keyed by actor. The transports
//! are stateless (the 2026-07-28 MCP revision removed transport
//! sessions), so continuity lives here — which is also what `USE` needs
//! to survive between an agent's tool calls. It lives in the session
//! crate because every door needs it (serverd's `/mcp` and `/query`,
//! the app door's frame reads).

use std::collections::HashMap;
use std::sync::Arc;

use glossql_catalog::Lake;
use glossql_glossary::{Actor, Store};
use tokio::sync::RwLock;

use crate::session::{FunctionRuntime, Session, SessionError};

pub struct Plane {
    store: Store,
    lake: Option<Lake>,
    runtime: Arc<dyn FunctionRuntime>,
    sessions: RwLock<HashMap<String, Arc<Session>>>,
    row_cap: usize,
}

impl Plane {
    pub fn new(store: Store, lake: Option<Lake>, runtime: Arc<dyn FunctionRuntime>) -> Self {
        Plane {
            store,
            lake,
            runtime,
            sessions: RwLock::new(HashMap::new()),
            row_cap: usize::MAX,
        }
    }

    /// The cap the doors render at, pushed down so the engine is not asked
    /// for rows nobody will read.
    pub fn with_row_cap(mut self, cap: usize) -> Self {
        self.row_cap = cap;
        self
    }

    /// The actor's session, created on first sight and kept for the
    /// server's life. Actor rides the connection (SPEC.md §1): the doors
    /// resolve who is speaking, this map keeps their session.
    pub async fn session(&self, actor: Actor) -> Result<Arc<Session>, SessionError> {
        let key = format!("{}:{}", actor.kind, actor.id);
        if let Some(session) = self.sessions.read().await.get(&key) {
            return Ok(Arc::clone(session));
        }
        let mut sessions = self.sessions.write().await;
        if let Some(session) = sessions.get(&key) {
            return Ok(Arc::clone(session));
        }
        let mut session = Session::new(self.store.clone(), actor)?
            .with_row_cap(self.row_cap)
            .with_runtime(Arc::clone(&self.runtime));
        if let Some(lake) = &self.lake {
            session = session.with_lake(lake.clone());
        }
        let session = Arc::new(session);
        sessions.insert(key, Arc::clone(&session));
        Ok(session)
    }
}
