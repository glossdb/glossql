//! A shipped read served from memory. The `next` read is a pure
//! function of the record's version and the dataset's pin — every arm
//! reads the record, the pinned tables and the door relations, nothing
//! outside them — and it costs the engine's planning of some 1,600
//! operators each time it runs, after every door call. The cube's rule
//! applied to a read: one run per key, shared by every reader of that
//! key, and a key that stops matching is simply never asked for again.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::prepass::ReadSet;
use crate::reads::Served;
use crate::session::SessionError;

/// The shipped reads served this way. `workspace_next` is not one: it
/// reads every dataset's landings, and no one dataset's pin keys that.
pub(crate) const MEMOIZED: &[&str] = &["next"];

pub(crate) fn memoized(name: &str) -> bool {
    MEMOIZED.contains(&name)
}

/// What one run answers for: the read, the dataset it ran on, the
/// record's version and the dataset's pin at the time.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct MemoKey {
    pub read: String,
    pub dataset: String,
    pub version: String,
    pub pin: String,
}

/// One run's rows, and what resolving the read touched — replayed on
/// a hit, so the statement classes and records as the run did.
#[derive(Debug)]
pub(crate) struct Memo {
    pub served: Served,
    pub record: bool,
    pub reads: ReadSet,
}

/// The cache: a few dozen entries, recency-evicted. An entry is a
/// handful of rows; a key that no longer matches is never asked for
/// again, so the bound is against the process's lifetime, not its
/// working set.
#[derive(Clone)]
pub struct ShippedCache {
    inner: moka::future::Cache<MemoKey, Arc<Memo>>,
    runs: Arc<AtomicU64>,
}

const ENTRIES: u64 = 64;

impl Default for ShippedCache {
    fn default() -> Self {
        Self::new()
    }
}

impl ShippedCache {
    pub fn new() -> Self {
        ShippedCache {
            inner: moka::future::Cache::builder()
                .max_capacity(ENTRIES)
                .eviction_policy(moka::policy::EvictionPolicy::lru())
                .build(),
            runs: Arc::new(AtomicU64::new(0)),
        }
    }

    /// How many runs this cache has made — one per miss, whatever the
    /// number of readers that shared it.
    pub fn runs(&self) -> u64 {
        self.runs.load(Ordering::Relaxed)
    }

    /// The entry at `key`, or one run of `run` shared by every reader
    /// that asks while it is in flight. A run that fails caches
    /// nothing and every waiting reader gets its error.
    pub(crate) async fn get_or_run<F, Fut>(
        &self,
        key: MemoKey,
        run: F,
    ) -> Result<Arc<Memo>, SessionError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Memo, SessionError>>,
    {
        self.inner
            .try_get_with(key, async {
                self.runs.fetch_add(1, Ordering::Relaxed);
                run().await.map(Arc::new)
            })
            .await
            .map_err(|e: Arc<SessionError>| SessionError::BadSubject(e.to_string()))
    }
}

impl std::fmt::Debug for ShippedCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShippedCache")
            .field("runs", &self.runs())
            .finish_non_exhaustive()
    }
}
