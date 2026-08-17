//! The narrow IO seam: a relation is rows in, rows out.
//!
//! Stage 1 made the rules pure functions over rows
//! (`reports/2026-08-17-the-foundation.md` §6). This is the other half —
//! everything a backend has to provide, which is less than it looks:
//!
//! - **scan** hands back history, not the current view. Supersession is
//!   [`crate::rules::latest_by`], applied on top. A backend that filtered
//!   would be reimplementing the rule, which is what the SQL
//!   `NOT EXISTS ... n.id > g.id` was.
//! - **append** adds rows. Replacement is a later row, never an update,
//!   so nothing here mutates and nothing needs a transaction beyond the
//!   one write.
//!
//! Two implementations meet here: sqlx over SQLite today, Iceberg v3
//! next. The only thing that differs is where [`Row::seq`] comes from —
//! an autoincrement id, or `(_last_updated_sequence_number, _pos)`.

use crate::Result;

/// One stored row: its cells in the relation's declared column order,
/// and what ordered the write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
    pub cells: Vec<Option<String>>,
    /// The write's position in the store's total order. SQLite's
    /// autoincrement id fills both halves; Iceberg v3 supplies the
    /// commit's data sequence number and the row's position in its file,
    /// which together order writes inside one commit as well as across
    /// them (spike 7, 2026-08-17).
    pub seq: (i64, i64),
}

impl Row {
    pub fn new(cells: Vec<Option<String>>, seq: (i64, i64)) -> Self {
        Row { cells, seq }
    }

    /// A cell by position in the relation's column order.
    pub fn get(&self, i: usize) -> Option<&str> {
        self.cells.get(i).and_then(|c| c.as_deref())
    }
}

/// Everything a store backend must provide. Deliberately two methods:
/// anything more is a rule that belongs in [`crate::rules`].
#[async_trait::async_trait]
pub trait Relations: Send + Sync + std::fmt::Debug {
    /// Every row ever written to the relation, in no guaranteed order —
    /// callers order by [`Row::seq`] because that is the rule.
    async fn scan(&self, relation: &str) -> Result<Vec<Row>>;

    /// Append rows as one write. Ordering inside one append is by
    /// position, so a caller that appends two rows sharing a supersession
    /// key gets the later one — see the batching ruling of 2026-08-17.
    async fn append(&self, relation: &str, rows: Vec<Vec<Option<String>>>) -> Result<()>;
}
