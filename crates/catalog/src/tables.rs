//! The catalog tables: every version of every landed table as rows of
//! the record's database, in the shapes the DuckLake 1.0 specification
//! names (`ducklake_snapshot`, `ducklake_schema`, `ducklake_table`,
//! `ducklake_column`, `ducklake_data_file`, …) so a DuckLake reader
//! attaches to the same database and reads the same files. All
//! twenty-eight tables are created; this server writes the ones a
//! landing, a replace, an append and a drop touch, and reads them back
//! as a dataset's pin. A file's life is the pair `begin_snapshot` /
//! `end_snapshot`: live while the end is null, ended by the commit that
//! replaced or dropped it, scheduled for deletion by that same commit.
//! A column's life is the same pair, version by version under one
//! `column_id`, as the specification has it: a replace keeps the row
//! of a column whose name, type and derivation are unchanged, begins
//! a new version of one whose type or derivation changed, ends one
//! that is gone. The derivation — the recipe's expression for the
//! column, where the landing knows it — is the column's
//! `glossql.expr` tag.
//!
//! Every write is one transaction that also appends the snapshot row,
//! so two writers racing for the same snapshot id meet the primary key
//! and the loser runs again on the next id.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};
use sqlx::Row as _;

use crate::record::Db;
use crate::{Error, Result, Shape};

/// The catalog's tables as DuckDB's own DuckLake extension creates
/// them (its 1.0 line), created if absent, in this order. Three types
/// follow the dialect the way that extension writes them: a boolean,
/// a timestamp and a uuid are `BIGINT`, `VARCHAR` and `VARCHAR` on
/// SQLite and `BOOLEAN`, `TIMESTAMP WITH TIME ZONE` and `UUID` on
/// Postgres.
const DDL: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS ducklake_metadata (\"key\" VARCHAR NOT NULL, \"value\" VARCHAR NOT NULL, \"scope\" VARCHAR, scope_id BIGINT)",
    "CREATE TABLE IF NOT EXISTS ducklake_snapshot (snapshot_id BIGINT PRIMARY KEY, snapshot_time {TS}, schema_version BIGINT, next_catalog_id BIGINT, next_file_id BIGINT)",
    "CREATE TABLE IF NOT EXISTS ducklake_snapshot_changes (snapshot_id BIGINT PRIMARY KEY, changes_made VARCHAR, author VARCHAR, commit_message VARCHAR, commit_extra_info VARCHAR)",
    "CREATE TABLE IF NOT EXISTS ducklake_schema (schema_id BIGINT PRIMARY KEY, schema_uuid {UUID}, begin_snapshot BIGINT, end_snapshot BIGINT, schema_name VARCHAR, path VARCHAR, path_is_relative {BOOL})",
    "CREATE TABLE IF NOT EXISTS ducklake_table (table_id BIGINT, table_uuid {UUID}, begin_snapshot BIGINT, end_snapshot BIGINT, schema_id BIGINT, table_name VARCHAR, path VARCHAR, path_is_relative {BOOL})",
    "CREATE TABLE IF NOT EXISTS ducklake_view (view_id BIGINT, view_uuid {UUID}, begin_snapshot BIGINT, end_snapshot BIGINT, schema_id BIGINT, view_name VARCHAR, dialect VARCHAR, \"sql\" VARCHAR, column_aliases VARCHAR)",
    "CREATE TABLE IF NOT EXISTS ducklake_tag (object_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, \"key\" VARCHAR, \"value\" VARCHAR)",
    "CREATE TABLE IF NOT EXISTS ducklake_column_tag (table_id BIGINT, column_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, \"key\" VARCHAR, \"value\" VARCHAR)",
    "CREATE TABLE IF NOT EXISTS ducklake_data_file (data_file_id BIGINT PRIMARY KEY, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, file_order BIGINT, path VARCHAR, path_is_relative {BOOL}, file_format VARCHAR, record_count BIGINT, file_size_bytes BIGINT, footer_size BIGINT, row_id_start BIGINT, partition_id BIGINT, encryption_key VARCHAR, mapping_id BIGINT, partial_max BIGINT)",
    "CREATE TABLE IF NOT EXISTS ducklake_file_column_stats (data_file_id BIGINT, table_id BIGINT, column_id BIGINT, column_size_bytes BIGINT, value_count BIGINT, null_count BIGINT, min_value VARCHAR, max_value VARCHAR, contains_nan {BOOL}, extra_stats VARCHAR)",
    "CREATE TABLE IF NOT EXISTS ducklake_file_variant_stats (data_file_id BIGINT, table_id BIGINT, column_id BIGINT, variant_path VARCHAR, shredded_type VARCHAR, column_size_bytes BIGINT, value_count BIGINT, null_count BIGINT, min_value VARCHAR, max_value VARCHAR, contains_nan {BOOL}, extra_stats VARCHAR)",
    "CREATE TABLE IF NOT EXISTS ducklake_delete_file (delete_file_id BIGINT PRIMARY KEY, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, data_file_id BIGINT, path VARCHAR, path_is_relative {BOOL}, format VARCHAR, delete_count BIGINT, file_size_bytes BIGINT, footer_size BIGINT, encryption_key VARCHAR, partial_max BIGINT)",
    "CREATE TABLE IF NOT EXISTS ducklake_column (column_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, table_id BIGINT, column_order BIGINT, column_name VARCHAR, column_type VARCHAR, initial_default VARCHAR, default_value VARCHAR, nulls_allowed {BOOL}, parent_column BIGINT, default_value_type VARCHAR, default_value_dialect VARCHAR)",
    "CREATE TABLE IF NOT EXISTS ducklake_table_stats (table_id BIGINT, record_count BIGINT, next_row_id BIGINT, file_size_bytes BIGINT)",
    "CREATE TABLE IF NOT EXISTS ducklake_table_column_stats (table_id BIGINT, column_id BIGINT, contains_null {BOOL}, contains_nan {BOOL}, min_value VARCHAR, max_value VARCHAR, extra_stats VARCHAR)",
    "CREATE TABLE IF NOT EXISTS ducklake_partition_info (partition_id BIGINT, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT)",
    "CREATE TABLE IF NOT EXISTS ducklake_partition_column (partition_id BIGINT, table_id BIGINT, partition_key_index BIGINT, column_id BIGINT, \"transform\" VARCHAR)",
    "CREATE TABLE IF NOT EXISTS ducklake_file_partition_value (data_file_id BIGINT, table_id BIGINT, partition_key_index BIGINT, partition_value VARCHAR)",
    "CREATE TABLE IF NOT EXISTS ducklake_files_scheduled_for_deletion (data_file_id BIGINT, path VARCHAR, path_is_relative {BOOL}, schedule_start {TS})",
    "CREATE TABLE IF NOT EXISTS ducklake_inlined_data_tables (table_id BIGINT, table_name VARCHAR, schema_version BIGINT)",
    "CREATE TABLE IF NOT EXISTS ducklake_column_mapping (mapping_id BIGINT, table_id BIGINT, \"type\" VARCHAR)",
    "CREATE TABLE IF NOT EXISTS ducklake_name_mapping (mapping_id BIGINT, column_id BIGINT, source_name VARCHAR, target_field_id BIGINT, parent_column BIGINT, is_partition {BOOL})",
    "CREATE TABLE IF NOT EXISTS ducklake_schema_versions (begin_snapshot BIGINT, schema_version BIGINT, table_id BIGINT)",
    "CREATE TABLE IF NOT EXISTS ducklake_macro (schema_id BIGINT, macro_id BIGINT, macro_name VARCHAR, begin_snapshot BIGINT, end_snapshot BIGINT)",
    "CREATE TABLE IF NOT EXISTS ducklake_macro_impl (macro_id BIGINT, impl_id BIGINT, dialect VARCHAR, \"sql\" VARCHAR, \"type\" VARCHAR)",
    "CREATE TABLE IF NOT EXISTS ducklake_macro_parameters (macro_id BIGINT, impl_id BIGINT, column_id BIGINT, parameter_name VARCHAR, parameter_type VARCHAR, default_value VARCHAR, default_value_type VARCHAR)",
    "CREATE TABLE IF NOT EXISTS ducklake_sort_info (sort_id BIGINT, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT)",
    "CREATE TABLE IF NOT EXISTS ducklake_sort_expression (sort_id BIGINT, table_id BIGINT, sort_key_index BIGINT, expression VARCHAR, dialect VARCHAR, sort_direction VARCHAR, null_order VARCHAR)",
];

/// Create every table, then the first snapshot and the metadata rows
/// when the catalog is new. `data_path` is the warehouse root every
/// relative path hangs under, with its trailing slash.
pub(crate) async fn create(db: &Db, data_path: &str) -> Result<()> {
    let (bool_type, ts_type, uuid_type) = if db.is_sqlite() {
        ("BIGINT", "VARCHAR", "VARCHAR")
    } else {
        ("BOOLEAN", "TIMESTAMP WITH TIME ZONE", "UUID")
    };
    for statement in DDL {
        let statement = statement
            .replace("{BOOL}", bool_type)
            .replace("{TS}", ts_type)
            .replace("{UUID}", uuid_type);
        sqlx::query(&statement).execute(db.pool()).await?;
    }
    let snapshots: i64 = sqlx::query("SELECT count(*) FROM ducklake_snapshot")
        .fetch_one(db.pool())
        .await?
        .try_get(0)?;
    if snapshots == 0 {
        // Snapshot 0 creates the `main` schema, as a DuckLake client
        // expects a catalog to open with one.
        sqlx::query(&format!(
            "INSERT INTO ducklake_snapshot (snapshot_id, snapshot_time, schema_version, \
             next_catalog_id, next_file_id) VALUES (0, '{}', 0, 1, 0)",
            now()
        ))
        .execute(db.pool())
        .await?;
        sqlx::query(
            "INSERT INTO ducklake_snapshot_changes (snapshot_id, changes_made, author, \
             commit_message, commit_extra_info) VALUES (0, 'created_schema:\"main\"', NULL, NULL, NULL)",
        )
        .execute(db.pool())
        .await?;
        sqlx::query(&db.sql(&format!(
            "INSERT INTO ducklake_schema (schema_id, schema_uuid, begin_snapshot, end_snapshot, \
             schema_name, path, path_is_relative) VALUES (0, '{}', 0, NULL, 'main', 'main/', ?)",
            uuid::Uuid::now_v7()
        )))
        .bind(true)
        .execute(db.pool())
        .await?;
        for (key, value) in [
            ("version", "1.0"),
            ("created_by", concat!("glossql ", env!("CARGO_PKG_VERSION"))),
            ("data_path", data_path),
            ("encrypted", "false"),
        ] {
            sqlx::query(&db.sql(
                "INSERT INTO ducklake_metadata (\"key\", \"value\", \"scope\", scope_id) VALUES (?, ?, NULL, NULL)",
            ))
            .bind(key)
            .bind(value)
            .execute(db.pool())
            .await?;
        }
    }
    Ok(())
}

/// The current instant as the specification's timestamp literal.
fn now() -> String {
    chrono::DateTime::<chrono::Utc>::from(std::time::SystemTime::now())
        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

// -- ids and the snapshot -----------------------------------------------

/// The snapshot a transaction is producing, and the ids it hands out
/// on the way: the specification keeps both counters on the snapshot
/// row, so a commit reads the last row's and writes its own.
struct Snapshot {
    id: i64,
    next_catalog_id: i64,
    next_file_id: i64,
}

impl Snapshot {
    fn catalog_id(&mut self) -> i64 {
        let id = self.next_catalog_id;
        self.next_catalog_id += 1;
        id
    }

    fn file_id(&mut self) -> i64 {
        let id = self.next_file_id;
        self.next_file_id += 1;
        id
    }
}

type Tx = sqlx::Transaction<'static, sqlx::Any>;

const COMMIT_ATTEMPTS: usize = 3;

/// One catalog commit: `body` runs inside a transaction holding the
/// snapshot it produces and names what it changed, and the snapshot
/// row lands with it. A second writer that took the same snapshot id
/// meets the primary key at its insert, and the commit runs again on
/// the next id.
async fn commit<T, F>(db: &Db, body: F) -> Result<T>
where
    F: for<'a> Fn(
        &'a Db,
        &'a mut Tx,
        &'a mut Snapshot,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<(T, String)>> + Send + 'a>>,
{
    let mut attempt = 0;
    loop {
        attempt += 1;
        let mut tx = db.pool().begin().await?;
        let last = sqlx::query(
            "SELECT snapshot_id, next_catalog_id, next_file_id FROM ducklake_snapshot \
             ORDER BY snapshot_id DESC LIMIT 1",
        )
        .fetch_one(&mut *tx)
        .await?;
        let mut snapshot = Snapshot {
            id: last.try_get::<i64, _>(0)? + 1,
            next_catalog_id: last.try_get(1)?,
            next_file_id: last.try_get(2)?,
        };
        let (out, changes) = body(db, &mut tx, &mut snapshot).await?;
        let landed = async {
            sqlx::query(&db.sql(&format!(
                "INSERT INTO ducklake_snapshot (snapshot_id, snapshot_time, schema_version, \
                 next_catalog_id, next_file_id) VALUES (?, '{}', 0, ?, ?)",
                now()
            )))
            .bind(snapshot.id)
            .bind(snapshot.next_catalog_id)
            .bind(snapshot.next_file_id)
            .execute(&mut *tx)
            .await?;
            sqlx::query(&db.sql(
                "INSERT INTO ducklake_snapshot_changes (snapshot_id, changes_made, author, \
                 commit_message, commit_extra_info) VALUES (?, ?, NULL, NULL, NULL)",
            ))
            .bind(snapshot.id)
            .bind(&changes)
            .execute(&mut *tx)
            .await?;
            tx.commit().await?;
            Result::Ok(())
        }
        .await;
        match landed {
            Ok(()) => return Ok(out),
            Err(Error::Record(sqlx::Error::Database(e)))
                if e.is_unique_violation() && attempt < COMMIT_ATTEMPTS =>
            {
                tracing::debug!(attempt, "the snapshot id was taken; committing again");
            }
            Err(e) => return Err(e),
        }
    }
}

// -- datasets --------------------------------------------------------------

/// The live schema's id, or none.
async fn schema_id(db: &Db, executor: &mut Tx, name: &str) -> Result<Option<i64>> {
    let row = sqlx::query(&db.sql(
        "SELECT schema_id FROM ducklake_schema WHERE schema_name = ? AND end_snapshot IS NULL",
    ))
    .bind(name)
    .fetch_optional(&mut **executor)
    .await?;
    Ok(row.map(|r| r.try_get(0)).transpose()?)
}

/// The dataset's schema row, created if absent; whether it was.
pub(crate) async fn ensure_schema(db: &Db, name: &str) -> Result<bool> {
    // Asked outside the commit first: a dataset that stands makes no
    // snapshot. The check inside the transaction is for the writer
    // that lost the race to create it.
    if schema_exists(db, name).await? {
        return Ok(false);
    }
    let name = name.to_string();
    commit(db, move |db, tx, snapshot| {
        let name = name.clone();
        Box::pin(async move {
            if schema_id(db, tx, &name).await?.is_some() {
                return Ok((false, String::new()));
            }
            let id = snapshot.catalog_id();
            sqlx::query(&db.sql(&format!(
                "INSERT INTO ducklake_schema (schema_id, schema_uuid, begin_snapshot, end_snapshot, \
                 schema_name, path, path_is_relative) VALUES (?, '{}', ?, NULL, ?, ?, ?)",
                uuid::Uuid::now_v7()
            )))
            .bind(id)
            .bind(snapshot.id)
            .bind(&name)
            .bind(format!("{name}/"))
            .bind(true)
            .execute(&mut **tx)
            .await?;
            Ok((true, format!("created_schema:\"{name}\"")))
        })
    })
    .await
}

pub(crate) async fn schema_exists(db: &Db, name: &str) -> Result<bool> {
    let row = sqlx::query(&db.sql(
        "SELECT count(*) FROM ducklake_schema WHERE schema_name = ? AND end_snapshot IS NULL",
    ))
    .bind(name)
    .fetch_one(db.pool())
    .await?;
    Ok(row.try_get::<i64, _>(0)? > 0)
}

pub(crate) async fn schema_names(db: &Db) -> Result<Vec<String>> {
    // Schema 0 is `main`, the one a DuckLake client opens on; it is
    // no dataset.
    let rows = sqlx::query(
        "SELECT schema_name FROM ducklake_schema WHERE end_snapshot IS NULL AND schema_id <> 0 \
         ORDER BY schema_name",
    )
    .fetch_all(db.pool())
    .await?;
    rows.iter().map(|r| Ok(r.try_get(0)?)).collect()
}

// -- tables, columns, files -----------------------------------------------

/// One live column row, as read back.
#[derive(Debug, Clone)]
struct ColumnRow {
    column_id: i64,
    parent: Option<i64>,
    order: i64,
    name: String,
    kind: String,
    nullable: bool,
    /// The snapshot this version of the column began at.
    begin: i64,
}

/// The table tag that carries its columns' derivations: one JSON
/// object, column name to expression. A table tag, not one per
/// column: a DuckLake reader takes `comment` alone as a column tag's
/// key and refuses the whole catalog on any other, and takes any key
/// on a table.
const EXPRS_TAG: &str = "glossql.exprs";

/// One live file of a table, relative to the table's directory.
#[derive(Debug, Clone)]
pub(crate) struct FileRow {
    pub path: String,
    pub size: u64,
}

/// A landed table at its current version, as the pin reads it.
#[derive(Debug, Clone)]
pub(crate) struct TableRows {
    pub name: String,
    /// The snapshot that last changed the table: its creation, a
    /// landing, a replace, an append.
    pub version: i64,
    pub schema: SchemaRef,
    pub files: Vec<FileRow>,
    /// Each column's live version and the table's newest column change.
    pub shape: Shape,
}

/// Every live table of the dataset — or the named ones, `None` when
/// a name is not a live table there — with columns and files.
pub(crate) async fn pin(
    db: &Db,
    dataset: &str,
    names: Option<&[String]>,
) -> Result<Option<Vec<TableRows>>> {
    let tables = sqlx::query(&db.sql(
        "SELECT t.table_id, t.table_name, t.begin_snapshot FROM ducklake_table t \
         JOIN ducklake_schema s ON s.schema_id = t.schema_id \
         WHERE s.schema_name = ? AND s.end_snapshot IS NULL AND t.end_snapshot IS NULL \
         ORDER BY t.table_name",
    ))
    .bind(dataset)
    .fetch_all(db.pool())
    .await?;
    let mut ids: Vec<(i64, String, i64)> = tables
        .iter()
        .map(|r| Ok((r.try_get(0)?, r.try_get(1)?, r.try_get(2)?)))
        .collect::<Result<_>>()?;
    if let Some(names) = names {
        ids.retain(|(_, name, _)| names.contains(name));
        if ids.len() != names.len() {
            return Ok(None);
        }
    }
    if ids.is_empty() {
        return Ok(Some(Vec::new()));
    }
    let list = ids
        .iter()
        .map(|(id, _, _)| id.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let columns = sqlx::query(&format!(
        "SELECT table_id, column_id, parent_column, column_order, column_name, column_type, \
         CAST(CAST(nulls_allowed AS INTEGER) AS BIGINT), begin_snapshot FROM ducklake_column \
         WHERE table_id IN ({list}) AND end_snapshot IS NULL ORDER BY table_id, column_order"
    ))
    .fetch_all(db.pool())
    .await?;
    let files = sqlx::query(&format!(
        "SELECT table_id, path, file_size_bytes, begin_snapshot FROM ducklake_data_file \
         WHERE table_id IN ({list}) AND end_snapshot IS NULL ORDER BY table_id, file_order"
    ))
    .fetch_all(db.pool())
    .await?;
    // A replace or a drop ends files at the snapshot it happened in, so
    // the newest end is a change too — the version moves even when the
    // replace landed no file.
    let ends = sqlx::query(&format!(
        "SELECT table_id, MAX(end_snapshot) FROM ducklake_data_file WHERE table_id IN ({list}) \
         GROUP BY table_id"
    ))
    .fetch_all(db.pool())
    .await?;
    // The table's newest column change: the newest end over every
    // version of every column it has had — a re-derived or retyped
    // column ends its previous version, a dropped column ends its
    // last. A column added ends nothing and is no change to what
    // stood.
    let shapes = sqlx::query(&format!(
        "SELECT table_id, MAX(end_snapshot) FROM ducklake_column \
         WHERE table_id IN ({list}) GROUP BY table_id"
    ))
    .fetch_all(db.pool())
    .await?;
    struct Slot {
        columns: Vec<ColumnRow>,
        files: Vec<FileRow>,
        version: i64,
        changed: i64,
    }
    let mut by_table: HashMap<i64, Slot> = HashMap::new();
    for (id, _, begin) in &ids {
        by_table.insert(
            *id,
            Slot {
                columns: Vec::new(),
                files: Vec::new(),
                version: *begin,
                changed: *begin,
            },
        );
    }
    for r in &columns {
        let id: i64 = r.try_get(0)?;
        if let Some(slot) = by_table.get_mut(&id) {
            slot.columns.push(ColumnRow {
                column_id: r.try_get(1)?,
                parent: r.try_get(2)?,
                order: r.try_get(3)?,
                name: r.try_get(4)?,
                kind: r.try_get(5)?,
                nullable: r.try_get::<i64, _>(6)? != 0,
                begin: r.try_get(7)?,
            });
        }
    }
    for r in &files {
        let id: i64 = r.try_get(0)?;
        if let Some(slot) = by_table.get_mut(&id) {
            let size: i64 = r.try_get(2)?;
            slot.files.push(FileRow {
                path: r.try_get(1)?,
                size: u64::try_from(size).unwrap_or(0),
            });
            let begin: i64 = r.try_get(3)?;
            slot.version = slot.version.max(begin);
        }
    }
    for r in &ends {
        let id: i64 = r.try_get(0)?;
        let end: Option<i64> = r.try_get(1)?;
        if let (Some(slot), Some(end)) = (by_table.get_mut(&id), end) {
            slot.version = slot.version.max(end);
        }
    }
    for r in &shapes {
        let id: i64 = r.try_get(0)?;
        let end: Option<i64> = r.try_get(1)?;
        if let (Some(slot), Some(end)) = (by_table.get_mut(&id), end) {
            slot.changed = slot.changed.max(end);
        }
    }
    let mut out = Vec::with_capacity(ids.len());
    for (id, name, _) in ids {
        let Some(slot) = by_table.remove(&id) else {
            continue;
        };
        let shape = Shape {
            columns: slot
                .columns
                .iter()
                .filter(|c| c.parent.is_none())
                .map(|c| (c.name.clone(), c.begin))
                .collect(),
            changed: slot.changed,
        };
        out.push(TableRows {
            name,
            version: slot.version,
            schema: Arc::new(schema_from_rows(&slot.columns)?),
            files: slot.files,
            shape,
        });
    }
    Ok(Some(out))
}

pub(crate) async fn table_names(db: &Db, dataset: &str) -> Result<Vec<String>> {
    let rows = sqlx::query(&db.sql(
        "SELECT t.table_name FROM ducklake_table t JOIN ducklake_schema s ON s.schema_id = t.schema_id \
         WHERE s.schema_name = ? AND s.end_snapshot IS NULL AND t.end_snapshot IS NULL \
         ORDER BY t.table_name",
    ))
    .bind(dataset)
    .fetch_all(db.pool())
    .await?;
    rows.iter().map(|r| Ok(r.try_get(0)?)).collect()
}

/// The live table's id in the dataset, or none.
async fn table_id(db: &Db, tx: &mut Tx, dataset: &str, table: &str) -> Result<Option<i64>> {
    let row = sqlx::query(&db.sql(
        "SELECT t.table_id FROM ducklake_table t JOIN ducklake_schema s ON s.schema_id = t.schema_id \
         WHERE s.schema_name = ? AND t.table_name = ? AND s.end_snapshot IS NULL AND t.end_snapshot IS NULL",
    ))
    .bind(dataset)
    .bind(table)
    .fetch_optional(&mut **tx)
    .await?;
    Ok(row.map(|r| r.try_get(0)).transpose()?)
}

/// A file a landing wrote, as the commit records it.
#[derive(Debug, Clone)]
pub struct LandedFile {
    /// Relative to the table's directory.
    pub path: String,
    pub size: u64,
    pub rows: i64,
    pub footer: i64,
}

/// How a landing joins its table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Landing {
    /// The table does not exist: its row, its columns and its files.
    Create,
    /// The table's live files end and these begin; the columns evolve
    /// to the shape, version by version.
    Replace,
    /// These files join the live ones; the shape is the table's.
    Append,
}

impl Landing {
    /// The landing's word, as the record's `imports.mode` spells it.
    pub fn as_str(self) -> &'static str {
        match self {
            Landing::Create => "create",
            Landing::Replace => "replace",
            Landing::Append => "append",
        }
    }
}

/// The landing as one commit; the snapshot it made is the table's new
/// version. `exprs` is each column's derivation where the landing
/// knows it, the table's derivations tag once a create or a replace
/// lands; `tags` are the table's tags to set, each ending the live
/// tag of the same key. The files a replace ended are scheduled for
/// deletion in the same transaction, and returned so the caller can
/// delete them.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn commit_landing(
    db: &Db,
    dataset: &str,
    table: &str,
    schema: &Schema,
    files: &[LandedFile],
    landing: Landing,
    exprs: &HashMap<String, String>,
    tags: &[(String, String)],
) -> Result<(i64, Vec<(i64, String)>)> {
    let dataset = dataset.to_string();
    let table = table.to_string();
    let files = files.to_vec();
    let schema = schema.clone();
    let exprs = exprs.clone();
    let tags = tags.to_vec();
    commit(db, move |db, tx, snapshot| {
        let (dataset, table, files, schema, exprs, tags) = (
            dataset.clone(),
            table.clone(),
            files.clone(),
            schema.clone(),
            exprs.clone(),
            tags.clone(),
        );
        Box::pin(async move {
            let Some(schema_id) = schema_id(db, tx, &dataset).await? else {
                return Err(Error::Workspace(format!("no dataset `{dataset}`")));
            };
            let existing = table_id(db, tx, &dataset, &table).await?;
            // One name space per schema, as a reader attached to the
            // catalog has it: a table never lands under a view's name.
            if existing.is_none() && view_id(db, tx, &dataset, &table).await?.is_some() {
                return Err(Error::Workspace(format!(
                    "`{table}` is a grounded metric's view in `{dataset}`: a table and a \
                     metric of one dataset never share a name"
                )));
            }
            let mut ended = Vec::new();
            let (id, changes) = match (landing, existing) {
                (Landing::Create, None) => {
                    let id = snapshot.catalog_id();
                    sqlx::query(&db.sql(&format!(
                        "INSERT INTO ducklake_table (table_id, table_uuid, begin_snapshot, end_snapshot, \
                         schema_id, table_name, path, path_is_relative) VALUES (?, '{}', ?, NULL, ?, ?, ?, ?)",
                        uuid::Uuid::now_v7()
                    )))
                    .bind(id)
                    .bind(snapshot.id)
                    .bind(schema_id)
                    .bind(&table)
                    .bind(format!("{table}/"))
                    .bind(true)
                    .execute(&mut **tx)
                    .await?;
                    let mut order = 0;
                    for field in schema.fields() {
                        insert_field(db, tx, snapshot, id, field, None, &mut order).await?;
                    }
                    (id, format!("created_table:\"{dataset}\".\"{table}\""))
                }
                (Landing::Create, Some(_)) => {
                    return Err(Error::Workspace(format!(
                        "`{dataset}.{table}` already exists"
                    )));
                }
                (Landing::Replace | Landing::Append, None) => {
                    return Err(Error::Workspace(format!("no table `{dataset}.{table}`")));
                }
                (Landing::Replace, Some(id)) => {
                    ended = end_files(db, tx, snapshot, id).await?;
                    let altered = evolve_columns(db, tx, snapshot, id, &schema, &exprs).await?;
                    let mut changes = format!("deleted_from_table:{id},inserted_into_table:{id}");
                    if altered {
                        changes.push_str(&format!(",altered_table:{id}"));
                    }
                    (id, changes)
                }
                (Landing::Append, Some(id)) => (id, format!("inserted_into_table:{id}")),
            };
            set_tags(db, tx, snapshot, id, &tags).await?;
            // The derivations are the create's or the replace's; an
            // append's shape is the table's and its tag stands.
            if landing != Landing::Append {
                set_exprs(db, tx, snapshot, id, &exprs).await?;
            }
            let from = sqlx::query(&db.sql(
                "SELECT COALESCE(MAX(file_order), -1) FROM ducklake_data_file WHERE table_id = ?",
            ))
            .bind(id)
            .fetch_one(&mut **tx)
            .await?
            .try_get::<i64, _>(0)?;
            for (i, file) in files.iter().enumerate() {
                let file_id = snapshot.file_id();
                sqlx::query(&db.sql(
                    "INSERT INTO ducklake_data_file (data_file_id, table_id, begin_snapshot, end_snapshot, \
                     file_order, path, path_is_relative, file_format, record_count, file_size_bytes, \
                     footer_size, row_id_start, partition_id, encryption_key, mapping_id, partial_max) \
                     VALUES (?, ?, ?, NULL, ?, ?, ?, 'parquet', ?, ?, ?, NULL, NULL, NULL, NULL, NULL)",
                ))
                .bind(file_id)
                .bind(id)
                .bind(snapshot.id)
                .bind(from + 1 + i as i64)
                .bind(&file.path)
                .bind(true)
                .bind(file.rows)
                .bind(i64::try_from(file.size).unwrap_or(i64::MAX))
                .bind(file.footer)
                .execute(&mut **tx)
                .await?;
            }
            Ok(((snapshot.id, ended), changes))
        })
    })
    .await
}

/// The table ended: its row, its columns, their tags and its files,
/// the files scheduled for deletion and returned.
pub(crate) async fn drop_table(db: &Db, dataset: &str, table: &str) -> Result<Vec<(i64, String)>> {
    let dataset = dataset.to_string();
    let table = table.to_string();
    commit(db, move |db, tx, snapshot| {
        let (dataset, table) = (dataset.clone(), table.clone());
        Box::pin(async move {
            let Some(id) = table_id(db, tx, &dataset, &table).await? else {
                return Err(Error::Workspace(format!("no table `{dataset}.{table}`")));
            };
            let ended = end_files(db, tx, snapshot, id).await?;
            for statement in [
                "UPDATE ducklake_tag SET end_snapshot = ? WHERE object_id = ? AND end_snapshot IS NULL",
                "UPDATE ducklake_column_tag SET end_snapshot = ? WHERE table_id = ? AND end_snapshot IS NULL",
                "UPDATE ducklake_column SET end_snapshot = ? WHERE table_id = ? AND end_snapshot IS NULL",
                "UPDATE ducklake_table SET end_snapshot = ? WHERE table_id = ? AND end_snapshot IS NULL",
            ] {
                sqlx::query(&db.sql(statement))
                    .bind(snapshot.id)
                    .bind(id)
                    .execute(&mut **tx)
                    .await?;
            }
            Ok((ended, format!("dropped_table:{id}")))
        })
    })
    .await
}

/// The table's live files ended at this snapshot and scheduled for
/// deletion; `(file id, path relative to the table)` for each.
async fn end_files(
    db: &Db,
    tx: &mut Tx,
    snapshot: &Snapshot,
    table_id: i64,
) -> Result<Vec<(i64, String)>> {
    let live = sqlx::query(&db.sql(
        "SELECT data_file_id, path FROM ducklake_data_file WHERE table_id = ? AND end_snapshot IS NULL",
    ))
    .bind(table_id)
    .fetch_all(&mut **tx)
    .await?;
    let ended: Vec<(i64, String)> = live
        .iter()
        .map(|r| Ok((r.try_get(0)?, r.try_get(1)?)))
        .collect::<Result<_>>()?;
    sqlx::query(&db.sql(
        "UPDATE ducklake_data_file SET end_snapshot = ? WHERE table_id = ? AND end_snapshot IS NULL",
    ))
    .bind(snapshot.id)
    .bind(table_id)
    .execute(&mut **tx)
    .await?;
    for (file_id, path) in &ended {
        sqlx::query(&db.sql(&format!(
            "INSERT INTO ducklake_files_scheduled_for_deletion (data_file_id, path, path_is_relative, \
             schedule_start) VALUES (?, ?, ?, '{}')",
            now()
        )))
        .bind(file_id)
        .bind(path)
        .bind(true)
        .execute(&mut **tx)
        .await?;
    }
    Ok(ended)
}

/// Every file scheduled for deletion before `before` (the
/// specification's timestamp literal), with the table directory it
/// hangs under: `(file id, dataset, table, relative path)`.
pub(crate) async fn scheduled(db: &Db, before: &str) -> Result<Vec<(i64, String, String, String)>> {
    let rows = sqlx::query(&format!(
        "SELECT f.data_file_id, s.schema_name, t.table_name, f.path \
         FROM ducklake_files_scheduled_for_deletion f \
         JOIN ducklake_data_file d ON d.data_file_id = f.data_file_id \
         JOIN ducklake_table t ON t.table_id = d.table_id \
         JOIN ducklake_schema s ON s.schema_id = t.schema_id \
         WHERE f.schedule_start <= '{before}'"
    ))
    .fetch_all(db.pool())
    .await?;
    rows.iter()
        .map(|r| Ok((r.try_get(0)?, r.try_get(1)?, r.try_get(2)?, r.try_get(3)?)))
        .collect()
}

/// The file is gone from the store: its schedule row goes too.
pub(crate) async fn unschedule(db: &Db, file_id: i64) -> Result<()> {
    sqlx::query(
        &db.sql("DELETE FROM ducklake_files_scheduled_for_deletion WHERE data_file_id = ?"),
    )
    .bind(file_id)
    .execute(db.pool())
    .await?;
    Ok(())
}

// -- tags on the table ------------------------------------------------------

/// The table's tags set at this snapshot: the live tag of each key
/// ends, the new one begins. The specification's tag table is what
/// `COMMENT ON TABLE` writes to, keyed the same way.
async fn set_tags(
    db: &Db,
    tx: &mut Tx,
    snapshot: &Snapshot,
    table_id: i64,
    tags: &[(String, String)],
) -> Result<()> {
    for (key, value) in tags {
        end_tag(db, tx, snapshot, table_id, key).await?;
        sqlx::query(&db.sql(
            "INSERT INTO ducklake_tag (object_id, begin_snapshot, end_snapshot, \"key\", \"value\") \
             VALUES (?, ?, NULL, ?, ?)",
        ))
        .bind(table_id)
        .bind(snapshot.id)
        .bind(key)
        .bind(value)
        .execute(&mut **tx)
        .await?;
    }
    Ok(())
}

/// The object's live tag of `key` ended at this snapshot.
async fn end_tag(
    db: &Db,
    tx: &mut Tx,
    snapshot: &Snapshot,
    object_id: i64,
    key: &str,
) -> Result<()> {
    sqlx::query(&db.sql(
        "UPDATE ducklake_tag SET end_snapshot = ? WHERE object_id = ? AND \"key\" = ? \
         AND end_snapshot IS NULL",
    ))
    .bind(snapshot.id)
    .bind(object_id)
    .bind(key)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// The table's derivations set at this snapshot: the live tag ends,
/// and one begins where the landing knows any, its columns in name
/// order.
async fn set_exprs(
    db: &Db,
    tx: &mut Tx,
    snapshot: &Snapshot,
    table_id: i64,
    exprs: &HashMap<String, String>,
) -> Result<()> {
    if exprs.is_empty() {
        return end_tag(db, tx, snapshot, table_id, EXPRS_TAG).await;
    }
    let ordered: BTreeMap<&str, &str> = exprs
        .iter()
        .map(|(column, expr)| (column.as_str(), expr.as_str()))
        .collect();
    let value = serde_json::to_string(&ordered)?;
    set_tags(
        db,
        tx,
        snapshot,
        table_id,
        &[(EXPRS_TAG.to_string(), value)],
    )
    .await
}

/// The live table's live tags, by key; empty for a table that is not
/// there.
pub(crate) async fn tags(db: &Db, dataset: &str, table: &str) -> Result<HashMap<String, String>> {
    let rows = sqlx::query(&db.sql(
        "SELECT g.\"key\", g.\"value\" FROM ducklake_tag g \
         JOIN ducklake_table t ON t.table_id = g.object_id \
         JOIN ducklake_schema s ON s.schema_id = t.schema_id \
         WHERE s.schema_name = ? AND t.table_name = ? AND s.end_snapshot IS NULL \
         AND t.end_snapshot IS NULL AND g.end_snapshot IS NULL",
    ))
    .bind(dataset)
    .bind(table)
    .fetch_all(db.pool())
    .await?;
    rows.iter()
        .map(|r| Ok((r.try_get(0)?, r.try_get(1)?)))
        .collect()
}

// -- views -----------------------------------------------------------------

/// The tag a view carries beside its definition: its columns and the
/// specification's spelling of their types, as the engine planned the
/// definition — what the mount advertises for it without planning.
const VIEW_SCHEMA_TAG: &str = "glossql.view.schema";

/// One live view of a dataset, as read back: the definition and the
/// dialect it is written in, and the schema its tag carries — empty
/// where the definition did not plan when it was written.
#[derive(Debug, Clone)]
pub struct ViewRow {
    pub name: String,
    pub dialect: String,
    pub sql: String,
    pub schema: SchemaRef,
}

/// The live view's id in the dataset, or none.
async fn view_id(db: &Db, tx: &mut Tx, dataset: &str, view: &str) -> Result<Option<i64>> {
    let row = sqlx::query(&db.sql(
        "SELECT v.view_id FROM ducklake_view v JOIN ducklake_schema s ON s.schema_id = v.schema_id \
         WHERE s.schema_name = ? AND v.view_name = ? AND s.end_snapshot IS NULL AND v.end_snapshot IS NULL",
    ))
    .bind(dataset)
    .bind(view)
    .fetch_optional(&mut **tx)
    .await?;
    Ok(row.map(|r| r.try_get(0)).transpose()?)
}

/// The view's schema as its tag spells it: one `[name, type,
/// nullable]` per column. None where a column's type is not one the
/// vocabulary spells, and the view then carries no schema.
fn view_schema_tag(schema: &Schema) -> Option<String> {
    let columns: Option<Vec<serde_json::Value>> = schema
        .fields()
        .iter()
        .map(|f| {
            let kind = ducklake_type(f.data_type()).ok()?;
            Some(serde_json::json!([f.name(), kind, f.is_nullable()]))
        })
        .collect();
    columns.map(|c| serde_json::Value::Array(c).to_string())
}

fn view_schema(tag: Option<&str>) -> SchemaRef {
    let fields: Vec<Field> = tag
        .and_then(|t| serde_json::from_str::<Vec<(String, String, bool)>>(t).ok())
        .unwrap_or_default()
        .into_iter()
        .filter_map(|(name, kind, nullable)| {
            Some(Field::new(name, arrow_type(&kind).ok()?, nullable))
        })
        .collect();
    Arc::new(Schema::new(fields))
}

/// The view landed in the dataset's schema under `name`, in one
/// commit: a live view of that name ends and this one begins, its
/// schema tag beside it. Refused where a live table holds the name.
/// The snapshot it made.
pub(crate) async fn put_view(
    db: &Db,
    dataset: &str,
    name: &str,
    dialect: &str,
    sql: &str,
    schema: Option<&Schema>,
) -> Result<i64> {
    let dataset = dataset.to_string();
    let name = name.to_string();
    let dialect = dialect.to_string();
    let sql = sql.to_string();
    let tag = schema.and_then(view_schema_tag);
    commit(db, move |db, tx, snapshot| {
        let (dataset, name, dialect, sql, tag) = (
            dataset.clone(),
            name.clone(),
            dialect.clone(),
            sql.clone(),
            tag.clone(),
        );
        Box::pin(async move {
            let Some(schema_id) = schema_id(db, tx, &dataset).await? else {
                return Err(Error::Workspace(format!("no dataset `{dataset}`")));
            };
            if table_id(db, tx, &dataset, &name).await?.is_some() {
                return Err(Error::Workspace(format!(
                    "`{name}` is a landed table of `{dataset}`: a table and a metric of one \
                     dataset never share a name"
                )));
            }
            if let Some(old) = view_id(db, tx, &dataset, &name).await? {
                for statement in [
                    "UPDATE ducklake_tag SET end_snapshot = ? WHERE object_id = ? AND end_snapshot IS NULL",
                    "UPDATE ducklake_view SET end_snapshot = ? WHERE view_id = ? AND end_snapshot IS NULL",
                ] {
                    sqlx::query(&db.sql(statement))
                        .bind(snapshot.id)
                        .bind(old)
                        .execute(&mut **tx)
                        .await?;
                }
            }
            let id = snapshot.catalog_id();
            // `column_aliases` is a reader's quoted list of the view's
            // column names; a view without aliases carries the empty
            // list, `''` — a reader parses the list and refuses a null.
            sqlx::query(&db.sql(&format!(
                "INSERT INTO ducklake_view (view_id, view_uuid, begin_snapshot, end_snapshot, \
                 schema_id, view_name, dialect, \"sql\", column_aliases) \
                 VALUES (?, '{}', ?, NULL, ?, ?, ?, ?, '')",
                uuid::Uuid::now_v7()
            )))
            .bind(id)
            .bind(snapshot.id)
            .bind(schema_id)
            .bind(&name)
            .bind(&dialect)
            .bind(&sql)
            .execute(&mut **tx)
            .await?;
            if let Some(tag) = &tag {
                sqlx::query(&db.sql(
                    "INSERT INTO ducklake_tag (object_id, begin_snapshot, end_snapshot, \"key\", \"value\") \
                     VALUES (?, ?, NULL, ?, ?)",
                ))
                .bind(id)
                .bind(snapshot.id)
                .bind(VIEW_SCHEMA_TAG)
                .bind(tag)
                .execute(&mut **tx)
                .await?;
            }
            Ok((snapshot.id, format!("created_view:{id}")))
        })
    })
    .await
}

/// The live view of that name ended, with its tags; false, and no
/// commit, where none stands.
pub(crate) async fn end_view(db: &Db, dataset: &str, name: &str) -> Result<bool> {
    if !view_exists(db, dataset, name).await? {
        return Ok(false);
    }
    let dataset = dataset.to_string();
    let name = name.to_string();
    commit(db, move |db, tx, snapshot| {
        let (dataset, name) = (dataset.clone(), name.clone());
        Box::pin(async move {
            let Some(id) = view_id(db, tx, &dataset, &name).await? else {
                return Ok((false, String::new()));
            };
            for statement in [
                "UPDATE ducklake_tag SET end_snapshot = ? WHERE object_id = ? AND end_snapshot IS NULL",
                "UPDATE ducklake_view SET end_snapshot = ? WHERE view_id = ? AND end_snapshot IS NULL",
            ] {
                sqlx::query(&db.sql(statement))
                    .bind(snapshot.id)
                    .bind(id)
                    .execute(&mut **tx)
                    .await?;
            }
            Ok((true, format!("dropped_view:{id}")))
        })
    })
    .await
}

pub(crate) async fn view_exists(db: &Db, dataset: &str, name: &str) -> Result<bool> {
    let n: i64 = sqlx::query_scalar(&db.sql(
        "SELECT count(*) FROM ducklake_view v JOIN ducklake_schema s ON s.schema_id = v.schema_id \
         WHERE s.schema_name = ? AND v.view_name = ? AND s.end_snapshot IS NULL AND v.end_snapshot IS NULL",
    ))
    .bind(dataset)
    .bind(name)
    .fetch_one(db.pool())
    .await?;
    Ok(n > 0)
}

/// Every live view of the dataset, by name.
pub(crate) async fn views(db: &Db, dataset: &str) -> Result<Vec<ViewRow>> {
    let rows = sqlx::query(&db.sql(
        "SELECT v.view_name, v.dialect, v.\"sql\", g.\"value\" FROM ducklake_view v \
         JOIN ducklake_schema s ON s.schema_id = v.schema_id \
         LEFT JOIN ducklake_tag g ON g.object_id = v.view_id AND g.\"key\" = ? \
              AND g.end_snapshot IS NULL \
         WHERE s.schema_name = ? AND s.end_snapshot IS NULL AND v.end_snapshot IS NULL \
         ORDER BY v.view_name",
    ))
    .bind(VIEW_SCHEMA_TAG)
    .bind(dataset)
    .fetch_all(db.pool())
    .await?;
    rows.iter()
        .map(|r| {
            let tag: Option<String> = r.try_get(3)?;
            Ok(ViewRow {
                name: r.try_get(0)?,
                dialect: r.try_get(1)?,
                sql: r.try_get(2)?,
                schema: view_schema(tag.as_deref()),
            })
        })
        .collect()
}

// -- columns, version by version -------------------------------------------

async fn live_columns(db: &Db, tx: &mut Tx, table_id: i64) -> Result<Vec<ColumnRow>> {
    let rows = sqlx::query(&db.sql(
        "SELECT column_id, parent_column, column_order, column_name, column_type, \
         CAST(CAST(nulls_allowed AS INTEGER) AS BIGINT), begin_snapshot FROM ducklake_column \
         WHERE table_id = ? AND end_snapshot IS NULL ORDER BY column_order",
    ))
    .bind(table_id)
    .fetch_all(&mut **tx)
    .await?;
    rows.iter()
        .map(|r| {
            Ok(ColumnRow {
                column_id: r.try_get(0)?,
                parent: r.try_get(1)?,
                order: r.try_get(2)?,
                name: r.try_get(3)?,
                kind: r.try_get(4)?,
                nullable: r.try_get::<i64, _>(5)? != 0,
                begin: r.try_get(6)?,
            })
        })
        .collect()
}

/// The table's live derivations, by column name: its tag as the last
/// create or replace wrote it; empty where none knew any.
async fn live_exprs(db: &Db, tx: &mut Tx, table_id: i64) -> Result<HashMap<String, String>> {
    let row = sqlx::query(&db.sql(
        "SELECT \"value\" FROM ducklake_tag \
         WHERE object_id = ? AND \"key\" = ? AND end_snapshot IS NULL",
    ))
    .bind(table_id)
    .bind(EXPRS_TAG)
    .fetch_optional(&mut **tx)
    .await?;
    match row {
        Some(row) => Ok(serde_json::from_str(&row.try_get::<String, _>(0)?)?),
        None => Ok(HashMap::new()),
    }
}

/// The rows by parent, each list in column order.
fn rows_by_parent(rows: &[ColumnRow]) -> HashMap<Option<i64>, Vec<ColumnRow>> {
    let mut by_parent: HashMap<Option<i64>, Vec<ColumnRow>> = HashMap::new();
    for row in rows {
        by_parent.entry(row.parent).or_default().push(row.clone());
    }
    for list in by_parent.values_mut() {
        list.sort_by_key(|r| r.order);
    }
    by_parent
}

/// The table's columns brought to `schema`, version by version: a
/// column whose name, type and derivation are unchanged keeps its row,
/// renumbered to its place in the new order; one whose type or
/// derivation changed begins a new version under the same id; one gone
/// from the schema ends; a new one takes a new id. Whether any column
/// changed.
async fn evolve_columns(
    db: &Db,
    tx: &mut Tx,
    snapshot: &mut Snapshot,
    table_id: i64,
    schema: &Schema,
    exprs: &HashMap<String, String>,
) -> Result<bool> {
    let live = live_columns(db, tx, table_id).await?;
    let had = schema_from_rows(&live)?;
    let tags = live_exprs(db, tx, table_id).await?;
    let by_parent = rows_by_parent(&live);
    let top = by_parent.get(&None).cloned().unwrap_or_default();
    let mut order = 0i64;
    let mut handled: Vec<i64> = Vec::new();
    let mut altered = false;
    for field in schema.fields() {
        let want = exprs.get(field.name());
        let standing = top.iter().find(|r| r.name == *field.name());
        match standing {
            Some(row)
                if had
                    .field_with_name(field.name())
                    .is_ok_and(|was| same_column(was, field))
                    && tags.get(field.name()) == want =>
            {
                for (column_id, at) in renumber(row, field, &by_parent, &mut order) {
                    sqlx::query(&db.sql(
                        "UPDATE ducklake_column SET column_order = ? \
                         WHERE table_id = ? AND column_id = ? AND end_snapshot IS NULL",
                    ))
                    .bind(at)
                    .bind(table_id)
                    .bind(column_id)
                    .execute(&mut **tx)
                    .await?;
                }
                handled.push(row.column_id);
            }
            Some(row) => {
                altered = true;
                end_column(db, tx, snapshot, table_id, row, &by_parent).await?;
                insert_field(
                    db,
                    tx,
                    snapshot,
                    table_id,
                    field,
                    Some(row.column_id),
                    &mut order,
                )
                .await?;
                handled.push(row.column_id);
            }
            None => {
                altered = true;
                insert_field(db, tx, snapshot, table_id, field, None, &mut order).await?;
            }
        }
    }
    for row in &top {
        if !handled.contains(&row.column_id) {
            altered = true;
            end_column(db, tx, snapshot, table_id, row, &by_parent).await?;
        }
    }
    Ok(altered)
}

/// Whether a landed column and a field are the same column: the same
/// type in the specification's vocabulary — what the rows would read
/// back as — and the same nullability.
fn same_column(was: &Field, want: &Field) -> bool {
    was.is_nullable() == want.is_nullable()
        && folded_type(want.data_type()).is_ok_and(|kind| *was.data_type() == kind)
}

/// A type as it reads back from its rows: every scalar through the
/// specification's spelling and back, nested types rebuilt the way
/// [`schema_from_rows`] rebuilds them.
fn folded_type(kind: &DataType) -> Result<DataType> {
    Ok(match kind {
        DataType::List(f) | DataType::LargeList(f) => DataType::List(Arc::new(Field::new(
            f.name(),
            folded_type(f.data_type())?,
            f.is_nullable(),
        ))),
        DataType::Struct(fields) => DataType::Struct(Fields::from(
            fields
                .iter()
                .map(|f| {
                    Ok(Field::new(
                        f.name(),
                        folded_type(f.data_type())?,
                        f.is_nullable(),
                    ))
                })
                .collect::<Result<Vec<_>>>()?,
        )),
        DataType::Map(entries, _) => match entries.data_type() {
            DataType::Struct(kv) => {
                let kv = kv
                    .iter()
                    .map(|f| {
                        Ok(Field::new(
                            f.name(),
                            folded_type(f.data_type())?,
                            f.is_nullable(),
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?;
                DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(Fields::from(kv)),
                        false,
                    )),
                    false,
                )
            }
            _ => kind.clone(),
        },
        other => arrow_type(&ducklake_type(other)?)?,
    })
}

/// The kept column's rows at their new places: the row itself, then
/// its descendants zipped with the field's, depth first — one counter
/// over the whole schema, so every order is unique within the snapshot.
fn renumber(
    row: &ColumnRow,
    field: &Field,
    by_parent: &HashMap<Option<i64>, Vec<ColumnRow>>,
    order: &mut i64,
) -> Vec<(i64, i64)> {
    let mut out = vec![(row.column_id, *order)];
    *order += 1;
    if let Some(rows) = by_parent.get(&Some(row.column_id)) {
        for (child, kind) in rows.iter().zip(children(field.data_type())) {
            out.extend(renumber(child, &kind, by_parent, order));
        }
    }
    out
}

/// The column's row, its descendants' rows and their tags ended at
/// this snapshot.
async fn end_column(
    db: &Db,
    tx: &mut Tx,
    snapshot: &Snapshot,
    table_id: i64,
    row: &ColumnRow,
    by_parent: &HashMap<Option<i64>, Vec<ColumnRow>>,
) -> Result<()> {
    let mut ids = vec![row.column_id];
    let mut i = 0;
    while i < ids.len() {
        if let Some(rows) = by_parent.get(&Some(ids[i])) {
            ids.extend(rows.iter().map(|r| r.column_id));
        }
        i += 1;
    }
    let list = ids
        .iter()
        .map(i64::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    for table in ["ducklake_column", "ducklake_column_tag"] {
        sqlx::query(&db.sql(&format!(
            "UPDATE {table} SET end_snapshot = ? WHERE table_id = ? AND column_id IN ({list}) \
             AND end_snapshot IS NULL"
        )))
        .bind(snapshot.id)
        .bind(table_id)
        .execute(&mut **tx)
        .await?;
    }
    Ok(())
}

/// One column row to write.
struct NewRow {
    id: i64,
    parent: Option<i64>,
    order: i64,
    name: String,
    kind: String,
    nullable: bool,
}

/// The field as column rows at this snapshot — under `id` when it is
/// a new version of a standing column, a fresh id otherwise; its
/// nested fields under it, each fresh.
async fn insert_field(
    db: &Db,
    tx: &mut Tx,
    snapshot: &mut Snapshot,
    table_id: i64,
    field: &Field,
    id: Option<i64>,
    order: &mut i64,
) -> Result<()> {
    fn flatten(
        snapshot: &mut Snapshot,
        field: &Field,
        id: i64,
        parent: Option<i64>,
        order: &mut i64,
        out: &mut Vec<NewRow>,
    ) -> Result<()> {
        out.push(NewRow {
            id,
            parent,
            order: *order,
            name: field.name().clone(),
            kind: ducklake_type(field.data_type())?,
            nullable: field.is_nullable(),
        });
        *order += 1;
        for child in children(field.data_type()) {
            let child_id = snapshot.catalog_id();
            flatten(snapshot, &child, child_id, Some(id), order, out)?;
        }
        Ok(())
    }
    let top = id.unwrap_or_else(|| snapshot.catalog_id());
    let mut rows = Vec::new();
    flatten(snapshot, field, top, None, order, &mut rows)?;
    for row in rows {
        sqlx::query(&db.sql(
            "INSERT INTO ducklake_column (column_id, begin_snapshot, end_snapshot, table_id, \
             column_order, column_name, column_type, initial_default, default_value, nulls_allowed, \
             parent_column, default_value_type, default_value_dialect) \
             VALUES (?, ?, NULL, ?, ?, ?, ?, NULL, NULL, ?, ?, NULL, NULL)",
        ))
        .bind(row.id)
        .bind(snapshot.id)
        .bind(table_id)
        .bind(row.order)
        .bind(&row.name)
        .bind(&row.kind)
        .bind(row.nullable)
        .bind(row.parent)
        .execute(&mut **tx)
        .await?;
    }
    Ok(())
}

/// A nested type's child fields, in the order their rows are written.
fn children(kind: &DataType) -> Vec<Arc<Field>> {
    match kind {
        DataType::List(f) | DataType::LargeList(f) => vec![Arc::clone(f)],
        DataType::Struct(fields) => fields.iter().cloned().collect(),
        DataType::Map(entries, _) => match entries.data_type() {
            DataType::Struct(kv) => kv.iter().cloned().collect(),
            _ => Vec::new(),
        },
        _ => Vec::new(),
    }
}

/// The rows back as a schema: top-level rows in order, each nested
/// type rebuilt from the rows under it.
fn schema_from_rows(rows: &[ColumnRow]) -> Result<Schema> {
    let mut by_parent: HashMap<Option<i64>, Vec<&ColumnRow>> = HashMap::new();
    for row in rows {
        by_parent.entry(row.parent).or_default().push(row);
    }
    for list in by_parent.values_mut() {
        list.sort_by_key(|r| r.order);
    }
    fn field(row: &ColumnRow, by_parent: &HashMap<Option<i64>, Vec<&ColumnRow>>) -> Result<Field> {
        let children: Vec<Field> = by_parent
            .get(&Some(row.column_id))
            .map(|c| c.iter().map(|r| field(r, by_parent)).collect::<Result<_>>())
            .transpose()?
            .unwrap_or_default();
        let kind = match row.kind.as_str() {
            "list" => {
                let Some(item) = children.into_iter().next() else {
                    return Err(Error::Workspace(format!(
                        "column `{}` is a list with no element row",
                        row.name
                    )));
                };
                DataType::List(Arc::new(item))
            }
            "struct" => DataType::Struct(Fields::from(children)),
            "map" => {
                let entries =
                    Field::new("entries", DataType::Struct(Fields::from(children)), false);
                DataType::Map(Arc::new(entries), false)
            }
            other => arrow_type(other)?,
        };
        Ok(Field::new(&row.name, kind, row.nullable))
    }
    let top = by_parent.get(&None).cloned().unwrap_or_default();
    let fields = top
        .iter()
        .map(|r| field(r, &by_parent))
        .collect::<Result<Vec<_>>>()?;
    Ok(Schema::new(fields))
}

// -- the type vocabulary ---------------------------------------------------

/// An Arrow type as the specification spells it. The landed type set
/// is what `glossql-import` folds every source type into, so a type
/// this refuses is one no recipe lands.
pub(crate) fn ducklake_type(kind: &DataType) -> Result<String> {
    Ok(match kind {
        DataType::Boolean => "boolean".into(),
        DataType::Int8 => "int8".into(),
        DataType::Int16 => "int16".into(),
        DataType::Int32 => "int32".into(),
        DataType::Int64 => "int64".into(),
        DataType::UInt8 => "uint8".into(),
        DataType::UInt16 => "uint16".into(),
        DataType::UInt32 => "uint32".into(),
        DataType::UInt64 => "uint64".into(),
        DataType::Float32 => "float32".into(),
        DataType::Float64 => "float64".into(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "varchar".into(),
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => "blob".into(),
        DataType::Date32 | DataType::Date64 => "date".into(),
        DataType::Time64(TimeUnit::Microsecond) => "time".into(),
        DataType::Timestamp(TimeUnit::Microsecond, None) => "timestamp".into(),
        DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => "timestamptz".into(),
        DataType::Timestamp(TimeUnit::Second, None) => "timestamp_s".into(),
        DataType::Timestamp(TimeUnit::Millisecond, None) => "timestamp_ms".into(),
        DataType::Timestamp(TimeUnit::Nanosecond, None) => "timestamp_ns".into(),
        DataType::Decimal128(p, s) => format!("decimal({p}, {s})"),
        DataType::List(_) | DataType::LargeList(_) => "list".into(),
        DataType::Struct(_) => "struct".into(),
        DataType::Map(..) => "map".into(),
        other => {
            return Err(Error::Workspace(format!(
                "the type `{other}` is not one a table lands as"
            )));
        }
    })
}

/// The specification's spelling back as the Arrow type a scan reads
/// the file with.
fn arrow_type(kind: &str) -> Result<DataType> {
    Ok(match kind {
        "boolean" => DataType::Boolean,
        "int8" => DataType::Int8,
        "int16" => DataType::Int16,
        "int32" => DataType::Int32,
        "int64" => DataType::Int64,
        "uint8" => DataType::UInt8,
        "uint16" => DataType::UInt16,
        "uint32" => DataType::UInt32,
        "uint64" => DataType::UInt64,
        "float32" => DataType::Float32,
        "float64" => DataType::Float64,
        "varchar" => DataType::Utf8,
        "blob" => DataType::LargeBinary,
        "date" => DataType::Date32,
        "time" => DataType::Time64(TimeUnit::Microsecond),
        "timestamp" => DataType::Timestamp(TimeUnit::Microsecond, None),
        "timestamptz" => DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
        "timestamp_s" => DataType::Timestamp(TimeUnit::Second, None),
        "timestamp_ms" => DataType::Timestamp(TimeUnit::Millisecond, None),
        "timestamp_ns" => DataType::Timestamp(TimeUnit::Nanosecond, None),
        decimal if decimal.starts_with("decimal(") => {
            let inner = decimal.trim_start_matches("decimal(").trim_end_matches(')');
            let (p, s) = inner.split_once(',').ok_or_else(|| {
                Error::Workspace(format!("the column type `{kind}` is not decimal(P, S)"))
            })?;
            let bad = || Error::Workspace(format!("the column type `{kind}` is not decimal(P, S)"));
            DataType::Decimal128(
                p.trim().parse().map_err(|_| bad())?,
                s.trim().parse().map_err(|_| bad())?,
            )
        }
        other => {
            return Err(Error::Workspace(format!(
                "the column type `{other}` is not one this server reads"
            )));
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every landed type crosses to the specification's spelling and
    /// back unchanged.
    #[test]
    fn the_type_vocabulary_round_trips() {
        for kind in [
            DataType::Boolean,
            DataType::Int32,
            DataType::Int64,
            DataType::Float64,
            DataType::Utf8,
            DataType::LargeBinary,
            DataType::Date32,
            DataType::Time64(TimeUnit::Microsecond),
            DataType::Timestamp(TimeUnit::Microsecond, None),
            DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
            DataType::Decimal128(18, 3),
        ] {
            let spelled = ducklake_type(&kind).unwrap();
            assert_eq!(arrow_type(&spelled).unwrap(), kind, "{spelled}");
        }
    }

    /// A nested schema survives its rows: the list's element and the
    /// struct's fields hang under their parents.
    #[test]
    fn nested_columns_rebuild_from_their_rows() {
        let rows = vec![
            ColumnRow {
                column_id: 1,
                parent: None,
                order: 0,
                name: "id".into(),
                kind: "int64".into(),
                nullable: false,
                begin: 0,
            },
            ColumnRow {
                column_id: 2,
                parent: None,
                order: 1,
                name: "tags".into(),
                kind: "list".into(),
                nullable: true,
                begin: 0,
            },
            ColumnRow {
                column_id: 3,
                parent: None,
                order: 2,
                name: "who".into(),
                kind: "struct".into(),
                nullable: true,
                begin: 0,
            },
            ColumnRow {
                column_id: 4,
                parent: Some(2),
                order: 3,
                name: "item".into(),
                kind: "varchar".into(),
                nullable: true,
                begin: 0,
            },
            ColumnRow {
                column_id: 5,
                parent: Some(3),
                order: 4,
                name: "name".into(),
                kind: "varchar".into(),
                nullable: true,
                begin: 0,
            },
            ColumnRow {
                column_id: 6,
                parent: Some(3),
                order: 5,
                name: "age".into(),
                kind: "int32".into(),
                nullable: true,
                begin: 0,
            },
        ];
        let schema = schema_from_rows(&rows).unwrap();
        assert_eq!(
            schema,
            Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new(
                    "tags",
                    DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                    true
                ),
                Field::new(
                    "who",
                    DataType::Struct(Fields::from(vec![
                        Field::new("name", DataType::Utf8, true),
                        Field::new("age", DataType::Int32, true),
                    ])),
                    true
                ),
            ])
        );
    }
}
