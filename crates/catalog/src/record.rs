//! The record: where the store's relations live.
//!
//! The glossary, the declarations (functions, aspects, witnesses,
//! sources, relationships) and the measurements are each one table in
//! the catalog's own database — the workspace directory's SQLite file,
//! or the Postgres server the catalog URI names. [`Record`] is the seam
//! they cross: scan history, append rows. Three decisions make this
//! smaller than it looks:
//!
//! - **Supersession is a read.** Every row carries `seq`, the table's
//!   identity column, assigned at insert; the rules order by it and
//!   keep the latest row per key. Nothing is minted here and nothing
//!   updates in place.
//! - **The dataset is a key column.** A workspace holds many datasets,
//!   so a relation about a dataset's subjects carries a `dataset`
//!   column and a read scoped to one dataset says so in its WHERE.
//! - **Every column is text except the ones that are numbers.** The
//!   relations read back as text through `relation_rows` and the rules
//!   parse what they need; a column the database types as a number
//!   ([`RelationSpec::numbers`]) is bound and read as one and crosses
//!   the seam as its text.
//!
//! Tables are created at open, once per relation, and never altered:
//! a changed shape is a wipe and a re-bootstrap. The two dialects
//! differ in their placeholders and their identity column, and in
//! nothing else this speaks.

use std::collections::HashMap;

use sqlx::any::{AnyPoolOptions, AnyRow};
use sqlx::{AnyPool, Row as _};

/// One stored row: its cells in the relation's declared column order,
/// and what ordered the write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
    pub cells: Vec<Option<String>>,
    /// The write's position in the relation's order: the identity the
    /// database assigned at insert.
    pub seq: i64,
}

impl Row {
    pub fn new(cells: Vec<Option<String>>, seq: i64) -> Self {
        Row { cells, seq }
    }

    /// A cell by position in the relation's column order.
    pub fn get(&self, i: usize) -> Option<&str> {
        self.cells.get(i).and_then(|c| c.as_deref())
    }
}

/// A column the database types as a number rather than text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Number {
    Integer,
    Real,
}

/// The shape of one relation the seam carries — declared by the store,
/// which owns the column order.
#[derive(Debug, Clone)]
pub struct RelationSpec {
    pub name: &'static str,
    pub columns: &'static [&'static str],
    /// The columns that are numbers, each naming an entry of
    /// `columns`; every other column is text.
    pub numbers: &'static [(&'static str, Number)],
}

impl RelationSpec {
    fn number(&self, column: &str) -> Option<Number> {
        self.numbers
            .iter()
            .find(|(name, _)| *name == column)
            .map(|(_, number)| *number)
    }
}

/// How the dialect spells a placeholder: SQLite takes question marks,
/// Postgres numbers its parameters.
#[derive(Debug, Clone, Copy)]
enum Bind {
    QMark,
    Dollar,
}

impl Bind {
    fn placeholder(self, i: usize) -> String {
        match self {
            Bind::QMark => "?".into(),
            Bind::Dollar => format!("${}", i + 1),
        }
    }
}

/// The store's relations, each one table in the catalog's database.
/// The database the catalog tables and the record share: one pool,
/// one bind style — `?` for SQLite, `$n` for Postgres — behind sqlx's
/// `any` driver.
#[derive(Clone)]
pub struct Db {
    pool: AnyPool,
    bind: Bind,
}

impl std::fmt::Debug for Db {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Db").field("bind", &self.bind).finish()
    }
}

impl Db {
    /// One pool on the URI (`sqlite:<file>` or `postgres://…`).
    pub async fn connect(uri: &str) -> crate::Result<Self> {
        let (scheme, _) = uri
            .split_once(':')
            .ok_or_else(|| crate::Error::Workspace("the catalog URI names no scheme".into()))?;
        let bind = match scheme.to_ascii_lowercase().as_str() {
            "sqlite" => Bind::QMark,
            "postgres" | "postgresql" => Bind::Dollar,
            other => {
                return Err(crate::Error::Workspace(format!(
                    "the catalog URI scheme `{other}` is not one this binary speaks — sqlite or postgres"
                )));
            }
        };
        sqlx::any::install_default_drivers();
        let pool = AnyPoolOptions::new().connect(uri).await?;
        Ok(Db { pool, bind })
    }

    pub(crate) fn pool(&self) -> &AnyPool {
        &self.pool
    }

    /// A statement written with `?` placeholders, in this database's
    /// bind style. No literal in the statements written here holds a
    /// question mark.
    pub(crate) fn sql(&self, text: &str) -> String {
        match self.bind {
            Bind::QMark => text.to_string(),
            Bind::Dollar => {
                let mut out = String::with_capacity(text.len() + 8);
                let mut n = 0;
                for c in text.chars() {
                    if c == '?' {
                        n += 1;
                        out.push('$');
                        out.push_str(&n.to_string());
                    } else {
                        out.push(c);
                    }
                }
                out
            }
        }
    }
}

pub struct Record {
    pool: AnyPool,
    bind: Bind,
    specs: HashMap<String, RelationSpec>,
}

impl std::fmt::Debug for Record {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Record").finish_non_exhaustive()
    }
}

/// The table a relation lives in. Prefixed so the store's tables sit
/// beside the catalog's own (`ducklake_*`) without a name of ours
/// colliding with one of theirs.
fn table(relation: &str) -> String {
    format!("\"glossql_{relation}\"")
}

impl Record {
    /// Open the record on the catalog's database — the URI the catalog
    /// itself was opened on — and create every relation's table that
    /// is not there yet.
    /// The relations on the database the lake opened, each created if
    /// absent.
    pub async fn open(db: &Db, relations: &[RelationSpec]) -> crate::Result<Self> {
        let record = Record {
            pool: db.pool.clone(),
            bind: db.bind,
            specs: relations
                .iter()
                .map(|s| (s.name.to_string(), s.clone()))
                .collect(),
        };
        for spec in relations {
            record.create(spec).await?;
        }
        Ok(record)
    }

    fn spec(&self, relation: &str) -> crate::Result<&RelationSpec> {
        self.specs
            .get(relation)
            .ok_or_else(|| crate::Error::Workspace(format!("no relation `{relation}`")))
    }

    /// The relation's table, if it is not there yet. The identity
    /// column is the one thing the two dialects spell differently.
    async fn create(&self, spec: &RelationSpec) -> crate::Result<()> {
        let seq = match self.bind {
            Bind::QMark => "\"seq\" INTEGER PRIMARY KEY AUTOINCREMENT",
            Bind::Dollar => "\"seq\" BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY",
        };
        let columns = spec
            .columns
            .iter()
            .map(|c| {
                let kind = match (spec.number(c), self.bind) {
                    (None, _) => "TEXT",
                    (Some(Number::Integer), Bind::QMark) => "INTEGER",
                    (Some(Number::Integer), Bind::Dollar) => "BIGINT",
                    (Some(Number::Real), Bind::QMark) => "REAL",
                    (Some(Number::Real), Bind::Dollar) => "DOUBLE PRECISION",
                };
                format!("\"{c}\" {kind}")
            })
            .collect::<Vec<_>>()
            .join(", ");
        sqlx::query(&format!(
            "CREATE TABLE IF NOT EXISTS {} ({seq}, {columns})",
            table(spec.name)
        ))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn select(
        &self,
        relation: &str,
        filter: Option<(&str, &str)>,
    ) -> crate::Result<Vec<Row>> {
        let span = tracing::info_span!("scan", relation, rows = tracing::field::Empty);
        tracing::Instrument::instrument(self.fetch(relation, filter), span).await
    }

    /// The select, under its span.
    async fn fetch(&self, relation: &str, filter: Option<(&str, &str)>) -> crate::Result<Vec<Row>> {
        let spec = self.spec(relation)?;
        let columns = spec
            .columns
            .iter()
            .map(|c| format!("\"{c}\""))
            .collect::<Vec<_>>()
            .join(", ");
        let mut sql = format!("SELECT {columns}, \"seq\" FROM {}", table(relation));
        if let Some((column, _)) = filter {
            if !spec.columns.contains(&column) {
                return Err(crate::Error::Workspace(format!(
                    "`{relation}` has no column `{column}`"
                )));
            }
            sql.push_str(&format!(
                " WHERE \"{column}\" = {}",
                self.bind.placeholder(0)
            ));
        }
        sql.push_str(" ORDER BY \"seq\"");
        let mut query = sqlx::query(&sql);
        if let Some((_, value)) = filter {
            query = query.bind(value.to_string());
        }
        let rows = query.fetch_all(&self.pool).await?;
        tracing::Span::current().record("rows", rows.len());
        rows.iter().map(|row| decode(spec, row)).collect()
    }
}

/// One fetched row as the seam's text cells: a number column reads as
/// its number and crosses as that number's text.
fn decode(spec: &RelationSpec, row: &AnyRow) -> crate::Result<Row> {
    let cells = spec
        .columns
        .iter()
        .enumerate()
        .map(|(i, c)| -> crate::Result<Option<String>> {
            Ok(match spec.number(c) {
                None => row.try_get::<Option<String>, _>(i)?,
                Some(Number::Integer) => row.try_get::<Option<i64>, _>(i)?.map(|n| n.to_string()),
                Some(Number::Real) => row.try_get::<Option<f64>, _>(i)?.map(|n| n.to_string()),
            })
        })
        .collect::<crate::Result<Vec<_>>>()?;
    let seq: i64 = row.try_get(spec.columns.len())?;
    Ok(Row::new(cells, seq))
}

/// Deliberately three methods: anything more is a rule, and rules live
/// in `glossql-glossary`. `scan` hands back **history**, not the
/// current view — supersession is `rules::latest_by` applied on top.
/// `append` adds rows — replacement is a later row, never an update, so
/// nothing here mutates.
impl Record {
    /// Every row ever written to the relation, in write order.
    pub async fn scan(&self, relation: &str) -> crate::Result<Vec<Row>> {
        self.select(relation, None).await
    }

    /// The rows whose `column` equals `value`, in write order — the
    /// predicate in the query, so a big relation's history is not read
    /// to serve one key.
    pub async fn scan_where(
        &self,
        relation: &str,
        column: &str,
        value: &str,
    ) -> crate::Result<Vec<Row>> {
        self.select(relation, Some((column, value))).await
    }

    /// Append rows as one write, in the order given: each row takes
    /// the next identity, so a caller that appends two rows sharing a
    /// supersession key gets the later one.
    pub async fn append(
        &self,
        relation: &str,
        rows: Vec<Vec<Option<String>>>,
    ) -> crate::Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let span = tracing::info_span!("append", relation, rows = rows.len());
        tracing::Instrument::instrument(self.insert(relation, rows), span).await
    }

    /// The write, under its span.
    async fn insert(&self, relation: &str, rows: Vec<Vec<Option<String>>>) -> crate::Result<()> {
        let spec = self.spec(relation)?;
        let columns = spec
            .columns
            .iter()
            .map(|c| format!("\"{c}\""))
            .collect::<Vec<_>>()
            .join(", ");
        let mut tx = self.pool.begin().await?;
        for row in &rows {
            // A missing cell is the literal NULL in the statement, never a
            // bound null: the `any` driver types a bound null on its own
            // (a double's null goes out as a float4's), and Postgres
            // holds that type in the prepared statement for every later
            // bind of the same text. The text varies with the null
            // pattern instead, and each variant is prepared with the
            // types of what it actually binds.
            let mut values = Vec::with_capacity(spec.columns.len());
            let mut bound = 0;
            for i in 0..spec.columns.len() {
                values.push(match row.get(i) {
                    Some(Some(_)) => {
                        bound += 1;
                        self.bind.placeholder(bound - 1)
                    }
                    _ => "NULL".to_string(),
                });
            }
            let sql = format!(
                "INSERT INTO {} ({columns}) VALUES ({})",
                table(relation),
                values.join(", ")
            );
            let mut query = sqlx::query(&sql);
            for (i, column) in spec.columns.iter().enumerate() {
                let Some(cell) = row.get(i).cloned().flatten() else {
                    continue;
                };
                query = match spec.number(column) {
                    None => query.bind(cell),
                    Some(Number::Integer) => query.bind(parse::<i64>(relation, column, cell)?),
                    Some(Number::Real) => query.bind(parse::<f64>(relation, column, cell)?),
                };
            }
            query.execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// Every relation at its version: the highest `seq` it holds,
    /// `None` while nothing has been written to it. What the store's
    /// version and its pin are made of.
    pub async fn versions(&self) -> crate::Result<Vec<(String, Option<i64>)>> {
        tracing::Instrument::instrument(self.max_seqs(), tracing::info_span!("versions")).await
    }

    /// The reads behind [`Record::versions`], under their span.
    async fn max_seqs(&self) -> crate::Result<Vec<(String, Option<i64>)>> {
        let mut names: Vec<&String> = self.specs.keys().collect();
        names.sort();
        let reads = names.iter().map(|name| async move {
            let row = sqlx::query(&format!("SELECT MAX(\"seq\") FROM {}", table(name)))
                .fetch_one(&self.pool)
                .await?;
            let max: Option<i64> = row.try_get(0)?;
            crate::Result::Ok((name.to_string(), max))
        });
        futures::future::try_join_all(reads).await
    }
}

/// A number column's cell, parsed as the number the database holds it
/// as. The writer owns the shape: text that is not a number is its
/// error, named here rather than stored as one.
fn parse<T: std::str::FromStr>(relation: &str, column: &str, cell: String) -> crate::Result<T> {
    cell.parse::<T>().map_err(|_| {
        crate::Error::Workspace(format!(
            "`{relation}.{column}` is a number and `{cell}` is not one"
        ))
    })
}
