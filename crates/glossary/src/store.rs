//! The sqlx-backed store. One SQLite file per workspace (`:memory:` in
//! tests); Postgres later is a connection string, so every query here stays
//! in portable SQL. Admission (SPEC.md §5.2, §7.1) happens on the write
//! paths; supersession is the `NOT EXISTS` read predicate, never an update.

use serde_json::Value;
use sqlx::Row as _;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions};

use glossql_parser::{
    AspectDecl, AspectKind, DatasetDecl, FunctionDecl, FunctionScope, Grain, JsonBody, RecipeDecl,
    SourceDecl, Speaker, WitnessDecl,
};

use crate::schemas::grounding_schema;
use crate::types::{
    Actor, ActorKind, AttestRow, CacheRow, CollapsedRow, Error, FunctionRow, RawRow,
    RecipeAdmission, RecipeRow, Result, WitnessRow,
};

/// What a read sweeps over (SPEC.md §5.3, §7.2): the whole dataset, or a
/// subject and everything under it (columns of a table, relationships rooted
/// at it).
#[derive(Debug, Clone)]
pub enum Scope {
    Dataset,
    Subject(String),
}

/// What the session knows and the store cannot: the subjects that exist
/// (tables and columns from the data plane — the disclosure grid enumerates
/// them so absence shows as a row, never as omission) and each table's
/// current snapshot (the staleness comparison). Empty context still collapses
/// correctly; it just cannot show `unassessed` subjects nobody wrote about
/// or mark snapshot staleness.
#[derive(Debug, Clone, Default)]
pub struct ReadContext {
    pub universe: Vec<String>,
    pub snapshots: std::collections::HashMap<String, i64>,
}

/// One current slot under (subject, aspect): a gloss (human or agent) or a
/// witness-bound function's cached output. The collapse and the raw read
/// both build from these.
#[derive(Debug, Clone)]
struct Slot {
    subject: String,
    aspect: String,
    /// 0 = human, 1 = agent, 2 = function — the precedence order.
    rank: u8,
    actor: String,
    witness: Option<String>,
    body: String,
    written_at: String,
    snapshot_id: Option<i64>,
}

impl Scope {
    /// Predicate over a `subject` column: exact, descendant (`s.…`), or a
    /// pair path the subject participates in — from either side (`s -> …`,
    /// `s <-> …`, `… -> s`, `… -> s.…`). The far endpoint's own context is
    /// never pulled in.
    fn predicate(&self, column: &str) -> (String, Vec<String>) {
        match self {
            Scope::Dataset => ("1 = 1".into(), vec![]),
            Scope::Subject(s) => {
                // The subject is data, not pattern: `order_items` must not
                // match `orderxitems` through LIKE's `_`, and a subject
                // carrying `%` must not sweep the dataset.
                let e = like_escape(s);
                (
                    format!(
                        "({column} = ? OR {column} LIKE ? ESCAPE '\\' \
                          OR {column} LIKE ? ESCAPE '\\' \
                          OR {column} LIKE ? ESCAPE '\\' \
                          OR {column} LIKE ? ESCAPE '\\')"
                    ),
                    vec![
                        s.clone(),
                        format!("{e}.%"),
                        format!("{e} %"),
                        format!("%> {e}"),
                        format!("%> {e}.%"),
                    ],
                )
            }
        }
    }
}

/// A `;` or `$` outside a single-quoted literal — the two characters that
/// let forwarded SQL become more than the one statement it was parsed as.
/// SQLite's own quoting rules decide what "outside" means here, because
/// SQLite is what will execute the string.
fn stray_statement_char(sql: &str) -> Option<char> {
    let mut quoted = false;
    let mut chars = sql.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\'' if quoted => {
                // `''` is an escaped quote, not the end of the literal.
                if chars.peek() == Some(&'\'') {
                    chars.next();
                } else {
                    quoted = false;
                }
            }
            '\'' => quoted = true,
            ';' | '$' if !quoted => return Some(c),
            _ => {}
        }
    }
    None
}

/// The row filter of a rendered `DELETE`, for re-aiming at a pre-select.
enum DeleteFilter {
    /// No `WHERE` — every row goes.
    All,
    /// The predicate text after `WHERE`.
    Where(String),
    /// The statement's shape hides which rows die — the caller goes blunt.
    Unknown,
}

/// Lift the filter from the session's rendered delete (sqlparser Display:
/// `DELETE FROM t [USING …] [WHERE pred] [RETURNING …] [ORDER BY …]
/// [LIMIT n]`, keywords uppercase). Any of the non-WHERE clause keywords
/// outside a literal — even inside a predicate's subquery — means the
/// predicate alone may not describe the doomed rows (a `LIMIT`ed delete
/// removes fewer than the predicate matches), so `Unknown`.
fn delete_filter(sql: &str) -> DeleteFilter {
    for kw in ["USING", "RETURNING", "ORDER", "LIMIT"] {
        if unquoted_word(sql, kw).is_some() {
            return DeleteFilter::Unknown;
        }
    }
    match unquoted_word(sql, "WHERE") {
        Some(i) => DeleteFilter::Where(sql[i + "WHERE".len()..].trim().to_string()),
        None => DeleteFilter::All,
    }
}

/// Byte offset of `word` as a standalone word outside single-quoted
/// literals — the same quoting rules as [`stray_statement_char`]. A `"`
/// counts as a word character, so a double-quoted identifier never matches.
fn unquoted_word(sql: &str, word: &str) -> Option<usize> {
    let is_word = |c: char| c.is_ascii_alphanumeric() || c == '_' || c == '"';
    let mut quoted = false;
    let mut prev: Option<char> = None;
    let mut chars = sql.char_indices().peekable();
    while let Some((i, c)) = chars.next() {
        match c {
            '\'' if quoted => {
                if chars.peek().map(|(_, c)| *c) == Some('\'') {
                    chars.next();
                } else {
                    quoted = false;
                }
            }
            '\'' => quoted = true,
            _ if !quoted
                && prev.is_none_or(|p| !is_word(p))
                && sql[i..].starts_with(word)
                && sql[i + word.len()..]
                    .chars()
                    .next()
                    .is_none_or(|n| !is_word(n)) =>
            {
                return Some(i);
            }
            _ => {}
        }
        prev = Some(c);
    }
    None
}

/// LIKE metacharacters in a literal subject, under `ESCAPE '\'`.
fn like_escape(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}

/// The schema, created at open. CREATE-only — there are no existing
/// databases to migrate (ruled 2026-08-06): a store predating a schema
/// change is wiped and re-bootstrapped, never patched in place.
const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS sources (
  name TEXT PRIMARY KEY,
  settings TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS datasets (
  name TEXT PRIMARY KEY,
  settings TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS recipes (
  dataset TEXT NOT NULL,
  table_name TEXT NOT NULL,
  source TEXT NOT NULL,
  sql TEXT NOT NULL,
  PRIMARY KEY (dataset, table_name)
);
CREATE TABLE IF NOT EXISTS relationships (
  dataset TEXT NOT NULL,
  left_path TEXT NOT NULL,
  op TEXT NOT NULL,
  right_path TEXT NOT NULL,
  PRIMARY KEY (dataset, left_path, op, right_path)
);
CREATE TABLE IF NOT EXISTS aspects (
  name TEXT PRIMARY KEY,
  schema TEXT NOT NULL,
  kind TEXT NOT NULL,
  grains TEXT,
  condition_aspect TEXT,
  condition_value TEXT
);
CREATE TABLE IF NOT EXISTS functions (
  name TEXT PRIMARY KEY,
  scope_dataset TEXT,
  script TEXT NOT NULL,
  accepts TEXT,
  returns TEXT
);
CREATE TABLE IF NOT EXISTS witnesses (
  name TEXT PRIMARY KEY,
  aspect TEXT NOT NULL,
  speakers TEXT NOT NULL,
  detector TEXT,
  threshold REAL
);
CREATE TABLE IF NOT EXISTS glossary (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  dataset TEXT NOT NULL,
  subject TEXT NOT NULL,
  aspect TEXT NOT NULL,
  actor_kind TEXT NOT NULL,
  actor_id TEXT NOT NULL,
  body TEXT NOT NULL,
  written_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  snapshot_id INTEGER
);
CREATE TABLE IF NOT EXISTS cache (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  dataset TEXT NOT NULL,
  subject TEXT NOT NULL,
  function TEXT NOT NULL,
  witness TEXT,
  body TEXT NOT NULL,
  computed_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  snapshot_id INTEGER
);
CREATE TABLE IF NOT EXISTS imports (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  dataset TEXT NOT NULL,
  table_name TEXT NOT NULL,
  source_rows INTEGER NOT NULL,
  landed_rows INTEGER NOT NULL,
  cast_failures TEXT,
  imported_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
CREATE INDEX IF NOT EXISTS glossary_key
  ON glossary (dataset, subject, aspect, actor_kind, id);
CREATE INDEX IF NOT EXISTS cache_key
  ON cache (dataset, subject, function, witness, id);
"#;

/// A store relation readable as a plain table through the doors. The
/// one place its shape lives: `columns` names exactly what `sql`
/// selects, in order — every other list (the session's read planner,
/// the doors' cap policy) derives from here.
pub struct Relation {
    pub name: &'static str,
    pub columns: &'static [&'static str],
    sql: &'static str,
}

/// Every relation [`Store::relation_rows`] serves — the glossary, the
/// evidence, and the declarations, so an agent lists what exists
/// instead of being told.
pub const RELATIONS: &[Relation] = &[
    Relation {
        name: "glossary",
        columns: &[
            "dataset",
            "subject",
            "aspect",
            "actor_kind",
            "actor_id",
            "body",
            "written_at",
            "snapshot_id",
        ],
        sql: "SELECT dataset, subject, aspect, actor_kind, actor_id, body, written_at, \
                     CAST(snapshot_id AS TEXT) AS snapshot_id \
              FROM glossary ORDER BY id",
    },
    Relation {
        name: "cache",
        columns: &[
            "dataset",
            "subject",
            "function",
            "witness",
            "body",
            "computed_at",
            "snapshot_id",
        ],
        sql: "SELECT dataset, subject, function, witness, body, computed_at, \
                     CAST(snapshot_id AS TEXT) AS snapshot_id \
              FROM cache ORDER BY id",
    },
    Relation {
        name: "imports",
        columns: &[
            "dataset",
            "table_name",
            "source_rows",
            "landed_rows",
            "dropped_rows_count",
            "cast_failures",
            "imported_at",
        ],
        // A fan-out join lands more rows than the source scan counted;
        // the difference is then unknown, never negative (2026-08-12).
        sql: "SELECT dataset, table_name, CAST(source_rows AS TEXT) AS source_rows, \
                     CAST(landed_rows AS TEXT) AS landed_rows, \
                     CASE WHEN landed_rows > source_rows THEN NULL \
                          ELSE CAST(source_rows - landed_rows AS TEXT) END AS dropped_rows_count, \
                     cast_failures, imported_at \
              FROM imports ORDER BY id",
    },
    Relation {
        name: "functions",
        columns: &["name", "scope", "script", "accepts", "returns"],
        sql: "SELECT name, COALESCE(scope_dataset, 'GLOBAL') AS scope, script, \
                     accepts, returns \
              FROM functions ORDER BY name",
    },
    Relation {
        name: "aspects",
        columns: &["name", "kind", "grains", "condition", "schema"],
        sql: "SELECT name, kind, grains, \
                     CASE WHEN condition_aspect IS NULL THEN NULL \
                          ELSE condition_aspect || ' = ''' || condition_value || '''' \
                     END AS condition, \
                     schema FROM aspects ORDER BY name",
    },
    Relation {
        name: "witnesses",
        columns: &["name", "aspect", "speakers", "detector", "threshold"],
        sql: "SELECT name, aspect, speakers, detector, \
                     CAST(threshold AS TEXT) AS threshold \
              FROM witnesses ORDER BY name",
    },
    Relation {
        name: "sources",
        columns: &["name", "settings"],
        sql: "SELECT name, settings FROM sources ORDER BY name",
    },
    Relation {
        name: "datasets",
        columns: &["name", "settings"],
        sql: "SELECT name, settings FROM datasets ORDER BY name",
    },
    Relation {
        name: "relationships",
        columns: &["dataset", "left_path", "op", "right_path"],
        sql: "SELECT dataset, left_path, op, right_path FROM relationships \
              ORDER BY dataset, left_path, op, right_path",
    },
];

/// The column shape of a readable store relation, `None` for any other
/// name — the lookup the session's planner and the doors share.
pub fn relation_columns(name: &str) -> Option<&'static [&'static str]> {
    RELATIONS.iter().find(|r| r.name == name).map(|r| r.columns)
}

/// The declaration relations `ACCEPTS` admits as invalidation edges
/// (ruled 2026-08-05): no context entry arrives — the script reads them
/// as tables through the door — but a write to the relation kills the
/// cache like an aspect value would. Only wired relations are admissible;
/// an unwired name would be a silent no-op edge, so the rest join as
/// consumers appear.
pub fn accepts_relation(name: &str) -> bool {
    matches!(name, "relationships" | "imports" | "glossary")
}

/// The grain of a canonical subject spelling: the dataset itself, a pair
/// path, `table.column` (a composite endpoint's tuple counts as its pair's
/// grain, never a column), or a bare table.
pub fn grain_of(dataset: &str, subject: &str) -> &'static str {
    if subject == dataset {
        "dataset"
    } else if subject.contains(" -> ") || subject.contains(" <-> ") {
        "relationship"
    } else if subject.contains('.') {
        "column"
    } else {
        "table"
    }
}

/// Grain admission (ruled 2026-08-05): an aspect declared `ON grain, …`
/// only accepts subjects of those grains; `None` (no clause) admits all.
/// A bare name is table-shaped; when it names a declared source it also
/// satisfies SOURCE grain (ruled 2026-08-12) — `is_source` is the
/// caller's lookup against the `sources` relation.
pub fn admit_grain(
    aspect: &str,
    grains: Option<&str>,
    dataset: &str,
    subject: &str,
    is_source: bool,
) -> Result<()> {
    let Some(declared) = grains else {
        return Ok(());
    };
    let grain = grain_of(dataset, subject);
    if declared.split(',').any(|g| g == grain) {
        return Ok(());
    }
    if is_source && grain == "table" && declared.split(',').any(|g| g == "source") {
        return Ok(());
    }
    Err(Error::GrainRefused {
        aspect: aspect.into(),
        subject: subject.into(),
        grain,
        declared: declared.to_uppercase().replace(',', ", "),
    })
}

#[derive(Debug, Clone)]
pub struct Store {
    pool: SqlitePool,
}

/// What the connect-time brief is composed from — see
/// [`Store::brief_counts`].
#[derive(Debug, Clone)]
pub struct BriefCounts {
    pub human_writings: i64,
    pub latest_human_at: Option<String>,
    pub approvals_pending: i64,
}

impl Store {
    pub async fn open(url: &str) -> Result<Self> {
        // WAL, so a read never waits behind the write of a gloss; a busy
        // timeout, so a concurrent writer waits instead of erroring. sqlx
        // sets neither by default (sqlx-sqlite-0.8.6 options/mod.rs:177).
        let options: SqliteConnectOptions = url
            .parse::<SqliteConnectOptions>()?
            .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
            .busy_timeout(std::time::Duration::from_secs(5));
        let pool = SqlitePoolOptions::new().connect_with(options).await?;
        sqlx::raw_sql(SCHEMA).execute(&pool).await?;
        Ok(Store { pool })
    }

    /// In-memory store. One connection, or every pool checkout would see a
    /// different empty database.
    pub async fn open_memory() -> Result<Self> {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await?;
        sqlx::raw_sql(SCHEMA).execute(&pool).await?;
        Ok(Store { pool })
    }

    /// The connect-time brief's raw counts (ruled 2026-08-12, delivery
    /// option B): what an agent connecting right now should know exists
    /// before it acts. Cheap by design — two queries, no collapse.
    pub async fn brief_counts(&self) -> Result<BriefCounts> {
        let humans = sqlx::query(
            "SELECT count(*) AS n, max(written_at) AS latest \
             FROM glossary WHERE actor_kind = 'human'",
        )
        .fetch_one(&self.pool)
        .await?;
        let approvals = sqlx::query(
            "SELECT count(*) AS n FROM glossary h \
             WHERE h.aspect = 'recipe_change' AND h.actor_kind = 'human' \
               AND json_extract(h.body, '$.table') IS NOT NULL \
               AND NOT EXISTS (SELECT 1 FROM glossary h2 \
                               WHERE h2.subject = h.subject AND h2.aspect = h.aspect \
                                 AND h2.actor_kind = 'human' AND h2.written_at > h.written_at) \
               AND NOT EXISTS (SELECT 1 FROM imports i \
                               WHERE i.table_name = json_extract(h.body, '$.table') \
                                 AND i.imported_at >= h.written_at)",
        )
        .fetch_one(&self.pool)
        .await?;
        Ok(BriefCounts {
            human_writings: humans.get::<i64, _>("n"),
            latest_human_at: humans.get::<Option<String>, _>("latest"),
            approvals_pending: approvals.get::<i64, _>("n"),
        })
    }

    // -- declarations ----------------------------------------------------

    pub async fn declare_source(&self, decl: &SourceDecl) -> Result<()> {
        sqlx::query("INSERT OR REPLACE INTO sources (name, settings) VALUES (?, ?)")
            .bind(decl.name.value.as_str())
            .bind(settings_json(&decl.settings))
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn declare_dataset(&self, decl: &DatasetDecl) -> Result<()> {
        sqlx::query("INSERT OR REPLACE INTO datasets (name, settings) VALUES (?, ?)")
            .bind(decl.name.value.as_str())
            .bind(settings_json(&decl.settings))
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Statement identity is content (SPEC.md §3): an unchanged recipe is a
    /// no-op; a changed one **supersedes and re-lands** (project lead,
    /// 2026-08-06 — the earlier refusal sent two consecutive runs into the
    /// same dead end on a post-landing defect). The recipe row supersedes
    /// like any declaration; glosses stay — they are knowledge, and
    /// snapshot ids disclose their age against the fresh landing.
    /// What the declaration would do, decided before anything is written:
    /// the row lands in [`Store::put_recipe`] only once the landing it
    /// describes has succeeded. A recipe that cannot materialize leaves no
    /// row behind claiming it did — the retry would otherwise answer
    /// `unchanged` over an empty table.
    pub async fn recipe_admission(&self, decl: &RecipeDecl) -> Result<RecipeAdmission> {
        let dataset = decl.dataset.value.as_str();
        let table = decl.table.value.as_str();
        self.require("dataset", "datasets", dataset).await?;
        self.require("source", "sources", decl.source.value.as_str())
            .await?;
        // A landed table is readable under its bare name; the store's own
        // relations answer to those names first, so the table would be
        // unreachable and the relation would look like data.
        if relation_columns(table).is_some() || table.eq_ignore_ascii_case("attest") {
            return Err(Error::ReservedTableName(table.into()));
        }
        Ok(match self.recipe(dataset, table).await? {
            None => RecipeAdmission::Created,
            Some(prior) if prior.source == decl.source.value && prior.sql == decl.sql => {
                RecipeAdmission::Unchanged
            }
            Some(_) => RecipeAdmission::Replaced,
        })
    }

    pub async fn put_recipe(&self, decl: &RecipeDecl) -> Result<()> {
        sqlx::query(
            "INSERT OR REPLACE INTO recipes (dataset, table_name, source, sql) VALUES (?, ?, ?, ?)",
        )
        .bind(decl.dataset.value.as_str())
        .bind(decl.table.value.as_str())
        .bind(decl.source.value.as_str())
        .bind(decl.sql.as_str())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn recipe(&self, dataset: &str, table: &str) -> Result<Option<RecipeRow>> {
        let row =
            sqlx::query("SELECT source, sql FROM recipes WHERE dataset = ? AND table_name = ?")
                .bind(dataset)
                .bind(table)
                .fetch_optional(&self.pool)
                .await?;
        Ok(row.map(|r| RecipeRow {
            source: r.get("source"),
            sql: r.get("sql"),
        }))
    }

    pub async fn source_settings(&self, name: &str) -> Result<Option<Value>> {
        let row = sqlx::query("SELECT settings FROM sources WHERE name = ?")
            .bind(name)
            .fetch_optional(&self.pool)
            .await?;
        row.map(|r| {
            serde_json::from_str(&r.get::<String, _>("settings"))
                .map_err(|e| Error::Corrupt(e.to_string()))
        })
        .transpose()
    }

    /// Endpoints arrive canonical (dataset-relative `table.column`); the
    /// session resolves prefixes first.
    pub async fn declare_relationship(
        &self,
        dataset: &str,
        left: &str,
        op: &str,
        right: &str,
    ) -> Result<()> {
        sqlx::query(
            "INSERT OR REPLACE INTO relationships (dataset, left_path, op, right_path) \
             VALUES (?, ?, ?, ?)",
        )
        .bind(dataset)
        .bind(left)
        .bind(op)
        .bind(right)
        .execute(&self.pool)
        .await?;
        // A new edge invalidates dataset-wide through the same ACCEPTS
        // machinery an aspect value uses (ruled 2026-08-05): an edge can
        // change any subject's evidence, and the next call repopulates.
        self.invalidate(dataset, "relationships", dataset).await?;
        Ok(())
    }

    /// Content-identical re-declaration is a no-op; changing an aspect while
    /// glosses under it exist is refused — delete them first (SPEC.md §5.1).
    pub async fn declare_aspect(&self, decl: &AspectDecl) -> Result<()> {
        if let Err(e) = jsonschema::validator_for(&decl.schema.value) {
            return Err(Error::BadAspectSchema {
                name: decl.name.value.clone(),
                detail: e.to_string(),
            });
        }
        let name = decl.name.value.as_str();
        let declared_grains = grains_str(&decl.grains);
        // Conditional relevance (ruled 2026-08-14): the referenced
        // sibling aspect must exist, and when its schema pins `value`
        // to an enum the literal must be a member — a condition nobody
        // could ever satisfy is a typo, refused at the door.
        let declared_condition = decl
            .condition
            .as_ref()
            .map(|(a, v)| (a.value.clone(), v.clone()));
        if let Some((cond_aspect, cond_value)) = &declared_condition {
            let Some((cond_schema, _, _)) = self.aspect(cond_aspect).await? else {
                return Err(Error::BadCondition {
                    name: name.into(),
                    detail: format!("WHEN references undeclared aspect `{cond_aspect}`"),
                });
            };
            if let Some(allowed) = cond_schema
                .pointer("/properties/value/enum")
                .and_then(Value::as_array)
                && !allowed
                    .iter()
                    .any(|v| v.as_str() == Some(cond_value.as_str()))
            {
                return Err(Error::BadCondition {
                    name: name.into(),
                    detail: format!(
                        "'{cond_value}' is not among `{cond_aspect}`'s declared values {allowed:?}"
                    ),
                });
            }
        }
        if let Some((schema, kind, grains)) = self.aspect(name).await? {
            let existing_condition: Option<(String, String)> = sqlx::query(
                "SELECT condition_aspect, condition_value FROM aspects WHERE name = ?",
            )
            .bind(name)
            .fetch_optional(&self.pool)
            .await?
            .and_then(|r| {
                match (
                    r.get::<Option<String>, _>("condition_aspect"),
                    r.get::<Option<String>, _>("condition_value"),
                ) {
                    (Some(a), Some(v)) => Some((a, v)),
                    _ => None,
                }
            });
            if schema == decl.schema.value
                && kind == kind_str(decl.kind)
                && grains == declared_grains
                && existing_condition == declared_condition
            {
                return Ok(());
            }
            let glosses: i64 = sqlx::query("SELECT count(*) AS n FROM glossary WHERE aspect = ?")
                .bind(name)
                .fetch_one(&self.pool)
                .await?
                .get("n");
            if glosses > 0 {
                return Err(Error::AspectInUse {
                    name: name.into(),
                    glosses,
                });
            }
            // Function values sit under the aspect too, through RETURNS —
            // and a MEASUREMENT aspect can only ever hold those, since
            // glossing one is refused. Without this the schema could change
            // under values that were validated against the old one.
            let values: i64 = sqlx::query(
                "SELECT count(*) AS n FROM cache WHERE witness IS NULL AND function IN \
                 (SELECT name FROM functions WHERE returns = ?)",
            )
            .bind(name)
            .fetch_one(&self.pool)
            .await?
            .get("n");
            if values > 0 {
                return Err(Error::AspectValued {
                    name: name.into(),
                    values,
                });
            }
        }
        sqlx::query(
            "INSERT OR REPLACE INTO aspects \
             (name, schema, kind, grains, condition_aspect, condition_value) \
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(decl.name.value.as_str())
        .bind(decl.schema.raw.as_str())
        .bind(kind_str(decl.kind))
        .bind(declared_grains)
        .bind(declared_condition.as_ref().map(|(a, _)| a.as_str()))
        .bind(declared_condition.as_ref().map(|(_, v)| v.as_str()))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// `ACCEPTS` names declared aspects — the context the server assembles
    /// for the script (SPEC.md §6); each must exist. Declaration relations
    /// may ride the list too, as invalidation edges only (ruled
    /// 2026-08-05): no context entry arrives, but a write to the relation
    /// kills the cache like an aspect value would.
    pub async fn declare_function(&self, decl: &FunctionDecl) -> Result<()> {
        for aspect in &decl.accepts {
            if accepts_relation(aspect.value.as_str()) {
                continue;
            }
            self.require("aspect", "aspects", aspect.value.as_str())
                .await?;
        }
        // RETURNS mirrors ACCEPTS (ruled 2026-08-04): it names the aspect
        // the output fills. A MEASUREMENT aspect has one producer; a FACT
        // aspect's returning functions are voices; a QUERY aspect is never
        // function-filled. No RETURNS declares a detector.
        if let Some(aspect) = &decl.returns {
            let aspect = aspect.value.as_str();
            // A self-edge: the function's own value would invalidate its own
            // cache the moment it is written, and the write would never be
            // readable. ACCEPTS and RETURNS name different aspects.
            if decl.accepts.iter().any(|a| a.value == aspect) {
                return Err(Error::SelfAccepting {
                    function: decl.name.value.clone(),
                    aspect: aspect.into(),
                });
            }
            let (_, kind, _) = self.aspect(aspect).await?.ok_or_else(|| Error::Unknown {
                what: "aspect",
                name: aspect.into(),
            })?;
            match kind.as_str() {
                "query" => {
                    return Err(Error::ReturnsQueryAspect {
                        function: decl.name.value.clone(),
                        aspect: aspect.into(),
                    });
                }
                "measurement" => {
                    let taken: Option<String> =
                        sqlx::query("SELECT name FROM functions WHERE returns = ? AND name != ?")
                            .bind(aspect)
                            .bind(decl.name.value.as_str())
                            .fetch_optional(&self.pool)
                            .await?
                            .map(|r| r.get("name"));
                    if let Some(existing) = taken {
                        return Err(Error::MeasurementProducerTaken {
                            aspect: aspect.into(),
                            existing,
                        });
                    }
                }
                _fact => {}
            }
        }
        let accepts = if decl.accepts.is_empty() {
            None
        } else {
            let names: Vec<Value> = decl
                .accepts
                .iter()
                .map(|a| Value::String(a.value.clone()))
                .collect();
            Some(Value::Array(names).to_string())
        };
        let scope = match &decl.scope {
            FunctionScope::Dataset(d) => Some(d.value.clone()),
            FunctionScope::Global => None,
        };
        sqlx::query(
            "INSERT OR REPLACE INTO functions \
             (name, scope_dataset, script, accepts, returns) \
             VALUES (?, ?, ?, ?, ?)",
        )
        .bind(decl.name.value.as_str())
        .bind(scope)
        .bind(decl.script.as_str())
        .bind(accepts)
        .bind(decl.returns.as_ref().map(|a| a.value.clone()))
        .execute(&self.pool)
        .await?;
        // A re-declared function is a different function; its cached results
        // no longer describe anything.
        sqlx::query("DELETE FROM cache WHERE function = ?")
            .bind(decl.name.value.as_str())
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn declare_witness(&self, decl: &WitnessDecl) -> Result<()> {
        let aspect = decl.aspect.value.as_str();
        let kind = self
            .aspect(aspect)
            .await?
            .ok_or_else(|| Error::Unknown {
                what: "aspect",
                name: aspect.into(),
            })?
            .1;

        // BY gates actor glosses only — function voices arrive via RETURNS
        // (ruled 2026-08-04). Measurements are never glossed, so a witness
        // on one carries only a detector; a witness naming neither BY nor
        // DETECTOR declares nothing.
        let speakers: Vec<Value> = decl
            .speakers
            .iter()
            .map(|s| match s {
                Speaker::Agent => Value::String("agent".into()),
                Speaker::Human => Value::String("human".into()),
            })
            .collect();
        if kind == "measurement" && !speakers.is_empty() {
            return Err(Error::MeasurementWitnessSpeakers(aspect.into()));
        }
        if speakers.is_empty() && decl.detector.is_none() {
            return Err(Error::WitnessNamesNothing(decl.name.value.clone()));
        }

        if let Some(detector) = &decl.detector {
            let name = detector.value.clone();
            let f = self
                .function(&name, None)
                .await?
                .ok_or_else(|| Error::Unknown {
                    what: "function",
                    name: name.clone(),
                })?;
            // Role by shape: a detector is a function without RETURNS.
            if f.returns.is_some() {
                return Err(Error::DetectorNotEligible { function: name });
            }
        }
        // THRESHOLD range is admission's job, not the grammar's.
        let threshold = match &decl.threshold {
            None => None,
            Some(t) => {
                let v: f64 = t
                    .parse()
                    .map_err(|_| Error::Corrupt(format!("threshold `{t}` is not a number")))?;
                if !(0.0..=1.0).contains(&v) {
                    return Err(Error::Corrupt(format!("threshold `{t}` is outside 0..1")));
                }
                Some(v)
            }
        };

        sqlx::query(
            "INSERT OR REPLACE INTO witnesses (name, aspect, speakers, detector, threshold) \
             VALUES (?, ?, ?, ?, ?)",
        )
        .bind(decl.name.value.as_str())
        .bind(aspect)
        .bind(Value::Array(speakers).to_string())
        .bind(decl.detector.as_ref().map(|d| d.value.clone()))
        .bind(threshold)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    // -- glosses ---------------------------------------------------------

    /// Admission by aspect kind (SPEC.md §5.2), then a plain insert; the
    /// supersession key (subject, aspect, actor kind) is applied by reads.
    /// `snapshot_id` is the subject's table snapshot at write time — `None`
    /// when the subject has no table (dataset-level, pair paths) or no data
    /// plane is attached.
    pub async fn gloss(
        &self,
        dataset: &str,
        actor: &Actor,
        aspect: &str,
        subject: &str,
        body: &JsonBody,
        snapshot_id: Option<i64>,
    ) -> Result<()> {
        let (schema, kind, grains) = self.aspect(aspect).await?.ok_or_else(|| Error::Unknown {
            what: "aspect",
            name: aspect.into(),
        })?;
        match kind.as_str() {
            "measurement" => return Err(Error::MeasurementGloss(aspect.into())),
            "fact" => validate(&schema, &body.value, format!("aspect `{aspect}` WITH"))?,
            _query => validate(
                &grounding_schema(),
                &body.value,
                "standard grounding".into(),
            )?,
        }
        let is_source = self.source_settings(subject).await?.is_some();
        admit_grain(aspect, grains.as_deref(), dataset, subject, is_source)?;
        // Where a witness exists, its BY list is the speaker gate (§7.1).
        let witnesses = self.witnesses_on(aspect).await?;
        if !witnesses.is_empty() {
            let admitted = witnesses.iter().any(|w| match actor.kind {
                ActorKind::Agent => w.admits_agent,
                ActorKind::Human => w.admits_human,
            });
            if !admitted {
                return Err(Error::SpeakerNotAdmitted {
                    aspect: aspect.into(),
                    kind: actor.kind,
                });
            }
        }
        sqlx::query(
            "INSERT INTO glossary (dataset, subject, aspect, actor_kind, actor_id, body, snapshot_id) \
             VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(dataset)
        .bind(subject)
        .bind(aspect)
        .bind(actor.kind.as_str())
        .bind(actor.id.as_str())
        .bind(body.raw.as_str())
        .bind(snapshot_id)
        .execute(&self.pool)
        .await?;
        self.invalidate(dataset, aspect, subject).await?;
        // The `glossary` relation edge (2026-08-06): a function that
        // ACCEPTS the glossary sweeps it whole, so a gloss write stales
        // its cache dataset-wide. Scoped 2026-08-12: the edge fires on
        // grounding (QUERY) writes only — what the edge's consumers
        // (metric_bands, detect_grounding_collisions) actually read —
        // because the unscoped edge made every fact gloss, a human
        // answer included, empty the band walk until an agent re-ran it.
        if kind == "query" {
            self.invalidate(dataset, "glossary", dataset).await?;
        }
        Ok(())
    }

    /// Writes invalidate, reads recompute, judgment only supersedes (project
    /// lead, 2026-08-04). A new value for an aspect kills the cached output
    /// of every function that `ACCEPTS` it, at and under the subject — the
    /// declared dependency edge, the only definition-level invalidation
    /// there is. Data freshness is snapshot staleness, marked at read.
    async fn invalidate(&self, dataset: &str, aspect: &str, subject: &str) -> Result<()> {
        let dependents = self.functions_accepting(aspect).await?;
        if !dependents.is_empty() {
            let scope = if subject == dataset {
                Scope::Dataset
            } else {
                Scope::Subject(subject.into())
            };
            let (pred, binds) = scope.predicate("subject");
            let marks = vec!["?"; dependents.len()].join(", ");
            let sql =
                format!("DELETE FROM cache WHERE dataset = ? AND function IN ({marks}) AND {pred}");
            let mut q = sqlx::query(&sql).bind(dataset);
            for f in &dependents {
                q = q.bind(f.as_str());
            }
            for b in &binds {
                q = q.bind(b);
            }
            q.execute(&self.pool).await?;
        }

        Ok(())
    }

    /// One import, recorded (project lead, 2026-08-04): rows the recipe
    /// filtered away are the author's to judge, on the files — the engine
    /// keeps the counts, readable through the `imports` relation.
    pub async fn import_put(
        &self,
        dataset: &str,
        table: &str,
        source_rows: i64,
        landed_rows: i64,
        cast_failures: &str,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO imports (dataset, table_name, source_rows, landed_rows, cast_failures) \
             VALUES (?, ?, ?, ?, ?)",
        )
        .bind(dataset)
        .bind(table)
        .bind(source_rows)
        .bind(landed_rows)
        .bind(cast_failures)
        .execute(&self.pool)
        .await?;
        // A landed table widens what dataset-sweeping measurements can
        // see; the `imports` ACCEPTS edge invalidates them dataset-wide.
        self.invalidate(dataset, "imports", dataset).await?;
        Ok(())
    }

    /// Glosses at or under a table — what `DROP TABLE` refuses on.
    pub async fn glosses_under(&self, dataset: &str, table: &str) -> Result<i64> {
        let (pred, binds) = Scope::Subject(table.to_string()).predicate("subject");
        let sql = format!("SELECT count(*) AS n FROM glossary WHERE dataset = ? AND {pred}");
        let mut q = sqlx::query(&sql).bind(dataset);
        for b in &binds {
            q = q.bind(b);
        }
        Ok(q.fetch_one(&self.pool).await?.get("n"))
    }

    /// The store side of `DROP TABLE` (PoC: caller has already refused on
    /// data and glosses): the recipe row, the cached evidence under the
    /// table, and the import records go together.
    /// A re-land's evidence sweep (supersede-and-reland, 2026-08-06): the
    /// table's data changed wholesale, so every cached measurement under it
    /// is stale — cleared here, recomputed at next read. Glosses and the
    /// import history stay: knowledge and record, not evidence.
    pub async fn invalidate_table_evidence(&self, dataset: &str, table: &str) -> Result<()> {
        let (pred, binds) = Scope::Subject(table.to_string()).predicate("subject");
        let sql = format!("DELETE FROM cache WHERE dataset = ? AND {pred}");
        let mut q = sqlx::query(&sql).bind(dataset);
        for b in &binds {
            q = q.bind(b);
        }
        q.execute(&self.pool).await?;
        Ok(())
    }

    pub async fn drop_table_records(&self, dataset: &str, table: &str) -> Result<()> {
        sqlx::query("DELETE FROM recipes WHERE dataset = ? AND table_name = ?")
            .bind(dataset)
            .bind(table)
            .execute(&self.pool)
            .await?;
        let (pred, binds) = Scope::Subject(table.to_string()).predicate("subject");
        let sql = format!("DELETE FROM cache WHERE dataset = ? AND {pred}");
        let mut q = sqlx::query(&sql).bind(dataset);
        for b in &binds {
            q = q.bind(b);
        }
        q.execute(&self.pool).await?;
        sqlx::query("DELETE FROM imports WHERE dataset = ? AND table_name = ?")
            .bind(dataset)
            .bind(table)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn functions_accepting(&self, aspect: &str) -> Result<Vec<String>> {
        let rows = sqlx::query("SELECT name, accepts FROM functions WHERE accepts IS NOT NULL")
            .fetch_all(&self.pool)
            .await?;
        let mut names = Vec::new();
        for r in rows {
            let accepts: Vec<String> = serde_json::from_str(&r.get::<String, _>("accepts"))
                .map_err(|e| Error::Corrupt(format!("ACCEPTS: {e}")))?;
            if accepts.iter().any(|a| a == aspect) {
                names.push(r.get("name"));
            }
        }
        Ok(names)
    }

    /// `(function, aspect)` for every function whose `RETURNS` names an
    /// aspect — the producer of a MEASUREMENT, the voices of a FACT. This
    /// binding is what wires a function's cache into the slots (ruled
    /// 2026-08-04; the function witnesses it replaced were ceremony).
    async fn returning(&self, aspect: Option<&str>) -> Result<Vec<(String, String)>> {
        let q = match aspect {
            Some(a) => sqlx::query("SELECT name, returns FROM functions WHERE returns = ?").bind(a),
            None => sqlx::query("SELECT name, returns FROM functions WHERE returns IS NOT NULL"),
        };
        Ok(q.fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(|r| (r.get("name"), r.get("returns")))
            .collect())
    }

    /// The newest write into (subject, aspect) across all slots — what a
    /// detector's verdict must be at least as fresh as.
    pub async fn newest_slot_write(
        &self,
        dataset: &str,
        subject: &str,
        aspect: &str,
    ) -> Result<Option<String>> {
        let mut newest: Option<String> = sqlx::query(
            "SELECT MAX(written_at) AS t FROM glossary \
             WHERE dataset = ? AND subject = ? AND aspect = ?",
        )
        .bind(dataset)
        .bind(subject)
        .bind(aspect)
        .fetch_one(&self.pool)
        .await?
        .get("t");
        for (f, _) in self.returning(Some(aspect)).await? {
            let t: Option<String> = sqlx::query(
                "SELECT MAX(computed_at) AS t FROM cache \
                 WHERE dataset = ? AND subject = ? AND function = ? AND witness IS NULL",
            )
            .bind(dataset)
            .bind(subject)
            .bind(f.as_str())
            .fetch_one(&self.pool)
            .await?
            .get("t");
            if let Some(t) = t
                && newest.as_deref().is_none_or(|n| t.as_str() > n)
            {
                newest = Some(t);
            }
        }
        Ok(newest)
    }

    // -- reads -----------------------------------------------------------

    /// The current slots under a scope: gloss slots by supersession (one per
    /// actor kind), plus the measurement slot of every witness-bound
    /// function, from the cache. Both read shapes build from these.
    async fn slots(&self, dataset: &str, scope: &Scope, aspect: Option<&str>) -> Result<Vec<Slot>> {
        let (pred, binds) = scope.predicate("g.subject");
        let aspect_clause = if aspect.is_some() {
            "AND g.aspect = ? "
        } else {
            ""
        };
        // A source-grain row: its subject names a declared source and its
        // aspect opted into SOURCE grain. Such rows read and supersede
        // workspace-wide (ruled 2026-08-12) — the deposit the next
        // dataset reads — while every other row stays dataset-scoped.
        // NULL grains (no ON clause) never qualify: the sweep is opt-in.
        const SOURCE_GRAIN: &str = "(g.subject IN (SELECT name FROM sources) \
             AND EXISTS (SELECT 1 FROM aspects a WHERE a.name = g.aspect \
               AND (',' || a.grains || ',') LIKE '%,source,%'))";
        let sql = format!(
            "SELECT g.subject, g.aspect, g.actor_kind, g.actor_id, g.body, g.written_at, \
                    g.snapshot_id \
             FROM glossary g \
             WHERE {pred} {aspect_clause}AND (\
               (NOT {SOURCE_GRAIN} AND g.dataset = ? AND NOT EXISTS (\
                 SELECT 1 FROM glossary n \
                 WHERE n.dataset = g.dataset AND n.subject = g.subject \
                   AND n.aspect = g.aspect AND n.actor_kind = g.actor_kind AND n.id > g.id)) \
               OR ({SOURCE_GRAIN} AND NOT EXISTS (\
                 SELECT 1 FROM glossary n \
                 WHERE n.subject = g.subject \
                   AND n.aspect = g.aspect AND n.actor_kind = g.actor_kind AND n.id > g.id)))"
        );
        let mut q = sqlx::query(&sql);
        for b in &binds {
            q = q.bind(b);
        }
        if let Some(a) = aspect {
            q = q.bind(a);
        }
        q = q.bind(dataset);
        let witnesses = self.witnesses_all().await?;
        let witness_on = |aspect: &str| {
            witnesses
                .iter()
                .find(|w| w.aspect == aspect)
                .map(|w| w.name.clone())
        };
        let mut rows: Vec<Slot> = q
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(|r| {
                let aspect: String = r.get("aspect");
                Slot {
                    subject: r.get("subject"),
                    rank: match r.get::<String, _>("actor_kind").as_str() {
                        "human" => 0,
                        _ => 1,
                    },
                    actor: r.get("actor_id"),
                    witness: witness_on(&aspect),
                    aspect,
                    body: r.get("body"),
                    written_at: r.get("written_at"),
                    snapshot_id: r.get("snapshot_id"),
                }
            })
            .collect();

        let (cpred, cbinds) = scope.predicate("c.subject");
        for (f, a) in self.returning(aspect).await? {
            // A function's own value, never a verdict: witness rows belong
            // to the detector that computed them, not to the slots.
            let sql = format!(
                "SELECT c.subject, c.body, c.computed_at, c.snapshot_id FROM cache c \
                 WHERE c.dataset = ? AND c.function = ? AND c.witness IS NULL AND {cpred} \
                   AND NOT EXISTS (\
                   SELECT 1 FROM cache n \
                   WHERE n.dataset = c.dataset AND n.subject = c.subject \
                     AND n.function = c.function AND n.witness IS NULL AND n.id > c.id)"
            );
            let mut q = sqlx::query(&sql).bind(dataset).bind(f.as_str());
            for b in &cbinds {
                q = q.bind(b);
            }
            for c in q.fetch_all(&self.pool).await? {
                rows.push(Slot {
                    subject: c.get("subject"),
                    aspect: a.clone(),
                    rank: 2,
                    actor: f.clone(),
                    witness: witness_on(&a),
                    body: c.get("body"),
                    written_at: c.get("computed_at"),
                    snapshot_id: c.get("snapshot_id"),
                });
            }
        }
        rows.sort_by(|a, b| {
            (&a.subject, &a.aspect, &a.actor).cmp(&(&b.subject, &b.aspect, &b.actor))
        });
        Ok(rows)
    }

    /// The raw read (SPEC.md §5.3): every current slot, one row each;
    /// precedence is the reader's business here. `kind` is the aspect's kind.
    pub async fn raw_read(
        &self,
        dataset: &str,
        scope: &Scope,
        aspect: Option<&str>,
    ) -> Result<Vec<RawRow>> {
        let kinds = self.aspect_kinds().await?;
        Ok(self
            .slots(dataset, scope, aspect)
            .await?
            .into_iter()
            .map(|s| RawRow {
                kind: kinds.get(&s.aspect).cloned().unwrap_or_default(),
                speaker: match s.rank {
                    0 => "human".into(),
                    1 => "agent".into(),
                    _ => "function".into(),
                },
                subject: s.subject,
                aspect: s.aspect,
                witness: s.witness,
                actor: s.actor,
                body: s.body,
                written_at: s.written_at,
            })
            .collect())
    }

    async fn aspect_kinds(&self) -> Result<std::collections::HashMap<String, String>> {
        let rows = sqlx::query("SELECT name, kind FROM aspects")
            .fetch_all(&self.pool)
            .await?;
        Ok(rows
            .into_iter()
            .map(|r| (r.get("name"), r.get("kind")))
            .collect())
    }

    /// The collapsed read (SPEC.md §5.3): value by precedence (human over
    /// agent over function), withheld only when the detector's score exceeds
    /// the witness threshold; `state` makes every gap visible — see
    /// [`CollapsedRow`]. The `ReadContext` universe adds `unassessed` rows
    /// for witnessed aspects nobody spoke to.
    pub async fn collapsed_read(
        &self,
        dataset: &str,
        scope: &Scope,
        aspect: Option<&str>,
        ctx: &ReadContext,
    ) -> Result<Vec<CollapsedRow>> {
        let slots = self.slots(dataset, scope, aspect).await?;
        let mut grouped: std::collections::BTreeMap<(String, String), Vec<&Slot>> =
            std::collections::BTreeMap::new();
        for s in &slots {
            grouped
                .entry((s.subject.clone(), s.aspect.clone()))
                .or_default()
                .push(s);
        }

        let witnesses = self.witnesses_all().await?;
        // The detectors' verdicts, per subject: (band, score, threshold),
        // each verdict beside its own witness's threshold — a score is
        // never compared against a neighbour witness's threshold (found
        // 2026-08-12). Plural witnesses per aspect are allowed (ruled
        // 2026-08-12); the slot is contested when ANY witness's verdict
        // crosses that witness's own threshold — conservative
        // withholding. Witness name order (the query's ORDER BY) keeps
        // the list deterministic.
        #[allow(clippy::type_complexity)]
        let mut verdicts: std::collections::HashMap<
            (String, String),
            Vec<(String, f64, Option<f64>)>,
        > = std::collections::HashMap::new();
        for w in &witnesses {
            if let Some(a) = aspect
                && w.aspect != a
            {
                continue;
            }
            let Some(detector) = &w.detector else {
                continue;
            };
            for c in self
                .latest_cache(dataset, scope, detector, Some(&w.name))
                .await?
            {
                let body: Value = serde_json::from_str(&c.body)
                    .map_err(|e| Error::Corrupt(format!("attest body for `{detector}`: {e}")))?;
                if let (Some(band), Some(score)) = (
                    body.pointer("/band").and_then(Value::as_str),
                    body.pointer("/score").and_then(Value::as_f64),
                ) {
                    verdicts
                        .entry((c.subject.clone(), w.aspect.clone()))
                        .or_default()
                        .push((band.into(), score, w.threshold));
                }
            }
        }

        let mut rows = Vec::new();
        for ((subject, aspect), mut group) in grouped {
            let verdict = verdicts.get(&(subject.clone(), aspect.clone()));
            let crossing =
                verdict.and_then(|v| v.iter().find(|(_, s, t)| t.is_some_and(|t| *s > t)));
            // The row carries one verdict: the crossing one when contested,
            // the first in witness name order otherwise.
            let shown = crossing.or_else(|| verdict.and_then(|v| v.first()));
            let (band, score) = match shown {
                Some((b, s, _)) => (Some(b.clone()), Some(*s)),
                None => (None, None),
            };
            if crossing.is_some() {
                rows.push(CollapsedRow {
                    subject,
                    aspect,
                    value: None,
                    band,
                    score,
                    state: "contested".into(),
                });
                continue;
            }
            group.sort_by_key(|s| s.rank);
            let serving = group[0];
            // Serve-and-mark (project lead, 2026-08-04): staleness never
            // suppresses a value, it shows beside it.
            let snapshot_moved = serving.snapshot_id.is_some_and(|seen| {
                table_of(&subject)
                    .and_then(|t| ctx.snapshots.get(t))
                    .is_some_and(|current| *current != seen)
            });
            rows.push(CollapsedRow {
                subject,
                aspect,
                value: Some(serving.body.clone()),
                band,
                score,
                state: if snapshot_moved {
                    "stale".into()
                } else {
                    "current".into()
                },
            });
        }

        // Disclosure (fixture 09's benchmark): an aspect somebody is bound
        // to speak to — witnessed, or produced by a function's RETURNS —
        // that nobody spoke to is a visible row, not an omission. Grain
        // (ruled 2026-08-05) bounds the grid: absence only shows on
        // subjects the aspect is declared to speak to.
        let mut witnessed: std::collections::BTreeSet<String> = witnesses
            .iter()
            .filter(|w| aspect.is_none_or(|a| w.aspect == a))
            .map(|w| w.aspect.clone())
            .collect();
        witnessed.extend(self.returning(aspect).await?.into_iter().map(|(_, a)| a));
        let mut grain_map: std::collections::HashMap<String, Option<String>> =
            std::collections::HashMap::new();
        let mut condition_map: std::collections::HashMap<String, (String, String)> =
            std::collections::HashMap::new();
        for row in sqlx::query(
            "SELECT name, grains, condition_aspect, condition_value FROM aspects",
        )
        .fetch_all(&self.pool)
        .await?
        {
            let name: String = row.get("name");
            grain_map.insert(name.clone(), row.get("grains"));
            if let (Some(a), Some(v)) = (
                row.get::<Option<String>, _>("condition_aspect"),
                row.get::<Option<String>, _>("condition_value"),
            ) {
                condition_map.insert(name, (a, v));
            }
        }
        // Conditional relevance (ruled 2026-08-14): a conditioned aspect
        // is owed on a subject only while the named sibling aspect's
        // winning slot carries the declared value — the human slot
        // outranking the agent's, contest notwithstanding. Absence is
        // decisive: no sibling slot spoken, nothing owed yet.
        let mut sibling_values: std::collections::HashMap<
            String,
            std::collections::HashMap<String, String>,
        > = std::collections::HashMap::new();
        for a in &witnessed {
            let Some((cond_aspect, _)) = condition_map.get(a) else {
                continue;
            };
            if sibling_values.contains_key(cond_aspect) {
                continue;
            }
            let mut winners: std::collections::HashMap<String, (u8, String)> =
                std::collections::HashMap::new();
            for slot in self.slots(dataset, scope, Some(cond_aspect)).await? {
                let value = serde_json::from_str::<Value>(&slot.body)
                    .ok()
                    .and_then(|b| b.pointer("/value").and_then(|v| v.as_str().map(String::from)));
                let Some(value) = value else { continue };
                match winners.get(&slot.subject) {
                    Some((rank, _)) if *rank <= slot.rank => {}
                    _ => {
                        winners.insert(slot.subject.clone(), (slot.rank, value));
                    }
                }
            }
            sibling_values.insert(
                cond_aspect.clone(),
                winners.into_iter().map(|(s, (_, v))| (s, v)).collect(),
            );
        }
        let present: std::collections::HashSet<(String, String)> = rows
            .iter()
            .map(|r| (r.subject.clone(), r.aspect.clone()))
            .collect();
        // Declared sources join the subject universe: a source-grain
        // aspect nobody spoke to is owed on every declared source
        // (ruled 2026-08-12), in whichever dataset the read runs.
        let source_names: std::collections::BTreeSet<String> =
            sqlx::query("SELECT name FROM sources")
                .fetch_all(&self.pool)
                .await?
                .into_iter()
                .map(|r| r.get("name"))
                .collect();
        let subjects: Vec<&String> = ctx
            .universe
            .iter()
            .chain(source_names.iter().filter(|s| !ctx.universe.contains(s)))
            .collect();
        for subject in subjects {
            let in_scope = match scope {
                Scope::Dataset => true,
                Scope::Subject(s) => subject == s || subject.starts_with(&format!("{s}.")),
            };
            if !in_scope {
                continue;
            }
            for a in &witnessed {
                let in_grain = match grain_map.get(a).and_then(|g| g.as_deref()) {
                    None => true,
                    Some(declared) => declared.split(',').any(|g| {
                        g == grain_of(dataset, subject)
                            || (g == "source" && source_names.contains(subject))
                    }),
                };
                if !in_grain {
                    continue;
                }
                if let Some((cond_aspect, cond_value)) = condition_map.get(a) {
                    let holds = sibling_values
                        .get(cond_aspect)
                        .and_then(|m| m.get(subject.as_str()))
                        .is_some_and(|v| v == cond_value);
                    if !holds {
                        continue;
                    }
                }
                if !present.contains(&(subject.clone(), a.clone())) {
                    rows.push(CollapsedRow {
                        subject: subject.clone(),
                        aspect: a.clone(),
                        value: None,
                        band: None,
                        score: None,
                        state: "unassessed".into(),
                    });
                }
            }
        }
        rows.sort_by(|a, b| (&a.subject, &a.aspect).cmp(&(&b.subject, &b.aspect)));
        Ok(rows)
    }

    /// `ATTEST(...)` (SPEC.md §7.2): detector outputs, served from the
    /// detector function's cache rows in the fixed attest shape.
    pub async fn attest_read(
        &self,
        dataset: &str,
        scope: &Scope,
        aspect: Option<&str>,
    ) -> Result<Vec<AttestRow>> {
        let mut rows = Vec::new();
        for w in self.witnesses_all().await? {
            if let Some(a) = aspect
                && w.aspect != a
            {
                continue;
            }
            let Some(detector) = &w.detector else {
                continue;
            };
            for c in self
                .latest_cache(dataset, scope, detector, Some(&w.name))
                .await?
            {
                let body: Value = serde_json::from_str(&c.body)
                    .map_err(|e| Error::Corrupt(format!("attest body for `{detector}`: {e}")))?;
                let band = body
                    .pointer("/band")
                    .and_then(Value::as_str)
                    .ok_or_else(|| Error::Corrupt(format!("`{detector}` output has no band")))?;
                let score = body
                    .pointer("/score")
                    .and_then(Value::as_f64)
                    .ok_or_else(|| Error::Corrupt(format!("`{detector}` output has no score")))?;
                rows.push(AttestRow {
                    subject: c.subject,
                    aspect: w.aspect.clone(),
                    witness: w.name.clone(),
                    band: band.into(),
                    score,
                    computed_at: c.computed_at,
                });
            }
        }
        rows.sort_by(|a, b| (&a.subject, &a.aspect).cmp(&(&b.subject, &b.aspect)));
        Ok(rows)
    }

    // -- the cache -------------------------------------------------------

    /// A cached function value. `witness` is the seat the row was computed
    /// for: `None` for a function's own output (keyed by subject, as any
    /// value is), the witness name for a detector's verdict — which depends
    /// on the aspect, the threshold and the slots that witness saw, so one
    /// detector serving three witnesses holds three verdicts, not one
    /// (defect found 2026-08-06, ruled the same day).
    pub async fn cache_get(
        &self,
        dataset: &str,
        subject: &str,
        function: &str,
        witness: Option<&str>,
    ) -> Result<Option<CacheRow>> {
        let row = sqlx::query(
            "SELECT subject, function, body, computed_at FROM cache \
             WHERE dataset = ? AND subject = ? AND function = ? AND witness IS ? \
             ORDER BY id DESC LIMIT 1",
        )
        .bind(dataset)
        .bind(subject)
        .bind(function)
        .bind(witness)
        .fetch_optional(&self.pool)
        .await?;
        row.map(cache_row).transpose()
    }

    pub async fn cache_put(
        &self,
        dataset: &str,
        subject: &str,
        function: &str,
        witness: Option<&str>,
        body: &str,
        snapshot_id: Option<i64>,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO cache (dataset, subject, function, witness, body, snapshot_id) \
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(dataset)
        .bind(subject)
        .bind(function)
        .bind(witness)
        .bind(body)
        .bind(snapshot_id)
        .execute(&self.pool)
        .await?;
        // A function's new value invalidates like a gloss would: through
        // the aspect its RETURNS names. Detectors return no aspect, so
        // their verdicts invalidate nothing.
        let returns: Option<String> = sqlx::query("SELECT returns FROM functions WHERE name = ?")
            .bind(function)
            .fetch_optional(&self.pool)
            .await?
            .and_then(|r| r.get("returns"));
        if let Some(aspect) = returns {
            self.invalidate(dataset, &aspect, subject).await?;
        }
        Ok(())
    }

    async fn latest_cache(
        &self,
        dataset: &str,
        scope: &Scope,
        function: &str,
        witness: Option<&str>,
    ) -> Result<Vec<CacheRow>> {
        let (pred, binds) = scope.predicate("c.subject");
        let sql = format!(
            "SELECT c.subject, c.function, c.body, c.computed_at FROM cache c \
             WHERE c.dataset = ? AND c.function = ? AND c.witness IS ? AND {pred} \
               AND NOT EXISTS (\
               SELECT 1 FROM cache n \
               WHERE n.dataset = c.dataset AND n.subject = c.subject \
                 AND n.function = c.function AND n.witness IS c.witness \
                 AND n.id > c.id) \
             ORDER BY c.subject"
        );
        let mut q = sqlx::query(&sql).bind(dataset).bind(function).bind(witness);
        for b in &binds {
            q = q.bind(b);
        }
        q.fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(cache_row)
            .collect()
    }

    // -- SQL forwarded from the session ----------------------------------

    /// `DELETE FROM glossary … / DELETE FROM cache …` — removal is SQL
    /// (SPEC.md §5.2, §6). The session routes only these two relations here;
    /// the target is re-checked because this executes verbatim.
    ///
    /// A delete invalidates like a write would (2026-08-05, closed on the
    /// remaining edges 2026-08-12): the doomed rows are pre-selected with
    /// the delete's own predicate, and after the removal the affected keys
    /// run the same edges — ACCEPTS for a struck gloss, the verdict sweep
    /// for a struck function voice. When the statement's shape hides which
    /// rows die, the sweep goes blunt instead of thin.
    pub async fn forward_delete(&self, target: &str, sql: &str) -> Result<u64> {
        if target != "glossary" && target != "cache" {
            return Err(Error::ForwardRejected(target.into()));
        }
        // SQLite executes every `;`-separated statement in a forwarded
        // string, and it reads `$tag$` as a bind parameter rather than a
        // quote — so a dollar-quoted body carrying a `;` became SQL of the
        // sender's choosing (found 2026-08-06). The caller renders from the
        // AST with those literals normalized; this is the check at the
        // point of execution, which is where it has to hold.
        if let Some(stray) = stray_statement_char(sql) {
            return Err(Error::ForwardUnsafe { char: stray });
        }
        let doomed_glossary = if target == "glossary" {
            self.glossary_delete_targets(sql).await
        } else {
            None
        };
        let doomed_cache = if target == "cache" {
            self.cache_delete_targets(sql).await
        } else {
            None
        };
        let done = sqlx::raw_sql(sql).execute(&self.pool).await?;
        if done.rows_affected() == 0 {
            return Ok(0);
        }
        if target == "glossary" {
            match doomed_glossary {
                Some(pairs) => {
                    for (dataset, aspect) in &pairs {
                        // The write-side ACCEPTS edge, applied on removal:
                        // a struck gloss changes the aspect's collapsed
                        // value as surely as a write does. The pre-select
                        // keys (dataset, aspect) — subjects unknown at this
                        // grain, so the sweep is dataset-wide.
                        self.invalidate(dataset, aspect, dataset).await?;
                    }
                    // `whatif` rows sit outside the functions table but
                    // replay the dataset's QUERY groundings, so any
                    // glossary strike can change what they replayed.
                    let datasets: std::collections::BTreeSet<&str> =
                        pairs.iter().map(|(d, _)| d.as_str()).collect();
                    for dataset in datasets {
                        sqlx::query("DELETE FROM cache WHERE dataset = ? AND function = 'whatif'")
                            .bind(dataset)
                            .execute(&self.pool)
                            .await?;
                    }
                }
                // Blunt but correct: the delete's shape (USING / ORDER BY /
                // LIMIT / RETURNING, or a failed pre-select) hides which
                // (dataset, aspect) pairs died, so every cached value and
                // verdict goes — under-invalidation would serve evidence
                // computed from struck glosses.
                None => {
                    sqlx::query("DELETE FROM cache").execute(&self.pool).await?;
                }
            }
            sqlx::query(
                "DELETE FROM cache WHERE function IN \
                 (SELECT name FROM functions WHERE returns IS NULL)",
            )
            .execute(&self.pool)
            .await?;
            // Functions sweeping the glossary whole (`ACCEPTS (glossary)`)
            // read what the delete just changed — same edge as the write
            // side, applied on removal (2026-08-06).
            for f in self.functions_accepting("glossary").await? {
                sqlx::query("DELETE FROM cache WHERE function = ?")
                    .bind(f.as_str())
                    .execute(&self.pool)
                    .await?;
            }
        }
        if target == "cache" {
            match doomed_cache {
                Some(triples) => {
                    for (dataset, subject, function) in &triples {
                        // A struck function voice makes the slot set older,
                        // never newer, so a cached verdict would pass the
                        // freshness check and keep withholding — the same
                        // hazard the glossary side closed (2026-08-05),
                        // applied to the cache target. The witnesses whose
                        // aspect the function RETURNS lose their verdicts
                        // for the struck subject.
                        sqlx::query(
                            "DELETE FROM cache WHERE dataset = ? AND subject = ? \
                             AND witness IN (SELECT w.name FROM witnesses w \
                               JOIN functions f ON f.returns = w.aspect WHERE f.name = ?)",
                        )
                        .bind(dataset)
                        .bind(subject)
                        .bind(function)
                        .execute(&self.pool)
                        .await?;
                    }
                }
                // Blunt but correct: which voices died is unknown, so every
                // verdict goes and recomputes at the next read.
                None => {
                    sqlx::query("DELETE FROM cache WHERE witness IS NOT NULL")
                        .execute(&self.pool)
                        .await?;
                }
            }
        }
        Ok(done.rows_affected())
    }

    /// The (dataset, aspect) pairs a forwarded glossary delete is about to
    /// remove — the delete's own predicate re-aimed at the key columns.
    /// `None` sends the caller blunt; any pre-select failure lands there
    /// too, because over-invalidation is the safe direction.
    async fn glossary_delete_targets(&self, sql: &str) -> Option<Vec<(String, String)>> {
        let select = match delete_filter(sql) {
            DeleteFilter::All => "SELECT DISTINCT dataset, aspect FROM glossary".into(),
            DeleteFilter::Where(pred) => {
                format!("SELECT DISTINCT dataset, aspect FROM glossary WHERE {pred}")
            }
            DeleteFilter::Unknown => return None,
        };
        let rows = sqlx::query(&select).fetch_all(&self.pool).await.ok()?;
        Some(
            rows.into_iter()
                .map(|r| (r.get("dataset"), r.get("aspect")))
                .collect(),
        )
    }

    /// The (dataset, subject, function) triples a forwarded cache delete is
    /// about to remove. Same contract as [`Store::glossary_delete_targets`].
    async fn cache_delete_targets(&self, sql: &str) -> Option<Vec<(String, String, String)>> {
        let select = match delete_filter(sql) {
            DeleteFilter::All => "SELECT DISTINCT dataset, subject, function FROM cache".into(),
            DeleteFilter::Where(pred) => {
                format!("SELECT DISTINCT dataset, subject, function FROM cache WHERE {pred}")
            }
            DeleteFilter::Unknown => return None,
        };
        let rows = sqlx::query(&select).fetch_all(&self.pool).await.ok()?;
        Some(
            rows.into_iter()
                .map(|r| (r.get("dataset"), r.get("subject"), r.get("function")))
                .collect(),
        )
    }

    /// Full relation dump for substrate `SELECT`s over the store's
    /// relations — the names and column shapes live in [`RELATIONS`].
    pub async fn relation_rows(&self, table: &str) -> Result<Vec<Vec<Option<String>>>> {
        let Some(relation) = RELATIONS.iter().find(|r| r.name == table) else {
            return Err(Error::ForwardRejected(table.into()));
        };
        let rows = sqlx::query(relation.sql).fetch_all(&self.pool).await?;
        Ok(rows
            .into_iter()
            .map(|r| {
                (0..r.len())
                    .map(|i| r.get::<Option<String>, _>(i))
                    .collect()
            })
            .collect())
    }

    // -- lookups the session needs ---------------------------------------

    pub async fn dataset_exists(&self, name: &str) -> Result<bool> {
        Ok(sqlx::query("SELECT 1 FROM datasets WHERE name = ?")
            .bind(name)
            .fetch_optional(&self.pool)
            .await?
            .is_some())
    }

    /// `(schema, kind, grains)` — grains is the declared `ON` list
    /// (comma-joined, lowercase), `None` when the aspect speaks to all
    /// grains.
    pub async fn aspect(&self, name: &str) -> Result<Option<(Value, String, Option<String>)>> {
        let Some(row) = sqlx::query("SELECT schema, kind, grains FROM aspects WHERE name = ?")
            .bind(name)
            .fetch_optional(&self.pool)
            .await?
        else {
            return Ok(None);
        };
        let schema: Value = serde_json::from_str(&row.get::<String, _>("schema"))
            .map_err(|e| Error::Corrupt(format!("aspect `{name}` schema: {e}")))?;
        Ok(Some((schema, row.get("kind"), row.get("grains"))))
    }

    /// Resolve a function visible from `dataset` (`FOR` scope or GLOBAL,
    /// SPEC.md §6). `None` skips the visibility check.
    pub async fn function(&self, name: &str, dataset: Option<&str>) -> Result<Option<FunctionRow>> {
        let Some(row) = sqlx::query(
            "SELECT name, scope_dataset, script, accepts, returns \
             FROM functions WHERE name = ?",
        )
        .bind(name)
        .fetch_optional(&self.pool)
        .await?
        else {
            return Ok(None);
        };
        let scope_dataset: Option<String> = row.get("scope_dataset");
        if let (Some(d), Some(scope)) = (dataset, &scope_dataset)
            && scope != d
        {
            return Ok(None);
        }
        let returns: Option<String> = row.get("returns");
        let accepts = match row.get::<Option<String>, _>("accepts") {
            None => Vec::new(),
            Some(text) => serde_json::from_str::<Vec<String>>(&text)
                .map_err(|e| Error::Corrupt(format!("function `{name}` ACCEPTS: {e}")))?,
        };
        Ok(Some(FunctionRow {
            name: row.get("name"),
            scope_dataset,
            script: row.get("script"),
            accepts,
            returns,
        }))
    }

    pub async fn witnesses_on(&self, aspect: &str) -> Result<Vec<WitnessRow>> {
        Ok(self
            .witnesses_all()
            .await?
            .into_iter()
            .filter(|w| w.aspect == aspect)
            .collect())
    }

    pub async fn witnesses_all(&self) -> Result<Vec<WitnessRow>> {
        let rows = sqlx::query(
            "SELECT name, aspect, speakers, detector, threshold FROM witnesses ORDER BY name",
        )
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|r| {
                let speakers: Value = serde_json::from_str(&r.get::<String, _>("speakers"))
                    .map_err(|e| Error::Corrupt(format!("witness speakers: {e}")))?;
                let list = speakers.as_array().cloned().unwrap_or_default();
                Ok(WitnessRow {
                    name: r.get("name"),
                    aspect: r.get("aspect"),
                    admits_agent: list.iter().any(|s| s.as_str() == Some("agent")),
                    admits_human: list.iter().any(|s| s.as_str() == Some("human")),
                    detector: r.get("detector"),
                    threshold: r.get("threshold"),
                })
            })
            .collect()
    }

    async fn require(&self, what: &'static str, table: &str, name: &str) -> Result<()> {
        let sql = format!("SELECT 1 FROM {table} WHERE name = ?");
        if sqlx::query(&sql)
            .bind(name)
            .fetch_optional(&self.pool)
            .await?
            .is_none()
        {
            return Err(Error::Unknown {
                what,
                name: name.into(),
            });
        }
        Ok(())
    }
}

/// The table a subject's snapshot rides on: its first path segment. Pair
/// paths (they contain spaces) have none.
fn table_of(subject: &str) -> Option<&str> {
    if subject.contains(' ') {
        return None;
    }
    Some(subject.split('.').next().unwrap_or(subject))
}

fn settings_json(settings: &[glossql_parser::Setting]) -> String {
    use glossql_parser::SettingValue;
    let map: serde_json::Map<String, Value> = settings
        .iter()
        .map(|s| {
            let v = match &s.value {
                SettingValue::Name(n) => Value::String(n.value.clone()),
                SettingValue::String(t) => Value::String(t.clone()),
                SettingValue::Number(n) => {
                    serde_json::from_str(n).unwrap_or_else(|_| Value::String(n.clone()))
                }
            };
            (s.key.value.clone(), v)
        })
        .collect();
    Value::Object(map).to_string()
}

fn kind_str(kind: AspectKind) -> &'static str {
    match kind {
        AspectKind::Measurement => "measurement",
        AspectKind::Fact => "fact",
        AspectKind::Query => "query",
    }
}

/// Canonical grains text: fixed order, deduped — idempotent redeclaration
/// compares this string. Empty list (no `ON` clause) is `None`: all grains.
fn grains_str(grains: &[Grain]) -> Option<String> {
    if grains.is_empty() {
        return None;
    }
    let order = [
        (Grain::Dataset, "dataset"),
        (Grain::Table, "table"),
        (Grain::Column, "column"),
        (Grain::Relationship, "relationship"),
        (Grain::Source, "source"),
    ];
    Some(
        order
            .iter()
            .filter(|(g, _)| grains.contains(g))
            .map(|(_, name)| *name)
            .collect::<Vec<_>>()
            .join(","),
    )
}

fn validate(schema: &Value, instance: &Value, which: String) -> Result<()> {
    crate::schemas::validate_instance(schema, instance)
        .map_err(|detail| Error::BodyRejected { which, detail })
}

fn cache_row(r: sqlx::sqlite::SqliteRow) -> Result<CacheRow> {
    Ok(CacheRow {
        subject: r.get("subject"),
        function: r.get("function"),
        body: r.get("body"),
        computed_at: r.get("computed_at"),
    })
}
