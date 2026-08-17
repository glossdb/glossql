//! The sqlx-backed store. One SQLite file per workspace (`:memory:` in
//! tests); Postgres later is a connection string, so every query here stays
//! in portable SQL. Admission (SPEC.md §5.2, §7.1) happens on the write
//! paths; supersession is the `NOT EXISTS` read predicate, never an update.

use std::sync::Arc;

use glossql_catalog::{Lake, Relations};
use serde_json::Value;
use sqlx::Row as _;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions};

use glossql_parser::{
    AspectDecl, AspectKind, DatasetDecl, FunctionDecl, FunctionScope, Grain, JsonBody, RecipeDecl,
    SourceDecl, Speaker, WitnessDecl,
};

use crate::schemas::grounding_schema;
use crate::rules::{self, Slot, admit_grain, grain_of, rank_of};
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
  source_scans TEXT NOT NULL,
  landed_rows INTEGER NOT NULL,
  dropped_rows_count INTEGER,
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
    /// The sqlite select, `None` once the relation has crossed to the
    /// lake — see [`STORE_NAMESPACE`]. A moved relation is served by
    /// scanning history and applying the rule, which is the same shape
    /// the `ORDER BY` used to hand-roll.
    sql: Option<&'static str>,
    /// Columns the lake table is identity-partitioned by — `["dataset"]`
    /// on the relations keyed by one, so each dataset's rows land in
    /// their own files. The format's split, not a layout of ours.
    partition: &'static [&'static str],
    /// The supersession key: serving keeps the latest row per key, in
    /// `(seq, pos)` order. Empty means the whole row — re-declaring the
    /// same content collapses, differing content stands beside it.
    key: &'static [&'static str],
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
        sql: Some("SELECT dataset, subject, aspect, actor_kind, actor_id, body, written_at, \
                     CAST(snapshot_id AS TEXT) AS snapshot_id \
              FROM glossary ORDER BY id"),
        partition: &["dataset"],
        key: &[],
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
        sql: Some("SELECT dataset, subject, function, witness, body, computed_at, \
                     CAST(snapshot_id AS TEXT) AS snapshot_id \
              FROM cache ORDER BY id"),
        partition: &["dataset"],
        key: &[],
    },
    Relation {
        name: "imports",
        columns: &[
            "dataset",
            "table_name",
            "source_scans",
            "landed_rows",
            "dropped_rows_count",
            "cast_failures",
            "imported_at",
        ],
        // Served from snapshots and their properties. `dropped_rows_count`
        // is decided at the landing, never re-derived: only the import
        // knows the recipe's shape, and NULL is the honest answer often
        // enough to be a value. `source_scans` is per-scan deliberately —
        // a sum across a join reads as "what was read" and is not
        // (found 2026-08-12: 16,817 phantom dropped rows).
        sql: None,
        partition: &[],
        key: &[],
    },
    Relation {
        name: "functions",
        columns: &["name", "scope", "script", "accepts", "returns"],
        sql: None,
        partition: &[],
        key: &["name"],
    },
    Relation {
        name: "aspects",
        columns: &["name", "kind", "grains", "condition", "schema"],
        sql: None,
        partition: &[],
        key: &["name"],
    },
    Relation {
        name: "witnesses",
        columns: &["name", "aspect", "speakers", "detector", "threshold"],
        sql: None,
        partition: &[],
        key: &["name"],
    },
    Relation {
        name: "sources",
        columns: &["name", "settings"],
        sql: None,
        partition: &[],
        key: &["name"],
    },
    // Served from the namespace list; settings ride as a property.
    Relation {
        name: "datasets",
        columns: &["name", "settings"],
        sql: None,
        partition: &[],
        key: &[],
    },
    // First across (2026-08-17): no supersession of its own, one writer,
    // one reader.
    Relation {
        name: "relationships",
        columns: &["dataset", "left_path", "op", "right_path"],
        sql: None,
        partition: &["dataset"],
        key: &[],
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

/// Where every crossed relation lives: one namespace, one table per
/// relation. A workspace holds many datasets — that scopes rows by a
/// `dataset` KEY column, with the physical per-dataset split supplied by
/// the format (identity partition), not by a namespace layout of ours.
/// The 2026-08-16 §5 `<dataset>_meta` pairing was built and reversed on
/// 2026-08-17: its whole justification is per-dataset REST grants, and
/// access rights are held open — if that decision lands on
/// namespace-level grants, the pairing returns then, by re-bootstrap.
const STORE_NAMESPACE: &str = "glossql";

/// Facts ride what they describe: a dataset's settings on its namespace,
/// a recipe on its table, a landing's source-side facts on its snapshot.
const SETTINGS_PROP: &str = "glossql.settings";
const RECIPE_SOURCE_PROP: &str = "glossql.recipe.source";
const RECIPE_SQL_PROP: &str = "glossql.recipe.sql";
pub const LANDING_SCANS_PROP: &str = "glossql.source-scans";
pub const LANDING_DROPPED_PROP: &str = "glossql.dropped-rows";
pub const LANDING_CASTS_PROP: &str = "glossql.cast-failures";

/// The seam over a workspace's lake, carrying every relation that has
/// crossed. The shapes come from [`RELATIONS`], so a relation crosses by
/// setting its `sql` to `None` and nothing else.
async fn lake_relations(lake: Lake) -> Result<Arc<dyn Relations>> {
    let moved: Vec<glossql_catalog::RelationSpec> = RELATIONS
        .iter()
        .filter(|r| r.sql.is_none())
        .map(|r| glossql_catalog::RelationSpec {
            name: r.name,
            columns: r.columns,
            partition: r.partition,
        })
        .collect();
    let relations = glossql_catalog::IcebergRelations::open(lake, STORE_NAMESPACE, &moved)
        .await?;
    Ok(Arc::new(relations))
}

#[derive(Debug, Clone)]
pub struct Store {
    pool: SqlitePool,
    lake: Lake,
    relations: Arc<dyn Relations>,
    /// Held for its lifetime, not read: [`Store::open_memory`] puts the
    /// lake in a temp dir, and dropping the handle would delete it out
    /// from under an open store.
    _scratch: Option<Arc<tempfile::TempDir>>,
}

/// What the connect-time brief is composed from — see
/// [`Store::brief_counts`]. `PartialEq` is load-bearing: the door
/// decides "did the brief move" by comparing these counts, never by
/// comparing rendered lines (string equality on non-keys is
/// forbidden, ruled 2026-08-14).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BriefCounts {
    pub human_writings: i64,
    pub latest_human_at: Option<String>,
    pub approvals_pending: i64,
    /// Ruling entries whose ruled key still stands below full
    /// confidence in the agent's current body — the fold-in debt.
    pub rulings_owed: i64,
}

impl Store {
    /// The store over a workspace's own lake. The relations that have
    /// crossed live there; the rest still ride the sqlite pool.
    pub async fn open(url: &str, lake: Lake) -> Result<Self> {
        // WAL, so a read never waits behind the write of a gloss; a busy
        // timeout, so a concurrent writer waits instead of erroring. sqlx
        // sets neither by default (sqlx-sqlite-0.8.6 options/mod.rs:177).
        let options: SqliteConnectOptions = url
            .parse::<SqliteConnectOptions>()?
            .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
            .busy_timeout(std::time::Duration::from_secs(5));
        let pool = SqlitePoolOptions::new().connect_with(options).await?;
        sqlx::raw_sql(SCHEMA).execute(&pool).await?;
        Ok(Store {
            pool,
            relations: lake_relations(lake.clone()).await?,
            lake,
            _scratch: None,
        })
    }

    /// The one lake behind this store — sessions and doors share it.
    pub fn lake(&self) -> Lake {
        self.lake.clone()
    }

    /// A throwaway store over a caller's lake: sqlite in memory. One
    /// connection, or every pool checkout would see a different empty
    /// database.
    pub async fn open_scratch(lake: Lake) -> Result<Self> {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await?;
        sqlx::raw_sql(SCHEMA).execute(&pool).await?;
        Ok(Store {
            pool,
            relations: lake_relations(lake.clone()).await?,
            lake,
            _scratch: None,
        })
    }

    /// A throwaway store with its own lake, in a temp dir that goes when
    /// the store does.
    pub async fn open_memory() -> Result<Self> {
        let scratch = tempfile::tempdir().map_err(|e| Error::Backend(e.to_string()))?;
        let lake = Lake::open(
            &scratch.path().join("catalog.sqlite"),
            &scratch.path().join("warehouse"),
        )
        .await?;
        let mut store = Self::open_scratch(lake).await?;
        store._scratch = Some(Arc::new(scratch));
        Ok(store)
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
        // An approval is pending while its table has no landing at or
        // after the ruling was written; landings read from the lake.
        let approvals = sqlx::query(
            "SELECT body, written_at FROM glossary h \
             WHERE h.aspect = 'recipe_change' AND h.actor_kind = 'human' \
               AND NOT EXISTS (SELECT 1 FROM glossary h2 \
                               WHERE h2.subject = h.subject AND h2.aspect = h.aspect \
                                 AND h2.actor_kind = 'human' AND h2.written_at > h.written_at)",
        )
        .fetch_all(&self.pool)
        .await?;
        let landings = self.import_rows().await?;
        let approvals_pending = approvals
            .iter()
            .filter(|r| {
                let body: Value =
                    serde_json::from_str(&r.get::<String, _>("body")).unwrap_or(Value::Null);
                let Some(table) = body["table"].as_str() else {
                    return false;
                };
                let written: String = r.get("written_at");
                !landings.iter().any(|l| {
                    l[1].as_deref() == Some(table)
                        && l[6].as_deref().is_some_and(|at| at >= written.as_str())
                })
            })
            .count() as i64;
        // A ruling awaits its fold-in while the assumption it rules —
        // named by its `key`, the agent-declared identity that survives
        // rephrasing (ruled 2026-08-14: prose is never a join column) —
        // still stands below full confidence in the agent's current
        // body. The agent's re-record clears the debt and the round's
        // question at once.
        let rulings = sqlx::query(
            "SELECT count(*) AS n FROM glossary r, json_each(r.body, '$.rulings') jr \
             WHERE r.aspect = 'ruling' AND r.actor_kind = 'human' \
               AND NOT EXISTS (SELECT 1 FROM glossary r2 \
                               WHERE r2.subject = r.subject AND r2.aspect = 'ruling' \
                                 AND r2.actor_kind = 'human' AND r2.written_at > r.written_at) \
               AND EXISTS ( \
                 SELECT 1 FROM glossary a, json_each(a.body, '$.assumptions') ja \
                 WHERE a.subject = r.subject AND a.actor_kind = 'agent' \
                   AND a.aspect = json_extract(jr.value, '$.aspect') \
                   AND NOT EXISTS (SELECT 1 FROM glossary a2 \
                                   WHERE a2.subject = a.subject AND a2.aspect = a.aspect \
                                     AND a2.actor_kind = 'agent' \
                                     AND a2.written_at > a.written_at) \
                   AND json_extract(ja.value, '$.key') IS NOT NULL \
                   AND json_extract(ja.value, '$.key') \
                         = json_extract(jr.value, '$.key') \
                   AND coalesce(json_extract(ja.value, '$.confidence'), 0.0) < 1.0)",
        )
        .fetch_one(&self.pool)
        .await?;
        Ok(BriefCounts {
            human_writings: humans.get::<i64, _>("n"),
            latest_human_at: humans.get::<Option<String>, _>("latest"),
            approvals_pending,
            rulings_owed: rulings.get::<i64, _>("n"),
        })
    }

    // -- declarations ----------------------------------------------------

    pub async fn declare_source(&self, decl: &SourceDecl) -> Result<()> {
        self.put(
            "sources",
            vec![
                Some(decl.name.value.clone()),
                Some(settings_json(&decl.settings)),
            ],
        )
        .await
    }

    /// A dataset is its namespace; the settings ride it as a property,
    /// set at create and not changed afterwards (ruled 2026-08-16).
    pub async fn declare_dataset(&self, decl: &DatasetDecl) -> Result<()> {
        let name = decl.name.value.as_str();
        if name == STORE_NAMESPACE {
            return Err(Error::ReservedTableName(name.into()));
        }
        self.lake
            .ensure_namespace(
                name,
                std::collections::HashMap::from([(
                    SETTINGS_PROP.to_string(),
                    settings_json(&decl.settings),
                )]),
            )
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
        if !self.dataset_exists(dataset).await? {
            return Err(Error::Unknown {
                what: "dataset",
                name: dataset.into(),
            });
        }
        if self.source_settings(decl.source.value.as_str()).await?.is_none() {
            return Err(Error::Unknown {
                what: "source",
                name: decl.source.value.clone(),
            });
        }
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

    /// The recipe rides its table as properties: one per (dataset,
    /// table), outright replacement, no actor — and it cannot outlive or
    /// precede the table it describes (ruled 2026-08-16).
    pub async fn put_recipe(&self, decl: &RecipeDecl) -> Result<()> {
        self.lake
            .set_table_properties(
                decl.dataset.value.as_str(),
                decl.table.value.as_str(),
                std::collections::HashMap::from([
                    (RECIPE_SOURCE_PROP.to_string(), decl.source.value.clone()),
                    (RECIPE_SQL_PROP.to_string(), decl.sql.clone()),
                ]),
            )
            .await
            .map_err(Error::from)
    }

    pub async fn recipe(&self, dataset: &str, table: &str) -> Result<Option<RecipeRow>> {
        let Some(props) = self
            .lake
            .table_properties(dataset, table)
            .await?
        else {
            return Ok(None);
        };
        match (props.get(RECIPE_SOURCE_PROP), props.get(RECIPE_SQL_PROP)) {
            (Some(source), Some(sql)) => Ok(Some(RecipeRow {
                source: source.clone(),
                sql: sql.clone(),
            })),
            _ => Ok(None),
        }
    }

    pub async fn source_settings(&self, name: &str) -> Result<Option<Value>> {
        self.sources_all()
            .await?
            .into_iter()
            .find(|(n, _)| n == name)
            .map(|(_, settings)| {
                serde_json::from_str(&settings).map_err(|e| Error::Corrupt(e.to_string()))
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
        // Append, where sqlite had `INSERT OR REPLACE` on a four-column
        // primary key. The key was the whole row, so a re-declaration
        // wrote what was already there; on the lake it appends a
        // duplicate, and `relation_rows` collapses identical rows the
        // same way the primary key did.
        self.relations
            .append(
                "relationships",
                vec![vec![
                    Some(dataset.to_string()),
                    Some(left.to_string()),
                    Some(op.to_string()),
                    Some(right.to_string()),
                ]],
            )
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
        let existing = self
            .aspects_all()
            .await?
            .into_iter()
            .find(|a| a.name == name);
        if let Some(a) = existing {
            let schema: Value = serde_json::from_str(&a.schema)
                .map_err(|e| Error::Corrupt(format!("aspect `{name}` schema: {e}")))?;
            if schema == decl.schema.value
                && a.kind == kind_str(decl.kind)
                && a.grains == declared_grains
                && a.condition == declared_condition
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
            let producers: Vec<String> = self
                .functions_all()
                .await?
                .into_iter()
                .filter(|f| f.returns.as_deref() == Some(name))
                .map(|f| f.name)
                .collect();
            if !producers.is_empty() {
                let marks = vec!["?"; producers.len()].join(", ");
                let sql = format!(
                    "SELECT count(*) AS n FROM cache \
                     WHERE witness IS NULL AND function IN ({marks})"
                );
                let mut q = sqlx::query(&sql);
                for p in &producers {
                    q = q.bind(p.as_str());
                }
                let values: i64 = q.fetch_one(&self.pool).await?.get("n");
                if values > 0 {
                    return Err(Error::AspectValued {
                        name: name.into(),
                        values,
                    });
                }
            }
        }
        self.put(
            "aspects",
            vec![
                Some(decl.name.value.clone()),
                Some(kind_str(decl.kind).to_string()),
                declared_grains,
                condition_text(declared_condition.as_ref()),
                Some(decl.schema.raw.clone()),
            ],
        )
        .await
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
            if self.aspect(aspect.value.as_str()).await?.is_none() {
                return Err(Error::Unknown {
                    what: "aspect",
                    name: aspect.value.clone(),
                });
            }
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
                    let taken = self.functions_all().await?.into_iter().find(|f| {
                        f.returns.as_deref() == Some(aspect) && f.name != decl.name.value
                    });
                    if let Some(existing) = taken {
                        return Err(Error::MeasurementProducerTaken {
                            aspect: aspect.into(),
                            existing: existing.name,
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
            FunctionScope::Dataset(d) => d.value.clone(),
            FunctionScope::Global => "GLOBAL".to_string(),
        };
        self.put(
            "functions",
            vec![
                Some(decl.name.value.clone()),
                Some(scope),
                Some(decl.script.clone()),
                accepts,
                decl.returns.as_ref().map(|a| a.value.clone()),
            ],
        )
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

        self.put(
            "witnesses",
            vec![
                Some(decl.name.value.clone()),
                Some(aspect.to_string()),
                Some(Value::Array(speakers).to_string()),
                decl.detector.as_ref().map(|d| d.value.clone()),
                threshold.map(|t: f64| t.to_string()),
            ],
        )
        .await
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

    /// A landed table widens what dataset-sweeping measurements can see;
    /// the `imports` ACCEPTS edge invalidates them dataset-wide. The
    /// landing itself is the snapshot — nothing else is recorded.
    pub async fn landing_invalidates(&self, dataset: &str) -> Result<()> {
        self.invalidate(dataset, "imports", dataset).await
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


    async fn functions_accepting(&self, aspect: &str) -> Result<Vec<String>> {
        Ok(self
            .functions_all()
            .await?
            .into_iter()
            .filter(|f| f.accepts.iter().any(|a| a == aspect))
            .map(|f| f.name)
            .collect())
    }

    /// `(function, aspect)` for every function whose `RETURNS` names an
    /// aspect — the producer of a MEASUREMENT, the voices of a FACT. This
    /// binding is what wires a function's cache into the slots (ruled
    /// 2026-08-04; the function witnesses it replaced were ceremony).
    async fn returning(&self, aspect: Option<&str>) -> Result<Vec<(String, String)>> {
        Ok(self
            .functions_all()
            .await?
            .into_iter()
            .filter_map(|f| f.returns.map(|r| (f.name, r)))
            .filter(|(_, r)| aspect.is_none_or(|a| a == r))
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
        // The query fetches history; `rules::latest_by` picks the current
        // row. A source-grain row — its subject names a declared source
        // and its aspect opted into SOURCE grain — reads and supersedes
        // workspace-wide (ruled 2026-08-12); every other row stays
        // dataset-scoped. NULL grains (no ON clause) never qualify.
        let sql = format!(
            "SELECT g.id, g.dataset, g.subject, g.aspect, g.actor_kind, g.actor_id, g.body, \
                    g.written_at, g.snapshot_id \
             FROM glossary g \
             WHERE {pred} {aspect_clause}"
        );
        let mut q = sqlx::query(&sql);
        for b in &binds {
            q = q.bind(b);
        }
        if let Some(a) = aspect {
            q = q.bind(a);
        }
        let source_names: std::collections::HashSet<String> = self
            .sources_all()
            .await?
            .into_iter()
            .map(|(name, _)| name)
            .collect();
        let source_aspects: std::collections::HashSet<String> = self
            .aspects_all()
            .await?
            .into_iter()
            .filter(AspectRow::source_grain)
            .map(|a| a.name)
            .collect();
        let witnesses = self.witnesses_all().await?;
        let witness_on = |aspect: &str| {
            witnesses
                .iter()
                .find(|w| w.aspect == aspect)
                .map(|w| w.name.clone())
        };
        // (id, source-grain, dataset, slot) — everything the rule needs.
        let history: Vec<(i64, bool, String, String, Slot)> = q
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(|r| {
                let aspect: String = r.get("aspect");
                let actor_kind: String = r.get("actor_kind");
                let subject: String = r.get("subject");
                (
                    r.get::<i64, _>("id"),
                    source_names.contains(&subject) && source_aspects.contains(&aspect),
                    r.get::<String, _>("dataset"),
                    actor_kind.clone(),
                    Slot {
                        subject,
                        rank: rank_of(&actor_kind),
                        actor: r.get("actor_id"),
                        witness: witness_on(&aspect),
                        aspect,
                        body: r.get("body"),
                        written_at: r.get("written_at"),
                        snapshot_id: r.get("snapshot_id"),
                    },
                )
            })
            // A dataset-scoped row from another dataset is not in scope at
            // all; a source-grain row is in scope from everywhere.
            .filter(|(_, source_grain, ds, _, _)| *source_grain || ds == dataset)
            .collect();

        // The key carries the dataset only where the row is dataset-scoped,
        // which is what makes a source-grain row supersede workspace-wide.
        let mut rows: Vec<Slot> = rules::latest_by(
            history,
            // Keyed on actor KIND, not rank: rank folds every non-human
            // kind together, which is the same thing only while there are
            // exactly two of them.
            |(_, source_grain, ds, kind, s)| {
                (
                    (!*source_grain).then(|| ds.clone()),
                    s.subject.clone(),
                    s.aspect.clone(),
                    kind.clone(),
                )
            },
            |(id, ..)| *id,
        )
        .into_iter()
        .map(|(.., slot)| slot)
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
                    rank: rules::RANK_FUNCTION,
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
        Ok(self
            .aspects_all()
            .await?
            .into_iter()
            .map(|a| (a.name, a.kind))
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
        for ((subject, aspect), group) in grouped {
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
            // Contested needs voices that can differ (2026-08-14, found
            // live: a single-speaker measurement whose detector crossed
            // read as `contested`, and the withholding hid the body at
            // its most interesting moment). One slot cannot contest —
            // the crossing still shows as its band, beside the value.
            if rules::contested(crossing.is_some(), group.len()) {
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
            let serving = group[rules::serving(&group).expect("a group is never empty")];
            let current = table_of(&subject).and_then(|t| ctx.snapshots.get(t)).copied();
            rows.push(CollapsedRow {
                subject,
                aspect,
                value: Some(serving.body.clone()),
                band,
                score,
                state: rules::state(serving.snapshot_id, current).into(),
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
        for a in self.aspects_all().await? {
            grain_map.insert(a.name.clone(), a.grains);
            if let Some(condition) = a.condition {
                condition_map.insert(a.name, condition);
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
            let slots = self.slots(dataset, scope, Some(cond_aspect)).await?;
            sibling_values.insert(cond_aspect.clone(), rules::sibling_winners(&slots));
        }
        let present: std::collections::HashSet<(String, String)> = rows
            .iter()
            .map(|r| (r.subject.clone(), r.aspect.clone()))
            .collect();
        // Declared sources join the subject universe: a source-grain
        // aspect nobody spoke to is owed on every declared source
        // (ruled 2026-08-12), in whichever dataset the read runs.
        let source_names: std::collections::BTreeSet<String> = self
            .sources_all()
            .await?
            .into_iter()
            .map(|(name, _)| name)
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
                    // A source name is SOURCE grain, never table grain —
                    // the admission rule above, mirrored so the backlog
                    // never carries rows admission would refuse.
                    Some(declared) => {
                        let effective = if source_names.contains(subject) {
                            "source"
                        } else {
                            grain_of(dataset, subject)
                        };
                        declared.split(',').any(|g| g == effective)
                    }
                };
                if !in_grain {
                    continue;
                }
                if let Some((cond_aspect, cond_value)) = condition_map.get(a) {
                    let sibling = sibling_values
                        .get(cond_aspect)
                        .and_then(|m| m.get(subject.as_str()));
                    if !rules::condition_holds(cond_value, sibling.map(String::as_str)) {
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
        let returns = self
            .function(function, None)
            .await?
            .and_then(|f| f.returns);
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
            let detectors: Vec<String> = self
                .functions_all()
                .await?
                .into_iter()
                .filter(|f| f.returns.is_none())
                .map(|f| f.name)
                .collect();
            if !detectors.is_empty() {
                let marks = vec!["?"; detectors.len()].join(", ");
                let sql = format!("DELETE FROM cache WHERE function IN ({marks})");
                let mut q = sqlx::query(&sql);
                for d in &detectors {
                    q = q.bind(d.as_str());
                }
                q.execute(&self.pool).await?;
            }
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
                    // A struck function voice makes the slot set older,
                    // never newer, so a cached verdict would pass the
                    // freshness check and keep withholding — the same
                    // hazard the glossary side closed (2026-08-05),
                    // applied to the cache target. The witnesses whose
                    // aspect the function RETURNS lose their verdicts
                    // for the struck subject.
                    let functions = self.functions_all().await?;
                    let witnesses = self.witnesses_all().await?;
                    for (dataset, subject, function) in &triples {
                        let returns = functions
                            .iter()
                            .find(|f| &f.name == function)
                            .and_then(|f| f.returns.as_deref());
                        for w in witnesses
                            .iter()
                            .filter(|w| returns == Some(w.aspect.as_str()))
                        {
                            sqlx::query(
                                "DELETE FROM cache WHERE dataset = ? AND subject = ? \
                                 AND witness = ?",
                            )
                            .bind(dataset)
                            .bind(subject)
                            .bind(w.name.as_str())
                            .execute(&self.pool)
                            .await?;
                        }
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

    /// One appended row into a crossed relation.
    async fn put(&self, relation: &str, cells: Vec<Option<String>>) -> Result<()> {
        self.relations
            .append(relation, vec![cells])
            .await
            .map_err(Error::from)
    }

    /// A crossed relation's current rows: latest per [`Relation`] key in
    /// `(seq, pos)` order, sorted by cells — what the sqlite primary key
    /// and `ORDER BY` used to do.
    async fn lake_rows(&self, relation: &Relation) -> Result<Vec<Vec<Option<String>>>> {
        let history = self
            .relations
            .scan(relation.name)
            .await?;
        let key: Vec<usize> = relation
            .key
            .iter()
            .map(|k| {
                relation
                    .columns
                    .iter()
                    .position(|c| c == k)
                    .expect("a key names one of its relation's columns")
            })
            .collect();
        let mut rows: Vec<_> = rules::latest_by(
            history,
            |r| {
                if key.is_empty() {
                    r.cells.clone()
                } else {
                    key.iter().map(|&i| r.cells[i].clone()).collect()
                }
            },
            |r| r.seq,
        )
        .into_iter()
        .map(|r| r.cells)
        .collect();
        rows.sort();
        Ok(rows)
    }

    /// Full relation dump for substrate `SELECT`s over the store's
    /// relations — the names and column shapes live in [`RELATIONS`].
    pub async fn relation_rows(&self, table: &str) -> Result<Vec<Vec<Option<String>>>> {
        let Some(relation) = RELATIONS.iter().find(|r| r.name == table) else {
            return Err(Error::ForwardRejected(table.into()));
        };
        // Two relations are the lake's own record, composed rather than
        // stored: datasets are the namespaces, imports are the snapshots.
        match relation.name {
            "datasets" => return self.dataset_rows().await,
            "imports" => return self.import_rows().await,
            _ => {}
        }
        let Some(sql) = relation.sql else {
            return self.lake_rows(relation).await;
        };
        let rows = sqlx::query(sql).fetch_all(&self.pool).await?;
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
        if name == STORE_NAMESPACE {
            return Ok(false);
        }
        Ok(self
            .lake
            .namespaces()
            .await?
            .iter()
            .any(|(n, _)| n == name))
    }

    async fn dataset_rows(&self) -> Result<Vec<Vec<Option<String>>>> {
        let mut rows: Vec<_> = self
            .lake
            .namespaces()
            .await?
            .into_iter()
            .filter(|(name, _)| name != STORE_NAMESPACE)
            .map(|(name, props)| {
                vec![
                    Some(name),
                    Some(props.get(SETTINGS_PROP).cloned().unwrap_or_else(|| "{}".into())),
                ]
            })
            .collect();
        rows.sort();
        Ok(rows)
    }

    async fn import_rows(&self) -> Result<Vec<Vec<Option<String>>>> {
        let mut rows = Vec::new();
        for (dataset, _) in self
            .lake
            .namespaces()
            .await?
        {
            if dataset == STORE_NAMESPACE {
                continue;
            }
            for l in self
                .lake
                .landings(&dataset)
                .await?
            {
                rows.push(vec![
                    Some(l.dataset),
                    Some(l.table),
                    l.properties.get(LANDING_SCANS_PROP).cloned(),
                    l.added_records.map(|n| n.to_string()),
                    l.properties.get(LANDING_DROPPED_PROP).cloned(),
                    l.properties.get(LANDING_CASTS_PROP).cloned(),
                    Some(l.committed_at),
                ]);
            }
        }
        rows.sort();
        Ok(rows)
    }

    // -- the crossed declarations, read whole (each is a handful of rows)

    async fn sources_all(&self) -> Result<Vec<(String, String)>> {
        Ok(self
            .lake_rows(relation("sources"))
            .await?
            .into_iter()
            .map(|r| (text(&r, 0), text(&r, 1)))
            .collect())
    }

    async fn aspects_all(&self) -> Result<Vec<AspectRow>> {
        Ok(self
            .lake_rows(relation("aspects"))
            .await?
            .iter()
            .map(AspectRow::from_cells)
            .collect())
    }

    async fn functions_all(&self) -> Result<Vec<FunctionRow>> {
        self.lake_rows(relation("functions"))
            .await?
            .iter()
            .map(function_row)
            .collect()
    }

    pub async fn witnesses_all(&self) -> Result<Vec<WitnessRow>> {
        self.lake_rows(relation("witnesses"))
            .await?
            .iter()
            .map(witness_row)
            .collect()
    }

    /// `(schema, kind, grains)` — grains is the declared `ON` list
    /// (comma-joined, lowercase), `None` when the aspect speaks to all
    /// grains.
    pub async fn aspect(&self, name: &str) -> Result<Option<(Value, String, Option<String>)>> {
        let Some(a) = self.aspects_all().await?.into_iter().find(|a| a.name == name) else {
            return Ok(None);
        };
        let schema = serde_json::from_str(&a.schema)
            .map_err(|e| Error::Corrupt(format!("aspect `{name}` schema: {e}")))?;
        Ok(Some((schema, a.kind, a.grains)))
    }

    /// Resolve a function visible from `dataset` (`FOR` scope or GLOBAL,
    /// SPEC.md §6). `None` skips the visibility check.
    pub async fn function(&self, name: &str, dataset: Option<&str>) -> Result<Option<FunctionRow>> {
        let Some(f) = self.functions_all().await?.into_iter().find(|f| f.name == name) else {
            return Ok(None);
        };
        match (dataset, &f.scope_dataset) {
            (Some(d), Some(scope)) if scope != d => Ok(None),
            _ => Ok(Some(f)),
        }
    }

    pub async fn witnesses_on(&self, aspect: &str) -> Result<Vec<WitnessRow>> {
        Ok(self
            .witnesses_all()
            .await?
            .into_iter()
            .filter(|w| w.aspect == aspect)
            .collect())
    }
}

fn relation(name: &str) -> &'static Relation {
    RELATIONS
        .iter()
        .find(|r| r.name == name)
        .expect("a declared relation")
}

fn text(cells: &[Option<String>], i: usize) -> String {
    cells.get(i).cloned().flatten().unwrap_or_default()
}

fn cell(cells: &[Option<String>], i: usize) -> Option<String> {
    cells.get(i).cloned().flatten()
}

/// An `aspects` row, cells parsed once. `condition` round-trips through
/// the served `aspect = 'value'` text — one renderer, one parser.
struct AspectRow {
    name: String,
    kind: String,
    grains: Option<String>,
    condition: Option<(String, String)>,
    schema: String,
}

impl AspectRow {
    fn from_cells(cells: &Vec<Option<String>>) -> AspectRow {
        AspectRow {
            name: text(cells, 0),
            kind: text(cells, 1),
            grains: cell(cells, 2),
            condition: cell(cells, 3).as_deref().and_then(parse_condition),
            schema: text(cells, 4),
        }
    }

    fn source_grain(&self) -> bool {
        self.grains
            .as_deref()
            .is_some_and(|g| g.split(',').any(|g| g == "source"))
    }
}

fn condition_text(condition: Option<&(String, String)>) -> Option<String> {
    condition.map(|(aspect, value)| format!("{aspect} = '{value}'"))
}

fn parse_condition(text: &str) -> Option<(String, String)> {
    let (aspect, rest) = text.split_once(" = '")?;
    Some((aspect.to_string(), rest.strip_suffix('\'')?.to_string()))
}

fn function_row(cells: &Vec<Option<String>>) -> Result<FunctionRow> {
    let name = text(cells, 0);
    let accepts = match cell(cells, 3) {
        None => Vec::new(),
        Some(t) => serde_json::from_str(&t)
            .map_err(|e| Error::Corrupt(format!("function `{name}` ACCEPTS: {e}")))?,
    };
    Ok(FunctionRow {
        scope_dataset: cell(cells, 1).filter(|s| s != "GLOBAL"),
        script: text(cells, 2),
        returns: cell(cells, 4),
        accepts,
        name,
    })
}

fn witness_row(cells: &Vec<Option<String>>) -> Result<WitnessRow> {
    let speakers: Vec<String> = serde_json::from_str(&text(cells, 2))
        .map_err(|e| Error::Corrupt(format!("witness speakers: {e}")))?;
    let threshold = cell(cells, 4)
        .map(|t| {
            t.parse()
                .map_err(|_| Error::Corrupt(format!("witness threshold `{t}`")))
        })
        .transpose()?;
    Ok(WitnessRow {
        name: text(cells, 0),
        aspect: text(cells, 1),
        admits_agent: speakers.iter().any(|s| s == "agent"),
        admits_human: speakers.iter().any(|s| s == "human"),
        detector: cell(cells, 3),
        threshold,
    })
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
