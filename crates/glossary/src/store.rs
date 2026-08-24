//! The store: every relation an Iceberg table in the workspace's lake,
//! every rule a pure function over rows. Admission (SPEC.md §5.2, §7.1)
//! happens on the write paths; supersession is a read — latest row per
//! key in the format's own `(sequence, position)` order — never an
//! update.

use std::sync::Arc;

use glossql_catalog::Lake;
use serde_json::Value;

use glossql_parser::{
    AspectDecl, AspectKind, DatasetDecl, FunctionDecl, FunctionScope, Grain, JsonBody, RecipeDecl,
    SourceDecl, Speaker, WitnessDecl,
};

use crate::rules::{self, Slot, admit_grain, grain_of, rank_of};
use crate::schemas::grounding_schema;
use crate::types::{
    Actor, ActorKind, AspectRow, CollapsedRow, Error, FunctionRow, GlossRow, MeasurementRow,
    RawRow, RecipeAdmission, RecipeRow, Result, WitnessRow,
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
    /// The statement's pin — what every measurement this read serves or
    /// computes is keyed by.
    pub pin: Pin,
    /// The store's version when this context was built — every relation
    /// table at its snapshot, derived from the catalog. This is what the
    /// cache is keyed by, and it is enumerated rather than curated so a
    /// relation added later joins the key on its own.
    ///
    /// Not the pin: the pin is the *semantic* input key and deliberately
    /// excludes `measurements`, an output. Keyed by pin, a landed
    /// measurement would move nothing and stay invisible to every reader
    /// but the one that computed it.
    pub version: String,
    /// The store, resolved once with the pin: one statement, one read of
    /// every relation — the rules below do no IO of their own.
    pub glossary: std::sync::Arc<Vec<GlossRow>>,
    /// Every (function, subject)'s newest landing in the dataset,
    /// whatever its pin — the serve-and-mark rule (SPEC.md §7): a voice
    /// from an earlier pin still serves, and the pin column says so.
    /// [`Store::measurement_in`] is the pin-exact lookup extraction
    /// serves a repeat from.
    pub measurements: std::sync::Arc<Vec<glossql_catalog::Row>>,
    pub functions: std::sync::Arc<Vec<FunctionRow>>,
    pub witnesses: std::sync::Arc<Vec<WitnessRow>>,
    pub sources: std::sync::Arc<Vec<(String, String)>>,
    pub aspects: std::sync::Arc<Vec<AspectRow>>,
}

/// The sorted (input → version) list of everything a computation can
/// read: data tables and declaration relations at their snapshots, and
/// the glossary at its write head while it still rides sqlite (its
/// component becomes a snapshot like the rest when it crosses). Under a
/// complete key there is no invalidation, only a miss.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Pin {
    pub text: String,
    pub digest: String,
}

impl Pin {
    fn new(mut parts: Vec<String>) -> Pin {
        parts.sort();
        let text = parts.join(",");
        // FNV-1a over the canonical text: an index into the relation,
        // never the truth — lookups compare the pin itself.
        let mut h: u64 = 0xcbf2_9ce4_8422_2325;
        for b in text.bytes() {
            h ^= u64::from(b);
            h = h.wrapping_mul(0x100_0000_01b3);
        }
        Pin {
            digest: format!("{h:016x}"),
            text,
        }
    }
}

impl Scope {
    /// Does the scope admit this subject — the same five shapes every
    /// read applies.
    pub fn admits(&self, subject: &str) -> bool {
        match self {
            Scope::Dataset => true,
            Scope::Subject(s) => {
                subject == s
                    || subject.starts_with(&format!("{s}."))
                    || subject.starts_with(&format!("{s} "))
                    || subject.ends_with(&format!("> {s}"))
                    || subject.contains(&format!("> {s}."))
            }
        }
    }
}

/// A store relation readable as a plain table through the doors. The
/// one place its shape lives: every other list (the session's read
/// planner, the doors' cap policy) derives from here.
pub struct Relation {
    pub name: &'static str,
    pub columns: &'static [&'static str],
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
        // a sum across a join reads as "what was read" and is not —
        // it fabricates phantom dropped rows.
        partition: &[],
        key: &[],
    },
    Relation {
        name: "functions",
        columns: &["name", "scope", "script", "returns"],
        partition: &[],
        key: &["name"],
    },
    Relation {
        name: "aspects",
        columns: &["name", "kind", "grains", "condition", "schema"],
        partition: &[],
        key: &["name"],
    },
    Relation {
        name: "witnesses",
        columns: &["name", "aspect", "speakers", "detector", "threshold"],
        partition: &[],
        key: &["name"],
    },
    Relation {
        name: "sources",
        columns: &["name", "settings"],
        partition: &[],
        key: &["name"],
    },
    // Served from the namespace list; settings ride as a property.
    Relation {
        name: "datasets",
        columns: &["name", "settings"],
        partition: &[],
        key: &[],
    },
    // First across: no supersession of its own, one writer,
    // one reader.
    Relation {
        name: "relationships",
        columns: &["dataset", "left_path", "op", "right_path"],
        partition: &["dataset"],
        key: &[],
    },
    // What extraction lands, keyed by the pin: under a complete key
    // there is no invalidation, only a miss, and old pins' rows are the
    // drift record rather than garbage (stage 4).
    Relation {
        name: "measurements",
        columns: &[
            "dataset",
            "function",
            "subject",
            "aspect",
            "pin_digest",
            "pin",
            "value",
            "computed_at",
        ],
        partition: &["dataset"],
        key: &[],
    },
];

/// The column shape of a readable store relation, `None` for any other
/// name — the lookup the session's planner and the doors share.
pub fn relation_columns(name: &str) -> Option<&'static [&'static str]> {
    RELATIONS.iter().find(|r| r.name == name).map(|r| r.columns)
}

/// Where every crossed relation lives: one namespace, one table per
/// relation. A workspace holds many datasets — that scopes rows by a
/// `dataset` KEY column, with the physical per-dataset split supplied by
/// the format (identity partition), not by a namespace layout of ours.
/// A `<dataset>_meta` sibling-namespace pairing is deliberately
/// absent: its whole justification is per-dataset REST grants, and
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
async fn lake_metadata(lake: Lake) -> Result<Arc<glossql_catalog::IcebergMetadata>> {
    // `datasets` and `imports` are the lake's own record, composed at
    // read — no table of ours carries them.
    let moved: Vec<glossql_catalog::RelationSpec> = RELATIONS
        .iter()
        .filter(|r| !matches!(r.name, "datasets" | "imports"))
        .map(|r| glossql_catalog::RelationSpec {
            name: r.name,
            columns: r.columns,
            partition: r.partition,
        })
        .collect();
    let relations = glossql_catalog::IcebergMetadata::open(lake, STORE_NAMESPACE, &moved).await?;
    Ok(Arc::new(relations))
}

/// Every store relation at its snapshot — what both the version and the
/// pin are derived from, shared rather than copied because every read
/// wants the same one.
type Snapshots = Arc<Vec<(String, Option<i64>)>>;

#[derive(Debug, Clone)]
pub struct Store {
    lake: Lake,
    metadata: Arc<glossql_catalog::IcebergMetadata>,
    /// The store namespace's head: every relation at its snapshot,
    /// walked once and held until a write moves it. The walk is a
    /// catalog round trip per relation (~2.4 ms on a small workspace)
    /// and it sits in front of every read, so holding it is the
    /// difference between paying it per statement and paying it per
    /// write.
    ///
    /// **A commit drops it** — see [`Store::put`], the one place a
    /// store relation moves. Correct only while one process owns the
    /// workspace: the head is this process's memory, and a second
    /// writer's commit would leave it stale with nothing to say so.
    /// Two processes need shared state, which is a different design.
    head: Arc<std::sync::RwLock<Option<Snapshots>>>,
    /// The resolved store behind a read, one entry per dataset, each
    /// holding the version it was built at. A read whose version still
    /// matches takes it; a read whose version moved replaces it. There
    /// is no eviction rule because there is nothing to evict: a moved
    /// version makes the old entry unreachable, and the map is bounded
    /// by the workspace's datasets.
    ///
    /// Keyed by dataset because `measurements` is dataset-scoped; the
    /// other five relations are workspace-wide and simply repeat.
    contexts: Arc<std::sync::RwLock<std::collections::HashMap<String, ReadContext>>>,
}

/// What the connect-time brief is composed from — see
/// [`Store::brief_counts`]. `PartialEq` is load-bearing: the door
/// decides "did the brief move" by comparing these counts, never by
/// comparing rendered lines (string equality on non-keys is
/// forbidden).
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
    /// The store over a workspace's own lake — where every relation
    /// lives; SQLite remains only as the catalog's own backend.
    pub async fn open(lake: Lake) -> Result<Self> {
        Ok(Store {
            metadata: lake_metadata(lake.clone()).await?,
            lake,
            head: Arc::new(std::sync::RwLock::new(None)),
            contexts: Arc::new(std::sync::RwLock::new(Default::default())),
        })
    }

    /// The one lake behind this store — sessions and doors share it.
    pub fn lake(&self) -> Lake {
        self.lake.clone()
    }

    /// The connect-time brief's raw counts: what an agent connecting
    /// right now should know exists
    /// before it acts. Cheap by design — two queries, no collapse.
    pub async fn brief_counts(&self) -> Result<BriefCounts> {
        let history = self.glossary_history().await?;
        let humans: Vec<&GlossRow> = history.iter().filter(|g| g.actor_kind == "human").collect();
        let latest_human_at = humans.iter().map(|g| g.written_at.clone()).max();
        // An approval is pending while its table has no landing at or
        // after the ruling was written. Landings read from the lake —
        // scoped to the datasets the standing approvals actually name,
        // never a walk of every landing in the workspace.
        // Keyed with `dataset`, as the store's own collapse is: a
        // subject name is unique within a dataset and not across a
        // workspace, so a bare subject key collapses two datasets'
        // approvals into one and under-reports what is pending.
        let approvals: Vec<(String, String, String)> = latest_rows(&history, |g| {
            (g.actor_kind == "human" && g.aspect == "recipe_change")
                .then(|| (g.dataset.clone(), g.subject.clone()))
        })
        .into_iter()
        .filter_map(|g| {
            let body: Value = serde_json::from_str(&g.body).unwrap_or(Value::Null);
            body["table"]
                .as_str()
                .map(|t| (g.dataset.clone(), t.to_string(), g.written_at.clone()))
        })
        .collect();
        let mut landed: std::collections::HashMap<String, Vec<(String, String)>> =
            Default::default();
        for dataset in approvals
            .iter()
            .map(|(d, _, _)| d.clone())
            .collect::<std::collections::BTreeSet<_>>()
        {
            let landings = self
                .lake
                .landings(&dataset)
                .await?
                .into_iter()
                .map(|l| (l.table, l.committed_at))
                .collect();
            landed.insert(dataset, landings);
        }
        let approvals_pending = approvals
            .iter()
            .filter(|(dataset, table, at)| {
                !landed.get(dataset).is_some_and(|ls| {
                    ls.iter()
                        .any(|(t, l_at)| t == table && l_at.as_str() >= at.as_str())
                })
            })
            .count() as i64;
        // A ruling awaits its fold-in while the assumption it rules —
        // named by its `key`, the agent-declared identity that survives
        // rephrasing (prose is never a join column) —
        // still stands below full confidence in the agent's current
        // body. The agent's re-record clears the debt and the round's
        // question at once.
        let mut rulings_owed = 0i64;
        for r in latest_rows(&history, |g| {
            (g.actor_kind == "human" && g.aspect == "ruling")
                .then(|| (g.dataset.clone(), g.subject.clone()))
        }) {
            let body: Value = serde_json::from_str(&r.body).unwrap_or(Value::Null);
            for jr in body["rulings"].as_array().into_iter().flatten() {
                let (Some(key), Some(aspect)) = (jr["key"].as_str(), jr["aspect"].as_str()) else {
                    continue;
                };
                // `r.dataset` in the filter, not the key: the key is the
                // unit, so every row admitted here collapses to one, and
                // without this leg that one row can be another dataset's
                // grounding — the fold-in debt read off the wrong body.
                let owed = latest_rows(&history, |g| {
                    (g.actor_kind == "agent"
                        && g.dataset == r.dataset
                        && g.subject == r.subject
                        && g.aspect == aspect)
                        .then_some(())
                })
                .into_iter()
                .any(|a| {
                    let body: Value = serde_json::from_str(&a.body).unwrap_or(Value::Null);
                    body["assumptions"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .any(|ja| {
                            ja["key"].as_str() == Some(key)
                                && ja["confidence"].as_f64().unwrap_or(0.0) < 1.0
                        })
                });
                if owed {
                    rulings_owed += 1;
                }
            }
        }
        Ok(BriefCounts {
            human_writings: humans.len() as i64,
            latest_human_at,
            approvals_pending,
            rulings_owed,
        })
    }

    // -- declarations ----------------------------------------------------

    pub async fn declare_source(&self, decl: &SourceDecl) -> Result<()> {
        let row = vec![
            Some(decl.name.value.clone()),
            Some(settings_json(&decl.settings)),
        ];
        self.put_unless_current("sources", row).await
    }

    /// A dataset is its namespace; the settings ride it as a property,
    /// set at create and not changed afterwards.
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
    /// no-op; a changed one **supersedes and re-lands** — refusing a
    /// changed recipe dead-ends every cure of a post-landing
    /// defect. The recipe row supersedes
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
        // The source is checked where it is read: the session resolves
        // the spec on the same statement and refuses an unknown source
        // with the same words.
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
    /// precede the table it describes.
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
        let Some(props) = self.lake.table_properties(dataset, table).await? else {
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
        let row = vec![
            Some(dataset.to_string()),
            Some(left.to_string()),
            Some(op.to_string()),
            Some(right.to_string()),
        ];
        self.put_unless_current("relationships", row).await
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
        // Conditional relevance: the referenced
        // sibling aspect must exist — nothing else ever errors on that.
        // The literal itself is not judged: a value no slot ever
        // carries makes a condition that never holds, exactly as it
        // would for any schema shape without an enum.
        let declared_condition = decl
            .condition
            .as_ref()
            .map(|(a, v)| (a.value.clone(), v.clone()));
        if let Some((cond_aspect, _)) = &declared_condition
            && self.aspect(cond_aspect).await?.is_none()
        {
            return Err(Error::BadCondition {
                name: name.into(),
                detail: format!("WHEN references undeclared aspect `{cond_aspect}`"),
            });
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
            let glosses = self
                .glossary_history()
                .await?
                .iter()
                .filter(|g| g.aspect == name)
                .count() as i64;
            if glosses > 0 {
                return Err(Error::AspectInUse {
                    name: name.into(),
                    glosses,
                });
            }
            // Measurement rows under the old schema need no guard: they
            // are keyed by a pin that includes the aspects relation, so
            // a re-declaration makes them unreachable, never mis-served.
        }
        self.put_unless_current(
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

    pub async fn declare_function(&self, decl: &FunctionDecl) -> Result<()> {
        // RETURNS names the aspect the output fills. A MEASUREMENT aspect
        // has one producer; a FACT aspect's returning functions are voices;
        // a QUERY aspect is never function-filled. No RETURNS declares a
        // detector.
        if let Some(aspect) = &decl.returns {
            let aspect = aspect.value.as_str();
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
        let scope = match &decl.scope {
            FunctionScope::Dataset(d) => d.value.clone(),
            FunctionScope::Global => "GLOBAL".to_string(),
        };
        // A CHANGED function is a different function: its old
        // measurements sit at pins that no longer resolve, because the
        // functions relation moved. An unchanged re-declare writes
        // nothing, so it moves no pin.
        let row = vec![
            Some(decl.name.value.clone()),
            Some(scope),
            Some(decl.script.clone()),
            decl.returns.as_ref().map(|a| a.value.clone()),
        ];
        self.put_unless_current("functions", row).await
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

        // BY gates actor glosses only — function voices arrive via
        // RETURNS. Measurements are never glossed, so a witness
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

        let row = vec![
            Some(decl.name.value.clone()),
            Some(aspect.to_string()),
            Some(Value::Array(speakers).to_string()),
            decl.detector.as_ref().map(|d| d.value.clone()),
            threshold.map(|t: f64| t.to_string()),
        ];
        self.put_unless_current("witnesses", row).await
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
        self.put(
            "glossary",
            vec![
                Some(dataset.to_string()),
                Some(subject.to_string()),
                Some(aspect.to_string()),
                Some(actor.kind.as_str().to_string()),
                Some(actor.id.clone()),
                Some(body.raw.clone()),
                Some(now_utc()),
                snapshot_id.map(|s| s.to_string()),
            ],
        )
        .await
    }

    /// Glosses at or under a table — what `DROP TABLE` refuses on.
    pub async fn glosses_under(&self, dataset: &str, table: &str) -> Result<i64> {
        let scope = Scope::Subject(table.to_string());
        Ok(self
            .glossary_history()
            .await?
            .iter()
            .filter(|g| g.dataset == dataset && scope.admits(&g.subject))
            .count() as i64)
    }

    // -- reads -----------------------------------------------------------

    /// The current slots under a scope: gloss slots by supersession (one per
    /// actor kind), plus the measurement slot of every returning function —
    /// its newest landing whatever the pin, marked `current` only when
    /// that pin is the read's. Both read shapes build from these.
    fn slots(dataset: &str, scope: &Scope, aspect: Option<&str>, ctx: &ReadContext) -> Vec<Slot> {
        let source_names: std::collections::HashSet<&str> =
            ctx.sources.iter().map(|(name, _)| name.as_str()).collect();
        let source_aspects: std::collections::HashSet<&str> = ctx
            .aspects
            .iter()
            .filter(|a| a.source_grain())
            .map(|a| a.name.as_str())
            .collect();
        let witness_on = |aspect: &str| {
            ctx.witnesses
                .iter()
                .find(|w| w.aspect == aspect)
                .map(|w| w.name.clone())
        };
        // The history, then `rules::latest_by` picks the current row. A
        // source-grain row — its subject names a declared source and its
        // aspect opted into SOURCE grain — reads and supersedes
        // workspace-wide; every other row stays
        // dataset-scoped, and a dataset-scoped row from another dataset
        // is not in scope at all.
        let history: Vec<((i64, i64), bool, &str, &str, Slot)> = ctx
            .glossary
            .iter()
            .filter(|g| scope.admits(&g.subject) && aspect.is_none_or(|a| g.aspect == a))
            .map(|g| {
                (
                    g.seq,
                    source_names.contains(g.subject.as_str())
                        && source_aspects.contains(g.aspect.as_str()),
                    g.dataset.as_str(),
                    g.actor_kind.as_str(),
                    Slot {
                        rank: rank_of(&g.actor_kind),
                        witness: witness_on(&g.aspect),
                        subject: g.subject.clone(),
                        aspect: g.aspect.clone(),
                        actor: g.actor_id.clone(),
                        body: g.body.clone(),
                        written_at: g.written_at.clone(),
                        snapshot_id: g.snapshot_id,
                        current: true,
                    },
                )
            })
            .filter(|(_, source_grain, ds, _, _)| *source_grain || *ds == dataset)
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
                    (!*source_grain).then(|| ds.to_string()),
                    s.subject.clone(),
                    s.aspect.clone(),
                    kind.to_string(),
                )
            },
            |(seq, ..)| *seq,
        )
        .into_iter()
        .map(|(.., slot)| slot)
        .collect();

        // The measurement slot: each returning function's newest landing
        // per subject, whatever its pin — served, and marked `current`
        // only at its own. Older rows are the drift record, never
        // served.
        let returning = ctx.functions.iter().filter_map(|f| {
            f.returns
                .as_deref()
                .filter(|r| aspect.is_none_or(|a| a == *r))
                .map(|r| (f.name.clone(), r.to_string()))
        });
        for (f, a) in returning {
            for r in ctx.measurements.iter().filter(|r| {
                r.get(0) == Some(dataset)
                    && r.get(1) == Some(f.as_str())
                    && r.get(2).is_some_and(|s| scope.admits(s))
            }) {
                rows.push(Slot {
                    subject: text(&r.cells, 2),
                    aspect: a.clone(),
                    rank: rules::RANK_FUNCTION,
                    actor: f.clone(),
                    witness: witness_on(&a),
                    body: text(&r.cells, 6),
                    written_at: text(&r.cells, 7),
                    snapshot_id: None,
                    current: r.get(5) == Some(ctx.pin.text.as_str()),
                });
            }
        }
        rows.sort_by(|a, b| {
            (&a.subject, &a.aspect, &a.actor).cmp(&(&b.subject, &b.aspect, &b.actor))
        });
        rows
    }

    /// The raw read (SPEC.md §5.3): every current slot, one row each;
    /// precedence is the reader's business here. `kind` is the aspect's kind.
    pub fn raw_read(
        dataset: &str,
        scope: &Scope,
        aspect: Option<&str>,
        ctx: &ReadContext,
    ) -> Vec<RawRow> {
        let kinds: std::collections::HashMap<&str, &str> = ctx
            .aspects
            .iter()
            .map(|a| (a.name.as_str(), a.kind.as_str()))
            .collect();
        Self::slots(dataset, scope, aspect, ctx)
            .into_iter()
            .map(|s| RawRow {
                kind: kinds
                    .get(s.aspect.as_str())
                    .map(|k| k.to_string())
                    .unwrap_or_default(),
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
                current: s.current,
            })
            .collect()
    }

    /// The collapsed read (SPEC.md §5.3): value by precedence (human over
    /// agent over function), withheld only when the detector's score exceeds
    /// the witness threshold; `state` makes every gap visible — see
    /// [`CollapsedRow`]. The `ReadContext` universe adds `unassessed` rows
    /// for witnessed aspects nobody spoke to.
    pub fn collapsed_read(
        dataset: &str,
        scope: &Scope,
        aspect: Option<&str>,
        ctx: &ReadContext,
        verdicts: &crate::types::Verdicts,
    ) -> Vec<CollapsedRow> {
        let slots = Self::slots(dataset, scope, aspect, ctx);
        let mut grouped: std::collections::BTreeMap<(String, String), Vec<&Slot>> =
            std::collections::BTreeMap::new();
        for s in &slots {
            grouped
                .entry((s.subject.clone(), s.aspect.clone()))
                .or_default()
                .push(s);
        }

        // The verdicts arrive computed (the session holds the runtime;
        // detectors run at read, never stored). Each is judged against
        // its own witness's threshold — a score is never compared to a
        // neighbour witness's. Plural witnesses per
        // aspect are allowed; the slot is contested when ANY witness's
        // verdict bands red — conservative withholding, and the
        // detector's own band is what says so ([`rules::withholds`]).
        let mut rows = Vec::new();
        for ((subject, aspect), group) in grouped {
            let verdict = verdicts.get(&(subject.clone(), aspect.clone()));
            let crossing = verdict.and_then(|v| v.iter().find(|v| rules::withholds(&v.band)));
            // The row carries one verdict: the crossing one when contested,
            // the first in witness name order otherwise.
            let shown = crossing.or_else(|| verdict.and_then(|v| v.first()));
            let (band, score) = match shown {
                Some(v) => (Some(v.band.clone()), Some(v.score)),
                None => (None, None),
            };
            // Contested needs voices that can differ: a single-speaker
            // measurement whose detector crossed would read as
            // `contested`, and the withholding would hide the body at
            // its most interesting moment. One slot cannot contest —
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
            let current = table_of(&subject)
                .and_then(|t| ctx.snapshots.get(t))
                .copied();
            // Served and marked either way: a gloss whose table moved
            // on, or a voice landed at an earlier pin.
            let state = if serving.current {
                rules::state(serving.snapshot_id, current)
            } else {
                "stale"
            };
            rows.push(CollapsedRow {
                subject,
                aspect,
                value: Some(serving.body.clone()),
                band,
                score,
                state: state.into(),
            });
        }

        // Disclosure (fixture 09's benchmark): an aspect somebody is bound
        // to speak to — witnessed, or produced by a function's RETURNS —
        // that nobody spoke to is a visible row, not an omission. Grain
        // bounds the grid: absence only shows on
        // subjects the aspect is declared to speak to.
        let mut witnessed: std::collections::BTreeSet<String> = ctx
            .witnesses
            .iter()
            .filter(|w| aspect.is_none_or(|a| w.aspect == a))
            .map(|w| w.aspect.clone())
            .collect();
        witnessed.extend(
            ctx.functions
                .iter()
                .filter_map(|f| f.returns.clone().filter(|r| aspect.is_none_or(|a| a == r))),
        );
        let mut grain_map: std::collections::HashMap<String, Option<String>> =
            std::collections::HashMap::new();
        let mut condition_map: std::collections::HashMap<String, (String, String)> =
            std::collections::HashMap::new();
        for a in ctx.aspects.iter() {
            grain_map.insert(a.name.clone(), a.grains.clone());
            if let Some(condition) = &a.condition {
                condition_map.insert(a.name.clone(), condition.clone());
            }
        }
        // Conditional relevance: a conditioned aspect
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
            let slots = Self::slots(dataset, scope, Some(cond_aspect), ctx);
            sibling_values.insert(cond_aspect.clone(), rules::sibling_winners(&slots));
        }
        let present: std::collections::HashSet<(String, String)> = rows
            .iter()
            .map(|r| (r.subject.clone(), r.aspect.clone()))
            .collect();
        // Declared sources join the subject universe: a source-grain
        // aspect nobody spoke to is owed on every declared source,
        // in whichever dataset the read runs.
        let source_names: std::collections::BTreeSet<String> =
            ctx.sources.iter().map(|(name, _)| name.clone()).collect();
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
                    // `rules::admit_grain`, mirrored so the backlog
                    // carries exactly the rows admission would take: a
                    // row it refuses is unfillable, and one it takes and
                    // the backlog hides is owed in silence.
                    Some(declared) => {
                        let is_source = source_names.contains(subject);
                        let effective = if is_source {
                            "source"
                        } else {
                            grain_of(dataset, subject)
                        };
                        let admits = |grain: &str| declared.split(',').any(|g| g == grain);
                        admits(effective) || (is_source && subject == dataset && admits("dataset"))
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
        rows
    }

    // -- SQL forwarded from the session ----------------------------------

    /// `DELETE FROM glossary …` — the strike (SPEC.md §5.2). Parked:
    /// the substrate cannot commit a row removal
    /// until iceberg-rust 0.11 lands the delete write path, so the
    /// refusal names the item instead of pretending.
    pub async fn forward_delete(&self, target: &str) -> Result<u64> {
        if target != "glossary" {
            return Err(Error::ForwardRejected(target.into()));
        }
        Err(Error::StrikeParked)
    }

    /// Statement identity is content (SPEC.md §3): a declaration whose
    /// current row already says exactly this writes nothing — and moves
    /// no pin, which is what keeps an idempotent re-declare from staling
    /// every measurement in the workspace.
    async fn put_unless_current(&self, name: &str, cells: Vec<Option<String>>) -> Result<()> {
        if self.lake_rows(relation(name)).await?.contains(&cells) {
            return Ok(());
        }
        self.put(name, cells).await
    }

    /// One appended row into a crossed relation — the only place a
    /// store relation moves, and therefore the only place the head has
    /// to be dropped.
    ///
    /// The drop follows the commit, never precedes it: dropped first, a
    /// concurrent reader would re-walk the old snapshots and cache them
    /// as current, and this write would land with nothing left to
    /// invalidate. That ordering is the whole correctness argument, and
    /// it holds because `append` returns only once the Iceberg commit
    /// is in. **If appends are ever batched, the flush becomes this
    /// place** — issuing a write is not what moves the head, landing it
    /// is.
    async fn put(&self, relation: &str, cells: Vec<Option<String>>) -> Result<()> {
        let landed = self.metadata.append(relation, vec![cells]).await;
        // Dropped on the way out whichever way the append went. A
        // failed append is not a write that did not happen: `append`
        // creates the relation before it fills it, so a failure between
        // the two leaves a table the head does not know about, and a
        // head that omits a relation is wrong in the direction that
        // hides rows.
        *self.head.write().expect("head lock") = None;
        landed.map_err(Error::from)?;
        Ok(())
    }

    /// A crossed relation's current rows: latest per [`Relation`] key in
    /// `(seq, pos)` order, sorted by cells — what the sqlite primary key
    /// and `ORDER BY` used to do.
    async fn lake_rows(&self, relation: &Relation) -> Result<Vec<Vec<Option<String>>>> {
        let history = self.metadata.scan(relation.name).await?;
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
        self.lake_rows(relation).await
    }

    pub async fn dataset_exists(&self, name: &str) -> Result<bool> {
        if name == STORE_NAMESPACE {
            return Ok(false);
        }
        // One existence query, not the whole workspace. `namespaces()`
        // lists every namespace and then reads the properties of each to
        // answer a question that never looks at them.
        Ok(self.lake.namespace_exists(name).await?)
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
                    Some(
                        props
                            .get(SETTINGS_PROP)
                            .cloned()
                            .unwrap_or_else(|| "{}".into()),
                    ),
                ]
            })
            .collect();
        rows.sort();
        Ok(rows)
    }

    async fn import_rows(&self) -> Result<Vec<Vec<Option<String>>>> {
        let mut rows = Vec::new();
        for (dataset, _) in self.lake.namespaces().await? {
            if dataset == STORE_NAMESPACE {
                continue;
            }
            for l in self.lake.landings(&dataset).await? {
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

    /// The statement's read context: the store resolved once. The
    /// session supplies what the lake knows (subjects and snapshots);
    /// this adds every relation's rows and the pin over the whole set.
    ///
    /// The six relation reads are the expensive half — one Iceberg scan
    /// each, and each scan opens one small file per row ever written
    /// there, so the cost tracks the workspace's write history rather
    /// than its data. They are held per dataset under the version they
    /// were read at, so a read whose version still stands pays none of
    /// it. What the lake knows is *not* cached: `universe`, `snapshots`
    /// and the pin they feed are rebuilt every read, so a landing is
    /// visible the moment it commits.
    pub async fn read_context(
        &self,
        dataset: &str,
        universe: Vec<String>,
        snapshots: std::collections::HashMap<String, i64>,
    ) -> Result<ReadContext> {
        let pin = self.pin(dataset, &snapshots).await?;
        let version = self.version().await?;
        let cached = self
            .contexts
            .read()
            .expect("contexts lock")
            .get(dataset)
            .filter(|held| held.version == version)
            .cloned();
        // The six are behind `Arc`s, so what is cloned here is six
        // pointers and the lake's own two fields.
        if let Some(mut ctx) = cached {
            ctx.universe = universe;
            ctx.snapshots = snapshots;
            ctx.pin = pin;
            return Ok(ctx);
        }
        let ctx = ReadContext {
            glossary: std::sync::Arc::new(self.glossary_history().await?),
            measurements: std::sync::Arc::new(self.measurements_newest(dataset).await?),
            functions: std::sync::Arc::new(self.functions_all().await?),
            witnesses: std::sync::Arc::new(self.witnesses_all().await?),
            sources: std::sync::Arc::new(self.sources_all().await?),
            aspects: std::sync::Arc::new(self.aspects_all().await?),
            universe,
            snapshots,
            pin,
            version,
        };
        self.contexts
            .write()
            .expect("contexts lock")
            .insert(dataset.to_string(), ctx.clone());
        Ok(ctx)
    }

    /// The store's version: every table currently in the store namespace
    /// at its snapshot, sorted and joined — the key a cached
    /// [`ReadContext`] is held under. Enumerated from the catalog, never
    /// curated, so any store write moves it.
    pub async fn version(&self) -> Result<String> {
        let mut parts: Vec<String> = self
            .store_snapshots()
            .await?
            .iter()
            .map(|(t, snap)| format!("{t}:{}", snap.map_or_else(|| "-".into(), |s| s.to_string())))
            .collect();
        parts.sort();
        Ok(parts.join(","))
    }

    /// Every store-namespace table at its snapshot — the one catalog
    /// walk both the version and the pin derive from, enumerated rather
    /// than curated so a relation added later can never be missed. A
    /// fresh workspace has no store namespace until the first write
    /// crosses; its enumeration is empty, not an error.
    ///
    /// Served from [`Store::head`] once walked. Two readers racing an
    /// empty head both walk and both store the same answer, which is
    /// why the walk holds no lock: a duplicate catalog round trip is
    /// cheaper than an async-aware lock, and the results agree.
    async fn store_snapshots(&self) -> Result<Snapshots> {
        if let Some(head) = self.head.read().expect("head lock").clone() {
            return Ok(head);
        }
        let mut out = Vec::new();
        if self.lake.namespace_exists(STORE_NAMESPACE).await? {
            for table in self.lake.table_names(STORE_NAMESPACE).await? {
                let snap = self.lake.snapshot_id(STORE_NAMESPACE, &table).await?;
                out.push((table, snap));
            }
        }
        let head = Arc::new(out);
        *self.head.write().expect("head lock") = Some(Arc::clone(&head));
        Ok(head)
    }

    // -- the pin, and the measurements it keys -------------------------

    /// The statement's pin over `dataset`, from the data snapshots the
    /// session resolved plus the declaration relations' own.
    pub async fn pin(
        &self,
        dataset: &str,
        data: &std::collections::HashMap<String, i64>,
    ) -> Result<Pin> {
        let mut parts: Vec<String> = data
            .iter()
            .map(|(table, snap)| format!("{dataset}.{table}:{snap}"))
            .collect();
        // The semantic inputs: every store relation except
        // `measurements` — an output, not an input — from the same
        // enumeration the version reads, so a relation added later
        // joins the pin on its own.
        for (relation, snap) in self.store_snapshots().await?.iter() {
            if relation == "measurements" {
                continue;
            }
            parts.push(format!(
                "{STORE_NAMESPACE}.{relation}:{}",
                snap.map_or_else(|| "-".into(), |s| s.to_string())
            ));
        }
        Ok(Pin::new(parts))
    }

    /// The measurements at one pin — the digest pushed into the format's
    /// scan, so the drift record's history is never read to serve today.
    /// Every (function, subject)'s newest landing in the dataset,
    /// whatever its pin — what a read context serves from. One scan of
    /// the relation by dataset; older rows stay as the drift record.
    async fn measurements_newest(&self, dataset: &str) -> Result<Vec<glossql_catalog::Row>> {
        let rows = self
            .metadata
            .scan_where("measurements", "dataset", dataset)
            .await?;
        Ok(rules::latest_by(
            rows,
            |r| (r.get(1).map(str::to_string), r.get(2).map(str::to_string)),
            |r| r.seq,
        ))
    }

    /// The measurement at the context's pin, newest write winning — two
    /// computations at one pin produced the same value. Pure over the
    /// context: the statement already read the relation.
    pub fn measurement_in(
        ctx: &ReadContext,
        dataset: &str,
        subject: &str,
        function: &str,
    ) -> Option<MeasurementRow> {
        ctx.measurements
            .iter()
            .filter(|r| {
                r.get(0) == Some(dataset)
                    && r.get(1) == Some(function)
                    && r.get(2) == Some(subject)
                    && r.get(5) == Some(ctx.pin.text.as_str())
            })
            .max_by_key(|r| r.seq)
            .map(|r| MeasurementRow {
                subject: subject.to_string(),
                function: function.to_string(),
                body: text(&r.cells, 6),
                computed_at: text(&r.cells, 7),
            })
    }

    /// Every subject's newest measurement by `function` — the context's
    /// rows, each marked `current` when it stands at the context's pin.
    /// Pure over the context: the statement already read the relation.
    pub fn measurements_in(
        ctx: &ReadContext,
        dataset: &str,
        function: &str,
    ) -> Vec<(MeasurementRow, bool)> {
        ctx.measurements
            .iter()
            .filter(|r| r.get(0) == Some(dataset) && r.get(1) == Some(function))
            .map(|r| {
                (
                    MeasurementRow {
                        subject: text(&r.cells, 2),
                        function: function.to_string(),
                        body: text(&r.cells, 6),
                        computed_at: text(&r.cells, 7),
                    },
                    r.get(5) == Some(ctx.pin.text.as_str()),
                )
            })
            .collect()
    }

    /// Land one measurement; the served row comes back so the caller
    /// need not re-read what it just wrote.
    pub async fn measurement_put(
        &self,
        dataset: &str,
        function: &str,
        subject: &str,
        aspect: &str,
        pin: &Pin,
        value: &str,
    ) -> Result<MeasurementRow> {
        let computed_at = now_utc();
        self.put(
            "measurements",
            vec![
                Some(dataset.to_string()),
                Some(function.to_string()),
                Some(subject.to_string()),
                Some(aspect.to_string()),
                Some(pin.digest.clone()),
                Some(pin.text.clone()),
                Some(value.to_string()),
                Some(computed_at.clone()),
            ],
        )
        .await?;
        Ok(MeasurementRow {
            subject: subject.to_string(),
            function: function.to_string(),
            body: value.to_string(),
            computed_at,
        })
    }

    /// The glossary, read whole: hundreds of rows, filtered by rules.
    async fn glossary_history(&self) -> Result<Vec<GlossRow>> {
        Ok(self
            .metadata
            .scan("glossary")
            .await?
            .into_iter()
            .map(|r| GlossRow {
                dataset: text(&r.cells, 0),
                subject: text(&r.cells, 1),
                aspect: text(&r.cells, 2),
                actor_kind: text(&r.cells, 3),
                actor_id: text(&r.cells, 4),
                body: text(&r.cells, 5),
                written_at: text(&r.cells, 6),
                snapshot_id: cell(&r.cells, 7).and_then(|s| s.parse().ok()),
                seq: r.seq,
            })
            .collect())
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
            .map(aspect_row)
            .collect())
    }

    async fn functions_all(&self) -> Result<Vec<FunctionRow>> {
        Ok(self
            .lake_rows(relation("functions"))
            .await?
            .iter()
            .map(function_row)
            .collect())
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
        let Some(a) = self
            .aspects_all()
            .await?
            .into_iter()
            .find(|a| a.name == name)
        else {
            return Ok(None);
        };
        let schema = serde_json::from_str(&a.schema)
            .map_err(|e| Error::Corrupt(format!("aspect `{name}` schema: {e}")))?;
        Ok(Some((schema, a.kind, a.grains)))
    }

    /// Resolve a function visible from `dataset` (`FOR` scope or GLOBAL,
    /// SPEC.md §6). `None` skips the visibility check.
    pub async fn function(&self, name: &str, dataset: Option<&str>) -> Result<Option<FunctionRow>> {
        let Some(f) = self
            .functions_all()
            .await?
            .into_iter()
            .find(|f| f.name == name)
        else {
            return Ok(None);
        };
        match (dataset, &f.scope_dataset) {
            (Some(d), Some(scope)) if scope != d => Ok(None),
            _ => Ok(Some(f)),
        }
    }

    async fn witnesses_on(&self, aspect: &str) -> Result<Vec<WitnessRow>> {
        Ok(self
            .witnesses_all()
            .await?
            .into_iter()
            .filter(|w| w.aspect == aspect)
            .collect())
    }
}

fn now_utc() -> String {
    chrono::Utc::now()
        .format("%Y-%m-%dT%H:%M:%S%.3fZ")
        .to_string()
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

/// The latest row per key, over the rows the key function admits — the
/// supersession shape the brief counts share.
fn latest_rows<'a, K: std::hash::Hash + Eq>(
    history: &'a [GlossRow],
    key: impl Fn(&GlossRow) -> Option<K>,
) -> Vec<&'a GlossRow> {
    rules::latest_by(
        history.iter().filter(|g| key(g).is_some()).collect(),
        |g| key(g).expect("filtered"),
        |g| g.seq,
    )
}

fn aspect_row(cells: &Vec<Option<String>>) -> AspectRow {
    AspectRow {
        name: text(cells, 0),
        kind: text(cells, 1),
        grains: cell(cells, 2),
        condition: cell(cells, 3).as_deref().and_then(parse_condition),
        schema: text(cells, 4),
    }
}

fn condition_text(condition: Option<&(String, String)>) -> Option<String> {
    condition.map(|(aspect, value)| format!("{aspect} = '{value}'"))
}

fn parse_condition(text: &str) -> Option<(String, String)> {
    let (aspect, rest) = text.split_once(" = '")?;
    Some((aspect.to_string(), rest.strip_suffix('\'')?.to_string()))
}

fn function_row(cells: &Vec<Option<String>>) -> FunctionRow {
    FunctionRow {
        name: text(cells, 0),
        scope_dataset: cell(cells, 1).filter(|s| s != "GLOBAL"),
        script: text(cells, 2),
        returns: cell(cells, 3),
    }
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
