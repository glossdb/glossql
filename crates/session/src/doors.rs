//! The doors over the record: the reads the planner answers by name,
//! each evaluated ahead of planning and served as a batch the planner
//! mounts. The registry ([`DOORS`]) is what serverd's function listing
//! and the window's vocabulary read; the dispatch ([`compute_batch`])
//! answers every name on it, and the two class readers beside it —
//! [`reads_the_record`], [`door_reads`] — decide a door's frame class
//! and its legs in this same file, so a new arm decides all three here.

use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::sql::planner::IdentNormalizer;
use datafusion::sql::sqlparser::ast::TableFactor;

use crate::reads::{attest_read, batch, glossary_read, utf8};
use crate::shared::{Held, ReadSet, Resolved, Served, SessionError, Shared, integer};

/// The reads the planner answers by name — the doors over the record
/// that no SQL file carries — each with the call that reaches it. The
/// dispatch below answers every name here. The door's function listing
/// and the window's vocabulary read this list, so neither can drift
/// from the dispatch, and the suite plans each call.
pub const DOORS: &[(&str, &str)] = &[
    ("glossary", "GLOSSARY(<subject>[, all => true])"),
    ("attest", "ATTEST(<dataset>)"),
    ("metric_axes", "metric_axes()"),
    (
        "metric_series",
        "metric_series([grain => '<grain>'][, world => '<world>'][, scenario => '<name>'][, plan => '<name>'])",
    ),
    ("fact_values", "fact_values()"),
    ("band_points", "band_points()"),
    ("forecast_points", "forecast_points()"),
    ("forecast_coverage", "forecast_coverage()"),
    ("scenario_points", "scenario_points(scenario => '<name>')"),
    ("scenario_levers", "scenario_levers(scenario => '<name>')"),
    ("plan_points", "plan_points()"),
    ("metric_sources", "metric_sources()"),
    ("metric_band_walk", "metric_band_walk('<dataset>')"),
    (
        "metric_forecast_ahead",
        "metric_forecast_ahead('<dataset>')",
    ),
    ("grounding_collisions", "grounding_collisions('<dataset>')"),
    ("behavior_anchors", "behavior_anchors('<subject>')"),
    ("relationship_checks", "relationship_checks('<dataset>')"),
    (
        "relationship_candidates",
        "relationship_candidates('<dataset>')",
    ),
    ("hierarchy_candidates", "hierarchy_candidates('<table>')"),
    ("derivation_candidates", "derivation_candidates('<table>')"),
    ("source_files", "source_files('<source>')"),
    ("pages", "pages()"),
    ("current_dataset", "current_dataset"),
];

impl Shared {
    /// The pins of a statement whose every relation is a plain table
    /// of the bound dataset: those tables, loaded by name, and nothing
    /// else of the dataset. `None` for any other statement — one that
    /// names another dataset's table, a read, a door, a store relation,
    /// or a name the dataset does not hold — and the caller takes
    /// [`Shared::statement_pins`], the whole walk, which is what those
    /// read and where a misspelt name finds its hint.
    pub(crate) async fn named_pins(
        &self,
        relations: &[datafusion::common::TableReference],
    ) -> Result<
        Option<std::collections::HashMap<String, Arc<dyn datafusion::catalog::TableProvider>>>,
        SessionError,
    > {
        use datafusion::common::TableReference;
        let Some(dataset) = self.dataset() else {
            return Ok(None);
        };
        let mut names = Vec::with_capacity(relations.len());
        for relation in relations {
            match relation {
                TableReference::Bare { table } => names.push(table.to_string()),
                TableReference::Partial { schema, table } if schema.as_ref() == dataset => {
                    names.push(table.to_string());
                }
                _ => return Ok(None),
            }
        }
        // A name this server claims is never asked of the catalog: it
        // would answer absent, one round trip later.
        let claimed = |name: &str| {
            glossql_glossary::RELATIONS.iter().any(|r| r.name == name)
                || crate::library::LIBRARY.iter().any(|(n, _)| *n == name)
                || DOORS.iter().any(|(n, _)| *n == name)
        };
        if names.is_empty() || names.iter().any(|n| claimed(n)) {
            return Ok(None);
        }
        names.sort();
        names.dedup();
        // The statement's walk, when it stands, holds every table of the
        // dataset: a name it lacks is no table, and the caller wants the
        // walk's hint for it.
        let walk = self.walk(&dataset);
        if let Some(walk) = walk {
            let mut out = std::collections::HashMap::with_capacity(names.len());
            for name in &names {
                match walk.iter().find(|p| &p.name == name) {
                    Some(p) => {
                        out.insert(name.clone(), Arc::clone(&p.provider));
                    }
                    None => return Ok(None),
                }
            }
            return Ok(Some(out));
        }
        let held = self.named.held().get(&dataset).cloned().unwrap_or_default();
        let mut out: std::collections::HashMap<_, _> = names
            .iter()
            .filter_map(|n| held.get(n).map(|p| (n.clone(), Arc::clone(p))))
            .collect();
        let missing: Vec<String> = names
            .iter()
            .filter(|n| !held.contains_key(*n))
            .cloned()
            .collect();
        if !missing.is_empty() {
            let Some(loaded) = self.lake().pin_tables(&dataset, &missing).await? else {
                return Ok(None);
            };
            let mut named = self.named.held_mut();
            let slot = named.entry(dataset).or_default();
            for p in loaded {
                slot.insert(p.name.clone(), Arc::clone(&p.provider));
                out.insert(p.name, p.provider);
            }
        }
        Ok(Some(out))
    }
}

/// Whether a factor [`compute_batch`] resolves serves the record —
/// the glossary directly (`glossary`, `GLOSSARY()`, `ATTEST()`) or
/// what collapsed slots say (the walk reads groundings, collisions and
/// anchors read claims, `metric_axes()` says what the cube admitted).
/// The pre-pass derives the statement's frame class from this
/// (`record`/`data`): a `record` answer can change under a glossary
/// write, a `data` answer cannot. The class is about what a door
/// SERVES, not what its build consumed: `metric_series()` enumerates
/// groundings to find its cells, but what it serves is the data at a
/// grain, so it stays out. A new arm in the match below decides its
/// class here, in the same file.
pub(crate) fn reads_the_record(idents: &IdentNormalizer, f: &TableFactor) -> bool {
    let TableFactor::Table { name, .. } = f else {
        return false;
    };
    let [part] = name.0.as_slice() else {
        return false;
    };
    part.as_ident().is_some_and(|i| {
        matches!(
            idents.normalize(i.clone()).as_str(),
            "glossary"
                | "attest"
                | "grounding_collisions"
                | "metric_band_walk"
                | "band_points"
                | "metric_forecast_ahead"
                | "forecast_points"
                | "forecast_coverage"
                | "scenario_points"
                | "scenario_levers"
                | "plan_points"
                | "metric_axes"
                | "metric_sources"
                | "behavior_anchors"
        )
    })
}

/// What a compute door READS — the legs of the measurement record
/// ([`ReadSet`], the currency rule). Beside
/// [`reads_the_record`] on purpose: a new arm in [`compute_batch`]
/// decides its class here, in the same file. Only called for factors
/// that resolved as compute doors; anything unlisted falls to
/// `everything`, the conservative floor — a record never narrower
/// than what was read.
pub(crate) fn door_reads(idents: &IdentNormalizer, f: &TableFactor, reads: &mut ReadSet) {
    let TableFactor::Table { name, args, .. } = f else {
        return;
    };
    let [part] = name.0.as_slice() else {
        // The `misfit.` door: a frame replays a grounding — not
        // enumerable from here.
        reads.everything = true;
        return;
    };
    let Some(door) = part.as_ident().map(|i| idents.normalize(i.clone())) else {
        return;
    };
    let arg = args.as_ref().and_then(single_string_arg);
    match (door.as_str(), args.is_some()) {
        // One table, the argument's own: candidate enumeration over
        // its columns.
        ("derivation_candidates" | "hierarchy_candidates", _) => {
            reads.tables.extend(arg);
        }
        // The whole dataset's tables; the anchor and check doors read
        // the declared edges beside them.
        ("relationship_candidates", _) => reads.all_tables = true,
        ("behavior_anchors" | "relationship_checks", _) => {
            reads.all_tables = true;
            reads.relations.insert("relationships".into());
        }
        // The slot walkers: the collapsed groundings run over the
        // landed data. The groundings are one leg of their own —
        // `glossql.grounding`, the digest of every QUERY writing, the
        // QUERY aspects and their witnesses — so a definitions entry,
        // an app page or a declared check moves none of these.
        (
            "grounding_collisions"
            | "metric_band_walk"
            | "metric_forecast_ahead"
            | "metric_sources",
            _,
        ) => {
            reads.all_tables = true;
            reads.relations.insert("grounding".into());
        }
        // The binding, not state: no pin leg moves it. The door's
        // pages are the binary's: nothing in the workspace moves them.
        ("current_dataset" | "pages", _) => {}
        // A store relation read as a plain table — bare names only:
        // `glossary` the relation is one leg, `GLOSSARY(subject)` the
        // door is a collapse over half the store and falls through.
        (name, false) if glossql_glossary::relation_columns(name).is_some() => {
            reads.relations.insert(name.to_string());
        }
        // GLOSSARY(), ATTEST(), the cube's reads, and anything a later
        // arm adds without a row here.
        _ => reads.everything = true,
    }
}

/// The one quoted string a door takes — `door('<what>')` — or the
/// refusal that spells the call.
fn one_quoted(
    a: &datafusion::sql::sqlparser::ast::TableFunctionArgs,
    door: &str,
    what: &str,
    example: &str,
) -> Result<String, SessionError> {
    single_string_arg(a).ok_or_else(|| {
        SessionError::shape(
            door,
            format!("{door} takes one quoted {what}: {door}('{example}')"),
        )
    })
}

/// A door that takes no arguments, with where its narrowing rides.
fn no_arguments(
    a: &datafusion::sql::sqlparser::ast::TableFunctionArgs,
    door: &str,
    road: &str,
) -> Result<(), SessionError> {
    if a.args.is_empty() {
        Ok(())
    } else {
        Err(SessionError::shape(
            door,
            format!("{door}() takes no arguments — {road}"),
        ))
    }
}

/// The one quoted string argument of a door, if that is what the args
/// hold.
fn single_string_arg(a: &datafusion::sql::sqlparser::ast::TableFunctionArgs) -> Option<String> {
    use datafusion::sql::sqlparser::ast::{Expr, FunctionArg, FunctionArgExpr, Value as SqlValue};
    if let [FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(v)))] = a.args.as_slice()
        && let SqlValue::SingleQuotedString(s) = &v.value
    {
        return Some(s.clone());
    }
    None
}

/// Evaluate one compute door ahead of planning — the async half behind
/// the planner's lookup. `None` for anything that is not a compute door.
/// `resolved` carries the statement's pins, which is how a search door
/// reads the same snapshot every other scan in the statement reads.
pub(crate) async fn compute_batch(
    shared: &Arc<Shared>,
    factor: &TableFactor,
    resolved: &Resolved,
) -> Result<Option<Served>, SessionError> {
    let TableFactor::Table { name, args, .. } = factor else {
        return Ok(None);
    };
    if let [prefix, door] = name.0.as_slice()
        && let (Some(prefix), Some(door)) = (prefix.as_ident(), door.as_ident())
    {
        if shared.idents().normalize(prefix.clone()) == "misfit" {
            let Some(a) = args else { return Ok(None) };
            if !a.args.is_empty() {
                return Err(SessionError::shape(
                    format!("misfit.{}", door.value),
                    format!(
                        "misfit.{}() takes no arguments — the frame is the aspect's \
                     grounding (fixture 20)",
                        door.value
                    ),
                ));
            }
            return Ok(Some(
                (Box::pin(crate::misfit::misfit_batch(shared, &door.value)).await?).into(),
            ));
        }
        return Ok(None);
    }
    let [part] = name.0.as_slice() else {
        return Ok(None);
    };
    let Some(fname) = part
        .as_ident()
        .map(|i| shared.idents().normalize(i.clone()))
    else {
        return Ok(None);
    };
    match (fname.as_str(), args) {
        // The search doors: candidate enumeration over a table's own
        // columns, computed here because a static SQL body cannot spell
        // a schema it does not know.
        ("derivation_candidates", Some(a)) => {
            let table = one_quoted(a, "derivation_candidates", "table", "table")?;
            Ok(Some(
                (crate::search::derivation_candidates(shared, resolved, &table).await?).into(),
            ))
        }
        ("behavior_anchors", Some(a)) => {
            let subject = one_quoted(a, "behavior_anchors", "subject", "table.column")?;
            Ok(Some(
                (crate::behavior::behavior_anchors(shared, resolved, &subject).await?).into(),
            ))
        }
        ("metric_band_walk", Some(a)) => {
            let dataset = one_quoted(a, "metric_band_walk", "dataset", "dataset")?;
            Ok(Some(
                (crate::search::metric_band_walk(shared, &dataset).await?).into(),
            ))
        }
        // The files under a source's location, through the engine's
        // object store — what a recipe can name, so discovering them
        // needs no door but this one.
        ("source_files", Some(a)) => {
            let source = single_string_arg(a).ok_or_else(|| {
                SessionError::shape(
                    "source_files",
                    "source_files takes one quoted source: source_files('erp')".to_string(),
                )
            })?;
            let settings =
                shared
                    .store
                    .source_settings(&source)
                    .await?
                    .ok_or(SessionError::Store(glossql_glossary::Error::Unknown {
                        what: "source",
                        name: source.clone(),
                    }))?;
            let spec = glossql_import::SourceSpec::from_settings(&source, &settings)?;
            let span =
                tracing::info_span!("listing", source = %source, files = tracing::field::Empty);
            let files =
                tracing::Instrument::instrument(glossql_import::list_source(&spec), span.clone())
                    .await?;
            span.record("files", files.len());
            Ok(Some(source_files_batch(files).into()))
        }
        // The groundings the cube does not chart — facts and relations
        // — served whole, the value where the frame is one row.
        ("fact_values", Some(a)) => {
            no_arguments(a, "fact_values", "filters ride WHERE")?;
            Ok(Some((crate::search::fact_values(shared).await?).into()))
        }
        // What feeds each grounding: the served fields and the table
        // columns they descend from — the cube's provenance walk as
        // rows, for a page that draws lineage.
        ("metric_sources", Some(a)) => {
            no_arguments(a, "metric_sources", "filters ride WHERE")?;
            Ok(Some((crate::search::metric_sources(shared).await?).into()))
        }
        // The recorded walk, flattened: what `metric_bands` landed for
        // the bound dataset, one row per metric per point.
        ("band_points", Some(a)) => {
            no_arguments(a, "band_points", "filters ride WHERE")?;
            Ok(Some((crate::search::band_points(shared).await?).into()))
        }
        // The forecast at an origin: the door behind `metric_forecast`,
        // and the two reads over what it landed.
        ("metric_forecast_ahead", Some(a)) => {
            let dataset = one_quoted(a, "metric_forecast_ahead", "dataset", "dataset")?;
            Ok(Some(
                (crate::search::metric_forecast_ahead(shared, &dataset).await?).into(),
            ))
        }
        ("forecast_points", Some(a)) => {
            no_arguments(a, "forecast_points", "filters ride WHERE")?;
            Ok(Some((crate::search::forecast_points(shared).await?).into()))
        }
        ("forecast_coverage", Some(a)) => {
            no_arguments(a, "forecast_coverage", "filters ride WHERE")?;
            Ok(Some(
                (crate::search::forecast_coverage(shared).await?).into(),
            ))
        }
        // The scenario world cell by cell (crate::scenario), the
        // scenario named.
        ("scenario_points", Some(a)) => {
            let name = crate::scenario::name_arg("scenario_points", &a.args)?;
            Ok(Some(
                (Box::pin(crate::scenario::scenario_points(shared, &name)).await?).into(),
            ))
        }
        // Each lever's part of the estimate, the scenario named.
        ("scenario_levers", Some(a)) => {
            let name = crate::scenario::name_arg("scenario_levers", &a.args)?;
            Ok(Some(
                (Box::pin(crate::scenario::scenario_levers(shared, &name)).await?).into(),
            ))
        }
        // Every plan against the actual and the forecast
        // (crate::plan_world, #117).
        ("plan_points", Some(a)) => {
            no_arguments(a, "plan_points", "filters ride WHERE")?;
            Ok(Some((crate::plan_world::plan_points(shared).await?).into()))
        }
        ("relationship_checks", Some(a)) => {
            let dataset = one_quoted(a, "relationship_checks", "dataset", "dataset")?;
            Ok(Some(
                (crate::search::relationship_checks(shared, &dataset).await?).into(),
            ))
        }
        ("grounding_collisions", Some(a)) => {
            let dataset = one_quoted(a, "grounding_collisions", "dataset", "dataset")?;
            Ok(Some(
                (crate::search::grounding_collisions(shared, &dataset).await?).into(),
            ))
        }
        ("relationship_candidates", Some(a)) => {
            let dataset = one_quoted(a, "relationship_candidates", "dataset", "dataset")?;
            Ok(Some(
                (crate::search::relationship_candidates(shared, &dataset).await?).into(),
            ))
        }
        ("hierarchy_candidates", Some(a)) => {
            let table = one_quoted(a, "hierarchy_candidates", "table", "table")?;
            Ok(Some(
                (crate::search::hierarchy_candidates(shared, resolved, &table).await?).into(),
            ))
        }
        ("glossary", Some(a)) => Ok(Some((glossary_read(shared, &a.args).await?).into())),
        ("attest", Some(a)) => Ok(Some((attest_read(shared, &a.args).await?).into())),
        // The cube's two reads (crate::cube): the cells at a grain in a
        // world, and the fact row per metric. Both build what is not
        // built.
        ("metric_series", Some(a)) => {
            let (grain, world, name) = crate::cube::series_args(&a.args)?;
            Ok(Some(
                crate::cube::metric_series_batch(shared, grain, world, name.as_deref()).await?,
            ))
        }
        ("metric_axes", Some(a)) => {
            no_arguments(a, "metric_axes", "filters ride WHERE")?;
            Ok(Some((crate::cube::metric_axes_batch(shared).await?).into()))
        }
        // The `USE`'d dataset, as a relation — the one thing a read
        // written in SQL cannot spell for itself, and the reason the
        // reads over the workspace-wide relations had no way to narrow
        // to the session they answer for.
        ("current_dataset", None) => Ok(Some((current_dataset_batch(shared)).into())),
        // The pages the door serves, as rows — the same pages that are
        // its MCP resources, for a client that has the statement tool
        // and nothing else.
        ("pages", Some(a)) => {
            no_arguments(a, "pages", "`WHERE uri = '…'` reads one")?;
            Ok(Some(pages_batch(shared).into()))
        }
        // The store's relations, readable as plain tables. Which names
        // qualify lives in one place: the store's RELATIONS table.
        (name, None) if glossql_glossary::relation_columns(name).is_some() => {
            let rows = shared.store.relation_rows(&fname).await?;
            Ok(Some((relation_batch(&fname, rows)).into()))
        }
        _ => Ok(None),
    }
}

/// `source_files('source')` — one row per file under the source's
/// location: `path` (relative, what a `read_*` call names), `size`,
/// `modified`.
fn source_files_batch(files: Vec<glossql_import::SourceFile>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        utf8("path"),
        Field::new("size", DataType::Int64, false),
        utf8("modified"),
    ]));
    batch(
        schema,
        vec![
            Arc::new(StringArray::from_iter_values(
                files.iter().map(|f| f.path.as_str()),
            )),
            Arc::new(datafusion::arrow::array::Int64Array::from(
                files.iter().map(|f| integer(f.size)).collect::<Vec<i64>>(),
            )),
            Arc::new(StringArray::from_iter_values(
                files.iter().map(|f| f.modified.as_str()),
            )),
        ],
    )
}

/// `current_dataset` — the bound dataset as a one-row relation, or no
/// rows at all when nothing is bound.
///
/// Zero rows rather than a NULL or a refusal, and that is the whole
/// design: a read scopes itself by joining this, so an unbound session
/// answers nothing instead of answering for every dataset in the
/// workspace. `GLOSSARY()` refuses an unbound session outright
/// ([`SessionError::NoDataset`]) because a sweep with no scope is a
/// caller's mistake; a read that joins for its scope needs an empty
/// relation, not an error, or every read building on it would have to
/// handle the refusal.
///
/// It is session state, not the record: a glossary write cannot change
/// it, only `USE` can, which is why it is absent from
/// [`reads_the_record`].
fn current_dataset_batch(shared: &Shared) -> RecordBatch {
    let bound = shared.dataset();
    batch(
        Arc::new(Schema::new(vec![utf8("dataset")])),
        vec![Arc::new(StringArray::from_iter(bound.iter().map(Some))) as ArrayRef],
    )
}

/// `pages()` — every page the door serves: `uri`, `title`, `body`.
fn pages_batch(shared: &Shared) -> RecordBatch {
    let pages = Arc::clone(&shared.pages.held());
    batch(
        Arc::new(Schema::new(vec![utf8("uri"), utf8("title"), utf8("body")])),
        vec![
            Arc::new(StringArray::from_iter_values(
                pages.iter().map(|p| p.uri.as_str()),
            )) as ArrayRef,
            Arc::new(StringArray::from_iter_values(
                pages.iter().map(|p| p.title.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                pages.iter().map(|p| p.body.as_str()),
            )),
        ],
    )
}

fn relation_batch(table: &str, rows: Vec<Vec<Option<String>>>) -> RecordBatch {
    // The store's RELATIONS table is the one home of each shape; the
    // planner only routes names that table knows.
    let names = glossql_glossary::relation_columns(table).expect("planner routed a known relation");
    let schema = Arc::new(Schema::new(
        names.iter().map(|n| utf8(n)).collect::<Vec<_>>(),
    ));
    let columns = (0..names.len())
        .map(|i| Arc::new(StringArray::from_iter(rows.iter().map(|r| r[i].as_deref()))) as ArrayRef)
        .collect();
    batch(schema, columns)
}
