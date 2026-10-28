# The kits plan

Two things. A demo of harmonisation: four systems of record, one
product and one site record across them, checks on every load,
tables shaped for a planning tool, every decision on record; its
corpus, basis and app live in the demo repo beside this one, and the
docket changes it needs are rows here. And configuration out of the
Rust code: the KPI setup as the open reference, a harmonisation setup
layered on it at image build time.

## The table

| Element | Risk | What it touches | When | Issue |
|---|---|---|---|---|
| Two goals in the agent's step list, map and serve, ahead of metrics | Low | `crates/session/reads/next.sql` | A feature branch beside the demo; one day | |
| A line on every result naming the docket page to open | Low | `crates/serverd/src/window.rs` | If time allows; half a day | |
| The docket's metrics tab lists views apart from the metrics | Low | `crates/apps/builtin/docket/frames/pulse.sql`, `metrics.html` | With the demo; half a day | #175 |
| The quality tab's checks list collapses per witness | Low | `frames/checks.sql`, `quality.html` | With the demo; half a day | #176 |
| The corridor tile names the metric and the month behind a red | Low | `frames/front.sql`, `index.html` | With the demo; half a day | #177 |
| The Explore tab: tables with columns and meanings, the source conventions, a query pane | Medium: one new element over the query door | `crates/apps/builtin/docket/`, `crates/apps/assets` | With the demo; two days | #178 |
| The docket's tabs as two groups: Questions, Metrics, Quality, Glossary, Lineage; then Export, Query | Low | `crates/apps/builtin/docket/app.toml`, `crates/apps/templates/shell.html`, `crates/apps/src/app.rs` | With the demo; one day | #179 |
| The quality tab's checks open to their evidence: the corridor's points, a voice check's differing columns | Medium: two evidence shapes behind one page param | `frames/checks.sql`, `quality.html`, new frames over `band_points()` and `glossary` | With the demo; two days | #180 |
| A view's page shows what it serves and its rows through the pane; the pane takes its SQL from the page | Low | `metrics.html`, `crates/apps/assets/gl-query.js` | With the demo; one day | #181 |
| A stopped surface folds under a closed line on Metrics and Export | Low | `frames/pulse.sql`, `views.sql`, `facts.sql`, `reads.sql` | With the demo; half a day | #182 |
| Kits out of the Rust code: the walk, the arrays, the step list split, the docket move | Medium: a build script over five lists | `crates/scripts/src/library.rs`, `crates/session/src/library.rs`, `crates/apps/src/builtin.rs`, `crates/serverd/src/skills.rs`, `crates/serverd/build.rs` | After the demo; three to four days | |

## The demo, step by step, on today's binary

The story: a company plans in a supply planning tool. Its product
exists four times, in the ERP, warehouse, transport and production
systems, each with its own identifier form, unit and meaning of
"available"; planners reconcile by hand before every cycle. The
layer reads the extracts and writes nothing back, maps the
identifiers, puts the definitions to a named person, checks every
load, and serves tables in the planning tool's shape.

1. **Load.** Four extracts land by recipe, one table each, a snapshot
   date on every row. Every load is a row of `imports`: rows scanned,
   landed, dropped, and per column how many cells a cast could not
   read with the top unreadable values. Runs today.
2. **Map.** The identifier rules are columns of the recipe SQL: strip
   the zeros, take the number out of the reference with a regular
   expression, join the warehouse product table. A declared
   relationship from each table to the product table, and the orphan
   rate per edge from `relationship_coherence`. Unresolved
   identifiers by system are one anti-join, served as a view and a
   download. Runs today. No text matching anywhere, by rule.
3. **Units.** `detect_derivations` finds the identity hectolitres =
   cases × hectolitres per case and counts the rows that break it;
   the slice that reports pallets as cases is the violations' view.
   Runs today.
4. **Define.** "Available stock" means three things. The agent writes
   its reading as a keyed assumption; the docket shows the card; the
   owner confirms or corrects; the ruling is stored with name, time
   and the data version. Runs today.
5. **Service levels.** Each indicator is a metric (below): required
   fields filled, identifiers that resolve, rows that keep the
   conversion, each as good rows over rows, per snapshot date, per
   market and per target table. The metric page slices them; the
   bands flag a drift in an indicator and in on-hand stock. Runs
   today. Objectives are agreed on a baseline, so none is shown.
6. **Mitigate.** A date the warehouse extract writes in a second
   format nulls under the cast, and the required-fields indicator
   drops for that market. The agent proposes the recipe fix as a
   keyed assumption, the owner confirms it on the card, the agent
   re-declares the recipe, and the loads table shows the nulled count
   fall. Every recipe version stays on record, so the SQL before and
   after is a read. Runs today. An approval form of its own, stored
   breach records, and who may rule on what are pilot work, shaped
   with the customer.
7. **Export.** The planning tool's table is a derived relation
   (`x-kind: relation`) over the mapped tables; the export page
   downloads it as Parquet or CSV. One value traces to its source
   rows through the view's joins and to its rulings through
   `ruling_entries`. Runs today.

### Service levels as metrics

A validation gives one verdict per table against an authored
tolerance and no slices. A metric gets what a KPI gets: the grounding
is row-grain SQL over the target view, `num` one where the row is
good, `den` one, the time axis the snapshot date, the dimensions
market, plant and target table (a `UNION ALL` over the target views
puts the table name in a column); `metric_series()` serves the rate
per period and member, the trend draws it, the bands walk it, the
docket slices it. The costs: the extract's date on every row, and
market, plant and table marked as slice columns. Freshness rides `imports` against a cut-off
convention; import acceptance needs the target tool's rejection file
and waits for the pilot, as does the objective: a validation on the
metric's latest period, or a plan on the metric.

## Kits out of the Rust code

Everything that configures the server is text, listed by four Rust
arrays and one SQL file: the vocabulary, checks and measurement
functions (`crates/scripts/src/library.rs`), the shipped reads
(`crates/session/src/library.rs`), the step list (`next.sql`, goal
order a `CASE` at its tail), the built-in app
(`crates/apps/src/builtin.rs`), the skills
(`crates/serverd/src/skills.rs`); `crates/serverd/build.rs` already
walks the skills, docs and vendor trees for the reference pages.

- **One folder per kit**, `kits/<name>/`, with `vocabulary/`,
  `functions/`, `reads/`, `goals/`, `skills/`, `apps/`. `kits/core`
  holds the thirteen names Rust reads (`cube`, `temporal_profile`,
  `behavior_evidence`, `dimension_relevance`, `behavior`,
  `dimension`, `lever`, `ruling`, `recipe_change`, the app metadata
  names); a test asserts they exist. `kits/kpi` holds the rest of
  today's setup; `kits/harmonisation` adds to it.
- **The build script walks `kits/`** as it walks skills, docs and
  vendor, and generates the arrays. All kits ship in the image; an
  environment variable at start names the active kits in order.
- **Later overrides earlier, by file name.** No merge logic; an
  identical redeclaration is a no-op, a changed definition under
  existing data is refused (fixture 13).
- **The step list is split per goal**, one file of SQL arms per goal
  under `goals/`, numbered; the build concatenates the active kits'
  goal files with the generic tail. Apps and skills per kit go
  through the same walk; the docket moves to `kits/kpi/apps/docket/`.

## The harmonisation kit

`source_system` on a source; `conformed` on a relationship (mapped
to the shared product or site list); `crosswalk` on a table (a
mapping table, its keys, the coverage tolerance); `mapping_coverage`,
a check with its tolerance; `target_schema` on a dataset (the views
that feed the target tool). Goals `map` and `serve` ahead of
`metrics`. One skill: load a source system, map a pair, declare a
mapping table, serve a target view. One app page: sources by system,
coverage per mapping table, target views, indicators per table and
market.

## Decisions that are the lead's

The indicators as metrics in the demo; the two goals on a feature
branch beside it; whether to open the second kit after the demo, and
the names above.
