# The kits plan

Configuration out of the Rust code: the KPI setup as the open
reference, a harmonisation setup layered on it at image build time.
The harmonisation demo that drove it, its corpus, basis, run-book
and app, lives in the demo repo beside this one.

## The table

| Element | Risk | What it touches | When | Issue |
|---|---|---|---|---|
| Two goals in the agent's step list, map and serve, ahead of metrics | Low | `crates/session/reads/next.sql` | A feature branch; one day | |
| A line on every result naming the docket page to open | Low | `crates/serverd/src/window.rs` | If time allows; half a day | |
| Kits out of the Rust code: the walk, the arrays, the step list split, the docket move | Medium: a build script over five lists | `crates/scripts/src/library.rs`, `crates/session/src/library.rs`, `crates/apps/src/builtin.rs`, `crates/serverd/src/skills.rs`, `crates/serverd/build.rs` | Three to four days | |

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

The two goals on a feature branch; whether to open the second kit,
and the names above.
