# glossql

A declarative context language over a SQL host, and a server that speaks
it. The language describes a dataset — sources, tables, relationships,
meanings, checks — so that agents and humans work on the same data with
the same context. Context is JSON validated by JSON Schemas; analytical
logic is scripts with JSON contracts; adjudication is witnesses and
detector functions, read back as bands, never written into data.

The full definition is **[SPEC.md](./SPEC.md)** — the single normative
document of this repository. `grammar.ebnf` is the machine source of
truth for syntax; the corpus (`crates/parser/tests/corpus/`) proves
every construct against real artifacts and runs as the parser's
acceptance suite.

The name: a *gloss* is a marginal annotation explaining a text's meaning;
a glossary is a collection of them. `GLOSS` is the write verb — you gloss
an aspect onto a subject; `GLOSSARY()` is the read.

## Why a grammar and JSON Schemas

Data work mixes two kinds of information that are usually tangled in
code: what you hold to be true *before* looking at the rows, and what
the rows themselves show. glossql keeps them apart and joins them at
read time.

- **A priori — the declared world model.** Aspects (each with a JSON
  Schema as its one validated contract), witnesses (who may speak to an
  aspect, and which detector adjudicates), relationships, sources, and
  recipes are all declarative statements. Because a world model is
  statements rather than code, an agent can author one as text, carry
  it, and apply it to whatever data lands — the vocabulary exists
  before the dataset does. A fresh workspace boots with one: the
  shipped measurement library and a semantic KPI kit.
- **A posteriori — the measured evidence.** Functions compute what the
  landed rows actually show: profiles, join evidence, stock/flow
  behavior, hierarchies, metric corridors. Measurements are tuned
  toward recall — they emit candidates with evidence, never
  conclusions — and the reading agent judges them against the data.
- **Glosses join the two.** A gloss writes a JSON value into a slot
  keyed (subject, aspect, actor kind) — function, agent, and human
  voices sit in separate slots and never overwrite each other. Reads
  collapse the slots with human > agent > function precedence;
  disagreement past a witness threshold surfaces as a contested state
  or a red band, never a silent resolution.

The write path is admission-checked, not trusted: the aspect's schema
validates the body, the witness gates the speaker, the aspect's grain
(and, where declared, its relevance condition) bounds which subjects
owe a value at all.

## The statement set

```
DECLARE SOURCE / RECIPE / DATASET          -- where data comes from
DECLARE RELATIONSHIP a.x -> b.y            -- declared structure (-> m:1, <-> 1:1)
DECLARE ASPECT ... AS MEASUREMENT|FACT|QUERY   -- the vocabulary
GLOSS aspect ON subject AS { ... }         -- the one write verb, body always JSON
DECLARE FUNCTION ... ACCEPTS ... RETURNS ...   -- scripts as functions
DECLARE WITNESS ... BY (...) DETECTOR ...  -- who may speak; who adjudicates
SELECT * FROM GLOSSARY(subject)            -- collapsed context read
SELECT * FROM ATTEST(subject.aspect)       -- adjudication read (band + score)
```

Reads, deletes, and function extraction are plain SQL. Schema-altering
DDL is closed: tables come only from recipes, so the landed table is
the typed table and import is a filter, not an ETL step.

## The server

A Rust workspace (`crates/`) on DataFusion and Iceberg: `parser`
(the grammar over DataFusion's parser), `glossary` (the store —
slots, supersession, admission, collapse), `session` (statement
routing, one session per actor and dataset), `catalog` + `import`
(the Iceberg lake and recipe execution), `scripts` (the rhai function
runtime and the reference library), `apps` (server-rendered data
apps from declarative artifacts), `serverd` (the doors).

One listener, three doors:

- **`/mcp`** — the MCP door: one `glossql` tool that takes statements
  and returns outcomes. Agents connect here; question forms for the
  human ride the same channel when the client supports them.
- **`/query`** — Arrow IPC streaming for programmatic reads.
- **`/app`** — server-rendered data apps (htmx + vega-lite, URL as the
  only state). A built-in model app ships in the binary: the
  verification surface over the glossary.

The door tells, skills teach: everything an agent must *learn* ships
as skills (`.claude/skills/`); everything *live* is read through the
language itself — the declared vocabulary, functions, witnesses, and
glossary are plain tables.

## The shipped library

A fresh workspace is not empty. The bootstrap declares the reference
measurement library:

| family | functions |
|---|---|
| profiling | `profile`, `outliers`, `temporal` |
| structure | `detect_relationships`, `relationship_coherence`, `detect_hierarchies`, `detect_derivations` |
| semantics | `behavior_evidence` (stock/flow, sign), `dimension_relevance`, `detect_grounding_collisions`, `slot_entropy` |
| metrics | `metric_cube`, `metric_bands`, `band_breach`, `rate_tolerance` |

plus the KPI kit: the semantic vocabulary (`meaning`, `role`,
`behavior`, `unit`, `dimension`, `entity`, …) with its witnesses, so
onboarding starts with a declared world model instead of hand-declared
scaffolding. Detection functions over-produce by design; the agent
reading them is the judge, and the false positives stay visible in the
measurement rather than being deleted.

## Building

A Rust workspace; `cargo build -p glossql-serverd` builds the server
(`--release` to run it in earnest). The dependency tree is heavy —
DataFusion, Iceberg, candle — and the parallel build is memory-hungry:
measured cold on a 15-core machine, compiler memory peaks at ~6 GB for
a dev build and ~9 GB for release, with single compile units up to
~2.6 GB. If the build dies without a compiler error — the OOM killer,
common in memory-capped containers — bound the parallelism to about
one job per 2 GB of available memory:

```
cargo build --release -j4    # or CARGO_BUILD_JOBS=4
```

The dev profile already trims debug info workspace-wide (the workspace
`Cargo.toml` records why); even a single job needs ~4 GB free for the
largest units.

## Status

A working proof-of-concept: the server runs, the corpus and the store,
session, and app suites are the standing invariant (`cargo test` at the
workspace root), and onboarding runs against real and generated data
are recorded in `reports/`. The language is a working draft — the
2026-08-03 simplification pivot is recorded in
`reports/2026-08-03-simplification.md`, and grammar changes still go
corpus-first: no construct lands without a fixture that survived
against a real artifact.

## Prior art

- [ggsql](https://github.com/posit-dev/ggsql) — grammar-of-graphics clauses as a SQL
  extension; the pattern for a declarative tail on SELECT.
- [Snowflake semantic views](https://docs.snowflake.com/en/sql-reference/sql/create-semantic-view) —
  declared semantics as SQL objects consumed by agents; declarations only, no
  evidence model.
- [Open Semantic Interchange / Apache Ossie](https://github.com/open-semantic-interchange/osi) —
  vendor-neutral interchange for semantic models; a possible export mapping for the
  vocabulary tier.
- [DuckLake](https://ducklake.select/) — lakehouse design: parquet data files, all
  metadata in a transactional SQL database; a persistence reference point.

What none of these carry — and this language treats as first-class — is
adjudicated context: measurements, agent assertions, and human assertions
held in separate slots per subject and aspect, with disagreement surfaced as
a band instead of silently resolved.
