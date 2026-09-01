# glossql

A declarative context language over a SQL host, and a server that speaks
it. The language describes a dataset — sources, tables, relationships,
meanings, checks — so that agents and humans work on the same data with
the same context. The server is one Rust binary with the query engine
(DataFusion) and the table format (Iceberg) in-process. An agent
connects over MCP, lands data, and builds the dataset's glossary;
questions the data cannot answer go to a human, and the answer is
stored as part of the record.

The full definition is **[SPEC.md](./SPEC.md)** — the single normative
document of this repository. `grammar.ebnf` is the machine source of
truth for syntax. The corpus (`crates/parser/tests/corpus/`) is the
parser's acceptance suite; each construct is checked against a real
artifact.

The name: a *gloss* is a marginal annotation explaining a text's meaning;
a glossary is a collection of them. `GLOSS` is the write verb — you gloss
an aspect onto a subject; `GLOSSARY()` is the read.

## What it is for

- **Agents define the glossary, not just its entries.** An aspect is
  a declared JSON Schema. An agent that needs vocabulary the workspace
  lacks declares it with statements: the aspect, the functions that
  measure it, the detector that scores agreement on it. The shipped
  measurement library and the KPI kit are declared the same way, so
  extending the workspace's analytics means writing statements, not
  building a plugin.
- **Ambiguity is resolved with a human, and the answer is stored.**
  Measurements answer what the rows can: stock or flow, grain, sign
  conventions. Definitions, conventions, and choices between readings
  become open questions, shown as cards on the docket app or as forms
  in the agent's MCP calls. The human's answer is written as a gloss
  in the human's slot, takes precedence over the agent's at every
  read, and persists — it is a stored row, not a chat message.
- **One process uses the whole machine.** Engine, lake, glossary, and
  apps run in one process; there is no network hop between an agent's
  query and the data. Work arrives as statements and SQL, the engine
  plans all of it, and it uses all available cores and memory.
  Isolation is deployment's job: one workspace per VM.
- **Iceberg snapshots version everything.** Every declared relation —
  data, glosses, rulings, the parts of an authored app — is an Iceberg
  table, and one statement is one commit. There is no separate
  version-control system to operate: history and audit are reads over
  snapshots, and each gloss stores the subject table's snapshot id, so
  it is always clear which data a claim was measured against.

The integration points are standard: sources land by recipe from files
or over ADBC (the SQL runs at the source), reads are served as Arrow
IPC over plain HTTP, and the lake is ordinary Iceberg — a local
catalog for development, a REST catalog in production.

## Why a grammar and JSON Schemas

Data work mixes two kinds of information: what you hold true before
looking at the rows, and what the rows show. Code usually tangles
them. glossql keeps them apart and joins them at read time.

- **A priori — the declared world model.** Aspects (each with a JSON
  Schema as its contract), witnesses (who may speak on an aspect, and
  which detector scores the voices), relationships, sources, and
  recipes are declarative statements. A world model made of statements
  is text: an agent can write one, carry it, and apply it to new data.
  The vocabulary exists before the dataset does. A fresh workspace
  starts with one: the shipped measurement library and the KPI kit.
- **A posteriori — the measured evidence.** Functions compute what the
  landed rows show: profiles, join evidence, stock/flow behavior,
  hierarchies, metric bands. Measurements are tuned for recall. They
  emit candidates with evidence, not conclusions. The reading agent
  judges them against the data.
- **Glosses join the two.** A gloss writes a JSON value into a slot
  keyed (subject, aspect, actor kind). Function, agent, and human
  voices sit in separate slots and never overwrite each other. Reads
  collapse the slots with human > agent > function precedence.
  Disagreement past a witness threshold shows as a contested state or
  a red band, never as a silent resolution.

The write path is checked, not trusted. The aspect's schema validates
the body. The witness gates the speaker. The aspect's grain — and its
relevance condition, where declared — bounds which subjects owe a
value.

## The statement set

```
DECLARE SOURCE / RECIPE / DATASET          -- where data comes from
DECLARE RELATIONSHIP a.x -> b.y            -- declared structure (-> m:1, <-> 1:1)
DECLARE ASPECT ... AS MEASUREMENT|FACT|QUERY   -- the vocabulary
GLOSS aspect ON subject AS { ... }         -- the one write verb, body always JSON
DECLARE FUNCTION ... AS $$ ... $$ RETURNS ...  -- SQL bodies as functions
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
routing, a channel per call), `catalog` + `import`
(the Iceberg lake and recipe execution), `scripts` (the native
kernels and the reference library), `apps` (server-rendered data
apps from declarative artifacts), `serverd` (the doors).

One listener, three doors, no server-side cursors. A browser stays on
one dataset, so `/query` and `/app` carry the dataset in the path. An
agent moves between datasets, so `/mcp` is one endpoint and the
dataset is named in the statements:

- **`/mcp`** — the MCP door: one `glossql` tool that takes
  statements and returns outcomes. Agents connect here and open each
  call with `USE <dataset>;`; question forms for the human ride the
  same call.
- **`/<dataset>/query`** — Arrow IPC streaming for programmatic reads.
- **`/<dataset>/app`** — server-rendered data apps (htmx + vega-lite,
  URL as the only state). The binary ships a docket app: open
  questions for the human, settled rulings, and the metrics and record
  behind them. A workspace can add its own apps — as a directory, or
  as glosses, which an agent can write.

A bearer token from the workspace's issuer says who is speaking; the
server verifies it against the issuer's published keys, and `sub` is
the actor id. The door sets the actor kind — `/mcp` is the agent door,
the other two are human — so an agent's connection never reaches the
human's slot.

Everything an agent must learn ships as skills (`skills/*/SKILL.md`),
served on the MCP door: each skill is an MCP resource
(`skill://<name>/SKILL.md`, its references beside it as
`skill://<name>/references/<page>.md`) and a prompt of the same name,
with `SPEC.md` and `grammar.ebnf` beside them as `doc://` resources.
Everything live is read through the language: the declared vocabulary,
functions, witnesses, and glossary are plain tables.

## The shipped library

The bootstrap declares the reference measurement library in every
fresh workspace:

| family | functions |
|---|---|
| profiling | `profile`, `outliers`, `temporal` |
| structure | `detect_relationships`, `relationship_coherence`, `detect_hierarchies`, `detect_derivations` |
| semantics | `behavior_evidence` (stock/flow, sign), `dimension_relevance`, `detect_grounding_collisions`, `slot_entropy` |
| metrics | `metric_bands`, `band_breach`, `rate_tolerance` |

plus the KPI kit: the semantic vocabulary (`meaning`, `role`,
`behavior`, `unit`, `dimension`, `entity`, …) with its witnesses.
Onboarding starts with a declared world model, not hand-built
scaffolding. Detection functions over-produce by design. The reading
agent judges; the false positives stay visible in the measurement.

## Building

A Rust workspace. `cargo build -p glossql-serverd` builds the server;
use `--release` to run it for real. The dependency tree is heavy —
DataFusion, Iceberg, candle — and the parallel build needs memory:
measured cold on a 15-core machine, compiler memory peaks at ~6 GB for
a dev build and ~9 GB for release, with single compile units up to
~2.6 GB. If the build dies without a compiler error, the OOM killer
hit — common in memory-capped containers. Bound the parallelism to
about one job per 2 GB of available memory:

```
cargo build --release -j4    # or CARGO_BUILD_JOBS=4
```

The dev profile already trims debug info workspace-wide (the workspace
`Cargo.toml` records why); even a single job needs ~4 GB free for the
largest units.

## Status

A working proof-of-concept. `cargo test` at the workspace root is the
standing invariant: the parser corpus, every fenced example under
`docs/` and `skills/`, and the store, session, and app suites. The
language is a working draft. Grammar changes are corpus-first: a
construct is only added together with a fixture that was checked
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

None of these carry adjudicated context: measurements, agent
assertions, and human assertions in separate slots per subject and
aspect, with disagreement shown as a band instead of silently
resolved. This language treats that as first-class.
