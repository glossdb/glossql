# glossql — language specification

Status: **working draft**, 2026-08-04. This is the simplified language; it
supersedes the 2026-07 draft (git history holds it; the pivot record is
`reports/2026-08-03-simplification.md`). SPEC.md is the only normative prose.
`grammar.ebnf` is the source of truth for syntax; the corpus
(`crates/parser/tests/corpus/`) holds the evidence that every construct
transcribes a real artifact.

## 1. Overview

glossql is a declarative context language over a SQL host. It describes a
dataset — its sources, tables, relationships, meanings, checks — so that
agents and humans can work on the same data with the same context. The
language adds a small set of statements and two table functions to the host;
it does not re-specify SQL. Recipes, views, SELECT bodies, and deletes are
host SQL and stay opaque to the grammar.

Ground rules:

- **Context stays folded.** Everything that is context — even structured
  context — is a JSON document validated by a [JSON Schema](https://json-schema.org).
  Rendering conventions ride the schema. Authored prose is opaque.
- **The actor rides the transport.** Every connection carries an actor
  (agent_id or human_id), DuckDB-style. An answer the door elicits from
  the human mid-call is server-witnessed and lands with human standing —
  the same rule; the transport is just not always a connection (fixture
  22, ruled 2026-08-13). There is no BY clause anywhere; the engine
  stamps writer and actor kind on every statement.
- **The grammar fixes keys, not mechanics.** History, replay, and supersession
  mechanics are implementation. The grammar fixes what supersedes what: the
  key is (subject, aspect, actor kind).
- **Functions are scripts.** The engine's analytical machinery — profiling,
  quality checks, detection, adjudication — lives in registered rhai scripts with
  JSON contracts; a function is either a measurement or a detector, never a
  metric. Metrics are concepts: QUERY aspects, run as their SQL (§5.1).
  Analytical logic does not live in the grammar.

## 2. Origins

The vocabulary was not invented here: every construct began as a
transcription of a real artifact from the predecessor production system.
Corpus fixtures 01–13 quote those artifacts inline beside their glossql
forms, each carrying a transcription verdict (the corpus README indexes
them); from fixture 14 on, the fixtures transcribe this system's own runs
and rulings. The artifact-by-artifact map that used to sit here retired
2026-08-14 — the fixtures are the record, and the predecessor is no
longer a live reference.

## 3. Sources and datasets

A **source** names where data comes from. A **recipe** materializes a table
from a source. A **dataset** is the working unit: one dataset per workspace
(the binding lives in the app, not the grammar).

```sql
DECLARE SOURCE erp_export SET (type: parquet, location: 'lake/erp');
DECLARE SOURCE crm SET (type: relational_db, location: 'postgres://crm.internal/prod', via: crm_prod);
```

- `type`: `relational_db | parquet | csv | json`.
- `location`: a url; for file sources, the root directory recipe paths
  resolve under — never credentials.
- `via`: a reference to engine-held secrets. Secrets never appear in
  statements, so they never enter the log.

```sql
DECLARE RECIPE segments ON fin FROM crm AS $$SELECT id, segment FROM customer_segments$$;
```

The recipe SQL runs **at the source**: a relational source executes it in
its own dialect; at a file source the server runs it, with `read_parquet` /
`read_csv` / `read_json` resolving paths under the source's location and
`try_to_date` / `try_to_timestamp` registered. **The recipe carries the
casts** (ruled 2026-08-04): typing is authored, not decided. The author
probes the source first — `PROBE source AS $$sql$$` is the recipe
rehearsal: the same SQL surface and path resolution, executed at the
source, landing nothing, its result always carrying its schema (a
`LIMIT 0` probe of the final SQL rehearses exactly the identity the
recipe will stamp). Then the recipe lands as table `segments` — the typed
table, snapshotted by Iceberg on every import. The default recipe is
`SELECT *`. The engine keeps one number per import — `dropped_rows_count`,
source rows minus landed rows — in the declaration's outcome at the
decision moment and in the `imports` relation for history; which rows
were dropped is the author's question, answered at the source.

Statement identity is content: the recipe SQL and the schema it produces.
An unchanged re-declaration is a no-op; a changed one supersedes and
re-lands (ruled 2026-08-06): the old landing and its cached evidence are
dropped, the table lands fresh, and the import history keeps both
landings. Glosses stay — no machinery deletes knowledge; their snapshot
ids disclose their age against the fresh landing. `DROP TABLE` removes a
table whole (the lake table, the recipe, the cached evidence, the import
records) and refuses while the table holds data or glosses.
Substrate SQL runs behind an allowlist: queries pass, `DESCRIBE` and
`EXPLAIN` pass as reads about schema and plans (`EXPLAIN` only over a
query), `DROP TABLE` routes to the rules above, and everything else that
would alter schema or data directly is refused. Tables come from recipes.

```sql
DECLARE DATASET fin SET (purpose: 'working-capital analysis over ERP and CRM exports');
USE fin;
```

`USE` sets the resolution context: unprefixed `table.column` paths resolve
against the USE'd dataset; the full `dataset.table.column` prefix is always
allowed.

Derived views (enrichment, cleaning, dedup as dataset→dataset SQL) are
closed with the rest of schema-altering SQL for now; they return as a
governed surface once the deletion cascade exists. When they do, views are
glossable like tables.

## 4. Subjects and relationships

A **subject** is what a gloss, a function SELECT, or a witness attaches to:

- `dataset`
- `dataset.table` — views count as tables
- `dataset.table.column`
- `table.column -> table.column` — a declared relationship, addressed by its
  pair path (relationships have no names). An endpoint is a column or a
  column tuple: `table.(a, b)` — the tuple is the key.

```sql
DECLARE RELATIONSHIP orders.customer_id -> customers.id;
DECLARE RELATIONSHIP invoices.order_id <-> orders.id;
DECLARE RELATIONSHIP txn.(business_id, account) -> coa.(business_id, code);
```

- `->` is many-to-one (the FK direction); one-to-many is `->` written from
  the other side. `<->` is one-to-one. Many-to-many decomposes via a junction
  table.
- Relationships are **detected → verified → declared**: a function proposes
  candidates (a MEASUREMENT aspect, §5.1), an agent or human declares. Only
  declared relationships exist; there is no rejected or negative form — a
  rejected candidate is simply not declared, and detection functions are
  deterministic, so it does not resurface as new knowledge.

## 5. The glossary

The glossary is the context store. An **aspect** is a declared vocabulary
entry — a name with a JSON Schema and a kind. A **gloss** applies an aspect
to a subject with a JSON body. There are no fact names: the aspect is the key.

### 5.1 Aspects

```sql
DECLARE ASPECT unit WITH $${
  "type": "object",
  "properties": {"value": {"type": "string"}, "source_column": {"type": "string"}}
}$$ AS FACT;

DECLARE ASPECT revenue WITH $${
  "title": "revenue",
  "description": "Income from sales or services",
  "x-kind": "measure",
  "x-indicators": ["revenue", "sales", "income", "turnover", "receipts"]
}$$ AS QUERY;

DECLARE ASPECT min_max WITH $${
  "type": "object",
  "properties": {"min": {}, "max": {}}
}$$ AS MEASUREMENT ON COLUMN;
```

The kind fixes the aspect's role:

- **FACT** — an authored JSON assertion (units are USD, `created_at` is a
  timestamp, this convention holds). The `WITH` schema validates the gloss
  body. Constants and formulas are FACT aspects — "cannot be grounded" means
  simply not `AS QUERY`.
- **QUERY** — an SQL-grounded concept (revenue, accounts_receivable, dso).
  Metrics are QUERY aspects: the value materializes by running the grounding
  SQL, never through a function. Glosses validate against the **standard
  grounding schema** (§5.2), not the `WITH` schema; the `WITH` schema
  carries the ontology entry (description, indicators, unit, parameters,
  rendering).
- **MEASUREMENT** — a statistical evaluation (min_max, outliers,
  relationship_candidates). Never glossed: its value is the bound function's
  cached JSON output (§6, §7), served by `GLOSSARY()` beside facts and
  groundings, from the `cache` relation (§6).

The optional `ON DATASET | TABLE | COLUMN | RELATIONSHIP | SOURCE, …` list
is the aspect's **grain**: the subject classes glosses (and a `RETURNS`
binding) may attach to. Absent, the aspect speaks to all grains. Disclosure
(§5.3) stays within it: absence shows only on subjects the aspect is
declared for.

A grain list may carry a **condition** (ruled 2026-08-14):
`ON COLUMN WHEN role = 'measure'` names a sibling aspect and a value, and
the aspect is owed on a subject only while that aspect's winning slot
(human over agent, contest notwithstanding) carries `value` = the literal.
The condition bounds `unassessed` disclosure and every count derived from
it — never writes: glosses stay gated by grain alone, and a spoken slot
outside its condition serves normally, so a later re-ruling of the sibling
strands nothing. No sibling slot spoken means nothing owed yet. At
`DECLARE`, the referenced aspect must exist, and when its schema pins
`value` to an enum the literal must be a member.

`SOURCE` grain (ruled 2026-08-12): the subject is a declared source's name
(§3) — no further formality; `DECLARE SOURCE` is the definition. Sources
are workspace rows, so source-grain slots read, supersede, and disclose
across every dataset — the deposit one onboarding makes is what the next
dataset reads. Only aspects that declare `ON SOURCE` get this sweep; a
grainless aspect on a bare name stays dataset-scoped.

Multiplicity lives inside the blob — array-typed schemas — never in extra
statements or slots.

Re-declaring an aspect with identical content is a no-op. Changing it while
glosses under it exist is refused — delete those rows first; existing bodies
never silently stop matching their schema.

### 5.2 Glosses

One uniform statement; every body is JSON. Bodies are dollar-quoted
(`$$ … $$`, postgres-style; `$tag$ … $tag$` if the body itself contains
`$$`), so the JSON document rides verbatim — no escaping, ever:

```sql
GLOSS unit ON orders.amount AS $${"value": "EUR", "source_column": "currency_code"}$$;

GLOSS revenue ON fin.journal_lines AS $${
  "sql": "SELECT debit_amount - credit_amount FROM journal_lines WHERE account_type = 'revenue'",
  "assumptions": [
    {"dimension": "sign", "assumption": "ledger stores debits positive",
     "basis": "column_stats", "confidence": 0.9}
  ]
}$$;

GLOSS fk_note ON orders.customer_id -> customers.id AS $${"value": "2% orphaned rows"}$$;
```

- You cannot gloss an aspect that was not declared. Admission validates the
  body by the aspect's kind: FACT → the aspect's `WITH` schema, QUERY → the
  standard grounding schema, MEASUREMENT → rejected.
- The **standard grounding schema** is fixed, like the attest schema (§7.2):

```json
{
  "type": "object",
  "required": ["sql"],
  "additionalProperties": false,
  "properties": {
    "sql": {"type": "string"},
    "behavior": {"enum": ["stock", "flow"]},
    "assumptions": {
      "type": "array",
      "items": {
        "type": "object",
        "required": ["assumption"],
        "properties": {
          "dimension": {"type": "string"},
          "assumption": {"type": "string"},
          "basis": {"type": "string"},
          "confidence": {"type": "number", "minimum": 0, "maximum": 1}
        }
      }
    }
  }
}
```

- `behavior` is the authored stock marker (ruled 2026-08-11 with the
  band walk): readers that window a grounding take last-per-window for
  `"stock"`, sum for `"flow"`; absent reads as flow. Landed 2026-08-11
  after the monitoring evaluation found the closed schema rejecting the
  marker both readers already honored.
- **Supersession key: (subject, aspect, actor kind).** A human re-gloss
  supersedes the human's value; an agent's supersedes the agent's. The slots
  stay separate; a witness adjudicates across them (§7).
- Two QUERY glosses of the same aspect on different tables may coexist — two
  ways to calculate revenue arriving at the same number is a correct state.
  Whether they reconcile is a witness's job (a detector runs both and returns
  band + score).
- The glossary is an ordinary queryable relation; removal is SQL:

```sql
DELETE FROM glossary WHERE subject = 'orders.amount' AND aspect = 'unit';
```

- Every row carries `snapshot_id` — the subject's table snapshot at write
  time (NULL for dataset-level subjects and pair paths). Provenance and
  staleness are a join against the table's snapshot history, never a guess.
  The read shapes in §5.3 are unchanged; the column lives on the relation.

### 5.3 Reading

One table function, plain SQL:

```sql
SELECT * FROM GLOSSARY(orders.amount);
```

The default, collapsed read: one row per (subject, aspect) —
`(subject, aspect, value, band, score, state)`. The value is the precedence
pick — human over agent over function — withheld only when the witness
detector's score exceeds the threshold. `state` makes every gap visible;
the read never hides one:

- `unassessed` — a witness exists, nobody spoke. The row still appears
  (absence is a visible row, never an omission — fixture 09's benchmark).
- `contested` — entropy above the threshold; value withheld, band and
  score say how badly.
- `current` — served, basis unchanged.
- `stale` — served **and marked**: the table's snapshot moved on since the
  write. Staleness never suppresses judgment; it shows beside it.

```sql
SELECT * FROM GLOSSARY(orders.amount, all => true);
```

The raw read: one row per (subject, aspect, kind, witness) —
`(subject, aspect, kind, witness, actor, body, written_at)` — all current
values side by side; precedence between them is the reader's business.
`kind` is the aspect's kind; who spoke is `actor`, under `witness`.

With no subject, `GLOSSARY()` sweeps the `USE`'d dataset. A subject serves
itself and what lies under it: a table serves its columns and every
relationship it participates in, from either side; the far endpoint's own
context is never pulled in.

`subject::aspect` narrows either read to one declared aspect, as in ATTEST
(§7.2) — a metric's declaration and grounding SQL are one narrowed read
away:

```sql
SELECT * FROM GLOSSARY(fin::dso);
```

## 6. The function library

Scripts registered as functions, with name and contract; static by nature —
ported by copying the script. A function is either a **measurement** — it
fills a MEASUREMENT aspect through that aspect's witness (§7) — or a
**detector** (§7.1). The library is the engine's analytical machinery
(profiling, quality checks, detection) moved into the server as rhai
scripts; metrics are not functions (§5.1). Typing is not in it — the
recipe carries the casts (§3).

```sql
DECLARE FUNCTION profile_min_max FOR fin FROM 'functions/profile_min_max.rhai'
  RETURNS min_max;

DECLARE FUNCTION outliers FOR GLOBAL FROM 'functions/outliers.rhai'
  ACCEPTS (column_profile)
  RETURNS outlier_profile;

DECLARE FUNCTION reconcile_bands FOR fin FROM 'functions/reconcile_bands.rhai';
```

- `FOR` scopes the function to a dataset, or `GLOBAL`.
- `FROM` names the script.
- `ACCEPTS` names the aspects whose current values the server hands the
  script as its context document — settings are context, never call
  arguments; calls are always bare `f()`. Absent `ACCEPTS`, the script
  receives no context. The declaration relations `relationships` and
  `imports` may ride the list too, as invalidation edges only: no
  context entry arrives — the script reads them as tables — but a write
  to the relation kills the cache like an aspect value would.
- `RETURNS` names the aspect the function's output fills, mirroring
  `ACCEPTS`: functions read aspects and write an aspect, and the aspect's
  schema is the one contract — output is validated against it at
  extraction, and `GLOSSARY()` serves it as-is. A MEASUREMENT aspect has
  exactly one returning function (its producer); a FACT aspect may be
  returned by functions too — each is a data-grounded *voice* whose cached
  output joins the spoken slots (§7). Results land in the `cache` relation
  below.
- **No `RETURNS` declares a detector** — role by shape. A detector is
  named only in a witness's `DETECTOR` clause; it receives the witness's
  slots and threshold, never table data, and its output must satisfy the
  standard attest schema (§7.2) — the engine's contract, not authored.
- Every function implicitly receives its subject, with its SQL schema and
  neighborhood (parent, siblings, children) as metadata. Scripts run
  against the dataset — any SQL; determinism is the script's contract, the
  workspace its boundary.

Extraction:

```sql
SELECT profile_min_max() FROM orders;
SELECT outliers() FROM orders.amount;
```

The first run computes and caches; later selects read the cache. A body
that carries a top-level `summary` object serves the summary at
extraction — the full body stays cached and reads back through
`GLOSSARY(subject::aspect)` (ruled 2026-08-14: a large measurement's
extraction result was effectively write-only at the door; the summary is
the function's own authorship, never a truncation). The cache
is an ordinary relation, like the glossary, named `cache`: one row per
(subject, function, witness) —
`(subject, function, witness, body, computed_at, snapshot_id)`, the
snapshot being the subject's table state the run computed against (§5.2).
`witness` is empty for a function's own value, which is keyed by its
subject like any value; it names the seat for a detector's verdict, which
depends on the aspect, threshold and slots that witness holds (§7.2). Re-running is removal, not a modifier — DELETE at whatever
grain the WHERE clause picks, and select again:

```sql
DELETE FROM cache WHERE function = 'dso';
```

**Writes invalidate, reads recompute, judgment only supersedes.** A new
value for an aspect — glossed, or a bound measurement's fresh output —
deletes the cached results of every function that `ACCEPTS` it, at and
under the subject: the declaration that names a script's inputs also names
what kills its cache, and it is the only definition-level invalidation
there is. A declared relationship or a recorded import invalidates
dataset-wide through the same edge, for functions that `ACCEPTS` the
relation. Data freshness is snapshot staleness, marked at read (§5.3) — a
table's definition never changes underneath its evidence, because a
changed recipe sweeps the table's cached evidence as it re-lands and
`DROP TABLE` takes the evidence with it (§3). Nothing recomputes at write time, and no machinery ever deletes a
gloss: stale judgment is served and marked, superseded only by whoever
owns the slot.

Whether multi-function
extraction fans out or runs one call after another is the caller's choice —
send one statement with many calls, or many statements; the grammar carries
no ordering surface. Functions never write the glossary; their results live
in the cache.

## 7. Witnesses

A witness is declared per aspect, dataset-wide. Per (subject, aspect) it
holds one slot per speaker: each function voice (served from the cache of
a function whose `RETURNS` names the aspect, §6), the agent's gloss, the
human's gloss — one current value each.

### 7.1 Declaration

```sql
DECLARE WITNESS behavior_w ON behavior
  BY (AGENT, HUMAN)
  DETECTOR behavior_entropy
  THRESHOLD 0.7;

DECLARE WITNESS reconciliation_w ON reconciliation
  DETECTOR reconcile_bands THRESHOLD 0.5;
```

- `BY` lists the actor kinds admitted to gloss the aspect — `AGENT`,
  `HUMAN`. Function voices are not gated here: a function speaks by
  `RETURNS` (§6). `BY` is refused on a MEASUREMENT aspect — measurements
  are never glossed.
- `DETECTOR` names a function without `RETURNS` (§6) that examines the
  slots and returns band + score.
- Both clauses are optional, but not together: a witness names `BY`, or
  `DETECTOR`, or both. On a MEASUREMENT aspect only the detector form is
  possible — judgment applied to a measurement's output, as in the second
  example.
- `THRESHOLD` (0..1) is the entropy cutoff used by the collapsed
  `GLOSSARY()` read (§5.3).

### 7.2 Attestation

```sql
SELECT * FROM ATTEST(orders.amount::behavior);
SELECT subject, band FROM ATTEST(fin.trial_balance) WHERE band = 'red';
```

The **standard attest schema** is fixed:
`(subject, aspect, witness, band, score, computed_at)` — `band` in
`green | yellow | orange | red`, `score` the disagreement/entropy in 0..1.
Detectors run **at read**: a verdict missing or older than the newest slot
write recomputes when `ATTEST()` or a collapsed `GLOSSARY()` read needs it,
and caches like any function result — `DELETE FROM cache` still forces it.
A verdict belongs to its **witness**, not to its detector: one detector
serving three witnesses holds three verdicts, computed from each witness's
own slots against its own threshold.
Detail lives in the value function's own cached output, reachable by
SELECT. Sweeps ("all contested
behavior columns") are WHERE clauses over the attest relation, never a
special form; with no argument, `ATTEST()` sweeps the `USE`'d dataset.
`subject::aspect` — the host's cast spelling — narrows attestation to one
declared aspect, unambiguously: `fin.trial_balance` names a table,
`fin::reconciliation` an aspect across the dataset.

Judgment lives here — in detector functions and read policy — never in
results: no construct writes a verdict into data.

## 8. Skills

Agents use the language through skills; agents are not part of the grammar.
The door tells, skills teach (ruled 2026-08-04): the server's one surface
takes statements and returns outcomes, and everything an agent must *learn*
ships as skills sourced from this repository's artifacts — the language
(this document, `grammar.ebnf`), the flows (corpus fixtures 11 and 12),
and function authoring (the reference
scripts and their kernels). Everything *live* — declared functions, the
glossary, the tables — is read through the language, never taught.

## 9. Open

One open question, fixture 09's remaining half: whether agents actually
compose their context from the reads — sweeping `state != 'current'`,
respecting bands — now that the collapsed shape discloses every gap
(§5.3, decided 2026-08-04: serving wrong information is not an
experiment). Closes by running the agent experiment, not by argument.

PoC notes: batch visibility comes from (long-running) transactions — the
running system's run_id + snapshot-head pointer is the verbose version of
the same guarantee · the actor rides the transport — the connection,
DuckDB-style, or a door-elicited answer, server-witnessed with human
standing ·
value-at-read, ruled 2026-08-06, bound 2026-08-07: a QUERY aspect's
value materializes as a namespaced table function — `FROM read.dso()`
expands the collapsed current grounding at plan time, nesting allowed
behind a cycle guard; running stays the reader's act. The prefix is
`read.` (ruled 2026-08-11, renamed from `metric.`, no alias): one
serving door over every QUERY gloss, whatever flavor `x-kind` names.
Analyses stay operation-named doors — `whatif.<scenario>()` (ruled
2026-08-11, fixture 19) serves a declared scenario: a FACT aspect per
scenario carrying overrides, the server replaying the recipes at a
bracketing grid of strengths and reading across the replayed worlds;
the grid, reach, and support guard are machinery, never statement
syntax; `misfit.<frame>()` (ruled 2026-08-11, fixture 20) ranks a
declared frame's rows against the frame itself — the frame is an
ordinary QUERY gloss, the density kernel and its caps machinery;
`metric_series()` (2026-08-13) serves the cached `metric_cube`
measurement as long rows — metric names become data so a static frame
(the built-in metrics app) slices any metric with plain value filters;
cached-only, nothing computes at read.

Deferred, not under discussion: access rights · portability · persistence
backend and engine mapping.
