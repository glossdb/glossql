# glossql — language specification

Status: **working draft**. SPEC.md is the only normative prose.
`grammar.ebnf` is the source of truth for syntax; the corpus
(`corpus/`) holds the evidence that every construct transcribes a
real artifact.

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
  (agent_id or human_id). An answer the interface elicits from
  the human mid-call is recorded through the interface and is stored with human standing —
  the same rule; the transport is just not always a connection (fixture
  22). There is no BY clause anywhere; the engine
  stamps writer and actor kind on every statement.
- **The grammar fixes keys, not mechanics.** History, replay, and supersession
  mechanics are implementation. The grammar fixes what supersedes what: the
  key is (subject, aspect, actor kind).
- **Names fold as the host's do.** An unquoted identifier is lowercased
  before it is created or matched; a double-quoted identifier keeps its
  case. This is the SQL host's rule for its own names, and every glossql
  statement follows it, so a name declared unquoted is reached unquoted.
  A column is the one name not declared here — it is loaded spelled as the
  export spelled it — so an unquoted column name that folds to no column
  of its table and differs from exactly one column's spelling by case
  alone reaches that column, in a read and in a subject alike; two such
  spellings refuse the name and name both. A table is reached as
  declared.
- **A function's body is one SQL query.** A function with `RETURNS` is
  a measurement — its query runs over data at the read's pin; a
  detector's query runs over its test's `slots` relation. A
  function is either a measurement or a detector, never a
  metric. Metrics are concepts: VIEW aspects, run as their SQL (§5.1).
  Analytical logic does not live in the grammar.

## 2. Origins

The vocabulary was not invented here: every construct began as a
transcription of a real artifact from the predecessor production system.
Corpus fixtures 01–13 quote those artifacts inline beside their glossql
forms, each carrying a transcription result (the corpus README indexes
them); from fixture 14 on, the fixtures transcribe this system's own runs
and review decisions. The fixtures are the acceptance tests; the predecessor is not a
live reference.

## 3. Sources and datasets

A **source** names where data comes from. A **recipe** materializes a table
from a source. A **dataset** is the working unit: one dataset per workspace
(the binding lives in the app, not the grammar).

```sql
DECLARE SOURCE erp_export SET (type: parquet, location: 'lake/erp');
DECLARE SOURCE crm SET (type: relational_db, location: 'postgres://crm.internal/prod', via: crm_prod);
```

- `type`: `relational_db | parquet | csv | json`; any other spelling is
  refused at the declaration.
- `location`: a url; for file sources, the root directory recipe paths
  resolve under — never credentials.
- `via`: a reference to engine-held secrets. Secrets never appear in
  statements, so they never enter the log.

```sql
DECLARE RECIPE segments ON fin FROM crm AS $$SELECT id, segment FROM customer_segments$$;
```

The recipe SQL runs **at the source**: a relational source executes it in
its own dialect; at a file source the engine runs it, with `read_parquet` /
`read_csv` / `read_json` resolving paths under the source's location, and
DuckDB's `try_strptime(value, format)` parsing dates (a list of formats
takes the first that parses; NULL on failure). **The recipe carries the
casts**: typing is authored, not decided. The author
probes the source first — `PREVIEW source AS $$sql$$` is the recipe
rehearsal: the same SQL surface and path resolution, executed at the
source, load nothing, its result always carrying its schema (a
`LIMIT 0` probe of the final SQL rehearses exactly the identity the
recipe will stamp). Then the recipe loads as table `segments` — the typed
table, versioned on every import. The default recipe is
`SELECT *`. A recipe may name its table's key, `SET (key: order_id)`
before `AS`, a column of its result or a comma-separated list of
them: a keyed recipe's result merges into the table by that key on
every import; an unkeyed recipe's result is the table. A keyed
recipe's result carries each key once, at every load; a result
that carries a key twice is refused, naming the key. Which row holds
is the recipe's to say. The engine
keeps one number per import — `dropped_rows_count`,
source rows minus loaded rows — in the declaration's outcome at the
decision moment and in the `imports` relation for history; which rows
were dropped is the author's question, answered at the source.

Statement identity is content: the recipe SQL and the schema it produces.
An unchanged re-declaration is a no-op; a changed one supersedes and
re-lands: the table is replaced whole, in one commit, and the recipe
it carries is superseded. The loads it holds stay on the catalog,
as every load does. Glosses stay — no machinery deletes knowledge;
a gloss on a column the re-land re-derived, retyped or dropped reads
as `stale`, one on a column it kept stays `current` (§5.2). `DROP
TABLE` drops the table, and refuses while it holds data or glosses.

```sql
IMPORT segments;
```

`IMPORT` is the data update: the table's recipe runs again over its
source, and the table becomes the result as the source is now, in
one commit — the table, the loads it holds and its glosses stand.
An unkeyed recipe replaces the table whole; a keyed recipe replaces
the rows whose key the result carries and keeps the rest. Which files
or rows the engine reads to get there is its own economy, never the
statement's. It reproduces the table's schema or it errors.
It is sent from outside, by a schedule or an agent; nothing watches a
source. The table is named as a read names it, under the `USE`'d
dataset or with its dataset in front.

Substrate SQL runs behind an allowlist: queries pass, `DESCRIBE` and
`EXPLAIN` pass as reads about schema and plans (`DESCRIBE` over any
name a read can plan — a loaded table, a store relation, a shipped
read, a metrics read with its arguments; `SHOW TABLES` lists the bound
dataset's tables and its metric definitions' views, typed; `EXPLAIN` only
over a query), `DROP TABLE` routes to the rules
above, and everything else that would alter schema or data directly is
refused. Tables come from recipes.

```sql
DECLARE DATASET fin SET (purpose: 'working-capital analysis over ERP and CRM exports');
USE fin;
```

`USE` sets the resolution context: unprefixed `table.column` paths resolve
against the USE'd dataset; the full `dataset.table.column` prefix is always
allowed. A two-part path whose head names both a dataset and a stored
table of the USE'd dataset is `table.column` — the USE'd dataset is the
nearer scope, and the other dataset's names are reached under its own
`USE`. A table that carries its own dataset's name has no table-grain
subject: the bare name is the dataset (§4), and its columns are reached
as `table.column`.

Derived views (enrichment, cleaning, dedup as dataset→dataset SQL) are
closed with the rest of schema-altering SQL for now; they return as a
governed surface once the deletion cascade exists. When they do, views are
glossable like tables.

## 4. Subjects and relationships

A **subject** is what a gloss, a function SELECT, or a test attaches to:

- `dataset`
- `dataset.table` — views count as tables
- `dataset.table.column`
- `table.column -> table.column` — a declared relationship, addressed by its
  pair path (relationships have no names). An endpoint is a column or a
  column tuple: `table.(a, b)` — the tuple is the key.

A dataset's name and a table's name carry no dot; a column's may,
spelled quoted, and the column is what follows the second dot.

```sql
DECLARE RELATIONSHIP orders.customer_id -> customers.id;
DECLARE RELATIONSHIP invoices.order_id <-> orders.id;
DECLARE RELATIONSHIP txn.(business_id, account) -> coa.(business_id, code);
```

- `->` is many-to-one (the FK direction); one-to-many is `->` written from
  the other side. `<->` is one-to-one. Many-to-many decomposes via a junction
  table.
- An endpoint is a column of a loaded table. A declaration naming a table
  or column the dataset does not have is refused.
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
}$$ AS VIEW;

DECLARE ASPECT min_max WITH $${
  "type": "object",
  "properties": {"min": {}, "max": {}}
}$$ AS MEASUREMENT ON COLUMN;
```

The kind fixes the aspect's role:

- **FACT** — an authored JSON assertion (units are USD, `created_at` is a
  timestamp, this convention holds). The `WITH` schema validates the gloss
  body. Constants and formulas are FACT aspects — "cannot be grounded" means
  simply not `AS VIEW`.
- **VIEW** — an SQL-grounded concept (revenue, accounts_receivable, dso).
  Metrics are VIEW aspects: the value materializes by running the metric definition
  SQL, never through a function. Glosses validate against the **metric definition
  contract** (§5.2), not the `WITH` schema; the `WITH` schema
  carries the ontology entry (description, indicators, parameters,
  rendering). Anything the company revises — meaning, unit, owner, source —
  belongs in a gloss instead, because a declaration cannot be re-declared
  once anything is glossed on the aspect and so can never be superseded,
  contested or outranked.
- **MEASUREMENT** — a statistical evaluation (min_max, outliers,
  relationship_candidates). Never glossed: its value is the bound
  function's stored output (§6, §7), extracted at the read's pin and
  served by `GLOSSARY()` beside facts and metric definitions.

The optional `ON DATASET | TABLE | COLUMN | RELATIONSHIP | SOURCE, …` list
is the aspect's **grain**: the subject classes glosses (and a `RETURNS`
binding) may attach to. Absent, the aspect speaks to all grains. A VIEW
aspect takes no grain clause: a metric is grounded on the dataset (§4),
so its grain is the dataset. Disclosure
(§5.3) stays within it: absence shows only on subjects the aspect is
declared for.

A grain list may carry a **condition**:
`ON COLUMN WHEN role = 'measure'` names a sibling aspect and a value, and
the aspect is required on a subject only while that aspect's winning value
(human over agent, contest notwithstanding) carries `value` = the literal.
The condition bounds `unassessed` disclosure and every count derived from
it — never writes: glosses stay gated by grain alone, and a spoken value
outside its condition serves normally, so a later re-ruling of the sibling
strands nothing. No sibling value spoken means nothing required yet. At
`DECLARE`, the referenced aspect must exist, and when its schema pins
`value` to an enum the literal must be a member.

A grain list of `COLUMN` alone may instead carry a **type condition**:
`ON COLUMN WHEN TYPE = 'GEOMETRY'` or `WHEN TYPE IN ('DOUBLE', 'DECIMAL')`
names data types, and the aspect is required on the columns whose base type
— the name before any parenthesis, `DECIMAL` for `DECIMAL(18,2)`,
matched without regard to case — is one of them. A kit for a kind of
column (geometry, text, time) narrows its words this way without a
sibling aspect judged on every column. The condition bounds disclosure
as the sibling form does and never gates a write; with any other grain
in the list it is refused at `DECLARE`.

`SOURCE` grain: the subject is a declared source's name
(§3) — no further formality; `DECLARE SOURCE` is the definition. Sources
are workspace rows, so source-grain values read, supersede, and disclose
across every dataset — the deposit one onboarding makes is what the next
dataset reads. Only aspects that declare `ON SOURCE` get this sweep; a
grainless aspect on a bare name stays dataset-scoped.

Multiplicity lives inside the blob — array-typed schemas — never in extra
statements or values.

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
  body by the aspect's kind: FACT → the aspect's `WITH` schema, VIEW → the
  metric definition contract, MEASUREMENT → rejected.
- An aspect whose grain names `TABLE` or `COLUMN` takes only loaded
  subjects: the table, and the column, must exist in the dataset. An
  aspect without a grain clause takes the subject as spelled; a VIEW
  aspect takes the dataset.
- The **metric definition contract** is fixed, like the test-results schema (§7.2): what
  the engine reads of a metric definition, and nothing a reader of its rows needs.
  It is open: a key it does not name passes, and a kit's metric definition schema
  is what closes the body.

```json
{
  "type": "object",
  "oneOf": [{"required": ["sql"]}, {"required": ["stopped"]}],
  "properties": {
    "sql": {"type": "string"},
    "stopped": {"type": "string"},
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

- A metric definition's view is a table of its dataset (§3), and its columns
  are subjects. What a reader needs to combine the view's rows — which
  column is the period, which is the measure and how it combines over
  periods and members (a flow sums, a stock keeps its last level, a
  ratio re-divides its summed halves, served as columns the claim
  names), which columns slice it, and the row identity — is glossed on
  the view and its columns under the aspects a kit declares, never
  keyed in the body. A reader that needs a claim nobody made refuses
  and names the gloss; it defaults nothing.
- A kit may declare a schema for a body key its readers need, an
  aspect flavored `x-kind: grounding`: the engine validates every
  VIEW gloss against each such schema after the contract. The aspect
  is never glossed, no test names it, and re-declaring it is
  refused while a current VIEW value would stop matching.
- `stopped` in place of `sql` records that the author serves no number,
  and why: what is missing, where, how much, what closes it. The read
  interface refuses with that text, a reader of the view's rows abstains
  with it, `metric_surfaces` serves it, and the assumptions ride the
  body as on any metric definition. Supersession is the
  usual: a human `sql` over an agent `stopped` serves, a human `stopped`
  over an agent `sql` stops.
- **A metric definition is a view in the catalog.** The dataset's schema holds
  a view under the metric's name carrying the serving metric definition's
  `sql`, in the engine's dialect; a superseding metric definition replaces it,
  a stop ends it. The bare name — or `dataset.metric` — plans as a
  table does. A dataset's schema
  is one name space: a metric definition on a loaded table's name is refused,
  and a recipe on a grounded metric's name is refused.
- **Supersession key: (subject, aspect, actor kind).** A human re-gloss
  supersedes the human's value; an agent's supersedes the agent's. The values
  stay separate; a test scores across them (§7).
- Two VIEW glosses of the same aspect on different tables may coexist — two
  ways to calculate revenue arriving at the same number is a correct state.
  Whether they reconcile is a test's job (a function runs both and
  reports the residual; a detector bands it).
- The glossary is an ordinary queryable relation; removal is SQL:

```sql
DELETE FROM glossary WHERE subject = 'orders.amount' AND aspect = 'unit';
```

- Every row carries `snapshot_id` — the catalog snapshot at write time
  (NULL for dataset-level subjects and pair paths). A gloss reads `stale`
  once its subject changed after that snapshot: a column re-derived,
  retyped or dropped, a table one of whose columns did. Rows arriving
  under the same shape do not age it. Provenance and staleness are a
  join against the catalog's column history, never a guess. The read
  shapes in §5.3 are unchanged; the column lives on the relation.

### 5.3 Reading

One table function, plain SQL:

```sql
SELECT * FROM GLOSSARY(orders.amount);
```

The default, collapsed read: one row per (subject, aspect) —
`(subject, aspect, value, band, score, state)`. The value is the precedence
pick — human over agent over function — withheld only when the test
detector's score exceeds the threshold. `state` makes every gap visible;
the read never hides one:

- `unassessed` — a test exists, nobody spoke. The row still appears
  (absence is a visible row, never an omission — fixture 09's benchmark).
- `contested` — entropy above the threshold; value withheld, band and
  score say how badly.
- `current` — served, basis unchanged.
- `stale` — served **and marked**: the subject changed since the write
  (§5.2), or an input the serving function value read has moved since
  it stored (§7). Staleness never suppresses judgment; it shows beside
  it.

```sql
SELECT * FROM GLOSSARY(orders.amount, all => true);
```

The raw read: one row per (subject, aspect, kind, test) —
`(subject, aspect, kind, test, actor, body, written_at, current)` —
every value side by side; precedence between them is the reader's
business. `kind` is the aspect's kind; who spoke is `actor`, under
`test`; `current` is false for a function value an input of which
has moved since it stored (§7).

With no subject, `GLOSSARY()` sweeps the `USE`'d dataset. A subject serves
itself and what lies under it: a table serves its columns and every
relationship it participates in, from either side; the far endpoint's own
context is never pulled in.

`subject::aspect` narrows either read to one declared aspect, as in TESTS
(§7.2) — a metric's declaration and metric definition SQL are one narrowed read
away:

```sql
SELECT * FROM GLOSSARY(fin::dso);
```

## 6. The function library

Functions registered with name, contract and body. A function is either
a **measurement** — it fills a MEASUREMENT aspect through that aspect's
test (§7), its query running over data — or a **detector** (§7.1),
whose query runs over its test's `slots`. The library is the engine's
analytical machinery (profiling, quality checks, detection) shipped as
declarations; metrics are not functions (§5.1). Typing is not in it —
the recipe carries the casts (§3).

```sql
DECLARE FUNCTION profile FOR GLOBAL AS $$
  SELECT profile(v) FROM subject_column($subject)
$$ RETURNS column_profile;

DECLARE FUNCTION reconcile_bands FOR fin AS $$
  SELECT subject,
         CASE WHEN max(body['delta']) <= $threshold THEN 'green' ELSE 'red' END AS band,
         coalesce(max(body['delta']), 0.0) AS score
  FROM slots GROUP BY subject
$$;
```

- `FOR` scopes the function to a dataset, or `GLOBAL`.
- `AS` carries the body itself (fixture 24) — never a path, which
  would put the body outside the language: an agent connected over the
  MCP interface has statements and no filesystem, so it could neither
  author a function nor read the library's own. The
  declaration supersedes on re-declare like any other, and
  `SELECT script FROM functions` serves the shipped library as worked
  examples.
- **One language.** Every body is one SQL query, planned and run by the
  engine — read-only. A measurement runs at the statement's pin,
  composing everything a read can; `$subject` arrives as a string
  literal, and `subject_column($subject)` is the subject's column as a
  relation named `v`. The result is stored by shape: one row and one column
  is the value, one row is an object of its columns, many rows are an
  array of row objects. Settings are context, never call arguments —
  calls are always bare `f()`. A body composes its context inline: a
  glossed value is a read over the glossary, a stored value a read over
  `measurements`, a statistic the same aggregate computed in place. A
  declaration relation reads as a table, which needs no naming.
- **A bound screen is an estimate.** A kit's screen may read a table
  up to a bound of cells; a larger table is read as a repeatable
  sample of its rows, and the measurement says how many rows it read.
  Such a value is an estimate.
- `RETURNS` names the aspect the function's output fills; the aspect's
  schema is the one contract — output is validated against it at
  extraction, and `GLOSSARY()` serves it as-is. A MEASUREMENT aspect has
  exactly one returning function (its producer); a FACT aspect may be
  returned by functions too — each is a data-grounded *value* whose
  output joins the spoken values (§7), computed at read.
- **No `RETURNS` declares a detector** — role by shape. A detector is
  named only in a test's `DETECTOR` clause. Its query plans over
  the `slots` relation — the test's raw rows (§5.3),
  narrowed to its aspect, with `speaker` and `current` beside them and
  `body` typed by the values' own JSON — and the test `THRESHOLD`
  binds as `$threshold`. Each returned row must satisfy the standard test-results
  contract (§7.2) — the engine's contract, not authored.
- A measurement receives its subject as `$subject`; a detector's
  relation carries `subject` per row, so one query answers every
  subject.

Extraction:

```sql
SELECT profile_min_max() FROM orders;
SELECT outliers() FROM orders.amount;
```

Extraction computes at the read's pin — the set of inputs, data and
declarations, the statement resolved — and writes one row in the
`measurements` relation:
`(dataset, function, subject, aspect, pin, value, computed_at, reads)`,
`reads` naming the inputs the body actually read — tables and store
relations by name, `*` for the dataset's every table, `**` when the
reads could not be enumerated (the row then holds only at its exact
pin), empty for a body that read nothing. A later extraction
serves that row while those inputs are unchanged; one of them moving
makes the next extraction compute — the effect scoped to its cause, so
there is no invalidation, only a miss, a write the body never reads is
not its staleness, and old rows stand as the drift record. A body that carries a top-level `summary`
object serves the summary at extraction — the full value reads back
through `GLOSSARY(subject::aspect)` (a large measurement's extraction
result would otherwise be write-only at the interface; the summary is the
function's own authorship, never a truncation).
Nothing recomputes at write time, and no machinery ever deletes a gloss:
stale judgment is served and marked, superseded only by whoever owns the
value.

Whether multi-function
extraction fans out or runs one call after another is the caller's choice —
send one statement with many calls, or many statements; the grammar carries
no ordering surface. Functions never write the glossary; their results
land in `measurements`.

## 7. Tests

A test is declared per aspect, dataset-wide. Per (subject, aspect) it
holds one value per speaker: each function value (the newest measurement
of a function whose `RETURNS` names the aspect, §6 — served whatever
its pin, marked `current` while what it read is unchanged), the agent's
gloss, the human's gloss — one value each.

### 7.1 Declaration

```sql
DECLARE TEST behavior_w ON behavior
  BY (AGENT, HUMAN)
  DETECTOR behavior_entropy
  THRESHOLD 0.7;

DECLARE TEST reconciliation_w ON reconciliation
  DETECTOR reconcile_bands THRESHOLD 0.5;
```

- `BY` lists the actor kinds admitted to gloss the aspect — `AGENT`,
  `HUMAN`. Function values are not gated here: a function speaks by
  `RETURNS` (§6). `BY` is refused on a MEASUREMENT aspect — measurements
  are never glossed.
- `DETECTOR` names a function without `RETURNS` (§6) that examines the
  values and returns band + score.
- Both clauses are optional, but not together: a test names `BY`, or
  `DETECTOR`, or both. On a MEASUREMENT aspect only the detector form is
  possible — judgment applied to a measurement's output, as in the second
  example.
- `THRESHOLD` (0..1) is the entropy cutoff used by the collapsed
  `GLOSSARY()` read (§5.3).

### 7.2 Test results

```sql
SELECT * FROM TESTS(orders.amount::behavior);
SELECT subject, band FROM TESTS(fin.trial_balance) WHERE band = 'red';
```

The **standard test-results schema** is fixed:
`(subject, aspect, test, band, score, computed_at, current, error)` —
`band` in `green | yellow | orange | red | error`, `score` the
disagreement/entropy in 0..1, `current` whether every function value
the result read holds at the read's pin, `error` null except where
`band` is `error`.
A detector's query returns the authored third of it — one
`(subject, band, score)` row per subject — and the engine completes
each row with the test, its aspect, and its own clock.
A detector that cannot answer — unresolved from the reading dataset,
a body that is not one SQL query, a plan or run the engine refuses —
refuses nothing: the failure is served as that test's result, one
row per subject holding values, `band` `error` with `score` 0 and the
failure text in `error`. An error is not a judgment: the §5.3
collapse never withholds on it, and every other test still speaks.
Detectors run **at read**. A result belongs to its **test**, not to
its detector: one detector serving three tests holds three results,
computed from each test's own values against its own threshold.
Detail lives in the value function's own output, reachable by
SELECT. Sweeps ("all contested
behavior columns") are WHERE clauses over the test-results relation, never a
special form; with no argument, `TESTS()` sweeps the `USE`'d dataset.
`subject::aspect` — the host's cast spelling — narrows test results to one
declared aspect, unambiguously: `fin.trial_balance` names a table,
`fin::reconciliation` an aspect across the dataset.

Judgment lives here — in detector functions and read policy — never in
results: no construct writes a result into data.

## 8. Skills

Agents use the language through skills; agents are not part of the grammar.
The interface tells, skills teach: the one surface
takes statements and returns outcomes, and everything an agent must *learn*
ships as skills sourced from this repository's artifacts — the language
(this document, `grammar.ebnf`), the flows (corpus fixtures 11 and 12),
and function authoring (the reference
library). Everything *live* — declared functions, the
glossary, the tables — is read through the language, never taught.

## 9. Open

One open question, fixture 09's remaining half: whether agents actually
compose their context from the reads — sweeping `state != 'current'`,
respecting bands — now that the collapsed shape discloses every gap
(§5.3: serving wrong information is not an experiment). Closes by
running the agent experiment, not by argument.

Second: the polymorphic reference — a column whose target table
switches by a sibling type column has no declarable edge, and the
skill says declare none. The candidate form is the conditioned edge,
one declaration per arm: `DECLARE RELATIONSHIP
stock_movements.source_document -> purchase_orders.id WHEN
source_type = 'purchase_order'`. It buys per-arm coherence and an
axis through an arm; it costs the predicate riding every
relationship consumer — a declared join stops being a plain equijoin.
And the word shifts: the aspect `WHEN` (§5.1) names a sibling
aspect's judged value, this one names a data column's row value.
Closes by transcription against a real polymorphic artifact, never by
argument.

Third: what binds a call to a dataset. `USE` binds the statements
after it and expires with the call (§3); a dataset's interface opens the
call bound, and the workspace interface opens it unbound. Whether the
workspace interface's `USE` earns its place once agents work at dataset
interfaces closes by counting over the agent record — dataset work done
under `USE` at the workspace interface against work at a dataset's interface —
never by argument.

PoC notes: batch visibility comes from (long-running) transactions — the
running system's run_id + snapshot-head pointer is the verbose version of
the same guarantee · the actor rides the transport — the connection,
or an interface-asked answer, recorded through the interface with human standing ·
value-at-read: a VIEW aspect's
value is its view — `FROM dso`, or `FROM fin.dso` — the collapsed
current metric definition expanded at plan time, nesting allowed behind a
cycle guard; running stays the reader's act: one serving interface over
every VIEW gloss, whatever flavor `x-kind` names.

Deferred, not under discussion: access rights · portability · persistence
backend and engine mapping.
