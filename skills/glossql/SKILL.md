---
name: glossql
description: Speak glossql through the server's MCP door — the statement set, the shipped reads, the outcome shape, and the substrate's sharp edges. Use when reading or writing anything in a glossql workspace (datasets, sources, recipes, glosses, functions, witnesses).
---

# Speaking glossql

glossql is one SQL-shaped surface for a workspace's data *and* its
context. Data lands in tables; context is JSON attached to subjects
(`table` or `table.column`) under declared aspects. Two artifacts are
normative — read them, don't reconstruct them:

- `doc://SPEC.md` — the language specification, a resource on this
  door. §3 sources, recipes, tables; §4 subjects and relationships;
  §5 the glossary (aspects, glosses, reading); §6 the function
  library; §7 witnesses and attestation.
- `doc://grammar.ebnf` — the machine-readable syntax, likewise.

Everything *live* — the declared vocabulary, the tables, the record —
is read through the language itself, never assumed.

## The door

One MCP tool, `glossql`. Its `statements` argument takes a statement or
a semicolon-separated sequence; the result is a JSON array, one outcome
per statement:

- a read — `{"columns": [{"name", "type"}, …], "rows": [...],
  "row_count": n, "truncated": bool}`. `columns` is the result's
  shape with engine types, present even with zero rows — a `LIMIT 0`
  rehearsal returns the schema. Data rows are capped at the server's
  `--row-cap`; `truncated: true` means the result held more than
  shown — refine (aggregate, WHERE, LIMIT) instead of reading a
  capped result as complete. Metadata reads — `GLOSSARY()`,
  `ATTEST()`, and the store relations — sent as their own single
  statement are uncapped.
- a write — `{"affected": n}` or `{"done": "…"}`. One write answers
  with rows instead: a `GLOSS` on a QUERY aspect — a metric's
  grounding — returns the metric's fact row in the `metric_axes()`
  shape, at the pin the write moved to: whether the SQL plans and a
  served date column is judged (`applicable`, `reason`), the verb and
  where it came from (`behavior`, `behavior_basis`), the axes admitted
  (`dims`), and every served column not admitted with the act that
  admits it (`unadmitted`, `unadmitted_why`). Read it before the next
  write: it is the check, and `metric_axes()` says the same later.
- a refused statement — a tool error whose text is the refusal. Read
  it; it names what was wrong, and in a sequence it names its place:
  `statement 2 of 7 refused: … — statement 1 landed; 3–7 not run`,
  with a second block `{"landed": [...]}` carrying the outcomes of
  the statements that stood, in the usual shape. What landed stays
  landed; the rest never ran.

Who you are (agent or human) rides the connection — the door's token
says which. There is no BY clause and nothing to declare.

The dataset does not ride the connection. **Open every call that
touches a dataset's names with `USE <dataset>;`.** It moves the
statements after it and expires when the call does, so it belongs in
each call, not just the first. A call that names none is
workspace-scoped, which is what `SELECT * FROM datasets` and a
source-grain gloss both want. A qualified `dataset.table.column`
reads across datasets from anywhere.

## The statement set

| statement | does |
|---|---|
| `USE ops;` | bind the statements after it in this call to a dataset — every call needs its own |
| `DECLARE DATASET ops SET (…);` | create a dataset |
| `DECLARE SOURCE erp SET (type: parquet, location: 'root');` | register a source; location is a root directory, globs belong in recipe SQL; a file `type` describes the export — the recipe's `read_parquet`/`read_csv`/`read_json` picks the reader |
| `PROBE erp AS $$sql$$;` | run recipe-shaped SQL at the source, landing nothing |
| `DECLARE RECIPE work_orders ON ops FROM erp AS $$sql$$;` | land the table the SQL produces — the landed table is the typed table |
| `DROP TABLE work_orders;` | remove a table — refused while it holds data |
| `DECLARE RELATIONSHIP a.col -> b.col;` | declare a join edge (`<->` both ways); a composite endpoint is a tuple: `a.(x, y) -> b.(x, y)`; both endpoints must be landed columns — the refusal lists what is |
| `DECLARE ASPECT name WITH $$json-schema$$ AS MEASUREMENT\|FACT\|QUERY [ON TABLE, COLUMN, … [WHEN aspect = 'value']];` | add to the vocabulary; the schema is the one validated contract; `ON` is the grain — the subject classes it speaks to (DATASET/TABLE/COLUMN/RELATIONSHIP/SOURCE, absent = all), and `unassessed` disclosure stays within it; `WHEN` narrows relevance to subjects whose sibling aspect carries the value (bounds disclosure, never writes); SOURCE-grain slots read and supersede across datasets |
| `GLOSS aspect ON subject AS $$json$$;` | speak a value into your slot; an aspect ON TABLE or ON COLUMN takes only a landed table or column; on a QUERY aspect the outcome is the metric's fact row (the `metric_axes()` shape, above) |
| `SELECT … FROM GLOSSARY(subject);` | the collapsed context; `all => true` for every slot |
| `DECLARE FUNCTION f FOR ops\|GLOBAL AS $$body$$ [RETURNS aspect];` | register a function — with `RETURNS` the body is one SQL query the engine plans, without it a detector script; the body rides the statement, so `SELECT script FROM functions` reads the shipped library back as worked examples (`glossql-functions` teaches writing one) |
| `SELECT f() FROM work_orders.duration_min;` | extract — computes at the read's pin and lands a `measurements` row; the same pin serves the row back, any input moving makes a new pin and recomputes; the outcome's `computed` column says which happened (false: the recorded row served, its `computed_at` the earlier run's); a body carrying a `summary` object serves the summary alone (the profile) — the full body reads back via `GLOSSARY(subject::aspect)`, uncapped |
| `DECLARE WITNESS w ON aspect [BY (AGENT, HUMAN)] [DETECTOR f THRESHOLD x];` | admit speakers, wire adjudication |
| `SELECT … FROM ATTEST(subject \| ops::aspect);` | bands and scores; sweeps are WHERE clauses |

There is no ordering surface: send statements in the order you need
them, one call or several.

Schema-altering substrate DDL — `CREATE VIEW` included — is closed:
tables come from recipes, and a composite edge is declared as a
tuple, not through a view.

## Reading live state

Never guess at workspace state — read it through the language. A
fresh workspace is not empty: the measurement
library and the KPI kit (the semantic vocabulary — `meaning`, `role`,
`behavior`, `unit`, `dimension`, `entity`, and the rest — with their
witnesses) are declared at boot; read them back before declaring
anything.

- `SELECT * FROM glossary` / `aspects` / `witnesses` / `functions` /
  `measurements` / `imports` / `relationships` — the store's relations
  as plain tables (who said what; the declared vocabulary and its
  speaker gates; what was measured at which pin; source rows vs landed
  rows; the declared join edges). `glossary`, `imports`,
  `relationships` and `measurements` carry a `dataset` column and serve
  the whole workspace — `USE` does not narrow them, so say which
  dataset you mean. The rest are workspace vocabulary and have no
  dataset to narrow to.
- `GLOSSARY(subject)` — the collapsed read, columns
  `(subject, aspect, value, band, score, state)` with `state` in
  `current | stale | contested | unassessed`; a contested value is
  withheld, and absence is a visible row. **`all => true` is a
  different shape**: the raw slots,
  `(subject, aspect, kind, witness, actor, body, written_at, current)`
  — no `value`, no `state`; the winning voice is yours to read off the
  slots, and `current` is false for a function voice landed before the
  last write (served and marked; re-run it to refresh). Don't mix the two column sets: `value` belongs to the
  collapse, `body` to the slots.
- `ATTEST(…)` — `(subject, aspect, witness, band, score, computed_at,
  current, error)`, band in green/yellow/orange/red, or `error` when
  the detector itself could not answer — the failure text rides
  `error`, nothing is withheld, and the fix is the detector's SQL;
  `current` false when a voice the verdict read was landed before the
  last write.
- ordinary SELECT over tables for the data itself.

**Shipped reads** — derived relations the binary carries, selectable
like any table, filters riding WHERE:

| read | serves |
|---|---|
| `workspace_next` | the surfaces this workspace can be extended through, what stands and what is open on each |
| `open_questions` | what stands open for a human to judge — the rows the door asks as forms |
| `ruling_entries` | the human's standing judgments, with `folded_in` |
| `owed` | what waits on an act: an unexecuted recipe approval, a formula newer than its materialization, a contested slot, a ruling awaiting its fold-in |
| `agent_assumptions` | every assumption you currently disclose |
| `metric_surfaces` | every declared metric with its unit, meaning, formula and whether it is grounded — the record; the cube's numbers are `metric_series()` and `metric_axes()` |
| `band_points()` | the recorded `metric_bands` walk as rows, one per metric per month, with each point's displacement — which metric and which month a red band verdict rests on, without re-running the walk |
| `source_files('erp')` | every file under a declared source's location — `path`, `size`, `modified` — what a recipe can name; needs no `USE` |
| `app_parts` | apps authored as glosses, one row per file (`glossql-apps` teaches writing one) |
| `current_dataset` | the dataset your `USE` bound, as a one-row relation — join it to narrow a read that answers for the whole workspace |

A shipped name is reserved: it shadows a table *and* a CTE of the same
name, so don't name a CTE after one. Every column of every read, and
the two cube reads, are `doc://docs/reference/reads.md`, served on
this door — open it before naming columns in a query; a guess costs
a refusal round trip.

`open_questions`, `ruling_entries` and `agent_assumptions` answer for
the whole workspace and carry a `dataset` column, so narrow them
yourself. `owed` narrows itself; `metric_surfaces` reads vocabulary,
which is workspace-wide.

```sql
SELECT surface, how, stands, open FROM workspace_next ORDER BY open DESC
```

```sql
SELECT what, why, since FROM owed ORDER BY since DESC
```

```sql
SELECT r.aspect, r.key, r.stance, r.folded_in FROM ruling_entries r
JOIN current_dataset d ON d.dataset = r.dataset ORDER BY r.written_at DESC
```

## The brief — the session's first read

Human answers land while you are away — through the door's question
forms or another session. Some govern immediately (the human slot
outranks at every read), some owe you an act. Read what changed once,
before the first write. `SELECT * FROM datasets` first: a workspace
with none has no brief to sweep — `owed`, `GLOSSARY(d)` and
`ATTEST(d)` all need a dataset — and `SELECT * FROM workspace_next`
is the whole of its live state; the metrics skill's landing page is
where it begins. With a dataset, `USE` it and read:

```glossql
SELECT subject, aspect, actor_id, written_at FROM glossary
WHERE actor_kind = 'human' ORDER BY written_at DESC LIMIT 20;
SELECT subject, aspect FROM GLOSSARY(ops) WHERE state = 'contested';
SELECT subject, band, score FROM ATTEST(ops) WHERE band = 'red';
SELECT what, why, since FROM owed ORDER BY since DESC;
```

The brief the door serves at connect leads with what is owed —
rulings awaiting your fold-in, approvals awaiting your re-declare,
and **judgment questions** (assumptions below full confidence —
conventions and definitions the data cannot arbitrate) — and closes
with the record's size: how many human writings stand and when the
latest landed. The first part is work; the second's timestamp tells
you something landed while you were away. The brief also rides any
tool result whose call moved it, as a
`brief: Live now: …` block — so mid-session changes reach you
without reconnecting; a call that carries no brief block changed
nothing.

A question is served once and then waits. Forms ride record reads: a
call that reads the glossary — `GLOSSARY()`, `ATTEST()`, the store
relations — and writes nothing carries one question form; landing
calls and plain data reads run uninterrupted, and nothing re-asks
until your next record read while the question stands. Waiting is not
a gate: the work goes on, the grounding stays yours, and the answer
lands as a ruling whether you are there or not — every grounding you
write with an assumption below 1.0 adds to the count, which is the
brief counting your disclosures, not an order to stop. A client
without question forms gets none — read what stands open yourself and
relay it in chat, multiple choice with your grounds, then run the
statement the answer names:

```sql
SELECT o.aspect, o.dimension, o.key, o.assumption, o.conf
FROM open_questions o JOIN current_dataset d ON d.dataset = o.dataset
ORDER BY o.conf ASC;
```

An answer lands as a **ruling** in the human's slot — the judgment
alone, naming the claim by its `key`, never a copy of your body. A
ruling holds its question closed; some rulings owe you an act, and the
brief counts them. What a ruling is, what each kind owes you (the
fold-in, an `unclear`, a formula answer, an approved recipe change, a
contested slot), why every assumption carries a `key`, and what the
confidence number means are `references/rulings.md` — open it the
moment the brief counts anything, and before disclosing your first
assumption.

## Measurements over-produce — you are the judge

Detection functions are tuned toward recall: they emit *candidates*
with evidence, never conclusions, and false positives are expected.
Reading a measurement is a judging job — verify each candidate against
the data itself, then declare or gloss only the survivors. The rejects
stay in the measurement, visible and undeclared; never delete them to
"clean up". Judgment lives in your reads, not in the function.

## Never ask a human for a statistic

A question the shipped
functions can settle is *your* work, and asking the human for it —
"is this a stock or a flow?" — is asking them to do statistics by
hand. The round and your chat relays carry judgment only: definitions,
conventions, business meaning, choices between readings. Before any
question leaves the workspace, walk this map and run what answers it:

| the question | what settles it |
|---|---|
| what values a column holds — range, nulls, distincts, top values | `profile()` |
| outliers on a numeric column | `outliers()` (profile first) |
| a date column's grain, span, gaps | `temporal()` |
| **stock or flow — may it be summed** | `behavior_evidence()` over declared edges; its anchors carry the verdict, alternatives, and Wilson support |
| **a sign convention** (source-signed vs natural) | the `sign` partition on a `behavior_evidence` anchor — primary/mirror counts, never column names |
| which columns join, and how well | `detect_relationships()`, then your anti-join judging; standing health is `relationship_coherence()` |
| whether a column derives from siblings (a = b × c) | `detect_derivations()` |
| which axes are worth slicing | `dimension_relevance()` (profiles first) |
| hierarchies inside dimensions | `detect_hierarchies()` |
| two metrics accidentally identical | `detect_grounding_collisions()` |
| whether a metric's month is surprising | `metric_bands()`, adjudicated by `band_breach` |
| the app's series and slices | `metric_series(grain => …)` — the cube, computed at read and cached, never landed; `metric_axes()` says what it admitted and, per served column, what keeps the rest out |
| which rows look wrong, on a signal | `misfit.<frame>()` |
| whether an authored expectation holds | a check function's voice + `rate_tolerance`, read via `ATTEST()` |

What remains askable after the map is walked is what the round
serves: an assumption whose basis is your judgment, held below full
confidence. The round enforces the boundary: it never serves an
assumption whose `dimension` is `behavior`, `sign`, or `grain` —
those are the functions' work, so record them at 1.0 citing the
measurement. When a measurement *abstains*, the abstention names why
— close the claim on your strongest remaining ground (a mirror
table, a reference system, the data's own shape) and cite it; relay
it as a judgment question only if it stays load-bearing, never as a
raw "which is it?".

The round is one of two registers. **Prose shapes the work; forms
rule the record.** Anything that decides what the work *is* — the
dataset's topic, which metrics to build, whether to widen the import
— is conversation: stop, present the facts, propose in prose,
interpret the human's prose. Forms carry only standing assumptions
to confirm or correct; they cannot replace conversation — there is
nothing standing to confirm yet.

The engine's refusals are exact and name what was wrong. Its own SQL
guide at this pin is served as `doc://vendor/datafusion/sql/…` — a
function's name or signature is a lookup there, never a guess — and
what fails here that the guide cannot say is `references/sql-here.md`,
served beside this page as `skill://glossql/<reference>`.
