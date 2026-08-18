---
name: glossql
description: Speak glossql through the server's MCP door — the statement set, the shipped reads, the outcome shape, and the substrate's sharp edges. Use when reading or writing anything in a glossql workspace (datasets, sources, recipes, glosses, functions, witnesses).
---

# Speaking glossql

glossql is one SQL-shaped surface for a workspace's data *and* its
context. Data lands in tables; context is JSON attached to subjects
(`table` or `table.column`) under declared aspects. Two artifacts are
normative — read them, don't reconstruct them:

- `SPEC.md` — the language specification. §3 sources, recipes, tables;
  §4 subjects and relationships; §5 the glossary (aspects, glosses,
  reading); §6 the function library; §7 witnesses and attestation.
- `grammar.ebnf` — the machine-readable syntax.

Everything *live* — the declared vocabulary, the tables, the record —
is read through the language itself, never assumed.

## The door

One MCP tool, `glossql`. Its `statements` argument takes a statement or
a semicolon-separated sequence; the result is a JSON array, one outcome
per statement:

- a read — `{"columns": [{"name", "type"}, …], "rows": [...],
  "row_count": n, "truncated": bool}`. `columns` is the result's
  shape with engine types, present even when zero rows come back — a
  `LIMIT 0` rehearsal returns the schema, which is its whole point.
  Data rows are capped at the server's `--row-cap`; `truncated: true`
  means the result held more than shown — refine (aggregate, WHERE,
  LIMIT) instead of reading a capped result as complete. Metadata
  reads — `GLOSSARY()`, `ATTEST()`, and the store relations — sent as
  their own single statement are uncapped: the map arrives whole.
- a write — `{"affected": n}` or `{"done": true}`.
- a refused statement — a tool error whose text is the refusal. Read
  it; it names what was wrong, and in a sequence it names its place:
  `statement 2 of 7 refused: … — statement 1 landed; 3–7 not run`.
  What landed stayed landed; the rest was never attempted.

Who you are (agent or human) rides the connection — there is no BY
clause. `USE <dataset>;` picks the dataset and survives between tool
calls: the server keeps one session per actor.

## The statement set

| statement | does |
|---|---|
| `USE ops;` | pick the dataset for this session |
| `DECLARE DATASET ops SET (…);` | create a dataset |
| `DECLARE SOURCE erp SET (type: parquet, location: 'root');` | register a source; location is a root directory, globs belong in recipe SQL |
| `PROBE erp AS $$sql$$;` | run recipe-shaped SQL at the source, landing nothing |
| `DECLARE RECIPE work_orders ON ops FROM erp AS $$sql$$;` | land the table the SQL produces — the landed table is the typed table |
| `DROP TABLE work_orders;` | remove a table — refused while it holds data |
| `DECLARE RELATIONSHIP a.col -> b.col;` | declare a join edge (`<->` both ways); a composite endpoint is a tuple: `a.(x, y) -> b.(x, y)` |
| `DECLARE ASPECT name WITH $$json-schema$$ AS MEASUREMENT\|FACT\|QUERY [ON TABLE, COLUMN, … [WHEN aspect = 'value']];` | add to the vocabulary; the schema is the one validated contract; `ON` is the grain — the subject classes it speaks to (DATASET/TABLE/COLUMN/RELATIONSHIP/SOURCE, absent = all), and `unassessed` disclosure stays within it; `WHEN` narrows relevance to subjects whose sibling aspect carries the value (bounds disclosure, never writes); SOURCE-grain slots read and supersede across datasets |
| `GLOSS aspect ON subject AS $$json$$;` | speak a value into your slot |
| `SELECT … FROM GLOSSARY(subject);` | the collapsed context; `all => true` for every slot |
| `DECLARE FUNCTION f FOR ops\|GLOBAL AS $$body$$ [RETURNS aspect];` | register a function — with `RETURNS` the body is one SQL query the engine plans, without it a detector script; the body rides the statement, so `SELECT script FROM functions` reads the shipped library back as worked examples (`glossql-functions` teaches writing one) |
| `SELECT f() FROM work_orders.duration_min;` | extract — computes at the read's pin and lands a `measurements` row; the same pin serves the row back, any input moving makes a new pin and recomputes; a body carrying a `summary` object serves the summary alone (the cube, the profile) — the full body reads back via `GLOSSARY(subject::aspect)`, uncapped |
| `DECLARE WITNESS w ON aspect [BY (AGENT, HUMAN)] [DETECTOR f THRESHOLD x];` | admit speakers, wire adjudication |
| `SELECT … FROM ATTEST(subject \| ops::aspect);` | bands and scores; sweeps are WHERE clauses |

There is no ordering surface: send statements in the order you need
them, one call or several.

Schema-altering substrate DDL — `CREATE VIEW` included — is closed:
tables come from recipes, and a composite edge is declared as a tuple,
never cured through a view.

## Reading live state

Never guess at workspace state — read it through the language, where it
is always current. A fresh workspace is not empty: the measurement
library and the KPI kit (the semantic vocabulary — `meaning`, `role`,
`behavior`, `unit`, `dimension`, `entity`, and the rest — with their
witnesses) are declared at boot; read them back before declaring
anything.

- `SELECT * FROM glossary` / `aspects` / `witnesses` / `functions` /
  `measurements` / `imports` / `relationships` — the store's relations
  as plain tables (who said what; the declared vocabulary and its
  speaker gates; what was measured at which pin; source rows vs landed
  rows; the declared join edges).
- `GLOSSARY(subject)` — the collapsed read, columns
  `(subject, aspect, value, band, score, state)` with `state` in
  `current | stale | contested | unassessed`; a contested value is
  withheld, and absence is a visible row. **`all => true` is a
  different shape**: the raw slots,
  `(subject, aspect, kind, witness, actor, body, written_at)` — no
  `value`, no `state`; the winning voice is yours to read off the
  slots. Don't mix the two column sets: `value` belongs to the
  collapse, `body` to the slots.
- `ATTEST(…)` — `(subject, aspect, witness, band, score, computed_at)`,
  band in green/yellow/orange/red.
- ordinary SELECT over tables for the data itself.

**Shipped reads** — derived relations the binary carries, selectable
like any table, filters riding WHERE. One file behind each, shared by
the door, the app and these examples:

| read | serves |
|---|---|
| `workspace_next` | the nine surfaces this workspace can be extended through, what stands and what is open on each |
| `open_questions` | what stands open for a human to judge — the rows the door asks as forms |
| `ruling_entries` | the human's standing judgments, with `folded_in` |
| `ruling_conflicts` | one claim ruled two ways on different aspects |
| `owed` | what waits on an act: an unexecuted recipe approval, a formula newer than its materialization, a contested slot, a ruling awaiting its fold-in |
| `agent_assumptions` | every assumption you currently disclose |
| `metric_surfaces` | every declared metric with its latest cube month, move, axes and formula |
| `app_parts` | apps authored as glosses, one row per file (`glossql-apps` teaches writing one) |

A shipped name is reserved: it shadows a table *and* a CTE of the same
name, so don't name a CTE after one.

```sql
SELECT surface, how, stands, open FROM workspace_next ORDER BY open DESC
```

```sql
SELECT what, why, since FROM owed ORDER BY since DESC
```

```sql
SELECT aspect, key, stance, folded_in FROM ruling_entries ORDER BY written_at DESC
```

## The brief — start every session with it

Human answers land while you are away — through the door's question
forms or another session. Some govern immediately (the human slot
outranks at every read), some owe you an act. Before acting on
anything else, sweep what changed:

```glossql
SELECT subject, aspect, actor_id, written_at FROM glossary
WHERE actor_kind = 'human' ORDER BY written_at DESC LIMIT 20;
SELECT subject, aspect FROM GLOSSARY(ops) WHERE state = 'contested';
SELECT subject, band, score FROM ATTEST(ops) WHERE band = 'red';
```

The brief the door serves at connect counts what stands — human
writings, approvals awaiting your re-declare, rulings awaiting your
fold-in, and **judgment questions** (assumptions below full
confidence — conventions and definitions the data cannot arbitrate).
It also rides any tool result whose call moved it, as a
`brief: Live now: …` block — so mid-session changes reach you
without reconnecting; a call that carries no brief block changed
nothing.
While that count is above zero, sweep the round. Forms ride record
reads: a call that reads the
glossary — `GLOSSARY()`, `ATTEST()`, the store relations — and
writes nothing carries one question form; landing calls and plain
data reads run uninterrupted. So the sweep is exactly the brief's
own reads, repeated until the round stays quiet.

An answer lands as a **ruling**: the judgment
alone — confirmed or corrected, naming the claim by its `key` — in
the human's `ruling` slot on the subject, never a copy of your body.
A ruling holds its question closed and the round moves on; your
grounding stays yours. Questions derive from *your current body*, so
raising a confidence with a measurement basis closes its question on
its own. A human's decline rests only while the workspace holds
still — your next write re-opens it for the next review. A client
without question forms gets nothing — relay the open questions in
chat yourself, multiple choice with your grounds, and run the
statement the answer names.

What stands open is a read, and you can see it yourself:

```sql
SELECT aspect, dimension, key, assumption, conf
FROM open_questions ORDER BY conf ASC;
```

`open_questions` is the derivation itself, not a summary of it — the
same rows the forms serve and the app's docket renders. Filter it like
any table (`WHERE aspect = 'cycle_time'`); order it where you read it,
since a read carries no ordering of its own. `ruling_entries` is what
the human has ruled; both build on it.

**One key ruled two ways is yours to reconcile.** `ruling_conflicts`
reports a claim confirmed on one aspect and corrected on another.
Nothing asks you about it and nothing resolves
it: read the rows, decide whether the aspects genuinely differ, and
record the reconciliation in the groundings themselves. Folding both
in literally is how a ruled component ends up contradicting the metric
that composes it.

**Every disclosed assumption carries a `key`** — a short slug you
write at disclosure (`business-days-only`, `completed-only`). The key
is the claim's identity and the only thing the record joins on:
rulings, question closure, and the fold-in debt all match
`(aspect, key)`. Assumption prose is what the human reads, never what
the system compares — no wording is ever matched against wording
anywhere in this system, and none ever will be. What that costs you,
stated plainly:

- **An assumption without a key is never asked.** It cannot be held
  closed, so the round would re-ask it forever; it is skipped
  instead. Your record shows it, no human is ever served it.
- **The same claim under two different keys reads as two claims.**
  Nothing detects it. If you disclose one decision on two aspects,
  use one key for it — or better, declare the shared concept as its
  own metric and compose both from it, so the decision lives in one
  place.
- **Dropping a key from your body clears its debt.** The claim is no
  longer disclosed, so nothing stands below full confidence. Drop a
  key only when you truly no longer rest on the claim.

Then close what owes an act, in the same session:

- **A ruling awaiting its fold-in** (the brief counts these):
  re-record the ruled grounding — the ruled assumption **under the
  same key**, at confidence 1.0 with `basis: "human-ruled"`, or
  re-grounded per the correction note in the ruling. The debt clears
  the moment your current body carries that key at full confidence;
  until then the ruling keeps the question closed for you both. Keep
  the key and rewrite the prose as freely as the correction requires
  — the join is on the key alone. **Fold in every standing ruling
  before re-reading the cube or the walk** — each grounding write moves
  the pin, so one batch of fold-ins then one recompute, never a
  recompute per ruling. Read the ruling notes as
  you fold: a note naming a sibling aspect ("differs from … by
  design", or a slip re-ruled) is the human's cross-aspect judgment —
  carry it into the grounding's assumption text.
- **A human formula answer newer than the metric's recorded gloss**:
  the two are one definition in two forms — re-record the
  materialization to match (or carry the difference as a disclosed
  assumption). Until you do, `read.<metric>()` serves the old SQL and
  the app shows the answer as waiting on you.
- **An approved `recipe_change`** (a human gloss carrying
  `{table, sql, reason}`): run the `DECLARE RECIPE` it approves — the
  approval is data, the act is yours.
- **A contested slot**: re-ground and re-judge as the contest section
  below says.
- **Human slots over your own**: read each back and re-compose what
  you still hold on top of the human's ruling — their word governs.

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
| the app's series and slices | `metric_cube()`, served by `metric_series()` |
| which rows look wrong, on a signal | `misfit.<frame>()` |
| whether an authored expectation holds | a check function's voice + `rate_tolerance`, read via `ATTEST()` |

What remains askable after the map is walked is exactly what the
round serves: an assumption whose basis is your judgment, held below
full confidence. The round enforces the boundary: it
never serves an assumption whose `dimension` is `behavior`, `sign`,
or `grain` — those are the functions' work, so record them at 1.0
citing the measurement. When a measurement *abstains*, the
abstention names why — close the claim on your strongest remaining
ground (a mirror table, a reference system, the data's own shape) and cite it;
relay it as a judgment question only if it stays load-bearing, never
as a raw "which is it?".

And the round is one of two registers, for one kind of interaction
only. **Prose shapes the work; forms rule the record.** Anything
that decides what the work *is* — the dataset's topic, which metrics
to build, whether to widen the import — is conversation: stop,
present the facts, propose in prose, interpret the human's prose.
The round's forms carry only standing assumptions to confirm or
correct — they work because confirming a stated judgment with the
facts on the table is easy, and they fail as a substitute for
conversation because there is nothing standing to confirm yet.

## Confidence means the number

Wherever a writing carries `confidence` (grounding assumptions are
the main carrier), one scale governs, anchored to the evidence
behind the number:

- **1.0** — ruled by a human, or verified by a named measurement or
  check. Nothing else.
- **~0.9** — independent evidence converges: a measurement plus a
  conventions gloss plus the data's own shape. A well-argued
  convention choice tops out here.
- **~0.7** — one source: a single measurement, or your reading of
  names and values.
- **0.5 and below** — a default you adopted to proceed. Exactly what
  the question round exists to surface.

Confidence is evidence, never a gate: nothing routes on it
mechanically — the round orders by it (lowest first) and every
assumption below 1.0 stays askable. An inflated number empties the
human's queue falsely; a deflated one wastes their attention.

State ambiguity plainly. The reader is a capable engineer: when a
verdict is ambiguous, name the readings you saw, which you took, and
why — in the report's front matter, not softened or buried. An
honest "two readings survive, I took A because B breaks the grain
check" is worth more than fluent certainty.

## What will bite

Postgres reflexes that fail at this pin, collected from real refusals:
`percentile_disc` and `mode()` (absent) · `to_char` PG patterns
(Chrono only) · 3-arg `date_trunc` with timezone · `date_add` /
`date_sub` / `age` · `SELECT * EXCLUDE` · `generate_series` in the
SELECT list (FROM clause or `unnest`) · window inheritance ·
`information_schema` (off — the glossary is the discovery surface, and
richer) · `lag` as "previous period" (previous *row*) · window
`last_value` as "partition's last" (frame-relative) · weekly
`date_bin` on Monday (Thursday without an origin) · a `LIKE` guard
before a `CAST` in the same WHERE (conjuncts reorder — only
`try_cast` is safe on dirty text) · aliasing a projection to its own
qualified source name (`round(j.x, 2) AS x`) · **two unaliased scalar
subqueries in one projection** ("Projections require unique expression
names" — alias both, or compute in a CTE).

Two more, specific to reads: an inner `ORDER BY` does not survive a
derived relation, so order where you consume; and a correlated
`NOT EXISTS` over a read that extracts JSON defeats decorrelation —
use a LEFT JOIN and a count instead.

## When a slot contests

`state = 'contested'` means voices differ on one slot and the
detector's score crossed the witness threshold — the value is withheld,
never adjudicated for you. Read the slots
(`GLOSSARY(subject, all => true)`), re-ground the question in the data,
and re-gloss only if the evidence moved you: your new gloss supersedes
your old one, and converged voices turn the band green. If the evidence
still says you were right, leave the slot contested — a human closes
it by conceding in their own slot. (Closure by striking a slot —
`DELETE FROM glossary WHERE …` — is parked until the substrate can
remove rows, iceberg-rust 0.11; the statement refuses and names this.)
Never change a gloss just to end a contest.
