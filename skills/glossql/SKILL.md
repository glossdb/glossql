---
name: glossql
description: Speak glossql through the server's MCP door — the statement set, the shipped reads, the outcome shape, and the substrate's sharp edges. Use when reading or writing anything in a glossql workspace (datasets, sources, recipes, glosses, functions, witnesses).
---

# Speaking glossql

glossql is one SQL-shaped surface for a workspace's data and its
context. Data lands in tables; context is JSON attached to subjects
(`table` or `table.column`) under declared aspects. The language is
`doc://SPEC.md` and `doc://grammar.ebnf` — read them, don't
reconstruct them. Every `skill://` and `doc://` name is a row of
`pages()` — `SELECT body FROM pages() WHERE uri = '…'` reads one — and
a resource for a client with a reader. Everything live — the
vocabulary, the tables, the record — is read through the language,
never assumed. The work itself — the seven goals and the judgment
inside each — is `skill://glossql-metrics/SKILL.md`.

## The door

One tool, `glossql`. Its `statements` argument takes a statement or a
semicolon-separated sequence; the result is a JSON array, one outcome
per statement:

- a read — `{"columns": [{"name", "type"}, …], "rows": [...],
  "row_count": n, "truncated": bool}`. `columns` stands even with
  zero rows — a `LIMIT 0` rehearsal returns the schema. `truncated:
  true` means the result held more than the cap: refine, never read a
  capped result as complete. `GLOSSARY()`, `ATTEST()` and the store
  relations are uncapped.
- a write — `{"affected": n}` or `{"done": "…"}`. One write answers
  with rows: a `GLOSS` on a QUERY aspect returns the metric's fact row
  in the `metric_axes()` shape — read it before the next write.
- a refused statement — a tool error whose text is the refusal. It
  names what was wrong and, in a sequence, its place: `statement 2 of
  7 refused: … — statement 1 landed; 3–7 not run`, with a
  `{"landed": [...]}` block for what stood. What landed stays landed.
- two closing lines on every result. `situation:` says whether your
  last statement landed or was refused, and for a grounding what its
  fact row said. `next:` names one act per goal — structure, metrics,
  slices, bands, checks, app, rulings — each with a link. To move a
  goal, read its link — `next://<dataset>/<goal>` as a resource, or
  `SELECT * FROM next WHERE surface = '<goal>'` through the tool — and
  send the statement it hands you, edited to your judgment; `<…>`
  marks what only you can fill. A blocked goal names what blocks it.
  None of it is an order: the goal is what the human asked for.

Who you are — agent or human — rides the connection; there is no BY
clause. The dataset rides the door: at a dataset's door
(`/<dataset>/mcp`) every call opens on it, and its tables and columns
resolve unprefixed. At the workspace door (`/mcp`) a call opens
unbound — **there, open every call that touches a dataset's names
with `USE <dataset>;`** — it moves the statements after it and
expires with the call. A call naming none is workspace-scoped, which
is what `SELECT * FROM datasets` wants; a qualified
`dataset.table.column` reads across datasets from anywhere.

## The statement set

| statement | does |
|---|---|
| `USE ops;` | bind the statements after it in this call to a dataset — the workspace door's way in; a dataset's door opens bound |
| `DECLARE DATASET ops SET (…);` | create a dataset |
| `DECLARE SOURCE erp SET (type: parquet, location: 'root');` | register a source; the location is a root — a directory on the server's machine, or an object-store URL it may read — and globs belong in recipe SQL; `type` describes the export, the recipe's `read_parquet`/`read_csv`/`read_json` picks the reader |
| `PROBE erp AS $$sql$$;` | run recipe-shaped SQL at the source, landing nothing |
| `DECLARE RECIPE work_orders ON ops FROM erp AS $$sql$$;` | land the table the SQL produces — the landed table is the typed table |
| `IMPORT work_orders;` | a data update: land the files the source holds new, as one more snapshot of the table; `unchanged` when there are none |
| `DROP TABLE work_orders;` | remove a table — refused while it holds data |
| `DECLARE RELATIONSHIP a.col -> b.col;` | declare a join edge (`<->` both ways); a composite endpoint is a tuple, `a.(x, y) -> b.(x, y)`; both endpoints must be landed columns |
| `DECLARE ASPECT name WITH $$json-schema$$ AS MEASUREMENT\|FACT\|QUERY [ON TABLE, COLUMN, … [WHEN aspect = 'value']];` | add to the vocabulary; the schema is the validated contract; `ON` is the grain — the subject classes it speaks to, absent = all; `WHEN` narrows relevance to subjects whose sibling aspect carries the value |
| `GLOSS aspect ON subject AS $$json$$;` | speak a value into your slot; an aspect ON TABLE or ON COLUMN takes only a landed one; a QUERY aspect is grounded on the dataset (`GLOSS revenue ON fin AS …`), and the outcome is the metric's fact row |
| `SELECT … FROM GLOSSARY(subject);` | the collapsed context, one row per aspect |
| `SELECT … FROM GLOSSARY(subject, all => true);` | every slot, raw; `all => true` is the call's second argument, never a WHERE condition |
| `DECLARE FUNCTION f FOR ops\|GLOBAL AS $$body$$ [RETURNS aspect];` | register a function — with `RETURNS` one SQL query the engine plans, without it a detector script; `SELECT script FROM functions` reads the shipped library back as worked examples (`glossql-functions` teaches writing one) |
| `SELECT f() FROM work_orders.duration_min;` | extract — computes at the read's pin and lands a `measurements` row; the same pin serves the row back, `computed` says which happened; a body with a `summary` serves the summary alone, the full body reads back via `GLOSSARY(subject::aspect)` |
| `DECLARE WITNESS w ON aspect [BY (AGENT, HUMAN)] [DETECTOR f THRESHOLD x];` | admit speakers, wire adjudication |
| `SELECT … FROM ATTEST(subject \| ops::aspect);` | bands and scores; sweeps are WHERE clauses |

There is no ordering surface: send statements in the order you need
them. Schema-altering substrate DDL — `CREATE VIEW` included — is
closed: tables come from recipes, and a composite edge is a tuple. A
body rides `$$…$$` as written — plain double quotes inside, nothing
escaped — and closes as `}$$;`.

## Reading live state

A fresh workspace is not empty: the measurement library and the KPI
kit — `meaning`, `role`, `behavior`, `unit`, `dimension`, `entity`,
and the rest, with their witnesses — are declared at boot; read them
back before declaring anything.

- The store's relations, as plain tables — these columns and no
  others; a guessed column costs a refusal, and `DESCRIBE <name>`
  serves the columns of any readable name, the shipped reads
  included: `glossary (dataset, subject, aspect, actor_kind,
  actor_id, body, written_at, snapshot_id)` · `aspects (name, kind,
  grains, condition, schema)` · `witnesses (name, aspect, speakers,
  detector, threshold)` · `functions (name, scope, script, returns)`
  · `measurements (dataset, function, subject, aspect, pin, value,
  computed_at, reads)` · `imports (dataset, table_name, source_scans,
  landed_rows, dropped_rows_count, cast_failures, imported_at)` ·
  `relationships (dataset, left_path, op, right_path)` · `sources
  (name, settings)` · `datasets (name, settings)`. The ones with a
  `dataset` column serve the whole workspace — the binding does not
  narrow them, so say which dataset you mean.
- `GLOSSARY(subject)` — the collapsed read, `(subject, aspect, value,
  band, score, state)`, `state` in `current | stale | contested |
  unassessed`; a contested value is withheld, and absence is a visible
  row. A subject is a bare path, never a string: `GLOSSARY(orders)`,
  `GLOSSARY(orders.amount)`, `GLOSSARY(fin::revenue)` for one aspect;
  `ATTEST(fin::revenue)` likewise. **`GLOSSARY(subject, all => true)` is a different shape**: the
  raw slots, `(subject, aspect, kind, witness, actor, body,
  written_at, current)` — no `value`, no `state`; `current` is false
  for a function voice landed before the last write. `value` belongs
  to the collapse, `body` to the slots.
- `ATTEST(…)` — `(subject, aspect, witness, band, score, computed_at,
  current, error)`, band in green/yellow/orange/red; `error` carries
  a detector's own failure, nothing withheld.
- the landed schema: `SELECT table_name, column_name, data_type FROM
  information_schema.columns WHERE table_name NOT LIKE '%$%'` serves
  every column the bound dataset mounted; `DESCRIBE <name>` serves
  one name.
- ordinary SELECT over tables for the data itself.
- names: an unquoted name folds to lowercase, a double-quoted one keeps
  its case. A column keeps the export's spelling and is reached by it
  either way — `joinedAt`, `PATIENT` and `users."joinedAt"` all reach
  the column, in a read and in a subject alike; only two columns that
  differ by case alone need the quotes. A table is reached as declared:
  `"SearchStream"` if it was declared quoted.

**Shipped reads** — derived relations selectable like any table,
filters riding WHERE. `DESCRIBE <read>` serves the columns of any of
them — `DESCRIBE band_points()`, `DESCRIBE metric_series(grain =>
'month')` — and every column's meaning is `doc://docs/reference/reads.md`;
a guessed column costs a refusal.

| read | serves |
|---|---|
| `workspace_next` | every surface, what stands and what is open — the counts behind the `next:` line |
| `open_questions` | what stands open for a human to judge — the rows the door asks as forms |
| `ruling_entries` | the human's standing judgments, with `folded_in` |
| `owed` | what waits on an act: a recipe approval, a formula answer, a contested slot, a measurement stale or never made, a ruling awaiting its fold-in |
| `agent_assumptions` | every assumption you currently disclose |
| `metric_surfaces` | every metric of the bound dataset — `name`, `title`, `kind`, `unit`, `meaning`, `formula`, `grounded`, `stopped` — the record; the numbers are `metric_series()` and `metric_axes()` |
| `metric_series(grain => 'month')` | the cube's cells — `metric`, `dimension` (`''` the total), `member`, `period`, `value`, `num`, `den`, `behavior`; the one argument is the grain, a metric is a WHERE filter — never `metric_series('name')` |
| `metric_axes()` | one row per grounding: `metric`, `applicable`, `judged_current`, `reason`, `behavior`, `behavior_basis`, `grain`, `resolution`, `window`, `dims`, `basis`, `admitted_by`, `axes_basis`, `bucketed`, `unadmitted`, `unadmitted_why`, `unadmitted_act`, `wanted`, `wanted_over`, `alternative`, `alternative_divergence`, `alternative_error`, `superseded_divergence`; takes no argument |
| `band_points()` | the recorded `metric_bands` walk, one row per metric and walked point — `seq`, `metric`, `applicable`, `reason`, `grain`, `aggregation`, `trained_on`, `axis`, `axis_judged`, `point_seq`, `period`, `actual`, `p05`, `p10`, `p50`, `p90`, `p95`, `pit`, `withheld`, `partial`, `displacement`, `computed_at`, `current`; no `band` and no `month` — a red is `WHERE partial = false ORDER BY displacement DESC` |
| `metric_sources()` | what feeds each grounding — `metric`, `field`, `source` (`table.column`), `table_name`, `reason`: per served field the column it descends from, and every table it scans |
| `source_files('erp')` | every file under a source's location — `path`, `size`, `modified`; needs no dataset |
| `app_parts` | apps authored as glosses, one row per file (`glossql-apps`) |
| `current_dataset` | the dataset the call is bound to, one row — join it to narrow a workspace-wide read |
| `pages()` | every page the door serves — `uri`, `title`, `body`; needs no dataset |

A shipped name is reserved: it shadows a table and a CTE of the same
name. `open_questions`, `ruling_entries` and `agent_assumptions` carry
a `dataset` column, so narrow them yourself; `owed` and
`metric_surfaces` narrow themselves.

```sql
SELECT r.aspect, r.key, r.stance, r.folded_in FROM ruling_entries r
JOIN current_dataset d ON d.dataset = r.dataset ORDER BY r.written_at DESC
```

## Questions and rulings

Human answers land while you are away, as **rulings** in the human's
slot — the judgment alone, naming your claim by its `key`, never a
copy of your body. The `rulings` goal on the `next:` line carries
every owed act — a fold-in, an approved recipe change, a formula
answer, a contested slot — and hands you the statement. A question is
served once, through the door's forms, and waits; the work goes on
and the grounding stays yours. A client without forms gets none: read
`open_questions` and relay it in chat, multiple choice with your
grounds, then run the statement the answer names. What a ruling is,
what each kind owes you, and what the confidence number means:
`references/rulings.md` — open it before disclosing your first
assumption.

## Measure, then judge — never ask a human for a statistic

Functions over-produce: candidates with evidence, never conclusions.
Verify each against the data and declare only the survivors; the
rejects stay in the measurement, visible and undeclared. A question
the functions settle — what a column holds, a date's grain, stock or
flow, which columns join, which axes matter — is your work, and
asking the human for it is asking them to do statistics by hand.
`references/settled.md` maps each question to the function that
settles it; walk it before any question leaves the workspace. A
measurement is called in the SELECT list with its subject in FROM —
`SELECT profile() FROM orders.amount` — never as a table function.

What remains askable is judgment: definitions, conventions, business
meaning, a choice between readings, held below full confidence. Prose
shapes the work — the topic, which metrics to build, whether to widen
the import — and forms rule the record: standing assumptions to
confirm or correct.

The engine's refusals are exact and name what was wrong. Its SQL
guide at this pin is `doc://vendor/datafusion/sql/…` — a function's
name or signature is a lookup there, never a guess — and what fails
here that the guide cannot say is `references/sql-here.md`.
