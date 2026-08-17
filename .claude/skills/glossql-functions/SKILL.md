---
name: glossql-functions
description: Write a function for a glossql workspace — measurements (RETURNS an aspect, body is SQL the engine plans), check voices, and detectors (no RETURNS, body is a rhai script over slots). Use when a shipped measurement does not fit this dataset, when a validation needs its measuring half, or when debugging a function that abstains.
---

# Writing functions

A function is declared over the door and run by the server. Two roles,
told apart by shape, and the shape also picks the body's language:

- **A measurement or a voice** — it `RETURNS` an aspect, and its output
  is validated against that aspect's schema. Its body is **one SQL
  query** the engine plans and runs (read-only, same doors as any
  statement). Filling a MEASUREMENT aspect makes it a measurement;
  filling a FACT aspect makes it a **voice** in that aspect's slots,
  beside the agent's and the human's. The check half of a validation is
  a voice. (Shipped bodies not yet rewritten to SQL still run as rhai
  scripts — read them as history, write new measurements as SQL.)
- **A detector** — no `RETURNS`. A rhai script named in a witness's
  `DETECTOR` clause; it sees the slots and never the table data.

Normative prose: SPEC.md §6 (functions) and §7 (witnesses, detectors).

## Read the closest one first

Fifteen reference scripts ship in every workspace and the body is a
column, so the library is readable through the door:

```sql
SELECT name, returns FROM functions ORDER BY name
```

```sql
SELECT script FROM functions WHERE name = 'rate_tolerance'
```

Read the one nearest your task before writing:

- `profile` — a measurement whose body is SQL over the engine's
  `profile` aggregate; the shape every new measurement takes.
- `outliers` — a measurement chained on another through `ACCEPTS`.
- `slot_entropy` — a detector.
- `rate_tolerance` — a detector over slot voices (authored expectation
  against check voice), which is the usual validation shape.

## Declaration

The body rides the statement. There is no path and no file: an agent
over the door has statements, so a function is written the way
everything else is (ruled 2026-08-15, fixture 24). A measurement's body
is one SQL query:

```glossql
DECLARE FUNCTION ap_settles_in_full_check FOR fin AS $$
  SELECT
    'a receipt settles its invoice in full; a short receipt is the exception' AS outcome,
    CASE WHEN count(*) = 0 THEN 0.0
         ELSE CAST(count(*) FILTER (WHERE settled < billed) AS DOUBLE) / count(*)
    END AS breach_rate
  FROM ar_settlement
$$ RETURNS ar_settles_in_full;
```

- `FOR` scopes to a dataset, or `GLOBAL`.
- `RETURNS` names the aspect the output fills, validated against that
  aspect's JSON Schema at extraction.
- `ACCEPTS` names aspects whose current values arrive as `context` — a
  script-body mechanism. A SQL body composes inline instead: another
  measurement's landed value is a read over `measurements`, and a
  statistic it needs is the same aggregate, computed in place.
- The body composes anything a read can: tables, `read.<aspect>()`
  groundings, the declaration relations as plain tables, and the
  shipped aggregates (`profile`, `mad`, `entropy` beside the engine's
  own). `$subject` arrives as a string literal wherever the body writes
  it; `subject_column($subject)` is the subject's column as a relation
  named `v`, for column-grain functions whose body cannot know the
  column's name.
- The result lands by a fixed rule: one row × one column → the value;
  one row → an object of its columns; many rows → an array of row
  objects. NULL keys are omitted. Size it like a claim — something that
  wants to be a table is a read, not a measurement.
- A re-declare supersedes. The body cannot contain `$$` — it would
  close the statement early.

The name is the key, workspace-wide: there is one row per name, and a
re-declare **replaces** it — old measurements sit at pins that no longer
resolve, so the next extraction recomputes. Take a shipped
name only on purpose: the store holds one body per name, and the
library's own survives nowhere else in the workspace, so replacing
`profile` costs the tested one until a rebuild lands it again.
For a MEASUREMENT this is the only route (a second name
returning the same aspect is refused); for anything else, use your own
name.

## The script contract

Three constants are in scope; the script's last expression is its
result, a map that must serialize as JSON.

- `subject` — a string, `"table"` or `"table.column"`. Split it for SQL.
- `context` — a map. A measurement gets one entry per accepted aspect,
  by aspect name, and the entry is `()` when that aspect has no value
  yet. A detector gets `slots` (one per speaker, each with a `body`),
  `threshold`, and `subject`/`aspect`/`witness` to echo back.
- `db` — the door into the dataset. **SQL over more than one line goes
  in backticks, not quotes** — a `"…"` string cannot span lines in rhai
  and the declaration fails with `Open string is not terminated`, which
  names nothing useful. `db.query("sql")` returns a Table;
  `db.query_all([sql, …])` returns an array of Tables answered in
  order. **The door overlaps a batch below the seam**, so a fan-out of
  small queries — pair scans, probes — belongs in one `query_all`,
  never a sequential loop. Any SQL; determinism is your contract and
  the workspace is your boundary.

Two free functions handle stored text: `parse_json(s)` turns a stored
body (a gloss, a measurement) back into a map and errors on text that
is not JSON; `canonical_sql(s)` reads SQL as an identity — parse and
re-render, so whitespace and keyword case collapse while identifiers
survive. A body the parser cannot read falls back to whitespace
normalization, which is weaker, and that is the honest limit.

## Kernels

Zero-copy readers over query results. The compute-heavy half of a
measurement lives here, in Rust — **a script nesting loops over rows or
pairs should be reaching for a kernel instead.**

Table: `num_rows()`, `columns()`, `col(name)` → Col, `cell(name)` —
the first row's value as a string, `()` for NULL or no rows (the
one-row aggregate read; parse with rhai's `.parse_int()` /
`.parse_float()`).

Col: `dtype()` (the Arrow type name — a `LIMIT 0` query types a column
without scanning it), `count()`, `null_count()`, `distinct()`,
`entropy()` — exact Shannon entropy (nats) over the non-null
distribution by typed key — `min()`, `max()`, `sum()`, `mean()`,
`stddev()`, `percentile(p)`, `mad()`, `top_k(k)`, `len_stats()`,
`match_rate(regex)`, `parse_rate(sql_type)`, `value_at(i)`, `floats()`
— the whole column as floats through one Arrow cast, `()` for NULL.

Read numbers you will loop over with `floats()`, never
`value_at().parse_float()` per cell: `value_at` renders display
strings and a hot loop through it is interpreter-bound. **A score reads
exact scalars** (`entropy()`, `distinct()`), never `top_k` buckets —
top_k is a display cap, and a display cap must not become a statistics
cap.

Statistical kernels:

- `key_vec()` on a Col — its distinct values as sorted typed keys;
  `count()` and `matched(other)` on the result give set size and
  intersection by linear merge. Containment between two columns is
  `a.matched(b) / a_distinct`, never a per-pair SQL join.
- `pair_keys(c1, c2)` on a Table — two columns' rows as combined keys
  (both non-null), for composite domains.
- `reconcile(y_table, m_table, terms)` — the stock/flow discriminator
  over two grouped results (`e, b, yv` and `e, b, s_<term>…`, both
  `ORDER BY e, b`): conventions — each term and every ordered pair
  difference — as one matrix product, with residuals and voting gates
  per entity; returns `n_common` and per-convention summaries.
- `tabicl_bands(train_x, train_y, test_x, alphas, actual)` — one TabICL
  fit and read: train on rows of features (arrays of numbers), predict
  one test row, return `q` (a band value per alpha, in order) and `pit`
  — the quantile at which `actual` lands, 0..1. What it buys a script:
  given these examples, the corridor a new value would have to land in
  for nobody to be surprised, and where the actual fell. The model is
  native and loads once from the workspace's `weights/` directory; a
  fit needs at least 2 training rows, and scripts should require more
  (`metric_bands` uses 5).

## Abstain, never throw

When the subject does not fit or the inputs are not there, return a
fact:

- `#{applicable: false}` — the subject genuinely does not fit; a text
  column has no outliers. Readers stop trying.
- `#{applicable: false, missing_aspects: ["column_profile"]}` — an
  accepted aspect's context entry was `()`. Name every missing one: an
  abstention naming absent inputs is an answer, never landed, so the
  next extraction recomputes and heals once the producer has run.

Keep `applicable` in the aspect's `required`; `missing_aspects` rides
the schema's open remainder.

An abstention that is *starvation* rather than a mismatch is worth
saying out loud in the read-back — it is a finding about the data's
shape, not a failure of the function.

## Detectors

The output must satisfy the standard attest schema, which is
engine-owned and not authored: `subject`, `aspect`, `witness`, `band`
(green|yellow|orange|red), `score` (0..1), `computed_at`.

Judgment lives in detectors and in read policy, never in results — no
measurement writes a verdict into data.

```glossql
DECLARE WITNESS ap_settles_w ON ar_settles_in_full BY (AGENT, HUMAN)
  DETECTOR rate_tolerance THRESHOLD 0.0;
```

## A bespoke function is the installation's recorded thinking

A workspace-scoped function (`FOR <dataset>`) is the honest home for
method that is specific to this data. When a shipped measurement
starves on this dataset's shape — `behavior_evidence` on a schema whose
only edges are document-keyed, say — a function that encodes how the
question is decided *here* beats a pile of hand judgments: it is
versioned, re-runnable, and its abstentions stay as honest as the
library's.

## Running

```glossql
SELECT outliers() FROM orders.amount;
```

Extraction computes at the read's pin — the data and declarations the
statement resolved — and lands a `measurements` row; the same pin serves
it back, and any input moving recomputes. A body carrying a `summary`
object serves the summary alone — the full body reads back through
`GLOSSARY(subject::aspect)`, uncapped.
