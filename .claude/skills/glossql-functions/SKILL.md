---
name: glossql-functions
description: Write a rhai function for a glossql workspace — measurements (RETURNS an aspect), check voices, and detectors (no RETURNS). The declaration carries the body, the script contract is subject/context/db, and the column kernels do the compute. Use when a shipped measurement does not fit this dataset, when a validation needs its measuring half, or when debugging a function that abstains.
---

# Writing functions

A function is a rhai script the server runs over the workspace. Two
roles, told apart by shape:

- **A measurement or a voice** — it `RETURNS` an aspect, and its output
  is validated against that aspect's schema. Filling a MEASUREMENT
  aspect makes it a measurement; filling a FACT aspect makes it a
  **voice** in that aspect's slots, beside the agent's and the human's.
  The check half of a validation is a voice.
- **A detector** — no `RETURNS`. Named in a witness's `DETECTOR`
  clause, it sees the slots and never the table data.

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

- `profile` — a measurement built from column kernels.
- `outliers` — a measurement chained on another through `ACCEPTS`.
- `temporal` — a measurement built from `db.query` SQL.
- `slot_entropy` — a detector.
- `rate_tolerance` — a detector over slot voices (authored expectation
  against check voice), which is the usual validation shape.

## Declaration

The body rides the statement. There is no path and no file: an agent
over the door has statements, so a function is written the way
everything else is (ruled 2026-08-15, fixture 24).

```glossql
DECLARE FUNCTION ap_settles_in_full_check FOR fin AS $$
  let m = db.query("SELECT count(*) FILTER (WHERE settled < billed) AS short,
                           count(*) AS n FROM ar_settlement");
  let n = m.cell("n").parse_float();
  #{
    "outcome": "a receipt settles its invoice in full; a short receipt is the exception",
    "breach_rate": if n > 0.0 { m.cell("short").parse_float() / n } else { 0.0 }
  }
$$ ACCEPTS (imports) RETURNS ar_settles_in_full;
```

- `FOR` scopes to a dataset, or `GLOBAL`.
- `ACCEPTS` names the aspects whose current values arrive as context —
  settings are context, never call arguments; calls are always bare
  `f()`. It is also the **invalidation edge**: a new value for an
  accepted aspect deletes your cached results. The declaration
  relations `relationships` and `imports` ride the list as edges only —
  no context entry arrives, you read them through `db`, but a declared
  edge or a landed table kills the cache dataset-wide.
- `RETURNS` names the aspect the output fills, validated against that
  aspect's JSON Schema at extraction.
- A re-declare supersedes and recompiles. The body cannot contain
  `$$` — it would close the statement early.

## The script contract

Three constants are in scope; the script's last expression is its
result, a map that must serialize as JSON.

- `subject` — a string, `"table"` or `"table.column"`. Split it for SQL.
- `context` — a map. A measurement gets one entry per accepted aspect,
  by aspect name, and the entry is `()` when that aspect has no value
  yet. A detector gets `slots` (one per speaker, each with a `body`),
  `threshold`, and `subject`/`aspect`/`witness` to echo back.
- `db` — the door into the dataset. `db.query("sql")` returns a Table;
  `db.query_all([sql, …])` returns an array of Tables answered in
  order. **The door overlaps a batch below the seam**, so a fan-out of
  small queries — pair scans, probes — belongs in one `query_all`,
  never a sequential loop. Any SQL; determinism is your contract and
  the workspace is your boundary.

Two free functions handle stored text: `parse_json(s)` turns a stored
body (a gloss, a cached value) back into a map and errors on text that
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
  accepted aspect's context entry was `()`. Name every missing one:
  readers run the producers first, and the cached abstention heals by
  itself when the dependency lands, through the `ACCEPTS` edge.

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

First run computes and caches; later selects read the cache. A body
carrying a `summary` object serves the summary alone — the full body
reads back through `GLOSSARY(subject::aspect)`, uncapped.

Force recomputation at the WHERE clause's grain:

```glossql
DELETE FROM cache WHERE function = 'outliers';
```
