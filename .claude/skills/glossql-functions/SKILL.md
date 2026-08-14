---
name: glossql-functions
description: Author rhai functions for a glossql workspace — measurements (RETURNS an aspect) and detectors (no RETURNS). The script contract (subject, context, db), the column kernels, and the abstention convention. Use when writing, changing, or debugging a .rhai function.
---

# Writing functions

A function is a rhai script registered with a declaration; the aspect
schema is its one validated contract. Normative prose: SPEC.md §6
(functions) and §7 (witnesses, the detector role). The reference
library under `crates/scripts/functions/` is the exemplar set — read
the one closest to your task before writing:

- `profile.rhai` — a measurement from column kernels.
- `outliers.rhai` — a measurement chained on another through `ACCEPTS`.
- `temporal.rhai` — a measurement built from `db.query` SQL.
- `slot_entropy.rhai` — a detector.
- `rate_tolerance.rhai` — a detector over slot voices (authored
  expectation vs check voice), the usual validation shape.

A workspace-scoped function (`FOR <dataset>`) is also the honest home
for **installation-specific method**: when a shipped measurement
starves on this dataset's shape (behavior_evidence on a schema whose
only edges are document-keyed, say), a bespoke function that encodes
how behavior is decided *here* beats a pile of hand judgments — it is
the installation's recorded thinking, versioned and re-runnable, and
its abstentions stay honest like the library's.

## Declaration

```glossql
DECLARE FUNCTION outliers FOR GLOBAL FROM 'functions/outliers.rhai'
  ACCEPTS (column_profile)
  RETURNS outlier_profile;
```

- `FOR` scopes to a dataset, or `GLOBAL`.
- `ACCEPTS` names the aspects whose current values arrive as context —
  settings are context, never call arguments; calls are always bare
  `f()`. It is also the invalidation edge: a new value for an accepted
  aspect deletes your cached results. The declaration relations
  `relationships` and `imports` may ride the list too, as invalidation
  edges only — no context entry arrives (read them through `db`), but
  a declared edge or a landed table kills your cache dataset-wide.
- `RETURNS` names the aspect the output fills; the output is validated
  against that aspect's JSON Schema at extraction.
- **No `RETURNS` declares a detector** — role by shape. Detectors are
  named in a witness's `DETECTOR` clause and never see table data.

## The script contract

Three constants are in scope; the script's last expression is its
result, a map that must serialize as JSON:

- `subject` — a string, `"table"` or `"table.column"`; split it for SQL.
- `context` — a map. For a measurement: one entry per accepted aspect,
  by aspect name; an entry is `()` when that aspect has no value yet.
  For a detector: `slots` (one per speaker, each with a `body`),
  `threshold`, plus `subject`/`aspect`/`witness` to echo back.
- `db` — the door into the dataset: `db.query("sql")` returns a Table;
  `db.query_all([sql, …])` returns an array of Tables, answered in
  order — the door overlaps the batch below the seam, so a fan-out of
  small queries (pair scans, probes) belongs in one `query_all`, never
  a sequential loop. Any SQL; determinism is your contract, the
  workspace your boundary.

Two free functions handle stored text:

- `parse_json(s)` — a stored body (a gloss, a cached value) back into a
  map; errors on text that is not JSON.
- `canonical_sql(s)` — SQL text as an identity: parse and re-render, so
  whitespace and keyword case collapse while identifiers survive. A
  body the parser cannot read falls back to whitespace normalization —
  weaker, and that is the honest limit.

## Kernels

Zero-copy readers on query results (authoritative list: the
registrations in `crates/scripts/src/lib.rs`).

Table: `num_rows()`, `columns()`, `col(name)` → Col,
`cell(name)` — the first row's value as a string, `()` for NULL or no
rows (the one-row aggregate read; parse with rhai's `.parse_int()` /
`.parse_float()`).

Col: `dtype()` (Arrow type name — a `LIMIT 0` query types a column
without scanning it), `count()`, `null_count()`, `distinct()`,
`entropy()` — exact Shannon entropy (nats) of the non-null value
distribution over typed keys, `min()`, `max()`, `sum()`, `mean()`,
`stddev()`, `percentile(p)`, `mad()`, `top_k(k)`, `len_stats()`,
`match_rate(regex)`, `parse_rate(sql_type)`, `value_at(i)`,
`floats()` — the whole column as floats via one Arrow cast, `()` for
NULL. Read numbers you will loop over with `floats()`, never
`value_at().parse_float()` per cell: `value_at` renders display
strings, and a hot loop through it is interpreter-bound. A score reads
exact scalars (`entropy()`, `distinct()`), never `top_k` buckets —
top_k is a display cap, and a display cap must not become a statistics
cap.

Statistical kernels — the compute-heavy halves of measurements live
here, in Rust; a script that finds itself nesting loops over rows or
pairs should be reaching for one instead:

- `key_vec()` on a Col — its distinct values as sorted typed keys;
  `count()` and `matched(other)` on the result give set size and
  intersection by linear merge. Containment between two columns is
  `a.matched(b) / a_distinct` — never a per-pair SQL join.
- `pair_keys(c1, c2)` on a Table — two columns' rows as combined keys
  (both non-null), for composite domains.
- `reconcile(y_table, m_table, terms)` — the stock/flow discriminator
  over two grouped results (`e, b, yv` and `e, b, s_<term>…`, both
  `ORDER BY e, b`): conventions (each term and every ordered pair
  difference) as one matrix product, residuals and voting gates per
  entity; returns `n_common` and per-convention summaries.
- `tabicl_bands(train_x, train_y, test_x, alphas, actual)` — one
  TabICL fit and read: train on rows of features (arrays of numbers),
  predict one test row, return `q` (a band value per alpha, in order)
  and `pit` — the quantile at which `actual` lands in the predicted
  distribution, 0..1. What it buys a script: given these examples,
  the corridor a new value would have to land in for nobody to be
  surprised — and where the actual fell. The model is native and
  loads once from the workspace's `weights/` directory; a fit needs
  at least 2 training rows (scripts should require more —
  `metric_bands.rhai` uses 5).

## Abstention

When the subject doesn't fit or the inputs aren't there, abstain —
return a fact, never throw:

- `#{ applicable: false }` — the subject genuinely doesn't fit (a text
  column has no outliers). Readers stop trying.
- `#{ applicable: false, missing_aspects: ["column_profile"] }` — an
  accepted aspect's context entry was `()`. Name every missing aspect;
  readers run the producers first. The cached abstention heals on its
  own: the dependency's landing invalidates it through the `ACCEPTS`
  edge.

Keep `applicable` in the aspect's `required`; `missing_aspects` rides
the schema's open remainder.

## Detectors

Output must satisfy the standard attest schema — engine-owned, not
authored: `subject`, `aspect`, `witness`, `band`
(green|yellow|orange|red), `score` (0..1), `computed_at`. Judgment
lives here and in read policy, never in results: no measurement writes
a verdict into data.

## Running

```glossql
SELECT outliers() FROM orders.amount;      -- computes once, then cached
DELETE FROM cache WHERE function = 'outliers';  -- force recomputation
```
