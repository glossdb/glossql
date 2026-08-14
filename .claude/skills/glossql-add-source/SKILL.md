---
name: glossql-add-source
description: Drive the add-source flow in a glossql workspace end to end — probe a declared source, author the typing recipe, land the table, run the measurement plane, and gloss every table and column into the shipped vocabulary. Use when connecting a new data source or landing a new table.
---

# Adding a source

The statement shapes below assume the `glossql` skill (the door, the
outcome shape). A fresh workspace already holds the measurement
library — `profile`, `outliers`, `temporal`, `behavior_evidence`,
`slot_entropy` and the rest arrive at boot with their aspects;
`SELECT * FROM functions` lists what is declared, and that read is
the authority, not this sentence.

**This is not ETL.** Probe and recipe are the filter: a dataset is a
curated working set for its topic (the onboard skill's stage 0),
never a mirror of the export. Land the tables the agreed topic and
cohort need; take only the columns the recipe's SELECT list earns;
filter a wide table in the recipe's WHERE. A table or column left
out costs one later `DECLARE RECIPE`; landing everything costs
attention on every flow after — more slots to gloss, more owed
claims, more noise between you and the questions that matter.

## 1. Dataset and source

```glossql
USE fin;
DECLARE SOURCE erp_export SET (type: parquet, location: 'lake/erp');
```

The location is a root directory. Globs and file paths belong in
recipe SQL, resolving under that root.

**Read the source's conventions before probing.** A source-grain
aspect's slots serve in every dataset, so what an earlier onboarding
learned about this system — placeholder dates, format warts, key
spellings — is already readable:

```glossql
SELECT value FROM GLOSSARY(erp_export) WHERE aspect = 'conventions';
```

And deposit what *you* learn the same way once it is confirmed — a
convention is a fact about the source system, not about this
dataset:

```glossql
GLOSS conventions ON erp_export AS $${
  "placeholder_date": "1900-01-01 stands for unset",
  "timestamp_format": "%b %e %Y %I:%M%p, month names mixed-language"
}$$;
```

Dataset-local evidence (orphan populations, grain verdicts) stays in
dataset glosses; only what the next export from the same system will
also carry belongs at source grain.

A relational source names a connection URI as its location and an ADBC
driver. The `driver:` value is the ADBC driver index **slug** — the
name the operator's install registered (`dbc install <slug>`) — or a
filesystem path to the driver library. Installing drivers is the
operator's job, not yours; the slugs: `bigquery`, `clickhouse`,
`databricks`, `datafusion`, `duckdb`, `exasol`, `flightsql`, `mssql`,
`mysql`, `postgresql`, `quack`, `redshift`, `singlestore`,
`snowflake`, `spark`, `sqlite`, `trino`.

```glossql
DECLARE SOURCE erp SET (type: relational_db,
                        driver: 'postgresql',
                        location: 'postgresql://host/erp');
```

Its probe and recipe SQL run **at the source**, in the source's own
dialect — `read_*` functions and `try_to_date`/`try_to_timestamp` do
not exist there; type with the backend's own casts. One SELECT per
statement; writes are refused at the door.

**The wire decides what you can land.** The driver maps the source's
types to Arrow and the landing keeps the wire type, so a dialect that
cannot express a type cannot land it:

- PostgreSQL and other typed backends carry real DATE/TIMESTAMP
  types: cast in the recipe and the column lands temporal.
- SQLite has no date type and dynamic typing — a declared type does
  not bind the rows: a DOUBLE column can hold text, and mixed rows
  land as `Utf8`. Force numerics in the recipe (`CAST(rate AS REAL)`
  fixes the storage class). Normalize dates with SQLite's own date
  functions (`date(x)` to ISO text, `unixepoch(x)` to integer
  seconds) — but never `CAST … AS DATE` there: DATE takes NUMERIC
  affinity, so `CAST(date(x) AS DATE)` silently lands `2010` for
  `'2010-12-27'` (an int64 on the wire — measured, not theorized). The
  honest spellings land untyped because the wire has no temporal
  type; the typed read is `CAST(col AS DATE)` at read time, and that
  gap belongs in the column's `meaning` gloss so no reader has to
  rediscover it.

Cast accounting reads `unaccounted` on this path — the source's
dialect owns the casts, so the landing cannot attribute a NULL to
one. Read the landed identity yourself the moment a table lands:
`DESCRIBE <table>` serves the landed schema, which is where a numeric
that landed as text shows up at the decision moment instead of three
flows later.

The source's catalog is probe-able like any table. Ask it for declared
keys before detecting relationships:

```glossql
PROBE erp AS $$SELECT tc.table_name, kcu.column_name, tc.constraint_type
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
  ON kcu.constraint_name = tc.constraint_name
WHERE tc.constraint_type IN ('PRIMARY KEY', 'FOREIGN KEY')$$;
```

(SQLite spells it `pragma_table_info('t')` and
`pragma_foreign_key_list('t')`.) **A declared key describes the
source's tables, not the tables you land** — recipes reshape: a join
in the recipe SQL, a renamed column, a filtered subset all break the
correspondence. Harvested keys are evidence for the relationship
judge, never declared relationships; declare only what survives the
judged read against the landed data (the glossql-relationships flow).

## 2. Probe — look before you write

`PROBE` runs recipe-shaped SQL at the source and lands nothing. Use it
to count what parses, then to rehearse the exact schema:

```glossql
PROBE erp_export AS $$SELECT count(raw) AS filled, count(parsed) AS parsed
FROM (SELECT "amount" AS raw, try_cast("amount" AS DOUBLE) AS parsed
      FROM read_parquet('orders/*.parquet'))$$;

PROBE erp_export AS $$SELECT order_id,
       try_cast(amount AS DOUBLE) AS amount,
       try_to_date(order_date, '%d.%m.%Y') AS order_date
FROM read_parquet('orders/*.parquet') LIMIT 0$$;
```

Alias the casts in a subquery before aggregating over both the raw and
the parsed column: the engine names a cast after its inner expression,
so `count("amount")` and `count(try_cast("amount" AS DOUBLE))` collide
in one aggregate — the `AS` aliases arrive too late to separate them.

A `LIMIT 0` probe's empty result still carries its schema — the
outcome's `columns` field lists every (name, type) even at zero rows
(fixed 2026-08-14; a run before that had to land a rehearsal recipe
just to `DESCRIBE` it) — so it rehearses exactly the identity the
recipe will stamp. **Run it per
file before authoring any recipe; row probes cannot replace it**:
probe rows omit null fields, so a column that is null in the rows
you sampled is invisible there — the first validated run lost three
columns this way, one of them a join key, and the missed
relationship rode the missed column. The schema probe is where you
see every column and *choose* which ones the topic earns. Taught
format patterns (date spellings, decimal marks) are FACT glosses on
the dataset — read them from the glossary before guessing formats.

## 3. Recipe — typing is authored

The recipe carries the casts and the column choices; there is no typing
machinery behind it. A value that fails its cast lands as NULL (a kept
row with a NULL cell, not a dropped row); a column you leave out of the
SELECT list is your judgment as author.

```glossql
DECLARE RECIPE orders ON fin FROM erp_export AS $$
  SELECT order_id,
         try_cast(amount AS DOUBLE) AS amount,
         try_to_date(order_date, '%d.%m.%Y') AS order_date
  FROM read_parquet('orders/*.parquet')$$;
```

The outcome carries the counts at the decision moment — rows landed,
rows dropped, **and the cast account**: for every `try_*` in the SELECT
list, how many cells held a value the cast nulled, with the top such
values by frequency (`cast-nulled cells — amount: 12 ['\N' ×10, …]`).
The full account persists in `imports.cast_failures`. Those tokens came
from the data, not from any list — judge them: a repeated token like
`\N` or `n/a` is usually a null marker (amend the recipe — `NULLIF`
before the cast, or a format the cast should carry — and re-declare; it
supersedes and re-lands), while a scattered long tail may be genuinely
bad data worth a FACT gloss. A recipe whose WHERE already drops failing
rows reads `casts clean` — those are dropped rows, not nulled cells.
History stays in `SELECT * FROM imports`. The landed table is the typed
table — `DESCRIBE <table>` reads its schema back. A changed
recipe under the same name **supersedes and re-lands**: the old landing
and its cached evidence go, glosses stay (their snapshot ids show their
age) — re-run the measurements and review glosses for columns the new
recipe changed. `DROP TABLE` is refused while the table holds data.

## 4. The measurement plane

Fan out the library per column — the grain is yours, the grammar
carries no ordering:

```glossql
SELECT profile() FROM orders.amount;
SELECT outliers() FROM orders.amount;
SELECT temporal() FROM orders.order_date;
```

Order matters only through `ACCEPTS`: `outliers` reads the cached
profile. If a result abstains with
`{"applicable": false, "missing_aspects": ["column_profile"]}`, run the
function that RETURNS the named aspect first — the abstention heals on
its own once the dependency lands. A bare `{"applicable": false}` means
the subject genuinely doesn't fit (a text column has no outliers); stop
trying.

One table-grain measurement joins the fan-out:

```glossql
SELECT detect_derivations() FROM orders;
SELECT value FROM GLOSSARY(orders::derivation_candidates) WHERE state = 'current';
```

Row-grain arithmetic identities among the table's numeric columns —
`total = units * unit_price`, `net = gross - tax` — with violation
counts. Generous by design (it proposes identities that hold at ≥0.95
over ≥20 rows); you judge which are real derivations rather than
numeric coincidence, and a confirmed one is worth a `meaning` gloss on
the derived column. It earns its keep after every later landing: a
confirmed identity re-checked per batch separates "the pipeline broke"
from "the business changed" — a corrupted slice violates the identity
at exactly its row coverage while every marginal statistic reads the
same change as a price move.

## 5. The semantic vocabulary ships — read it back

The workspace boots with the KPI kit already declared: `meaning`,
`entity`, `role`, `behavior`, `unit`, `dimension`, `conventions`,
`formulas`, `definitions`, `recipe_change`, each with its witness.
Don't redeclare them — read them back and gloss:

```glossql
SELECT name, kind, grains FROM aspects;
SELECT name, aspect, speakers, detector FROM witnesses;
```

The `ON` grain in each declaration is the contract: glosses outside
it are refused, and the `unassessed` grid stays within it. Declare a
new aspect only for what your source genuinely adds that the kit
doesn't name.

A witnessed aspect that can fail to apply declares its **judged
negative** — `none` beside the real values, with `grounds`.
"Examined, does not apply" and "nobody judged yet" are different
facts: the first is a `none` gloss, the second an `unassessed` row,
and only that split lets the backlog read walk to zero. Free-string
aspects (`unit`) carry the same convention as the value `none` plus
grounds.

## 6. Gloss every table — the entity verdict

Before the columns, say what each table *is*. Every correct aggregate
downstream depends on this verdict, and it is judged from the data,
never from the table's name:

- **value** — what one row is, in business words ("one journal line",
  "a customer master record").
- **role** — `fact` (events/measures at volume, carrying the numbers)
  or `dimension` (descriptive, referenced by others). Read it from the
  evidence: measures, an event date, row counts, who references whom.
- **grain** — the columns that identify one row. Verify, never assert:
  `COUNT(*)` vs `COUNT(DISTINCT (col, …))` must agree. A table whose
  real grain is composite gets the composite; a table with no key gets
  none — say so in `meaning` rather than inventing one. Watch for
  document-header values repeated onto every line (constant within the
  document id): summing them at row grain multiplies by line count.
- **time_axis** — the column recording *when the row's event
  happened*. Attribute dates (due_date, hire_date) are not an axis;
  one anchor at most; a table with only attribute dates has none.
- **identity_columns** — structural observation only: which columns
  identify entities (theirs or another table's).

```glossql
GLOSS entity ON orders AS $${"value": "sales order line", "role": "fact",
  "grain": ["order_id", "line_no"], "time_axis": "order_date"}$$;
```

## 7. Gloss every column

This is the content the flow exists to produce. Read the measurements
first (`SELECT * FROM GLOSSARY(orders.amount)` serves the profile),
then speak to each aspect on every landed column:

- **meaning** — `value` is one sentence, specific to the business
  context, saying what the column contains and how it is used; `term`
  is the human-readable name a report would print (`txn_amt` →
  "Transaction Amount"). Never state stock-or-flow or summability in
  the prose — that verdict has one home, `behavior`.
- **role** — the structural role, judged from this table alone:
  `key` = primary identifier (unique, non-null) · `measure` = numeric
  value meant for aggregation · `dimension` = categorical value for
  grouping and filtering · `timestamp` = date or datetime ·
  `attribute` = descriptive, neither aggregated nor grouped on. Never
  call a column a foreign key here — references are
  `DECLARE RELATIONSHIP`, decided against the other table.
- **behavior** — numeric measures only. `stock` is a carried
  point-in-time level (balance, position, headcount) that must not be
  summed across periods; `flow` is a per-period movement (payment,
  sale, change) that accumulates and is summable. A column's own
  trajectory cannot decide this — a trending flow and a mean-reverting
  stock look alike — so read the evidence before glossing:
  `SELECT behavior_evidence() FROM orders.amount;` reconciles the
  column against period movements aggregated from event tables
  reachable over *declared* relationships (declare edges first; a new
  edge or import invalidates the evidence cache, so the next call
  recomputes and abstentions heal on their own). Each alignment is
  served raw AND year-scoped: a cumulative that resets — season
  standings, a year-to-date balance — abstains at raw grain (every
  boundary injects a full period's error) and reconciles as a stock
  on the `scope: "year"` anchor; read the pair together. Each anchor
  carries a verdict beside its evidence —
  entity votes, agreement, both residuals, the runner-up
  conventions — and `abstain` is a complete answer, not a defect. The
  verdict is evidence for *your* judgment, never a ruling: you may
  out-judge it by testing against the data yourself. Names lie
  either way — a "trial balance" column can carry period turnover (a
  flow) rather than balances; the measurement reads the data, not the
  label. When it starves on a column — every anchor abstains, no
  entity persists across periods — the ladder is: your own data test,
  cited as the basis (a mirror table, the GL, a hand reconciliation);
  and, as the last rung on an installation where a whole family of
  columns needs it, **author a workspace-scoped function**
  (`FOR <dataset>`, per the glossql-functions skill — needs workspace
  filesystem access) that evaluates behavior the way *this* dataset
  demands. That function then IS the installation's recorded thinking
  about its own behaviors — versioned, re-runnable, and honest about
  its method in a way a one-off judgment never is. Unsure and
  unwilling to climb the ladder? Don't gloss: absence shows as an
  honest `unassessed` row; a guess does not. Relevance is conditional (ruled 2026-08-14):
  a column owes `behavior` and `unit` only once its `role` says
  `measure`, and `dimension` only on `role = 'dimension'` — so gloss
  role first and the rest of the backlog derives from it. "Does not
  apply" *within* relevance is still a judgment, not "unsure": a
  ratio is a measure with no stock/flow nature, and that lands as
  `{"value": "none", "grounds": "…"}` (§5), never as a permanent
  unassessed row.
- **unit** — where a magnitude has one: currency, quantity unit,
  percentage. `source_column` names the column carrying the unit when
  it rides beside the value.

```glossql
GLOSS meaning ON orders.amount AS $${"value": "gross invoiced amount per order line", "term": "Order Amount"}$$;
GLOSS role ON orders.amount AS $${"value": "measure"}$$;
GLOSS behavior ON orders.amount AS $${"value": "flow"}$$;
GLOSS unit ON orders.amount AS $${"value": "EUR", "source_column": "currency_code"}$$;
```

## 8. Read back what's open

```glossql
SELECT count(*) FROM GLOSSARY(fin) WHERE state = 'unassessed';
SELECT subject, band, score FROM ATTEST(fin::behavior) WHERE band = 'red';
```

Witnessed aspects nobody spoke to appear as rows — absence is visible,
not an omission. Red bands are where a human must close what you could
not. An unwritten witnessed claim on a measure (`behavior`, `unit`) is
*your* measurement backlog, never a human question (ruled 2026-08-13):
run `behavior_evidence` and gloss from its verdict; read units from
profiles and currency columns. The door asks the human judgment only.
Leave a slot unwritten rather than guessed when the measurement
abstains and nothing else grounds it — and say so in the read-back
with the abstention's reason.

Close with a read-back the human can judge at its real size. The
load-bearing verdicts — entity, behavior, unit, anything a wrong value
silently corrupts downstream — get named one by one. For the
descriptive long tail (a hundred `meaning` glosses), exhaustive review
is theater: show the distribution and a spot-check sample — "111
column meanings, 78 cite measured evidence; here are five at random" —
and treat a failed spot-check as the batch's problem, not the row's.
