---
name: glossql-add-source
description: Drive the add-source flow in a glossql workspace end to end — probe a declared source, author the typing recipe, land the table, run the measurement plane, frame the semantic vocabulary, and gloss every column. Use when connecting a new data source or landing a new table.
---

# Adding a source

The statement shapes below assume the `glossql` skill (the door, the
outcome shape). A fresh workspace already holds the measurement
library — `profile`, `outliers`, `temporal`, `behavior_evidence`,
`slot_entropy` and the rest arrive at boot with their aspects;
`SELECT * FROM functions` lists what is declared, and that read is
the authority, not this sentence.

## 1. Dataset and source

```glossql
USE fin;
DECLARE SOURCE erp_export SET (type: parquet, location: 'lake/erp');
```

The location is a root directory. Globs and file paths belong in
recipe SQL, resolving under that root.

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
  `'2010-12-27'` (an int64 on the wire — measured 2026-08-07). The
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

A `LIMIT 0` probe's empty result still carries its schema — it
rehearses exactly the identity the recipe will stamp. Taught format
patterns (date spellings, decimal marks) are FACT glosses on the
dataset — read them from the glossary before guessing formats.

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

## 5. Frame the semantic vocabulary

The workspace ships with measurements only. Declare the vocabulary
before glossing — send once, verbatim. The `ON` list is each aspect's
grain: glosses outside it are refused, and the `unassessed` grid stays
within it.

```glossql
DECLARE ASPECT meaning WITH $${
  "type": "object", "required": ["value"],
  "properties": {"value": {"type": "string"}, "term": {"type": "string"}}
}$$ AS FACT ON TABLE, COLUMN, RELATIONSHIP;
DECLARE ASPECT entity WITH $${
  "type": "object", "required": ["value"],
  "properties": {"value": {"type": "string"},
                 "role": {"enum": ["fact", "dimension"]},
                 "grain": {"type": "array", "items": {"type": "string"}},
                 "time_axis": {"type": "string"},
                 "identity_columns": {"type": "array", "items": {"type": "string"}}}
}$$ AS FACT ON TABLE;
DECLARE ASPECT role WITH $${
  "type": "object", "required": ["value"],
  "properties": {"value": {"enum": ["key", "measure", "dimension",
                                    "timestamp", "attribute"]}}
}$$ AS FACT ON COLUMN;
DECLARE ASPECT behavior WITH $${
  "type": "object", "required": ["value"],
  "properties": {"value": {"enum": ["stock", "flow"]}}
}$$ AS FACT ON COLUMN;
DECLARE ASPECT unit WITH $${
  "type": "object", "required": ["value"],
  "properties": {"value": {"type": "string"},
                 "source_column": {"type": "string"}}
}$$ AS FACT ON COLUMN;

DECLARE WITNESS meaning_w ON meaning BY (AGENT, HUMAN);
DECLARE WITNESS entity_w ON entity BY (AGENT, HUMAN);
DECLARE WITNESS role_w ON role BY (AGENT, HUMAN)
  DETECTOR slot_entropy THRESHOLD 0.7;
DECLARE WITNESS behavior_w ON behavior BY (AGENT, HUMAN)
  DETECTOR slot_entropy THRESHOLD 0.7;
DECLARE WITNESS unit_w ON unit BY (AGENT, HUMAN)
  DETECTOR slot_entropy THRESHOLD 0.7;
```

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
  label. Unsure? Don't gloss: absence shows as an honest `unassessed`
  row; a guess does not. (Only the `dimension` aspect has a judged
  negative today; for these aspects an `unassessed` row covers both
  "not yet judged" and "does not apply".)
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
not.
