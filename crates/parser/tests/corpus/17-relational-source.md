# 17 · The relational spine — the sqlite run

Source: our own test run (2026-08-07, run 8) — dataset `fin2` over the
BookSQL SQLite dump (`~/glossql-data/finance_2/accounting.sqlite`, seven
tables, 1,012,948 rows), the first full flow over a relational source
since the ADBC executor landed 2026-08-06. The spine below is what the
run spoke, abridged to the columns that carried the findings; the full
record is `reports/2026-08-07-sqlite-relational-run.md`.

## Declare the source — the driver is the index slug

```glossql
USE fin2;

DECLARE SOURCE books SET (type: relational_db,
                          driver: 'sqlite',
                          location: '/Users/x/glossql-data/finance_2/accounting.sqlite');
```

The run first spelled `driver: 'adbc_driver_sqlite'` (the skill's old
example) and got a bare NotFound — the loadable name is the ADBC driver
index slug the operator installed. Hardcoded list since 2026-08-07; the
load error now teaches it.

## Harvest the source's own catalog — evidence, never declarations

```glossql
PROBE books AS $$SELECT name, type FROM pragma_table_info('master_txn_table')$$;
PROBE books AS $$SELECT "table", "from", "to"
FROM pragma_foreign_key_list('master_txn_table')$$;
```

Six declared FKs came back. Two survived judging against the landed
data — a declared key describes the source's tables, not the tables a
recipe lands, so harvested keys enter the relationship judge as
evidence only.

## The recipe runs at the source, in the source's dialect

```glossql
DECLARE RECIPE ledger ON fin2 FROM books AS $$
  SELECT id,
         businessID   AS business_id,
         CAST(Quantity AS REAL) AS quantity,
         CAST(Rate     AS REAL) AS rate,
         CAST(Credit   AS REAL) AS credit,
         CAST(Debit    AS REAL) AS debit,
         date(Transaction_DATE) AS transaction_date
  FROM master_txn_table$$;
```

Two dialect facts the run paid for, both now in the add-source skill:

- **Forced storage classes.** SQLite's dynamic typing means a declared
  `DOUBLE` column can hold text rows (`billing_rate`: text in all
  50,000 rows); `CAST(x AS REAL)` in the recipe fixes the storage
  class so the wire carries a numeric.
- **The cast trap.** `CAST(date(x) AS DATE)` does not fail — `DATE`
  takes NUMERIC affinity, and `'2010-12-27'` lands as int64 `2010`.
  Measured through the driver, 2026-08-07:

```glossql
PROBE books AS $$SELECT date('2010-12-27')              AS iso,
                        CAST(date('2010-12-27') AS DATE) AS trap,
                        unixepoch('2010-12-27')          AS epoch$$;
```

→ `iso` wires as string `2010-12-27`, `trap` as int64 `2010`, `epoch`
as int64 `1293408000`. The honest spellings land untyped; the typed
read is at read time.

## Read back what landed

```glossql
DESCRIBE ledger;
SELECT CAST(transaction_date AS DATE) AS d, credit FROM ledger LIMIT 5;
```

`DESCRIBE` passes the allowlist since 2026-08-07 (the run burned a
diagnostic re-landing to learn its types). The second read is the
typed contract this dialect can carry: cast at read over ISO text.

## Verdicts

- **TRANSCRIBES** — the spine (source with driver, harvest probe,
  at-source recipe, read-back) is existing surface; the run needed no
  new grammar.
- **INFORMATION LOST** — the temporal *type*: SQLite's wire has no
  temporal storage class, so no recipe spelling lands a Date/Timestamp
  column from this dialect. `temporal()` names the gap in its
  abstention reason; `behavior_evidence()` abstains dataset-wide
  without a typed period axis. A typed backend (PostgreSQL) does not
  share the loss — the recipe cast lands temporal there. Ruled
  2026-08-07: no landing-side machinery; the dialect teaching carries
  it, and SQLite stays a cheap ADBC test, not a product target.
