# Load from a relational database

A relational source runs the load query itself, in its own SQL dialect.
The example loads a ledger from a SQLite file: declare the source, read
the source's own catalog, write the load query with explicit casts,
read the table back, and load again, first in full and then
incrementally by key.

## Declare the source

```glossql
USE fin2;

DECLARE SOURCE books SET (type: relational_db,
                          location: 'exports/accounting.sqlite');
```

## Read the source's catalog

The source's declared foreign keys are evidence for a relationship,
not a declaration.

```glossql
PREVIEW books AS $$SELECT name, type FROM pragma_table_info('master_txn_table')$$;
PREVIEW books AS $$SELECT "table", "from", "to"
FROM pragma_foreign_key_list('master_txn_table')$$;
```

## Write the load query in the source's dialect

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

In SQLite, `CAST(date(x) AS DATE)` returns an integer. The preview
shows it:

```glossql
PREVIEW books AS $$SELECT date('2010-12-27')              AS iso,
                        CAST(date('2010-12-27') AS DATE) AS trap,
                        unixepoch('2010-12-27')          AS epoch$$;
```

## Read back what was loaded

```glossql
DESCRIBE ledger;
SELECT CAST(transaction_date AS DATE) AS d, credit FROM ledger LIMIT 5;
```

## Load again

A load replaces the table with the query's result.

```glossql
IMPORT ledger;
```

With a key (`SET (key: id)`), a load merges the query's rows into the
table by that key.

```glossql
DECLARE RECIPE ledger ON fin2 FROM books SET (key: id) AS $$
  SELECT id,
         businessID   AS business_id,
         CAST(Credit  AS REAL) AS credit,
         CAST(Debit   AS REAL) AS debit,
         date(Transaction_DATE) AS transaction_date
  FROM master_txn_changes$$;
IMPORT ledger;
```
