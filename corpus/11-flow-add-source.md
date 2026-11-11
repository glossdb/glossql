# Flow: add a source

Adding a source is a sequence of statements: declare the source, preview
it, write the load query, load, profile the columns, annotate them and
read the test results. The example walks the sequence for a folder of
Parquet files.

## Declare the source and preview it

A preview (`PREVIEW`) runs SQL at the source and loads nothing.

```glossql
USE fin;
DECLARE SOURCE erp_export SET (type: parquet, location: 'lake/erp');

PREVIEW erp_export AS $$SELECT * FROM read_parquet('orders/*.parquet') LIMIT 50$$;
PREVIEW erp_export AS $$SELECT count("order_date") AS filled,
       count(try_strptime("order_date", '%d.%m.%Y')) AS parsed
FROM read_parquet('orders/*.parquet')$$;
```

## Write the load query

The load query (`DECLARE RECIPE`) carries the casts. The loaded table
has the types the query produces.

```glossql
DECLARE RECIPE orders ON fin FROM erp_export AS $$
  SELECT order_id,
         try_cast(amount AS DECIMAL(12,2)) AS amount,
         try_strptime(order_date, '%d.%m.%Y')::DATE AS order_date
  FROM read_parquet('orders/*.parquet')$$;

SELECT sum(amount) FROM orders;
```

## Load again

A load (`IMPORT`) runs the same load query against the source as it is
now. It is asked for by name.

```glossql
IMPORT orders;
IMPORT fin.orders;
```

## Remove a table

```glossql
DROP TABLE orders;
```

## Count the rows the load query filtered out

```glossql
SELECT dropped_rows_count FROM imports WHERE table_name = 'orders';
```

## Profile the columns

Each profile is an aspect a function fills: statistics, outliers, and
the cadence and gaps of a time column.

```glossql
DECLARE ASPECT column_profile WITH $${
  "type": "object",
  "required": ["total", "null_ratio", "distinct"],
  "properties": {
    "total": {"type": "integer"}, "null_ratio": {"type": "number"},
    "distinct": {"type": "integer"}, "cardinality_ratio": {"type": "number"},
    "min": {}, "max": {}, "top_values": {"type": "array"},
    "lengths": {"type": "object"},
    "numeric": {
      "type": "object",
      "required": ["mean", "mad", "percentiles"],
      "properties": {"mean": {}, "stddev": {}, "mad": {},
                     "percentiles": {"type": "object"}}
    }
  }
}$$ AS MEASUREMENT;
DECLARE FUNCTION profile FOR GLOBAL AS $$/* min/max/nulls/distincts per column, from kernels */$$
  RETURNS column_profile;

DECLARE ASPECT outlier_profile WITH $${
  "type": "object", "required": ["applicable"],
  "properties": {"applicable": {"type": "boolean"},
                 "iqr": {"type": "object"}, "zscore": {"type": "object"}}
}$$ AS MEASUREMENT;
DECLARE FUNCTION outliers FOR GLOBAL AS $$/* iqr and z-score fences over the profile */$$
  RETURNS outlier_profile;

DECLARE ASPECT temporal_profile WITH $${
  "type": "object", "required": ["applicable"],
  "properties": {"applicable": {"type": "boolean"},
                 "granularity": {"type": "string"},
                 "confidence": {"type": "number"},
                 "completeness": {"type": "object"}, "gaps": {"type": "object"}}
}$$ AS MEASUREMENT;
DECLARE FUNCTION temporal FOR GLOBAL AS $$/* cadence, completeness and gaps on a time column */$$
  RETURNS temporal_profile;

SELECT profile(), outliers() FROM fin.orders.amount;
SELECT temporal() FROM fin.orders.order_date;
```

## Annotate

```glossql
SELECT * FROM GLOSSARY(fin.orders.amount);

GLOSS meaning ON orders.amount AS $${"value": "gross invoiced amount per order line"}$$;
GLOSS additivity ON orders.amount AS $${"value": "additive"}$$;
GLOSS unit ON orders.amount AS $${"value": "EUR", "source_column": "currency_code"}$$;
```

## Read the test results

```glossql
SELECT * FROM TESTS(fin::additivity);
SELECT subject, state, score FROM TESTS(fin::unit) WHERE state = 'red';
```
