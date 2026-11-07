# Describing data

You describe data in two steps. Declare a property the harness asks for
(an aspect): a name, a JSON Schema for its values, and a kind. Then
record a value for it on an asset (`GLOSS`). Every value is validated
when it is written and stored with its author and the data version it
describes.

## Declare a property

```sql
DECLARE ASPECT unit WITH $${
  "type": "object",
  "properties": {"value": {"type": "string"}, "source_column": {"type": "string"}}
}$$ AS FACT;

DECLARE ASPECT min_max WITH $${
  "type": "object",
  "properties": {"min": {}, "max": {}}
}$$ AS MEASUREMENT ON COLUMN;

DECLARE ASPECT revenue WITH $${"title": "revenue", "x-kind": "measure"}$$ AS QUERY;
```

| Kind | The value is | Validated against |
|---|---|---|
| `FACT` | stated by an agent or a person | the `WITH` schema |
| `MEASUREMENT` | computed by a function; never written by hand | the `WITH` schema, at extraction ([measurements](measurements.md)) |
| `QUERY` | SQL, served as a view under the property's name: a metric, a single value, or a derived relation | the metric contract below |

Re-declaring with the same content does nothing. Changing a property
that has values under it is refused: delete the values first.

### What it applies to

`ON DATASET | TABLE | COLUMN | RELATIONSHIP | SOURCE` lists the kinds of
asset the property applies to. Without the clause it applies to any. A
`QUERY` property takes no clause: a metric belongs to the dataset.

A `SOURCE` property's values are shared across every dataset that
reads the source.

### When it is required

A condition makes the property required only where another property
says so, or only on columns of some types:

```sql
DECLARE ASPECT currency WITH $${"type": "object", "required": ["value"]}$$
  AS FACT ON COLUMN WHEN role = 'measure';
DECLARE ASPECT crs WITH $${"type": "object", "required": ["value"]}$$
  AS FACT ON COLUMN WHEN TYPE = 'GEOMETRY';
```

- `WHEN role = 'measure'` names another property and a value. The
  property is required on an asset while that property's winning value
  (a person's over an agent's) is the literal. The named property must
  exist; if its schema lists allowed values, the literal must be one.
- `WHEN TYPE = 'GEOMETRY'` or `WHEN TYPE IN ('DOUBLE', 'DECIMAL')`
  matches a column's base type (`DECIMAL` for `DECIMAL(18,2)`), case
  aside. It goes with `ON COLUMN` alone.

A condition decides what is required and counted as missing. It never
blocks a write.

## Record a value

Every body is JSON in dollar quotes, so it needs no escaping
(`$tag$ … $tag$` when the body contains `$$`):

```sql
GLOSS unit ON orders.amount AS $${"value": "EUR", "source_column": "currency_code"}$$;
GLOSS fk_note ON orders.customer_id -> customers.id AS $${"value": "2% orphaned rows"}$$;
```

- The property must be declared. A `FACT` body must match its schema; a
  `MEASUREMENT` property is refused.
- A property that applies to tables or columns takes only ones that
  exist in the dataset.
- One value per asset, property and actor kind
  ([sessions](sessions.md#say-who-is-speaking)): a person's new value
  replaces the person's old one, an agent's the agent's. Both are kept
  and read side by side.
- Every value stores the lake snapshot it was written at. It reads
  `stale` once its column was retyped, dropped or derived differently.

## Define a metric

A `QUERY` value is SQL. The extension creates a view in the dataset's
schema under the property's name:

```sql
DECLARE ASPECT revenue WITH $${"title": "revenue"}$$ AS QUERY;
GLOSS revenue ON fin AS $${
  "sql": "SELECT sum(amount) AS revenue FROM fin.orders",
  "assumptions": [{"dimension": "scope", "key": "all-orders", "assumption": "every order is revenue", "confidence": 0.8}]
}$$;
SELECT * FROM fin.revenue;
```

The body needs `sql`, or `stopped` with the reason no number is served.
`assumptions` lists what the SQL assumes, each with an optional
`dimension`, `basis` and `confidence` (0 to 1). Other keys pass, unless
a kit declares a schema for them (an aspect with `"x-kind":
"grounding"`).

- The view is a table of its dataset: its columns are assets. What a
  reader needs to combine its rows (which column is the period, which
  the measure and how it adds up, which columns slice it) is recorded
  on those columns, under a kit's properties.
- A newer value replaces the view; `stopped` drops it. The same SQL
  recorded again keeps the view and everything recorded on it.
- A metric cannot take a loaded table's name, nor a load query a
  metric's.

## Remove values

The glossary is a table; removing values is a `DELETE`:

```sql
DELETE FROM glossary WHERE subject = 'orders.amount' AND aspect = 'unit';
```

It runs on the catalog's table in the lake's metadata catalog and
returns the number of rows removed.

Normative text: [SPEC.md §5](../../SPEC.md#5-the-glossary).
