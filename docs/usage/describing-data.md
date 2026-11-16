# Describing data

You describe data in two steps. Declare a property (`DECLARE ASPECT`):
a name, a JSON Schema for its values, and a kind. Then annotate an
asset with a value for it (`GLOSS`). The value is validated on write
and stored with its actor and the lake snapshot it was written at.

## Declare a property

```sql
DECLARE ASPECT unit WITH $${
  "type": "object",
  "required": ["value"],
  "properties": {"value": {"type": "string"}}
}$$ AS FACT ON COLUMN;

DECLARE ASPECT row_count WITH $${"type": "integer"}$$ AS MEASUREMENT ON TABLE;

DECLARE ASPECT headcount WITH $${"title": "Headcount"}$$ AS VIEW;
```

| Kind | The value is | Validated against |
|---|---|---|
| `FACT` | stated by an agent or a person | the `WITH` schema |
| `MEASUREMENT` | computed by a function; `GLOSS` on it is refused | the `WITH` schema, when the function runs ([measurements](measurements.md)) |
| `VIEW` | SQL, served as a view under the property's name: a metric, a single value or a table | the view contract below |

Declaring a property again with the same content does nothing.
Changing a property that has annotations is refused: delete them
first.

### What it applies to

`ON DATASET | TABLE | COLUMN | RELATIONSHIP | SOURCE` lists the kinds
of asset the property applies to, one or several. Without the clause
it applies to all. An annotation on another kind of asset is refused.
A `VIEW` property takes no clause: it is annotated on the dataset.

An annotation on a source is shared by every dataset.

### When it is required

A condition makes the property required only where another property
has a given value, or only on columns of some types:

```sql
DECLARE ASPECT currency WITH $${"type": "object", "required": ["value"]}$$
  AS FACT ON COLUMN WHEN role = 'measure';
DECLARE ASPECT srid WITH $${"type": "object", "required": ["value"]}$$
  AS FACT ON COLUMN WHEN TYPE = 'GEOMETRY';
```

- `WHEN role = 'measure'` names another property and a value. The
  property is required on an asset while the served `value` of that
  property there is the literal. The named property must be declared.
- `WHEN TYPE = 'GEOMETRY'` or `WHEN TYPE IN ('DOUBLE', 'DECIMAL')`
  matches a column's base type (`DECIMAL` for `DECIMAL(18,2)`). It is
  accepted with `ON COLUMN` alone.

A condition decides what `GLOSSARY()` reports as `missing`. It does
not block a write.

## Annotate

The body is JSON in dollar quotes, so it needs no escaping (use
`$tag$ … $tag$` when the body contains `$$`):

```sql
GLOSS unit ON employees.salary AS $${"value": "EUR per year"}$$;
```

The statement answers `glossed unit on staff.employees.salary as agent`.

- The property must be declared, and the body must match its schema.
- The table or column must exist in the dataset.
- One value is kept per asset, property and kind of actor (`agent` or
  `human`, [set on the connection](../installation.md#set-the-actor)).
  A person's new value replaces the person's old one, an agent's the
  agent's. `GLOSSARY()` serves the person's value before the agent's.
- If the property has a test with a `BY` clause, only the kinds of
  actor it lists may annotate ([tests](tests.md)).
- The value reads `stale` once its column changes
  ([reading](reading.md#when-a-value-is-stale)).

## Define a view

A `VIEW` property's value is SQL. The extension creates a view in the
dataset's schema under the property's name. Use it for a metric, a
single value or a table.

```sql
DECLARE ASPECT headcount WITH $${"title": "Headcount"}$$ AS VIEW;
GLOSS headcount ON staff AS $${
  "sql": "SELECT department, count(*) AS headcount FROM staff.employees GROUP BY department",
  "assumptions": [{"assumption": "every row is an active employee", "confidence": 0.8}]
}$$;
SELECT * FROM staff.headcount;
```

The view contract:

- `sql`, or `stopped` with the reason no view is served. One of the two
  is required.
- `assumptions`, optional: a list of objects, each with an `assumption`
  and optionally `dimension`, `basis` and `confidence` (0 to 1).
- Other keys are accepted. A kit can declare a schema for them: a
  property whose schema carries `"x-kind": "definition"`. Every `VIEW`
  value is then validated against that schema too.

What else to know:

- The view is a table of its dataset, and its columns are assets.
  Annotate them like a loaded table's columns: which column is the
  period, which the measure and how it adds up.
- A newer value replaces the view; `stopped` drops it. The same SQL
  annotated again keeps the view and the annotations on its columns.
- A view cannot take the name of a loaded table.

## Declare a relationship

A relationship is a declared key between two tables.

```sql
DECLARE RELATIONSHIP employees.department_id -> departments.id;
DECLARE RELATIONSHIP badges.employee_id <-> employees.id;
DECLARE RELATIONSHIP shifts.(site, employee_no) -> employees.(site, employee_no);
```

- `->` is many-to-one, in the foreign-key direction. `<->` is
  one-to-one.
- A side is a column, or a tuple of columns for a composite key.
- Every column must exist in the dataset; views count. Otherwise the
  statement is refused.

A relationship is an asset, addressed by its two sides:

```sql
GLOSS meaning ON employees.department_id -> departments.id AS
  $${"value": "each employee belongs to one department"}$$;
```

`GLOSSARY()` on a table includes the relationships it takes part in,
from either side. The `quality` kit proposes candidates and checks
declared relationships against the data ([kits](../kits.md)).

## Remove annotations

```sql
DELETE FROM glossary WHERE subject = 'staff.employees.salary' AND aspect = 'unit';
```

The statement removes the rows from the catalog and returns their
count. The newest remaining row per asset, property and kind of actor
is then served.

Normative text: [SPEC.md §4](../../SPEC.md#4-subjects-and-relationships)
and [§5](../../SPEC.md#5-the-glossary).
