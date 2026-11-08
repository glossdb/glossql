# Relationships

A relationship is a declared key between two tables. Functions in the
`quality` kit propose candidates; an agent or a person checks them and
declares the ones that hold. Only declared relationships exist.

## Declare one

```sql
DECLARE RELATIONSHIP orders.customer_id -> customers.id;
DECLARE RELATIONSHIP invoices.order_id <-> orders.id;
DECLARE RELATIONSHIP txn.(business_id, account) -> coa.(business_id, code);
```

- `->` is many-to-one, in the foreign-key direction. Write one-to-many
  from the other side.
- `<->` is one-to-one. A many-to-many relationship goes through a
  junction table.
- An end is a column, or a tuple of columns for a composite key.
- Every column must exist in the dataset; views count. Otherwise the
  declaration is refused.

There is no rejected form: a candidate that does not hold is simply
not declared. Detection is deterministic, so it does not come back as
new.

## Describe it

A relationship is an asset, addressed by its pair of ends:

```sql
GLOSS meaning ON orders.customer_id -> customers.id AS
  $${"value": "each order has one customer; orphans are cancelled orders"}$$;
```

A table's `GLOSSARY()` includes every relationship it takes part in,
from either side ([reading](reading.md)).

## Check it

The `quality` kit's `relationship_checks` and
`relationship_coherence` say whether a declared relationship holds:
rows that resolve, orphans, and whether a join multiplies rows
([kits](../kits.md)).

Normative text: [SPEC.md §4](../../SPEC.md#4-subjects-and-relationships).
