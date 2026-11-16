# Review decisions: a person corrects the glossary

A person who reviews the glossary corrects it with the statements an
agent uses. The decision is stored as the person's annotation and takes
precedence over the agent's. The examples correct a unit, confirm a
relationship, add a drill-down hierarchy and add a date pattern.

A unit:

```glossql
GLOSS unit ON orders.amount AS $${"value": "EUR"}$$;
```

A relationship:

```glossql
DECLARE RELATIONSHIP orders.customer_id -> customers.id;
```

A hierarchy, as relationships between columns of one table, finer to
coarser:

```glossql
DECLARE RELATIONSHIP customers.city -> customers.region;
DECLARE RELATIONSHIP customers.region -> customers.country;
GLOSS meaning ON customers.city -> customers.region AS $${"value": "taught drill-down level"}$$;
```

A date pattern for the dataset:

```glossql
GLOSS type_patterns ON fin AS $${
  "items": [{
    "name": "eu_date",
    "pattern": "^\\d{2}\\.\\d{2}\\.\\d{4}$",
    "inferred_type": "DATE",
    "standardization": "try_strptime(\"{col}\", '%d.%m.%Y')::DATE"
  }]
}$$;
```
