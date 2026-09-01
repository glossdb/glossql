# Relationships

A relationship is a declared join edge between columns. It has no
name: it is addressed by its pair path, and that path is a subject —
glosses, measurements, and witnesses attach to it like to any table or
column.

```glossql
DECLARE RELATIONSHIP orders.customer_id -> customers.id;
DECLARE RELATIONSHIP invoices.order_id <-> orders.id;
DECLARE RELATIONSHIP txn.(business_id, account) -> coa.(business_id, code);
```

`->` is many-to-one, written from the FK side; `<->` is one-to-one;
many-to-many decomposes through a junction table. A composite endpoint
is a tuple — the tuple is the key, and there is no view or surrogate
standing in for it.

## Detected, verified, declared

Measurement proposes relationships; judgment declares them.
`detect_relationships()` emits candidates with evidence — containment
of one column's values inside another's is the statistic — and
candidates over-produce by design. The declaring actor verifies each against the data (anti-joins,
count-before/after are the cheap decisive checks) and declares only
the ones that pass.

Only declared relationships exist. There is no rejected or negative
form: a rejected candidate is not declared, and detection is
deterministic, so it does not resurface as new knowledge. Record what
a verification found as a gloss on the edge:

```glossql
GLOSS fk_note ON orders.customer_id -> customers.id AS $${"value": "2% orphaned rows"}$$;
```

## What a declared edge asserts, measured

Declaration is a claim, and the claim has measurable health:
`relationship_coherence()` measures what each declared join asserts —
the orphan rate (exact; it catches invented-key shapes including the
single repeated orphan that defeats rare-category counting) and
child-before-parent date incoherence, the trace a wrong pairing
leaves. No column-shaped statistic sees either on a high-cardinality
key.

Downstream evidence also depends on declared edges: behavior evidence
reconciles across them, and the quality layer's lineage identities
read through them. An undeclared true edge
costs exactly the analyses that would have used it; a declared false
one corrupts them — which is why the verify step is judgment, not
ceremony.
