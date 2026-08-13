---
name: glossql-relationships
description: Ground the join structure of a glossql dataset — run the shipped detect_relationships measurement, judge every candidate against the data, and declare only the survivors. Use after tables have landed, before cross-table analysis.
---

# Declaring relationships

The arc is candidate → verified → declared. A high-recall measurement
proposes; you judge; the grammar records. The workspace ships
`detect_relationships` at boot.

## 1. Measure

```glossql
USE fin;
SELECT detect_relationships() FROM fin;
SELECT value FROM GLOSSARY(fin::relationship_candidates) WHERE state = 'current';
```

It runs at dataset grain over every landed table: columns that look
like keys (near-unique) become `to` sides, every type-compatible
column is tried as a `from` side, and any pair where at least half the
from-side values resolve survives. Each candidate carries `from`,
`to`, `cardinality`, `overlap`, plus evidence — `matched`, `orphans`,
`from_distinct`, `to_distinct`. The list is deliberately generous:
high recall, false positives included, you are the precision. A newly
landed table invalidates the cached candidates on its own (the
`imports` ACCEPTS edge) — the next call re-measures.

A candidate carrying `key_columns` is a **composite**: its `from`/`to`
anchor is no key alone, but together with the scoping leg it
identifies rows — the multi-tenant shape, `(business_id, name)`. The
measurement only proposes composites the data rescued (the combined
to side is near-unique and the two-leg join resolves); the anchor is
the identifying leg, the scope the tenant leg. The declared form is
the tuple — anchor and scope legs together, in one endpoint.

## 2. Judge every candidate

Before declaring anything, per candidate:

- **Anti-join both directions.** Count and *read* what doesn't
  resolve:
  ```glossql
  SELECT count(*) FROM orders o LEFT JOIN customers c
    ON o.customer_id = c.id WHERE c.id IS NULL;
  ```
- **Ground the orphans.** An orphan count is a question, not a
  verdict. Orphans that are exactly a business population (the
  cancelled invoices, the pre-migration accounts) confirm the edge —
  declare it and gloss the finding. Random misses argue against it.
- **Distrust coincidence.** Two unique integer columns overlap
  perfectly without meaning it — parallel row-number sequences are the
  classic false positive. A join must mean something: the names, the
  values, and the business objects have to agree.
- **Check the claimed cardinality** on the data
  (`GROUP BY … HAVING count(*) > 1`) rather than trusting the label.
- **Judge a composite on all its legs.** Anti-join on the anchor
  *and* the scope together; joining on the anchor alone fans out and
  silently over-counts — that fan-out is what the composite exists to
  collapse.
- **Ground the verdict, not the story.** Declare or reject on what
  the joins measure. *Why* the data looks this way (a bad export, a
  missing tenant set, an upstream system that never enforced the
  reference)
  is a hypothesis — verify it before writing it down, or state it as
  a hypothesis. A correct rejection with a wrong causal story
  misleads everyone who reads the grounds later.

## 3. Declare the survivors

```glossql
DECLARE RELATIONSHIP orders.customer_id -> customers.id;
DECLARE RELATIONSHIP invoices.order_id <-> orders.id;
```

`->` is a reference; `<->` when both sides resolve each other. A
same-table candidate (`staff.manager_id -> staff.employee_id`) is a
hierarchy — declare it like any edge. A composite key declares as a
tuple endpoint — the tuple is the key (never declare the anchor leg
alone: unscoped it licenses the fan-out the composite exists to
collapse):

```glossql
DECLARE RELATIONSHIP txn.(business_id, account) -> coa.(business_id, account_name);
```

Rejected candidates are *not declared and not erased* — they stay
visible in the measurement, which is the record that they were seen
and judged.

## 4. Record the grounds

Declared edges accept glosses on the pair path — the KPI kit ships
`meaning` with relationship grain for exactly this. Say why the edge
holds and what the orphans are:

```glossql
GLOSS meaning ON orders.customer_id -> customers.id AS
  $${"value": "each order belongs to one customer; 140 orphans are the cancelled orders, never posted"}$$;
```

## 5. Watch what you declared

Once edges are declared, `relationship_coherence` measures what each
one asserts, at dataset grain:

```glossql
SELECT relationship_coherence() FROM fin;
SELECT value FROM GLOSSARY(fin::relationship_coherence) WHERE state = 'current';
```

Per declared relationship: `orphans` / `orphan_rate` (from-side values
that resolve to no row — exact, and it catches shapes no column
statistic can, including a single repeated invented key), and
`temporal` — for each date-column pair across the join, how often the
child's date precedes the parent's. The temporal read is evidence, not
a verdict: a child event before a related *deadline* is ordinary; a
child event dated before its parent record *exists* is the trace a
wrong pairing leaves (a payment preceding its invoice's creation, a
confirmation preceding its order). Read the pair's names before
concluding anything. Re-run it
after new batches land — a rising orphan rate or a fresh
precedes signal on a previously quiet pair is an admission question.

## 6. Read back

```glossql
SELECT * FROM relationships;
SELECT subject, aspect, value FROM GLOSSARY(orders);
```

The `relationships` relation is the declared structure; a table's
`GLOSSARY()` sweep picks up the pair paths it participates in —
composite pairs included. Substrate SQL spells no tuples inside
`GLOSSARY(…)`, so address a composite pair through the sweep plus
`WHERE subject = '…'` on the subject text.
