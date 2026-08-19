# Aspects

An **aspect** is a declared vocabulary entry: a name, a JSON Schema,
and a kind. A **gloss** applies an aspect to a subject with a JSON
body. There are no fact names — the aspect is the key, and you cannot
gloss an aspect that was not declared. Normative definition:
[SPEC.md §5](../../SPEC.md).

## The three kinds

The kind fixes the aspect's role:

- **FACT** — an authored JSON assertion: units are USD, this
  convention holds, this is the definition of record. The `WITH`
  schema validates every gloss body. Constants and formulas are FACT
  aspects.
- **QUERY** — an SQL-grounded concept: revenue, dso, any metric. The
  gloss body is a **grounding** — SQL plus disclosed assumptions —
  validated against the fixed grounding schema; the `WITH` schema
  carries the ontology entry (description, indicators, rendering).
  The value materializes by running the grounding SQL (`read.<aspect>()`),
  never through a function. Anything the company revises — meaning,
  unit, owner — belongs in a gloss, not in the declaration, because a
  gloss supersedes and a declaration cannot once spoken under.
- **MEASUREMENT** — a statistical evaluation: min_max, outliers,
  relationship candidates. Never glossed: its value is the returning
  function's landed output — extracted at a pin
  (`SELECT f() FROM subject`) and served by `GLOSSARY()` at that pin,
  beside facts and groundings.

```glossql
DECLARE ASPECT unit WITH $${
  "type": "object",
  "properties": {"value": {"type": "string"}, "source_column": {"type": "string"}}
}$$ AS FACT;

DECLARE ASPECT min_max WITH $${
  "type": "object",
  "properties": {"min": {}, "max": {}}
}$$ AS MEASUREMENT ON COLUMN;
```

## Subjects and grain

A gloss attaches to a **subject**: a dataset, a table, a column, or a
declared relationship addressed by its pair path
(`orders.customer_id -> customers.id`; a composite key is a tuple
endpoint). The optional `ON DATASET | TABLE | COLUMN | RELATIONSHIP |
SOURCE` list is the aspect's **grain** — the subject classes glosses
may attach to. Absent, the aspect speaks to all grains. Disclosure
stays within grain: absence shows only on subjects the aspect is
declared for.

A grain may carry a **condition**: `ON COLUMN WHEN role = 'measure'`
names a sibling aspect and a value, and the aspect is owed on a
subject only while that sibling's winning slot carries the value. The
condition bounds disclosure and the counts derived from it — it never
gates writes.

`SOURCE` grain makes the subject a declared source's name. Sources are
workspace rows, so source-grain slots read, supersede, and disclose
across every dataset — what one onboarding deposits, the next dataset
reads.

## Change

Multiplicity lives inside the body — array-typed schemas — never in
extra statements. Re-declaring an aspect with identical content is a
no-op. Changing it while glosses under it exist is refused: existing
bodies never silently stop matching their schema.

```glossql
GLOSS revenue ON fin.journal_lines AS $${
  "sql": "SELECT debit_amount - credit_amount FROM journal_lines WHERE account_type = 'revenue'",
  "assumptions": [
    {"dimension": "sign", "assumption": "ledger stores debits positive",
     "basis": "column_stats", "confidence": 0.9}
  ]
}$$;
```
