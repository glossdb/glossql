# 01 · Concept `revenue` — TRANSCRIBES (concept = QUERY aspect)

Source: `dataraum-context/packages/dataraum-config/verticals/finance/ontology.yaml`

```yaml
- name: revenue
  description: Income from sales or services
  indicators: [revenue, sales, income, turnover, receipts]
  exclude_patterns: [cost, expense]
  kind: measure
  unit_from_concept: currency
```

Pack envelope around it (same file): `name: financial_reporting`,
`version: "1.0.0"`, pack-level `description`.

## Transcription

Each concept is its own aspect; the ontology entry rides the `WITH` schema as
annotations. `AS QUERY` makes its glosses groundings (fixture 07).

```glossql
DECLARE ASPECT revenue WITH $${
  "title": "revenue",
  "description": "Income from sales or services",
  "x-kind": "measure",
  "x-indicators": ["revenue", "sales", "income", "turnover", "receipts"],
  "x-exclude": ["cost", "expense"],
  "x-unit-from": "currency"
}$$ AS QUERY ON DATASET, TABLE;
```

The `compositions:` block in the same file (`whole: current_assets`,
`parts: [cash, accounts_receivable, inventory]`) goes in-blob on the whole's
aspect — multiplicity lives inside the schema, never in extra statements:

```glossql
DECLARE ASPECT current_assets WITH $${
  "title": "current_assets",
  "x-kind": "measure",
  "x-parts": ["cash", "accounts_receivable", "inventory"]
}$$ AS QUERY ON DATASET, TABLE;
```

## Findings

- Concept row: clean — one declaration, supersession per concept, roster = the
  list of declared aspects.
- In-blob composition drops the declaration-time membership check the engine's
  lint provides today (`concept_edge_store.py:78-90`): a dangling part name is
  silent. Accepted with the in-blob decision; a witness can check it.
- **DROPPED BY DESIGN — pack envelope.** `financial_reporting`, `version:
  "1.0.0"`: portability is postponed entirely; a vertical is a folder of
  scripts and aspect declarations, ported by copying.
