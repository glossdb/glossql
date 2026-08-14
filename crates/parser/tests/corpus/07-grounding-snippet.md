# 07 · Grounding / `sql_snippets` — TRANSCRIBES (QUERY gloss, standard grounding schema)

Source: `sql_snippets` (engine schema.sql), semantic key:

```sql
CONSTRAINT uq_snippet_semantic_key UNIQUE (snippet_type, standard_field,
  statement, aggregation, predicate, schema_mapping_id, parameter_value)
```

Verified 2026-07-30: `schema_mapping_id` ≈ workspace; `parameter_value`
is constants-only (not groundings); the statement axis has exactly two values in
the finance vertical; relation is not a key member; grounding is one extract per
concept per run (`grounding_collision.py`). `provenance` on healthy rows carries
`assumptions: [{dimension, assumption, basis, confidence}]`; retained failures
carry `failure_mode ∈ (execution_failed, verifier_rejected, provenance_invalid,
disjoint_collision)`.

## Transcription

A grounding is a gloss of a QUERY aspect (fixture 01 declares `revenue`,
likewise `accounts_receivable`). The body validates against the **standard
grounding schema** — fixed, like the attest schema: `sql` required,
`assumptions[]` optional. Assumptions ride inside the grounding.

```glossql
GLOSS accounts_receivable ON fin.journal_lines AS $${
  "sql": "SELECT debit_amount - credit_amount FROM journal_lines JOIN chart_of_accounts USING (account_id) WHERE account_type = 'asset'",
  "assumptions": [
    {"dimension": "sign", "assumption": "ledger stores debits positive",
     "basis": "column_stats", "confidence": 0.9},
    {"dimension": "scope", "assumption": "asset accounts only",
     "basis": "chart_of_accounts", "confidence": 0.95}
  ]
}$$;
```

## Findings

- **TRANSCRIBES.** The old track's `DECLARE GROUNDING … IN … AS … WHERE …`
  construct with its own key debate collapses into the uniform gloss;
  supersession is (subject, aspect, actor kind).
- *Reconciled 2026-08-06*: the v0.3 snippet's scan target was an enriched
  view; the views ruling makes the grounding compose its grain-checked
  joins inline, so the subject is the fact table itself. Fixture 16 §2
  carries the full shape (grain-free extracts).
- **Coexistence, decided:** two QUERY glosses of the same aspect on different
  tables may coexist — two ways to calculate revenue arriving at the same
  number is the correct answer, not a conflict. Whether they reconcile is a
  witness's job (a detector runs both and returns band + score).
- The old track's weakest-assumption confidence gate is a detector's business:
  assumptions are in the body, readable by any function.
- **DROPPED BY DESIGN — retained failures.** `disjoint_collision` and friends
  were negative knowledge against re-authoring. Functions are deterministic —
  a rejected candidate is simply not declared; candidate memory, where wanted,
  is a `relationship_candidates`-style MEASUREMENT aspect (the function's
  cached output is glossary-visible without ever being declared).
