# 03 · Metric `dso` — TRANSCRIBES (metric = QUERY aspect, run as its SQL)

Source: `dataraum-context/packages/dataraum-config/verticals/finance/metrics/working_capital/dso.yaml`
(persisted per `metrics` / `metric_parameters` / `metric_derives_from`, engine schema.sql)

```yaml
graph_id: dso
version: '1.0'
metadata:
  name: Days Sales Outstanding
  description: Average days to collect payment after sale
  category: working_capital
  tags: [ar, collection, working-capital]
output: {type: scalar, metric_id: dso, unit: days, decimal_places: 1}
parameters:
  days_in_period:
    type: integer
    default: 30
    options: [30, 90, 365]
    derivation: period_grain
dependencies:
  accounts_receivable:
    type: extract
    source: {standard_field: accounts_receivable, statement: balance_sheet}
    aggregation: sum
  revenue:
    type: extract
    source: {standard_field: revenue, statement: income_statement}
    aggregation: sum
  days_in_period: {type: constant, parameter: days_in_period, default: 30}
  dso:
    type: formula
    expression: (accounts_receivable / revenue) * days_in_period
    output_step: true
    validation:
    - {condition: 0 <= value <= 365, severity: warning, message: DSO outside typical range}
interpretation:
  ranges:
  - {min: 0,  max: 30,  label: EXCELLENT,  description: Very efficient collection}
  - {min: 31, max: 45,  label: GOOD,       description: Strong collection performance}
  - {min: 46, max: 60,  label: CONCERNING, description: Review collection processes}
  - {min: 61, max: 90,  label: POOR,       description: Significant working capital tied up}
  - {min: 91, max: 999, label: CRITICAL,   description: Urgent intervention required}
```

## Transcription

A metric is a **concept** — a QUERY aspect, declared like `revenue`
(fixture 01) and grounded in SQL like any grounding (fixture 07). The
running system says so: metrics move through declare → compose → execute,
where compose is an agent writing SQL over the input concepts' groundings
and execute runs that SQL (`pipeline/phases/metrics_phase.py`; the working
SQL lands as `sql_snippets`). The value is never returned by a function —
it materializes by running the metric's SQL.

```glossql
DECLARE ASPECT dso WITH $${
  "title": "Days Sales Outstanding",
  "description": "Average days to collect payment after sale",
  "x-kind": "metric",
  "x-category": "working_capital",
  "x-unit": "days",
  "x-decimal-places": 1,
  "x-parameters": {
    "days_in_period": {"type": "integer", "default": 30, "enum": [30, 90, 365]}
  },
  "x-interpretation": [
    {"min": 0,  "max": 30,  "label": "EXCELLENT"},
    {"min": 31, "max": 45,  "label": "GOOD"},
    {"min": 46, "max": 60,  "label": "CONCERNING"},
    {"min": 61, "max": 90,  "label": "POOR"},
    {"min": 91, "max": 999, "label": "CRITICAL"}
  ]
}$$ AS QUERY ON DATASET;

GLOSS dso ON fin AS $${
  "sql": "SELECT (sum(accounts_receivable) / sum(revenue)) * 30 FROM monthly_balances",
  "assumptions": [
    {"dimension": "inputs", "assumption": "accounts_receivable and revenue read per their concept groundings",
     "basis": "sql_snippets", "confidence": 0.9}
  ]
}$$;
```

## Findings

- **TRANSCRIBES as a QUERY aspect.** The v0.3 lifecycle maps one to one:
  *declare* is the aspect (the yaml's ontology half — name, category, unit,
  parameters, interpretation — is the `WITH` schema, like fixture 01's
  `x-indicators`); *compose* is an agent glossing the composed SQL; *execute*
  is running that SQL. The dependency DAG and formula dissolve into the
  composed SQL — they were always a description of SQL to be written.
- **There is no function here.** A function is a measurement or a detector
  (engine machinery as a rhai script); a metric is neither — it runs as its
  SQL. The earlier transcription of this fixture as `DECLARE FUNCTION dso`
  was wrong and is superseded by this one (2026-08-03).
- Parameter mechanics (`days_in_period`, `derivation: period_grain`) ride
  with the composing agent in v0.3 — the chosen value is baked into the
  composed SQL. Whether parameter variants are separate glosses **closed
  2026-08-06** (fixture 16 §3): the parameter was the window, and the window
  belongs to the reader — definitions are grain-free, windows are read
  policy, and a verified composition may be recorded as the metric's gloss.
- The yaml's step validation (`0 <= value <= 365`) is adjudication —
  witness territory (fixture 04), open with the witness questions.
