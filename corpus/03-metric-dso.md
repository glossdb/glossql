# Metric: days sales outstanding

Days sales outstanding (DSO) is the average number of days between a
sale and its payment. The example defines the metric for the dataset
`fin` in SQL, with its meaning, its interpretation ranges as a key of
the body that a reader of its own reads, and the assumption the
definition rests on. Its unit is an annotation on the value column.

```glossql
GLOSS dso ON fin AS $${
  "kind": "metric",
  "sql": "SELECT period, (sum(accounts_receivable) / sum(revenue)) * 30 AS value FROM monthly_balances GROUP BY period",
  "meaning": "Average days to collect payment after sale",
  "x-interpretation": [
    {"min": 0,  "max": 30,  "label": "EXCELLENT"},
    {"min": 31, "max": 45,  "label": "GOOD"},
    {"min": 46, "max": 60,  "label": "CONCERNING"},
    {"min": 61, "max": 90,  "label": "POOR"},
    {"min": 91, "max": 999, "label": "CRITICAL"}
  ],
  "assumptions": [
    {"dimension": "inputs", "assumption": "accounts_receivable and revenue read per their concept definitions",
     "basis": "sql_snippets", "confidence": 0.9}
  ]
}$$;

GLOSS unit ON fin.dso.value AS $${"value": "days"}$$;
```
