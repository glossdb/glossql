# Metric: days sales outstanding

Days sales outstanding (DSO) is the average number of days between a
sale and its payment. The example declares the metric as a view aspect
with its unit, parameter and interpretation ranges, then defines it for
the dataset `fin` in SQL, with the assumption the definition rests on.

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
}$$ AS VIEW;

GLOSS dso ON fin AS $${
  "sql": "SELECT (sum(accounts_receivable) / sum(revenue)) * 30 FROM monthly_balances",
  "assumptions": [
    {"dimension": "inputs", "assumption": "accounts_receivable and revenue read per their concept definitions",
     "basis": "sql_snippets", "confidence": 0.9}
  ]
}$$;
```
