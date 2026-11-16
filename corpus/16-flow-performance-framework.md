# Flow: a metric scorecard

A scorecard is a set of metrics and validations over one dataset. The
example builds one for a general ledger: declare the measures and
metrics, define them in SQL, read them at any time grain, state the
formulas of the derived metrics, and declare the validations with their
tests.

## Declare the measures and metrics

```glossql
USE fin;

DECLARE ASPECT revenue WITH $${
  "title": "Revenue",
  "description": "Income recognized in the ledger",
  "x-kind": "measure", "x-unit": "currency"
}$$ AS VIEW;

DECLARE ASPECT accounts_receivable WITH $${
  "title": "Accounts Receivable",
  "description": "Open receivables — a stock at (account, period) grain",
  "x-kind": "measure", "x-unit": "currency"
}$$ AS VIEW;

DECLARE ASPECT dso WITH $${
  "title": "Days Sales Outstanding",
  "description": "Average days to collect a receivable",
  "x-kind": "metric", "x-unit": "days"
}$$ AS VIEW;

DECLARE ASPECT gross_profit WITH $${
  "title": "Gross Profit",
  "description": "Revenue less expenses",
  "x-kind": "metric", "x-unit": "currency"
}$$ AS VIEW;
```

## Define the measures

A definition names no time grain. It returns rows with the date and the
dimensions as columns, and its assumptions cite the annotations they
rest on.

```glossql
GLOSS revenue ON fin AS $${
  "sql": "SELECT e.date, l.credit - l.debit AS value, l.cost_center, l.currency FROM journal_lines l JOIN journal_entries e ON l.entry_id = e.entry_id JOIN chart_of_accounts a ON l.account_id = a.account_id WHERE a.account_type = 'revenue'",
  "assumptions": [
    {"dimension": "sign", "assumption": "revenue accounts carry credit balances; value = credit - debit",
     "basis": "conventions gloss (natural balance)", "confidence": 0.95},
    {"dimension": "grain", "assumption": "both joins are grain-preserving; no header-value multiplication",
     "basis": "relationship glosses (grain-check counts on the pairs)", "confidence": 1.0},
    {"dimension": "additivity", "assumption": "a flow: sums are valid over any partition",
     "basis": "additivity_evidence on journal_lines.credit", "confidence": 0.95}
  ]
}$$;

GLOSS accounts_receivable ON fin AS $${
  "sql": "SELECT t.period, t.account_id, t.debit_balance - t.credit_balance AS value FROM trial_balance t JOIN chart_of_accounts a ON t.account_id = a.account_id WHERE a.account_type = 'asset' AND a.name LIKE '%Receivable%'",
  "assumptions": [
    {"dimension": "additivity", "assumption": "a stock at (account, period) source grain: a window takes its LAST period, never a sum across periods",
     "basis": "additivity_evidence on trial_balance.debit_balance", "confidence": 0.95},
    {"dimension": "scope", "assumption": "AR accounts selected by name within asset type",
     "basis": "chart_of_accounts meaning glosses", "confidence": 0.85}
  ]
}$$;
```

## Read at the reader's grain

The reader picks the time grain and the dimension. A view is read by
its name.

```glossql
SELECT date_trunc('month', date) AS month, sum(value) FROM revenue GROUP BY 1 ORDER BY 1;
SELECT date_trunc('decade', date) AS decade, sum(value) FROM revenue GROUP BY 1;
SELECT cost_center, sum(value) FROM revenue GROUP BY 1 ORDER BY 2 DESC;
```

## Formulas of the derived metrics

A formula is written over a window `w` and names no window.

```glossql
DECLARE ASPECT formulas WITH $${
  "type": "object",
  "properties": {"formulas": {"type": "object"}}
}$$ AS FACT ON DATASET;

GLOSS formulas ON fin AS $${
  "formulas": {
    "gross_profit": "revenue[w] - expenses[w]",
    "dso": "accounts_receivable[end of w] / revenue[w] * days[w]",
    "revenue_growth_pct": "(revenue[w] - revenue[w-1]) / revenue[w-1]"
  }
}$$;
```

## Validations

The expectation is an annotation, the check is a function that returns
the same aspect, and a test scores the two. The second expectation is
deliberately not 100 percent.

```glossql
DECLARE ASPECT journal_balanced WITH $${
  "type": "object",
  "properties": {
    "outcome": {"type": "string"},
    "tolerance": {"type": "number"},
    "severity": {"enum": ["critical", "warning", "info"]}
  }
}$$ AS FACT ON TABLE;

GLOSS journal_balanced ON journal_lines AS $${
  "outcome": "Total debits equal total credits, exactly.",
  "tolerance": 0.0,
  "severity": "critical"
}$$;

DECLARE ASPECT bank_reconciliation WITH $${
  "type": "object",
  "properties": {
    "outcome": {"type": "string"},
    "expected_rate": {"type": "number"},
    "tolerance": {"type": "number"},
    "severity": {"enum": ["critical", "warning", "info"]}
  }
}$$ AS FACT ON TABLE;

GLOSS bank_reconciliation ON bank_transactions AS $${
  "outcome": "The reconciled fraction matches the source's own dirt; 1.0 means overcleaning and is itself a failure.",
  "expected_rate": 0.895,
  "tolerance": 0.02,
  "severity": "warning"
}$$;

DECLARE FUNCTION journal_balance_check FOR fin
  AS $$/* debits equal credits, as a breach rate */$$
  RETURNS journal_balanced;
DECLARE FUNCTION reconciliation_check FOR fin
  AS $$/* the reconciled fraction against the source's own dirt */$$
  RETURNS bank_reconciliation;
DECLARE FUNCTION balance_states FOR fin AS $$/* detector: gives the balance values a state */$$;

DECLARE TEST journal_balanced_test ON journal_balanced BY (AGENT, HUMAN)
  DETECTOR balance_states THRESHOLD 0.5;
DECLARE TEST bank_reconciliation_test ON bank_reconciliation BY (AGENT, HUMAN)
  DETECTOR balance_states THRESHOLD 0.5;

SELECT journal_balance_check() FROM journal_lines;
SELECT reconciliation_check() FROM bank_transactions;
SELECT * FROM TESTS(journal_lines::journal_balanced);
```

## Read the scorecard

```glossql
SELECT subject, state, score FROM TESTS(fin) WHERE state = 'red';
SELECT subject, aspect, value FROM GLOSSARY(fin) WHERE state = 'current';
```

```glossql
SELECT date_trunc('week', date) AS week, sum(value) FROM revenue GROUP BY 1;
SELECT * FROM dso;
```
