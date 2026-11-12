# Validation: trial balance

A trial balance check validates that total debits equal total credits.
The example states the expectation as an annotation on the dataset,
declares a function that measures it and a function that scores the
result, binds them in a test (`DECLARE TEST`) and reads the test
results.

```glossql
DECLARE ASPECT trial_balance WITH $${
  "type": "object",
  "properties": {
    "tolerance": {"type": "number"},
    "severity": {"enum": ["critical", "warning", "info"]},
    "outcome": {"type": "string"},
    "guidance": {"type": "string"},
    "cycles": {"type": "array", "items": {"type": "string"}},
    "conventions": {"type": "array", "items": {"type": "string"}}
  }
}$$ AS FACT ON DATASET;

GLOSS trial_balance ON fin AS $${
  "tolerance": 0.01,
  "severity": "critical",
  "outcome": "Total debits must equal total credits across all account types.",
  "guidance": "Join the trial balance table with the chart of accounts before summing by account type.",
  "cycles": ["journal_entry_cycle", "accounts_receivable", "accounts_payable"],
  "conventions": ["sign_natural_balance"]
}$$;

DECLARE FUNCTION trial_balance_check FOR fin AS $$/* debits minus credits per period, as a breach rate */$$
  RETURNS trial_balance;

DECLARE FUNCTION balance_states FOR fin AS $$/* detector: gives the balance values a state */$$;

DECLARE TEST tb ON trial_balance BY (HUMAN)
  DETECTOR balance_states;

SELECT * FROM TESTS(fin.trial_balance);
```
