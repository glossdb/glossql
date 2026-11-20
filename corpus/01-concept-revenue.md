# Glossary entry: revenue

A business glossary entry names a measure, describes it and lists the
words that point to it. In glossql the entry is the measure's view
definition: its SQL, its meaning and its tags, in one statement.

```glossql
GLOSS revenue ON fin AS $${
  "kind": "metric",
  "sql": "SELECT e.date, l.credit - l.debit AS value FROM journal_lines l JOIN journal_entries e ON l.entry_id = e.entry_id WHERE l.account_type = 'revenue'",
  "meaning": "Income from sales or services",
  "tags": ["revenue", "sales", "income", "turnover", "receipts"]
}$$;
```

A measure made of other measures names its parts in its SQL:

```glossql
GLOSS current_assets ON fin AS $${
  "kind": "metric",
  "sql": "SELECT date, sum(value) AS value FROM (SELECT date, value FROM cash UNION ALL SELECT date, value FROM accounts_receivable UNION ALL SELECT date, value FROM inventory) GROUP BY date",
  "meaning": "Cash, accounts receivable and inventory"
}$$;
```
