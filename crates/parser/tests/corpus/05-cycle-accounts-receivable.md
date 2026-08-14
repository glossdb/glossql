# 05 · Cycle `accounts_receivable` + family `settlement` — TRANSCRIBES (in-blob)

Source: `dataraum-context/packages/dataraum-config/verticals/finance/cycles.yaml`

```yaml
cycle_types:
  accounts_receivable:
    description: "AR collection cycle: customer invoices settled by INCOMING flows …"
    business_value: high
    aliases: [ar_cycle, receivables_cycle, collection_cycle]
    typical_stages:
      - {name: "Invoice Created",  order: 1, indicators: [created, new, open, issued]}
      - {name: "Invoice Sent",     order: 2, indicators: [sent, delivered, notified]}
      - {name: "Payment Due",      order: 3, indicators: [due, outstanding, pending]}
      - {name: "Payment Received", order: 4, indicators: [paid, received, collected, cleared]}
    completion_indicators: [paid, collected, cleared, closed]
    feeds_into: [journal_entry_cycle]
cycle_families:
  settlement:
    directions: {incoming: accounts_receivable, outgoing: accounts_payable}
```

Asserted side: `detected_business_cycles` (schema.sql) — stages, status_table,
status_column, completion_value, completion_rate, family, direction, evidence.

## Transcription

Stage order and terminal labels are schema annotations (JSON Schema has no
native order; rendering conventions already ride the schema, so `x-order` /
`x-terminal` are consistent). Value→stage bindings are the gloss on the
status column.

```glossql
DECLARE ASPECT ar_stage WITH $${
  "type": "object",
  "properties": {
    "mappings": {
      "type": "array",
      "items": {
        "type": "object",
        "required": ["token", "stage"],
        "properties": {
          "token": {"type": "string"},
          "stage": {"enum": ["created", "sent", "due", "paid"]}
        }
      }
    }
  },
  "x-order": ["created", "sent", "due", "paid"],
  "x-terminal": ["paid"]
}$$ AS FACT ON COLUMN;

GLOSS ar_stage ON invoices.status AS $${
  "mappings": [
    {"token": "delivered", "stage": "sent"},
    {"token": "paid", "stage": "paid"}
  ]
}$$;
```

The family and its directions are dataset-level facts:

```glossql
DECLARE ASPECT cycles WITH $${
  "type": "object",
  "properties": {
    "families": {"type": "object"}
  }
}$$ AS FACT ON DATASET;

GLOSS cycles ON fin AS $${
  "families": {
    "settlement": {"incoming": "accounts_receivable", "outgoing": "accounts_payable"}
  }
}$$;
```

## Findings

- **TRANSCRIBES in-blob.** The old track needed three constructs (ordered
  VALUES aspects, argumented applications, `DECLARE CYCLE FAMILY`); here it is
  two FACT aspects.
- Progression and completion checks (the derived validations of the old §3.2)
  become functions reading `x-order` / `x-terminal` — witnesses adjudicate,
  ATTEST serves the verdict.
- Completion-rate is a MEASUREMENT aspect filled by a function when wanted.
- **INFORMATION LOST, by decision:** `feeds_into`, per-stage indicator lists,
  aliases — authoring guidance for the detecting function, script-side.
