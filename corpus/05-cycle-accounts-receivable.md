# Business process: accounts receivable stages

A business process moves through ordered stages: an invoice is created,
sent, due, paid. The example declares the stages with their order and
the closing stage, then maps the values of a status column to them.

```glossql
DECLARE PROPERTY ar_stage WITH $${
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

Accounts receivable and accounts payable are the two directions of one
settlement process. That is an annotation on the dataset:

```glossql
DECLARE PROPERTY cycles WITH $${
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
