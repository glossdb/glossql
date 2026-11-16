# Review question and answer

A review question asks a person to decide a definition. The example
shows the answer: an annotation written by the person, which takes
precedence over the agent's.

```glossql
GLOSS definitions ON fin AS $${"definitions": {
  "revenue": {"meaning": "Product and Service revenue families only"},
  "gross_profit": {"meaning": "revenue minus COGS"}
}}$$;
```

The language has no statement for the question itself. This is by
design: a question is not stored, only its answer.

```glossql-gap
ASK definitions ON fin WITH $${"question": "revenue scope: which accounts?"}$$;
```
