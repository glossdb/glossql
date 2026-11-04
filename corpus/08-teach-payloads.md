# 08 · Teach payloads (8 types) — TRANSCRIBE (teach = re-gloss on a human connection)

Source: `dataraum-context/packages/cockpit/src/tools/teach.validation.ts` —
TYPE_SCHEMAS roster: `type_pattern, null_value, unit, relationship, hierarchy,
validation, cycle, metric` (all Zod-validated; a 9th direct-read type
`expected_dependency` lives outside the registry, `core/overlay.py:49-55`).

## Transcription

There is no teach construct. A teach is an ordinary statement on a connection
whose actor is a human; supersession by (subject, aspect, actor kind) does the
rest — the human's gloss supersedes the human's, and the detector sees both
the human and agent slots.

`unit` teach `{table, column, unit}`:

```glossql
GLOSS unit ON orders.amount AS $${"value": "EUR"}$$;
```

`relationship` teach `{action: confirm|add, from_column_id, to_column_id}`:

```glossql
DECLARE RELATIONSHIP orders.customer_id -> customers.id;
```

`hierarchy` teach, `add` action (reconciled 2026-08-06: a hierarchy is
recorded as same-table relationships, finer → coarser, with the grounds
on the pair — the dimensions ruling — never a levels blob):

```glossql
DECLARE RELATIONSHIP customers.city -> customers.region;
DECLARE RELATIONSHIP customers.region -> customers.country;
GLOSS meaning ON customers.city -> customers.region AS $${"value": "taught drill-down level"}$$;
```

`type_pattern` and `null_value` — the workspace-scoped vocabulary teaches the
old spec wrestled with (§8.3) — land as dataset-scoped FACT glosses, which is
their real scope:

```glossql
GLOSS type_patterns ON fin AS $${
  "items": [{
    "name": "eu_date",
    "pattern": "^\\d{2}\\.\\d{2}\\.\\d{4}$",
    "inferred_type": "DATE",
    "standardization": "try_to_date(\"{col}\", '%d.%m.%Y')"
  }]
}$$;
```

(Expr vocabulary respelled 2026-08-04 with fixture 13: patterns speak the
substrate's SQL, and only NULL-on-failure functions may appear in them.)

`validation` / `cycle` / `metric` teaches are fixtures 04/05/03 authored on a
human connection. A relationship *reject* teach has no statement: rejected
does not exist — it is simply not declared (fixture 07's candidate-memory
note).

## Findings

- **TRANSCRIBES — teach dissolves.** All eight types are covered by GLOSS,
  DECLARE RELATIONSHIP, or the fixtures they compose; the dedicated teach
  vocabulary, its Zod registry, and the §8.3 scope mismatch all disappear.
- The `standardization` SQL string rides inside the JSON body — authored
  content, opaque to the grammar, executable by whichever function applies the
  pattern.
- A removal teach is `DELETE FROM glossary WHERE ...` — the glossary is an
  ordinary queryable relation.
