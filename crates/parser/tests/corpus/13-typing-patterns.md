# 13 · Typing patterns and null values — config as glosses

Source artifacts (verified 2026-08-03):
`packages/dataraum-config/phases/typing.yaml` — the pattern list; a real
pattern carries name, regex, inferred_type, examples, ambiguity flag,
standardization SQL (`STRPTIME("{col}", '%d.%m.%Y')`), case sensitivity,
locale, and PII marks, plus file-level `min_confidence` and sample size.
`packages/dataraum-config/null_values.yaml` — categorized null strings with
per-value flags. `packages/engine/src/dataraum/core/overlay.py`
(`_apply_type_pattern`, `_apply_null_value`) — merges human teaches into
those files: a hand-built base-plus-amendment mechanism.

Fork tested: a dedicated `DECLARE PATTERN [regex] FOR [TYPE | NULL_VALUE]`
head fails transcription — the real pattern shape needs eight more fields
than a regex and a target, so the head grows back into a JSON body with a
keyword in front, and the grammar by one head. The surviving fork: the
configs are FACT glosses on the dataset. The recipe's author reads the
latest body; the base set is written at vertical replay; a teach is a human
re-gloss superseding whole-body (approved: the bodies are small JSON
documents, edited and read as wholes). Base-vs-taught falls out of the
(subject, aspect, actor kind) key; the overlay module has nothing left to do.

```glossql
USE fin;

DECLARE ASPECT null_values WITH $${
  "type": "object",
  "properties": {
    "values": {"type": "array", "items": {"type": "object",
      "properties": {"value": {"type": "string"},
                     "case_sensitive": {"type": "boolean"},
                     "category": {"type": "string"}},
      "required": ["value"]}}},
  "required": ["values"],
  "additionalProperties": false
}$$ AS FACT;

GLOSS null_values ON fin AS $${"values": [
  {"value": "", "category": "standard"},
  {"value": "NULL", "case_sensitive": false, "category": "standard"},
  {"value": "#N/A", "category": "spreadsheet"},
  {"value": "TBD", "category": "missing_indicator"}
]}$$;

DECLARE ASPECT type_patterns WITH $${
  "type": "object",
  "properties": {
    "min_confidence": {"type": "number"},
    "patterns": {"type": "array", "items": {"type": "object",
      "properties": {"name": {"type": "string"},
                     "pattern": {"type": "string"},
                     "inferred_type": {"type": "string"},
                     "ambiguous": {"type": "boolean"},
                     "standardization_expr": {"type": "string"},
                     "examples": {"type": "array"}},
      "required": ["name", "pattern", "inferred_type"]}}},
  "required": ["patterns"],
  "additionalProperties": false
}$$ AS FACT;

GLOSS type_patterns ON fin AS $${"min_confidence": 0.85, "patterns": [
  {"name": "iso_date", "pattern": "^\\d{4}-\\d{2}-\\d{2}$",
   "inferred_type": "DATE", "examples": ["2024-01-15"]},
  {"name": "eu_date", "pattern": "^\\d{1,2}\\.\\d{1,2}\\.\\d{2,4}$",
   "inferred_type": "DATE",
   "standardization_expr": "try_to_date(\"{col}\", '%d.%m.%Y')"},
  {"name": "us_date", "pattern": "^\\d{1,2}/\\d{1,2}/\\d{2,4}$",
   "inferred_type": "DATE", "ambiguous": true,
   "standardization_expr": "try_to_date(\"{col}\", '%m/%d/%Y')"}
]}$$;
```

A teach is the same statement on a human connection — the whole amended body,
read first via `GLOSSARY(fin.null_values)`:

```glossql
GLOSS null_values ON fin AS $${"values": [
  {"value": "", "category": "standard"},
  {"value": "NULL", "case_sensitive": false, "category": "standard"},
  {"value": "#N/A", "category": "spreadsheet"},
  {"value": "TBD", "category": "missing_indicator"},
  {"value": "~~~~~", "category": "taught"}
]}$$;
```

## Findings

- Zero grammar change: both artifacts transcribe with existing constructs —
  `DECLARE ASPECT … AS FACT` + `GLOSS`, subject = the dataset.
- **Exprs speak the substrate's SQL** (respelled 2026-08-04, ruled with the
  M4 pass): the source yaml's `STRPTIME(…)` is DuckDB vocabulary; migrated
  patterns spell `try_to_date` / `try_to_timestamp` — the engine's NULL-on-
  failure parsers, registered because the substrate's own `to_date` aborts
  a whole scan on one dirty value and ships no try_ variant. A pattern's
  expr must never be able to error a scan: it lands inside a recipe, and
  one dirty value must cost a NULL cell, never the import.
- Whole-body supersession replaces the overlay's per-entry merge: the teach
  skill does read–amend–re-gloss. The human slot supersedes the base slot by
  the ordinary key; no merge machinery survives.
- **The author is the consumer** (respelled 2026-08-04, with the
  authored-typing ruling): the patterns' reader was the typing function
  (`infer_types ACCEPTS (type_patterns, null_values)`); it is now the
  agent authoring a recipe, reading the same glosses through
  `GLOSSARY(fin::type_patterns)` before writing the casts — a pattern's
  `standardization_expr` is exactly the SQL the author pastes. The corpus
  evidence for the `ACCEPTS (aspect, …)` form lives in fixture 11:
  `outliers ACCEPTS (column_profile)`, where the server hands the script
  the aspect's current value as its context document — one schema,
  referenced, never copied.
