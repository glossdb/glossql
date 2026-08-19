# Functions

A **function** is declared analytical machinery: profiling, quality
checks, detection. Every body is one SQL query the engine plans and
runs. It is either a **measurement** — it `RETURNS` an aspect and its
query runs over data — or a **detector** — no `RETURNS`, its query
runs over a witness's `slots` relation. Role is told by shape.
Metrics are neither: a metric is a QUERY aspect, run as its grounding
SQL. Normative definition: [SPEC.md §6](../../SPEC.md).

## Measurements and voices

A `RETURNS` body is SQL — read-only, planned at the statement's pin,
composing anything a read can: tables, `read.<aspect>()` groundings,
the declaration relations, the shipped aggregates. The aspect's JSON
Schema is the one contract: output is validated against it at
extraction, and `GLOSSARY()` serves it as-is.

- Filling a **MEASUREMENT** aspect makes the function that aspect's
  producer — exactly one function returns it.
- Filling a **FACT** aspect makes the function a **voice**: a
  data-grounded speaker whose landed output — the measurement at the
  read's pin — joins the human's and agent's slots. The check half of
  a validation is a voice ([validation](validation.md)).

```glossql
DECLARE FUNCTION on_time_check FOR ops AS $$
  SELECT
    'a work order completes by its promised date; a late close is the exception' AS outcome,
    CASE WHEN count(*) = 0 THEN 0.0
         ELSE CAST(count(*) FILTER (WHERE completed_at > promised_at) AS DOUBLE) / count(*)
    END AS breach_rate
  FROM work_orders
$$ RETURNS on_time_completion;
```

`FOR` scopes to a dataset, or `GLOBAL`. The body rides the
declaration — there is no path and no file, so an agent over the door
authors a function the way it writes anything else, and the shipped
library reads back as worked examples: `SELECT script FROM functions`.

`ACCEPTS` mirrors `RETURNS` on the input side: it names the aspects
whose current values the server hands a script body as its context —
settings are context, never call arguments; calls are always bare
`f()`. A SQL body composes inline instead: a landed value is a read
over `measurements`, a needed statistic is the same aggregate computed
in place. A function whose ACCEPTS inputs are absent abstains and
names them — `missing_aspects` — so the gap reads as owed context, not
as an error.

## Extraction and the pin

```glossql
SELECT outliers() FROM work_orders.duration_min;
```

Extraction computes at the read's **pin** — the exact set of inputs,
data and declarations, the statement resolved — and lands one row in
the `measurements` relation. The same pin serves the row back; any
input moving makes a new pin, so there is no invalidation, only a
miss, and old rows stand as the drift record. A body that carries a
`summary` object serves the summary at extraction; the full value
reads back through `GLOSSARY(subject::aspect)`.

Functions never write the glossary, and no measurement writes a
verdict into data — judgment lives in detectors and read policy.

## Abstention

When the subject does not fit, the answer is a fact, not a failure:
`applicable: false` with a reason that names the lead (a date landed
as text is a typing gap in the recipe, not a dead end). An abstention
caused by starvation — the data's shape, not a mismatch — is itself a
finding.

## Detectors

A detector is named only in a witness's `DETECTOR` clause. Its query
plans over the `slots` relation — the witness's raw rows,
narrowed to its aspect, with `speaker` beside them and `body` typed by
the slots' own JSON. The witness `THRESHOLD` binds as `$threshold`.
The query
returns one row per subject — `subject`, `band`, `score` — and the
engine completes the attest row with the witness, its aspect, and its
own clock.
