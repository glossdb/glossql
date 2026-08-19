---
name: glossql-functions
description: Write a function for a glossql workspace — measurements (RETURNS an aspect, query over data), check voices, and detectors (no RETURNS, query over the witness's slots). Every body is one SQL query. Use when a shipped measurement does not fit this dataset, when a validation needs its measuring half, or when debugging a function that abstains.
---

# Writing functions

A function is declared over the door and run by the server. Every
body is **one SQL query** the engine plans and runs (read-only). Two
roles, told apart by shape:

- **A measurement or a voice** — it `RETURNS` an aspect, and its output
  is validated against that aspect's schema. Its query runs over data,
  composing everything a read can. Filling a MEASUREMENT aspect makes
  it a measurement; filling a FACT aspect makes it a **voice** in that
  aspect's slots, beside the agent's and the human's. The check half of
  a validation is a voice.
- **A detector** — no `RETURNS`. Named in a witness's `DETECTOR`
  clause; its query runs over the witness's `slots` relation.

## Read the closest one first

The reference library ships in every workspace and the body is a
column, so the library is readable through the door:

```sql
SELECT name, returns FROM functions ORDER BY name
```

```sql
SELECT script FROM functions WHERE name = 'rate_tolerance'
```

Read the one nearest your task before writing:

- `profile` — a measurement whose body is SQL over the engine's
  `profile` aggregate; the shape every new measurement takes.
- `outliers` — a measurement composing another measurement's
  aggregate inline (its fences derive from `profile(v)` in the same
  statement).
- `slot_entropy` — a detector.
- `rate_tolerance` — a detector over slot voices (authored expectation
  against check voice), which is the usual validation shape.

## Declaration

The body rides the statement. There is no path and no file: an agent
over the door has statements, so a function is written the way
everything else is. A measurement's body is one SQL query:

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

- `FOR` scopes to a dataset, or `GLOBAL`.
- `RETURNS` names the aspect the output fills, validated against that
  aspect's JSON Schema at extraction.
- The body composes its context inline: a glossed value is a read
  over the glossary, another measurement's landed value a read over
  `measurements`, and a statistic it needs is the same aggregate,
  computed in place.
- The body composes anything a read can: tables, `read.<aspect>()`
  groundings, the declaration relations as plain tables, and the
  shipped `profile` aggregate beside the engine's own (`mad` and
  `entropy` ride inside its struct: `profile(v)['numeric']['mad']`,
  `profile(v)['entropy']`). `$subject` arrives as a string literal wherever the body writes
  it; `subject_column($subject)` is the subject's column as a relation
  named `v`, for column-grain functions whose body cannot know the
  column's name.
- The result lands by a fixed rule: one row × one column → the value;
  one row → an object of its columns; many rows → an array of row
  objects. NULL keys are omitted. Size it like a claim — something that
  wants to be a table is a read, not a measurement.
- A re-declare supersedes. The body cannot contain `$$` — it would
  close the statement early.

The name is the key, workspace-wide: there is one row per name, and a
re-declare **replaces** it — old measurements sit at pins that no longer
resolve, so the next extraction recomputes. Take a shipped
name only on purpose: the store holds one body per name, and the
library's own survives nowhere else in the workspace, so replacing
`profile` costs the tested one until a rebuild lands it again.
For a MEASUREMENT this is the only route (a second name
returning the same aspect is refused); for anything else, use your own
name.

## Abstain, never throw

When the subject does not fit, the answer is a fact, not a failure:
`applicable: false` with a `reason` — a text column has no outliers,
and the reason should name the lead (a date landed as text is a typing
gap in the recipe, not a dead end). Keep `applicable` in the aspect's
`required`. An abstention that is *starvation* rather than a mismatch
is worth saying out loud in the read-back — it is a finding about the
data's shape.

## Detectors

A detector's query plans over the **`slots`** relation — one row per
slot on the witness's aspect:
`(subject, aspect, kind, witness, actor, speaker, written_at, body)`.
`speaker` is `human` | `agent` | `function`. `body` arrives **typed**:
when every slot body is a JSON object (an aspect's slots share its
schema, so they do), the column is a struct and fields read as
`body['tolerance']`, nested arrays through `unnest(body['metrics'])`;
anything else leaves `body` as text. A field no slot carries is a
plan-time error naming it — which is the read telling you what is owed
(an expectation gloss, usually), not a case to code around.

The witness's `THRESHOLD` binds as `$threshold` (NULL when the witness
declares none — coalesce your default). The query returns one row per
subject with `subject`, `band` (green|yellow|orange|red), and `score`
(0..1); the engine completes the attest row with the witness, its
aspect, and its own clock. One query answers every subject — group by
`subject`, and keep a row for subjects whose slots say nothing (a
LEFT JOIN back to `SELECT DISTINCT subject FROM slots`), because no
row means no verdict, not green.

```glossql
DECLARE FUNCTION spread_bands FOR ops AS $$
  WITH s AS (
    SELECT subject,
           CASE WHEN count(*) <= 1 THEN 0.0
                ELSE (count(DISTINCT body['value']) - 1.0) / (count(*) - 1.0)
           END AS score
    FROM slots GROUP BY subject
  )
  SELECT subject, score,
         CASE WHEN score = 0.0 THEN 'green'
              WHEN score <= coalesce($threshold, 1.0) THEN 'yellow'
              ELSE 'red' END AS band
  FROM s
$$;
```

Judgment lives in detectors and in read policy, never in results — no
measurement writes a verdict into data.

```glossql
DECLARE WITNESS on_time_w ON on_time_completion BY (AGENT, HUMAN)
  DETECTOR rate_tolerance THRESHOLD 0.0;
```

## A bespoke function is the installation's recorded thinking

A workspace-scoped function (`FOR <dataset>`) is the honest home for
method that is specific to this data. When a shipped measurement
starves on this dataset's shape — `behavior_evidence` on a schema whose
only edges are document-keyed, say — a function that encodes how the
question is decided *here* beats a pile of hand judgments: it is
versioned, re-runnable, and its abstentions stay as honest as the
library's.

## Running

```glossql
SELECT outliers() FROM work_orders.duration_min;
```

Extraction computes at the read's pin — the data and declarations the
statement resolved — and lands a `measurements` row; the same pin serves
it back, and any input moving recomputes. A body carrying a `summary`
object serves the summary alone — the full body reads back through
`GLOSSARY(subject::aspect)`, uncapped.
