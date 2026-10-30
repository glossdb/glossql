---
name: gloss-kpi
description: The KPI kit's vocabulary for a gloss workspace — what each aspect means, the order of a round, the keys a metric's grounding carries. Use after gloss_kit('kits/kpi/kit.sql') when glossing columns, tables, sources and metrics.
---

# The KPI kit: what it teaches

For an actor working in a DuckDB that has the glossql extension loaded and
this kit declared. The extension knows the grammar and the store; this
file is the vocabulary, the order of a round, and the keys a metric's
grounding carries. Everything live is read through the language, never
taught: the tables, the declarations, the glossary.

## Load and check

```sql
FROM gloss_kit('kits/kpi/kit.sql');     -- one row per statement
SELECT * FROM glossql.kits;                -- what this workspace runs
SELECT name, kind, grains FROM glossql.aspects;
```

## The words

| Aspect | On | Body | Says |
|---|---|---|---|
| `meaning` | table, column, relationship | `value`: a sentence; `term`: the business term | what it is |
| `role` | column | `value`: `key`, `measure`, `dimension`, `timestamp`, `attribute` | what kind of column; the first claim, the others wait on it |
| `behavior` | measure columns | `value`: `stock`, `flow`, `none`; `grounds` | how the number adds over time; `none`: examined, does not apply |
| `unit` | measure columns | `value`: the unit; `source_column`: where a row's unit sits | what the number counts |
| `dimension` | dimension columns | `value`: `primary`, `supporting`, `none`; `grounds` | whether reports slice by it |
| `entity` | table | `value`: what one row is; `role`; `grain`; `time_axis`; `identity_columns` | the identity every aggregate stands on |
| `conventions` | source | any object | fiscal calendar, signs, what nulls mean |
| `definitions` | dataset | `definitions`: one object per metric name | the handbook: meaning, unit, owner, source of each metric |

`role`, `behavior` and `unit` are judged by `slot_entropy`: when the
agent's and the human's values differ, the claim is `contested`, its
value withheld, the verdict `red` above 0.7. `meaning`, `entity` and
`dimension` take both speakers without a detector.

## The order of a round

1. Read what is owed. Every gap is a row, never an omission:
   `SELECT * FROM GLOSSARY(orders) WHERE state <> 'current';`
2. Gloss `role` on every column first. `behavior` and `unit` are owed
   only on measures, `dimension` only on dimensions, so the backlog opens
   as roles land.
3. Extract before claiming. `SELECT column_profile() FROM orders.amount;`
   and `SELECT outlier_profile() FROM orders.amount;` land evidence that
   `GLOSSARY()` serves beside the glosses; cite it in `grounds`.
4. Gloss with grounds: `GLOSS behavior ON orders.amount AS $${"value":
   "flow", "grounds": "sums over orders, never a balance"}$$;`. You speak
   as `agent`. A person's ruling arrives as `human`, which the wrapper
   sets with `SET VARIABLE gloss_actor = 'human'`; never set it yourself.
5. Read the verdicts: `SELECT * FROM ATTEST(orders.amount::role);`. A red
   band is a disagreement to resolve by re-glossing, not to overwrite.

## Metrics

A metric is a QUERY aspect, declared thin and grounded by a gloss on the
dataset. The grounding is served as a view under the metric's name.

```sql
DECLARE ASPECT revenue WITH $${"title": "Revenue", "x-kind": "measure"}$$ AS QUERY;
GLOSS revenue ON fin AS $${
  "sql": "SELECT customer, sum(amount) AS revenue FROM fin.orders GROUP BY customer",
  "behavior": "flow", "grain": ["customer"], "axes": ["customer"],
  "assumptions": [{"dimension": "scope", "assumption": "amounts are net of tax",
                   "basis": "column_stats", "confidence": 0.9}]
}$$;
FROM fin.revenue;
```

The body's keys, and nothing else, by this kit's grounding schema:

- `sql` or `stopped`: the engine's contract. `stopped` records that you
  serve no number, and why: what is missing, where, what closes it.
- `assumptions`: disclosed, each with its `assumption`; `dimension`,
  `basis` and `confidence` in 0..1 when you have them.
- `behavior`: `stock` or `flow`. A reader that windows the grounding takes
  the last value per window for a stock and the sum for a flow. Absent
  reads as flow.
- `grain`: the served columns whose combination identifies a row. A
  reader that aggregates refuses a frame that breaks it.
- `axes`: the served columns the metric is sliced by, in order; `[]`
  closes them all, which holds for a ratio or a distinct count.

Re-glossing supersedes your own slot; a human's slot stands beside it.
What the company revises, the metric's meaning, unit, owner or source,
goes into `definitions` on the dataset, never into the declaration: a
declaration cannot change once anything is glossed on it.

## What this kit does not do

It records `behavior`, `grain` and `axes` for a reader that windows and
slices, and ships none: this extension serves the grounding as the view
it is. Doors, identity and the cube stay outside.
