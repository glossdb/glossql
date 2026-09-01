# 21 · Grounding grain / stock serving shape — TRANSCRIBES (grain key, standard grounding schema)

Source: the 2026-08 finance integration-run harvest — an agent's
`ar_balance` grounding, a running balance of signed events marked
`"behavior": "stock"`. Every row carried the exact cumulative level
(9,946,340.30 against a ground truth of 9,946,340.31), and the cube
published ~56× the true value.

## Transcription

Two competing forms for the same artifact. Both parse; the fork is
relational, not syntactic.

The losing form serves the running level at event grain — one output
row per contributing event. SQL's default RANGE frame gives all
same-date rows the identical day-end total, so the frame holds dozens
of correct duplicates per date:

```glossql
GLOSS ar_balance ON fin AS $${
  "sql": "SELECT date, sum(delta) OVER (ORDER BY date) AS value FROM events",
  "behavior": "stock"
}$$;
```

The surviving form collapses the events to the frame's own grain
first, then runs the level, and declares that grain:

```glossql
GLOSS ar_balance ON fin AS $${
  "sql": "WITH daily AS (SELECT date, sum(delta) AS delta FROM events GROUP BY date) SELECT date, sum(delta) OVER (ORDER BY date) AS value FROM daily",
  "behavior": "stock",
  "grain": ["date"]
}$$;
```

A multi-entity stock declares the entity beside the period —
`"grain": ["date", "account_id"]` — with the window partitioned by
the entity and the entity served as a column.

## Findings

- **TRANSCRIBES.** `grain` enters the standard grounding schema:
  optional, an array of served column names whose combination
  identifies a row, `minItems` 1.
- **Why nothing below the grounding could catch it.** A stock frame
  may legitimately carry several rows per period — one per account —
  and the cube's stock verb must sum what stands at the bucket's
  latest date (that is how a multi-account balance totals).
  Duplication is indistinguishable from multi-entity at that layer;
  only a declaration separates them. The artifact's own downstream
  consumers read the frame correctly (`last_value` per window) — the
  served relation is what breaks every aggregating reader.
- **Ruling.** A declared grain is validated where the frame is built:
  a frame that breaks it abstains, the reason naming the columns and
  the road out. Absent, the shape is undeclared and disclosed as
  such — never refused.
- `grain` stays a statistics dimension in the question round's gate:
  it is settled by measurement against the declaration, never asked
  of a human.
