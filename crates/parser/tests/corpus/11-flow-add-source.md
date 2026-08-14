# 11 · Flow: add source — modelled as a statement sequence

Source: the running system's add-source pipeline (verified 2026-08-03).
Cockpit acquisition: `packages/cockpit/src/server/import-sources.ts`
(`persistImportSet`, collision guard), `routes/api/upload.ts`. Engine spine:
`packages/engine/src/dataraum/worker/workflows.py:211` (`AddSourceWorkflow`),
phases in `pipeline/phases/`. Ordered steps and what each produces:

| step | kind | produces (today) |
|---|---|---|
| stage upload / probe query | human | staged bytes / recipe spec |
| frame vertical | LLM | concepts, conventions, validations, cycles, metrics rows |
| register sources | deterministic | `sources` rows |
| import | deterministic | raw all-VARCHAR tables + table/column rows |
| typing | deterministic | `type_candidates`, `type_decisions`, typed + quarantine tables |
| statistics / eligibility / quality / temporal | deterministic | `statistical_profiles`, `column_eligibility`, `statistical_quality_metrics`, `temporal_column_profiles` |
| semantic_per_column | LLM | `semantic_annotations` per column |
| detect | deterministic | `entropy_objects`, `claim_witnesses`, `entropy_readiness` |
| promote_to_latest | deterministic | snapshot head flip |
| assess & auto-ground loop | LLM + orchestration | teach rows or `awaiting_input` park |

## Transcription

The actor kinds of the pipeline map onto the ways of speaking:
deterministic phases are **functions** (MEASUREMENT aspects), LLM phases
are **agent glosses**, teaches and parks are **human glosses**. The typing
phase maps to none of them — it becomes **authorship**, the
probe-and-recipe conversation below.

Human registers the source; the agent probes it — `PROBE` is the recipe
rehearsal (ruled 2026-08-04): the same SQL surface, the same path
resolution, executed at the source, landing nothing (v0.3's "probe query"
step, returned to its place). The result always carries its schema, so a
`LIMIT 0` probe of the final recipe SQL rehearses exactly the identity a
`DECLARE RECIPE` would stamp:

```glossql
USE fin;
DECLARE SOURCE erp_export SET (type: parquet, location: 'lake/erp');

PROBE erp_export AS $$SELECT * FROM read_parquet('orders/*.parquet') LIMIT 50$$;
PROBE erp_export AS $$SELECT count("order_date") AS filled,
       count(try_to_date("order_date", '%d.%m.%Y')) AS parsed
FROM read_parquet('orders/*.parquet')$$;
```

Typing is authored, not decided (ruled 2026-08-04): the recipe carries the
casts. The agent writes it from the probes and the taught patterns
(fixture 13 — still FACT glosses, now read by the author instead of
consumed by machinery); the human approves. The default is `SELECT *`;
the landed table is the typed table, snapshotted by Iceberg on every
import:

```glossql
DECLARE RECIPE orders ON fin FROM erp_export AS $$
  SELECT order_id,
         try_cast(amount AS DECIMAL(12,2)) AS amount,
         try_to_date(order_date, '%d.%m.%Y') AS order_date
  FROM read_parquet('orders/*.parquet')$$;

SELECT sum(amount) FROM orders;
```

The table is its recipe's result — identity is content, the hash of the
SQL and the schema it produces (the v0.3 engine already keys recipes this
way). The declaration's outcome carries the counts at the decision moment
(`DECLARE RECIPE orders ON fin (2 rows landed, 1 dropped)`). A data update
re-runs the same recipe and appends a snapshot; it must reproduce the
schema or it errors. Correcting a wrong recipe is removal first:

```glossql
DROP TABLE orders;
```

— refused while the table holds data; a wrong recipe is re-declared
under the same name and **supersedes**: the changed recipe drops the
old landing and lands fresh, sweeping the table's cached evidence
while glosses stay (ruled 2026-08-06 — runs 5 and 6 both dead-ended
on the earlier new-name rule). Rows the recipe filtered away are the
author's to judge, on the files, outside the box; the engine keeps one
number:

```glossql
SELECT dropped_rows_count FROM imports WHERE table_name = 'orders';
```

Framing the vertical is replaying the vertical folder's declarations —
aspects, check functions, witnesses (fixtures 01, 02, 04); no construct.

The deterministic profile plane — declared once (vertical/global), fanned
out per column (extraction grain is the subject; the fan-out is the
caller's loop, the grammar carries no ordering). The quality plane chains
on it through `ACCEPTS`: the outlier fences reuse the profile's quartiles
and MAD, and a re-profile kills the outlier cache. The temporal plane is
the same shape — window, cadence, completeness, gaps, all pure functions
of the landed column's instants (v0.3's `temporal_column_profiles`, minus
its one wall-clock field). An all-null column needs no machinery — the
author leaves it out of the recipe, or keeps it, deliberately:

```glossql
DECLARE ASPECT column_profile WITH $${
  "type": "object",
  "required": ["total", "null_ratio", "distinct"],
  "properties": {
    "total": {"type": "integer"}, "null_ratio": {"type": "number"},
    "distinct": {"type": "integer"}, "cardinality_ratio": {"type": "number"},
    "min": {}, "max": {}, "top_values": {"type": "array"},
    "lengths": {"type": "object"},
    "numeric": {
      "type": "object",
      "required": ["mean", "mad", "percentiles"],
      "properties": {"mean": {}, "stddev": {}, "mad": {},
                     "percentiles": {"type": "object"}}
    }
  }
}$$ AS MEASUREMENT;
DECLARE FUNCTION profile FOR GLOBAL FROM 'functions/profile.rhai'
  RETURNS column_profile;

DECLARE ASPECT outlier_profile WITH $${
  "type": "object", "required": ["applicable"],
  "properties": {"applicable": {"type": "boolean"},
                 "iqr": {"type": "object"}, "zscore": {"type": "object"}}
}$$ AS MEASUREMENT;
DECLARE FUNCTION outliers FOR GLOBAL FROM 'functions/outliers.rhai'
  ACCEPTS (column_profile)
  RETURNS outlier_profile;

DECLARE ASPECT temporal_profile WITH $${
  "type": "object", "required": ["applicable"],
  "properties": {"applicable": {"type": "boolean"},
                 "granularity": {"type": "string"},
                 "confidence": {"type": "number"},
                 "completeness": {"type": "object"}, "gaps": {"type": "object"}}
}$$ AS MEASUREMENT;
DECLARE FUNCTION temporal FOR GLOBAL FROM 'functions/temporal.rhai'
  RETURNS temporal_profile;

SELECT profile(), outliers() FROM fin.orders.amount;
SELECT temporal() FROM fin.orders.order_date;
```

Semantic annotation stays agent glosses (an agent connection, reading the
measurements first). A typing correction is a recipe correction — the
same SQL hands that wrote it — never a gloss:

```glossql
SELECT * FROM GLOSSARY(fin.orders.amount);

GLOSS meaning ON orders.amount AS $${"value": "gross invoiced amount per order line"}$$;
GLOSS behavior ON orders.amount AS $${"value": "flow"}$$;
GLOSS unit ON orders.amount AS $${"value": "EUR", "source_column": "currency_code"}$$;
```

Adjudication replaces the detect/resolve/readiness tail: witnesses on the
contested aspects, bands read back; the auto-ground loop is an agent skill
sweeping the attest relation and re-glossing where it may:

```glossql
SELECT * FROM ATTEST(fin::behavior);
SELECT subject, band, score FROM ATTEST(fin::unit) WHERE band = 'red';
```

A human closes what the agent could not — the same statements on a human
connection supersede the human slot (fixture 08); nothing parks in a queue
that the grammar knows about.

## Findings

- **Location is a root, not a glob** (respelled 2026-08-04, with the M3
  build-out): the original transcription had `location:
  'lake/erp/*.parquet'` while the recipe read `'orders/*.parquet'` — two
  globs that cannot compose. The source names the root directory; the globs
  belong to recipe SQL, resolving under it.
- **The flow transcribes with no flow construct.** Sequencing, retries,
  budgets, the replay-or-surface loop, and the column-limit gate are
  orchestration — app concern. The grammar carries no ordering surface at
  all (`SEQUENTIAL | PARALLEL` was dropped 2026-08-03): the caller either
  sends one extraction with many calls or several statements in sequence.
- **Typing is authored in the recipe** (ruled 2026-08-04 — the third
  respell of this finding, and the arc is the record): the original
  transcription hand-wrote `CREATE VIEW orders_typed` with strict CASTs;
  the M4 build derived the typed view from `type` glosses, with
  `orders_raw` and `orders_quarantined` beside it. Both put typing in
  machinery. The ruling puts it in authorship: the recipe carries the
  casts, written by the agent from probes and patterns, approved by the
  human, and the landed table is the typed table — served types are
  catalog fact, not judgment. `type`, `type_candidates`, and `eligible`
  leave the engine's vocabulary; the derived pair, the raw twin, and
  reactive view invalidation leave the engine.
- **Eligibility dissolved into authorship** (ruled 2026-08-04, hours after
  the projection gate landed): column selection is the recipe's SELECT
  list. The v0.3 findings stand — the phase's `ALTER`-drop was
  irreversible with no override, and its `WARN` tier was read by nobody —
  but the corrected answer is a line the author writes, not a gate the
  engine owns.
- **Table lifecycle is content identity plus coarse rules** (ruled
  2026-08-04, after holding the design against dbt and dlt): identity is
  the recipe-and-schema hash; a data update must reproduce the schema or
  error (the frozen-contract rule); `DROP TABLE` refuses while data
  exists (PoC). Replacement-by-new-name was part of this ruling and was
  superseded 2026-08-06 — a changed recipe now supersedes and re-lands;
  the full deletion cascade stays future work — tricky through
  relations and actor-generated SQL. No
  reactive invalidation of definitions anywhere: declared `ACCEPTS` edges
  and snapshot staleness are the only freshness mechanisms.
- **Filtered rows are the author's judgment** (ruled 2026-08-04): the
  engine keeps one number, `dropped_rows_count` — source rows minus
  landed rows. It arrives twice, deliberately: in the `DECLARE RECIPE`
  outcome at the decision moment, and in the `imports` relation beside
  `glossary` and `cache` for history — a third name in a convention
  agents already know (the store's relations read as plain tables), not
  a new convention. Which rows were dropped is the agent's question,
  answered on the files.
- **`PROBE` is a statement head** (ruled 2026-08-04, closing the fork):
  the first transcription bound probes by a path-prefix convention (the
  source name as the path's first segment) — magic an agent must be told.
  The ruled form mirrors the recipe: `PROBE source AS $$sql$$`, one
  concept (recipe-shaped SQL runs FROM a source; PROBE rehearses, RECIPE
  lands). The grammar grew by one head and lost a convention; the router
  stopped sniffing SELECTs for `read_*` references. The deciding
  advantage: a probe's result carries the schema it would land — `LIMIT 0`
  rehearses the identity.
- **Benford's law dropped** (ruled 2026-08-04): the only domain-leaning
  measurement in the deterministic plane — and the only numpy/scipy
  dependency in it — consumed by nothing as a signal. It never ports;
  whoever wants it writes a script.
- **`RETURNS` mirrors `ACCEPTS`** (ruled 2026-08-04): a function reads
  aspects and fills an aspect — both clauses reference the vocabulary, and
  the aspect's schema became the single, live contract (shape-descriptive:
  `required` for what every value carries, `properties` for what readers
  consume, open beyond that). The function witnesses died as ceremony —
  the `RETURNS` binding is what wires a function's cache into the collapse
  — and a function without `RETURNS` *is* a detector: role by shape, the
  attest contract engine-owned. Detectors migrate only where adjudication
  is real (same ruling): v0.3's readout detectors (`null_ratio`,
  `unit_entropy`, `business_meaning`, `type_fidelity`) dissolve into
  measurements agents read directly; unit detection dissolves into
  authorship — a recipe splits a value-carried unit into two columns with
  a regex.
- **The temporal family ports as one ordinary function** (2026-08-04):
  with tables that never change under their evidence, temporal needs no
  machinery of its own — window, cadence, completeness and gaps are pure
  functions of the landed column. Cadence is the nearest named grain to
  the median gap between *distinct* instants (a duplicate-heavy fact
  column counts each day once — v0.3 learned that the hard way);
  completeness counts calendar buckets over the column's own window, by
  calendar arithmetic, never by nominal grain seconds; gaps are the
  stretches beyond twice the median. Two v0.3 fields stayed behind:
  `is_stale` — the family's only wall-clock-dependent field, and a verdict
  about *now* is judgment, which lives in detectors and read policy, never
  in results (an agent who wants it is one `max(column)` from it) — and
  `last_period_complete`, low-signal by its own documentation and read by
  nothing. The cadence is what fed v0.3's slice-and-drill grain floor;
  here it is a measurement an agent reads before bucketing.
- `run_id` versioning and the promote/head flip are the cache and
  supersession mechanics — implementation by ground rule; the only surface
  is deleting cached rows to force recomputation (REFRESH was dropped
  2026-08-03 with the sqlparser respell).
- The vertical binding (`workspace_settings.active_vertical`) is replaying a
  folder (fixture 01) — confirmed against the real frame step, which writes
  seed rows exactly like a replay would.
