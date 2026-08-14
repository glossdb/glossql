# 15 · The consumption surface — TRANSCRIBES (the reads compose it; two gaps named)

Source: the v0.3 cockpit, swept 2026-08-06 — context assembly
(`packages/cockpit/src/tools/query-context.ts`), the tool set
(`look_*` / `why_*`), the drill
(`tools/drill-axes.ts`, `widgets/drillable-grid.tsx`), the
`/operating-model` tabs, the `/governance` briefing. This is the side
of the system fixtures 01–12 never transcribed; producer dispositions
flip-flopped exactly because their consumers were unread (lead's
diagnosis, 2026-08-06). Fixture 09 dropped the *serving construct* by
design — skills over `GLOSSARY()` are the bet; this fixture gives the
bet its concrete target: for each surface, the reads that must
compose it.

Rulings carried (lead, 2026-08-06): the bus-matrix UI was agent
context in disguise — if the reads cover it, the UI goes, and the
same test applies to every surface; the metrics graph is the UI
genuinely worth keeping; and the standing balance — what statistics
settle deterministically stays out of agent exploration, and
meaning-shaped judgment never moves into a function.

## 1. The context block (`query-context.ts`)

v0.3 fans eight builders into one cached prompt block: schema with
meaning and stock/flow markers, dimensions with relevance and
curation disclosure, relationships as runnable JOIN predicates,
entities with grain and the one event-time anchor, drivers, grain
notes, concepts, conventions. Every builder is a projection of
persisted rows — no builder computes.

The glossql composition, one read per builder family:

```glossql
USE fin;
SELECT * FROM imports;
SELECT subject, aspect, value FROM GLOSSARY(fin) WHERE state = 'current';
SELECT left_path, right_path FROM relationships;
SELECT count(*) FROM GLOSSARY(fin) WHERE state = 'unassessed';
SELECT subject, band, score FROM ATTEST(fin) WHERE band != 'green';
```

**TRANSCRIBES.** The disclosure duties v0.3 hand-built (curation
counts, omitted-edge counts, withheld sections) are the collapsed
read's native shape: `unassessed` rows and bands ride the same
relations. Whether an agent actually composes this block well is
fixture 09's experiment, unchanged — the operating-model run is its
vehicle.

## 2. The drill

The deepest consumer: per axis it reads slice relevance and
interest, hierarchy chains, per-(metric, axis) additivity verdicts
(`current_metric_axis_additivity` — the time gate withholds a bucket
grain rather than guessing), a cross-currency unit gate, and the
metric's persisted clause parts. Refusals render with reasons.

The glossql composition per gate: the time gate is the `behavior`
gloss (a stock never sums across periods; the year-scoped
behavior_evidence anchor says when a season/YTD reset bounds it);
the unit gate is the `unit` gloss's `source_column` plus a distinct
count; the chain is the declared same-table relationships; axis
choice is the `dimension` verdict (including the judged negative
`none`) beside `dimension_relevance`:

```glossql
SELECT value FROM GLOSSARY(results.points::behavior) WHERE state = 'current';
SELECT value FROM GLOSSARY(results.points::unit) WHERE state = 'current';
SELECT value FROM GLOSSARY(races.circuit_id::dimension) WHERE state = 'current';
SELECT count(*) FROM results r JOIN races a ON r.race_id = a.race_id;
```

**TRANSCRIBES for the time and unit gates · SEMANTICS UNDEFINED for
the general axis verdict.** v0.3's additivity table is keyed per
(metric, axis); our claim is that behavior + unit + entity grain
compose the same refusals at read. The time case is covered
(behavior is exactly it). The general case — a metric non-additive
over an arbitrary categorical axis — has no home yet and gets none
until a drill-shaped consumer proves the composition insufficient:
name the gap, do not build for it.

## 3. The answer confidence strip

Band, grounded ratio, assumptions, concepts used. All of it rides
constructs that exist: the grounding's `assumptions[]` array is in
the gloss body, bands come from `ATTEST`, and the concepts used are
the QUERY aspects the SQL was composed from.

```glossql
SELECT * FROM ATTEST(fin.trial_balance);
```

**TRANSCRIBES.** The ratio arithmetic is app-side arithmetic over
served rows, not a construct.

## 4. The operating-model tabs

**Metrics DAG — the keeper UI (ruled 2026-08-06).** v0.3 draws
metric → metric → measure → table from graph definitions and snippet
state; clicking a runnable node opens live values. In glossql every
edge is readable: QUERY aspects from the `aspects` relation, the
definitional DAG from formula FACT glosses, groundings from the
glossary, landed tables from `imports` — the app draws, nothing
serves a drawing. The *click* was the consumer that pulled
value-at-read out of §9's parking: ruled 2026-08-06 (fixture 16 §6) —
`read.` table functions, bound when the UI transformation starts;
until then the app runs the served SQL through the query door:

```glossql
SELECT name, kind, grains FROM aspects;
SELECT subject, aspect, value FROM GLOSSARY(fin) WHERE state = 'current';
```

**Concepts tab.** Ancestry, reconciliation state, multi-groundings —
glossary reads plus witness bands. TRANSCRIBES.

**Bus matrix — the UI dies (ruled 2026-08-06).** It was agent
context: which cross-fact comparisons are composable. The content is
already in the reads — fact/dimension roles from `entity` glosses,
edges from `relationships`, grain safety from the judged-join prose.
Run 5 on f1 produced it unprompted (races, drivers, constructors
named as conformed dimensions in pair prose). One FORK stays open
with the lead: the conformed *group* today lives in prose on pairs,
which an agent reads but an app cannot key on. If a consumer ever
needs it structured, the spelling is a field beside the verdict —
not a new construct:

```glossql
GLOSS meaning ON results.driver_id -> drivers.driver_id AS $${
  "value": "each result row names its driver; grain-preserving (26,080 = 26,080)",
  "conformed": "driver"
}$$;
```

## 5. The governance briefing

The standing state of the union: bands, artifact counts, staleness,
a "needs you" inbox. The reads exist; the inbox is a WHERE clause:

```glossql
SELECT subject, band, score FROM ATTEST(fin) WHERE band = 'red';
SELECT count(*) FROM GLOSSARY(fin) WHERE state = 'unassessed';
SELECT * FROM imports;
```

**TRANSCRIBES.** Seeding a chat from a blocker is app orchestration
(fixture 11's verdict: no flow construct).

## The negative space

Fourteen engine views are mirrored into the cockpit schema with zero
consumers — induced-validation staging, measure aggregation lineage,
metric parameters, concept edges, workspace calendar among them.
Every one sits in a lane the port list dropped or absorbed. The
consumption trace confirms the drop list from the consumer side:
nothing users see depends on anything we cut.

## Findings

- **TRANSCRIBES**: every surface composes from `GLOSSARY()` /
  `ATTEST()` / the declaration relations / the query door. No serving
  construct is missing; fixture 09's bet survives contact with the
  full consumer inventory.
- **SEMANTICS UNDEFINED, named not built**: the general
  per-(metric, axis) additivity verdict (§2). Watch it when a
  drill-shaped consumer arrives.
- **FORK, open with the lead**: the conformed-dimension group as a
  structured field vs prose (§4). Value-at-read, the other fork this
  fixture opened, was ruled 2026-08-06 — fixture 16 §6 records it.
- Closed same day by the run fixes: the judged-negative dimension
  verdict (`none`) and the exact relevance score (a display cap must
  not become a statistics cap).
- What users will do stays app-side by design — the door tells,
  skills teach, the app renders.
