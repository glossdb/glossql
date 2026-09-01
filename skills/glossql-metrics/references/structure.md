# Structure — read once tables stand: what each is, the joins, the slice axes

## Say what each table is

Before the columns. Every correct aggregate downstream depends on this
verdict, and it is judged from the data, never from the name.

- **value** — what one row is, in business words.
- **role** — `fact` (events at volume, carrying numbers) or
  `dimension` (descriptive, referenced by others), read from the
  evidence: measures, an event date, row counts, who references whom.
- **grain** — the columns identifying one row. **Verify, never
  assert**: `COUNT(*)` must equal `SELECT count(*) FROM (SELECT DISTINCT
  col, …)`. Spell it as that subquery — `COUNT(DISTINCT (col, …))`
  builds a struct per row and runs out of memory on a large table.
  A composite grain gets the composite; a table with no key gets none,
  said plainly. Watch for document-header values repeated onto every
  line — summing them at row grain multiplies by line count.
- **time_axis** — the column recording when the row's event happened.
  Attribute dates (due_date, hire_date) are not an axis; one at most.

```glossql
GLOSS entity ON work_orders AS $${"value": "one site visit on a work order", "role": "fact",
  "grain": ["order_id", "visit_no"], "time_axis": "completed_at"}$$;
```

## Judge the join structure

`detect_relationships` proposes at high recall — false positives
included, you are the precision. Per candidate, before declaring:

- **Anti-join both directions and *read* what doesn't resolve.** An
  orphan count is a question, not a verdict: orphans that are exactly
  a business population (the cancelled orders, the pre-migration
  records) confirm the edge; random misses argue against it.
- **Distrust coincidence.** Two unique integer columns overlap
  perfectly without meaning it — parallel row-number sequences are the
  classic false positive. Names, values and business objects must all
  agree.
- **Judge a composite on all its legs.** Anti-join anchor *and* scope
  together; the anchor alone fans out and over-counts, which is what
  the composite exists to collapse. Declare it as a tuple, never the
  anchor leg alone.
- **Ground the verdict, not the story.** Why the data looks this way
  is a hypothesis — verify it or label it. A correct rejection with a
  wrong causal story misleads everyone reading the grounds later.

```glossql
DECLARE RELATIONSHIP work_orders.site_id -> sites.id;
DECLARE RELATIONSHIP visits.(region_id, site_code) -> sites.(region_id, site_code);
GLOSS meaning ON work_orders.site_id -> sites.id AS
  $${"value": "each order serves one site; the orphans are the cancelled orders, never dispatched"}$$;
```

Rejected candidates stay in the measurement — visible and undeclared
is the record that they were seen and judged. Once edges are declared,
`relationship_coherence` watches them: orphan rate (exact, and it
catches shapes no column statistic can, including a single repeated
invented key) and the temporal read — a child event dated before its
parent record *exists* is the trace a wrong pairing leaves, while a
child event before a *deadline* is ordinary. Re-run after new batches.

## Score the slice axes

`dimension_relevance` scores `coverage × evenness` — zero free
parameters, one scale for every axis. The number answers "is this axis
usable, how much does it resolve"; **interest is yours**. A
near-uniform sequence column scores high and is still `none`.
Abstentions are gates, not defects: near-keys, null-dominated columns,
constants.

`detect_hierarchies` screens within-table dependencies at high recall.
Judge each:

- **λ < 0.5 is the vacuous-skew signature.** A ≥98%-dominant dependent
  passes the screen vacuously; the floor kills those in bulk with no
  truth lost — treat it as binding.
- **A perfect 1:1 is a relabeling or a coincidence, and only meaning
  separates them.** A code↔label bijection collapses to one axis; an
  entity key that happens to align with a timestamp must not.
- **Same-family role columns stay apart**, however cleanly they align
  — an origin and a destination, a bill-to and a pay-to. Merging them
  corrupts every aggregation that crosses them.

Record a surviving nest as a same-table relationship, finer → coarser;
coherence checks it as a dependency (one coarser value per finer
value), never as a join. A same-table edge whose to-side is the
table's key — `accounts.parent_id -> accounts.account_id` — is a
self-reference, not a nest, and coherence checks it as a join against
itself; the to-side's uniqueness tells the two apart.
Then the judged join: **run the grain check before trusting any
join** — `COUNT(*)` before and after must be equal, exactly, or the
join is not grain-preserving and multiplies every downstream
aggregate. Check each join alone in a one-hop star.
