# The shipped function library

Declared into every fresh workspace at boot. A function's body rides
its declaration, so the whole library reads back as worked examples:

```sql
SELECT name, script FROM functions;
```

A **measurement** RETURNS an aspect: run it with `SELECT <name>() FROM
<subject>` and the result lands as a measurement row keyed by the
workspace pin; the full value reads back via
`GLOSSARY(<subject>::<aspect>)`. When a measurement cannot speak it
returns `applicable: false` with a `reason` — an abstention is a
complete answer, never an error. A **detector** has no RETURNS: it is
named by a witness's `DETECTOR` clause, sees only the slots written
under that aspect, and answers a band and a score.

Measurements propose; they never verdict. The judge — the agent
reading them — removes false positives.

## Column grain

### profile → `column_profile`

`SELECT profile() FROM <table>.<column>`. The deterministic profile:
counts, null ratio, distinct and cardinality ratio, min/max, top
values, string lengths, and for numeric columns mean, stddev, MAD and
percentiles. Measures the landed table — the recipe carried the casts,
so `null_ratio` counts what the author's `try_cast`s surrendered.
Extraction serves the summary; the full profile reads back via
`GLOSSARY(<table>.<column>::column_profile)`.

### outliers → `outlier_profile`

`SELECT outliers() FROM <table>.<column>`. IQR fences (1.5×IQR) and
the modified Z-score (|0.6745·(x − median)/MAD| > 3.5) — universal
distribution facts, no domain rules. Composes the profile inline, so
the first ask computes with nothing pre-landed. Abstains on
non-numeric columns (`applicable: false`); a MAD of 0 (half the values
identical) makes every deviation infinite, so the Z-score arm counts
zero and the IQR fences stand alone.

### temporal → `temporal_profile`

`SELECT temporal() FROM <table>.<column>`. Window, cadence,
completeness, gaps. Cadence is the named grain nearest the median gap
between distinct instants; completeness counts calendar buckets over
the column's own window; gaps are stretches beyond twice the median
(count exact, sample capped at the 20 largest, largest first with
earliest-first breaking ties). Abstains when the column type is not
Date/Timestamp
(the reason says so — a date landed as text needs typing in the
recipe) or when no non-null values bound a window. The output carries
no staleness verdict — judgment about now lives in detectors and read
policy, never in results.

### behavior_evidence → `behavior_evidence`

`SELECT behavior_evidence() FROM <table>.<column>`. The stock/flow
discriminator: reconciles the measure against every viable anchor
(event table + alignment), each anchor voting flow or stock with its
convention, support, and reconciliation scores; the summary aggregates
the votes. A composite (tuple) endpoint takes part like any other.
Extraction serves the summary alone; every anchor reads back via
`GLOSSARY(<table>.<column>::behavior_evidence)`. Evidence for the
judge before glossing `behavior` — never a voice in the behavior
slots.

### dimension_relevance → `dimension_relevance`

`SELECT dimension_relevance() FROM <table>.<column>`.
`relevance = coverage × evenness`: coverage is the fraction of rows in
a named bucket, evenness is Pielou's index (entropy over its maximum),
clamped to [0, 1]. No free parameters. Admission gates, each named in
the abstention reason: a non-empty column, at least two buckets with
NULL counted as one, `null_ratio ≤ 0.5`, and cardinality ratio < 0.9
(a near-key is an identifier, not an axis). What the score deliberately does not do:
prefer few groups to many — which axis a reader wants first is
business judgment.

## Table grain

### detect_hierarchies → `hierarchy_candidates`

`SELECT detect_hierarchies() FROM <table>`. Pairwise functional-
dependency screens over one table's dimension-like columns, high
recall. Candidates ship at g3 ≤ 0.05; a near-copy both ways at
g3 ≤ 0.01 is served as kind `alias` — whether that is a code↔label
relabeling or a coincidence is exactly what no statistic settles. λ is
served beside every candidate, never gated: λ < 0.5 is the recorded
vacuous-skew signature the judge reads.

### detect_derivations → `derivation_candidates`

`SELECT detect_derivations() FROM <table>`. Row-grain arithmetic
identities among numeric columns — `a = b·c` and `a = b+c` — with
violation counts. An identity holds at match rate ≥ 0.95 over ≥ 20
supporting rows. A confirmed identity re-checked per batch is the
admission instrument for subtle corruption: a scoped artifact violates
the lineage identity at exactly its row coverage while every marginal
statistic confuses it with a real business move.

## Dataset grain

### detect_relationships → `relationship_candidates`

`SELECT detect_relationships() FROM <dataset>`. Every plausible join
pair across the landed tables — generous by design; the judge removes
false positives against the data. Per candidate: endpoints,
cardinality, overlap, matched/orphan counts, distinct counts; a
composite endpoint rides `key_columns` (the tuple is the key).
Ordered by overlap.

### relationship_coherence → `relationship_coherence`

`SELECT relationship_coherence() FROM <dataset>`. What each DECLARED
join asserts, checked against the rows: filled count, orphan count and
rate, and child-before-parent date incoherence per temporal column
pair. The two facts no column-shaped check can see on a
high-cardinality key.

### detect_grounding_collisions → `grounding_collisions`

`SELECT detect_grounding_collisions() FROM <dataset>`. Buckets every
current grounding by canonical SQL and by served monthly series; a
bucket holding two or more concepts is a collision — two concepts
grounding to the same extract make every ratio between them compute
1.0, silently. Reported, never resolved: deliberate synonyms exist,
and telling them from errors is the judge's call against the
definitions.

### metric_bands → `metric_bands`

`SELECT metric_bands() FROM <dataset>`. For every grounded metric,
walks the recent months and asks the TabICL forward what range each
month should have landed in given everything before it. Each walked
point records the band quantiles (p05–p95) and its PIT — the quantile
at which the actual landed. This measurement only reports; the
`band_breach` detector adjudicates.

### metric_cube → `metric_cube`

`SELECT metric_cube() FROM <dataset>`. Every grounded metric's monthly
series — the total, the slices along its served dimension columns, and
the named rival where a grounding discloses `alternative_sql` — landed
as one measurement that `metric_series()` then serves to any frame.
The axes come from judged verdicts, never from the data's own shape:
a served column enters as a dimension when its collapsed
`dimension_relevance` is applicable, relevance orders the admitted,
and the time axis is the served date column whose collapsed
`temporal_profile` shows a named cadence, highest completeness first.
A column without a verdict is a gap, not a candidate — run the
profilers over the served columns before the cube.
Caps, stated in the result: 4 dims, 48 months, and a bucketing target
of 24 members: at 24 or under every member is named, above that the
top 23 by weight
keep their names and the rest fold into `other` (each metric's
`bucketed` field names the dimensions this happened to). A ratio cell
carries its summed halves (`num`, `den`) so a coarser window can
re-derive the division, and `metric_series()` serves them beside
`behavior`, the metric's verb. Extraction
serves the
summary; the whole cube reads back via
`GLOSSARY(<dataset>::metric_cube)`. `metric_days('<metric>')` is the
cube's live sibling: one metric's last 90 observed days at the same
verb and the same judged time axis, computed at read from the
grounding — the cube stays monthly.

## Detectors

### slot_entropy

Bound by the kit to `role_w`, `behavior_w`, `unit_w` (threshold 0.7).
Score is the fraction of extra distinct values across the aspect's
slots — 0.0 when every voice agrees. Band: green at 0, yellow up to
the threshold, red above it.

### band_breach

Bound by the kit to `bands_w` on `metric_bands` (threshold 0.98).
Reads each monitored metric's latest walked point; displacement is
|2·PIT − 1| — 0 at the median, 0.8 at a nominal-80 edge. Score is the
worst displacement; band: green ≤ 0.8, yellow ≤ 0.9, orange ≤
threshold, red past it. Which metric and which month live in the
measurement's own output.

### rate_tolerance

The validation detector. Wire it per validation aspect: the
expectation gloss carries `tolerance`, the check function's voice
carries `breach_rate` (the violation share — 0.0 means fully passing).
The authored tolerance wins; the witness `THRESHOLD` is the fallback
while no expectation slot carries one (and 0.0 when neither does).
Green at rate ≤ tolerance, red above, yellow while no check voice has
spoken. One-sided by design — a known-dirt source that expects its own
rate wants a custom detector that goes red on both sides, since
overcleaning is also a failure.
