# What will bite — read on the first refusal, or before a complex read

Postgres reflexes that fail at this pin:
`percentile_disc` and `mode()` (absent) · `to_char` PG patterns
(Chrono only) · 3-arg `date_trunc` with timezone · `date_add` /
`date_sub` / `age` · `SELECT * EXCLUDE` · `generate_series` in the
SELECT list (FROM clause or `unnest`) · window inheritance ·
`information_schema` (off — the glossary is the discovery surface, and
richer) · `lag` as "previous period" (previous *row*) · window
`last_value` as "partition's last" (frame-relative) · weekly
`date_bin` on Monday (Thursday without an origin) · a `LIKE` guard
before a `CAST` in the same WHERE (conjuncts reorder — only
`try_cast` is safe on dirty text) · aliasing a projection to its own
qualified source name (`round(j.x, 2) AS x`) · **two unaliased scalar
subqueries in one projection** ("Projections require unique expression
names" — alias both, or compute in a CTE).

Two more, specific to reads: an inner `ORDER BY` does not survive a
derived relation, so order where you consume; and a correlated
`NOT EXISTS` over a read that extracts JSON defeats decorrelation —
use a LEFT JOIN and a count instead.
