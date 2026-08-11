---
name: glossql-windows
description: Write exceptional window and aggregation SQL over glossed data — one-scan shapes, the flow/stock substitution rule, dense time axes, slices from the glossary. Use when composing any read over a glossql workspace's tables or metric surfaces — frames, probes, analysis, groundings.
---

# Windows over slices

First writing (2026-08-10). Every syntax claim below is verified
against the pinned substrate — DataFusion 53.1, sqlparser 0.61,
postgres dialect — not against the docs of latest, which show
several things this pin does not parse.

The habit this skill exists to break: agents write postgres-shaped
SQL — a self-join per comparison, a subquery per rank, `date_trunc`
and hope. The engine is columnar and its window machinery is better
than that, and three of its strongest tools (`QUALIFY`,
interval-valued RANGE frames, aggregate `last_value`) do not exist
in postgres, so nobody reaches for them from memory. The glossary
tells you which of these shapes is *legal*; this skill is the two of
them together.

## 1. One scan, many answers

Before writing a join, ask whether a window or a FILTER already has
it. In one pass over a metric's rows you can carry:

```sql
SELECT period, driver, value,
  value - lag(value) OVER w              AS delta,       -- period-over-period
  sum(value) OVER (PARTITION BY period)  AS period_total,
  value / sum(value) OVER (PARTITION BY period) AS share, -- contribution
  avg(value) OVER (w ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS ma3,
  sum(value) FILTER (WHERE driver = 'EMEA') OVER (PARTITION BY period) AS emea
FROM t
WINDOW w AS (PARTITION BY driver ORDER BY period)
```

Declare the canonical window once with `WINDOW w AS (…)` and reuse
it — one per frame is good hygiene. (Window *inheritance* —
`w2 AS (w1 ORDER BY …)` — does not parse at this pin; flat only.)

`FILTER (WHERE …)` works on plain and window aggregates and pivots a
driver into columns without a self-join. It is not WHERE: WHERE
removes rows from every aggregate and every frame; FILTER from one
aggregate only. Empty filter: COUNT gives 0, SUM/AVG give NULL —
coalesce before dividing.

Top-N per period without a subquery:

```sql
SELECT period, customer, value
FROM read.billings()
QUALIFY rank() OVER (PARTITION BY date_trunc('month', period_date)
                     ORDER BY value DESC) <= 10
```

QUALIFY runs after windows, before ORDER BY/LIMIT. Postgres doesn't
have it; reach for it on purpose. `row_number` breaks ties
arbitrarily — give the ORDER BY a full tiebreaker or the dashboard
reshuffles per refresh. `ntile` splits row count, not value mass; a
revenue-decile intent is cumulative share
(`sum(value) OVER (ORDER BY value DESC) / sum(value) OVER ()`).

The self-join is still right in one place — see §3.

## 2. The substitution rule

The `behavior` gloss is the gatekeeper, and it compresses to one
substitution:

- **flow** — the period verb is `sum(value)`. Regroups to any
  window, runs as a running total, takes shares across time.
- **stock** — the period verb is `last_value(value ORDER BY ts)`,
  the *aggregate* form:

```sql
SELECT date_trunc('month', d) AS period, account,
       last_value(balance ORDER BY d) AS closing
FROM balances GROUP BY 1, 2
```

  No frame gymnastics, composes with GROUP BY and ROLLUP on entity
  dimensions. The ORDER BY inside is mandatory — without it you get
  an arbitrary row, not a closing. (`DISTINCT ON (account) … ORDER BY
  account, d DESC` and `QUALIFY row_number() = 1` are the same idiom
  in other clothes.)

Everything else follows:

- A stock's period-over-period *delta* is legal — it is the derived
  flow. A running `sum` of a stock is arithmetic nonsense.
- Shares: a flow takes share-of-total across a window; a stock's
  share is legal only with the partition pinned to one instant.
- ROLLUP across the time column is illegal for stocks — the
  "all periods" row sums levels. Roll stocks up along entity
  dimensions inside one period.
- Gap-filling after densifying (§3): a flow coalesces missing
  periods to 0 — no transactions is zero flow. A stock never does —
  a missing level is unknown, not zero; carry it forward explicitly
  or leave the NULL showing.

Percentiles and medians are level statistics — safe for both
behaviors, which makes p50/p95 the honest summary when behavior is
unglossed (and an unglossed measure belongs in the judgement queue,
not under a sum).

## 3. The dense axis

`lag`/`lead` and ROWS frames count *rows*, not periods. On an axis
with a missing month, `lag(value, 12)` is quietly thirteen months
ago and `ROWS 2 PRECEDING` means "last 3 observations". Two honest
escapes:

- **Densify first.** `generate_series` works in FROM as a table
  function at this pin:

```sql
SELECT months.m AS period, coalesce(b.billings, 0) AS billings
FROM generate_series(DATE '2018-01-01', DATE '2023-05-01',
                     INTERVAL '1' MONTH) AS months(m)
LEFT JOIN monthly b ON b.period = months.m
```

  (`generate_series` includes the upper bound; `range` excludes it —
  off-by-one-period bugs live there. The coalesce is the flow rule;
  a stock keeps its NULLs.)

- **Or make the frame value-based.** RANGE accepts intervals at this
  pin, though the docs undersell it:

```sql
avg(value) OVER (ORDER BY period
  RANGE BETWEEN INTERVAL '2' MONTH PRECEDING AND CURRENT ROW)
```

  That is "last three calendar months" regardless of gaps.

For year-over-year on a possibly-sparse axis, the self-join earns
its place: `LEFT JOIN prev ON prev.period = cur.period - INTERVAL
'1 year' AND prev.driver = cur.driver` cannot be fooled by a hole.
Windows are the dense-axis optimization, not a replacement.

Two frame traps with defaults: with an ORDER BY and no frame clause
the frame is `UNBOUNDED PRECEDING AND CURRENT ROW` *with peers* —
tied ORDER BY values share their running total (order by the
grain-unique period column), and window `last_value` under that
default returns the current row, not the partition's last — spell
`ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`, or use
the aggregate form from §2 and skip the trap.

## 4. Slices from the glossary

Never guess a GROUP BY. The judged reads hand you the axes:

- **PARTITION BY / GROUP BY columns** are the judged drivers —
  `dimension` glosses (primary first), ranked by the
  `dimension_relevance` measurement. Low *coverage* warns that a
  top-N over the axis speaks for a minority; low *evenness* warns
  that ranks and ntiles degenerate onto one dominant value.
- **ROLLUP order comes from the declared hierarchy**, coarse to
  fine: `ROLLUP(region, country, city)`. Reversed, the subtotals
  are junk aggregations. Don't CUBE across axes nobody judged —
  cost is exponential and the slices mean nothing.
- Subtotal rows carry NULL in the rolled-up columns. A real NULL
  driver value looks identical — branch on `grouping(col)`, never
  on `IS NULL`, one call per column.
- `GROUP BY ALL` is for interactive probing; checked-in frames spell
  the GROUP BY, because ALL silently re-groups when someone edits
  the select list.

## 5. Time mechanics

- `date_trunc(precision, d)` down to the metric's grain and no
  finer — truncating below grain fabricates resolution. `'week'`
  is ISO Monday.
- `date_bin(stride, d, origin)` for everything calendar months
  can't say: 15-minute bins, 4-week fiscal periods, offset weeks.
  The default origin is the epoch, and 1970-01-01 was a Thursday —
  weekly bins without an origin start Thursday. The declared fiscal
  calendar lands in the origin argument.
- `date_part` folds years together — `('month', d)` compares all
  Januaries: the seasonality idiom. Trends stay with `date_trunc`.
  `dow` is Sunday=0, `isodow` Monday=1.
- Shifts are `± INTERVAL` arithmetic (calendar-correct for `'1
  year'` at month grain). There is no `date_add`, no `age()`.
- `to_char` takes **Chrono** formats — `'%Y-%m'`, never
  `'YYYY-MM'`; postgres patterns pass through as garbage text.
- A bare `TIMESTAMP` column carries no zone; whether it is
  UTC-instant or local-naive is gloss knowledge. Local-day binning:
  `date_bin(INTERVAL '1 day', to_local_time(ts AT TIME ZONE
  'Europe/Zurich'))`.

## 6. Statistics that pay

- `percentile_cont(0.95) WITHIN GROUP (ORDER BY v)` — exact;
  `approx_percentile_cont` for scale (t-digest; don't diff approx
  values across refreshes and call it a trend). One ordering
  expression only. `percentile_disc` and `mode()` **do not exist**
  — median via `median`/`percentile_cont`, mode via GROUP BY +
  ORDER BY count DESC LIMIT 1.
- `regr_slope(value, date_part('epoch', period))` per driver is a
  which-segments-are-declining detector in one aggregate — no
  windows. Aggregate a flow to its grain first, then regress the
  periods; y comes first and swapping the arguments produces a
  wrong number, not an error. Slopes are per-second — scale before
  showing.
- `stddev(value) / nullif(avg(value), 0)` ranks volatility;
  `approx_distinct(customer)` per period is active-count at scale;
  `corr` needs period-aligned series — densify or join first, or
  the number is noise.
- `array_agg(x ORDER BY period)` for series payloads — unordered
  array_agg is nondeterministic; never feed it to a sparkline.

## 7. What will bite

Postgres reflexes that fail at this pin, collected: `percentile_disc`
and `mode()` (absent) · `to_char` PG patterns (Chrono only) ·
3-arg `date_trunc` with timezone (absent) · `date_add`/`date_sub`/
`age` (absent) · `SELECT * EXCLUDE/EXCEPT` (dialect-gated off) ·
`generate_series` in the SELECT list (FROM clause or `unnest`) ·
window inheritance (off) · `information_schema` (off — the glossary
is the discovery surface, and it is richer) · `lag` as "previous
period" (previous *row*) · window `last_value` as "partition's last"
(frame-relative) · weekly `date_bin` on Monday (Thursday without an
origin) · ROLLUP NULLs as real NULLs (`grouping()`).

Frames follow glossql-apps on top of this — casting view types,
stated caps, params. This skill is the SQL inside them.
