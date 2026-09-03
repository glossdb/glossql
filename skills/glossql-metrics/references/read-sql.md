# Reading — read before any read over a metric: the verbs, and what this engine has that postgres lacks

## Read at the reader's grain

Grain is the reader's: the app asks by month, another reader by day,
the same definitions answer both. Evaluate through `read.<aspect>()` —
the current grounding as an ordinary relation, human slot outranking
agent, so a human answer is what runs.

- **Flows sum** over any partition.
- **Stocks take the last period per window** —
  `last_value(value ORDER BY ts)`, the aggregate form, ORDER BY
  mandatory. A running sum of a stock is arithmetic nonsense; a
  period-over-period delta is legal, it is the derived flow. ROLLUP
  across time is illegal for stocks.
- **Ratios don't roll up**: compose per the formula at the window
  asked — numerator and denominator each at the new scope, never an
  average of finer ratios. Never regroup a ratio's output rows.
- **Distinct counts don't roll up either**: distinct customers per
  region do not sum to distinct customers. Recompute
  `count(DISTINCT …)` at the scope asked; never add members.
- **Gap-filling**: a flow coalesces a missing period to 0 — no
  transactions is zero flow. A stock never does — a missing level is
  unknown, not zero.
- **`lag` and ROWS frames count rows, not periods.** On an axis with a
  missing month, `lag(value, 12)` is quietly thirteen months ago.
  Densify the axis, or use a RANGE frame with an interval.

Percentiles and medians are level statistics — safe for both
behaviors, which makes p50/p95 the honest summary when behavior is
unglossed.

### This is columnar, not postgres

You know how to write windows; what you cannot know from memory is
which ones exist *here*. The engine is DataFusion at a pinned version,
and three of its strongest tools have no postgres equivalent, so
nobody reaches for them by reflex. The habit to break is the
postgres shape: a self-join per comparison, a subquery per rank.
The engine's own guide at this pin is on the door —
`doc://vendor/datafusion/sql/…`, the scalar functions by family under
`scalar/`; the four refusals every session meets are on the `glossql`
skill's page, and `skill://glossql/references/sql-here.md` is the
rest of what fails here that the guide cannot say. This is what
pays.

**One scan, many answers.** Declare the window once and reuse it;
`FILTER (WHERE …)` pivots a driver into a column without a self-join
(it is not WHERE — WHERE removes rows from every aggregate, FILTER
from one; an empty filter gives COUNT 0 but SUM/AVG NULL, so coalesce
before dividing). Window *inheritance* does not parse at this pin —
flat definitions only.

```sql
SELECT period, driver, value,
  value - lag(value) OVER w                     AS delta,
  sum(value) OVER (PARTITION BY period)         AS period_total,
  value / sum(value) OVER (PARTITION BY period) AS share,
  avg(value) OVER (w ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS ma3,
  sum(value) FILTER (WHERE driver = 'EMEA') OVER (PARTITION BY period) AS emea
FROM series
WINDOW w AS (PARTITION BY driver ORDER BY period)
```

**`QUALIFY` filters on a window result** — top-N per period with no
subquery. It runs after windows, before ORDER BY and LIMIT. Give the
ORDER BY a full tiebreaker or `row_number` reshuffles on every
refresh. `ntile` splits row *count*, not value mass — a
top-decile-by-value question is cumulative share,
`sum(value) OVER (ORDER BY value DESC) / sum(value) OVER ()`.

```sql
SELECT period, site, value FROM completions
QUALIFY rank() OVER (PARTITION BY period ORDER BY value DESC, site) <= 10
```

**Densify before you lag.** `generate_series` works as a table
function in FROM (not in the SELECT list) and includes its upper
bound; `range` excludes it. The coalesce is the flow rule — a stock
keeps its NULLs.

```sql
SELECT months.m AS period, coalesce(b.value, 0) AS value
FROM generate_series(DATE '2025-01-01', DATE '2025-12-01', INTERVAL '1' MONTH) AS months(m)
LEFT JOIN monthly b ON b.period = months.m
```

Or make the frame value-based — **RANGE accepts intervals here**,
which is "the last three calendar months" regardless of holes:

```sql
SELECT period, avg(value) OVER (ORDER BY period
  RANGE BETWEEN INTERVAL '2' MONTH PRECEDING AND CURRENT ROW) AS smoothed
FROM monthly
```

Two frame defaults that bite: with an ORDER BY and no frame clause the
frame is `UNBOUNDED PRECEDING AND CURRENT ROW` **with peers**, so tied
ORDER BY values share a running total — order by the grain-unique
column. And window `last_value` under that default returns the current
row, not the partition's last; use the aggregate form from the stock
rule above. For year-over-year on a sparse axis the self-join earns
its place: `ON prev.period = cur.period - INTERVAL '1 year'` cannot
be fooled by a hole.

**Never guess a GROUP BY — the judged reads hand you the axes.**
Partition on `dimension` glosses, primary first, ranked by
`dimension_relevance`: low *coverage* means a top-N over that axis
speaks for a minority, low *evenness* means ranks and ntiles
degenerate onto one dominant value. ROLLUP order comes from the
declared hierarchy, coarse to fine — `ROLLUP(region, country, city)`;
reversed, the subtotals are junk. Subtotal rows carry NULL in the
rolled-up columns and a real NULL driver looks identical, so branch on
`grouping(col)`, never on `IS NULL`. `GROUP BY ALL` is for probing;
a recorded grounding spells its GROUP BY, because ALL silently
re-groups when someone edits the select list.

**Time mechanics.** `date_trunc` down to the metric's grain and no
finer — truncating below grain fabricates resolution; `'week'` is ISO
Monday. `date_bin(stride, d, origin)` for everything calendar months
cannot say (fiscal periods, offset weeks) — the default origin is the
epoch and 1970-01-01 was a Thursday, so weekly bins without an origin
start on Thursday; the declared fiscal calendar goes in that argument.
`date_part('month', d)` folds years together — the seasonality idiom;
trends stay with `date_trunc`. `dow` is Sunday=0, `isodow` Monday=1.
Shifts are `± INTERVAL` arithmetic.

**Statistics that pay.** `percentile_cont(0.95) WITHIN GROUP (ORDER BY
v)` is exact, `approx_percentile_cont` scales (t-digest — never diff
approximate values across refreshes and call it a trend).
`regr_slope(value, date_part('epoch', period))` per driver is a
which-segments-are-declining detector in one aggregate: aggregate the
flow to its grain first, y comes first, and swapping the arguments
gives a wrong number rather than an error — slopes are per-second, so
scale before showing. `stddev(value) / nullif(avg(value), 0)` ranks
volatility; `approx_distinct` is active-count at scale; `corr` needs
period-aligned series, so densify or join first or the number is
noise; `array_agg(x ORDER BY period)` — unordered `array_agg` is
nondeterministic and must never feed a sparkline.

**Record what a read proves.** A composed evaluation you verified may
land as the metric's own QUERY gloss — durable executable knowledge,
served by `read.<aspect>()` from then on. Compose it `FROM
read.throughput()` where you can, so a re-ruled component propagates
through every metric built on it.
