# The function library splits: judge, read, statistic, search (2026-08-17)

A follow-on to `2026-08-16-store-to-the-lake.md`, which left one seam
open: a rhai script re-enters the engine through `db.query`, and no
threading answer makes that a good design. This report reads the
shipped library to find out what those scripts actually are. They are
not one thing, and only one of the four is a function.

## 1. Ruled (project lead, 2026-08-17)

- **A measurement does not take part in the collapse hierarchy.** An
  outlier is an outlier; neither an agent nor a human overrules it.
  Human-over-agent precedence belongs to *assumptions* — a FACT such
  as "a business month is 30 days" — and to the groundings, never to a
  computed value. §3 records that the store already enforces this at
  admission and that the ranking in `slots()` is vestigial.

Proposed by the project lead and verified here, not yet ruled: moving
the heavy measurements into Rust and exposing them as SQL functions
(§5–§7), and narrowing the extension points to four (§6).

## 2. What is in the library

Thirteen rhai functions. Counting `db.query` sites against column
kernel calls separates them cleanly:

| function | lines | queries | kernels |
|---|---|---|---|
| `behavior_evidence` | 604 | 8 sites, nested 4–5 loops deep | 0 |
| `metric_cube` | 295 | 7 | 0 |
| `relationships` | 278 | 6 | 0 |
| `metric_bands` | 208 | 6 | 0 (one `tabicl_bands` kernel) |
| `temporal` | 175 | 5 | 0 |
| `grounding_collisions` | 167 | 1 | `canonical_sql` |
| `hierarchies` | 144 | 7 | 0 |
| `coherence` | 129 | 3 | 0 |
| `derivations` | 121 | 3 | 0 |
| `dimension_relevance` | 77 | 0 | 0 |
| `profile` | 64 | 1 | 12 |
| `outliers` | 46 | 1 | 0 |
| `band_breach` · `slot_entropy` · `rate_tolerance` | 35 · 28 · 25 | 0 | 0 |

**`profile` is the only cruncher.** One query pulls a column, twelve
kernels reduce it; §4 of the previous report measures it at 32 ms/row.
Everything else with a query is composing SQL and branching on small
results — string building, not computation.

`behavior_evidence` is the extreme: its query sites sit at indent
depth 8, inside loops over tables × columns × pointers × axes
(`behavior_evidence.rhai:234,242,250,252,271`), each query returning
one cell that decides a branch. It memoises into `cache[mg_key]` at
that depth, which is the author fighting round-trip cost by hand. Its
943 ms/row is round-trip count, not compute.

## 3. What the cache actually was, and why measurements do not collapse

`slots()` (`store.rs:1202-1300`) is glossary rows unioned with cache
rows: for every function returning that aspect, its latest cache row
per subject, pushed in at `rank: 2` (`:1288`) below human (0) and
agent (1). Every read shape builds on it — `GLOSSARY()`, the collapse,
and `context_value` → `collapsed_read` → `slots`, which is how
`outliers` receives `context.column_profile`
(`session.rs:1129-1158`).

Reading that, one might conclude a measurement is a low-precedence
speaker a human can outrank. **It is not, and the store already
refuses to let it be:**

- `gloss()` rejects a MEASUREMENT aspect outright — `"measurement" =>
  return Err(Error::MeasurementGloss(...))` (`store.rs:964`).
- Exactly one function may return a given measurement aspect
  (`MeasurementProducerTaken`, `store.rs:818-830`).
- A witness on a measurement aspect may name no speakers
  (`MeasurementWitnessSpeakers`, `store.rs:894`).

So for a measurement aspect the slot set contains exactly one row: the
computed one. The rank never competes, the `sort_by_key(|s| s.rank)`
at `:1434` sorts a single element, and the winner comparison at
`:1511-1513` has nothing to compare. The ranking is live only for FACT
and QUERY aspects — the assumptions and the groundings — which is
where it belongs.

**Consequence for the rewrite.** Measurements are a UNION beside the
collapsed spoken slots, not a merge into the slot pipeline. The
cache-union branch inside `slots()` exists only because the cache was
the storage; with values computed at read, the measurement half is a
separate branch that never touches supersession. That is a
simplification the previous report did not have.

## 4. `ACCEPTS` carries two meanings, and only one survives

From `bootstrap.glossql:172-221`, nine functions declare `ACCEPTS`.
They split in two:

- **Relation edges** — `imports`, `glossary`, `relationships`. Seven
  of the nine. These are skipped during context assembly
  (`accepts_relation`, `session.rs:779`); they exist *only* as cache
  invalidation edges. The 2026-08-16 report deletes that concept, so
  these clauses become empty of meaning and come out of the
  declarations with it.
- **Aspect edges** — a real dependency on another function's output.
  Exactly two: `outliers` ← `column_profile` and
  `dimension_relevance` ← `column_profile`.

So the dependency graph among functions is one producer, two
consumers, depth 2. And under the split below it stops needing a
mechanism at all: if `profile` is an aggregate function, its consumers
are judges over its result — a nested expression or a join, in SQL.
**`ACCEPTS` was function composition done through the only channel
available, which was the cache.** SQL composes for free.

## 5. The four shapes

Read by what they compute rather than how they are written:

| shape | in | out | example |
|---|---|---|---|
| **judge** | one value | one value | `band_breach`, `dimension_relevance` |
| **statistic** | many rows | one value | `profile`, `tabicl_bands` |
| **search** | a table | a table | `hierarchies`, `derivations` |
| **measure over declarations** | store rows ⋈ data | rows | `coherence`, `metric_cube` |

The library already contains three pairs that split measuring from
judging correctly — `metric_bands`/`band_breach`,
`profile`/`outliers`, `profile`/`dimension_relevance`. The searches
are the ones that never split, and `hierarchies` says so in its own
header ("the measurement's job is recall; the judge removes false
positives") before doing both in one loop.

**The searches are algorithms, not analyses.** No agent authors a new
functional-dependency screen; it uses the shipped one and judges the
output. That is why they became 144–604 line scripts assembling SQL:
they are engine capabilities written in a scripting language.

## 6. The extension points, narrowed

| point | mechanism | author | shape |
|---|---|---|---|
| **judge** | `ScalarUDF` backed by rhai | agent | row → row |
| **read** | `.sql` in the read library | agent | relation → relation |
| **statistic** | `AggregateUDF`, Rust | shipped | many rows → one |
| **search** | `TableFunctionImpl`, Rust | shipped | table → table |

Applying a function to a read's result — `SELECT f(x) FROM read.abc` —
is the first and third rows; which applies is only arity. Literal
`f(*)` syntax exists in SQL for `count(*)` alone, but naming the
column covers every shape in the library.

The repo already registers UDFs this way
(`crates/import/src/casts.rs:23`, `try_to_date`/`try_to_timestamp`),
so neither the statistic nor the judge point is a new concept.

**The two genuinely missing statistics are `mad` and `entropy`.**
Everything else `profile` computes has a built-in:
`avg` · `stddev` · `variance` · `median` · `percentile_cont` ·
`approx_percentile_cont` · `approx_distinct` · `correlation` ·
`covariance` · `min_max` · `count`
(`datafusion-functions-aggregate-53.1.0/src`). Length statistics are
`length()` plus aggregates; top values are `GROUP BY … ORDER BY count
DESC LIMIT n`; dtype and row count are schema.

## 7. The schema limitation, and what dissolves it

A `.sql` read cannot take a table's schema as an argument, so the
searches cannot be plain reads: their candidate enumeration is over an
arbitrary table's columns. `UNPIVOT` would have helped and does not
exist here — it parses in sqlparser
(`sqlparser-0.60.0/src/ast/query.rs:1355`) and DataFusion's SQL
planner contains no reference to it, so it parses and then fails to
plan.

A table function does not have the limitation.
`TableFunctionImpl::call(&self, args: &[Expr]) ->
Result<Arc<dyn TableProvider>>`
(`datafusion-catalog-53.1.0/src/table.rs:489-492`), registered through
`SessionContext::register_udtf`
(`datafusion-53.1.0/src/execution/context/mod.rs:1524`). Arguments are
planned expressions — scalars only; there is no `TABLE t` polymorphic
argument and the planner records the gap
(`datafusion-sql-53.1.0/src/relation/mod.rs:290`).

That is enough, because of an asymmetry: **the input schema varies,
the output schema is fixed.** `hierarchy_candidates` returns the same
columns whatever table it is aimed at. So `schema()` answers
synchronously and `scan()` — which is async — resolves the input
table, reads its schema, and builds the plan.

```sql
SELECT * FROM hierarchy_candidates('journal_lines')
```

**Verified for the cross-table case too.** `behavior_evidence` returns
`#{ applicable, anchors: [...], summary: {...} }` where `summary`
carries optional keys (`r_flow`, `r_stock`, `sign`, `reason`). That is
not a fixed Arrow schema as written — but the variability is an
artifact of measuring and judging in one function. Flattened to one
row per anchor with nullable columns, the schema is fixed; picking the
winning anchor by support, and the abstain reason, are judgment and
belong in the judge.

## 8. Worked example: `hierarchies`

Today: `SELECT * FROM t LIMIT 0` for schema, `count(*)` for n, one
wide aggregate for the pool, then one modal query per pool column and
three grouped scans per unordered pair — `2 + k + 3·k(k−1)/2` round
trips, 148 at k=10 and 593 at k=20, each re-planned from a string.

As a table function the pair fan-out is a self-join the engine
performs, and the whole search is one plan. The equivalent SQL, which
the Rust implementation builds rather than templates, is:

```sql
WITH src AS (SELECT row_number() OVER () AS rid, * FROM journal_lines),
long AS (                       -- one arm per dimension-like column
  SELECT rid, 'account_id' AS col, CAST(account_id AS VARCHAR) AS val FROM src
  UNION ALL SELECT rid, 'cost_center', CAST(cost_center AS VARCHAR) FROM src
  UNION ALL SELECT rid, 'entry_type',  CAST(entry_type  AS VARCHAR) FROM src
),
n AS (SELECT count(*) AS n FROM src),
cell AS (SELECT col, val, count(*) AS c FROM long GROUP BY 1, 2),
colstat AS (                    -- NULL is a category: GROUP BY keeps it
  SELECT col, count(*) AS groups, max(c) AS modal,
         count(*) FILTER (WHERE val IS NOT NULL) AS distinct_vals,
         sum(c)   FILTER (WHERE val IS NOT NULL) AS filled
  FROM cell GROUP BY 1),
pool AS (SELECT col, groups, modal FROM colstat
         WHERE groups >= 2 AND NOT (filled > 0 AND distinct_vals = filled)),
pair AS (SELECT a.col AS a, b.col AS b, a.val AS av, b.val AS bv, count(*) AS c
         FROM long a JOIN long b ON a.rid = b.rid AND a.col < b.col
         WHERE a.col IN (SELECT col FROM pool)
           AND b.col IN (SELECT col FROM pool)
         GROUP BY 1, 2, 3, 4),
pg  AS (SELECT a, b, count(*) AS pair_groups FROM pair GROUP BY 1, 2),
fwd AS (SELECT a, b, sum(mx) AS agree FROM
          (SELECT a, b, av, max(c) AS mx FROM pair GROUP BY 1,2,3) t GROUP BY 1,2),
rev AS (SELECT a, b, sum(mx) AS agree FROM
          (SELECT a, b, bv, max(c) AS mx FROM pair GROUP BY 1,2,3) t GROUP BY 1,2)
SELECT pg.a, pg.b, pg.pair_groups, n.n AS rows,
       pa.groups AS distinct_a, pb.groups AS distinct_b,
       CAST(n.n - fwd.agree AS DOUBLE) / n.n                   AS g3_ab,
       CAST(n.n - rev.agree AS DOUBLE) / n.n                   AS g3_ba,
       CAST(fwd.agree - pb.modal AS DOUBLE) / (n.n - pb.modal) AS lambda_ab,
       CAST(rev.agree - pa.modal AS DOUBLE) / (n.n - pa.modal) AS lambda_ba
FROM pg JOIN fwd ON fwd.a = pg.a AND fwd.b = pg.b
        JOIN rev ON rev.a = pg.a AND rev.b = pg.b
        JOIN pool pa ON pa.col = pg.a
        JOIN pool pb ON pb.col = pg.b
        CROSS JOIN n
```

No thresholds appear: this half optimises recall. The judge is what
remains in rhai —

```rhai
let edges = [];
for r in context.hierarchy_candidates.rows {
    let alias = r.g3_ab <= 0.01 && r.g3_ba <= 0.01;   // both ways, conservative
    if r.g3_ab <= 0.05 { edges.push(#{ from: r.a, to: r.b, g3: r.g3_ab,
        lambda: r.lambda_ab, kind: if alias { "alias" } else { "edge" } }); }
    if r.g3_ba <= 0.05 { edges.push(#{ from: r.b, to: r.a, g3: r.g3_ba,
        lambda: r.lambda_ba, kind: if alias { "alias" } else { "edge" } }); }
}
#{ applicable: true, candidates: edges }
```

144 lines become one table function and eighteen lines of judgment, and
the two constants anyone might argue with — 0.05 and 0.01 — are the
only constants in a file with nothing else in it.

## 9. What the move costs

The shipped library reads back through `SELECT script FROM functions`
as worked examples an MCP-only agent can open. Porting the searches
removes four of them from that surface, and their comments carry real
falsification history — the g3 band loosened deliberately, λ served
and never gated, permutation nulls refused as judge-less apparatus,
the near-key gate that once stripped legitimate material. That history
must survive the port as Rust documentation or it is lost.

The counter-argument: a 604-line search was never a good worked
example. The judges are, and they stay.

## 10. Open

- **The port itself is not ruled** — §5–§7 verify that it is
  available, not that it is taken.
- **`temporal`** is classed as a search on shape alone; its branching
  is phase-dependent (dtype → span → gaps → granularity) and it has
  not been read closely enough to say whether two phases suffice.
- **`metric_cube` and `coherence` as reads** assumes their fan-out
  over declared objects expresses as a join. Neither has been written
  out the way `hierarchies` has in §8.
