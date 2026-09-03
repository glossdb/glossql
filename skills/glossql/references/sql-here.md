# What will bite — read on the first refusal, or before a complex read

The engine is DataFusion at a pinned version, and its own SQL guide at
that pin is on this door as `doc://vendor/datafusion/sql/…` — select,
subqueries, windows, aggregates, operators, types, and the scalar
functions by family under `scalar/`. A function's name, signature or
absence is a lookup there, not a guess. This page is what the guide
cannot say.

Names that postgres reflexes get wrong here: `sign` is `signum` ·
`len` is `length` · `regexp_extract` is `regexp_match` (a list —
index it, `[1]`) · `strptime` / `try_strptime` is `to_timestamp(x,
format)` and `try_to_timestamp` · `to_char` takes a Chrono pattern
(`%Y-%m`), never `YYYY-MM` · `date_diff` / `datediff` do not exist —
a difference is `to_unixtime(b) - to_unixtime(a)` in seconds, on
dates as well as timestamps. The refusal suggests the near miss;
these are the far ones.

Shapes the parser refuses at this pin: the parser dialect is postgres,
so generic-dialect syntax from the guide does not parse — `SELECT *
EXCLUDE (…)`, `SELECT * EXCEPT (…)` · window *inheritance* (`OVER (w
ORDER BY …)` extending a named window; a named window itself is fine)
· `=>` outside the door's table functions — the engine's own functions
are positional · `information_schema` (off — the glossary is the
discovery surface, and richer; `SHOW TABLES` lists the bound dataset's
tables and `DESCRIBE <name>` describes any readable name).

Names are case-folded: an unquoted `AdsInfo` reaches `adsinfo`, and a
table landed with capitals is found only quoted — `"AdsInfo"` — or
landed lowercase.

`EXISTS` and `IN (SELECT …)` are rewritten into joins only as plain
WHERE conjuncts whose subquery mentions the outer row in its own WHERE
alone. Anywhere else — inside `FILTER (WHERE …)`, a SELECT list, a
CASE, an OR — or with the outer column in the subquery's SELECT list,
a window, an aggregate or a LIMIT, the subquery reaches the planner
unrewritten: `Physical plan does not support logical expression
Exists`. Write the LEFT JOIN and a count instead.

What lands wrong without failing: a `LIKE` guard before a `CAST` in
the same WHERE (conjuncts reorder — only `try_cast` is safe on dirty
text) · aliasing a projection to its own qualified source name
(`round(j.x, 2) AS x`) · **two unaliased scalar subqueries in one
projection** ("Projections require unique expression names" — alias
both, or compute in a CTE) · `try_to_timestamp` with a date-only
format (NULL — `try_to_date` parses it; a timestamp format must cover
the whole value, seconds included) · an inner `ORDER BY` does not
survive a derived relation, so order where you consume.

Memory: `count(DISTINCT (a, b, c))` over millions of rows builds a
struct per row and exhausts the pool (`Resources exhausted`). Use
`approx_distinct`, or count distinct over a CTE that groups the keys
first.

A column that landed as text: `max`/`min` are lexicographic (`'99' >
'100'`) and `sum` refuses (`Function 'sum' requires Decimal, but
received String`). The cast belongs in the recipe; `try_cast` at the
read is the stopgap.
