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
(`%Y-%m`), never `YYYY-MM`. The refusal suggests the near miss; these
are the far ones.

Shapes the parser refuses at this pin: the parser dialect is postgres,
so generic-dialect syntax from the guide does not parse — `SELECT *
EXCLUDE (…)`, `SELECT * EXCEPT (…)` · window *inheritance* (`OVER (w
ORDER BY …)` extending a named window; a named window itself is fine)
· `=>` outside the door's table functions — the engine's own functions
are positional · `information_schema` (off — the glossary is the
discovery surface, and richer; `DESCRIBE <table>` for a landed table).

Names are case-folded: an unquoted `AdsInfo` reaches `adsinfo`, and a
table landed with capitals is found only quoted — `"AdsInfo"` — or
landed lowercase.

Correlated subqueries are rewritten into joins, and the planner refuses
the shapes it cannot rewrite (a `NOT EXISTS` over a read that extracts
JSON is one); the refusal names it — write the LEFT JOIN and a count
instead.

What lands wrong without failing: a `LIKE` guard before a `CAST` in
the same WHERE (conjuncts reorder — only `try_cast` is safe on dirty
text) · aliasing a projection to its own qualified source name
(`round(j.x, 2) AS x`) · **two unaliased scalar subqueries in one
projection** ("Projections require unique expression names" — alias
both, or compute in a CTE) · `try_to_timestamp` with a date-only
format (NULL — `try_to_date` parses it; a timestamp format must cover
the whole value, seconds included) · an inner `ORDER BY` does not
survive a derived relation, so order where you consume.

A column that landed as text: `max`/`min` are lexicographic (`'99' >
'100'`) and `sum` refuses (`Function 'sum' requires Decimal, but
received String`). The cast belongs in the recipe; `try_cast` at the
read is the stopgap.
