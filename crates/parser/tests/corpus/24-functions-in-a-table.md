# 24 · Functions carry their own body — the run-4 verdict

Source: our own test run (2026-08-15,
`reports/2026-08-15-onboarding-run-shrunk.md`), driven through the MCP
door alone. Two validations were judged, authored and left half-built:
the inventory roll-forward closed exactly on 5,280 of 5,280
product-location-months, and the AP mismatch analysis settled that a
payment settles its bill in full. Both landed as expectation glosses
with their tolerances. Neither got a measuring voice, because a
function's body lived on disk and **an agent connected over MCP has
statements, not a filesystem**.

The same wall stands in front of reading one. The reference library is
fifteen worked examples that ship in every workspace, and the skill
that taught function authoring said "read the one closest to your task
before writing" — pointing at a directory the door cannot open:

```sql
SELECT name, script FROM functions
-- band_breach | functions/band_breach.rhai
```

Ruled 2026-08-15: **a function's body is data.** `FROM 'path'` is
replaced by `AS $$…$$`, the store keeps the script itself, and the
`functions/` directory retires. Writing a function and reading one
become the same two statements every other kind of knowledge uses.

## The declarations the run could not make

```glossql
USE fin;

DECLARE FUNCTION inventory_rollforward_check FOR fin AS $$
  let m = db.query("SELECT sum(abs(gap)) AS bad, count(*) AS n FROM (
    SELECT s.units_on_hand
             - lag(s.units_on_hand) OVER (PARTITION BY s.product_id, s.location ORDER BY s.period)
             - coalesce(f.moved, 0) AS gap
    FROM stock_levels s
    LEFT JOIN monthly_moves f
      ON f.product_id = s.product_id AND f.location = s.location AND f.period = s.period)");
  let n = m.number("n", 0);
  #{
    "outcome": "prior level plus the month's movements equals this month's level",
    "breach_rate": if n > 0 { m.number("bad", 0) / n } else { 0.0 }
  }
$$ ACCEPTS (imports) RETURNS inventory_rollforward;

DECLARE FUNCTION ar_settles_in_full_check FOR fin AS $$
  let m = db.query("SELECT count(*) FILTER (WHERE settled < billed) AS short,
                           count(*) AS n FROM ar_settlement");
  let n = m.number("n", 0);
  #{
    "outcome": "a receipt settles its invoice in full; a short receipt is the exception",
    "breach_rate": if n > 0 { m.number("short", 0) / n } else { 0.0 }
  }
$$ ACCEPTS (imports) RETURNS ar_settles_in_full;

DECLARE WITNESS inventory_rollforward_w ON inventory_rollforward
  BY (AGENT, HUMAN) DETECTOR rate_tolerance THRESHOLD 0.0;
```

## Reading a shipped one is the same statement it always was

The exemplar set becomes readable the moment the body is the column,
so the authoring instruction stops pointing outside the door:

```sql
SELECT script FROM functions WHERE name = 'rate_tolerance'
```

## The retired form

```glossql-gap
DECLARE FUNCTION outliers FOR GLOBAL FROM 'functions/outliers.rhai'
  ACCEPTS (column_profile) RETURNS outlier_profile;
```

## Findings

- **The body is the only thing that moved.** `FOR`, `ACCEPTS`,
  `RETURNS` and role-by-shape (no `RETURNS` declares a detector) are
  untouched, and so is the script contract — `subject`, `context`,
  `db`. A declaration that named a path now carries what the path
  pointed at.
- **Two capabilities arrive with it, and neither is new grammar.**
  Authoring: the check half of a validation is writable over the door,
  which run 4 could not do. Reading: `SELECT script FROM functions`
  serves the reference library as fifteen worked examples instead of
  fifteen paths, so a skill can say "read the closest one" and mean it.
- **`FROM` and `AS` did not both survive.** Two homes for one body is
  the staleness the `x-unit` ruling (2026-08-12) was written against —
  the copies disagree and nothing says which wins. The path form is
  retired rather than deprecated; the gap block above is its epitaph.
- The workspace `functions/` directory retires with it. Bootstrap
  carried the reference scripts to disk so an operator could edit
  them; an operator edits them with a statement now, like everyone
  else, and the declaration supersedes.
- INFORMATION LOST (accepted): a function body no longer sits in a
  file an editor can open, so authoring it is composing a
  dollar-quoted string. The reference library stays in the repo as the
  source the binary ships from — a file for the operator who builds
  the server, never a file the workspace reads.
