---
name: glossql
description: Speak glossql through the server's MCP door — the statement set, the outcome shape, and where the normative artifacts live. Use when reading or writing anything in a glossql workspace (datasets, sources, recipes, glosses, functions, witnesses).
---

# Speaking glossql

glossql is one SQL-shaped surface for a workspace's data *and* its
context. Data lands in tables; context is JSON attached to subjects
(`table` or `table.column`) under declared aspects. Two artifacts are
normative — read them, don't reconstruct them:

- `SPEC.md` — the language specification. §2 maps constructs to the
  system they transcribe; §3 sources, recipes, tables; §4 subjects and
  relationships; §5 the glossary (aspects, glosses, reading); §6 the
  function library; §7 witnesses and attestation.
- `grammar.ebnf` — the machine-readable syntax.

## The door

One MCP tool, `glossql`. Its `statements` argument takes a statement or
a semicolon-separated sequence; the result is a JSON array, one outcome
per statement:

- a read — `{"rows": [...], "row_count": n, "truncated": bool}`. Data
  rows are capped at the server's `--row-cap`; `truncated: true` means
  the result held more than shown — refine (aggregate, WHERE, LIMIT)
  instead of reading a capped result as complete. Metadata reads —
  `GLOSSARY()`, `ATTEST()`, and the store relations — sent as their
  own single statement are uncapped: the map arrives whole.
- a write — `{"affected": n}` or `{"done": true}`.
- a refused statement — a tool error whose text is the refusal. Read
  it; it names what was wrong.

Who you are (agent or human) rides the connection — there is no BY
clause. `USE <dataset>;` picks the dataset and survives between tool
calls: the server keeps one session per actor.

## The statement set

| statement | does | SPEC |
|---|---|---|
| `USE fin;` | pick the dataset for this session | §3 |
| `DECLARE DATASET fin SET (…);` | create a dataset | §3 |
| `DECLARE SOURCE erp SET (type: parquet, location: 'root');` | register a source; location is a root directory, globs belong in recipe SQL | §3 |
| `PROBE erp AS $$sql$$;` | run recipe-shaped SQL at the source, landing nothing | §3 |
| `DECLARE RECIPE orders ON fin FROM erp AS $$sql$$;` | land the table the SQL produces — the landed table is the typed table | §3 |
| `DROP TABLE orders;` | remove a table — refused while it holds data | §3 |
| `DECLARE RELATIONSHIP a.col -> b.col;` | declare a join edge (`<->` both ways); a composite endpoint is a tuple: `a.(x, y) -> b.(x, y)` | §4 |
| `DECLARE ASPECT name WITH $$json-schema$$ AS MEASUREMENT\|FACT\|QUERY [ON TABLE, COLUMN, …];` | add to the vocabulary; the schema is the one validated contract; `ON` is the grain — the subject classes it speaks to (DATASET/TABLE/COLUMN/RELATIONSHIP/SOURCE, absent = all), and `unassessed` disclosure stays within it; SOURCE-grain slots read and supersede across datasets | §5.1 |
| `GLOSS aspect ON subject AS $$json$$;` | speak a value into your slot | §5.2 |
| `SELECT … FROM GLOSSARY(subject);` | the collapsed context; `all => true` for every slot | §5.3 |
| `DECLARE FUNCTION f FOR fin\|GLOBAL FROM 'f.rhai' [ACCEPTS (…)] [RETURNS aspect];` | register a script (see the glossql-functions skill) | §6 |
| `SELECT f() FROM orders.amount;` | extract — first run computes and caches, later selects read the cache | §6 |
| `DELETE FROM cache WHERE …;` | force recomputation at the WHERE clause's grain | §6 |
| `DECLARE WITNESS w ON aspect [BY (AGENT, HUMAN)] [DETECTOR f THRESHOLD x];` | admit speakers, wire adjudication | §7.1 |
| `SELECT … FROM ATTEST(subject \| fin::aspect);` | bands and scores; sweeps are WHERE clauses | §7.2 |

There is no ordering surface: send statements in the order you need
them, one call or several.

Schema-altering substrate DDL — `CREATE VIEW` included — is closed
(SPEC §3): tables come from recipes, and a composite edge is declared
as a tuple, never cured through a view.

## Reading live state

Never guess at workspace state — read it through the language, where it
is always current:

- `SELECT * FROM glossary` / `cache` / `imports` / `relationships` —
  the store's relations as plain tables (who said what; what is
  computed; source rows vs landed rows; the declared join edges).
- `GLOSSARY(subject)` — collapsed values with `state`
  (`current | stale | contested | unassessed`); a contested value is
  withheld, and absence is a visible row.
- `ATTEST(…)` — `(subject, aspect, witness, band, score, computed_at)`,
  band in green/yellow/orange/red.
- ordinary SELECT over tables for the data itself.

## Measurements over-produce — you are the judge

Detection functions are tuned toward recall: they emit *candidates*
with evidence, never conclusions, and false positives are expected.
Reading a measurement is a judging job — verify each candidate against
the data itself, then declare or gloss only the survivors. The rejects
stay in the measurement, visible and undeclared; never delete them to
"clean up". Judgment lives in your reads, not in the function.

## When a slot contests

`state = 'contested'` means voices differ on one slot and the
detector's score crossed the witness threshold — the value is withheld,
never adjudicated for you. Read the slots
(`GLOSSARY(subject, all => true)`), re-ground the question in the data,
and re-gloss only if the evidence moved you: your new gloss supersedes
your old one, and converged voices turn the band green. If the evidence
still says you were right, leave the slot contested — a human closes
it, by conceding in their own slot or by striking one
(`DELETE FROM glossary WHERE subject = '…' AND aspect = '…'
AND actor_kind = 'agent'`). Never change a gloss just to end a contest.
