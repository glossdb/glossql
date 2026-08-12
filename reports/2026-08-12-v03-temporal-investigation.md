# 2026-08-12 — What v0.3 actually did about temporal typing and cast accounting

Build-line item 6 said: investigate `../dataraum-context` before
proposing anything for the relational path — v0.3 "solved the
timestamp half in part". This is that report. Swept thoroughly,
load-bearing claims re-verified in source; file:line throughout.
The 2026-08-07 no-landing-side-machinery ruling stands; nothing here
is a proposal, the closing section names the open questions.

## The half v0.3 solved: string dates from untyped sources

For file sources (everything lands VARCHAR-first), v0.3 detects date
formats **by value patterns, never column names**, and the whole
mechanism is config, not code:

- Nine temporal regex patterns in
  `packages/dataraum-config/phases/typing.yaml:16-74`, each carrying
  an `inferred_type` and a DuckDB `standardization_expr`
  (`STRPTIME("{col}", '%m/%d/%Y')`; `2024-Q1` becomes a `MAKE_DATE`
  expression). US/EU slash dates share one regex, marked
  `ambiguous: true` — whichever STRPTIME parses wins.
- Two gates, then an average: match rate over ≤10,000 distinct
  sampled values must clear 0.5, full-column parse rate must clear
  0.8 (`analysis/typing/inference.py:242-255`), confidence is their
  mean (`:276`) against `min_confidence: 0.85`; below it the column
  falls back to VARCHAR. Mixed-format columns compose a `COALESCE`
  of several patterns' exprs (`inference.py:295-351`).
- The typed artifact is a **physical typed table built from
  generated DDL** (`analysis/typing/resolution.py:331-356`), and the
  DDL is stored as a `MaterializationRecipe` row per table and run
  (`analysis/typing/db_models.py:160-219`). In glossql words: v0.3
  *generates the recipe*; glossql's ruling makes the agent author
  it. The construct is the same — what glossql lacks is only the
  measurement that proposes the format, not any machinery.
- A live wart worth keeping: a bare `TRY_CAST` over-rejects formats
  the pattern parser accepts (their DAT-457 — `"2025-02"` parses via
  STRPTIME, not via cast), and an inner `STRPTIME` throws through an
  outer `TRY_CAST`, so they rewrite inner functions to `TRY_*` at
  pattern load (`analysis/typing/patterns.py:26-43`).
- The teach loop is a config overlay: a `type_pattern` override
  (regex + expr) merged over the YAML (`core/overlay.py:124-144`) —
  one of only three teaches an agent may auto-apply. glossql's
  equivalent already exists and is finer-grained: taught formats are
  glosses, and since fixture 21 they ride source grain across
  datasets.

Cast accounting on this path has three surfaces: a row-grain
quarantine table (any column's failed cast quarantines the whole raw
row, `resolution.py:359-383` — no column attribution; they recompute
it from raw when needed, `entropy/detectors/loaders.py:131-150`),
per-column `parse_success_rate` / `quarantine_rate` / ≤5
`failed_examples` on the type candidates
(`analysis/typing/db_models.py:87-103`), and an unamplified
type-fidelity score on top (`entropy/stats.py:71-78`). glossql's
file-path accounting is cell-grain (a failed cast is a kept row with
a NULL cell, counted per token in `imports.cast_failures`) — the
finer of the two, nothing to import here.

## The half v0.3 did not solve: the relational path

- **There is no ADBC anywhere in that repo** — zero hits. Database
  sources go through DuckDB `ATTACH` extensions
  (`sources/backends.py:22-47`), and whatever DuckDB's scanner says
  becomes the type of record.
- **Value-level verification of a declared type is explicitly
  declined**, with the rationale stated in code
  (`pipeline/phases/typing_phase.py:145-151`): TRY_CASTing a
  natively-typed column against its own type can never fail, so only
  a schema mismatch is physically detectable — the safety net
  (DAT-748) is a re-DESCRIBE cross-check of catalog vs live schema,
  not a data scan. Consequences, all in code: no quarantine table
  for DB sources (`:279-281`), no candidates and therefore no parse
  rates, fidelity score 0.0 unconditionally for every DB-sourced
  column.
- **SQLite weak typing is dead code**: a `TypeSystemStrength` enum
  names SQLite as "weak — advisory types"
  (`sources/base.py:84-89`) and is referenced nowhere; the live test
  is binary — any non-VARCHAR column makes the whole table trusted
  (`typing_phase.py:126-129`), which also means **a VARCHAR date
  column inside an otherwise-typed DB table never gets date
  detection.**
- The one net that catches a string date *after* typing gave up is
  semantic, not structural: if the LLM annotated a column
  `timestamp` and its resolved type is not datetime-like, an entropy
  detector scores 1.0 and samples ten raw values as evidence
  (`entropy/detectors/semantic/temporal_entropy.py:97-140`). It
  depends entirely on the annotation.

So the memory "v0.3 solved it partly" resolves precisely: it solved
**format detection for untyped (file) sources**. On the relational
path it holds the same posture glossql already holds — trust the
wire, verify schema not values — except glossql says so honestly at
the decision moment (`CastAccounting::Unchecked`, DESCRIBE-at-landing
taught in the add-source skill) where v0.3 silently reports fidelity
0.0.

## What this means for item 6 (open, not decided)

1. **The file-source half maps to one shipped measurement, not
   machinery.** A format-detection function (recall: per-column
   pattern candidates with match/parse rates, the two-gate shape is
   proven) whose output the agent judges before *authoring* the
   recipe cast — the ruled posture untouched, the guess-the-format
   gap closed. Detected conventions would feed the source-grain
   deposit (fixture 21). Whether to ship it is the lead's call.
2. **The relational half has no precedent to import.** v0.3 declined
   value verification with a rationale that holds for glossql too.
   The honest levers already exist (landed-identity read, meaning
   glosses carrying the read-time cast); the one cheap addition
   v0.3 suggests is a role-vs-type check — a column glossed
   `role: timestamp` whose landed type is text is a red flag no one
   computes today. A detector-shaped question, not a landing change.
3. **DAT-457 belongs in the teaching**: `try_cast` rejects formats
   `try_to_date` accepts — worth one line in the add-source skill's
   probe section whenever it is next touched.

The 2026-08-07 ruling stands as written.
