# glossql transcription corpus

Each file pairs a **real artifact** from `../dataraum-context` (quoted, with path)
with its glossql transcription per SPEC.md. These are test fixtures — the §9.1
evidence base — not design docs.

Block tags, enforced by the corpus suite in `crates/parser`
(`cargo test -p glossql-parser`):

- ` ```glossql ` — must parse under `grammar.ebnf`. A failure is a regression.
- ` ```glossql-gap ` — invented syntax documenting a grammar gap; must **fail**
  to parse. When the grammar gains the form, the checker flags "gap closed" and
  the tag flips to ` ```glossql `.

Verdict vocabulary: TRANSCRIBES · GRAMMAR GAP · SEMANTICS UNDEFINED ·
INFORMATION LOST · DROPPED BY DESIGN (a deliberate cut — the record names what
was dropped and where it went). One fixture may carry several, per field.

Sources snapshot 2026-07-30; transcriptions re-done 2026-08-03 against the
simplified language (see `reports/2026-08-03-simplification.md`).

| # | fixture | verdict (dominant) |
|---|---|---|
| 01 | concept `revenue` | TRANSCRIBES (concept = QUERY aspect) · pack envelope DROPPED |
| 02 | convention `sign_natural_balance` | TRANSCRIBES (in-blob, incl. `targets`) |
| 03 | metric `dso` | TRANSCRIBES (metric = QUERY aspect, run as its SQL; function form superseded 2026-08-03 · value-at-read parked in SPEC §9, re-flagged interesting 2026-08-05) |
| 04 | validation `trial_balance` | TRANSCRIBES (aspect + witness, no dedicated construct) |
| 05 | cycle `accounts_receivable` | TRANSCRIBES (in-blob) |
| 06 | witnesses + reliabilities | slots TRANSCRIBE · calibration DROPPED |
| 07 | grounding / `sql_snippets` | TRANSCRIBES (standard grounding schema) |
| 08 | teach payloads | TRANSCRIBES (teach = re-gloss) |
| 09 | answer-agent served context | DROPPED BY DESIGN (the agent experiment) |
| 10 | remaining engine artifacts | TRANSCRIBES (coverage completion) |
| 11 | flow: add source | TRANSCRIBES (no flow construct; orchestration app-side) |
| 12 | flow: begin session | TRANSCRIBES (measure → read → declare → attest) |
| 13 | typing patterns | TRANSCRIBES (patterns as FACT glosses) |
| 14 | composite relationships (finance_2 run, 2026-08-05) | TRANSCRIBES (composite = tuple endpoint; view cure retired) |
| 15 | consumption surface (cockpit sweep, 2026-08-06) | TRANSCRIBES (reads compose it) · axis additivity SEMANTICS UNDEFINED · conformed-group FORK open |
| 16 | flow: performance framework (scorecard target, 2026-08-06) | TRANSCRIBES (extracts + formulas; windows are read policy; `read.` value-at-read ruled, bind deferred to UI) |
| 17 | relational spine (sqlite run, 2026-08-07) | TRANSCRIBES (existing surface end to end) · temporal type INFORMATION LOST on this dialect (read-time cast carries it) |
| 18 | app authoring (model app, 2026-08-11) | GRAMMAR GAP (`$$`-carried app artifacts, per-artifact form) · publish verb SEMANTICS UNDEFINED |
| 19 | what-if scenario (evaluation runs, 2026-08-11) | TRANSCRIBES (scenario = FACT aspect per scenario; `whatif.` door; replay grid is machinery) |
