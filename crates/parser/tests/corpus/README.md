# glossql transcription corpus

Each file pairs a **real artifact** — the predecessor production system's
(quoted inline, with its original path) or, from fixture 14 on, this
system's own runs — with its glossql transcription per SPEC.md. These are
test fixtures, not design docs.

Block tags, enforced by the corpus suite in `crates/parser`
(`cargo test -p glossql-parser`):

- ` ```glossql ` — must parse under `grammar.ebnf`. A failure is a regression.
- ` ```glossql-gap ` — invented syntax documenting a grammar gap; must **fail**
  to parse. When the grammar gains the form, the checker flags "gap closed" and
  the tag flips to ` ```glossql `.

Verdict vocabulary: TRANSCRIBES · GRAMMAR GAP · SEMANTICS UNDEFINED ·
INFORMATION LOST · DROPPED BY DESIGN (a deliberate cut — the record names what
was dropped and where it went). One fixture may carry several, per field.

The predecessor sources are a fixed snapshot; transcriptions follow the
simplified language.

| # | fixture | verdict (dominant) |
|---|---|---|
| 01 | concept `revenue` | TRANSCRIBES (concept = QUERY aspect) · pack envelope DROPPED |
| 02 | convention `sign_natural_balance` | TRANSCRIBES (in-blob, incl. `targets`) |
| 03 | metric `dso` | TRANSCRIBES (metric = QUERY aspect, run as its SQL; function form superseded · value-at-read parked in SPEC §9) |
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
| 14 | composite relationships (finance_2 run) | TRANSCRIBES (composite = tuple endpoint; view cure retired) |
| 15 | consumption surface (cockpit sweep) | TRANSCRIBES (reads compose it) · axis additivity SEMANTICS UNDEFINED · conformed-group FORK open |
| 16 | flow: performance framework (scorecard target) | TRANSCRIBES (extracts + formulas; windows are read policy; `read.` value-at-read ruled, bind deferred to UI) |
| 17 | relational spine (sqlite run) | TRANSCRIBES (existing surface end to end) · temporal type INFORMATION LOST on this dialect (read-time cast carries it) |
| 18 | app authoring (model app) | GRAMMAR GAP (`$$`-carried app artifacts, per-artifact form) · publish verb SEMANTICS UNDEFINED |
| 19 | what-if scenario (evaluation runs) | TRANSCRIBES (scenario = FACT aspect per scenario; `whatif.` door; replay grid is machinery) |
| 20 | misfit sample frame (evaluation runs) | TRANSCRIBES (frame = QUERY aspect + `x-kind`; `misfit.` door; self-fit density is machinery) |
| 21 | source conventions (both halves run) | TRANSCRIBES (`AS FACT ON SOURCE`; deposit → read-before-probe → workspace-wide supersede) |
| 22 | question loop (pin-queue run + elicitation spike) | RULED: no question object — GLOSS + actor kind is the whole record; questions are ephemeral transport · `ASK` GRAMMAR GAP kept as evidence · pin_questions DROPPED BY DESIGN |
| 23 | conditional relevance (medium run) | RULED: `WHEN` narrows what a subject owes — the backlog counts what is real, not columns × vocabulary |
| 24 | functions in a table (run 4) | RULED: a function's body is data — `AS $$…$$` replaces `FROM 'path'`, the `functions/` directory retires, and the shipped library reads back as examples · body-in-a-file INFORMATION LOST |
