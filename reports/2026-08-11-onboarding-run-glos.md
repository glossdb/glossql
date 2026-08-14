# 2026-08-11 — Onboarding evaluation: first real-world run (glos manufacturing data)

Evaluation record of a full onboarding run against external company data —
three MSSQL-style parquet exports from a Swiss window manufacturer
(production confirmations, WIP snapshot, cell master). The agent drove all
four flows through the MCP door against `serverd` (`--workspace
~/glossql-ws`): add-source → relationships → dimensions → metrics, with the
project lead answering one pin question mid-run. Every artifact referenced
below is still in `~/glossql-ws` (glossary.sqlite; dataset `glos`) and every
finding can be reproduced with the door reads quoted.

Companion to `corpus/18-flow-definitions-first.md`: §4 below validates that
fixture's claims against this run and records what a no-context assessment
could not see.

Run shape: 3 tables landed (110,249 / 33,905 / 18 rows, 47 columns), 4
recipe supersessions, 8 declared relationships, 21 dimension verdicts, 4
groundings + 1 recorded evaluation, 3 validations (all green), ~50 door
calls end to end. One engineer correction mid-run ("pieces count at barcode
level") exercised the supersession path 8 glosses deep.

## 1. What held — mechanisms validated by the run

- **Recipes as the cure surface.** Four supersede-and-reland cycles, each
  triggered by measured evidence: a `Zeit` column whose 1899-epoch
  assumption broke (timestamps landing in 2152 — cured by subtracting the
  column's own midnight), `1900-01-01` placeholder dates (85% of
  `wunschtermin`), a leaked operator note polluting seven columns of 454
  rows (`NULLIF` before cast), and a cell-key spelling normalization
  (`zelle_key`). Casts-clean accounting and `DESCRIBE` at the decision
  moment worked as specified.
- **Judge over measurement.** `detect_relationships` returned zero
  candidates — correctly: the only near-unique key column spells its codes
  without the `Z` prefix the fact tables use, so overlap was nil. The
  judged read recovered the edges: the `Z120 = Z110_120 = '110/120'`
  identity was grounded in data (both sit exclusively at meldepunkt
  `03_Umfahrung`), not name similarity, then landed as recipe-authored
  `zelle_key`. `detect_hierarchies` over-produced as designed (~100
  candidates); the λ-vacuity rule and the null-cluster reading filtered to
  5 declared nests. High recall + agent precision behaved as the skills
  teach.
- **Grain checks earned their place.** The rueckmeldungen→zellen join
  drops 1,469 rows INNER (LEFT verdict recorded on the pair); the
  vorrat→zellen join preserves exactly; the fact-to-fact composite is
  many-to-many and recorded as aggregate-first. All three verdicts were
  cheap and all three would have corrupted metrics silently if skipped.
- **Composition propagated an executable definition change.** The recorded
  `wip_reichweite` evaluation composes `FROM metric.wip_stock()`. When the
  engineer's correction re-grounded `wip_stock` (value = 1 per element
  instead of `sum(stueck)`), the composed numbers corrected themselves —
  the re-record was only needed for the assumptions prose. The
  `metric.<aspect>()` composition design did exactly what it exists for.
- **The validation pattern carried the "expected dirt" case.** Three
  validations (duplicate-booking rate, cell resolution, orphan rate) as
  expectation gloss + check voice + `framework_bands` detector + `ATTEST`.
  The duplicate check bands green at the *known* 1.26% rate — a 0.0 report
  would band red as overcleaning. Authored expectations beside check
  voices, one schema per aspect: no friction.
- **`unassessed` and `ATTEST` as read-back surfaces** — used throughout;
  absence stayed visible; nothing contested; bands answered.

## 2. What broke or went missing — findings, ranked

Each finding names its specimen in the workspace.

**F1 — The two-plane asymmetry. Anything a company would revise must live
where supersession lives — and today that is only the gloss plane.**
Specimen: the `wip_stock` aspect still says `x-unit: "pieces"` while its
grounding counts elements. The engineer's correction superseded eight
glosses in one act — meaning, role, entity, grounding, formulas, recorded
evaluation — every one with actor and timestamp; the aspect `WITH` blobs
changed zero times and one is now stale. Glosses supersede, contest,
attest, outrank by actor. Declarations (aspects, recipes, relationships,
witnesses) have none of that. Consequence for fixture 18 §1: putting the
KPI handbook's definition of record into aspect `description` puts the
company's most contested knowledge on the plane with the least machinery —
handbook v4 has no supersession story there. Definitions belong in gloss
bodies on thin aspects; the fixture's own `formulas` FACT gloss is the
right pattern (it superseded cleanly mid-correction in this run).

**F2 — No judged negative for aspect applicability; the backlog read never
converges.** Specimen: the workspace sits at a permanent floor of 276
`unassessed` rows, mostly `behavior`/`unit` slots on columns where those
aspects genuinely do not apply (a text column has no stock/flow verdict).
"Not yet judged" and "never applicable" are different facts wearing the
same row, and there is no way to close a slot as N/A. The dimensions plane
already solved this locally (`dimension: "none"` is a judged negative with
grounds). The general rule fixture 18 §2 needs: every witnessed aspect
needs its judged negative, or the stakeholder's count-to-zero backlog read
is dead at 47-column scale.

**F3 — The human slot went unused in the one flow built for it.** The
engineer pinned a definition mid-run; the gloss that landed is an *agent*
slot at confidence 0.85 citing "engineer confirmed 2026-08-11" in prose.
If the engineer later disagrees, they contest their own relayed decision.
The actor model's centerpiece — HUMAN outranks — was bypassed because the
human act has no low-friction path into the human slot (it would require
speaking glossql through the door as the HUMAN actor). Approval needs a
one-gesture path that writes the human slot, or actor ranking is
decorative in exactly the definitional flows it differentiates.

**F4 — Definition changes fan out; the executable half propagates, the
prose half is memory.** The stueck correction's blast radius was eight
glosses; the composed read corrected itself (see §1), but meaning / role /
entity / unit were traced by the agent remembering where the old
definition had leaked. No reverse index exists from a definition to its
dependent glosses; the "change one, update the other in the same act" rule
is discipline, not mechanism. At company scale a handbook revision's blast
radius is invisible. A `grounding_collisions`-style detector over
assumption `basis` strings is a plausible first mechanism (recall, agent
judges).

**F5 — Knowledge deposits are workspace-trapped; onboarding cost is flat
per source.** What this run deposited and where it lives: source-system
conventions (mixed German/English month abbreviations, `%b %e %Y %I:%M%p`,
the `1900-01-01` placeholder, the note-leakage token) — FACT glosses in
*this* dataset; key spellings (`Z` prefix) — a recipe in *this* dataset;
the pinned element-grain definition — glosses in *this* dataset. The next
export from the same ERP starts blind. The de-facto central mechanism
today is replay-from-skill-documents, and this run displayed its failure
modes: unversioned, identity-free, agent-typed. Fixture 18 §4's loss list
underclaims: replay and bootstrap lose not just concept identity and
vocabulary version but the *trust history* of glossed knowledge — speaker,
time, evidence lineage all reset on arrival. Whatever shape the held-open
ruling takes, the unit of centrality is a shared glossary plane (subjects
at company grain, existing supersession/witness machinery intact), not a
declarations file — it is the only candidate that preserves what the other
two demonstrably drop.

**F6 — Recipe-borne semantic corrections execute without an approval
surface.** The `Z120 → Z110_120` mapping is baked into the vorrat recipe.
The grounds are glossed on the relationship pair, but the gloss documents;
nothing gates. A wrong judgment here rewrites landed data with no
witnessed act to contest. Smaller than F1–F5, same root as F1.

**Dev notes (minor, door ergonomics):** MCP sessions are reaped between
calls with a bare `Not Found` (the client needs re-init + `USE`
replay — cost one wasted round trip per resumption); case-sensitive
identifiers surprised on first probe (error message was good); the
aggregate-alias collision (`count(x)` vs `count(cast(x))`) matched the
skill's warning exactly — the subquery-alias rule held.

## 3. The onboarding cost curve — the product question

This run: ~50 door calls, four flows, heavy judgment. Dataset N+1 from the
same company costs the same today — nothing deposited here is withdrawable
there. For the medium-company target (dozens of sources), the curve has to
bend: each onboarding should deposit into a layer the next one reads.
Decomposition observed in this run, with different rightful homes:

- **Company definitions** (what a Stück is, which formula family
  "Reichweite" means): global, owned, revisable → needs F1 + F5 resolved.
- **Source-system conventions** (export dialect warts, placeholders, key
  spellings): global, keyed by source system → a `conventions` gloss at
  SOURCE grain would let the add-source flow read before probing.
- **Dataset-local evidence** (grain verdicts, orphan populations, edges):
  correctly local — the Z180_270 orphans and the ×66 barcodes are facts
  about this export, not the company. Centralizing them would rot. The
  `basis` field is the seam: local verdicts citing global definitions.

## 4. Fixture 18 validation verdict

| Fixture claim | Verdict | Evidence from this run |
|---|---|---|
| Definitions-first transcribes as declaration order, no new construct | **Holds** | Metrics phase declared concept aspects in exactly that shape; nothing orders vocabulary after data |
| Onboarding backlog is the `unassessed` read | **Holds in mechanism, fails at scale** | Used throughout; permanent 276-row floor without judged negatives (F2) |
| Deviation duty inverts the pinning agenda | **Holds, with a gap** | The pin round trip ran end to end; but the human's answer landed in an agent slot (F3) |
| Provenance rides the existing blob (aspect `description`) | **Rejected as the definition home** | The blob is outside supersession; stale `x-unit: "pieces"` is the specimen (F1). Citation-for-readers: fine. Definition-of-record: gloss bodies |
| Central-vocabulary fork: replay / bootstrap / construct | **Confirmed as the real fork; loss lists incomplete** | Replay is already live as skill documents; both non-construct options also lose gloss provenance (F5) |
| Term-to-data lookup needs no construct at one-workspace scope | **Holds** | `GLOSSARY(glos)` filtered reads served it |

## 5. Ordered next steps (sized)

1. **Judged negatives per witnessed aspect** (F2) — vocabulary convention
   plus skill teaching; no grammar growth; unblocks the backlog read.
2. **Definitions-in-glosses convention for concepts** (F1) — fixture 18 §1
   edit plus skill teaching; aspects stay thin schema; no grammar growth.
3. **A human pin path** (F3) — product/door work: an approval gesture that
   writes the HUMAN slot (the app door is the natural surface). Without
   it, the actor model does not differentiate in practice.
4. **Definition-dependency read** (F4) — start as a detector over
   assumption bases; recall, agent judges; no grammar growth.
5. **The company grain** (F5, the §4 fork) — project-lead ruling,
   corpus-first: competing statement forms tested against this run's
   workspace as the real artifact. The evidence says the ruling shape is a
   shared glossary plane, not a vocabulary file.

Sober summary: the four flows and the judge discipline survived first
contact with real company data — nothing in the statement spine failed.
What failed is where knowledge *settles*: definitions sit on the wrong
plane (F1), absence cannot be closed honestly (F2), the human's authority
arrives as prose (F3), revisions fan out by memory (F4), and nothing
carries across workspaces (F5). All five are pre-construct: two are
conventions, two are product surfaces, one is the held-open ruling.
