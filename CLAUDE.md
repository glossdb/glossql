# glossql — workspace rules

The context language and its server. Current phase: **PoC server
build-out**; corpus fixture 11 is the acceptance test. Grammar changes
follow the corpus-first process below.

## The map

- `SPEC.md` — the only normative language prose. Open questions live
  in §9 and close by transcription verdict, never by argument.
- `grammar.ebnf` — the machine-readable grammar; the source of truth
  for syntax.
- `crates/parser/tests/corpus/` — transcriptions of real artifacts,
  the parser's acceptance suite: ` ```glossql ` must parse,
  ` ```glossql-gap ` documents a gap and must fail.
- `crates/` — the server, a Cargo workspace: `parser` · `glossary` ·
  `session` · `catalog` · `import` · `scripts` · `apps` · `serverd`.
  What each is and how it is built: the crate headers and
  `docs/architecture/`. Directories unprefixed, packages `glossql-*`;
  datafusion moves in lockstep with iceberg-datafusion, sqlx with
  iceberg-catalog-sql (the workspace `Cargo.toml` comment).
- `docs/` — the curated statement of the system: what is implemented
  and what is planned (`start/`, `concepts/`, `reference/`,
  `methods/`, `architecture/`, and the concern pages). For agents and
  humans alike; enterprise-customer quality, constantly current.
  Never normative language prose.
- `../glossin` — the product skills (vendor-neutral Agent Plugins
  shape), where a door-connected agent learns the language; a
  required sibling checkout, gated by this repo's test suite.
  `.claude/skills/` keeps the substrate skill for building the
  server.
- `.claude/notes/` (gitignored) — working notes tied to an open
  issue; deleted when it closes.

## The record: issues, code, docs, notes

GitHub issues (`glossdb/glossql`) are the work record. One issue is
one actionable unit at a uniform depth: what it buys, what stands
(`file:line`, fixtures), done-when. Apache Software Foundation
quality, and public: facts only — no session narrative, no run
anecdotes, no actor names, nothing private, no other vendor's product
as a reference point. Labels stay small: `bug` · `ruling-candidate` ·
`upstream` · `debt` · `docs`. Grammar and language questions never
become issues — SPEC.md §9 keeps that role.

Code and its comments are the source of truth for what is implemented
and live. A comment states a standing constraint or behavior the code
cannot show — never history, never a run anecdote.

Docs state what the system is and what is planned — never what was
rejected, reversed, or not built. Git history is the time machine;
what is written is the rule — no "ruled [date]" stamps anywhere.

Roadmapping is ad hoc: when an issue closes, the project lead picks
the next one or bundles a few in a session. No standing sequence.

## Ground rules

- **One document.** SPEC.md is the only normative prose — no
  satellite design docs, no assumption files, no per-topic notes.
- **Standing invariant.** Workspace `cargo test` passes: every
  ```sql block in SPEC.md parses, every corpus fixture behaves as
  tagged, every fenced example under `docs/`, `.claude/skills/`, and
  the `../glossin` sibling parses and plans, and the store and
  session suites hold the execution semantics. A change that breaks
  it doesn't land.
- **Ideation before prose.** No idea enters SPEC.md until it survives
  a corpus test: competing statement forms for the same real
  artifact, checked against grammar and real table shapes, the forks
  presented to the project lead. The surviving fork becomes a SPEC.md
  diff that shrinks or holds the spec, never grows it by essay.
- **Build on the substrate.** Extend DataFusion and iceberg-rust at
  their own seams, never around them; the glossql-substrate skill
  carries the seam register and its rules.
- **Grounding.** The corpus fixtures are the empirical record;
  coverage and semantics questions settle against this repo's own
  code and runs.
- **Design authority.** The language has a single owner: the project
  lead. Every grammar change is reviewed by them; propose as SPEC.md
  edits with rationale — the grammar never drifts through
  implementation convenience. Sober voice everywhere: definition
  before significance, claims sized to named mechanisms, no selling.

## Language decisions in force

Work in progress, not settled — the project lead may reopen any of it:

language before implementation · a workspace holds many datasets, an
app binds to one · everything-context is JSON against JSON Schemas ·
the aspect trichotomy (`AS MEASUREMENT | FACT | QUERY`) with one
uniform `GLOSS` statement · supersession key (subject, aspect, actor
kind) · actor rides the transport, no BY clause · functions are
scripts with JSON contracts · witness slot model with detector
adjudication (band + score) · judgment in detectors and read policy,
never in results · authored prose is opaque · `GLOSS` is the write
verb, `GLOSSARY()`/`ATTEST()` are the reads.

## Held open (do not decide in passing)

Persistence backend · engine substrate and its mapping · governance
and access rights · actor transport mechanics · cross-workspace
portability.
