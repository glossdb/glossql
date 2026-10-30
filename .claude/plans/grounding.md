# The grounding plan

The evaluation behind one SPEC.md §5.2 change: the standard grounding
schema becomes the **grounding contract**, what the engine reads of a
grounding, plus a **grounding schema a kit declares**, which the engine
validates every QUERY gloss against. The cube's three keys, `behavior`,
`grain` and `axes`, become the KPI kit's: declared once in its script,
taught by its skill, enforced by the engine. The change surfaced while
building the DuckDB extension beside this repo (`../gloss`), a host
with the grammar and the store and no cube; the extension runs this form
and its suites hold the cases. It is a proposal: the language has one
owner, and the corpus decides first.

## The table

| Element | Risk | What it touches | When | Issue |
|---|---|---|---|---|
| Fixture 28: a grounding outside the cube, two forks for one artifact | Low | `crates/parser/tests/corpus/28-grounding-schema.md`, the corpus README row | First; the lead's verdict closes it | language, not an issue |
| SPEC §5.1 and §5.2: the contract and the kit's schema replace the fixed schema; the cube's keys re-headed, their bullets held | Low: a shrink | `SPEC.md` §5.1 QUERY bullet, §5.2 admission bullet and schema block; fixtures 07, 26 headings; `docs/concepts/aspects.md:19`, `docs/methods/plan.md:17` | On the verdict; half a day | language, not an issue |
| The engine: a QUERY body meets the contract, then every aspect flavored `x-kind: grounding`; such an aspect is never glossed, no witness names it, and re-declaring it is refused while a current QUERY slot would stop matching | Low | `crates/glossary/src/schemas.rs:8`, `crates/glossary/src/store.rs:1083`, the witness arm | After the SPEC diff; one day | drafted below |
| The KPI kit declares `kpi_grounding`, closed as §5.2 is today; the metrics skill names the keys as the kit's | Low | `crates/scripts/functions/kpi_kit.glossql`, `skills/glossql-metrics/SKILL.md`, `references/cube.md` | With the engine row; half a day | |

## What stands

- SPEC §5.2 fixes the schema, `additionalProperties: false`, six keys:
  `sql`, `stopped`, `assumptions`, `behavior`, `grain`, `axes`. The
  engine holds it verbatim in `schemas.rs` and applies it in `store.rs`'s
  QUERY arm; the FACT arm applies the `WITH` schema.
- The engine reads a grounding in three places: admission finds `sql`
  or `stopped` and builds or ends the view; the subject is the dataset;
  the view's name must not be a landed table's. Nothing in the engine
  reads `behavior`, `grain`, `axes` or `assumptions`; the cube reads the
  first three at `crates/session/src/cube.rs:1099`, `:2101`, `:2195`.
- No kit declares a QUERY aspect. Metrics are declared by the agent at
  run time with annotation-only `WITH` schemas, so the kit cannot type a
  grounding's keys per metric; it must say them once.
- `x-kind` is how a flavor rides an aspect, ruled twice (fixtures 16,
  19, 20), on QUERY aspects and on FACT aspects alike (`scenario`).
- The DuckDB extension validates the contract, then each `x-kind:
  grounding` aspect's schema; its KPI kit declares `kpi_grounding`
  closed, and its suites show a kit key refused, an undeclared key
  admitted where no kit speaks, the schema never glossed, no witness on
  it, and a schema refused that would strand a current slot.

## The reasoning

- **A key belongs to the reader that acts on it, and the kit that ships
  the reader declares it.** The engine keeps the one job it has, typing
  the body at admission, and takes the schema from the kit instead of
  the language. Nothing moves to read time; the cube's defaults for an
  absent key stay as §5.2 states them.
- **Why not the metric's `WITH` schema.** The agent declares the metric,
  so the kit cannot put its keys there, and a key an agent forgets is a
  key the engine never checks. One declaration per workspace is what a
  kit can promise.
- **Why not a fourth kind.** `AS GROUNDING` would fork the grammar and
  the store for a schema that is an aspect's `WITH` schema already; the
  flavor rides `x-kind`, as ruled.
- **What the openness admits.** A second kit declares grounding keys of
  its own: the harmonisation demo's service levels, in the demo repo,
  carry no stock or flow; a window kit carries a window. A host without
  the cube admits a grounding with no reader key. An agent without any
  kit glosses a metric under the contract alone, which is right: nobody
  has taught it a convention yet.
- **One workspace, one grounding vocabulary.** Two grounding schemas
  compose by conjunction, so a closed one and a second one conflict by
  construction; a kit layered on the KPI kit re-declares `kpi_grounding`
  with the union, before any metric is glossed under it.
- **The core ships no aspects; declaration stays the mechanism.** The
  aspect is the key and an undeclared aspect is refused. `behavior` is
  the KPI kit's word, on a column and on a grounding alike.
- **QUERY stays a kind.** It names a mechanism, SQL served as a view at
  the dataset's grain, not a vocabulary item.
- **The two rulings that leaned on the closed schema hold.** Fixture
  20's `reference_sql` died on the closed schema and on the measurement
  model; fixture 27's `plan` key died on the schema and on the slot and
  actor kind. Their concern, a key where it means nothing, is met: the
  KPI kit's schema stays closed, and a key enters it by the kit's own
  edit, never by widening the language.
- **The attest contract is the same pattern**, already drawn: `score`
  and `error` are mechanism, the colour words a convention the grammar
  fixes. It stays as it is; it is the precedent for the split.

## Fixture 28, the forks

Source: the harmonisation demo's service-level metric, a share of rows
that keep their required fields, sliced by system of record, no stock
and no flow; and the DuckDB extension's host, a grounding served as a
view with no cube reading it. Both forks parse; the fork is relational.

- **A. The fixed schema holds.** The service level carries no reader
  key and validates; a kit key of its own does not. The extension
  embeds the cube's keys it never reads.
- **B. The contract plus the kit's schema.** The engine validates the
  contract and whatever aspect is flavored `x-kind: grounding`; the KPI
  kit declares today's six keys, closed; a harmonisation kit declares
  its own; the extension's host declares none and runs on the contract.

## The SPEC diff, proposed

- §5.1, QUERY bullet: "Glosses validate against the standard grounding
  schema (§5.2), not the `WITH` schema" becomes "Glosses validate
  against the grounding contract and the kit's grounding schema (§5.2),
  not the `WITH` schema". The ontology-entry sentence holds.
- §5.2, admission bullet: "QUERY → the standard grounding schema"
  becomes "QUERY → the grounding contract, then every grounding schema".
- §5.2, the block: "The **standard grounding schema** is fixed, like the
  attest schema (§7.2)" becomes "The **grounding contract** is fixed,
  like the attest schema (§7.2): what the engine reads", with the JSON
  keeping `oneOf`, `sql`, `stopped` and `assumptions`, and one paragraph
  after it: "A kit declares the schema its readers expect of a grounding
  as an aspect flavored `x-kind: grounding`; the engine validates every
  QUERY gloss against each such schema after the contract. The aspect is
  never glossed, no witness names it, and re-declaring it is refused
  while a current QUERY slot would stop matching."
- §5.2, the three bullets: one heading line, "The KPI kit's grounding
  schema carries three keys and closes the body", then the bullets as
  they stand.
- Fixtures 07 and 26, the headings' "standard grounding schema" becomes
  "grounding contract"; the corpus README rows likewise.
- The one decision inside the diff: whether `assumptions` stays in the
  contract. In, by this plan: fixture 07 transcribed it from the source
  artifact's provenance, and disclosure is the language's concern, not
  a reader's convention. Out is one line less.

## The draft issue, the engine row

**A QUERY gloss meets the grounding contract, then every kit-declared
grounding schema.** What it buys: a kit declares the keys its readers
expect of a grounding, once, and the engine enforces them; a host or kit
without the cube admits a grounding with none. What stands:
`crates/glossary/src/schemas.rs:8` holds the fixed schema;
`crates/glossary/src/store.rs:1083` validates the QUERY arm against it;
fixtures 07 and 26; the store suite in
`crates/glossary/tests/suite/store.rs`. Done when: the constant is the
contract as §5.2 states it; the QUERY arm validates the contract and each
aspect whose schema carries `x-kind: grounding`; glossing such an aspect
and naming it in a witness are refused; re-declaring it validates every
current QUERY slot first; `kpi_kit.glossql` declares `kpi_grounding` with
today's six keys, closed; the store tests cover the four refusals and the
contract-only workspace; every `sql` block in SPEC.md parses; workspace
`cargo test` passes.
