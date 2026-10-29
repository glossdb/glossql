# The sources plan

What comes in: workbooks, documents, search, and the ranking
experiments. The shared frame, the terms, the gates and the
decisions are in `kernel.md`. This plan touches the import crate and
one scalar function. Its seam is the general service call, one
table in and one table out, which stands; the scalar shape lands
here with `embed(text)`, its first user. It runs under the phase 1
gate.

## The table

| Element | Difficulty | What it touches | When | Issue |
|---|---|---|---|---|
| Spreadsheets as a source, in process; the budget and the driver formulas land as rows | Easy | import crate, SPEC §3 | First | |
| Documents as a source, a processor service | Easy | import crate, SPEC §3, one service | After workbooks | |
| Search: embed as a remote scalar function, distance in plain SQL | Easy | scripts, one service; the async scalar seam exists at our pin | The table shape stands; the scalar shape lands here | |
| Ranking and CLM experiments | Easy, outside the tree | nothing here; glossval | Beside it; blocks nothing | |
| Vector index in parquet | Medium | catalog, an optimizer rule | When the full scan is too slow | |

## What stands

**Sources today.**

- The kinds: `crates/import/src/lib.rs:93` (`SourceKind`: parquet,
  csv, json, relational). The spec: SPEC.md §3, `type: relational_db
  | parquet | csv | json`; grammar.ebnf line 67 lists the same keys
  in a comment, so a new kind is a spec change, not a production
  change.
- A recipe opens as a stream: `open_recipe` (292). The reader
  functions a recipe may name: `reader_functions` (762), registered
  by `registry` (736). A source's files: `list_source` (1027).
- Relational: `crates/import/src/adbc.rs`, the driver a shared library
  the source's `driver` setting names.

**Search today.**

- `array_distance` is registered at our pin
  (`datafusion-functions-nested-55.1.0/src/lib.rs:195`; the
  `nested_expressions` feature is on in the workspace `Cargo.toml`).
- The async scalar function seam:
  `datafusion-expr-55.1.0/src/async_udf.rs` (`AsyncScalarUDFImpl`).
- No remote scalar function exists. No vector column has been landed.
- The quality rule that bounds search: identity is an explicit key,
  never text matching (`docs/quality/README.md`). Search ranks rows
  for a reader; a similarity is never a join key, an identity or what
  closes a question.

## Issues, drafted

In the repo's form: what it buys, what stands, done-when. Facts only.
When one is opened, its number goes into the table and the draft
leaves this file.

### Workbooks as a source

- **What it buys.** A company's workbook lands as tables, and its
  formulas, defined names, macro modules and queries land as rows.
  An agent reads a formula and writes the metric's grounding; the
  basis names workbook, sheet and cell. A migration is checked
  against the workbook's own stored values. A budget lands as the
  plan's table, and the driver formulas land as the definitions the
  propagation runs through. No calculation engine.
- **What stands.** `SourceKind` (`crates/import/src/lib.rs:93`) and
  SPEC.md §3 name four kinds. A recipe lands rows typed by the
  recipe; the rows are the interface. A reader crate that reads
  values, formulas, macro source, defined names and tables from
  xlsx, xlsm, xlsb, xls and ods exists (calamine). Pivot tables and
  Power Query are read by none of the candidates.
- **Done when.** The corpus holds one real workbook, and SPEC.md §3
  names the kind. A fixture workbook lands as tables and as
  definition rows keyed by path, sheet, and cell or name. A
  grounding's basis names a cell, and the grounding's result equals
  the workbook's stored value on the fixture. The suite holds it.

### Documents as a source, through a processor service

- **What it buys.** A handbook, a standard or a schema description
  lands as passages. An agent finds the passage it needs, writes a
  convention or a definition, and cites the passage by key. The
  critical data stays in the systems of record; documents give
  context.
- **What stands.** Same seams as the workbook: `SourceKind`, SPEC.md
  §3, the recipe as the landing. A processor that reads PDF, Word
  and slides into structure, tables and reading order exists as an
  open server (docling) and runs per deployment, since text is
  customer content. Nothing in the tree calls a service from a
  recipe.
- **Done when.** The corpus holds one real handbook, and SPEC.md §3
  names the kind. A fixture document lands as passage rows: document,
  reference, ordinal, headings, kind, text, page; the key is document
  and reference. The same document processed twice, the second time
  with one paragraph added, keeps the untouched passages' references,
  or the issue records that a key of our own is needed. A gloss cites
  a passage by key and the suite holds it.

### Search: embed as a remote scalar function

- **What it buys.** An agent finds the passage it needs by meaning,
  not by string, over the passages a document landed.
- **What stands.** `array_distance` is registered
  (`datafusion-functions-nested-55.1.0/src/lib.rs:195`). The async
  scalar function seam exists (`datafusion-expr-55.1.0/src/async_udf.rs`).
  No remote scalar function exists in `crates/scripts`. The service
  call's table shape is the futures plan's first row; the scalar
  shape is this issue's. The
  quality rule: identity never rides similarity
  (`docs/quality/README.md`).
- **Done when.** `embed(text)` is a scalar function the recipe names,
  answered by an embedder service the deployment names, refused by
  name when none is. The recipe lands the vector at unit length and
  the column records the model's name and revision. A query ordered
  by `array_distance` over a full scan returns the fixture passage in
  the top rows. 10,000 passages land with the embedder stopped
  half-way, and no half table results. The time per passage is
  recorded on the issue.

## For the other repos

- **glossval**: the ranking experiment. Question 1, steps: where the
  door offered several, does a picker rank first the one the
  high-scoring sessions took, against today's order. Question 2,
  candidates: on the relationship truth, does it rank the true edges
  above the coincidences, against today's ranking. Untuned first. If
  it does not clearly beat the baselines, it gets no place. It blocks
  nothing in either plan. The ranker to try first is the contrastive
  language model (https://github.com/Contrastive-LM/CLM): a frozen
  encoder with trained heads that scores candidates by dot product,
  one pass per new text; the training signal is glossval's record of
  every door call of every scored session. If it wins, it becomes a
  service point, `GLOSSQL_RANK_URL`, refused by name when unset, on
  the kernel service's GPU.
