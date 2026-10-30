# The lineage plan

The evaluation behind one addition to SPEC.md §3 and §5.2: a landing
records what computed each of its columns, in one vocabulary both
engines spell the same, and `stale` reads re-derivation from that
record. The addition surfaced while building the DuckDB extension
beside this repo (`../gloss`), where a walk of the bound plan
resolves every output column of a recipe down to the base scans; the
extension runs this form, serves it as `gloss_lineage(sql)`, and its
suite holds the cases. It is a proposal: the language has one owner.

## The table

| Element | Risk | What it touches | When | Issue |
|---|---|---|---|---|
| One derivation vocabulary: a leaf is `scan.column:type`, an expression is its function's or operator's name over its children's texts, with no alias, no binding and nothing of the plan's shape; a column the walk cannot follow carries a digest of the recipe | Low: text | a lineage page under `docs/concepts/`; the extension's `src/gloss_lineage.cpp` is the reference reading | First; the lead's verdict closes it | language, not an issue |
| The engine's `derivations` becomes a walk: through Projection, Aggregate, Window, Union, Join, SubqueryAlias and the CTEs down to TableScan, the leaf typed from the scan's schema; a relational source keeps its empty map | Medium: DataFusion's plan shapes | `crates/import/src/lib.rs:399`; the `glossql.exprs` tag and `evolve_columns` in `crates/catalog/src/tables.rs` as they stand | After the verdict; two days | drafted below |
| SPEC §3 and §5.2 say it: a landing records each column's derivation, and a gloss reads `stale` once the derivation behind its column moved, a source sending a column under another type included | Low | `SPEC.md` §3 re-land paragraph, §5.2 `snapshot_id` bullet | With the engine row | language, not an issue |
| The read: `lineage(sql)`, the derivation of each column of a query as the session would plan it, in both engines; the landing's lineage readable on the import record | Low | a session read; the extension has it | After the engine row; one day | |
| A measurement's lineage: the same walk over its body names the columns it read, finer than the tables `reads` names | Medium | `crates/session/src/reads.rs`; the extension's `ReadsOf` | Later | |

## What stands

- SPEC §3: a changed recipe re-lands, and a gloss on a column the
  re-land re-derived, retyped or dropped reads `stale`. §5.2: every
  gloss carries `snapshot_id`, and staleness is a join against the
  catalog's column history, never a guess.
- The engine keeps a derivation per column already. `derivations()` at
  `crates/import/src/lib.rs:399` takes the root projection's or
  aggregate's expressions with column references unqualified, and
  nothing for a union or a join without a projection over it. The
  landing writes the map as the table tag `glossql.exprs`
  (`crates/catalog/src/tables.rs:319`); `evolve_columns`
  (`tables.rs:1135`) begins a new column version when the type or the
  derivation changed, which `rules::state` reads as `stale`. A
  relational source lands an empty map.
- The extension walks the bound plan before any optimizer runs, bottom
  up, a text per column binding: a scan's column is `scan.column:type`,
  a projection, aggregate, window or unnest writes its expressions'
  texts, a set operation joins its sides' texts, a CTE is walked
  definition first, filters, orders, limits and joins pass bindings
  through. Each landing lands the map on its imports row; its stale
  rule compares the landing that stood at the gloss's snapshot with
  the newest, per column, a table being stale once any key moved. A
  column added or a filter changed ages nothing. Its suite holds a
  recipe that re-derives one column, and a source under `SELECT *`
  whose column arrives as a double where it was an integer while the
  landed column keeps its type.
- The two differ in depth and in the leaf. The engine's text stops at
  the root's expressions, bare names, no types; the extension's resolves
  through joins, unions and CTEs to the source's columns and their
  types. A source that sends a column under another type moves the
  extension's leaf and nothing in the engine's text: `CAST(amount AS
  Float64)` reads the same over an integer and over a double.

## The reasoning

- **The leaf carries the source's type** because that is where drift
  the catalog cannot see lives: under a cast, or under a recipe that
  lands the source's type as it comes, the landed column keeps its type
  while the source changed its mind. The column history says current;
  the lineage says moved.
- **Per landing, not per declaration.** A declaration fixes the SQL; a
  landing is the SQL meeting the source as it stands. Recording at each
  landing is what makes the landings' diff a drift record of what a
  source sends, readable with plain SQL.
- **Through joins and unions**, because a recipe over two tables is the
  common case and a column from a join is derived from something; a
  text that stops at the root cannot say what, so the root's bare name
  is equal under any change below it.
- **Text, no structure.** The rule is two texts equal or not. A
  structured lineage, a graph a reader walks, is a later reader's
  business; the text is what both engines can produce today and
  compare tomorrow.
- **Where it lives is the record's business.** The engine tags the
  table version; the extension writes the imports row. The read is what
  is shared, and the vocabulary; `glossql.exprs` keeps its place.
- **One vocabulary across pins, reads and lineage.** A pin names
  `dataset.table:version`, reads name `dataset.table`, a lineage leaf
  names `dataset.table.column:type`. A measurement's lineage is then
  the same walk over its body, which is the last row of the table.

## The SPEC diff, proposed

- §3, the re-land paragraph: "a gloss on a column the re-land
  re-derived, retyped or dropped reads as `stale`" holds, with one
  sentence after it: "Every landing records what computed each column,
  resolved to the source's columns and their types; a column is
  re-derived once that record moved, which a changed recipe does and so
  does a source that sends a column under another type."
- §5.2, the `snapshot_id` bullet: "a column re-derived, retyped or
  dropped" holds; "Provenance and staleness are a join against the
  catalog's column history, never a guess" becomes "Provenance and
  staleness are a join against the catalog's column history and the
  landings' lineage, never a guess."

## The next row, drafted

**The engine's derivations become a walk.** `derivations()` in
`crates/import/src/lib.rs` resolves each output column of the recipe's
`LogicalPlan` to the vocabulary above: a `TableScan` column is
`scan.column:type` from the scan's schema, with the scan named as the
pin names it; a `Projection`, `Aggregate` or `Window` writes its
expressions' texts with column references replaced by what they
resolve to; `Union` joins its inputs' texts; `Join`, `Filter`, `Sort`,
`Limit`, `Distinct` and `SubqueryAlias` pass columns through; a CTE
resolves to its definition. A node the walk does not know marks the
column with a digest of the recipe. The map lands where it lands today,
the `glossql.exprs` tag, so `evolve_columns` and `stale` need no change.
Done when `cargo test` holds a recipe that re-derives one column, a
recipe that adds one, a filter that changes, and a CSV source whose
column arrives as a double where it was an integer under a cast, each
read as the extension's suite reads them.
