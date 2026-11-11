# The data harness

A harness is what surrounds an agent's work and tells it the state of
that work. For code, that is a type system, a compiler, linters, tests,
CI and review. glossql is the language for writing the same for data.

## Why data needs a harness

- **The meaning is not in the rows.** A column called `amount` does
  not say whether it is net of tax, a balance or a movement, or which
  currency it is in. An agent that guesses still gets a result.
- **Errors are quiet.** A join that repeats rows, or a key without its
  parent, changes a total. The query runs and returns a number.
- **The data changes.** A definition that held for last month's load
  may not hold for this one, and the database does not say so.
- **Work does not carry over.** Without a stored description, the next
  session or the next person decides again, and may decide
  differently.

A harness answers each of these. What must be known is declared. What
an agent or a person states is validated and stored. The statements
are tested on the data. Every result is tied to the version of the
data it read. A person can review and decide.

## The parts, side by side

| Coding harness | Data harness | In glossql |
|---|---|---|
| type system | the harness schema: what must be known about each dataset, table, column, relationship and metric | `DECLARE ASPECT` |
| type annotations, docstrings | annotations: what one row is, a column's unit, a measure's additivity | `GLOSS` |
| compiler error | validation on write against the property's JSON Schema | |
| linter | quality screens: keys, joins, hierarchies, stock or flow | the `quality` kit |
| unit test | measurements and tests on the data | `DECLARE FUNCTION`, `DECLARE TEST` |
| test coverage | coverage of the required annotations | `GLOSSARY()`, state `missing` |
| CI on every commit | results tied to the data version they read; outdated when it changes | state `stale` |
| git history | snapshots of the data and the catalog together | DuckLake |
| code review | a person's annotation beside the agent's; a test scores where they differ | `BY (AGENT, HUMAN)` |
| rule packs | kits: a domain's schema, measurements and tests, with a skill | `glossql_kit()` |

## How the parts work together

**Declare.** A property of the harness schema (`DECLARE ASPECT`) has a
JSON Schema for its values, the assets it applies to, and a condition
for when it is required: a unit on every measure column, a grain on
every table. See [describing data](usage/describing-data.md).

**State.** An annotation (`GLOSS`) is one actor's value for one
property on one asset. It is validated when it is written and stored
in the glossary with its actor and the data snapshot it describes.

**Measure.** A function is one SQL query over the data: a profile, a
relationship search, a derivation check. Its result is stored with
the versions of everything it read, and it runs again when those
change. See [measurements](usage/measurements.md).

**Test.** A test (`DECLARE TEST`) collects every value for a property
on an asset: the measurements, the agent's annotation, a person's
annotation. A detector scores how far they agree. See
[tests](usage/tests.md).

**Read.** `GLOSSARY()` returns what is known per asset and property
with its state: `current`, `stale`, `conflict` or `missing`.
`TESTS()` returns the test results. See [reading](usage/reading.md).

**Version.** The catalog is stored in the DuckLake beside the data.
Every load is a snapshot, so a description belongs to a version of
the data. See [lineage](usage/lineage.md).

## In a server

[glossdb](https://github.com/glossdb/glossdb) runs this harness for
agents and people. It adds the tests on every metric, the next steps
after each call, a review queue and a console. Agents use it to build
a company's data intelligence layer: what the data means, how its
sources map, whether it can be trusted, and who decides.
