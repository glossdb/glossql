# The data harness

A harness surrounds an agent's work and tells it the state of that
work. For code, that is a type system, a compiler, linters, tests, CI
and review. glossql is a language for writing the same for data.

## Why data needs a harness

- **The meaning is not in the rows.** A column called `amount` does
  not say whether it is net of tax, a balance or a movement, or which
  currency it is in. An agent that guesses still gets a result.
- **Errors are quiet.** A join that repeats rows, or a key without its
  parent, changes a total. The query runs and returns a number.
- **The data changes.** A definition that was right for last month's
  load may be wrong for this one, and the database does not say so.
- **Work does not carry over.** Without a stored description, the next
  session or the next person decides again, and may decide
  differently.

A harness answers each of these. You declare what must be known. What
an agent or a person states is validated and stored. The statements
are tested on the data. Every result is tied to the version of the
data it read. A person can review and decide.

## The parts, side by side

| Coding harness | Data harness | In glossql |
|---|---|---|
| type system | what must be known about each dataset, table, column and relationship | `DECLARE PROPERTY` |
| type annotations, docstrings | annotations: what one row is, a column's unit, a measure's additivity | `GLOSS` |
| compiler error | validation on write against the property's JSON Schema | |
| linter | quality checks: keys, joins, hierarchies | the `quality` kit |
| unit test | measurements and tests on the data | `DECLARE FUNCTION`, `DECLARE TEST` |
| test coverage | coverage of the required annotations | `GLOSSARY()`, state `missing` |
| CI on every commit | results stored with the inputs they read; out of date when one changes | state `stale` |
| git history | snapshots of the data and the catalog together | DuckLake |
| code review | a person's annotation beside the agent's; a test scores where they differ | `BY (AGENT, HUMAN)` |
| rule packs | kits: a domain's properties, measurements and tests, with a skill | `glossql_kit()` |

## How the parts work together

Declare a property, annotate assets with it, measure the data, test
the values against each other, and read the state of each. The usage
pages cover each step: [describing data](usage/describing-data.md),
[measurements](usage/measurements.md), [tests](usage/tests.md),
[reading](usage/reading.md).

[glossdb](https://github.com/glossdb/glossdb) is the server that runs
this harness for agents and people.
