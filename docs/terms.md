# Terms

The words glossql uses, beside the industry's. Each is defined once
here. The normative definitions are in [SPEC.md](../SPEC.md). How a
server builds a harness for agents on them: [how the harness
works](https://github.com/glossdb/glossdb/blob/main/docs/overview/how-it-works.md).

## The language

| Term | Industry word | What it is |
|---|---|---|
| asset (subject) | asset | What a value attaches to: a dataset, a table, a column, a relationship, a source, a view and its columns |
| aspect | property of the harness schema | One property the harness asks for, with a JSON Schema, the assets it applies to, and when it is required |
| an aspect's grain | scope | What the aspect applies to: `ON COLUMN`, `ON TABLE`, the dataset |
| `FACT` aspect | asserted property | A value an actor states |
| `MEASUREMENT` aspect | computed property | A value a function computes from the data |
| `VIEW` aspect | view | SQL served as a view under the aspect's name: a metric, a scalar, or a derived relation |
| gloss | annotation | One actor's value for one aspect on one asset, validated and versioned |
| function | measurement | One SQL query that measures the data; its result is stored with its provenance |
| detector | scoring function | A function that scores how far the values for an aspect agree |
| test (`DECLARE TEST`) | data test | Declared on an aspect: who may annotate it and which detector scores agreement, with a threshold |
| `TESTS()` | test results | One row per asset and aspect: band, score, whether it is current |
| `GLOSSARY()` | the glossary | What is known: one row per asset and aspect, with its state |
| `current` · `stale` · `contested` · `unassessed` | valid · outdated · conflict · missing | The state of an asset's aspect in the glossary |
| band | severity, confidence | green, yellow, orange, red, or error |
| `pin`, `reads` | provenance | The versions of what a measurement read |
| source | connection | A directory, a URL prefix, or an attached database |
| `RECIPE` | load query | The SQL that loads a table from a source |
| `IMPORT` | load | Runs a load query; every load is a DuckLake snapshot with its row count and column lineage |
| `PREVIEW` | dry run, preview | Runs a query on a source without loading |
| kit | rule pack | A domain's aspects, functions and tests, with a skill that teaches them |
| actor | user, service account | An agent, a person, or a person working with an agent |

## The semantic layer (the `kpi` kit)

| Term | Industry word | What it is |
|---|---|---|
| `meaning` | description | What a table, column or relationship means, and the business term it carries |
| `role` | column role | key, measure, dimension, timestamp, attribute |
| `unit` | unit of measure | A measure's unit or currency |
| `behavior`: flow · stock · ratio | additivity: additive · semi-additive · non-additive | Whether a measure may be summed, and over which axes |
| `dimension`: primary · supporting | dimension | Whether a dimension column is one to slice a metric by |
| `entity` | grain and key | What one row of a table is, and the columns that identify it |
| `definitions` | business glossary | Each metric's meaning, unit, owner, source and formula, kept per dataset |
| `conventions` | source conventions | A source's fiscal calendar, sign conventions, what its nulls mean |
| time grains | time grains | day, week, month, quarter: the grains a metric is served at |
| metric definition | metric | A view whose columns carry role, additivity and time |

## Beside the standards

| Standard | What it names | glossql |
|---|---|---|
| dbt data tests, Great Expectations, Soda checks, DataHub assertions | a check on the data with a pass or a fail | `DECLARE TEST` and `TESTS()`; `green` · `red` · `unassessed` · `stale` read as pass · fail · skipped · stale |
| Apache Ossie (incubating), formerly Open Semantic Interchange | the semantic model exchanged between tools: datasets, fields, metrics, relationships, `ai_context` | the `kpi` kit's words on a view's columns, and `DECLARE RELATIONSHIP`; a server exports it |
| OpenLineage | which columns a run read and wrote | `glossql_views_lineage()`, `glossql_views_scans()`, and the lineage on every load |
| data contract | a producer's promise about a table: schema, keys, quality rules | a kit's required aspects and the tests on a source's tables |
