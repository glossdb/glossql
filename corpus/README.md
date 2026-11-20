# The glossql corpus

The corpus is the language's acceptance tests. Each file shows how one
real artifact of a data team is written in glossql: a metric, a
convention, a validation, a load, a review question.

`test/sql/glossql_corpus.test` checks every fenced block by its tag, and
`make test` runs it:

- ` ```glossql ` must parse under `grammar.ebnf`.
- ` ```glossql-gap ` must fail to parse. It shows a form the language
  does not have.

| File | Shows |
|---|---|
| [01](01-concept-revenue.md) | a glossary entry for a measure, as its view definition |
| [02](02-convention-sign-natural-balance.md) | an accounting convention on a dataset |
| [03](03-metric-dso.md) | a metric: its definition in SQL, with its meaning |
| [04](04-validation-trial-balance.md) | a validation: expectation, check and test |
| [05](05-cycle-accounts-receivable.md) | the stages of a business process on a status column |
| [06](06-test-additivity.md) | a test that compares annotations of a column's additivity |
| [07](07-view-definition.md) | a view's definition: SQL and assumptions |
| [08](08-review-decisions.md) | a person's corrections to the glossary |
| [09](09-glossary-reads.md) | reads of the glossary and the test results |
| [10](10-catalog-annotations.md) | sources, annotations, relationships, dimensions and statistics |
| [11](11-flow-add-source.md) | the flow from a new source to tested annotations |
| [12](12-flow-relationships-and-dimensions.md) | the flow from loaded tables to relationships, dimensions and reconciliation |
| [13](13-typing-patterns.md) | type patterns and null markers |
| [14](14-composite-relationships.md) | relationships on composite keys, with their referential integrity |
| [15](15-data-app-reads.md) | the reads behind a data app |
| [16](16-flow-performance-framework.md) | a metric scorecard: metrics, formulas, validations |
| [17](17-relational-source.md) | a load from a relational database, in full and by key |
| [18](18-app-authoring.md) | data app authoring, which the language does not express |
| [21](21-source-conventions.md) | conventions annotated on a source and read by every dataset |
| [22](22-review-question.md) | a review question and the person's answer |
| [23](23-conditional-relevance.md) | an annotation required only where it applies |
| [24](24-functions-in-a-table.md) | a function with its body in the statement |
| [25](25-mixed-case-names.md) | mixed-case table and column names |
| [28](28-view-definition-keys.md) | a view's additivity, row identity and dimensions |
