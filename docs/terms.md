# Terms

The keywords of glossql, each beside the word the industry uses. The
normative definitions are in [SPEC.md](../SPEC.md). The words a kit
adds are in the kit's `SKILL.md` ([kits](kits.md)).

| Keyword | Industry word | What it is |
|---|---|---|
| subject | asset | What a value is attached to: a dataset, a table, a column, a relationship, a source. A view and its columns count as a table and its columns |
| `PROPERTY` | property | One property the glossary holds, with a JSON Schema, the kinds of asset it applies to, and when it is required |
| `ON COLUMN`, `ON TABLE`, … (grains) | scope | The kinds of asset a property applies to |
| `FACT` property | stated property | A value an agent or a person states |
| `MEASUREMENT` property | computed property | A value a function computes from the data |
| view definition, `GLOSS <name> ON <dataset>` | view | A named SQL query on a dataset with its reason, declared by its first definition. Its `kind` says what it is for: none (a kept query), `table`, `value`, `metric` or `rule` (the rows that break a rule) |
| `GLOSS` | annotation | One actor's value for one property on one asset, validated and stored with the lake snapshot |
| `DECLARE FUNCTION … RETURNS` | function | One SQL query run per asset; its result is stored with the inputs it read and the lake snapshot it read at |
| `DECLARE FUNCTION` without `RETURNS` (detector) | scoring function | A query that scores how far the values for a property agree |
| `DECLARE TEST` | test | Declared on a property: who may annotate it, which detector scores it, and a threshold |
| `TESTS()` | test results | One row per asset and test: `state`, `score`, whether it is current |
| `GLOSSARY()` | business glossary | One row per asset and property, with its value and state |
| `current` · `stale` · `possibly` · `conflict` · `missing` | valid · outdated · possibly outdated · conflict · missing value | The state of a row in `GLOSSARY()` |
| `state` in `TESTS()`, `test_state` in `GLOSSARY()` | test status | `green`, `yellow`, `orange`, `red` or `error` |
| the `inputs` table | provenance | What a result read: one row per asset and what was read of it |
| `DECLARE SOURCE` | connection | A directory, a URL prefix, or an attached database |
| `DECLARE RECIPE` | load query | The SQL that loads one table from a source |
| `IMPORT` | load | Runs a load query again; every load is a DuckLake snapshot |
| `PREVIEW` | preview, dry run | Runs a query at a source and loads nothing |
| kit | rule pack | A domain's properties, functions and tests, with a skill that teaches them |
| actor | user, service account | An agent or a person; set on the connection |
