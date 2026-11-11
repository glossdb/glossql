# Terms

The keywords of glossql, each beside the word the industry uses. The
normative definitions are in [SPEC.md](../SPEC.md). The words a kit
adds are in the kit's `SKILL.md` ([kits](kits.md)).

| Keyword | Industry word | What it is |
|---|---|---|
| subject | asset | What a value is attached to: a dataset, a table, a column, a relationship, a source. A view and its columns count as a table and its columns |
| `ASPECT` | property | One property the glossary holds, with a JSON Schema, the kinds of asset it applies to, and when it is required |
| `ON COLUMN`, `ON TABLE`, … (grains) | scope | The kinds of asset an aspect applies to |
| `FACT` aspect | stated property | A value an agent or a person states |
| `MEASUREMENT` aspect | computed property | A value a function computes from the data |
| `VIEW` aspect | view | SQL served as a view under the aspect's name: a metric, a single value or a table |
| `GLOSS` | annotation | One actor's value for one aspect on one asset, validated and stored with the lake snapshot |
| `DECLARE FUNCTION … RETURNS` | measurement | One SQL query over the data; its result is stored with the data version it read |
| `DECLARE FUNCTION` without `RETURNS` (detector) | scoring function | A query that scores how far the values for an aspect agree |
| `DECLARE TEST` | test | Declared on an aspect: who may annotate it, which detector scores it, and a threshold |
| `TESTS()` | test results | One row per asset and test: `state`, `score`, whether it is current |
| `GLOSSARY()` | business glossary | One row per asset and aspect, with its value and state |
| `current` · `stale` · `conflict` · `missing` | valid · outdated · conflict · missing value | The state of a row in `GLOSSARY()` |
| `state` in `TESTS()`, `test_state` in `GLOSSARY()` | test status | `green`, `yellow`, `orange`, `red` or `error` |
| `data_version`, `reads` | provenance | The tables a measurement read, and the version of each |
| `DECLARE SOURCE` | connection | A directory, a URL prefix, or an attached database |
| `DECLARE RECIPE` | load query | The SQL that loads one table from a source |
| `IMPORT` | load | Runs a load query again; every load is a DuckLake snapshot |
| `PREVIEW` | preview, dry run | Runs a query at a source and loads nothing |
| kit | rule pack | A domain's aspects, functions and tests, with a skill that teaches them |
| actor | user, service account | An agent or a person; set on the connection |
