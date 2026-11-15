# Kits

A kit is a script of glossql declarations for one domain (`kit.sql`),
with a `SKILL.md` beside it that teaches an agent its words.

| Kit | Adds |
|---|---|
| [`lineage/`](lineage/SKILL.md) | a dataset's lineage as one relation, `lineage(dataset)`; the read functions a function's body reads through, `annotations()`, `results()`, `definition()`; `PRAGMA glossql` loads it |
| [`kpi/`](kpi/SKILL.md) | the semantic model's words: `meaning`, `role`, `additivity`, `unit`, `dimension`, `entity`, `conventions`, `definitions`; two column profiles; the `value_entropy` detector |
| [`quality/`](quality/SKILL.md) | data quality checks: relationship candidates and checks, hierarchies, derivations, duplicate metric definitions, stock or flow; loaded after `kpi/` |
| [`spatial/`](spatial/SKILL.md) | geometry columns: `shape`, `crs`, `geometry_profile` |

```sql
FROM glossql_kit('kits/kpi/kit.sql');
FROM glossql_kit('kits/quality/kit.sql');
```

How a kit is loaded and how to write one: [docs/kits.md](../docs/kits.md).
