# Landing — read before the first PROBE or DECLARE RECIPE

## Agree the topic before anything lands

A dataset has a topic — on-time delivery, capacity, cost control —
and the topic makes every later choice decidable: which tables to
land, which metrics to propose, which questions matter. Propose one
in prose from what you can see, let the user shape it, then declare
it:

```glossql
DECLARE DATASET ops SET (purpose: 'service delivery — what gets done, how fast, and where it stalls');
```

**Then propose the metric cohort** — what the topic implies, including
the heavy ones (an end-to-end cycle time, real utilization), not just
what looks easy to compute. The user prunes and extends in prose. That
conversation is where scope questions surface while they are cheap:
"cycle time from which timestamp?" costs one sentence now and a wrong
dashboard later. Aim high deliberately — **a cohort metric the data
cannot ground is a finding, not a failure.** Name what is missing and
which tables would close it; surfacing that gap is the product
working.

This is conversation, not a form. **Prose shapes the work; forms rule
the record.** Anything deciding what the work *is* — the topic, the
cohort, whether to widen the import — is chat: present the facts,
propose, interpret the answer. The question round carries only
standing assumptions to confirm or correct; it cannot replace
conversation because nothing is standing yet.

## Land what the topic needs — this is not ETL

Probe and recipe are the filter. A dataset is a curated working set
for its topic, never a mirror of the export: land the tables the
cohort needs, take only the columns the recipe's SELECT list earns,
filter wide tables in the recipe's WHERE. Leaving something out costs
one later `DECLARE RECIPE`; landing everything costs attention on
every read after — more slots to gloss, more owed claims, more noise,
until the deep scope questions drown in a hundred-column long tail.
Width also costs compute: the structure searches scale with a table's
column pairs, so a wide table landed whole is what makes them slow.

**Read the source's conventions before probing.** Source-grain slots
serve in every dataset, so what an earlier onboarding learned about
this system — placeholder dates, format warts, key spellings — is
already readable. The source is the subject, bound to a dataset or
not:

```glossql
SELECT value FROM GLOSSARY(erp_export) WHERE aspect = 'conventions';
```

What you learn goes back the same way:

```glossql
GLOSS conventions ON erp_export AS $${
  "placeholder_date": "1900-01-01 stands for unset",
  "timestamp_format": "%b %e %Y %I:%M%p, month names mixed-language"
}$$;
```

Only what the *next* export from that system will also carry belongs
at source grain; dataset-local evidence stays in dataset glosses.

**List the files before naming one.** A recipe names its files, and
the listing is a read under the source's location, subdirectories
included — no other door is needed:

```glossql
SELECT path, size, modified FROM source_files('erp_export') ORDER BY path;
```

**Rehearse the schema with `LIMIT 0`, per file, before authoring any
recipe.** A zero-row probe still carries every `(name, type)`. Row
probes cannot replace it — probe rows omit null fields, so a column
that is null in your sample is invisible there, and a missed join key
carries a missed relationship with it.

```glossql
PROBE erp_export AS $$SELECT order_id,
       try_cast(amount AS DOUBLE) AS amount,
       try_to_date(order_date, '%d.%m.%Y') AS order_date
FROM read_parquet('orders/*.parquet') LIMIT 0$$;
```

**Name the columns — never `SELECT *`.** A star recipe survives a
schema change in the source and fails later, downstream, where
nothing points back at the source; a named SELECT list fails at the
re-import, where the drift is and where you can fix it.

**Typing is authored.** The recipe carries the casts and the column
choices; there is no typing machinery behind it. A failed cast lands
NULL — a kept row with a NULL cell, not a dropped row.

```glossql
DECLARE RECIPE orders ON ops FROM erp_export AS $$
  SELECT order_id,
         try_cast(amount AS DOUBLE) AS amount,
         try_to_date(order_date, '%d.%m.%Y') AS order_date
  FROM read_parquet('orders/*.parquet')$$;
```

**One date column may carry several conventions.** `try_to_date` and
`try_to_timestamp` take as many formats as you name and use the first
that parses, so a mixed column is one call, not a coalesce ladder
over three copies of the value:

```glossql
PROBE erp_export AS $$SELECT
  try_to_date(paid, '%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%d-%b-%y') AS paid
FROM read_csv('payments.csv') LIMIT 5$$;
```

Order is your claim about the source and it decides the ambiguous
rows: `02/03/2025` is March 2nd under `%d/%m/%Y` and February 3rd under
`%m/%d/%Y`, and whichever you name first wins. Name the unambiguous
formats first. Where two readings both parse and the count matters,
measure it (`substr` the parts and count which are impossible under
each) and disclose the residual with a key — say how many rows no
evidence could decide, rather than picking quietly.

The outcome carries the **cast account** at the decision moment: for
every `try_*`, how many cells the cast nulled and the top such values.
Those tokens came from the data, not a list — judge them. A repeated
`\N` or `n/a` is a null marker: amend the recipe (`NULLIF` before the
cast) and re-declare, which supersedes and re-lands. A scattered long
tail may be genuinely bad data worth a `meaning` gloss. A re-landing
keeps the glosses (their snapshot ids show their age) — re-run the
measurements for columns the new recipe changed.

For a relational source, probe and recipe SQL run **at the source** in
its own dialect, and the wire decides what can land. SQLite has no
date type: `CAST(date(x) AS DATE)` silently lands `2010` for
`'2010-12-27'` because DATE takes NUMERIC affinity — measured, not
theorized. Land it as text and cast at read time, and put that gap in
the column's `meaning` so nobody rediscovers it. Read `DESCRIBE
<table>` the moment a table lands: a numeric that landed as text shows
up there instead of three reads later.

## Dirt is measured, never assumed

Nobody hands you a list of the data's defects. The user does not know
them either. You find them, and every finding follows one rule:
measure first, decide second, write the decision where the next
reader looks.

- **Measure.** Which subject, what share of rows, since when. The cast
  account at landing; `profile()` for nulls, distincts and top values;
  `temporal()` for gaps; `relationship_coherence()` for orphans and
  child-before-parent dates; `detect_derivations()` for an identity
  that breaks. A finding is a number, never an impression.
- **Typing goes in the recipe; nothing else does.** A null marker, a
  cast, a date format is typing. A row you exclude, a value you
  recode, a sentinel you turn to NULL, a duplicate you drop is a
  decision, and a decision inside a recipe hides from every read
  after it. Put it in one derived relation the metrics compose from
  (`x-kind: "relation"`), and disclose it on each metric as a keyed
  `scope` or `convention` assumption below 1.0, so the round asks it.
- **Write what you found where the next reader looks.** What the next
  export will also carry goes to `conventions` on the source. What is
  true of this landing goes to `meaning` on the column or table. What
  must stay watched becomes a validation, the expectation authored at
  the rate you measured today: `ATTEST` is green now and turns red
  when the source drifts (`references/validate.md`). The validation is
  what the user sees; a finding that never becomes one is known to
  you alone.

Whether a metric is served on that data, or stopped, is decided at
the grounding: `references/ground.md`.
