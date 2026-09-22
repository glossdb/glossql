# The workspace

A workspace is one lake and one record. It holds datasets — the
working units an analysis lives in — and the glossary vocabulary
those datasets share. An app binds to one dataset; the workspace
itself holds as many as its sources warrant.

## Datasets and the lake

A dataset is a schema in the workspace's catalog. Its tables land
through recipes as parquet files under the warehouse, and every
landing, replace and import is one commit that gives the table a new
version; the record's `imports` relation keeps one row per landing.

Three names are not dataset names: `glossql` is the record's own
namespace, and `mcp` and `assets` are paths the server answers beside
the datasets' pages. `DECLARE DATASET` refuses each by name.

The record lives in the same database as the catalog: every relation
the language declares — the glossary, aspects, functions, witnesses,
sources, datasets, recipes, relationships, measurements, imports — is
a table of rows there. Writes are appends, one row per statement; no
machinery deletes knowledge.

**Supersession is a read, not an update.** The current value of a slot
is the latest row per (subject, aspect, actor kind). Re-speaking a
gloss appends a row that wins the read; the old row remains as
history.

Every gloss row carries the `snapshot_id` of its subject's table at
write time, so provenance and staleness are a join against the table's
snapshot history, never a guess: a gloss written before the table
moved on is served *and marked* `stale`.

## Sessions

The actor — an agent id or a human id — rides the connection; there is
no BY clause anywhere. A session belongs to one actor and one dataset:

```glossql
DECLARE DATASET fin SET (purpose: 'working-capital analysis over ERP and CRM exports');
USE fin;
```

`USE` sets the resolution context for the statements after it in the
call; a dataset's own doors (`/<dataset>/mcp`, `/<dataset>/query`,
`/<dataset>/app`) open the call on the dataset, so a caller there needs
no `USE`. Unprefixed `table.column` paths resolve against the bound
dataset;
the full `dataset.table.column` spelling is always allowed. A head
that names both a dataset and a landed table of the `USE`'d dataset is
the table: the nearer scope wins, and the other dataset's names are
reached under its own `USE`. A table named like its own dataset is
reached by its columns only; the bare name is the dataset. An app
part's subject is the app's, `<app>.<part>` under the `USE`'d
dataset, so an app named like its dataset keeps its parts. Two actors
on the same dataset hold two sessions; one actor on two datasets holds
two sessions.

## What crosses dataset lines

Almost nothing, deliberately. Declared aspects are workspace
vocabulary (a function is scoped `FOR` a dataset or `GLOBAL`); glosses
are dataset-scoped —
with one exception: an aspect declared `ON SOURCE` attaches to a
declared source, and sources live outside datasets, so source-grain
slots read, supersede, and disclose across every dataset in the
workspace. What one onboarding learns about a source system, the next
dataset reads before its first probe (see [`imports.md`](imports.md)).
