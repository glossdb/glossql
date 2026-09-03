# 25 · Mixed-case names (avito run) — RULED: an unquoted name folds to lowercase

Source: our own sealed runs on the Avito export, whose files and
columns carry capitals (`AdsInfo.tsv`, `SearchStream.tsv`, `AdID`).
The agent landed the tables under the file names and read them back
unquoted; the statement had kept `AdsInfo` and the engine looked for
`adsinfo`. Ruled: glossql names fold as the host's do (SPEC.md §1) —
an unquoted name is lowercased at the declaration and at the read, a
double-quoted one keeps its case.

## 1. Landed under the file's name, read back unquoted

```glossql
USE avito;
DECLARE SOURCE export SET (type: csv, location: 'avito');
DECLARE RECIPE AdsInfo ON avito FROM export AS $$
  SELECT "AdID" AS ad_id, "CategoryID" AS category_id, "Price" AS price
  FROM read_csv('AdsInfo.tsv')$$;
SELECT count(*) FROM adsinfo;
SELECT count(*) FROM AdsInfo;
```

Both reads reach the same table: `AdsInfo` folds to `adsinfo` in the
declaration and in the read. The recipe aliases the export's columns
lowercase, so no read of this table needs quotes.

## 2. A quoted name keeps its case

```glossql
USE avito;
DECLARE RECIPE "SearchStream" ON avito FROM export AS $$
  SELECT "SearchID" AS search_id, "AdID" AS ad_id FROM read_csv('SearchStream.tsv')$$;
SELECT count(*) FROM "SearchStream";
GLOSS entity ON "SearchStream" AS $${"value": "one ad shown in one search"}$$;
```

Unquoted, `SearchStream` folds to `searchstream` and misses. The
quotes are the whole difference — in the declaration, the read and
the subject alike.
