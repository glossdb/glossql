# Mixed-case names

Exports often carry capitals in file and column names. The examples
show the rule: an unquoted name is lowercased where it is declared and
where it is read, and a double-quoted name keeps its case.

## A table declared unquoted

Both reads reach the same table.

```glossql
USE avito;
DECLARE SOURCE export SET (type: csv, location: 'avito');
DECLARE RECIPE AdsInfo ON avito FROM export AS $$
  SELECT "AdID" AS ad_id, "CategoryID" AS category_id, "Price" AS price
  FROM read_csv('AdsInfo.tsv')$$;
SELECT count(*) FROM adsinfo;
SELECT count(*) FROM AdsInfo;
```

## A table declared quoted

Unquoted, `SearchStream` reads as `searchstream` and finds no table.

```glossql
USE avito;
DECLARE RECIPE "SearchStream" ON avito FROM export AS $$
  SELECT "SearchID" AS search_id, "AdID" AS ad_id FROM read_csv('SearchStream.tsv')$$;
SELECT count(*) FROM "SearchStream";
GLOSS entity ON "SearchStream" AS $${"value": "one ad shown in one search"}$$;
```

## Columns keep the export's spelling

An unquoted column name reaches the one column that differs from it by
case alone. Where two columns differ by case alone, the quotes decide.

```glossql
USE avito;
DECLARE RECIPE location ON avito FROM export AS $$
  SELECT * FROM read_csv('Location.tsv')$$;
SELECT count(*) FROM location WHERE RegionID = 3;
SELECT count(*) FROM location WHERE "RegionID" = 3;
GLOSS role ON location.LocationID AS $${"value": "key"}$$;
```
