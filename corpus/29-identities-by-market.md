# Relationships on a key unique only within its market

Three markets run three ERPs: SAP in Denmark, Dynamics 365 in
Switzerland, Monolith in Poland. Each writes its own item code. The
group's identity sheet holds one row per market and local code with
the group's material number; the hierarchy sheet holds one row per
market and local hierarchy node. A local code is unique only within its
market. The Swiss tables carry no market column: every row of them is
Swiss. The views read the sheets with the market in the join:
`JOIN model_identities i ON i.market = 'CH' AND i.local_code = d.item_id`.

The artifact is the supply demo (`statements/10-land.glossql`,
`20-structure.glossql`, `30-harmonise.glossql`) and the harmonise kit's
fixture of the same three systems. Ten relationships reach the two
sheets from the markets' tables: seven to the identities, three to the
hierarchy. The statements below write the ten in each candidate form.
No form is decided; the blocks tagged `glossql-gap` show forms the
language does not have.

The demo declares three of the ten today, on the local code alone; a
Swiss code would also match a Polish row of the same code. Its views
join the other seven with the market in the join, over no declared
relationship.

```glossql
USE supply;

DECLARE RELATIONSHIP material_sap.matnr_sap -> model_identities.local_code;
DECLARE RELATIONSHIP product_d365.item_id -> model_identities.local_code;
DECLARE RELATIONSHIP product_mono.indeks -> model_identities.local_code;
```

## 1. The market as a constant inside the key

The child's key is a tuple whose first part is the market every row of
the table belongs to.

```glossql-gap
DECLARE RELATIONSHIP material_sap.('DK', matnr_sap) -> model_identities.(market, local_code);
DECLARE RELATIONSHIP product_d365.('CH', item_id) -> model_identities.(market, local_code);
DECLARE RELATIONSHIP stock_d365.('CH', item_id) -> model_identities.(market, local_code);
DECLARE RELATIONSHIP orders_d365.('CH', item_id) -> model_identities.(market, local_code);
DECLARE RELATIONSHIP product_mono.('PL', indeks) -> model_identities.(market, local_code);
DECLARE RELATIONSHIP stock_mono.('PL', indeks) -> model_identities.(market, local_code);
DECLARE RELATIONSHIP orders_mono.('PL', indeks) -> model_identities.(market, local_code);
DECLARE RELATIONSHIP material_sap.('DK', hierarchy_node) -> model_hierarchy.(market, local_node);
DECLARE RELATIONSHIP product_d365.('CH', category_path) -> model_hierarchy.(market, local_node);
DECLARE RELATIONSHIP product_mono.('PL', group_code) -> model_hierarchy.(market, local_node);
```

The constant is part of the relationship's address:

```glossql-gap
GLOSS meaning ON stock_d365.('CH', item_id) -> model_identities.(market, local_code) AS
  $${"value": "a Swiss on-hand row's item is the group material the identity sheet maps for Switzerland"}$$;
SELECT relationship_coherence() FROM stock_d365.('CH', item_id) -> model_identities.(market, local_code);
```

## 2. A row condition on the relationship, with `WHEN`

The key stays the local code; a condition names the parent rows the
key is unique among.

```glossql-gap
DECLARE RELATIONSHIP material_sap.matnr_sap -> model_identities.local_code WHEN model_identities.market = 'DK';
DECLARE RELATIONSHIP product_d365.item_id -> model_identities.local_code WHEN model_identities.market = 'CH';
DECLARE RELATIONSHIP stock_d365.item_id -> model_identities.local_code WHEN model_identities.market = 'CH';
DECLARE RELATIONSHIP orders_d365.item_id -> model_identities.local_code WHEN model_identities.market = 'CH';
DECLARE RELATIONSHIP product_mono.indeks -> model_identities.local_code WHEN model_identities.market = 'PL';
DECLARE RELATIONSHIP stock_mono.indeks -> model_identities.local_code WHEN model_identities.market = 'PL';
DECLARE RELATIONSHIP orders_mono.indeks -> model_identities.local_code WHEN model_identities.market = 'PL';
DECLARE RELATIONSHIP material_sap.hierarchy_node -> model_hierarchy.local_node WHEN model_hierarchy.market = 'DK';
DECLARE RELATIONSHIP product_d365.category_path -> model_hierarchy.local_node WHEN model_hierarchy.market = 'CH';
DECLARE RELATIONSHIP product_mono.group_code -> model_hierarchy.local_node WHEN model_hierarchy.market = 'PL';
```

Where the condition is part of the address, every reference carries it:

```glossql-gap
GLOSS meaning ON stock_d365.item_id -> model_identities.local_code WHEN model_identities.market = 'CH' AS
  $${"value": "a Swiss on-hand row's item is the group material the identity sheet maps for Switzerland"}$$;
SELECT relationship_coherence() FROM stock_d365.item_id -> model_identities.local_code WHEN model_identities.market = 'CH';
```

In this artifact each of the ten pair paths is distinct without the
condition, since each child table belongs to one market. Where the
condition is not part of the address, a reference reads as today:

```glossql
GLOSS meaning ON stock_d365.item_id -> model_identities.local_code AS
  $${"value": "a Swiss on-hand row's item is the group material the identity sheet maps for Switzerland"}$$;
SELECT relationship_coherence() FROM stock_d365.item_id -> model_identities.local_code;
```

## 3. No new form: the load writes the market

Each recipe of a market's tables adds the market as a column, and the
existing composite key is used. The three Swiss recipes; the three
Danish and three Polish recipes take the same line with `'DK'` and
`'PL'`.

```glossql
DECLARE RECIPE product_d365 ON supply FROM d365 AS $$
  SELECT 'CH' AS market,
         p."ItemId" AS item_id,
         p."ProductName" AS name,
         p."UnitId" AS unit_id,
         try_cast(p."GlobalItemNumber" AS BIGINT) AS global_item,
         p."CategoryPath" AS category_path,
         try_cast(p."ShelfLifeDays" AS INT) AS shelf_life_days,
         try_cast(c."Factor" AS DOUBLE) AS per_pallet
  FROM read_csv('products.csv') p
  LEFT JOIN read_csv('unitconversions.csv') c ON c."ItemId" = p."ItemId" AND c."FromUnitId" = 'Pl'$$;
DECLARE RECIPE stock_d365 ON supply FROM d365 AS $$
  SELECT 'CH' AS market,
         "ItemId" AS item_id,
         "InventSiteId" AS site_id,
         "InventLocationId" AS location_id,
         try_cast("PhysicalInvent" AS DOUBLE) AS physical,
         try_cast("AvailPhysical" AS DOUBLE) AS available,
         try_cast("ReservPhysical" AS DOUBLE) AS reserved,
         "UnitId" AS unit_id,
         CAST(try_to_timestamp("SnapshotDate", '%Y-%m-%dT%H:%M:%SZ') AS DATE) AS snapshot_date
  FROM read_csv('inventsum_*.csv')$$;
DECLARE RECIPE orders_d365 ON supply FROM d365 AS $$
  SELECT 'CH' AS market,
         "SalesId" AS order_no,
         try_cast("LineNum" AS INT) AS line_no,
         "ItemId" AS item_id,
         "InventSiteId" AS site_id,
         "CustAccount" AS customer,
         try_cast("SalesQty" AS DOUBLE) AS quantity,
         "SalesUnit" AS sales_unit,
         CAST(try_to_timestamp("CreatedDateTime", '%Y-%m-%dT%H:%M:%SZ') AS DATE) AS order_date,
         try_to_date("ShippingDateRequested", '%Y-%m-%d') AS due_date,
         try_cast("LineAmount" AS DOUBLE) AS amount,
         "CurrencyCode" AS currency
  FROM read_csv('salesline_*.csv')$$;
```

```glossql
USE supply;

DECLARE RELATIONSHIP material_sap.(market, matnr_sap) -> model_identities.(market, local_code);
DECLARE RELATIONSHIP product_d365.(market, item_id) -> model_identities.(market, local_code);
DECLARE RELATIONSHIP stock_d365.(market, item_id) -> model_identities.(market, local_code);
DECLARE RELATIONSHIP orders_d365.(market, item_id) -> model_identities.(market, local_code);
DECLARE RELATIONSHIP product_mono.(market, indeks) -> model_identities.(market, local_code);
DECLARE RELATIONSHIP stock_mono.(market, indeks) -> model_identities.(market, local_code);
DECLARE RELATIONSHIP orders_mono.(market, indeks) -> model_identities.(market, local_code);
DECLARE RELATIONSHIP material_sap.(market, hierarchy_node) -> model_hierarchy.(market, local_node);
DECLARE RELATIONSHIP product_d365.(market, category_path) -> model_hierarchy.(market, local_node);
DECLARE RELATIONSHIP product_mono.(market, group_code) -> model_hierarchy.(market, local_node);
```

## 3b. No new form: one identity table per market

The identity sheet is loaded once per market, as the FERC Form 1
mapping tables keep one table per filing system
(`utilities_old`, `utilities_new`). Each key is unique in its table.

```glossql
DECLARE RECIPE identities_dk ON supply FROM model AS $$
  SELECT "local_code" AS local_code, try_cast("global_material" AS BIGINT) AS global_material, "note" AS note
  FROM read_csv('identities.csv') WHERE "market" = 'DK'$$;
DECLARE RECIPE identities_ch ON supply FROM model AS $$
  SELECT "local_code" AS local_code, try_cast("global_material" AS BIGINT) AS global_material, "note" AS note
  FROM read_csv('identities.csv') WHERE "market" = 'CH'$$;
DECLARE RECIPE identities_pl ON supply FROM model AS $$
  SELECT "local_code" AS local_code, try_cast("global_material" AS BIGINT) AS global_material, "note" AS note
  FROM read_csv('identities.csv') WHERE "market" = 'PL'$$;
DECLARE RECIPE hierarchy_dk ON supply FROM model AS $$
  SELECT "local_node" AS local_node, "global_brand" AS global_brand, nullif("global_category", '') AS global_category, "local_name" AS local_name
  FROM read_csv('hierarchy.csv') WHERE "market" = 'DK'$$;
DECLARE RECIPE hierarchy_ch ON supply FROM model AS $$
  SELECT "local_node" AS local_node, "global_brand" AS global_brand, nullif("global_category", '') AS global_category, "local_name" AS local_name
  FROM read_csv('hierarchy.csv') WHERE "market" = 'CH'$$;
DECLARE RECIPE hierarchy_pl ON supply FROM model AS $$
  SELECT "local_node" AS local_node, "global_brand" AS global_brand, nullif("global_category", '') AS global_category, "local_name" AS local_name
  FROM read_csv('hierarchy.csv') WHERE "market" = 'PL'$$;
```

```glossql
USE supply;

DECLARE RELATIONSHIP material_sap.matnr_sap -> identities_dk.local_code;
DECLARE RELATIONSHIP product_d365.item_id -> identities_ch.local_code;
DECLARE RELATIONSHIP stock_d365.item_id -> identities_ch.local_code;
DECLARE RELATIONSHIP orders_d365.item_id -> identities_ch.local_code;
DECLARE RELATIONSHIP product_mono.indeks -> identities_pl.local_code;
DECLARE RELATIONSHIP stock_mono.indeks -> identities_pl.local_code;
DECLARE RELATIONSHIP orders_mono.indeks -> identities_pl.local_code;
DECLARE RELATIONSHIP material_sap.hierarchy_node -> hierarchy_dk.local_node;
DECLARE RELATIONSHIP product_d365.category_path -> hierarchy_ch.local_node;
DECLARE RELATIONSHIP product_mono.group_code -> hierarchy_pl.local_node;
```

## 4. A row condition with `WHERE`

Form 2 with SQL's word for a row condition, so `WHEN` keeps its one
meaning on an aspect.

```glossql-gap
DECLARE RELATIONSHIP material_sap.matnr_sap -> model_identities.local_code WHERE model_identities.market = 'DK';
DECLARE RELATIONSHIP product_d365.item_id -> model_identities.local_code WHERE model_identities.market = 'CH';
DECLARE RELATIONSHIP stock_d365.item_id -> model_identities.local_code WHERE model_identities.market = 'CH';
DECLARE RELATIONSHIP orders_d365.item_id -> model_identities.local_code WHERE model_identities.market = 'CH';
DECLARE RELATIONSHIP product_mono.indeks -> model_identities.local_code WHERE model_identities.market = 'PL';
DECLARE RELATIONSHIP stock_mono.indeks -> model_identities.local_code WHERE model_identities.market = 'PL';
DECLARE RELATIONSHIP orders_mono.indeks -> model_identities.local_code WHERE model_identities.market = 'PL';
DECLARE RELATIONSHIP material_sap.hierarchy_node -> model_hierarchy.local_node WHERE model_hierarchy.market = 'DK';
DECLARE RELATIONSHIP product_d365.category_path -> model_hierarchy.local_node WHERE model_hierarchy.market = 'CH';
DECLARE RELATIONSHIP product_mono.group_code -> model_hierarchy.local_node WHERE model_hierarchy.market = 'PL';
```

## The reference whose target table depends on a type column

SPEC §9 names a second shape: one column whose target table depends on
a sibling type column. None of the corpora this transcription read has
one (the supply demo, the harmonise fixture, BookSQL, RelBench rel-f1
and rel-event, EIA-860M, FERC Form 1). BookSQL keeps customers and
vendors in separate columns of its transaction table. The shape stays
open.
