---
name: gloss-spatial
description: The spatial kit's vocabulary for a gloss workspace — which columns are geometry and what one stands for, the CRS the coordinates are in, the profile that measures them, how geometry lands. Use after gloss_kit('kits/spatial/kit.sql') when a workspace lands or reads geometry.
---

# The spatial kit: what it teaches

For an actor working in a DuckDB that has the gloss extension loaded and
this kit declared. The kit brings DuckDB's spatial extension in, its
first lines install and load it, and declares two words, one
measurement and their witnesses.

## Load

```sql
FROM gloss_kit('kits/spatial/kit.sql');   -- and the same line in the opener
```

A DuckDB extension is loaded per process. The opener's replay of the
kit is what loads spatial next time; without it a grounding over
geometry stops at the first `ST_` function.

## The words

| Aspect | On | Body | Says |
|---|---|---|---|
| `shape` | geometry columns (`WHEN TYPE = 'GEOMETRY'`) | `value`: `point`, `line`, `polygon`, `mixed`; `represents`; `grounds` | what one geometry stands for |
| `crs` | geometry columns (`WHEN TYPE = 'GEOMETRY'`) | `value`: the identifier, `EPSG:2263`; `unit`: what a coordinate counts; `grounds` | the coordinate reference system the coordinates are in |
| `geometry_profile` | column, a measurement | `total`, `null_ratio`, `kinds`, `invalid`, `empty`, `extent`, `area_sum` | what the geometries are: `SELECT geometry_profile() FROM zones.geom;` |

Both witnesses take the agent and the human without a detector.

## Landing geometry

- A recipe over the extension's own reader takes the full path:
  `ST_Read('/data/taxi_zones/taxi_zones.shp')`. The source's location
  is a label there; DuckDB's file search path does not reach GDAL.
- The reader's column carries the CRS in its type,
  `GEOMETRY('EPSG:2263')`. The lake lands it as plain `GEOMETRY`, and
  `st_crs` returns NULL on the landed column. Read the CRS at the
  source, `SELECT layers[1].geometry_fields[1].crs FROM
  ST_Read_Meta(path)`, and gloss it: the `crs` gloss is where it
  survives.
- A grounding over geometry calls the extension's functions and serves
  in the CRS's units. Say the unit in its assumptions: EPSG:2263 is US
  survey feet, so `ST_Area` is square feet there.

## The order of a round

1. Read what is owed: `SELECT * FROM GLOSSARY(zones) WHERE state <>
   'current';`. Both words are owed on the geometry columns and nowhere
   else.
2. Extract first, `SELECT geometry_profile() FROM zones.geom;`, and
   gloss `shape` with the kinds as grounds.
3. Gloss `crs` on the geometry column from `ST_Read_Meta`, with the
   unit the CRS implies.
4. Ground what is asked in the CRS's units, the conversion in the
   assumptions.
