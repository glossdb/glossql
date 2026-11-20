---
name: glossql-spatial
description: The spatial kit's properties for a glossql workspace — which columns are geometry and what one geometry represents, the CRS of the coordinates, the profile that measures them, how geometry is loaded. Use after glossql_kit('kits/spatial/kit.sql') when a workspace loads or reads geometry.
---

# The spatial kit

For an agent working in a DuckDB that has the glossql extension and
this kit loaded. The kit's first lines install and load DuckDB's
`spatial` extension. It declares two properties, one measurement and
their tests.

## Load

```sql
FROM glossql_kit('kits/spatial/kit.sql');   -- also in the opener
```

A DuckDB extension is loaded per process. Loading the kit in the
opener loads `spatial` in every session; without it a view over
geometry fails at the first `ST_` function.

## The properties

| Property | On | Body | Says |
|---|---|---|---|
| `shape` | columns of type `GEOMETRY` | `value`: `point`, `line`, `polygon`, `mixed`; `represents`; `basis` | what one geometry represents |
| `crs` | columns of type `GEOMETRY` | `value`: the identifier, such as `EPSG:2263`; `unit`: the unit of a coordinate; `basis` | the coordinate reference system |
| `geometry_profile` | column, a measurement | `total`, `null_ratio`, `kinds`, `invalid`, `empty`, `extent`, `area_sum` | what the geometries are: `SELECT geometry_profile() FROM zones.geom;` |

Both properties are required on geometry columns only. Their tests
take annotations from an agent and a person and have no detector.

## Load geometry

- A load query over `ST_Read` takes the full path:
  `ST_Read('/data/taxi_zones/taxi_zones.shp')`. The source's location
  does not apply: DuckDB's file search path does not reach GDAL.
- The reader's column has the CRS in its type,
  `GEOMETRY('EPSG:2263')`. The lake stores it as plain `GEOMETRY`, and
  `st_crs` returns NULL on the stored column. Read the CRS at the
  source, `SELECT layers[1].geometry_fields[1].crs FROM
  ST_Read_Meta(path)`, and annotate it as `crs`.
- A view over geometry returns results in the CRS's units. State the
  unit in the view's assumptions: EPSG:2263 is in US survey feet, so
  `ST_Area` returns square feet there.

## The order of the work

1. Read what is missing: `SELECT * FROM GLOSSARY(zones) WHERE state <>
   'current';`.
2. Profile first, `SELECT geometry_profile() FROM zones.geom;`, and
   annotate `shape` with the kinds as its basis.
3. Annotate `crs` on the geometry column from `ST_Read_Meta`, with the
   unit of the CRS.
4. Define views in the CRS's units and put the conversion in the
   assumptions.

`examples/spatial/` runs these steps on a public shapefile.
