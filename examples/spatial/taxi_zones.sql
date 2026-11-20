-- The spatial kit on the New York City taxi zones: load the polygons with the
-- spatial extension's reader, measure them, annotate what the measurement and the
-- reader's metadata say, define one view in the CRS's units, read the
-- glossary. {DATA} is the directory holding taxi_zones.shp; run.sh fills it.

DECLARE DATASET nyc SET (purpose: 'the TLC taxi zones of New York City, one polygon per zone');
DECLARE SOURCE tlc SET (type: shapefile, location: '{DATA}');
USE nyc;

-- ST_Read takes the full path: the source's location does not apply to GDAL
DECLARE RECIPE zones ON nyc FROM tlc SET (key: locationid) AS $$SELECT OBJECTID, LocationID, zone, borough, Shape_Leng, Shape_Area, geom FROM ST_Read('{DATA}/taxi_zones.shp')$$;

-- what is missing: the kit's properties on the geometry column
SELECT subject, property, state FROM GLOSSARY(zones) WHERE state <> 'current';

-- measure before annotating
SELECT geometry_profile() FROM zones.geom;
SELECT layers[1].geometry_fields[1].crs.auth_name || ':' || layers[1].geometry_fields[1].crs.auth_code AS crs FROM ST_Read_Meta('{DATA}/taxi_zones.shp');

GLOSS shape ON zones.geom AS $${"value": "polygon", "represents": "a taxi zone's boundary", "basis": "geometry_profile: 240 POLYGON, 23 MULTIPOLYGON, 0 invalid, 0 empty"}$$;
GLOSS crs ON zones.geom AS $${"value": "EPSG:2263", "unit": "US survey foot", "basis": "ST_Read_Meta: NAD83 / New York Long Island (ftUS); the reader's column type carried it, the loaded column does not"}$$;

-- one view, defined in the CRS's units
GLOSS borough_area ON nyc AS $${"kind": "table", "sql": "SELECT borough, round(sum(ST_Area(geom)) * 0.09290341 / 1e6, 2) AS sq_km, count(*) AS zones FROM nyc.zones GROUP BY borough ORDER BY borough", "assumptions": [{"dimension": "unit", "assumption": "ST_Area in EPSG:2263 is square US survey feet, 0.09290341 m2 each", "basis": "the crs annotation on zones.geom", "confidence": 0.9}]}$$;
FROM nyc.borough_area;

-- the glossary, the lineage, the kits
SELECT subject, property, value, state FROM GLOSSARY(zones) WHERE subject LIKE '%.geom';
SELECT count(*) AS missing FROM GLOSSARY(zones) WHERE state <> 'current';
FROM glossql_lineage($$SELECT borough, ST_Area(geom) AS a FROM nyc.zones$$);
SELECT name, statements FROM kits;
