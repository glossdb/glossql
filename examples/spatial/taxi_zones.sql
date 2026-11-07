-- The taxi zones through the spatial kit: land the polygons through the
-- extension's reader, measure them, gloss what the measurement and the
-- reader's metadata say, ground one question in the CRS's units, read the
-- record. {DATA} is the directory holding taxi_zones.shp; run.sh fills it.

DECLARE DATASET nyc SET (purpose: 'the TLC taxi zones of New York City, one polygon per zone');
DECLARE SOURCE tlc SET (type: shapefile, location: '{DATA}');
USE nyc;

-- the extension's reader takes the full path: the source's location is a label to GDAL
DECLARE RECIPE zones ON nyc FROM tlc SET (key: locationid) AS $$SELECT OBJECTID, LocationID, zone, borough, Shape_Leng, Shape_Area, geom FROM ST_Read('{DATA}/taxi_zones.shp')$$;

-- what is owed: the kit's words on the geometry column alone
SELECT subject, aspect, state FROM GLOSSARY(zones) WHERE state <> 'current';

-- measure before claiming
SELECT geometry_profile() FROM zones.geom;
SELECT layers[1].geometry_fields[1].crs.auth_name || ':' || layers[1].geometry_fields[1].crs.auth_code AS crs FROM ST_Read_Meta('{DATA}/taxi_zones.shp');

GLOSS shape ON zones.geom AS $${"value": "polygon", "represents": "a taxi zone's boundary", "grounds": "geometry_profile: 240 POLYGON, 23 MULTIPOLYGON, 0 invalid, 0 empty"}$$;
GLOSS crs ON zones.geom AS $${"value": "EPSG:2263", "unit": "US survey foot", "grounds": "ST_Read_Meta: NAD83 / New York Long Island (ftUS); the reader's column type carried it, the landed column does not"}$$;

-- one question, grounded in the CRS's units
DECLARE ASPECT borough_area WITH $${"title": "Area per borough"}$$ AS VIEW;
GLOSS borough_area ON nyc AS $${"sql": "SELECT borough, round(sum(ST_Area(geom)) * 0.09290341 / 1e6, 2) AS sq_km, count(*) AS zones FROM nyc.zones GROUP BY borough ORDER BY borough", "assumptions": [{"dimension": "unit", "assumption": "ST_Area in EPSG:2263 is square US survey feet, 0.09290341 m2 each", "basis": "the crs gloss on zones.geom", "confidence": 0.9}]}$$;
FROM nyc.borough_area;

-- the record
SELECT subject, aspect, value, state FROM GLOSSARY(zones) WHERE subject LIKE '%.geom';
SELECT count(*) AS owed FROM GLOSSARY(zones) WHERE state <> 'current';
FROM glossql_lineage($$SELECT borough, ST_Area(geom) AS a FROM nyc.zones$$);
SELECT name, statements FROM glossql.kits;
