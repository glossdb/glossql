-- The spatial kit: what a workspace learns about a geometry column. Its first
-- lines bring DuckDB's spatial extension in; a workspace that runs this kit
-- names it in its opener, so every process starts with the extension loaded.
-- What an actor must learn about these words is SKILL.md beside this file.
--
--   FROM glossql_kit('kits/spatial/kit.sql');

INSTALL spatial;
LOAD spatial;

-- the words are owed on geometry columns alone: WHEN TYPE is the gate
DECLARE ASPECT shape WITH $${
  "type": "object", "required": ["value", "represents"],
  "properties": {"value": {"enum": ["point", "line", "polygon", "mixed"]},
                 "represents": {"type": "string"}, "grounds": {"type": "string"}}
}$$ AS FACT ON COLUMN WHEN TYPE = 'GEOMETRY';

DECLARE ASPECT crs WITH $${
  "type": "object", "required": ["value"],
  "properties": {"value": {"type": "string"}, "unit": {"type": "string"}, "grounds": {"type": "string"}}
}$$ AS FACT ON COLUMN WHEN TYPE = 'GEOMETRY';

-- the profile: what the geometries are. The CRS is not here: the lake lands a
-- geometry without the CRS its reader carried, so the crs gloss is where it lives
DECLARE ASPECT geometry_profile WITH $${
  "type": "object", "required": ["total", "null_ratio", "kinds", "invalid", "empty"],
  "properties": {"total": {"type": "integer"}, "null_ratio": {"type": "number"}, "kinds": {},
                 "invalid": {"type": "integer"}, "empty": {"type": "integer"},
                 "extent": {}, "area_sum": {"type": "number"}}
}$$ AS MEASUREMENT ON COLUMN;

DECLARE FUNCTION geometry_profile FOR GLOBAL AS $$
  SELECT count(*) AS total,
         CASE WHEN count(*) = 0 THEN 0.0 ELSE (count(*) - count(v)) / count(*)::DOUBLE END AS null_ratio,
         (SELECT json_group_object(kind, n)
            FROM (SELECT ST_GeometryType(v)::VARCHAR AS kind, count(*) AS n
                    FROM subject_column($subject) WHERE v IS NOT NULL GROUP BY 1)) AS kinds,
         count(*) FILTER (WHERE v IS NOT NULL AND NOT ST_IsValid(v)) AS invalid,
         count(*) FILTER (WHERE v IS NOT NULL AND ST_IsEmpty(v)) AS empty,
         ST_Extent_Agg(v)::VARCHAR AS extent,
         sum(ST_Area(v)) AS area_sum
  FROM subject_column($subject)
$$ RETURNS geometry_profile;

DECLARE WITNESS shape_w ON shape BY (AGENT, HUMAN);
DECLARE WITNESS crs_w ON crs BY (AGENT, HUMAN);
