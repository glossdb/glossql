-- The lineage kit: the lineage of a dataset as one relation, and the three
-- read functions a function's body reads the catalog through. It declares
-- no property; every other kit's functions read through it, so it loads
-- first. What an actor must learn about these words is SKILL.md beside
-- this file.
--
--   FROM glossql_kit('kits/lineage/kit.sql');
--
-- A read function's arguments are a what (a property, a function) and a
-- where (a place, walked from the subject over the lineage), both
-- constants. The subject is the running function's: the engine sets
-- glossql_subject on the connection that computes a result, and a person
-- reading by hand sets it the same way (SET VARIABLE glossql_subject =
-- 'fin.revenue'); where it is not set, the subject is the bound dataset
-- (read_subject()). The engine stores the reads of each function's body
-- with the function (the functions relation, `reads`).

CREATE OR REPLACE MACRO main.read_subject() AS coalesce(getvariable('glossql_subject'), current_schema());

--===------------------------------------------------------------------===--
-- lineage(dataset): the assets of a dataset and the edges between them,
-- one row per edge: from, to, kind. An asset is named by its path: the
-- dataset, dataset.table (a view too), dataset.table.column, and a
-- relationship by its pair path, as it is declared.
--   contains  dataset -> table, view, relationship; table or view -> column
--   reads     view -> the tables and columns it scans and joins on, as the
--             view's SQL resolves to the loaded tables; view -> each view
--             its SQL names
--   relates   relationship -> each of its columns
--   plans     plan -> the metric it plans (x-plan-of)
--===------------------------------------------------------------------===--
-- contains_edges(dataset): the lineage's contains edges alone
CREATE OR REPLACE MACRO main.contains_edges(dataset_name) AS TABLE
WITH cols AS (SELECT table_name, column_name FROM glossql_columns(dataset_name))
SELECT DISTINCT dataset_name AS "from", dataset_name || '.' || table_name AS "to", 'contains' AS kind FROM cols
UNION ALL SELECT dataset_name || '.' || table_name, dataset_name || '.' || table_name || '.' || column_name, 'contains' FROM cols
UNION ALL SELECT dataset_name, left_side || ' ' || op || ' ' || right_side, 'contains' FROM main.relationships WHERE dataset = dataset_name;

CREATE OR REPLACE MACRO main.lineage(dataset_name) AS TABLE
WITH cols AS (SELECT table_name, column_name, table_type FROM glossql_columns(dataset_name)),
views AS (SELECT DISTINCT table_name AS name FROM cols WHERE table_type = 'VIEW'),
rels AS (
  SELECT left_side, right_side, left_side || ' ' || op || ' ' || right_side AS rel
  FROM main.relationships WHERE dataset = dataset_name),
-- a side is table.column or table.(a, b): one edge per column
sides AS (
  SELECT rel, regexp_extract(side, '^([^.]+\.[^.]+)\.', 1) AS tbl,
         unnest(string_split(regexp_replace(regexp_replace(side, '^[^.]+\.[^.]+\.', ''), '^\((.*)\)$', '\1'), ', ')) AS col
  FROM (SELECT rel, left_side AS side FROM rels UNION ALL SELECT rel, right_side FROM rels)),
definitions AS (
  SELECT view AS name, json_extract_string(body, '$.sql') AS sql
  FROM glossql_views_definitions(dataset_name) WHERE view IN (SELECT name FROM views)),
named AS (
  SELECT d.name, unnest(json_extract_string(json_serialize_sql(d.sql), '$..table_name')) AS other
  FROM definitions d WHERE d.sql IS NOT NULL)
SELECT * FROM main.contains_edges(dataset_name)
UNION ALL SELECT DISTINCT rel, tbl || '.' || col, 'relates' FROM sides
UNION ALL SELECT DISTINCT * FROM (
  SELECT dataset_name || '.' || s.view, s.table_name, 'reads' FROM glossql_views_scans(dataset_name) s
  UNION SELECT dataset_name || '.' || s.view, s.table_name || '.' || c, 'reads' FROM glossql_views_scans(dataset_name) s, unnest(s.columns) u(c)
  UNION SELECT dataset_name || '.' || j.view, j.left_column, 'reads' FROM glossql_views_joins(dataset_name) j
  UNION SELECT dataset_name || '.' || j.view, j.right_column, 'reads' FROM glossql_views_joins(dataset_name) j
  UNION SELECT dataset_name || '.' || j.view, regexp_extract(j.left_column, '^([^.]+\.[^.]+)\.', 1), 'reads' FROM glossql_views_joins(dataset_name) j
  UNION SELECT dataset_name || '.' || j.view, regexp_extract(j.right_column, '^([^.]+\.[^.]+)\.', 1), 'reads' FROM glossql_views_joins(dataset_name) j
  UNION SELECT dataset_name || '.' || n.name, dataset_name || '.' || n.other, 'reads' FROM named n
        WHERE n.other <> n.name AND n.other IN (SELECT name FROM views))
UNION ALL SELECT dataset_name || '.' || a.name, dataset_name || '.' || json_extract_string(a.schema, '$.x-plan-of'), 'plans'
          FROM main.aspects a
          WHERE a.kind = 'VIEW' AND json_extract_string(a.schema, '$.x-plan-of') IS NOT NULL
            AND a.name IN (SELECT name FROM views);

-- The class of an asset by its path: what a property's grains name
CREATE OR REPLACE MACRO main.asset_grain(asset) AS
  CASE WHEN contains(asset, ' -> ') OR contains(asset, ' <-> ') THEN 'RELATIONSHIP'
       WHEN NOT contains(asset, '.') THEN 'DATASET'
       WHEN len(string_split(asset, '.')) = 2 THEN 'TABLE'
       ELSE 'COLUMN' END;

--===------------------------------------------------------------------===--
-- place(where): the assets a where names, walked from the running
-- function's subject over the lineage, one row per asset.
--   'itself'                   the subject
--   'its columns'              what the subject contains
--   'its sources'              the tables, columns and views a view reads
--   'the columns it joins on'  the two columns of each join in a view
--   'its relationships'        a view's: those between the tables it reads;
--                              a table's or a column's: those that touch it;
--                              a dataset's: all; a relationship: itself
--   'its metric'               the metric a plan plans
--   'the dataset'              the subject's dataset and every asset in it
-- Any other where names no asset.
--===------------------------------------------------------------------===--
-- Each where is a macro of its own, place_<where>(), and place() plans the
-- one its constant names (query()), so a read plans no walk it does not take.
CREATE OR REPLACE MACRO main.place_itself() AS TABLE SELECT main.read_subject() AS asset;

CREATE OR REPLACE MACRO main.place_columns() AS TABLE
SELECT "to" AS asset FROM main.contains_edges(split_part(main.read_subject(), '.', 1)) WHERE "from" = main.read_subject();

CREATE OR REPLACE MACRO main.place_sources() AS TABLE
SELECT "to" AS asset FROM main.lineage(split_part(main.read_subject(), '.', 1)) WHERE "from" = main.read_subject() AND kind = 'reads';

CREATE OR REPLACE MACRO main.place_joined() AS TABLE
SELECT DISTINCT side AS asset FROM glossql_views_joins(split_part(main.read_subject(), '.', 1)) j, unnest([j.left_column, j.right_column]) u(side)
WHERE split_part(main.read_subject(), '.', 1) || '.' || j.view = main.read_subject();

CREATE OR REPLACE MACRO main.place_relationships() AS TABLE
WITH here AS (SELECT main.read_subject() AS subject, split_part(main.read_subject(), '.', 1) AS dataset_name),
walked AS MATERIALIZED (SELECT * FROM main.lineage(split_part(main.read_subject(), '.', 1)) WHERE kind IN ('reads', 'relates')),
source_tables AS (SELECT walked."to" AS t FROM walked, here WHERE walked."from" = here.subject AND walked.kind = 'reads' AND main.asset_grain(walked."to") = 'TABLE'),
endpoints AS (SELECT walked."from" AS rel, walked."to" AS endpoint, regexp_extract(walked."to", '^([^.]+\.[^.]+)\.', 1) AS t FROM walked WHERE walked.kind = 'relates')
SELECT DISTINCT rel AS asset FROM endpoints, here
WHERE CASE WHEN here.subject = here.dataset_name THEN true
           WHEN main.asset_grain(here.subject) = 'RELATIONSHIP' THEN rel = here.subject
           WHEN EXISTS (FROM source_tables)
             THEN NOT EXISTS (SELECT 1 FROM endpoints x WHERE x.rel = endpoints.rel AND x.t NOT IN (FROM source_tables))
           ELSE EXISTS (SELECT 1 FROM endpoints x WHERE x.rel = endpoints.rel AND here.subject IN (x.t, x.endpoint)) END;

CREATE OR REPLACE MACRO main.place_metric() AS TABLE
SELECT "to" AS asset FROM main.lineage(split_part(main.read_subject(), '.', 1)) WHERE "from" = main.read_subject() AND kind = 'plans';

CREATE OR REPLACE MACRO main.place_dataset() AS TABLE
SELECT split_part(main.read_subject(), '.', 1) AS asset
UNION ALL SELECT DISTINCT "to" FROM main.contains_edges(split_part(main.read_subject(), '.', 1));

CREATE OR REPLACE MACRO main.place(where_) AS TABLE
FROM query(CASE where_ WHEN 'itself' THEN 'FROM main.place_itself()' WHEN 'its columns' THEN 'FROM main.place_columns()'
                       WHEN 'its sources' THEN 'FROM main.place_sources()' WHEN 'the columns it joins on' THEN 'FROM main.place_joined()'
                       WHEN 'its relationships' THEN 'FROM main.place_relationships()' WHEN 'its metric' THEN 'FROM main.place_metric()'
                       WHEN 'the dataset' THEN 'FROM main.place_dataset()' ELSE 'SELECT NULL::VARCHAR AS asset WHERE false' END);

--===------------------------------------------------------------------===--
-- annotations(property, where): one row per asset of the place the
-- property is declared on, with the annotation the catalog holds,
-- collapsed: asset, property, value (JSON), state (current, stale,
-- conflict, as GLOSSARY() says; missing, and no value, where nobody
-- annotated the asset). A property declared on tables returns no row
-- for a column. A what of the form 'x-kind <kind>' names every property
-- declared with that x-kind, a scenario's among them: one row per
-- property and asset, the value a person's newest annotation, else an
-- agent's, state current, or missing.
--===------------------------------------------------------------------===--
CREATE OR REPLACE MACRO main.annotations_of(property, where_) AS TABLE
WITH grains AS (SELECT grains FROM main.aspects WHERE name = property)
SELECT p.asset, property AS property, g.value, coalesce(g.state, 'missing') AS state
FROM main.place(where_) p
LEFT JOIN glossql_glossary(split_part(main.read_subject(), '.', 1), property, false) g ON g.subject = p.asset
WHERE EXISTS (SELECT 1 FROM grains WHERE len(grains) = 0 OR list_contains(grains, main.asset_grain(p.asset)));

CREATE OR REPLACE MACRO main.annotations_of_kind(kind, where_) AS TABLE
WITH props AS (SELECT name, grains FROM main.aspects WHERE json_extract_string(schema, '$.x-kind') = kind),
held AS (
  SELECT subject, aspect, body FROM main.glossary
  WHERE dataset = split_part(main.read_subject(), '.', 1) AND aspect IN (SELECT name FROM props) AND actor_kind IN ('human', 'agent')
  QUALIFY row_number() OVER (PARTITION BY subject, aspect ORDER BY CASE actor_kind WHEN 'human' THEN 0 ELSE 1 END, begin_snapshot DESC) = 1)
SELECT p.asset, pr.name AS property, h.body AS value, CASE WHEN h.body IS NULL THEN 'missing' ELSE 'current' END AS state
FROM main.place(where_) p
JOIN props pr ON len(pr.grains) = 0 OR list_contains(pr.grains, main.asset_grain(p.asset))
LEFT JOIN held h ON h.subject = p.asset AND h.aspect = pr.name;

CREATE OR REPLACE MACRO main.annotations(property, where_) AS TABLE
FROM query(CASE WHEN starts_with(property, 'x-kind ')
                THEN 'FROM main.annotations_of_kind(''' || replace(substr(property, 8), '''', '''''') || ''', ''' || replace(where_, '''', '''''') || ''')'
                ELSE 'FROM main.annotations_of(''' || replace(property, '''', '''''') || ''', ''' || replace(where_, '''', '''''') || ''')' END);

--===------------------------------------------------------------------===--
-- results(function, where): one row per asset of the place, with the
-- result of the function stored on it: asset, function, value (JSON),
-- error; no value and no error where none is stored.
--===------------------------------------------------------------------===--
CREATE OR REPLACE MACRO main.results(function_, where_) AS TABLE
SELECT p.asset, function_ AS function, m.value, m.error
FROM main.place(where_) p
LEFT JOIN main.measurements m
  ON m.dataset = split_part(main.read_subject(), '.', 1) AND m.function = function_ AND m.subject = p.asset;

--===------------------------------------------------------------------===--
-- definition(where): one row per view of the place: view (its path), kind
-- (the property's x-kind), plan_of, covers, the annotation that defines it
-- (body, sql, stopped), its columns with what each is computed from
-- (name, position, derivation, shape), what it reads of each loaded table
-- (table_name, columns) and its joins (left_column, right_column,
-- left_unique, right_unique).
--===------------------------------------------------------------------===--
CREATE OR REPLACE MACRO main.definition(where_) AS TABLE
WITH cols AS (FROM glossql_columns(split_part(main.read_subject(), '.', 1))),
v AS (
  SELECT DISTINCT p.asset AS view, split_part(p.asset, '.', 2) AS name
  FROM main.place(where_) p
  WHERE main.asset_grain(p.asset) = 'TABLE'
    AND split_part(p.asset, '.', 2) IN (SELECT name FROM main.aspects WHERE kind = 'VIEW')),
lin AS (FROM glossql_views_lineage(split_part(main.read_subject(), '.', 1))),
sc AS (FROM glossql_views_scans(split_part(main.read_subject(), '.', 1))),
jo AS (FROM glossql_views_joins(split_part(main.read_subject(), '.', 1))),
bodies AS (
  SELECT view AS name, body FROM glossql_views_definitions(split_part(main.read_subject(), '.', 1))
  WHERE view IN (SELECT name FROM v))
SELECT v.view,
       json_extract_string(a.schema, '$.x-kind') AS kind,
       json_extract_string(a.schema, '$.x-plan-of') AS plan_of,
       json_extract_string(a.schema, '$.x-covers') AS covers,
       b.body,
       json_extract_string(b.body, '$.sql') AS sql,
       json_extract_string(b.body, '$.stopped') AS stopped,
       (SELECT list({'name': c.column_name, 'position': c.ordinal_position, 'derivation': l.derivation, 'shape': l.shape}
                    ORDER BY c.ordinal_position)
        FROM cols c LEFT JOIN lin l ON l.view = c.table_name AND l."column" = c.column_name
        WHERE c.table_name = v.name) AS columns,
       (SELECT list({'table_name': s.table_name, 'columns': s.columns} ORDER BY s.table_name)
        FROM sc s WHERE s.view = v.name) AS reads,
       (SELECT list({'left_column': j.left_column, 'right_column': j.right_column,
                     'left_unique': j.left_unique, 'right_unique': j.right_unique} ORDER BY j.left_column, j.right_column)
        FROM jo j WHERE j.view = v.name) AS joins
FROM v JOIN main.aspects a ON a.name = v.name AND a.kind = 'VIEW' LEFT JOIN bodies b ON b.name = v.name
ORDER BY v.view;
