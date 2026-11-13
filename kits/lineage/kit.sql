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

-- view_bodies(dataset): the annotation that defines each view of a
-- dataset, (view, body), the newest on the dataset
CREATE OR REPLACE MACRO main.view_bodies(dataset_name) AS TABLE
SELECT aspect AS view, body FROM main.glossary
WHERE dataset = dataset_name AND subject = dataset_name AND aspect IN (SELECT name FROM main.aspects WHERE kind = 'VIEW')
QUALIFY row_number() OVER (PARTITION BY aspect ORDER BY begin_snapshot DESC) = 1;

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
  FROM main.view_bodies(dataset_name) WHERE view IN (SELECT name FROM views)),
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
-- Each where is a macro of its own over the whole dataset, place_<where>
-- (dataset): (subject, asset) for every subject at once, so that the
-- engine evaluates the reads of every stored result in one pass
-- (places()); 'the dataset' is anchored at the dataset. place() plans the
-- one its constant names (query()) for the running function's subject.
CREATE OR REPLACE MACRO main.place_columns(dataset_name) AS TABLE
SELECT "from" AS subject, "to" AS asset FROM main.contains_edges(dataset_name);

CREATE OR REPLACE MACRO main.place_sources(dataset_name) AS TABLE
SELECT "from" AS subject, "to" AS asset FROM main.lineage(dataset_name) WHERE kind = 'reads';

CREATE OR REPLACE MACRO main.place_joined(dataset_name) AS TABLE
SELECT DISTINCT dataset_name || '.' || j.view AS subject, side AS asset
FROM glossql_views_joins(dataset_name) j, unnest([j.left_column, j.right_column]) u(side);

CREATE OR REPLACE MACRO main.place_relationships(dataset_name) AS TABLE
WITH walked AS MATERIALIZED (SELECT * FROM main.lineage(dataset_name) WHERE kind IN ('reads', 'relates')),
endpoints AS (SELECT "from" AS rel, "to" AS endpoint, regexp_extract("to", '^([^.]+\.[^.]+)\.', 1) AS t FROM walked WHERE kind = 'relates'),
sources AS (SELECT "from" AS subject, "to" AS t FROM walked WHERE kind = 'reads' AND main.asset_grain("to") = 'TABLE'),
rels AS (SELECT DISTINCT rel FROM endpoints)
SELECT dataset_name AS subject, rel AS asset FROM rels
UNION SELECT rel, rel FROM rels
UNION SELECT s.subject, r.rel FROM (SELECT DISTINCT subject FROM sources) s, rels r
      WHERE NOT EXISTS (SELECT 1 FROM endpoints x WHERE x.rel = r.rel
                        AND NOT EXISTS (SELECT 1 FROM sources y WHERE y.subject = s.subject AND y.t = x.t))
UNION SELECT u.a, e.rel FROM endpoints e, unnest([e.t, e.endpoint]) u(a) WHERE u.a NOT IN (SELECT subject FROM sources);

CREATE OR REPLACE MACRO main.place_metric(dataset_name) AS TABLE
SELECT "from" AS subject, "to" AS asset FROM main.lineage(dataset_name) WHERE kind = 'plans';

CREATE OR REPLACE MACRO main.place_dataset(dataset_name) AS TABLE
SELECT dataset_name AS subject, dataset_name AS asset
UNION SELECT dataset_name, "to" FROM main.contains_edges(dataset_name);

-- places(dataset): every place of every subject of a dataset, (subject,
-- where, asset); 'itself' is the subject and has no row here
CREATE OR REPLACE MACRO main.places(dataset_name) AS TABLE
SELECT subject, 'its columns' AS "where", asset FROM main.place_columns(dataset_name)
UNION ALL SELECT subject, 'its sources', asset FROM main.place_sources(dataset_name)
UNION ALL SELECT subject, 'the columns it joins on', asset FROM main.place_joined(dataset_name)
UNION ALL SELECT subject, 'its relationships', asset FROM main.place_relationships(dataset_name)
UNION ALL SELECT subject, 'its metric', asset FROM main.place_metric(dataset_name)
UNION ALL SELECT subject, 'the dataset', asset FROM main.place_dataset(dataset_name);

CREATE OR REPLACE MACRO main.place(where_) AS TABLE
FROM query(CASE where_
  WHEN 'itself' THEN 'SELECT main.read_subject() AS asset'
  WHEN 'its columns' THEN 'SELECT asset FROM main.place_columns(split_part(main.read_subject(), ''.'', 1)) WHERE subject = main.read_subject()'
  WHEN 'its sources' THEN 'SELECT asset FROM main.place_sources(split_part(main.read_subject(), ''.'', 1)) WHERE subject = main.read_subject()'
  WHEN 'the columns it joins on' THEN 'SELECT asset FROM main.place_joined(split_part(main.read_subject(), ''.'', 1)) WHERE subject = main.read_subject()'
  WHEN 'its relationships' THEN 'SELECT asset FROM main.place_relationships(split_part(main.read_subject(), ''.'', 1)) WHERE subject = main.read_subject()'
  WHEN 'its metric' THEN 'SELECT asset FROM main.place_metric(split_part(main.read_subject(), ''.'', 1)) WHERE subject = main.read_subject()'
  WHEN 'the dataset' THEN 'SELECT asset FROM main.place_dataset(split_part(main.read_subject(), ''.'', 1))'
  ELSE 'SELECT NULL::VARCHAR AS asset WHERE false' END);

-- tables_read(dataset): the relations of the lake whose rows each
-- asset's data is, (asset, asset_kind, table_name): a table its own; a
-- view itself, the views it reads and the tables it scans; a column those
-- of its table or view; a relationship those of its columns; the dataset
-- every table. What a stored result that read data depends on: each at
-- DuckLake's version of it, a view's the definition it holds
CREATE OR REPLACE MACRO main.tables_read(dataset_name) AS TABLE
WITH cols AS (SELECT table_name, column_name, table_type FROM glossql_columns(dataset_name)),
tables AS (SELECT DISTINCT dataset_name || '.' || table_name AS t FROM cols WHERE table_type = 'BASE TABLE'),
views AS (SELECT DISTINCT dataset_name || '.' || table_name AS v FROM cols WHERE table_type = 'VIEW'),
lin AS MATERIALIZED (FROM main.lineage(dataset_name)),
relations AS (
  SELECT t AS asset, 'table' AS asset_kind, t AS table_name FROM tables
  UNION SELECT v, 'view', v FROM views
  UNION SELECT dataset_name || '.' || s.view, 'view', s.table_name FROM glossql_views_scans(dataset_name) s
  UNION SELECT l."from", 'view', l."to" FROM lin l WHERE l.kind = 'reads' AND l."to" IN (SELECT v FROM views))
SELECT asset, asset_kind, table_name FROM relations
UNION ALL SELECT dataset_name || '.' || c.table_name || '.' || c.column_name, 'column', r.table_name
          FROM cols c JOIN relations r ON r.asset = dataset_name || '.' || c.table_name
UNION ALL SELECT DISTINCT l."from", 'relationship', r.table_name
          FROM lin l JOIN relations r ON r.asset = regexp_extract(l."to", '^([^.]+\.[^.]+)\.', 1)
          WHERE l.kind = 'relates'
UNION ALL SELECT dataset_name, 'dataset', t FROM tables;

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
-- definition(where): one row per view of the place, its definition: view
-- (its path), kind (the property's x-kind), plan_of, covers, the SQL of
-- the annotation that defines it (sql, or stopped), its columns with what
-- each is computed from (name, position, derivation, shape), what it
-- reads of each loaded table (table_name, columns) and its joins
-- (left_column, right_column, left_unique, right_unique). The rest of the
-- annotation, its assumptions, is no part of the definition a body reads.
-- definitions(dataset) is the same for every view of a dataset at once.
--===------------------------------------------------------------------===--
CREATE OR REPLACE MACRO main.definitions(dataset_name) AS TABLE
WITH cols AS (FROM glossql_columns(dataset_name)),
lin AS (FROM glossql_views_lineage(dataset_name)),
sc AS (FROM glossql_views_scans(dataset_name)),
jo AS (FROM glossql_views_joins(dataset_name)),
bodies AS (FROM main.view_bodies(dataset_name))
SELECT dataset_name || '.' || a.name AS view,
       json_extract_string(a.schema, '$.x-kind') AS kind,
       json_extract_string(a.schema, '$.x-plan-of') AS plan_of,
       json_extract_string(a.schema, '$.x-covers') AS covers,
       json_extract_string(b.body, '$.sql') AS sql,
       json_extract_string(b.body, '$.stopped') AS stopped,
       (SELECT list({'name': c.column_name, 'position': c.ordinal_position, 'derivation': l.derivation, 'shape': l.shape}
                    ORDER BY c.ordinal_position)
        FROM cols c LEFT JOIN lin l ON l.view = c.table_name AND l."column" = c.column_name
        WHERE c.table_name = a.name) AS columns,
       (SELECT list({'table_name': s.table_name, 'columns': s.columns} ORDER BY s.table_name)
        FROM sc s WHERE s.view = a.name) AS reads,
       (SELECT list({'left_column': j.left_column, 'right_column': j.right_column,
                     'left_unique': j.left_unique, 'right_unique': j.right_unique} ORDER BY j.left_column, j.right_column)
        FROM jo j WHERE j.view = a.name) AS joins
FROM main.aspects a LEFT JOIN bodies b ON b.view = a.name
WHERE a.kind = 'VIEW';

CREATE OR REPLACE MACRO main.definition(where_) AS TABLE
SELECT d.* FROM (SELECT DISTINCT asset FROM main.place(where_)) p
JOIN main.definitions(split_part(main.read_subject(), '.', 1)) d ON d.view = p.asset
ORDER BY d.view;
