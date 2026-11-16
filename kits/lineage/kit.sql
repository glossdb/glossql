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
--   loaded from  table -> the source its recipe loads it from, a source
--             named as it is declared
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

-- loaded_from_edges(dataset): the lineage's loaded from edges alone,
-- table -> source, for each loaded table of the dataset a recipe loads
CREATE OR REPLACE MACRO main.loaded_from_edges(dataset_name) AS TABLE
SELECT dataset_name || '.' || r.table_name AS "from", r.source AS "to", 'loaded from' AS kind
FROM main.recipes r
WHERE r.dataset = dataset_name
  AND r.table_name IN (SELECT DISTINCT table_name FROM glossql_columns(dataset_name) WHERE table_type = 'BASE TABLE');

-- plans_edges(dataset): the lineage's plans edges alone, plan -> metric
CREATE OR REPLACE MACRO main.plans_edges(dataset_name) AS TABLE
SELECT dataset_name || '.' || a.name AS "from", dataset_name || '.' || json_extract_string(a.schema, '$.x-plan-of') AS "to",
       'plans' AS kind
FROM main.aspects a
WHERE a.kind = 'VIEW' AND json_extract_string(a.schema, '$.x-plan-of') IS NOT NULL
  AND a.name IN (SELECT DISTINCT table_name FROM glossql_columns(dataset_name) WHERE table_type = 'VIEW');

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
  FROM definitions d WHERE d.sql IS NOT NULL),
sc AS MATERIALIZED (FROM glossql_views_scans(dataset_name)),
jo AS MATERIALIZED (FROM glossql_views_joins(dataset_name))
SELECT * FROM main.contains_edges(dataset_name)
UNION ALL SELECT DISTINCT rel, tbl || '.' || col, 'relates' FROM sides
UNION ALL SELECT DISTINCT * FROM (
  SELECT dataset_name || '.' || s.view, s.table_name, 'reads' FROM sc s
  UNION SELECT dataset_name || '.' || s.view, s.table_name || '.' || c, 'reads' FROM sc s, unnest(s.columns) u(c)
  UNION SELECT dataset_name || '.' || j.view, j.left_column, 'reads' FROM jo j
  UNION SELECT dataset_name || '.' || j.view, j.right_column, 'reads' FROM jo j
  UNION SELECT dataset_name || '.' || j.view, dataset_name || '.' || c.table_name, 'reads' FROM jo j
        JOIN cols c ON dataset_name || '.' || c.table_name || '.' || c.column_name IN (j.left_column, j.right_column)
  UNION SELECT dataset_name || '.' || n.name, dataset_name || '.' || n.other, 'reads' FROM named n
        WHERE n.other <> n.name AND n.other IN (SELECT name FROM views))
UNION ALL SELECT * FROM main.plans_edges(dataset_name)
UNION ALL SELECT * FROM main.loaded_from_edges(dataset_name);

-- The class of an asset by its path: what a property's grains name. A bare
-- name is a dataset, or a source where no dataset carries it
CREATE OR REPLACE MACRO main.asset_grain(asset) AS
  CASE WHEN contains(asset, ' -> ') OR contains(asset, ' <-> ') THEN 'RELATIONSHIP'
       WHEN NOT contains(asset, '.') AND asset IN (SELECT name FROM main.sources)
            AND asset NOT IN (SELECT name FROM main.datasets) THEN 'SOURCE'
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
--   'its relationships'        a view's: those between the tables it reads
--                              and those that touch the view; a table's or
--                              a column's: those that touch it;
--                              a dataset's: all; a relationship: itself
--   'its metric'               the metric a plan plans, and its columns
--   'its source systems'       a table's: the source it is loaded from; a
--                              view's: those of the tables it reads
--   'its dataset'              the subject's dataset itself
--   'the dataset'              the subject's dataset and every asset in it
--   'the dataset's metrics'    the views of the subject's dataset that are
--                              metrics (no plan, relation, single value or
--                              sample), and their columns
-- Any other where is refused, not_a_place.
--===------------------------------------------------------------------===--
-- place_list(): the places, one entry each: the where, and whether it is
-- anchored, the same for every subject of the dataset. The engine reads the
-- places from here.
CREATE OR REPLACE MACRO main.place_list() AS [
  {'where': 'itself', 'anchored': false},
  {'where': 'its columns', 'anchored': false},
  {'where': 'its sources', 'anchored': false},
  {'where': 'the columns it joins on', 'anchored': false},
  {'where': 'its relationships', 'anchored': false},
  {'where': 'its metric', 'anchored': false},
  {'where': 'its source systems', 'anchored': false},
  {'where': 'its dataset', 'anchored': true},
  {'where': 'the dataset''s metrics', 'anchored': true},
  {'where': 'the dataset', 'anchored': true}];

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
WITH walked AS MATERIALIZED (SELECT * FROM main.lineage(dataset_name) WHERE kind IN ('reads', 'relates', 'contains')),
endpoints AS (SELECT w."from" AS rel, w."to" AS endpoint, c."from" AS t FROM walked w
              JOIN walked c ON c.kind = 'contains' AND c."to" = w."to" WHERE w.kind = 'relates'),
sources AS (SELECT "from" AS subject, "to" AS t FROM walked WHERE kind = 'reads' AND main.asset_grain("to") = 'TABLE'),
rels AS (SELECT DISTINCT rel FROM endpoints)
SELECT dataset_name AS subject, rel AS asset FROM rels
UNION SELECT rel, rel FROM rels
UNION SELECT s.subject, r.rel FROM (SELECT DISTINCT subject FROM sources) s, rels r
      WHERE NOT EXISTS (SELECT 1 FROM endpoints x WHERE x.rel = r.rel
                        AND NOT EXISTS (SELECT 1 FROM sources y WHERE y.subject = s.subject AND y.t = x.t))
UNION SELECT u.a, e.rel FROM endpoints e, unnest([e.t, e.endpoint]) u(a);

CREATE OR REPLACE MACRO main.place_metric(dataset_name) AS TABLE
WITH planned AS (SELECT "from" AS subject, "to" AS metric FROM main.plans_edges(dataset_name))
SELECT subject, metric AS asset FROM planned
UNION SELECT p.subject, c."to" FROM planned p JOIN main.contains_edges(dataset_name) c ON c."from" = p.metric;

CREATE OR REPLACE MACRO main.place_systems(dataset_name) AS TABLE
WITH loaded AS (SELECT "from" AS t, "to" AS source FROM main.loaded_from_edges(dataset_name))
SELECT t AS subject, source AS asset FROM loaded
UNION SELECT dataset_name || '.' || s.view, l.source
      FROM glossql_views_scans(dataset_name) s JOIN loaded l ON l.t = s.table_name;

CREATE OR REPLACE MACRO main.place_its_dataset(dataset_name) AS TABLE
SELECT dataset_name AS subject, dataset_name AS asset;

CREATE OR REPLACE MACRO main.place_dataset(dataset_name) AS TABLE
SELECT dataset_name AS subject, dataset_name AS asset
UNION SELECT dataset_name, "to" FROM main.contains_edges(dataset_name);

CREATE OR REPLACE MACRO main.place_metrics(dataset_name) AS TABLE
WITH metrics AS (
  SELECT dataset_name || '.' || name AS m FROM main.aspects
  WHERE kind = 'VIEW' AND coalesce(json_extract_string(schema, '$.x-kind'), 'metric') = 'metric'
    AND name IN (SELECT DISTINCT table_name FROM glossql_columns(dataset_name) WHERE table_type = 'VIEW'))
SELECT DISTINCT dataset_name AS subject, "to" AS asset FROM main.contains_edges(dataset_name)
WHERE "to" IN (SELECT m FROM metrics) OR "from" IN (SELECT m FROM metrics);

-- places(dataset): every place of every subject of a dataset, (subject,
-- where, asset); 'itself' is the subject and has no row here
CREATE OR REPLACE MACRO main.places(dataset_name) AS TABLE
SELECT subject, 'its columns' AS "where", asset FROM main.place_columns(dataset_name)
UNION ALL SELECT subject, 'its sources', asset FROM main.place_sources(dataset_name)
UNION ALL SELECT subject, 'the columns it joins on', asset FROM main.place_joined(dataset_name)
UNION ALL SELECT subject, 'its relationships', asset FROM main.place_relationships(dataset_name)
UNION ALL SELECT subject, 'its metric', asset FROM main.place_metric(dataset_name)
UNION ALL SELECT subject, 'its source systems', asset FROM main.place_systems(dataset_name)
UNION ALL SELECT subject, 'the dataset', asset FROM main.place_dataset(dataset_name)
UNION ALL SELECT subject, 'the dataset''s metrics', asset FROM main.place_metrics(dataset_name)
UNION ALL SELECT subject, 'its dataset', asset FROM main.place_its_dataset(dataset_name);

-- view_reads(dataset): the tables and views each view of a dataset
-- reads, through the views it reads, (view, relation)
CREATE OR REPLACE MACRO main.view_reads(dataset_name) AS TABLE
WITH RECURSIVE direct AS (SELECT DISTINCT "from" AS view, "to" AS relation FROM main.lineage(dataset_name)
                          WHERE kind = 'reads' AND len(string_split("to", '.')) = 2),
walk(view, relation) AS (SELECT view, relation FROM direct
  UNION SELECT w.view, d.relation FROM walk w JOIN direct d ON d.view = w.relation)
SELECT view, relation FROM walk;

-- place_nodes(dataset): the nodes each place's walk passes, (subject,
-- where, asset, what), beside the assets places() lists: a relation's
-- definition, a view's and those of what it reads; the relationships of a
-- table, a view and what it reads, or a dataset; a table's recipe; a
-- dataset's assets; which of a dataset's views are metrics. A result that
-- read a place read these too, so a member added to the place later
-- changes a node it read.
CREATE OR REPLACE MACRO main.place_nodes(dataset_name) AS TABLE
WITH cols AS MATERIALIZED (SELECT table_name, column_name, table_type FROM glossql_columns(dataset_name)),
walked AS MATERIALIZED (
  SELECT DISTINCT dataset_name || '.' || table_name AS subject, table_type = 'VIEW' AS is_view,
         dataset_name || '.' || table_name AS relation FROM cols
  UNION SELECT view, true, relation FROM main.view_reads(dataset_name)),
plans AS (FROM main.plans_edges(dataset_name))
SELECT w.subject, u.place AS "where", w.relation AS asset, 'definition' AS what
FROM walked w, unnest(['its columns', 'its sources', 'the columns it joins on', 'its relationships',
                       'its source systems']) u(place)
WHERE w.is_view OR u.place = 'its columns'
UNION ALL SELECT subject, 'its relationships', relation, 'relationships' FROM walked
UNION ALL SELECT subject, 'its source systems', relation, 'recipe' FROM walked
UNION ALL SELECT dataset_name || '.' || table_name || '.' || column_name, 'its relationships',
                 dataset_name || '.' || table_name, 'relationships' FROM cols
UNION ALL SELECT left_side || ' ' || op || ' ' || right_side, 'its relationships', dataset_name, 'relationships'
          FROM main.relationships WHERE dataset = dataset_name
UNION ALL SELECT p."from", 'its metric', w.relation, 'definition' FROM plans p
          JOIN walked w ON (w.subject = p."from" AND w.relation = p."from") OR w.subject = p."to"
UNION ALL SELECT dataset_name, u.place, dataset_name, s.what
          FROM unnest(['its columns', 'the dataset']) u(place), unnest(['assets', 'relationships']) s(what)
-- the dataset's metrics: which views are metrics, and the definition of each
UNION ALL SELECT dataset_name, 'the dataset''s metrics', dataset_name, 'views metric'
UNION ALL SELECT dataset_name, 'the dataset''s metrics', asset, 'definition' FROM main.place_metrics(dataset_name)
          WHERE len(string_split(asset, '.')) = 2
UNION ALL SELECT dataset_name, 'its relationships', dataset_name, 'relationships';

CREATE OR REPLACE MACRO main.place(where_) AS TABLE
FROM query(CASE where_
  WHEN 'itself' THEN 'SELECT main.read_subject() AS asset'
  WHEN 'its columns' THEN 'SELECT asset FROM main.place_columns(split_part(main.read_subject(), ''.'', 1)) WHERE subject = main.read_subject()'
  WHEN 'its sources' THEN 'SELECT asset FROM main.place_sources(split_part(main.read_subject(), ''.'', 1)) WHERE subject = main.read_subject()'
  WHEN 'the columns it joins on' THEN 'SELECT asset FROM main.place_joined(split_part(main.read_subject(), ''.'', 1)) WHERE subject = main.read_subject()'
  WHEN 'its relationships' THEN 'SELECT asset FROM main.place_relationships(split_part(main.read_subject(), ''.'', 1)) WHERE subject = main.read_subject()'
  WHEN 'its metric' THEN 'SELECT asset FROM main.place_metric(split_part(main.read_subject(), ''.'', 1)) WHERE subject = main.read_subject()'
  WHEN 'its source systems' THEN 'SELECT asset FROM main.place_systems(split_part(main.read_subject(), ''.'', 1)) WHERE subject = main.read_subject()'
  WHEN 'the dataset' THEN 'SELECT asset FROM main.place_dataset(split_part(main.read_subject(), ''.'', 1))'
  WHEN 'its dataset' THEN 'SELECT split_part(main.read_subject(), ''.'', 1) AS asset'
  WHEN 'the dataset''s metrics' THEN 'SELECT asset FROM main.place_metrics(split_part(main.read_subject(), ''.'', 1))'
  ELSE 'SELECT error(''not_a_place: ' || replace(where_, '''', '''''') || ' names no place; place_list() lists them'')::VARCHAR AS asset' END);

-- tables_read(dataset): the relation whose rows each asset's data is,
-- (asset, asset_kind, table_name): a table or a view itself; a column its
-- table's or view's; a relationship those of its two sides; the dataset
-- every table. What a view reads in turn is view_reads().
CREATE OR REPLACE MACRO main.tables_read(dataset_name) AS TABLE
WITH cols AS (SELECT table_name, column_name, table_type FROM glossql_columns(dataset_name)),
rels AS (SELECT DISTINCT dataset_name || '.' || table_name AS r, CASE table_type WHEN 'VIEW' THEN 'view' ELSE 'table' END AS k
         FROM cols)
SELECT r AS asset, k AS asset_kind, r AS table_name FROM rels
UNION ALL SELECT dataset_name || '.' || table_name || '.' || column_name, 'column', dataset_name || '.' || table_name FROM cols
UNION ALL SELECT left_side || ' ' || op || ' ' || right_side, 'relationship', split_part(side, '.', 1) || '.' || split_part(side, '.', 2)
          FROM main.relationships, unnest([left_side, right_side]) u(side) WHERE dataset = dataset_name
UNION ALL SELECT dataset_name, 'dataset', r FROM rels WHERE k = 'table';

--===------------------------------------------------------------------===--
-- annotations(property, where): one row per asset of the place the
-- property is declared on, with the annotation the catalog holds,
-- collapsed: asset, property, value (JSON), state (current, stale,
-- conflict, as GLOSSARY() says; missing, and no value, where nobody
-- annotated the asset), threshold (the THRESHOLD of the test declared
-- on the property, NULL where it has none). A property declared on
-- tables returns no row for a column. A what of the form 'x-kind <kind>' names every property
-- declared with that x-kind, a scenario's among them: one row per
-- property and asset, the value a person's newest annotation, else an
-- agent's, state current, or missing.
--===------------------------------------------------------------------===--
CREATE OR REPLACE MACRO main.annotations_of(property, where_) AS TABLE
WITH grains AS (SELECT grains FROM main.aspects WHERE name = property)
SELECT p.asset, property AS property, g.value, coalesce(g.state, 'missing') AS state,
       (SELECT max(t.threshold) FROM main.tests t WHERE t.aspect = property) AS threshold
FROM main.place(where_) p
LEFT JOIN glossql_glossary(split_part(main.read_subject(), '.', 1), property, false) g ON g.subject = p.asset
WHERE EXISTS (SELECT 1 FROM grains WHERE len(grains) = 0 OR list_contains(grains, main.asset_grain(p.asset)));

CREATE OR REPLACE MACRO main.annotations_of_kind(kind, where_) AS TABLE
WITH props AS (SELECT name, grains FROM main.aspects WHERE json_extract_string(schema, '$.x-kind') = kind),
held AS (
  SELECT subject, aspect, body FROM main.glossary
  WHERE dataset = split_part(main.read_subject(), '.', 1) AND aspect IN (SELECT name FROM props) AND actor_kind IN ('human', 'agent')
  QUALIFY row_number() OVER (PARTITION BY subject, aspect ORDER BY CASE actor_kind WHEN 'human' THEN 0 ELSE 1 END, begin_snapshot DESC) = 1)
SELECT p.asset, pr.name AS property, h.body AS value, CASE WHEN h.body IS NULL THEN 'missing' ELSE 'current' END AS state,
       (SELECT max(t.threshold) FROM main.tests t WHERE t.aspect = pr.name) AS threshold
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
-- (left_column, right_column, left_unique, right_unique, join_number: the
-- pairs of one join share its number), each loaded table
-- with the source its recipe loads it from (source; NULL for a view it
-- reads or a table loaded without a recipe), and per read of a table, by
-- the branch of the union it is read in and its alias, what the view's
-- joins and filters require of that read alone (conditions: table_name,
-- branch, alias, condition, NULL where nothing is required; rendered as
-- the lineage renders a derivation). The rest of the annotation, its
-- assumptions, is no part of the definition a body reads.
-- definitions(dataset) is the same for every view of a dataset at once:
-- the views the dataset defines, never a table of the same name.
--===------------------------------------------------------------------===--
CREATE OR REPLACE MACRO main.definitions(dataset_name) AS TABLE
WITH cols AS (FROM glossql_columns(dataset_name)),
lin AS (FROM glossql_views_lineage(dataset_name)),
sc AS (SELECT s.*, l."to" AS source FROM glossql_views_scans(dataset_name) s
       LEFT JOIN main.loaded_from_edges(dataset_name) l ON l."from" = s.table_name),
jo AS (FROM glossql_views_joins(dataset_name)),
ma AS (SELECT DISTINCT view, table_name, branch, alias, condition FROM glossql_views_mapping(dataset_name) WHERE table_name IS NOT NULL),
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
       (SELECT list({'table_name': s.table_name, 'columns': s.columns, 'source': s.source} ORDER BY s.table_name)
        FROM sc s WHERE s.view = a.name) AS reads,
       (SELECT list({'left_column': j.left_column, 'right_column': j.right_column,
                     'left_unique': j.left_unique, 'right_unique': j.right_unique, 'join_number': j.join_number}
                    ORDER BY j.join_number, j.left_column, j.right_column)
        FROM jo j WHERE j.view = a.name) AS joins,
       (SELECT list({'table_name': m.table_name, 'branch': m.branch, 'alias': m.alias, 'condition': m.condition}
                    ORDER BY m.table_name, m.branch, m.alias)
        FROM ma m WHERE m.view = a.name) AS conditions
FROM main.aspects a LEFT JOIN bodies b ON b.view = a.name
WHERE a.kind = 'VIEW'
  AND (b.view IS NOT NULL OR a.name IN (SELECT table_name FROM cols WHERE table_type = 'VIEW'));

CREATE OR REPLACE MACRO main.definition(where_) AS TABLE
SELECT d.* FROM (SELECT DISTINCT asset FROM main.place(where_)) p
JOIN main.definitions(split_part(main.read_subject(), '.', 1)) d ON d.view = p.asset
ORDER BY d.view;
