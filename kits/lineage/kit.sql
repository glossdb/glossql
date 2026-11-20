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
--             its SQL names. The edges of each view's stored lineage
--             (view_edges())
--   relates   relationship -> each of its columns
--   loaded from  table -> the source its recipe loads it from, a source
--             named as it is declared
--   and the edges of each kind of asset a kit declares
--             (glossql_kind_edges(): its lineage_edges_<kind> macro)
-- Each part is a read over the catalog's rows: a view's lineage is the
-- result of `lineage` on the view, stored when the view is defined and
-- computed again, as any result, once a node it read changed.
--===------------------------------------------------------------------===--
-- contains_edges(dataset): the lineage's contains edges alone
CREATE OR REPLACE MACRO main.contains_edges(dataset_name) AS TABLE
WITH cols AS (SELECT table_name, column_name FROM glossql_columns(dataset_name))
SELECT DISTINCT dataset_name AS "from", dataset_name || '.' || table_name AS "to", 'contains' AS kind FROM cols
UNION ALL SELECT dataset_name || '.' || table_name, dataset_name || '.' || table_name || '.' || column_name, 'contains' FROM cols
UNION ALL SELECT dataset_name, left_side || ' ' || op || ' ' || right_side, 'contains' FROM main.relationships WHERE dataset = dataset_name;

-- relates_edges(dataset): the lineage's relates edges alone; a side is
-- table.column or table.(a, b): one edge per column
CREATE OR REPLACE MACRO main.relates_edges(dataset_name) AS TABLE
WITH rels AS (
  SELECT left_side, right_side, left_side || ' ' || op || ' ' || right_side AS rel
  FROM main.relationships WHERE dataset = dataset_name),
sides AS (
  SELECT rel, regexp_extract(side, '^([^.]+\.[^.]+)\.', 1) AS tbl,
         unnest(string_split(regexp_replace(regexp_replace(side, '^[^.]+\.[^.]+\.', ''), '^\((.*)\)$', '\1'), ', ')) AS col
  FROM (SELECT rel, left_side AS side FROM rels UNION ALL SELECT rel, right_side FROM rels))
SELECT DISTINCT rel AS "from", tbl || '.' || col AS "to", 'relates' AS kind FROM sides;

-- view_edges(dataset): the edges the stored lineage of each view of a
-- dataset holds, view -> each table, column and view it reads
CREATE OR REPLACE MACRO main.view_edges(dataset_name) AS TABLE
SELECT "from", "to", kind FROM main.edges WHERE split_part("from", '.', 1) = dataset_name;

-- view_bodies(dataset): the annotation that defines each view of a
-- dataset, (view, body), the newest on the dataset
CREATE OR REPLACE MACRO main.view_bodies(dataset_name) AS TABLE
SELECT property AS view, body FROM main.glossary
WHERE dataset = dataset_name AND subject = dataset_name AND property IN (SELECT name FROM main.properties WHERE kind = 'VIEW')
QUALIFY row_number() OVER (PARTITION BY property ORDER BY begin_snapshot DESC) = 1;

-- loaded_from_edges(dataset): the lineage's loaded from edges alone,
-- table -> source, for each loaded table of the dataset a recipe loads
CREATE OR REPLACE MACRO main.loaded_from_edges(dataset_name) AS TABLE
SELECT dataset_name || '.' || r.table_name AS "from", r.source AS "to", 'loaded from' AS kind
FROM main.recipes r
WHERE r.dataset = dataset_name
  AND r.table_name IN (SELECT DISTINCT table_name FROM glossql_columns(dataset_name) WHERE table_type = 'BASE TABLE');

CREATE OR REPLACE MACRO main.lineage(dataset_name) AS TABLE
SELECT * FROM main.contains_edges(dataset_name)
UNION ALL SELECT * FROM main.relates_edges(dataset_name)
UNION ALL SELECT * FROM main.view_edges(dataset_name)
UNION ALL SELECT * FROM main.loaded_from_edges(dataset_name)
UNION ALL SELECT "from", "to", kind FROM glossql_kind_edges(dataset_name);

--===------------------------------------------------------------------===--
-- A view's stored lineage, read whole for a dataset (NULL: every dataset),
-- one row per view, view the view's name:
--   views_lineage   per output column: column, derivation (what computes
--                   it, each scanned column as scan.column:type), complete
--                   (false where the walk could not follow a step), shape
--                   (ratio, average or distinct, where that decides how it
--                   adds up); a view that does not plan has one row, no
--                   column, complete false
--   views_scans     per table it scans, joins and filters included:
--                   table_name, columns (what it reads of it)
--   views_joins     per pair of scanned columns a join or a WHERE equates,
--                   as schema.table.column: left_column (the predicate's
--                   left input), right_column, left_unique and right_unique
--                   (that side joined one row per value: grouped by the
--                   join columns, DISTINCT on them, or filtered to
--                   row_number() = 1 over them), join_number (the pairs of
--                   one join, its key, share it)
--   views_mapping   per output column, branch of the union its rows come
--                   from (1 where it has none) and scanned column: the
--                   branch's derivation, where a table read more than once
--                   carries each scan's alias as scan[alias].column;
--                   table_name and table_column; alias; condition (what
--                   the view's joins and filters require of that scan
--                   alone); lookup (the equalities of the join that joins
--                   that scan to the rows before it); literals (what the
--                   derivation tests or returns, as written); complete; a
--                   view that does not plan has one row, complete false
--===------------------------------------------------------------------===--
CREATE OR REPLACE MACRO main.lineage_values(dataset_name) AS TABLE
SELECT dataset, split_part(subject, '.', 2) AS view, value, error FROM main.results
WHERE function = 'lineage' AND (dataset_name IS NULL OR dataset = dataset_name);

CREATE OR REPLACE MACRO main.views_lineage(dataset_name) AS TABLE
WITH l AS (FROM main.lineage_values(dataset_name))
SELECT l.dataset, l.view, u.c.name AS "column", u.c.derivation, (l.value->>'complete')::BOOLEAN AS complete, u.c.shape
FROM l, unnest(from_json(l.value->'columns', '[{"name": "VARCHAR", "derivation": "VARCHAR", "shape": "VARCHAR"}]')) u(c)
UNION ALL SELECT dataset, view, NULL, NULL, false, NULL FROM l
WHERE value IS NULL OR json_array_length(value->'columns') = 0;

CREATE OR REPLACE MACRO main.views_scans(dataset_name) AS TABLE
SELECT l.dataset, l.view, u.s.table_name, u.s.columns
FROM main.lineage_values(dataset_name) l,
     unnest(from_json(l.value->'scans', '[{"table_name": "VARCHAR", "columns": ["VARCHAR"]}]')) u(s);

CREATE OR REPLACE MACRO main.views_joins(dataset_name) AS TABLE
SELECT l.dataset, l.view, u.j.left_column, u.j.right_column, u.j.left_unique, u.j.right_unique, u.j.join_number
FROM main.lineage_values(dataset_name) l,
     unnest(from_json(l.value->'joins', '[{"left_column": "VARCHAR", "right_column": "VARCHAR", "left_unique": "BOOLEAN",
                                         "right_unique": "BOOLEAN", "join_number": "INTEGER"}]')) u(j);

CREATE OR REPLACE MACRO main.views_mapping(dataset_name) AS TABLE
WITH l AS (FROM main.lineage_values(dataset_name))
SELECT l.dataset, l.view, u.m."column", u.m.branch, u.m.derivation, u.m.table_name, u.m.table_column, u.m.alias,
       u.m.condition, u.m.lookup, u.m.literals, u.m.complete
FROM l, unnest(from_json(l.value->'mapping', '[{"column": "VARCHAR", "branch": "INTEGER", "derivation": "VARCHAR",
                                              "table_name": "VARCHAR", "table_column": "VARCHAR", "alias": "VARCHAR",
                                              "condition": "VARCHAR", "lookup": "VARCHAR", "literals": ["VARCHAR"],
                                              "complete": "BOOLEAN"}]')) u(m)
UNION ALL SELECT dataset, view, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, false FROM l
WHERE value IS NULL OR json_array_length(value->'mapping') = 0;

-- The class of an asset by its path: what a property's grains name. A bare
-- name is a dataset, or a source where no dataset carries it. An asset of a
-- kind a kit declares reads by its path; the store admits an annotation on
-- it by its kind
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
--   'its sources'              the tables, columns and views a view reads;
--                              a view column's: the columns it is
--                              computed from
--   'the columns it joins on'  the two columns of each join in a view
--   'its relationships'        a view's: those between the tables it reads
--                              and those that touch the view; a table's or
--                              a column's: those that touch it;
--                              a dataset's: all; a relationship: itself
--   'its source systems'       a table's: the source it is loaded from; a
--                              view's: those of the tables it reads
--   'its dataset'              the subject's dataset itself
--   'the dataset'              the subject's dataset and every asset in it
--   'the dataset's metrics'    the views of the subject's dataset that are
--                              metrics (no relation or single value), and
--                              their columns
--   'the dataset's single values'  the views of the subject's dataset that
--                              are single values (x-kind scalar), and their
--                              columns
-- and the places of each kind of asset a kit declares
-- (glossql_kind_places(): its lineage_places_<kind> macro; a row with no
-- subject names a place it adds). Any other where is refused, not_a_place.
--===------------------------------------------------------------------===--
-- place_list(): the places, one entry each: the where, and whether it is
-- anchored, the same for every subject of the dataset. The engine reads the
-- places from here.
CREATE OR REPLACE MACRO main.place_list() AS list_concat([
  {'where': 'itself', 'anchored': false},
  {'where': 'its columns', 'anchored': false},
  {'where': 'its sources', 'anchored': false},
  {'where': 'the columns it joins on', 'anchored': false},
  {'where': 'its relationships', 'anchored': false},
  {'where': 'its source systems', 'anchored': false},
  {'where': 'its dataset', 'anchored': true},
  {'where': 'the dataset''s metrics', 'anchored': true},
  {'where': 'the dataset''s single values', 'anchored': true},
  {'where': 'the dataset', 'anchored': true}],
  coalesce((SELECT list(DISTINCT {'where': "where", 'anchored': false}) FROM glossql_kind_places(NULL) WHERE subject IS NULL), []));

-- Each where is a macro of its own over the whole dataset, place_<where>
-- (dataset): (subject, asset) for every subject at once, so that the
-- engine evaluates the reads of every stored result in one pass
-- (places()); 'the dataset' is anchored at the dataset. place() plans the
-- one its constant names (query()) for the running function's subject.
CREATE OR REPLACE MACRO main.place_columns(dataset_name) AS TABLE
SELECT "from" AS subject, "to" AS asset FROM main.contains_edges(dataset_name);

CREATE OR REPLACE MACRO main.place_sources(dataset_name) AS TABLE
SELECT "from" AS subject, "to" AS asset FROM main.view_edges(dataset_name) WHERE kind = 'reads'
UNION SELECT dataset_name || '.' || l.view || '.' || l."column", u.path
FROM main.views_lineage(dataset_name) l,
     unnest(regexp_extract_all(coalesce(l.derivation, ''), '([A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*):', 1)) u(path)
WHERE l."column" IS NOT NULL;

CREATE OR REPLACE MACRO main.place_joined(dataset_name) AS TABLE
SELECT DISTINCT dataset_name || '.' || j.view AS subject, side AS asset
FROM main.views_joins(dataset_name) j, unnest([j.left_column, j.right_column]) u(side);

CREATE OR REPLACE MACRO main.place_relationships(dataset_name) AS TABLE
WITH walked AS MATERIALIZED (
  SELECT * FROM main.contains_edges(dataset_name) UNION ALL SELECT * FROM main.relates_edges(dataset_name)
  UNION ALL SELECT * FROM main.view_edges(dataset_name) WHERE kind = 'reads'),
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

CREATE OR REPLACE MACRO main.place_systems(dataset_name) AS TABLE
WITH loaded AS (SELECT "from" AS t, "to" AS source FROM main.loaded_from_edges(dataset_name))
SELECT t AS subject, source AS asset FROM loaded
UNION SELECT dataset_name || '.' || s.view, l.source
      FROM main.views_scans(dataset_name) s JOIN loaded l ON l.t = s.table_name;

CREATE OR REPLACE MACRO main.place_its_dataset(dataset_name) AS TABLE
SELECT dataset_name AS subject, dataset_name AS asset;

CREATE OR REPLACE MACRO main.place_dataset(dataset_name) AS TABLE
SELECT dataset_name AS subject, dataset_name AS asset
UNION SELECT dataset_name, "to" FROM main.contains_edges(dataset_name);

-- place_views_of(dataset, kind): the dataset's views of one x-kind ('metric' where the property names
-- none) and their columns
CREATE OR REPLACE MACRO main.place_views_of(dataset_name, x_kind) AS TABLE
WITH members AS (
  SELECT dataset_name || '.' || name AS m FROM main.properties
  WHERE kind = 'VIEW' AND coalesce(json_extract_string(schema, '$.x-kind'), 'metric') = x_kind
    AND name IN (SELECT DISTINCT table_name FROM glossql_columns(dataset_name) WHERE table_type = 'VIEW'))
SELECT DISTINCT dataset_name AS subject, "to" AS asset FROM main.contains_edges(dataset_name)
WHERE "to" IN (SELECT m FROM members) OR "from" IN (SELECT m FROM members);

CREATE OR REPLACE MACRO main.place_metrics(dataset_name) AS TABLE FROM main.place_views_of(dataset_name, 'metric');

CREATE OR REPLACE MACRO main.place_single_values(dataset_name) AS TABLE FROM main.place_views_of(dataset_name, 'scalar');

-- places(dataset): every place of every subject of a dataset, (subject,
-- where, asset); 'itself' is the subject and has no row here
CREATE OR REPLACE MACRO main.places(dataset_name) AS TABLE
SELECT subject, 'its columns' AS "where", asset FROM main.place_columns(dataset_name)
UNION ALL SELECT subject, 'its sources', asset FROM main.place_sources(dataset_name)
UNION ALL SELECT subject, 'the columns it joins on', asset FROM main.place_joined(dataset_name)
UNION ALL SELECT subject, 'its relationships', asset FROM main.place_relationships(dataset_name)
UNION ALL SELECT subject, 'its source systems', asset FROM main.place_systems(dataset_name)
UNION ALL SELECT subject, 'the dataset', asset FROM main.place_dataset(dataset_name)
UNION ALL SELECT subject, 'the dataset''s metrics', asset FROM main.place_metrics(dataset_name)
UNION ALL SELECT subject, 'the dataset''s single values', asset FROM main.place_single_values(dataset_name)
UNION ALL SELECT subject, 'its dataset', asset FROM main.place_its_dataset(dataset_name)
UNION ALL SELECT subject, "where", asset FROM glossql_kind_places(dataset_name) WHERE subject IS NOT NULL;

-- view_reads(dataset): the tables and views each view of a dataset
-- reads, through the views it reads, (view, relation): a walk over the
-- stored lineage of each view on the way
CREATE OR REPLACE MACRO main.view_reads(dataset_name) AS TABLE
WITH RECURSIVE direct AS (SELECT DISTINCT "from" AS view, "to" AS relation FROM main.view_edges(dataset_name)
                          WHERE kind = 'reads' AND len(string_split("to", '.')) = 2),
walk(view, relation) AS (SELECT view, relation FROM direct
  UNION SELECT w.view, d.relation FROM walk w JOIN direct d ON d.view = w.relation)
SELECT view, relation FROM walk;

-- place_nodes(dataset): the nodes each place's walk passes, (subject,
-- where, asset, what), beside the assets places() lists: a table's
-- definition; a view's stored lineage, and that of each view its walk
-- reads through; the relationships of a table, a view and what it reads,
-- or a dataset; a table's recipe; a dataset's assets; which of a
-- dataset's views are metrics, and which are single values. A result that read a place read these too,
-- so a member added to the place later changes a node it read. A view's
-- lineage computed again with the same value leaves the result current.
CREATE OR REPLACE MACRO main.place_nodes(dataset_name) AS TABLE
WITH cols AS MATERIALIZED (SELECT table_name, column_name, table_type FROM glossql_columns(dataset_name)),
rels AS MATERIALIZED (
  SELECT DISTINCT dataset_name || '.' || table_name AS relation,
         CASE table_type WHEN 'VIEW' THEN 'result lineage' ELSE 'definition' END AS what FROM cols),
vr AS MATERIALIZED (FROM main.view_reads(dataset_name)),
walked AS MATERIALIZED (SELECT relation AS subject, relation FROM rels UNION SELECT view, relation FROM vr)
SELECT r.relation AS subject, u.place AS "where", r.relation AS asset, r.what
FROM rels r, unnest(['its columns', 'its sources', 'the columns it joins on', 'its relationships',
                     'its source systems']) u(place)
WHERE r.what = 'result lineage' OR u.place = 'its columns'
UNION ALL SELECT v.view, 'its relationships', v.relation, 'result lineage' FROM vr v
          JOIN rels r ON r.relation = v.relation AND r.what = 'result lineage'
UNION ALL SELECT subject, 'its relationships', relation, 'relationships' FROM walked
UNION ALL SELECT subject, 'its source systems', relation, 'recipe' FROM walked
UNION ALL SELECT dataset_name || '.' || table_name || '.' || column_name, 'its relationships',
                 dataset_name || '.' || table_name, 'relationships' FROM cols
UNION ALL SELECT dataset_name || '.' || table_name || '.' || column_name, 'its sources',
                 dataset_name || '.' || table_name, 'result lineage' FROM cols WHERE table_type = 'VIEW'
UNION ALL SELECT left_side || ' ' || op || ' ' || right_side, 'its relationships', dataset_name, 'relationships'
          FROM main.relationships WHERE dataset = dataset_name
-- a kit's place: its subject's own node, and of each relation among its assets a table's definition, a
-- view's stored lineage
UNION ALL SELECT k.subject, k."where", a.node_asset, a.node_what FROM glossql_kind_places(dataset_name) k
          JOIN glossql_kind_assets(dataset_name) a ON a.asset = k.subject
UNION ALL SELECT DISTINCT k.subject, k."where", r.relation, r.what FROM glossql_kind_places(dataset_name) k
          JOIN rels r ON r.relation = split_part(k.asset, '.', 1) || '.' || split_part(k.asset, '.', 2)
UNION ALL SELECT dataset_name, u.place, dataset_name, s.what
          FROM unnest(['its columns', 'the dataset']) u(place), unnest(['assets', 'relationships']) s(what)
-- the dataset's metrics: which views are metrics, and the definition of each
UNION ALL SELECT dataset_name, 'the dataset''s metrics', dataset_name, 'views metric'
UNION ALL SELECT dataset_name, 'the dataset''s metrics', asset, 'definition' FROM main.place_metrics(dataset_name)
          WHERE len(string_split(asset, '.')) = 2
-- the dataset's single values: which views are single values, and the definition of each
UNION ALL SELECT dataset_name, 'the dataset''s single values', dataset_name, 'views scalar'
UNION ALL SELECT dataset_name, 'the dataset''s single values', asset, 'definition' FROM main.place_single_values(dataset_name)
          WHERE len(string_split(asset, '.')) = 2
UNION ALL SELECT dataset_name, 'its relationships', dataset_name, 'relationships';

CREATE OR REPLACE MACRO main.place(where_) AS TABLE
FROM query(CASE where_
  WHEN 'itself' THEN 'SELECT main.read_subject() AS asset'
  WHEN 'its columns' THEN 'SELECT asset FROM main.place_columns(split_part(main.read_subject(), ''.'', 1)) WHERE subject = main.read_subject()'
  WHEN 'its sources' THEN 'SELECT asset FROM main.place_sources(split_part(main.read_subject(), ''.'', 1)) WHERE subject = main.read_subject()'
  WHEN 'the columns it joins on' THEN 'SELECT asset FROM main.place_joined(split_part(main.read_subject(), ''.'', 1)) WHERE subject = main.read_subject()'
  WHEN 'its relationships' THEN 'SELECT asset FROM main.place_relationships(split_part(main.read_subject(), ''.'', 1)) WHERE subject = main.read_subject()'
  WHEN 'its source systems' THEN 'SELECT asset FROM main.place_systems(split_part(main.read_subject(), ''.'', 1)) WHERE subject = main.read_subject()'
  WHEN 'the dataset' THEN 'SELECT asset FROM main.place_dataset(split_part(main.read_subject(), ''.'', 1))'
  WHEN 'its dataset' THEN 'SELECT split_part(main.read_subject(), ''.'', 1) AS asset'
  WHEN 'the dataset''s metrics' THEN 'SELECT asset FROM main.place_metrics(split_part(main.read_subject(), ''.'', 1))'
  WHEN 'the dataset''s single values' THEN 'SELECT asset FROM main.place_single_values(split_part(main.read_subject(), ''.'', 1))'
  ELSE 'SELECT asset FROM (SELECT *, bool_or(subject IS NULL AND "where" = ''' || replace(where_, '''', '''''')
       || ''') OVER () AS known FROM glossql_kind_places(split_part(main.read_subject(), ''.'', 1))) WHERE CASE WHEN known THEN "where" = '''
       || replace(where_, '''', '''''') || ''' AND subject = main.read_subject() ELSE error(''not_a_place: '
       || replace(replace(where_, '''', ''''''), '''', '''''') || ' names no place; place_list() lists them'') END' END);

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
-- declared with that x-kind: one row per
-- property and asset, the value a person's newest annotation, else an
-- agent's, state current, or missing; on an asset of a kind a kit declares
-- by an annotation, that annotation alone.
--===------------------------------------------------------------------===--
CREATE OR REPLACE MACRO main.annotations_of(property, where_) AS TABLE
WITH grains AS (SELECT grains FROM main.properties WHERE name = property)
SELECT p.asset, property AS property, g.value, coalesce(g.state, 'missing') AS state,
       (SELECT max(t.threshold) FROM main.tests t WHERE t.property = property) AS threshold
FROM main.place(where_) p
LEFT JOIN glossql_glossary(split_part(main.read_subject(), '.', 1), property, false) g ON g.subject = p.asset
WHERE EXISTS (SELECT 1 FROM grains WHERE len(grains) = 0 OR list_contains(grains, main.asset_grain(p.asset)));

CREATE OR REPLACE MACRO main.annotations_of_kind(kind, where_) AS TABLE
WITH props AS (SELECT name, grains FROM main.properties WHERE json_extract_string(schema, '$.x-kind') = kind),
held AS (
  SELECT subject, property, body FROM main.glossary
  WHERE dataset = split_part(main.read_subject(), '.', 1) AND property IN (SELECT name FROM props) AND actor_kind IN ('human', 'agent')
  QUALIFY row_number() OVER (PARTITION BY subject, property ORDER BY CASE actor_kind WHEN 'human' THEN 0 ELSE 1 END, begin_snapshot DESC) = 1)
SELECT p.asset, pr.name AS property, h.body AS value, CASE WHEN h.body IS NULL THEN 'missing' ELSE 'current' END AS state,
       (SELECT max(t.threshold) FROM main.tests t WHERE t.property = pr.name) AS threshold
FROM main.place(where_) p
-- an asset of a kind a kit declares by an annotation: that annotation is its own
LEFT JOIN glossql_kind_assets(split_part(main.read_subject(), '.', 1), kind) k
  ON k.asset = p.asset AND starts_with(k.node_what, 'annotation ')
JOIN props pr ON CASE WHEN k.asset IS NOT NULL THEN pr.name = substr(k.node_what, 12)
                      ELSE len(pr.grains) = 0 OR list_contains(pr.grains, main.asset_grain(p.asset)) END
LEFT JOIN held h ON h.subject = coalesce(k.node_asset, p.asset) AND h.property = pr.name;

CREATE OR REPLACE MACRO main.annotations(property, where_) AS TABLE
FROM query(CASE WHEN starts_with(property, 'x-kind ')
                THEN 'FROM main.annotations_of_kind(''' || replace(substr(property, 8), '''', '''''') || ''', ''' || replace(where_, '''', '''''') || ''')'
                ELSE 'FROM main.annotations_of(''' || replace(property, '''', '''''') || ''', ''' || replace(where_, '''', '''''') || ''')' END);

--===------------------------------------------------------------------===--
-- results(function, where): one row per asset of the place, with the
-- result of the function stored on it: asset, function, value (JSON),
-- error; no value and no error where none is stored; history, each value
-- the result held, oldest first, once per change of its value, the newest
-- last (a failure holds none). The history changes when the result does,
-- so it is the same node.
--===------------------------------------------------------------------===--
CREATE OR REPLACE MACRO main.results(function_, where_) AS TABLE
WITH held AS (
  SELECT subject, list(value ORDER BY changed_at) AS history
  FROM (SELECT DISTINCT subject, changed_at, value FROM main.results_history
        WHERE dataset = split_part(main.read_subject(), '.', 1) AND function = function_ AND value IS NOT NULL)
  GROUP BY subject)
SELECT p.asset, function_ AS function, m.value, m.error, h.history
FROM main.place(where_) p
LEFT JOIN main.results m
  ON m.dataset = split_part(main.read_subject(), '.', 1) AND m.function = function_ AND m.subject = p.asset
LEFT JOIN held h ON h.subject = p.asset;

--===------------------------------------------------------------------===--
-- definition(where): one row per view of the place, its definition: view
-- (its path), kind (the property's x-kind), schema (the property's, where a
-- kit reads its own keys), the SQL of
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
WITH lv AS (SELECT split_part(subject, '.', 2) AS view, value FROM main.results
            WHERE dataset = dataset_name AND function = 'lineage'),
loaded AS (FROM main.loaded_from_edges(dataset_name)),
bodies AS (FROM main.view_bodies(dataset_name))
SELECT dataset_name || '.' || a.name AS view,
       json_extract_string(a.schema, '$.x-kind') AS kind,
       a.schema::JSON AS schema,
       json_extract_string(b.body, '$.sql') AS sql,
       json_extract_string(b.body, '$.stopped') AS stopped,
       list_transform(from_json(l.value->'columns', '[{"name": "VARCHAR", "derivation": "VARCHAR", "shape": "VARCHAR"}]'),
                      lambda c, i: {'name': c.name, 'position': i, 'derivation': c.derivation, 'shape': c.shape}) AS columns,
       (SELECT list({'table_name': s.table_name, 'columns': s.columns, 'source': r."to"} ORDER BY s.table_name)
        FROM unnest(from_json(l.value->'scans', '[{"table_name": "VARCHAR", "columns": ["VARCHAR"]}]')) u(s)
        LEFT JOIN loaded r ON r."from" = s.table_name) AS reads,
       CASE WHEN json_array_length(l.value->'joins') > 0
            THEN from_json(l.value->'joins', '[{"left_column": "VARCHAR", "right_column": "VARCHAR", "left_unique": "BOOLEAN",
                                              "right_unique": "BOOLEAN", "join_number": "INTEGER"}]') END AS joins,
       CASE WHEN json_array_length(l.value->'conditions') > 0
            THEN from_json(l.value->'conditions', '[{"table_name": "VARCHAR", "branch": "INTEGER", "alias": "VARCHAR",
                                                   "condition": "VARCHAR"}]') END AS conditions
FROM main.properties a LEFT JOIN bodies b ON b.view = a.name LEFT JOIN lv l ON l.view = a.name
WHERE a.kind = 'VIEW'
  AND (b.view IS NOT NULL OR a.name IN (SELECT table_name FROM glossql_columns(dataset_name) WHERE table_type = 'VIEW'));

CREATE OR REPLACE MACRO main.definition(where_) AS TABLE
SELECT d.* FROM (SELECT DISTINCT asset FROM main.place(where_)) p
JOIN main.definitions(split_part(main.read_subject(), '.', 1)) d ON d.view = p.asset
ORDER BY d.view;
