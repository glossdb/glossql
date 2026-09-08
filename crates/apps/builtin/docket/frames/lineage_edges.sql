-- The lineage graph's edges: every declared key edge of the bound
-- dataset, spelled as declared (a composite endpoint as
-- `table.(a, b)` — the element draws one line per member), and for
-- every current grounding the table columns its served fields descend
-- from — the cube's provenance walk as rows. `m2o` is `->`, `o2o` is
-- `<->`, `reads` is a grounding reading the column.
SELECT arrow_cast(r.left_path, 'Utf8') AS src,
       arrow_cast(r.right_path, 'Utf8') AS dst,
       arrow_cast(CASE r.op WHEN '<->' THEN 'o2o' ELSE 'm2o' END, 'Utf8') AS kind
FROM relationships r
JOIN current_dataset d ON d.dataset = r.dataset
UNION ALL
SELECT arrow_cast(source, 'Utf8') AS src,
       arrow_cast('read.' || metric || '().' || field, 'Utf8') AS dst,
       'reads' AS kind
FROM metric_sources()
WHERE field IS NOT NULL
