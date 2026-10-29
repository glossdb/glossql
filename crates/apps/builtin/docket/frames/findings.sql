-- The findings after the corridor's: every verdict that is not green
-- from a witness other than the bands one, every column whose cast
-- refused cells at its table's newest landing, every defect a
-- function measured — a join that leaves rows unresolved, an identity
-- that breaks. Each is a sentence from the record's fields, the
-- numbers behind it, the metrics that read the subject, its
-- provenance and a link. Worst first: the verdicts by band, then the
-- refusals, then the measured defects, by size within each. A
-- measurement proposes and never judges: a measured defect carries no
-- hue, only a verdict does. The shipped detectors have their own
-- sentence; another is named as it is.
WITH reads AS (
  SELECT ms.source,
         array_to_string(array_agg(DISTINCT coalesce(json_get_str(a.schema, 'title'), ms.metric)), ', ') AS metrics
  FROM metric_sources() ms LEFT JOIN aspects a ON a.name = ms.metric
  WHERE ms.source IS NOT NULL
  GROUP BY ms.source
),
meanings AS (
  SELECT g.subject, json_get_str(g.body, 'value') AS meaning,
         row_number() OVER (PARTITION BY g.subject
           ORDER BY CASE g.actor_kind WHEN 'human' THEN 0 ELSE 1 END, g.written_at DESC) AS rn
  FROM glossary g JOIN current_dataset d ON d.dataset = g.dataset
  WHERE g.aspect = 'meaning'
),
verdicts AS (
  SELECT a.subject, a.aspect, a.witness, a.band, a.score, a.error, a.computed_at, w.detector, w.threshold,
         CASE a.band WHEN 'red' THEN 4 WHEN 'orange' THEN 3 WHEN 'yellow' THEN 2 ELSE 1 END AS sev
  FROM ATTEST() a JOIN witnesses w ON w.name = a.witness
  WHERE w.detector IS NOT NULL AND w.detector <> 'band_breach' AND a.band <> 'green'
),
voices AS (
  SELECT g.subject, g.aspect,
         array_to_string(array_agg(
           g.actor_kind || ': ' || coalesce(json_get_str(g.body, 'value'), CAST(g.body AS VARCHAR))
           ORDER BY CASE g.actor_kind WHEN 'human' THEN 0 WHEN 'agent' THEN 1 ELSE 2 END, g.written_at DESC), ' · ') AS said
  FROM glossary g JOIN current_dataset d ON d.dataset = g.dataset
  JOIN (SELECT DISTINCT subject, aspect FROM verdicts) v ON v.subject = g.subject AND v.aspect = g.aspect
  GROUP BY g.subject, g.aspect
),
rates AS (
  SELECT g.subject, g.aspect,
         max(CASE WHEN g.actor_kind IN ('human', 'agent') THEN json_get_str(g.body, 'outcome') END) AS outcome,
         max(CASE WHEN g.actor_kind IN ('human', 'agent') THEN json_get_float(g.body, 'tolerance') END) AS tolerance,
         max(CASE WHEN g.actor_kind NOT IN ('human', 'agent') THEN json_get_float(g.body, 'breach_rate') END) AS rate
  FROM glossary g JOIN current_dataset d ON d.dataset = g.dataset
  JOIN (SELECT DISTINCT subject, aspect FROM verdicts) v ON v.subject = g.subject AND v.aspect = g.aspect
  GROUP BY g.subject, g.aspect
),
judged AS (
  SELECT CAST(v.band AS VARCHAR) AS chip, 'warn' AS bcls,
         CASE v.detector
           WHEN 'slot_entropy' THEN 'The voices on ' || v.aspect || ' differ on ' || v.subject || '.'
           WHEN 'rate_tolerance' THEN v.subject || ' breaks what was written for ' || v.aspect || '.'
           ELSE CASE WHEN v.band = 'error' THEN v.witness || ' could not judge ' || v.subject || '.'
                     ELSE v.witness || ' reads ' || v.band || ' on ' || v.subject || '.' END END AS headline,
         CASE v.detector
           WHEN 'slot_entropy' THEN coalesce('The slots say ' || vo.said || '. ', '')
             || 'The score is the share of extra distinct values across the slots, '
             || CAST(round(v.score * 1000) / 1000 AS VARCHAR) || ' against the threshold ' || coalesce(v.threshold, 'none')
             || '. A person''s word settles it.'
           WHEN 'rate_tolerance' THEN coalesce('Written: ' || r.outcome, 'No expectation is written')
             || coalesce(', within ' || CAST(r.tolerance AS VARCHAR), '') || '. '
             || coalesce('Measured: a breach rate of ' || CAST(r.rate AS VARCHAR) || '.', 'No check has spoken.')
           ELSE CASE WHEN v.band = 'error' THEN coalesce(v.error, 'the detector failed without a word')
                     ELSE 'The detector ' || v.detector || ' scored ' || CAST(round(v.score * 1000) / 1000 AS VARCHAR)
                       || coalesce(' against the threshold ' || v.threshold, '') || '.' END END AS why,
         coalesce(rd.metrics, '') AS reads,
         v.witness || ' · ' || v.detector || coalesce(' @ ' || v.threshold, '') || ' on ' || v.aspect
           || ' · measured ' || to_char(CAST(v.computed_at AS TIMESTAMP), '%d %b %Y %H:%M') AS meta,
         '?check=' || v.witness || '&kind=' || CASE v.detector WHEN 'slot_entropy' THEN 'voices'
                                                                WHEN 'rate_tolerance' THEN 'rate'
                                                                ELSE 'verdicts' END AS link,
         'the check' AS link_text,
         CAST(v.sev AS DOUBLE) AS sev,
         CAST(v.score AS DOUBLE) AS size
  FROM verdicts v
  LEFT JOIN voices vo ON vo.subject = v.subject AND vo.aspect = v.aspect
  LEFT JOIN rates r ON r.subject = v.subject AND r.aspect = v.aspect
  LEFT JOIN reads rd ON rd.source = v.subject
),
latest AS (
  SELECT i.table_name, max(i.imported_at) AS imported_at
  FROM imports i JOIN current_dataset d ON d.dataset = i.dataset
  GROUP BY i.table_name),
newest AS (
  SELECT i.table_name, i.source_scans, i.landed_rows, i.cast_failures, i.imported_at
  FROM imports i
  JOIN latest l ON l.table_name = i.table_name AND l.imported_at = i.imported_at
  JOIN current_dataset d ON d.dataset = i.dataset),
scans AS (
  SELECT n.table_name,
         array_to_string(array_agg(
           json_get_str(json_get(n.source_scans, s.i), 'relation') || ' ('
             || CAST(json_get_int(json_get(n.source_scans, s.i), 'rows') AS VARCHAR) || ' rows)'
           ORDER BY s.i), ' joined to ') AS read
  FROM newest n CROSS JOIN generate_series(0, 19) AS s(i)
  WHERE s.i < json_length(n.source_scans)
  GROUP BY n.table_name),
checked AS (
  SELECT n.table_name, n.landed_rows, n.imported_at, json_get(json_get(n.cast_failures, 'checked'), c.i) AS chk
  FROM newest n CROSS JOIN generate_series(0, 99) AS c(i)
  WHERE c.i < json_length(n.cast_failures, 'checked')),
failed AS (
  SELECT table_name, landed_rows, imported_at,
         json_get_str(chk, 'column') AS col,
         json_get_int(chk, 'failed') AS failed,
         json_get(chk, 'tokens') AS tokens
  FROM checked
  WHERE json_get_int(chk, 'failed') > 0),
tops AS (
  SELECT f.table_name, f.col, f.failed, f.landed_rows, f.imported_at,
         array_to_string(array_agg(
           json_get_str(json_get(f.tokens, t.j), 0) || ' ×'
             || CAST(json_get_int(json_get(f.tokens, t.j), 1) AS VARCHAR)
           ORDER BY t.j), ', ') AS top
  FROM failed f CROSS JOIN generate_series(0, 7) AS t(j)
  WHERE t.j < json_length(f.tokens)
  GROUP BY f.table_name, f.col, f.failed, f.landed_rows, f.imported_at),
refusals AS (
  SELECT 'landing' AS chip, '' AS bcls,
         CAST(t.failed AS VARCHAR) || CASE WHEN t.failed = 1 THEN ' cell of ' ELSE ' cells of ' END
           || t.table_name || '.' || t.col || ' could not be read.' AS headline,
         'The newest landing of ' || t.table_name || ' read ' || coalesce(s.read, 'its source')
           || ' and landed ' || CAST(t.landed_rows AS VARCHAR) || ' rows; the cast of ' || t.col
           || ' refused ' || CAST(t.failed AS VARCHAR) || ' and landed them empty. The values it refused, most frequent first: '
           || coalesce(t.top, 'none kept') || '.'
           || coalesce(' The record says of the column: ' || m.meaning || '.', '') AS why,
         coalesce(rd.metrics, '') AS reads,
         'imports · landed ' || to_char(CAST(t.imported_at AS TIMESTAMP), '%d %b %Y %H:%M') AS meta,
         '/' || d.dataset || '/app/docket/p/glossary?table=' || t.table_name AS link,
         t.table_name AS link_text,
         0.5 AS sev,
         CAST(t.failed AS DOUBLE) AS size
  FROM tops t
  CROSS JOIN current_dataset d
  LEFT JOIN scans s ON s.table_name = t.table_name
  LEFT JOIN meanings m ON m.subject = t.table_name || '.' || t.col AND m.rn = 1
  LEFT JOIN reads rd ON rd.source = t.table_name || '.' || t.col
),
measured AS (
  SELECT g.subject, g.aspect, g.body, g.written_at
  FROM GLOSSARY(all => true) g
  WHERE g.kind = 'measurement' AND g.current
),
joins AS (
  SELECT json_get(json_get(m.body, 'relationships'), r.i) AS rel, m.written_at
  FROM measured m CROSS JOIN generate_series(0, 99) AS r(i)
  WHERE m.aspect = 'relationship_coherence' AND r.i < json_length(m.body, 'relationships')),
orphaned AS (
  SELECT json_get_str(rel, 'relationship') AS rel,
         json_get_int(rel, 'orphans') AS orphans,
         json_get_int(rel, 'filled') AS filled,
         written_at
  FROM joins
  WHERE json_get_int(rel, 'orphans') > 0),
orphans AS (
  SELECT 'measured' AS chip, '' AS bcls,
         CAST(o.orphans AS VARCHAR) || ' of ' || CAST(o.filled AS VARCHAR) || ' ' || split_part(o.rel, '.', 1)
           || ' rows find no ' || split_part(split_part(o.rel, ' -> ', 2), '.', 1) || '.' AS headline,
         'The join ' || o.rel || ' leaves them unresolved.'
           || coalesce(' The record says of the join: ' || m.meaning || '.', '') AS why,
         coalesce(rd.metrics, '') AS reads,
         'relationship_coherence · measured ' || to_char(CAST(o.written_at AS TIMESTAMP), '%d %b %Y %H:%M') AS meta,
         '/' || d.dataset || '/app/docket/p/glossary?table=' || split_part(o.rel, '.', 1) AS link,
         split_part(o.rel, '.', 1) AS link_text,
         0.4 AS sev,
         CAST(o.orphans AS DOUBLE) AS size
  FROM orphaned o
  CROSS JOIN current_dataset d
  LEFT JOIN meanings m ON m.subject = o.rel AND m.rn = 1
  LEFT JOIN reads rd ON rd.source = split_part(o.rel, ' -> ', 1)
),
identities AS (
  SELECT m.subject, json_get(json_get(m.body, 'candidates'), c.i) AS cand, m.written_at
  FROM measured m CROSS JOIN generate_series(0, 49) AS c(i)
  WHERE m.aspect = 'derivation_candidates' AND c.i < json_length(m.body, 'candidates')),
broken AS (
  SELECT subject,
         json_get_str(cand, 'target') AS target,
         json_get_str(cand, 'form') AS form,
         json_get_str(json_get(cand, 'operands'), 0) AS op1,
         json_get_str(json_get(cand, 'operands'), 1) AS op2,
         json_get_int(cand, 'support') AS support,
         json_get_int(cand, 'violations') AS violations,
         written_at
  FROM identities
  WHERE json_get_int(cand, 'violations') > 0),
breaks AS (
  SELECT 'measured' AS chip, '' AS bcls,
         CAST(b.violations AS VARCHAR) || ' ' || b.subject || ' rows break ' || b.target || ' = ' || b.op1
           || CASE b.form WHEN 'product' THEN ' × ' ELSE ' + ' END || b.op2 || '.' AS headline,
         'The identity holds on ' || CAST(b.support AS VARCHAR) || ' of ' || CAST(b.support + b.violations AS VARCHAR)
           || ' rows and breaks on ' || CAST(b.violations AS VARCHAR) || '.' AS why,
         coalesce(rd.metrics, '') AS reads,
         'derivation_candidates · measured ' || to_char(CAST(b.written_at AS TIMESTAMP), '%d %b %Y %H:%M') AS meta,
         '/' || d.dataset || '/app/docket/p/glossary?table=' || b.subject AS link,
         b.subject AS link_text,
         0.4 AS sev,
         CAST(b.violations AS DOUBLE) AS size
  FROM broken b
  CROSS JOIN current_dataset d
  LEFT JOIN reads rd ON rd.source = b.subject || '.' || b.target
),
every AS (
  SELECT * FROM judged
  UNION ALL SELECT * FROM refusals
  UNION ALL SELECT * FROM orphans
  UNION ALL SELECT * FROM breaks
)
SELECT arrow_cast(chip, 'Utf8') AS chip,
       arrow_cast(bcls, 'Utf8') AS bcls,
       arrow_cast(headline, 'Utf8') AS headline,
       arrow_cast(why, 'Utf8') AS why,
       arrow_cast(CASE WHEN reads = '' THEN '' ELSE 'Read by ' || reads || '.' END, 'Utf8') AS reads,
       arrow_cast(meta, 'Utf8') AS meta,
       arrow_cast(link, 'Utf8') AS link,
       arrow_cast(link_text, 'Utf8') AS link_text,
       sev,
       size
FROM every
ORDER BY sev DESC, size DESC, headline
