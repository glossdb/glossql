-- Every read the dataset serves, for the Export tab: one row per
-- QUERY aspect that stands here — `metric_surfaces`, the record —
-- with the files the download door serves for `read.<name>()`. The
-- chip says kind and standing in one word: what the read is while it
-- serves (the skill's `x-kind`, a grounding without one is a metric),
-- else why it does not. A read that serves no file hides its links.
SELECT s.name,
       arrow_cast('read.' || s.name || '()', 'Utf8') AS read,
       s.title,
       s.unit,
       arrow_cast(CASE WHEN s.stopped <> '' THEN 'stopped: ' || s.stopped ELSE s.meaning END, 'Utf8') AS what,
       arrow_cast(CASE
         WHEN s.stopped <> '' THEN 'stopped'
         WHEN NOT s.grounded THEN 'no grounding'
         WHEN s.kind IN ('', 'measure', 'metric') THEN 'metric'
         ELSE s.kind END, 'Utf8') AS chip,
       arrow_cast(CASE WHEN s.stopped <> '' OR NOT s.grounded THEN 'warn' ELSE 'ok' END, 'Utf8') AS ccls,
       arrow_cast(CASE WHEN s.stopped <> '' OR NOT s.grounded THEN 'off' ELSE '' END, 'Utf8') AS fcls,
       arrow_cast('/' || d.dataset || '/app/export/read.' || s.name || '.csv', 'Utf8') AS csv,
       arrow_cast('/' || d.dataset || '/app/export/read.' || s.name || '.parquet', 'Utf8') AS parquet
FROM metric_surfaces s CROSS JOIN current_dataset d
ORDER BY CASE
    WHEN s.stopped <> '' THEN 4
    WHEN NOT s.grounded THEN 3
    WHEN s.kind IN ('', 'measure', 'metric') THEN 0
    WHEN s.kind = 'fact' THEN 1
    ELSE 2 END,
  s.name
