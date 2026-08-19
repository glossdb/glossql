-- Band-breach detector over the metric_bands slots and the witness
-- threshold. Each walked point carries its PIT — the
-- quantile where the actual landed. Displacement is |2*pit - 1|: 0.0 at
-- the median, 0.8 at a nominal-80 band edge, 0.9 at a nominal-90 edge.
-- Score is the worst displacement across the latest point of every
-- monitored metric; band maps displacement against the band edges, with
-- the witness threshold as the red line (default 0.98). A slot without
-- walked points scores 0.0 — nothing breached. Detail — which metric,
-- which month — lives in the measurement's own cached output.
WITH m AS (
  SELECT subject, unnest(body['metrics']) AS metric FROM slots
),
d AS (
  SELECT subject,
         max(abs(2.0 * metric['points'][CAST(cardinality(metric['points']) AS BIGINT)]['pit'] - 1.0)) AS worst
  FROM m
  WHERE cardinality(metric['points']) > 0
  GROUP BY subject
)
SELECT s.subject,
       coalesce(d.worst, 0.0) AS score,
       CASE WHEN coalesce(d.worst, 0.0) <= 0.8 THEN 'green'
            WHEN d.worst <= 0.9 THEN 'yellow'
            WHEN d.worst <= coalesce($threshold, 0.98) THEN 'orange'
            ELSE 'red'
       END AS band
FROM (SELECT DISTINCT subject FROM slots) s
LEFT JOIN d ON s.subject = d.subject
