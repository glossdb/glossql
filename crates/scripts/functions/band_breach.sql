-- Band-breach detector over the metric_bands slots and the witness
-- threshold. Each walked point carries its PIT — the
-- quantile where the actual landed. Displacement is |2*pit - 1|: 0.0 at
-- the median, 0.8 at a nominal-80 band edge, 0.9 at a nominal-90 edge.
-- Score is the worst displacement across the newest complete point of
-- every monitored metric — the last point, or the one before it when
-- the last is `partial` (the extract's horizon inside its month; the
-- walk marks the newest point only, so one step back is the whole
-- rule) — raised to the number of metrics scored: the worst of k
-- calibrated displacements lands under an edge e with probability
-- e^k, so the power keeps the edges' base rate at any metric count.
-- One metric at displacement 0.9 among three scores 0.729, green;
-- alone it scores 0.9, yellow. Band maps the score against the band
-- edges, with the witness threshold as the red line (default 0.98). A
-- slot without walked points, or whose newest complete points all
-- withhold their PIT, scores 0.0 — nothing breached. Detail — which
-- metric, which month — lives in the measurement's own cached output.
WITH m AS (
  SELECT subject, unnest(body['metrics']) AS metric FROM slots
),
p AS (
  SELECT subject, metric['points'] AS points,
         CAST(cardinality(metric['points']) AS BIGINT) AS n
  FROM m
  WHERE cardinality(metric['points']) > 0
),
chosen AS (
  SELECT subject,
         CASE WHEN coalesce(points[n]['partial'], false)
              THEN points[n - 1]
              ELSE points[n]
         END AS point
  FROM p
),
d AS (
  SELECT subject,
         power(max(abs(2.0 * point['pit'] - 1.0)),
               CAST(count(point['pit']) AS DOUBLE)) AS score
  FROM chosen
  GROUP BY subject
)
SELECT s.subject,
       coalesce(d.score, 0.0) AS score,
       CASE WHEN coalesce(d.score, 0.0) <= 0.8 THEN 'green'
            WHEN d.score <= 0.9 THEN 'yellow'
            WHEN d.score <= coalesce($threshold, 0.98) THEN 'orange'
            ELSE 'red'
       END AS band
FROM (SELECT DISTINCT subject FROM slots) s
LEFT JOIN d ON s.subject = d.subject
