-- Where a breached metric's move is: for each axis the metric is
-- sliced by, the member that moved most between the previous and the
-- newest complete walked month, with its move and the net of the
-- rest — the cube's own cells at month grain, the two months the walk
-- scored. A member absent in one of the months counts as zero there.
-- The metric is a page param the finding's frame fills in. Numbers
-- stay numbers; the row tile groups their digits.
WITH walked AS (
  SELECT period, row_number() OVER (ORDER BY point_seq DESC) AS recency
  FROM band_points()
  WHERE metric = CAST($metric AS VARCHAR) AND applicable AND partial = false AND period IS NOT NULL
),
newest AS (SELECT max(period) AS period FROM walked WHERE recency = 1),
previous AS (SELECT max(period) AS period FROM walked WHERE recency = 2),
cells AS (
  SELECT s.dimension AS axis, s.member, to_char(s.period, '%Y-%m') AS ym, s.value
  FROM metric_series(grain => 'month') s
  WHERE s.metric = CAST($metric AS VARCHAR) AND s.world = 'actual' AND s.dimension NOT IN ('', 'alternative')
),
moves AS (
  SELECT c.axis, c.member,
         max(CASE WHEN c.ym = p.period THEN c.value END) AS before,
         max(CASE WHEN c.ym = n.period THEN c.value END) AS after
  FROM cells c CROSS JOIN newest n CROSS JOIN previous p
  WHERE c.ym = n.period OR c.ym = p.period
  GROUP BY c.axis, c.member
),
ranked AS (
  SELECT axis, member, before, after,
         coalesce(after, 0) - coalesce(before, 0) AS delta,
         row_number() OVER (PARTITION BY axis ORDER BY abs(coalesce(after, 0) - coalesce(before, 0)) DESC, member) AS rank,
         count(*) OVER (PARTITION BY axis) AS members,
         sum(coalesce(after, 0) - coalesce(before, 0)) OVER (PARTITION BY axis) AS total
  FROM moves
)
SELECT arrow_cast(axis, 'Utf8') AS axis,
       arrow_cast(member, 'Utf8') AS member,
       before,
       after,
       arrow_cast(CASE WHEN delta >= 0 THEN '+' ELSE '−' END, 'Utf8') AS sign,
       abs(delta) AS moved,
       arrow_cast(CASE WHEN members - 1 = 0 THEN 'the only member'
                       WHEN members - 1 = 1 THEN 'the other one'
                       ELSE 'the other ' || CAST(members - 1 AS VARCHAR) || ' together' END, 'Utf8') AS rest,
       arrow_cast(CASE WHEN members - 1 = 0 THEN '' WHEN total - delta >= 0 THEN '+' ELSE '−' END, 'Utf8') AS rest_sign,
       CASE WHEN members - 1 = 0 THEN NULL ELSE abs(total - delta) END AS rest_moved
FROM ranked
WHERE rank = 1
ORDER BY moved DESC, axis
