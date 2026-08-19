-- Slot-disagreement detector (fixture 06): one query over the
-- witness's `slots`. Score is the fraction of extra
-- distinct `value`s across each subject's slots; band maps score
-- against the witness threshold.
WITH s AS (
  SELECT subject,
         CASE WHEN count(*) <= 1 THEN 0.0
              ELSE (count(DISTINCT body['value']) - 1.0) / (count(*) - 1.0)
         END AS score
  FROM slots
  GROUP BY subject
)
SELECT subject,
       score,
       CASE WHEN score = 0.0 THEN 'green'
            WHEN score <= coalesce($threshold, 1.0) THEN 'yellow'
            ELSE 'red'
       END AS band
FROM s
