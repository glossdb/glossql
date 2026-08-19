-- rate_tolerance — the validation detector: reads the authored
-- expectation and the check voice from the slots.
-- Wire it per validation aspect: the aspect's schema carries a
-- `tolerance` key (the expectation gloss — owed before the detector
-- can plan) and a `breach_rate` key (the check function's voice — the
-- VIOLATION share, 0.0 means fully passing; the key is named for its
-- polarity because a pass rate read as a breach rate bands a
-- 100%-passing check red). The witness's THRESHOLD overrides the
-- authored tolerance when set. No voice yet is yellow, never green.
-- One-sided by design — a known-dirt source that expects its own rate
-- wants a custom detector that goes red on both sides, since
-- overcleaning is also a failure.
WITH s AS (
  SELECT subject,
         max(body['breach_rate']) AS rate,
         max(body['tolerance']) AS tolerance
  FROM slots
  GROUP BY subject
)
SELECT subject,
       coalesce(rate, 0.0) AS score,
       CASE WHEN rate IS NULL THEN 'yellow'
            WHEN rate <= coalesce($threshold, tolerance, 0.0) THEN 'green'
            ELSE 'red'
       END AS band
FROM s
