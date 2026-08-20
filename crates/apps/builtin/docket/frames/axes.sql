-- The cube's fact row for this metric: what it admitted and why not —
-- the resolution its cells stand at, the window, the verb, the axes,
-- the rival's outcome. The window control reads `resolution` to offer
-- only the grains the cells can answer; a metric that abstains carries
-- its reason here. Record-class: it says what the glossary's judged
-- verdicts admitted, so a ruling refreshes it.
SELECT metric, applicable,
  arrow_cast(coalesce(reason, ''), 'Utf8') AS reason,
  arrow_cast(coalesce(behavior, ''), 'Utf8') AS behavior,
  arrow_cast(coalesce(resolution, ''), 'Utf8') AS resolution,
  arrow_cast(coalesce(window, ''), 'Utf8') AS window,
  arrow_cast(coalesce(array_to_string(dims, ' · '), ''), 'Utf8') AS dims,
  arrow_cast(coalesce(array_to_string(bucketed, ' · '), ''), 'Utf8') AS bucketed,
  arrow_cast(coalesce(alternative, ''), 'Utf8') AS alternative,
  arrow_cast(coalesce(alternative_error, ''), 'Utf8') AS alternative_error
FROM metric_axes()
WHERE metric = CAST($metric AS VARCHAR)
