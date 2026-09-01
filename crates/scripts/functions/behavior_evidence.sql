-- Behavior evidence: the stock/flow discriminator from v0.3's lineage
-- reconcile, reshaped as an evidence measurement — the judge reads it
-- before glossing `behavior`; it is never a voice in the behavior
-- slots. Runs at column grain
-- (`SELECT behavior_evidence() FROM trial_balance.debit_balance`).
-- The discovery, the policy and the arithmetic live in the
-- behavior_anchors door and the reconcile kernel behind the runtime
-- seam; since the port a composite (tuple) endpoint takes part like
-- any other — every leg an identifier, the entity key the tuple —
-- where the script skipped them (§7a). Extraction serves the summary
-- alone (run 4 spent 60KB of an agent's context — 102 anchors — to
-- learn the word "flow"); every anchor reads back via GLOSSARY when
-- the judge wants to see what the losers said.
WITH be AS (SELECT * FROM behavior_anchors($subject)),
be_a AS (
  SELECT coalesce(array_agg(named_struct(
    'event', event, 'align', align,
    'measure_time', measure_time, 'event_time', event_time,
    'grain', grain, 'scope', scope,
    'entities', entities, 'viable_entities', viable_entities,
    'identifier_columns', identifier_columns,
    'verdict', verdict, 'convention', convention, 'voted', voted,
    'agreement', agreement, 'support', support,
    'r_flow', r_flow, 'r_stock', r_stock,
    'sign', CASE WHEN sign_primary IS NOT NULL THEN named_struct(
      'primary', sign_primary, 'mirror', sign_mirror, 'both', sign_both) END,
    'reason', reason,
    'alternatives', alternatives
  ) ORDER BY seq) FILTER (WHERE event IS NOT NULL), []) AS anchors
  FROM be
),
be_f AS (SELECT * FROM be WHERE event IS NULL)
SELECT
  f.applicable,
  f.reason AS reason,
  CASE WHEN f.applicable THEN a.anchors END AS anchors,
  CASE WHEN f.applicable THEN named_struct(
    'anchors', f.anchors_n, 'decided', f.decided, 'event', f.s_event,
    'verdict', f.s_verdict, 'support', f.s_support, 'voted', f.s_voted,
    'convention', f.s_convention, 'align', f.s_align, 'scope', f.s_scope,
    'r_flow', f.s_r_flow, 'r_stock', f.s_r_stock,
    'sign', CASE WHEN f.s_sign_primary IS NOT NULL THEN named_struct(
      'primary', f.s_sign_primary, 'mirror', f.s_sign_mirror, 'both', f.s_sign_both) END,
    'tiebreak', f.s_tiebreak,
    'reason', f.s_reason,
    'note', 'cached — every anchor reads back via GLOSSARY(table.column::behavior_evidence)'
  ) END AS summary
FROM be_f f CROSS JOIN be_a a
