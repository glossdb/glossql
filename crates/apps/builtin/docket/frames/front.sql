-- The Questions counts: what stands open for a human, straight from
-- `open_questions` — the same rows the door asks as forms, so this
-- number and the forms cannot drift apart; the rulings that stand,
-- from `ruling_entries`; and what waits on an act, from `owed`. All
-- three scope to the bound dataset. The corridor is the quality tab's.
SELECT
  (SELECT count(*) FROM open_questions q
   JOIN current_dataset d ON d.dataset = q.dataset) AS open,
  (SELECT count(*) FROM ruling_entries r
   JOIN current_dataset d ON d.dataset = r.dataset) AS ruled,
  (SELECT count(*) FROM owed) AS waiting
