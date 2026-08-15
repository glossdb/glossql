-- open_questions — what still stands open for a human to judge.
--
-- The one derivation. The door's round serves these rows as questions,
-- the app's docket renders them, and the skills name this read instead
-- of describing it again; before this file existed the same CTEs lived
-- three times, in three languages.
--
-- Derived from `agent_assumptions` — the agent's CURRENT body, never a
-- frozen copy (the 2026-08-14 run: deriving from the winning human
-- slot re-asked every answered question, because the copy kept the
-- stale confidences).
-- Four gates beyond "below full confidence": the aspect is a grounding
-- (query kind); the assumption carries a `key`, its declared identity
-- (an unkeyed assumption cannot be closed, so it is never asked — a
-- known, accepted gap); the dimension is not one the function map owns
-- (`behavior`, `sign`, `grain` are statistics, ruled 2026-08-13 — no
-- human is asked for a number); and no standing ruling names the same
-- (aspect, key). A ruling holds its question closed until the agent's
-- fold-in raises that key to full confidence, at which point the row
-- drops out on its own.
--
-- `sibling` carries what the human already ruled on the SAME key under
-- a different aspect, so the form can say so while asking. It used to
-- be a second query with the subject and key spliced into SQL text;
-- here it is a join.
--
-- `sibling_stance` and `sibling_note` carry that same ruling in parts,
-- so the round can offer it back as an answer instead of only
-- mentioning it. One decision spelled with one key across three
-- metrics is asked three times — that is deliberate, since a human may
-- rule the same key differently on two aspects and has (run 2:
-- `goods-only` confirmed on purchases, corrected on dpo) — but the
-- repeat should cost a click, not a re-reading.
--
-- No cap and no display: the rows are the whole of what is owed, and
-- glyphs, links and ordering are the caller's business. Filters ride
-- WHERE, like every other read here.
--
-- No ORDER BY, deliberately. A read expands as a derived relation, and
-- an inner ordering does not survive planning — measured, not assumed:
-- the door's round served the wrong question first until it ordered at
-- the call site. Order where you consume.
SELECT o.dataset, o.subject, o.aspect, o.idx, o.dimension, o.key, o.assumption, o.basis, o.conf,
       max(s.stance || ' on ' || s.aspect) AS sibling,
       max(s.stance) AS sibling_stance,
       max(s.note) AS sibling_note
FROM agent_assumptions o
JOIN aspects a ON a.name = o.aspect AND a.kind = 'query'
LEFT JOIN ruling_entries s
  ON s.subject = o.subject AND s.key = o.key AND s.aspect <> o.aspect
WHERE o.conf < 1.0 AND o.key IS NOT NULL
  AND coalesce(o.dimension, '-') NOT IN ('behavior', 'sign', 'grain')
  AND NOT EXISTS (SELECT 1 FROM ruling_entries r
                  WHERE r.subject = o.subject AND r.aspect = o.aspect
                    AND r.key = o.key)
GROUP BY o.dataset, o.subject, o.aspect, o.idx, o.dimension, o.key, o.assumption, o.basis, o.conf
