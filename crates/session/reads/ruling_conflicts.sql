-- ruling_conflicts — one claim ruled two ways.
--
-- The same `key` confirmed on one aspect and corrected on another.
-- Run 2 produced exactly this: `purchases`'s goods-only assumption was
-- confirmed in the same session where `dpo`'s goods-only assumption was
-- corrected. Both rulings are legitimate on their own aspect, and the
-- agent reconciled them by hand — but nothing surfaced the tension, and
-- a less careful fold-in would have silently dropped one.
--
-- It is a read, not a protocol. Nothing pairs, resolves, or asks: the
-- rows show on the docket, the agent can select from them, and
-- reconciling is the agent's act recorded in its own groundings. The
-- machinery this replaces carried a resolution stance, a `settles_with`
-- list, and its own question shape — more code than the thing it
-- caught.
--
-- `j > b.idx` orders the pair, so each conflict reports once with the
-- later ruling as the newer one.
--
-- A conflict clears when BOTH sides are folded in (2026-08-15). Two
-- aspects may genuinely differ on one key, and when they do the pair
-- stands forever with nothing anyone can do about it — noise on the
-- docket that never goes away. `folded_in` is the answer already in
-- the record: it means the agent re-recorded that grounding at full
-- confidence carrying the ruled key, which IS the reconciliation this
-- read exists to provoke. Once both sides carry it, the tension was
-- surfaced and answered; until then it stands. Nothing is marked done
-- by hand, and no third ruling is ever needed — re-ruling either side
-- overwrites that entry, and if the stances then agree the pair stops
-- matching at all.
SELECT a.subject AS subject,
       a.key AS key,
       a.aspect AS newer_aspect,
       b.aspect AS older_aspect,
       a.stance AS newer_stance,
       b.stance AS older_stance,
       a.dimension AS dimension,
       a.assumption AS assumption
FROM ruling_entries a
JOIN ruling_entries b
  ON a.subject = b.subject AND a.key = b.key
 AND a.aspect <> b.aspect AND a.stance <> b.stance AND a.idx > b.idx
WHERE a.key IS NOT NULL
  AND NOT (a.folded_in AND b.folded_in)
