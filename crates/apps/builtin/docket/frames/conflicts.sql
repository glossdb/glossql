-- One claim ruled two ways: the same key confirmed on one aspect and
-- corrected on another. Nothing asks about it and nothing resolves it
-- — the agent reconciles in its own groundings. Shown because the
-- alternative is that nobody notices.
SELECT arrow_cast(c.key, 'Utf8') AS claim,
       arrow_cast(c.assumption, 'Utf8') AS what,
       arrow_cast(c.newer_aspect || ': ' || c.newer_stance, 'Utf8') AS newer,
       arrow_cast(c.older_aspect || ': ' || c.older_stance, 'Utf8') AS older,
       arrow_cast('/app/docket/p/metrics?metric=' || c.newer_aspect, 'Utf8') AS link
FROM ruling_conflicts c
ORDER BY claim
