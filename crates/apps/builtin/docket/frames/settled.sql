-- The settled band: every standing ruling, in the human's own words,
-- with what the agent did about it. `folded_in` is derived — the
-- ruling stands unfolded while its key is still disclosed below full
-- confidence, and clears when the re-record lands, so nothing here is
-- marked done by hand.
SELECT arrow_cast(r.aspect, 'Utf8') AS subj,
       arrow_cast(CASE WHEN r.folded_in THEN 'rail settled' ELSE 'rail' END, 'Utf8') AS rcls,
       arrow_cast(coalesce(r.dimension, '-') || ' · ' || r.key, 'Utf8') AS asp,
       arrow_cast(r.assumption, 'Utf8') AS what,
       arrow_cast(r.stance, 'Utf8') AS stance,
       -- The human's own words, quoted as theirs. A confirmation with
       -- no note stays empty rather than being given a sentence it
       -- never said.
       arrow_cast(CASE WHEN coalesce(r.note, '') = '' THEN ''
                       ELSE '“' || r.note || '”' END, 'Utf8') AS note,
       arrow_cast(CASE WHEN r.folded_in THEN 'folded in'
                       ELSE 'awaiting the fold-in' END, 'Utf8') AS state,
       CASE WHEN r.folded_in THEN 'ok' ELSE 'warn' END AS scls,
       arrow_cast(substr(r.written_at, 1, 10), 'Utf8') AS at,
       arrow_cast('/app/docket/p/metrics?metric=' || r.aspect, 'Utf8') AS link
FROM ruling_entries r
JOIN current_dataset d ON d.dataset = r.dataset
ORDER BY r.written_at DESC, subj
