-- Column profile (fixture 11): the deterministic profile plane as one
-- measurement — counts, distribution shape, string lengths, top values.
-- It measures the landed table — the recipe carried the casts,
-- so null_ratio counts what the author's try_casts
-- surrendered and min/max order as their types — v0.3's gate exactly
-- (statistics ran on the typed table). The statistics live in the
-- engine's `profile` aggregate; extraction serves the body's `summary`
-- and the full profile reads back via GLOSSARY(table.column::column_profile).
SELECT profile(v) FROM subject_column($subject)
