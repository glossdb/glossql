# Derivations — lineage identities as the corruption instrument

`detect_derivations` finds row-grain arithmetic identities among a
table's numeric columns — `a = b * c` and `a = b + c` — and reports
each candidate with its support, violation count, and match rate. It
runs at table grain (`SELECT detect_derivations() FROM orders`).

## Why identities, not statistics

A scoped corruption and a real business change move a metric
identically. A slice whose amounts were silently scaled shifts the
mean, the quantiles, and the distribution exactly as a price change
would — every marginal statistic confuses the two. The derivation the
lineage carries is the one instrument that separates them: an identity
like `line_amount = units * unit_price` holds at violation rate 0.0 on
clean data and fires at exactly the corrupted rows' coverage, because
a scaled copy of one operand can no longer reproduce the target row by
row. The identity held at violation rate 0.0 on every clean corpus and
fired at the artifact's exact row coverage — no marginal statistic
reaches this.

A confirmed identity, re-checked per batch, is therefore the admission
check for subtle corruption: "the pipeline broke" and "the business
changed" answer differently to it.

## Mechanism

The search door (`derivation_candidates`,
`crates/session/src/search.rs`) is generous by design: it counts every
triple that passes a structural prune, and the prune is cost, not
judgment — a product or sum whose operand magnitudes cannot land
within 30× of the target is skipped, with no recall lost at the body's
bar. One aggregate scan sizes the prune; one aggregate scan counts
every remaining triple. The measurement body
(`crates/scripts/functions/derivations.sql`) applies the only two
arguable constants, in the open: an identity holds at
`match_rate >= 0.95` over `>= 20` supporting rows. The judge confirms
which candidates are real derivations rather than coincidence.

## Limits

- Numeric columns are capped at 12 per table; the result says
  `truncated` when the cap applied.
- Two forms only (`b * c`, `b + c`); deeper compositions are read
  through the metric layer, not this instrument.
- A coincidental identity can pass the bar on small tables — the judge
  is part of the method.
