// rate_tolerance — the validation detector: reads the authored
// expectation and the check voice from the slots, sees no table data.
// Wire it per validation aspect: the aspect's schema carries a
// `tolerance` key (the expectation gloss) and a `breach_rate` key (the
// check function's voice — the VIOLATION share, 0.0 means fully
// passing; the key is named for its polarity because a pass rate read
// as a breach rate banded a 100%-passing check red, 2026-08-14); the
// witness's THRESHOLD overrides the authored tolerance when set.
// One-sided by design — a known-dirt source that expects its own rate
// wants a custom detector that goes red on both sides, since
// overcleaning is also a failure.

let tolerance = if context.threshold != () { context.threshold } else { 0.0 };
let rate = 0.0;
let found = false;
for s in context.slots {
    if s.body == () || type_of(s.body) != "map" { continue; }
    if "tolerance" in s.body { tolerance = s.body.tolerance; }
    if "breach_rate" in s.body { rate = s.body.breach_rate; found = true; }
}
let band = if !found { "yellow" }
    else if rate <= tolerance { "green" }
    else { "red" };
#{ subject: context.subject, aspect: context.aspect,
   witness: context.witness, band: band, score: rate, computed_at: "" }
