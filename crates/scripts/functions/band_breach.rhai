// Band-breach detector: sees the metric_bands slots and the witness
// threshold, never table data. Each walked point carries its PIT — the
// quantile where the actual landed. Displacement is |2*pit - 1|: 0.0 at
// the median, 0.8 at a nominal-80 band edge, 0.9 at a nominal-90 edge.
// Score is the worst displacement across the latest point of every
// monitored metric; band maps displacement against the band edges, with
// the witness threshold as the red line (default 0.98). Detail — which
// metric, which month — lives in the measurement's own cached output.
let score = 0.0;
for s in context.slots {
    if s.body == () || type_of(s.body) != "map" || !("metrics" in s.body) {
        continue;
    }
    for m in s.body.metrics {
        if !("points" in m) || m.points.is_empty() { continue; }
        let last = m.points[m.points.len() - 1];
        let d = 2.0 * last.pit - 1.0;
        if d < 0.0 { d = -d; }
        if d > score { score = d; }
    }
}
let red_line = if context.threshold != () { context.threshold } else { 0.98 };
let band = if score <= 0.8 {
    "green"
} else if score <= 0.9 {
    "yellow"
} else if score <= red_line {
    "orange"
} else {
    "red"
};
#{
    subject: context.subject, aspect: context.aspect, witness: context.witness,
    band: band, score: score, computed_at: "",
}
