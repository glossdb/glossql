// Metric expectation bands: for every grounded metric, walk the recent
// months and ask the TabICL forward what range each month should have
// landed in, given everything before it. The protocol here — monthly
// flow read, the feature recipe, the walk — is authored policy; the
// model call is one kernel (`tabicl_bands`), never reimplemented in
// script. Evaluated against generated ground truth before it shipped
// (dataraum-tabicl README, stage 3; the E2.1 walk).
//
// The feature recipe and the fill tracked back to the graded protocol
// on 2026-08-12 — three drifts had entered in the port (raw t-3 lag for
// the trailing mean, a whole-series fill that was not point-in-time, a
// fabricated month-0 training row). Change it only against
// `../tfmeval/experiments/e2_1_bands.py`, which is what the coverage
// figures in that repo's FINDINGS.md were measured on.
//
// Each walked point records its bands and its PIT — the quantile at
// which the actual landed in the predicted distribution, 0..1 and
// ordinal by construction (raw densities never leave the kernel). The
// band_breach detector adjudicates PITs; this measurement only reports.
//
// Runs at dataset grain (`SELECT metric_bands() FROM fin`). ACCEPTS
// (glossary): a grounding write stales this cache. The monthly read
// follows the grounding's authored behavior: flows sum per month; a
// body carrying `"behavior": "stock"` takes the last value per month
// instead — no marker reads as flow, so stocks must be marked (the
// metrics skill teaches the marker). Months are taken as returned,
// gap detection being temporal_profile's business.

let alphas = [0.05, 0.10, 0.50, 0.90, 0.95];
let min_train = 5;
let max_walk = 6;

// Latest QUERY grounding per (subject, aspect), human over agent —
// the same collapse grounding_collisions does.
let rows = db.query(`
    SELECT g.subject, g.aspect, g.actor_kind, g.body
    FROM glossary g
    JOIN aspects a ON a.name = g.aspect
    WHERE a.kind = 'query' AND g.dataset = '${subject}'
      AND NOT EXISTS (
        SELECT 1 FROM glossary n
        WHERE n.dataset = g.dataset AND n.subject = g.subject
          AND n.aspect = g.aspect AND n.actor_kind = g.actor_kind
          AND n.written_at > g.written_at)
`);
let slots = #{};
for i in 0..rows.num_rows() {
    let key = `${rows.col("subject").value_at(i)} ${rows.col("aspect").value_at(i)}`;
    let kind = rows.col("actor_kind").value_at(i);
    if key in slots && slots[key].kind == "human" && kind != "human" {
        continue;
    }
    slots[key] = #{
        aspect: rows.col("aspect").value_at(i),
        kind: kind,
        body: rows.col("body").value_at(i),
    };
}

// Median over the present values of one feature column; unit when the
// column is absent throughout. Even counts average the two middles, as
// the graded protocol's pandas median does.
fn col_median(values) {
    let s = values.filter(|x| x != ());
    if s.is_empty() { return (); }
    s.sort();
    let k = s.len();
    if k % 2 == 1 { s[k / 2] } else { (s[k / 2 - 1] + s[k / 2]) / 2.0 }
}

// Apply a step's fills to one feature row.
fn filled(row, fills) {
    let out = [];
    for c in 0..row.len() {
        out.push(if row[c] == () { fills[c] } else { row[c] });
    }
    out
}

let metrics = [];
for key in slots.keys() {
    let body = parse_json(slots[key].body);
    if type_of(body) != "map" || !("sql" in body) { continue; }

    // The grounding is a grain-free extract with a time axis and a
    // `value` column; find the time column by dtype and apply the
    // monthly grain here, as any reader does.
    let probe = db.query(`SELECT * FROM (${body.sql}) LIMIT 1`);
    if !probe.columns().contains("value") { continue; }
    let tcol = "";
    for c in probe.columns() {
        let d = probe.col(c).dtype();
        if d.contains("Date") || d.contains("Timestamp") { tcol = c; break; }
    }
    if tcol == "" { continue; }

    // Flows sum per month; a marked stock takes its last value per
    // month — a level summed across a partition is not a level.
    let is_stock = "behavior" in body && body.behavior == "stock";
    let series = if is_stock {
        db.query(`
            SELECT period, value FROM (
                SELECT date_trunc('month', "${tcol}") AS period, value,
                       row_number() OVER (
                           PARTITION BY date_trunc('month', "${tcol}")
                           ORDER BY "${tcol}" DESC) AS rn
                FROM (${body.sql})
            ) WHERE rn = 1 ORDER BY period
        `)
    } else {
        db.query(`
            SELECT date_trunc('month', "${tcol}") AS period, sum(value) AS value
            FROM (${body.sql}) GROUP BY 1 ORDER BY 1
        `)
    };
    let v = series.col("value").floats();
    let n = v.len();
    // Feature rows start at month 1 — month 0 has no lag of its own and
    // the graded protocol builds no row for it — so a walk point at t
    // trains on t-1 rows and the earliest walkable t is min_train + 1.
    if n < min_train + 2 {
        metrics.push(#{ metric: slots[key].aspect, applicable: false,
                        reason: `${n} months, need ${min_train + 2}` });
        continue;
    }

    // The walk: for each recent month, train on everything before it.
    // Features for month i (row i-1): index, month-of-year, the 1- and
    // 12-month lags, and the trailing 3-month MEAN — `lag3m` in the
    // graded protocol (tfmeval `experiments/e2_1_bands.py:71-77`), not
    // the raw t-3 value. Absent features stay absent here; they fill
    // per walk step from training rows only, below.
    let feats = [];
    let labels = [];
    for i in 1..n {
        let period = series.col("period").value_at(i);
        let moy = period.sub_string(5, 2).parse_int();
        let lag1 = if v[i - 1] != () { v[i - 1] } else { () };
        let lo = if i >= 3 { i - 3 } else { 0 };
        let s3 = 0.0;
        let c3 = 0;
        for j in lo..i { if v[j] != () { s3 += v[j]; c3 += 1; } }
        let lag3m = if c3 == 0 { () } else { s3 / c3.to_float() };
        let lag12 = if i >= 12 && v[i - 12] != () { v[i - 12] } else { () };
        feats.push([i.to_float(), moy.to_float(), lag1, lag3m, lag12]);
        labels.push(v[i]);
    }

    let points = [];
    let floor = min_train + 1;
    let start = if n - max_walk > floor { n - max_walk } else { floor };
    for t in start..n {
        if v[t] == () { continue; }
        // Absent features fill from the training rows only, per feature,
        // recomputed at this step — so the fit stays point-in-time. A
        // feature absent throughout training fills 0.0, as the protocol
        // does after an all-absent column's median.
        let fills = [];
        for c in 0..5 {
            let col = [];
            for r in 0..(t - 1) { col.push(feats[r][c]); }
            let m = col_median(col);
            fills.push(if m == () { 0.0 } else { m });
        }
        let train_x = [];
        let train_y = [];
        for r in 0..(t - 1) {
            if labels[r] == () { continue; }
            train_x.push(filled(feats[r], fills));
            train_y.push(labels[r]);
        }
        if train_y.len() < min_train { continue; }
        let read = tabicl_bands(train_x, train_y, filled(feats[t - 1], fills), alphas, v[t]);
        points.push(#{
            period: series.col("period").value_at(t).sub_string(0, 7),
            actual: v[t],
            p05: read.q[0], p10: read.q[1], p50: read.q[2],
            p90: read.q[3], p95: read.q[4],
            pit: read.pit,
        });
    }
    metrics.push(#{
        metric: slots[key].aspect, applicable: true, grain: "month",
        aggregation: if is_stock { "last" } else { "sum" },
        trained_on: n, points: points,
    });
}

#{
    applicable: metrics.len() > 0,
    metrics: metrics,
}
