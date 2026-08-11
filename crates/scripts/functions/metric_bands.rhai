// Metric expectation bands: for every grounded metric, walk the recent
// months and ask the TabICL forward what range each month should have
// landed in, given everything before it. The protocol here — monthly
// flow read, the feature recipe, the walk — is authored policy; the
// model call is one kernel (`tabicl_bands`), never reimplemented in
// script. Evaluated against generated ground truth before it shipped
// (dataraum-tabicl README, stage 3; the E2.1 walk).
//
// Each walked point records its bands and its PIT — the quantile at
// which the actual landed in the predicted distribution, 0..1 and
// ordinal by construction (raw densities never leave the kernel). The
// band_breach detector adjudicates PITs; this measurement only reports.
//
// Runs at dataset grain (`SELECT metric_bands() FROM fin`). ACCEPTS
// (glossary): a grounding write stales this cache. Simplifications,
// stated: the monthly read is sum-per-month — a stock metric summed
// across is wrong (the metrics skill's warning) and stock handling
// waits on the behavior verdict wiring; months are taken as returned,
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

fn median(values) {
    if values.is_empty() { return 0.0; }
    let s = values;
    s.sort();
    s[s.len() / 2]
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

    let series = db.query(`
        SELECT date_trunc('month', "${tcol}") AS period, sum(value) AS value
        FROM (${body.sql}) GROUP BY 1 ORDER BY 1
    `);
    let v = series.col("value").floats();
    let n = v.len();
    if n < min_train + 1 {
        metrics.push(#{ metric: slots[key].aspect, applicable: false,
                        reason: `${n} months, need ${min_train + 1}` });
        continue;
    }
    let fill = median(v.filter(|x| x != ()));

    // The walk: for each recent month, train on everything before it.
    // Features per month i: index, month-of-year, and the 1/3/12-month
    // lags, absent lags filled with the series median.
    let feats = [];
    for i in 0..n {
        let period = series.col("period").value_at(i);
        let moy = period.sub_string(5, 2).parse_int();
        let lag = |k| if i >= k && v[i - k] != () { v[i - k] } else { fill };
        feats.push([i.to_float(), moy.to_float(), lag.call(1), lag.call(3), lag.call(12)]);
    }

    let points = [];
    let start = if n - max_walk > min_train { n - max_walk } else { min_train };
    for t in start..n {
        if v[t] == () { continue; }
        let train_x = [];
        let train_y = [];
        for i in 0..t {
            if v[i] == () { continue; }
            train_x.push(feats[i]);
            train_y.push(v[i]);
        }
        if train_y.len() < min_train { continue; }
        let read = tabicl_bands(train_x, train_y, feats[t], alphas, v[t]);
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
        aggregation: "sum", trained_on: n, points: points,
    });
}

#{
    applicable: metrics.len() > 0,
    metrics: metrics,
}
