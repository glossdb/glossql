// The metric cube: every grounded metric's monthly series — the total,
// the slices along its served dimension columns, and the named rival
// where a grounding discloses one — cached as one measurement. What it
// exists for: a static frame cannot name a metric in FROM, but a
// function composes SQL freely, so this script turns metric names into
// data once and the `metric_series()` read serves the rows to any
// frame with plain value filters.
//
// Caps, recorded in the body: at most 2 dimension columns per metric
// (a served column is admitted at 2..24 distinct members; fewest
// members win), every member of an admitted dimension, the last 24
// months. A grounding assumption carrying `alternative_sql` (the
// metrics skill's named-rival convention) contributes one more series
// under dimension 'alternative', so what moves between the chosen and
// the rival reading is a chart, not an argument. The rival SQL is
// authored but never admission-validated — it runs behind a guard and
// a failure is reported, not thrown.
//
// Runs at dataset grain (`SELECT metric_cube() FROM fin`). ACCEPTS
// (glossary, imports): a grounding write or a landed import stales
// this cache; fact glosses do not (the edge is QUERY-scoped in the
// store). The monthly verb follows the grounding's authored behavior:
// flows sum per month; a body carrying `"behavior": "stock"` sums the
// rows at the month's latest observed date — a one-row-per-period
// balance serves unchanged, and a stock extract with many rows per
// period (per product, per location) totals its final snapshot
// instead of one arbitrary row. Disclosed limit: entities absent from
// that latest date are not carried forward — a stock whose entities
// report on different days undercounts; pre-aggregate in the
// grounding SQL when observation dates are ragged.

let dims_cap = 2;
let members_cap = 24;
let months_cap = 24;

// Latest QUERY grounding per (subject, aspect), human over agent —
// the same collapse metric_bands and grounding_collisions run.
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

// One monthly series for an extract: [[period, value], …], the verb
// picked by behavior. Periods render as YYYY-MM.
fn monthly(door, sql, tcol, is_stock) {
    let series = if is_stock {
        door.query(`
            SELECT period, sum(value) AS value FROM (
                SELECT date_trunc('month', "${tcol}") AS period, value,
                       rank() OVER (
                           PARTITION BY date_trunc('month', "${tcol}")
                           ORDER BY "${tcol}" DESC) AS rk
                FROM (${sql})
            ) WHERE rk = 1 GROUP BY period ORDER BY period
        `)
    } else {
        door.query(`
            SELECT date_trunc('month', "${tcol}") AS period, sum(value) AS value
            FROM (${sql}) GROUP BY 1 ORDER BY 1
        `)
    };
    let v = series.col("value").floats();
    let out = [];
    for i in 0..series.num_rows() {
        if v[i] == () { continue; }
        out.push([series.col("period").value_at(i).sub_string(0, 7), v[i]]);
    }
    out
}

// The time column of an extract, by dtype; "" when none.
fn time_col(probe) {
    for c in probe.columns() {
        let d = probe.col(c).dtype();
        if d.contains("Date") || d.contains("Timestamp") { return c; }
    }
    ""
}

let metrics = [];
for key in slots.keys() {
    let body = parse_json(slots[key].body);
    if type_of(body) != "map" || !("sql" in body) { continue; }
    let name = slots[key].aspect;

    let probe = db.query(`SELECT * FROM (${body.sql}) LIMIT 1`);
    if !probe.columns().contains("value") {
        metrics.push(#{ metric: name, applicable: false, reason: "no value column" });
        continue;
    }
    let tcol = time_col(probe);
    if tcol == "" {
        metrics.push(#{ metric: name, applicable: false, reason: "no time column" });
        continue;
    }
    let is_stock = "behavior" in body && body.behavior == "stock";

    // Served dimension candidates: every column that is neither the
    // value nor time-typed. Admission is by member count, batched
    // through one door call.
    let cand = [];
    for c in probe.columns() {
        if c == "value" { continue; }
        let d = probe.col(c).dtype();
        if d.contains("Date") || d.contains("Timestamp") { continue; }
        cand.push(c);
    }
    let counts = [];
    if !cand.is_empty() {
        let qs = [];
        for c in cand {
            qs.push(`SELECT count(DISTINCT "${c}") AS n FROM (${body.sql})`);
        }
        let tables = db.query_all(qs);
        for i in 0..cand.len() {
            let n = tables[i].cell("n");
            counts.push(#{ name: cand[i], n: if n == () { 0 } else { n.parse_int() } });
        }
    }
    let dims = [];
    while dims.len() < dims_cap {
        let best_name = "";
        let best_n = 0;
        for c in counts {
            if c.n < 2 || c.n > members_cap { continue; }
            if dims.contains(c.name) { continue; }
            if best_name == "" || c.n < best_n {
                best_name = c.name;
                best_n = c.n;
            }
        }
        if best_name == "" { break; }
        dims.push(best_name);
    }

    // The total series bounds the window: the last months_cap periods,
    // held as a set every other series filters against.
    let total = monthly(db, body.sql, tcol, is_stock);
    let start = if total.len() > months_cap { total.len() - months_cap } else { 0 };
    let keep = #{};
    for i in start..total.len() { keep[total[i][0]] = true; }
    let out = [];
    for p in total {
        if p[0] in keep { out.push(["", "", p[0], p[1]]); }
    }

    // Member series per admitted dimension, same verb, same window.
    for dcol in dims {
        let series = if is_stock {
            db.query(`
                SELECT period, member, sum(value) AS value FROM (
                    SELECT date_trunc('month', "${tcol}") AS period,
                           CAST("${dcol}" AS VARCHAR) AS member, value,
                           rank() OVER (
                               PARTITION BY date_trunc('month', "${tcol}"), "${dcol}"
                               ORDER BY "${tcol}" DESC) AS rk
                    FROM (${body.sql}) WHERE "${dcol}" IS NOT NULL
                ) WHERE rk = 1 GROUP BY period, member ORDER BY period, member
            `)
        } else {
            db.query(`
                SELECT date_trunc('month', "${tcol}") AS period,
                       CAST("${dcol}" AS VARCHAR) AS member, sum(value) AS value
                FROM (${body.sql}) WHERE "${dcol}" IS NOT NULL
                GROUP BY 1, 2 ORDER BY 1, 2
            `)
        };
        let v = series.col("value").floats();
        for i in 0..series.num_rows() {
            if v[i] == () { continue; }
            let period = series.col("period").value_at(i).sub_string(0, 7);
            if !(period in keep) { continue; }
            out.push([dcol, series.col("member").value_at(i), period, v[i]]);
        }
    }

    // The named rival, when a grounding assumption discloses one.
    let entry = #{
        metric: name, applicable: true, behavior: if is_stock { "stock" } else { "flow" },
        dims: dims,
    };
    if "assumptions" in body && type_of(body.assumptions) == "array" {
        for a in body.assumptions {
            if type_of(a) != "map" || !("alternative_sql" in a) { continue; }
            let rival = if "alternative" in a { a.alternative } else { "(rival)" };
            try {
                let aprobe = db.query(`SELECT * FROM (${a.alternative_sql}) LIMIT 1`);
                let atcol = time_col(aprobe);
                if !aprobe.columns().contains("value") || atcol == "" {
                    entry.alternative_error = "rival SQL serves no value/time column";
                } else {
                    for p in monthly(db, a.alternative_sql, atcol, is_stock) {
                        if p[0] in keep { out.push(["alternative", rival, p[0], p[1]]); }
                    }
                    entry.alternative = rival;
                }
            } catch (e) {
                entry.alternative_error = `rival SQL failed: ${e}`;
            }
            break;
        }
    }
    entry.rows = out;
    metrics.push(entry);
}

let metric_names = [];
let total_rows = 0;
for m in metrics { metric_names.push(m.metric); total_rows += m.rows.len(); }

#{
    applicable: metrics.len() > 0,
    caps: #{ dims: dims_cap, members: members_cap, months: months_cap },
    metrics: metrics,
    // Extraction serves this alone (ruled 2026-08-14: the 54 KB body
    // was write-only through the door) — the full cube is the cache's
    // business: metric_series() slices it, GLOSSARY reads it whole.
    summary: #{
        metrics: metric_names,
        rows: total_rows,
        note: "cached — slice with metric_series(); the whole cube reads back via GLOSSARY(<dataset>::metric_cube)",
    },
}
