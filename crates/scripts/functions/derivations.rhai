// Derivation candidates: row-grain arithmetic identities among a
// table's numeric columns — `a = b * c` and `a = b + c` — with their
// violation counts. Runs at table grain
// (`SELECT detect_derivations() FROM orders`).
//
// Why this exists (tfmeval, 2026-08-10): a scoped unit-mix artifact and
// a real price change move a metric identically; the only instrument
// that separates them is the derivation the lineage carries —
// `line_amount = units * unit_price` held at violation rate 0.0 on
// every clean corpus and fired at the artifact's exact row coverage.
// No marginal statistic reaches this. The measurement is generous by
// design: it proposes identities that HOLD (match rate >= 0.95 over
// >= 20 supporting rows), and the judge confirms which are real
// derivations rather than coincidence. A confirmed identity re-checked
// per batch is the admission instrument.
//
// Cost: one LIMIT 0, one aggregate scan, then one count query per
// surviving triple — survival is an algebraic scale prune (a product
// or sum whose operand magnitudes cannot land within 30x of the target
// is skipped with no recall lost at the 0.95 bar). Numeric columns are
// capped at 12, reported as truncated.

let table = subject;
let head = db.query(`SELECT * FROM "${table}" LIMIT 0`);

let numeric = [];
for c in head.columns() {
    let dt = head.col(c).dtype();
    if dt.contains("Int") || dt.contains("Float") || dt.contains("Decimal") {
        numeric.push(c);
    }
}
let truncated = numeric.len() > 12;
if truncated { numeric = numeric.extract(0..12); }

if numeric.len() < 3 {
    return #{ applicable: false, reason: "fewer than three numeric columns" };
}

// One scan: mean magnitude and filled count per column decide which
// triples are worth a count query.
let select = "";
for c in numeric {
    if select != "" { select += ", "; }
    select += `avg(abs("${c}")) AS "m_${c}", count("${c}") AS "f_${c}"`;
}
let agg = db.query(`SELECT ${select} FROM "${table}"`);
let mag = #{};
let filled = #{};
for c in numeric {
    let m = agg.col(`m_${c}`).value_at(0);
    mag[c] = if m == () { 0.0 } else { m.parse_float() };
    filled[c] = agg.col(`f_${c}`).value_at(0).parse_int();
}

// Every (target; operand pair) triple, both forms, scale-pruned. The
// tolerance is money-shaped: half a cent plus a relative hair, so
// per-row rounding never counts as a violation.
let tol = |a| `greatest(0.011, abs("${a}") * 0.000001)`;
let specs = [];
for a in numeric {
    if filled[a] < 20 { continue; }
    for i in 0..numeric.len() {
        let b = numeric[i];
        if b == a { continue; }
        for j in (i + 1)..numeric.len() {
            let c = numeric[j];
            if c == a { continue; }
            if filled[b] < 20 || filled[c] < 20 { continue; }
            let forms = [];
            if mag[b] > 0.0 && mag[c] > 0.0 && mag[a] > 0.0 {
                let prod = mag[b] * mag[c];
                if prod < mag[a] * 30.0 && mag[a] < prod * 30.0 { forms.push("product"); }
            }
            let s = mag[b] + mag[c];
            if s > 0.0 && mag[a] > 0.0 && s < mag[a] * 30.0 && mag[a] < s * 30.0 {
                forms.push("sum");
            }
            if forms.len() > 0 { specs.push(#{ a: a, b: b, c: c, forms: forms }); }
        }
    }
}

let sqls = [];
for s in specs {
    let present = `"${s.a}" IS NOT NULL AND "${s.b}" IS NOT NULL AND "${s.c}" IS NOT NULL`;
    let parts = `count(*) AS support`;
    for f in s.forms {
        let expr = if f == "product" { `("${s.b}" * "${s.c}")` } else { `("${s.b}" + "${s.c}")` };
        parts += `, count(*) FILTER (WHERE abs("${s.a}" - ${expr}) > ${tol.call(s.a)}) AS "viol_${f}"`;
    }
    sqls.push(`SELECT ${parts} FROM "${table}" WHERE ${present}`);
}
let answers = db.query_all(sqls);

let candidates = [];
for i in 0..specs.len() {
    let s = specs[i];
    let support = answers[i].col("support").value_at(0).parse_int();
    if support < 20 { continue; }
    for f in s.forms {
        let viol = answers[i].col(`viol_${f}`).value_at(0).parse_int();
        let rate = 1.0 - viol.to_float() / support.to_float();
        if rate < 0.95 { continue; }
        candidates.push(#{
            target: s.a, form: f, operands: [s.b, s.c],
            support: support, violations: viol, match_rate: rate,
        });
    }
}

candidates.sort(|x, y| if x.match_rate < y.match_rate {
    1
} else if x.match_rate > y.match_rate {
    -1
} else {
    0
});

#{ applicable: true, rows: filled.values().reduce(|m, v| if v > m { v } else { m }, 0),
   candidates: candidates, truncated: truncated }
