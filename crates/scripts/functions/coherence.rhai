// Relationship coherence: what the declared joins assert, checked
// against the rows. Runs at dataset grain
// (`SELECT relationship_coherence() FROM fin`) over DECLARED
// relationships only — the candidate plane proposes, the judge
// declares, this measures what the declaration now claims.
//
// Two facts per relationship, both invisible to every column-shaped
// check (tfmeval, 2026-08-10: tier0, PSI and the classifier two-sample
// test all scored exactly 0.0 on every referential defect on a
// high-cardinality key — the join is the only instrument):
//
//   orphans   from-side values that resolve to no to-side row. Exact,
//             catches the invented-key shapes including the repeated
//             orphan that defeats rare-category counting.
//   temporal  for each date column pair across the join, how often the
//             child's date precedes its parent's. Evidence, not
//             verdict: a payment before its invoice's DUE date is
//             ordinary; before the invoice EXISTS is the trace a
//             wrong pairing leaves. The judge reads the pair names.
//
// Cost: two count queries per relationship plus one joined scan when
// both sides carry date columns (capped at three per side), all
// batched.

let rels = db.query("SELECT left_path, op, right_path FROM relationships");

let entries = [];
for i in 0..rels.num_rows() {
    let left = rels.col("left_path").value_at(i);
    let right = rels.col("right_path").value_at(i);
    let lp = left.split(".");
    let rp = right.split(".");
    if lp.len() != 2 || rp.len() != 2 { continue; }
    entries.push(#{
        relationship: `${left} ${rels.col("op").value_at(i)} ${right}`,
        ct: lp[0], cc: lp[1], pt: rp[0], pc: rp[1],
    });
}

if entries.len() == 0 {
    return #{ applicable: false, reason: "no declared relationships" };
}

// Date columns per involved table, once.
let date_cols = #{};
for e in entries {
    for t in [e.ct, e.pt] {
        if t in date_cols { continue; }
        let head = db.query(`SELECT * FROM "${t}" LIMIT 0`);
        let dates = [];
        for c in head.columns() {
            let dt = head.col(c).dtype();
            if dt.contains("Date") || dt.contains("Timestamp") {
                if dates.len() < 3 { dates.push(c); }
            }
        }
        date_cols[t] = dates;
    }
}

// rhai's for iterates copies — pair specs live in a parallel array,
// indexed with the entries at readback.
let entry_pairs = [];
let sqls = [];
for e in entries {
    sqls.push(`SELECT count("${e.cc}") AS filled FROM "${e.ct}"`);
    sqls.push(`SELECT count(*) AS orphans FROM "${e.ct}"
               WHERE "${e.cc}" IS NOT NULL
                 AND "${e.cc}" NOT IN (SELECT "${e.pc}" FROM "${e.pt}"
                                       WHERE "${e.pc}" IS NOT NULL)`);
    let pairs = [];
    for cd in date_cols[e.ct] {
        for pd in date_cols[e.pt] {
            pairs.push(#{ cd: cd, pd: pd });
        }
    }
    entry_pairs.push(pairs);
    if pairs.len() > 0 {
        let parts = "";
        for k in 0..pairs.len() {
            let p = pairs[k];
            if parts != "" { parts += ", "; }
            parts += `count(*) FILTER (WHERE c."${p.cd}" IS NOT NULL
                          AND p."${p.pd}" IS NOT NULL) AS "n_${k}",
                      count(*) FILTER (WHERE c."${p.cd}" < p."${p.pd}") AS "pre_${k}"`;
        }
        sqls.push(`SELECT ${parts} FROM "${e.ct}" c
                   JOIN "${e.pt}" p ON c."${e.cc}" = p."${e.pc}"`);
    }
}
let answers = db.query_all(sqls);

let out = [];
let cursor = 0;
for ei in 0..entries.len() {
    let e = entries[ei];
    let pairs = entry_pairs[ei];
    let filled = answers[cursor].col("filled").value_at(0).parse_int();
    let orphans = answers[cursor + 1].col("orphans").value_at(0).parse_int();
    cursor += 2;
    let temporal = [];
    if pairs.len() > 0 {
        let joined = answers[cursor];
        cursor += 1;
        for k in 0..pairs.len() {
            let n = joined.col(`n_${k}`).value_at(0).parse_int();
            if n == 0 { continue; }
            let pre = joined.col(`pre_${k}`).value_at(0).parse_int();
            temporal.push(#{
                child_column: pairs[k].cd, parent_column: pairs[k].pd,
                joined: n, precedes: pre,
                precedes_rate: pre.to_float() / n.to_float(),
            });
        }
    }
    out.push(#{
        relationship: e.relationship,
        filled: filled, orphans: orphans,
        orphan_rate: if filled > 0 { orphans.to_float() / filled.to_float() } else { 0.0 },
        temporal: temporal,
    });
}

#{ applicable: true, relationships: out }
