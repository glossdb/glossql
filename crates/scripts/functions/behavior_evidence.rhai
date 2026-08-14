// Behavior evidence (port list item 2, ruled 2026-08-05): the stock/flow
// discriminator from v0.3's lineage reconcile (analysis/lineage/
// reconcile.py, transcribed 2026-08-05), reshaped as an evidence
// measurement — the judge reads it before glossing `behavior`; it is
// never a voice in the behavior slots. Runs at column grain
// (`SELECT behavior_evidence() FROM trial_balance.debit_balance`).
//
// The falsification that shaped it (v0.3's own record): a column's OWN
// trajectory cannot decide stock vs flow — a trending flow and a
// mean-reverting stock look alike. The evidence must be cross-table:
// an INDEPENDENT per-period movement m aggregated from a related event
// table, scored against two hypotheses with scale-free residuals
//   FLOW : y[t]  ≈ m[t]        R_flow  = Σ|y − m| / Σ|m|
//   STOCK: Δy[t] ≈ m[t] (t≥1)  R_stock = Σ|Δy − m'| / Σ|m'|
//
// Anchors come from DECLARED relationships only (ruled 2026-08-05): the
// plane order is relationships first, and the wrong-anchor gate then
// guards misdeclared edges rather than a combinatorial sweep. Composite
// (tuple) endpoints are skipped for now. The declaration reads them as
// invalidation edges — `ACCEPTS (relationships, imports)`, ruled
// 2026-08-05 — so a new edge or a landed table kills this cache and the
// next call recomputes; abstentions heal on their own.
//
// The division of labor (2026-08-06, replacing interpreter-bound
// loops): this script discovers anchors and holds POLICY — axes,
// alignments, grain, which terms are movements, Wilson support, the
// winner and its alternatives. The `reconcile` kernel holds the
// ARITHMETIC — alignment by hash join on typed keys, conventions (each
// term and every ordered pair difference, v0.3's enumeration) as one
// matrix product over the stacked entity series, residuals and gates
// per entity. Pairing is on the intersection of (entity, period) cells
// present on both sides; a calendar gap between kept cells makes Δy
// span it and read as noise — honest abstention absorbs the miss.
//
// Anchor shape (the f1 lessons, kept from the rework):
// - Declared-edge legs leave the movement pool: a join leg is an
//   identifier, not a movement. Served on each anchor as
//   `identifier_columns` — visible, never silent.
// - A table's own time axes are preferred; it borrows through an edge
//   only when it has none (journal_lines borrowing journal_entries'
//   date stays; every-table-also-via-races goes).
// - The anchor grain is the COARSER of the two sides' native grains
//   (year > quarter > month > day): a monthly snapshot against a daily
//   ledger reconciles monthly, two daily tables reconcile daily — a
//   race-day standings row against that race's points, exactly.
// - Two additions from the medium run's starvation (2026-08-14),
//   both recall-first with the gates deciding: a day-native alignment
//   also serves a month variant (document-keyed pairs share too few
//   (entity, day) cells), and a measure table whose only edges are
//   document-keyed borrows its entity one hop through the document
//   (payments → ar_invoices → customers) — the same move as the
//   borrowed time axis.
// - An alignment where fewer than two entities carry 4+ periods
//   abstains on one HAVING probe, before either side is scanned wide.
//
// Cost: one LIMIT 0 per landed table, one grain probe per involved
// axis, one viability probe per alignment, two grouped aggregates per
// surviving anchor; the arithmetic is one kernel call.

fn wilson_lcb(successes, n) {
    // Wilson score lower bound (Wilson 1927): the parameter-free way to
    // rank a vote rate under small n. n MUST be the pairing's COMMON
    // entity denominator, never a convention's own aligned subset —
    // v0.3's recorded support-gameability trap: sparse pair differences
    // structurally out-raced true singles on half-entity subsets.
    if n <= 0 { return 0.0; }
    let z = 1.96;
    let nf = n.to_float();
    let p = successes.to_float() / nf;
    let denom = 1.0 + z * z / nf;
    let centre = p + z * z / (2.0 * nf);
    let margin = z * sqrt(p * (1.0 - p) / nf + z * z / (4.0 * nf * nf));
    let v = (centre - margin) / denom;
    if v < 0.0 { 0.0 } else { v }
}

fn grain_rank(g) {
    switch g { "year" => 3, "quarter" => 2, "month" => 1, _ => 0 }
}

fn coarser(a, b) {
    if grain_rank(a) >= grain_rank(b) { a } else { b }
}

// ---- subject and schema ----

let parts = subject.split(".");
let out = #{ applicable: false };

if parts.len() == 2 {
    let table = parts[0];
    let column = parts[1];

    // Landed tables and their shapes — LIMIT 0 types without scanning.
    let tables = [];
    let names = db.query("SELECT DISTINCT table_name FROM imports ORDER BY table_name")
        .col("table_name");
    for i in 0..names.count() { tables.push(names.value_at(i)); }

    let schemas = #{};
    for t in tables {
        let head = db.query(`SELECT * FROM "${t}" LIMIT 0`);
        let cols = [];
        for c in head.columns() {
            cols.push(#{ name: c, dtype: head.col(c).dtype() });
        }
        schemas[t] = cols;
    }

    let numeric = false;
    if table in schemas {
        for c in schemas[table] {
            if c.name == column {
                let d = c.dtype;
                numeric = d.starts_with("Int") || d.starts_with("UInt")
                    || d.starts_with("Float") || d.starts_with("Decimal");
            }
        }
    }

    if !numeric {
        // Not a numeric measure — genuinely doesn't fit, like outliers
        // on text.
        out = #{ applicable: false };
    } else {
        // Declared edges as directed pointers; "<->" points both ways.
        // Composite (tuple) endpoints are skipped for now.
        let pointers = [];
        let rels = db.query("SELECT left_path, op, right_path FROM relationships");
        let lcol = rels.col("left_path");
        let ocol = rels.col("op");
        let rcol = rels.col("right_path");
        for i in 0..rels.num_rows() {
            let lp = lcol.value_at(i);
            let rp = rcol.value_at(i);
            if lp.contains("(") || rp.contains("(") { continue; }
            let l = lp.split(".");
            let r = rp.split(".");
            if l.len() != 2 || r.len() != 2 { continue; }
            if !(l[0] in schemas) || !(r[0] in schemas) { continue; }
            pointers.push(#{ src_t: l[0], src_c: l[1], dst_t: r[0], dst_c: r[1] });
            if ocol.value_at(i) == "<->" {
                pointers.push(#{ src_t: r[0], src_c: r[1], dst_t: l[0], dst_c: l[1] });
            }
        }

        // A declared edge's legs are identifiers — they never carry a
        // movement (the f1 lesson: an id-heavy schema floods the
        // conventions otherwise). Excluded from movement pools, served
        // on every anchor as `identifier_columns`.
        let legs = #{};
        for p in pointers {
            legs[`${p.src_t}|${p.src_c}`] = true;
            legs[`${p.dst_t}|${p.dst_c}`] = true;
        }

        // Time axes of a table t: its OWN date columns; only a table
        // with none borrows one declared hop away (journal_lines
        // borrowing journal_entries' date through entry_id).
        let all_axes = #{};
        for t in tables {
            let axes = [];
            for c in schemas[t] {
                if c.dtype.starts_with("Date") || c.dtype.starts_with("Timestamp") {
                    axes.push(#{ label: `${t}.${c.name}`, borrowed: false,
                                 time_col: c.name });
                }
            }
            if axes.len() == 0 {
                for p in pointers {
                    if p.src_t != t || p.dst_t == t { continue; }
                    for c in schemas[p.dst_t] {
                        if c.dtype.starts_with("Date") || c.dtype.starts_with("Timestamp") {
                            let label = `${p.dst_t}.${c.name} via ${p.src_c}`;
                            let seen = false;
                            for a in axes { if a.label == label { seen = true; } }
                            if !seen {
                                axes.push(#{ label: label, borrowed: true, fk: p.src_c,
                                             time_table: p.dst_t, key: p.dst_c,
                                             time_col: c.name });
                            }
                        }
                    }
                }
            }
            all_axes[t] = axes;
        }

        let m_axes = all_axes[table];
        if m_axes.len() == 0 {
            out = #{ applicable: false,
                     reason: `no period axis: no date column on ${table} or one declared hop away` };
        } else {
            let anchors = [];
            let cache = #{};

            for maxis in m_axes {
                // FROM clause for the measure side; s = subject table,
                // tt = borrowed time table.
                let m_from = `"${table}" s`;
                let m_texpr = `s."${maxis.time_col}"`;
                if maxis.borrowed {
                    m_from += ` JOIN "${maxis.time_table}" tt ON s."${maxis.fk}" = tt."${maxis.key}"`;
                    m_texpr = `tt."${maxis.time_col}"`;
                }
                let m_ts = `CAST(${m_texpr} AS TIMESTAMP)`;

                // The axis's NATIVE grain: the coarsest named grain it
                // is already truncated to (a period-keyed snapshot
                // axis); event-grain axes are daily. The anchor aligns
                // at the coarser of its two sides' native grains.
                let mg_key = maxis.label;
                if !(mg_key in cache) {
                    let g = db.query(`SELECT
                        SUM(CASE WHEN date_trunc('year', ${m_ts}) = ${m_ts} THEN 0 ELSE 1 END) AS ny,
                        SUM(CASE WHEN date_trunc('quarter', ${m_ts}) = ${m_ts} THEN 0 ELSE 1 END) AS nq,
                        SUM(CASE WHEN date_trunc('month', ${m_ts}) = ${m_ts} THEN 0 ELSE 1 END) AS nm
                        FROM ${m_from} WHERE ${m_texpr} IS NOT NULL`);
                    if g.cell("ny") == () {
                        cache[mg_key] = "empty";
                    } else if g.cell("ny").parse_int() == 0 {
                        cache[mg_key] = "year";
                    } else if g.cell("nq").parse_int() == 0 {
                        cache[mg_key] = "quarter";
                    } else if g.cell("nm").parse_int() == 0 {
                        cache[mg_key] = "month";
                    } else {
                        cache[mg_key] = "day";
                    }
                }
                let m_native = cache[mg_key];
                if m_native == "empty" { continue; }

                for ev in tables {
                    if ev == table { continue; }

                    // Entity alignments: a declared edge between the two
                    // tables directly, or two edges meeting at the same
                    // dimension key (tb.account_id and jl.account_id both
                    // pointing at chart_of_accounts.account_id).
                    let aligns = [];
                    for p in pointers {
                        if p.src_t == table && p.dst_t == ev {
                            aligns.push(#{ m_col: p.src_c, e_col: p.dst_c });
                        }
                        if p.src_t == ev && p.dst_t == table {
                            aligns.push(#{ m_col: p.dst_c, e_col: p.src_c });
                        }
                    }
                    for p in pointers {
                        if p.src_t != table { continue; }
                        for q in pointers {
                            if q.src_t == ev && q.dst_t == p.dst_t && q.dst_c == p.dst_c
                                && p.dst_t != table && p.dst_t != ev {
                                aligns.push(#{ m_col: p.src_c, e_col: q.src_c });
                            }
                        }
                    }
                    // Borrowed entity (2026-08-14, the medium run's
                    // starvation): a measure table whose only edges are
                    // document-keyed (payments → ar_invoices) reaches a
                    // persistent dimension one hop away — the same move
                    // the borrowed time axis makes. The measure side
                    // joins the document table and keys on ITS dimension
                    // column; the event side must carry the key on a
                    // direct edge of its own.
                    // The via table may BE the event table — receipts
                    // that carry only the invoice reference borrow the
                    // customer from ar_invoices and reconcile against
                    // ar_invoices' own amounts.
                    for p1 in pointers {
                        if p1.src_t != table || p1.dst_t == table { continue; }
                        for p2 in pointers {
                            if p2.src_t != p1.dst_t || p2.dst_t == table || p2.dst_t == ev { continue; }
                            for q in pointers {
                                if q.src_t == ev && q.dst_t == p2.dst_t && q.dst_c == p2.dst_c {
                                    aligns.push(#{ m_col: p2.src_c, e_col: q.src_c,
                                                   via_t: p1.dst_t, via_fk: p1.src_c,
                                                   via_key: p1.dst_c });
                                }
                            }
                        }
                    }
                    let seen = [];
                    let deduped = [];
                    for a in aligns {
                        let via = if "via_t" in a { `${a.via_t}>` } else { "" };
                        let k = `${via}${a.m_col}=${a.e_col}`;
                        if !(k in seen) { seen.push(k); deduped.push(a); }
                    }
                    if deduped.len() == 0 { continue; }

                    let e_axes = all_axes[ev];
                    if e_axes.len() == 0 { continue; }

                    // Movement candidates: the event table's numeric
                    // columns, minus declared-edge legs and the
                    // alignment column itself.
                    let identifier_cols = [];
                    let base_pool = [];
                    for c in schemas[ev] {
                        let d = c.dtype;
                        if !(d.starts_with("Int") || d.starts_with("UInt")
                            || d.starts_with("Float") || d.starts_with("Decimal")) {
                            continue;
                        }
                        if `${ev}|${c.name}` in legs {
                            identifier_cols.push(c.name);
                        } else {
                            base_pool.push(c.name);
                        }
                    }

                    for al in deduped {
                        let terms_pool = [];
                        for c in base_pool {
                            if c != al.e_col { terms_pool.push(c); }
                        }
                        if terms_pool.len() == 0 { continue; }

                        for eaxis in e_axes {
                            let e_from = `"${ev}" s`;
                            let e_texpr = `s."${eaxis.time_col}"`;
                            if eaxis.borrowed {
                                e_from += ` JOIN "${eaxis.time_table}" tt ON s."${eaxis.fk}" = tt."${eaxis.key}"`;
                                e_texpr = `tt."${eaxis.time_col}"`;
                            }
                            let e_ts = `CAST(${e_texpr} AS TIMESTAMP)`;

                            let eg_key = eaxis.label;
                            if !(eg_key in cache) {
                                let g = db.query(`SELECT
                                    SUM(CASE WHEN date_trunc('year', ${e_ts}) = ${e_ts} THEN 0 ELSE 1 END) AS ny,
                                    SUM(CASE WHEN date_trunc('quarter', ${e_ts}) = ${e_ts} THEN 0 ELSE 1 END) AS nq,
                                    SUM(CASE WHEN date_trunc('month', ${e_ts}) = ${e_ts} THEN 0 ELSE 1 END) AS nm
                                    FROM ${e_from} WHERE ${e_texpr} IS NOT NULL`);
                                if g.cell("ny") == () {
                                    cache[eg_key] = "empty";
                                } else if g.cell("ny").parse_int() == 0 {
                                    cache[eg_key] = "year";
                                } else if g.cell("nq").parse_int() == 0 {
                                    cache[eg_key] = "quarter";
                                } else if g.cell("nm").parse_int() == 0 {
                                    cache[eg_key] = "month";
                                } else {
                                    cache[eg_key] = "day";
                                }
                            }
                            let e_native = cache[eg_key];
                            if e_native == "empty" { continue; }
                            // A day-native alignment also serves a month
                            // variant (2026-08-14): document-keyed pairs
                            // and sparse dimension pairings share too few
                            // (entity, day) cells — per-month cells pair
                            // densely, and the viability probe and the
                            // wrong-anchor gates keep whichever grain
                            // reconciles. Native first, so the first
                            // anchor per event keeps its old meaning.
                            let native = coarser(m_native, e_native);
                            let grains = [native];
                            if native == "day" { grains.push("month"); }
                            for grain in grains {

                            // A cumulative that RESETS — season
                            // standings, year-to-date balances — is a
                            // stock only inside its scope; at raw
                            // entity grain every boundary injects a
                            // full period's error and the wrong-anchor
                            // gate (rightly) abstains. Recall-first:
                            // serve the raw alignment AND a year-scoped
                            // variant; the gates kill the wrong one
                            // (found on rel-f1, 2026-08-06: 74 seasons
                            // of standings abstained at driver grain,
                            // reconcile exactly at driver-season grain).
                            for scope in if grain == "year" { ["none"] } else { ["none", "year"] } {
                            // Borrowed entity: the measure side joins the
                            // document table and keys on ITS dimension
                            // column; `dd` never collides with the
                            // borrowed-time alias `tt`.
                            let am_from = m_from;
                            let am_base = `s."${al.m_col}"`;
                            let alabel = al.m_col;
                            let align_label = `${table}.${al.m_col} = ${ev}.${al.e_col}`;
                            if "via_t" in al {
                                am_from += ` JOIN "${al.via_t}" dd ON s."${al.via_fk}" = dd."${al.via_key}"`;
                                am_base = `dd."${al.m_col}"`;
                                alabel = `${al.via_t}>${al.m_col}`;
                                align_label = `${table} via ${al.via_t}.${al.m_col} = ${ev}.${al.e_col}`;
                            }
                            let m_e = am_base;
                            let e_e = `s."${al.e_col}"`;
                            if scope == "year" {
                                m_e = `CAST(${am_base} AS VARCHAR) || '@' || CAST(extract(year FROM ${m_ts}) AS VARCHAR)`;
                                e_e = `CAST(s."${al.e_col}" AS VARCHAR) || '@' || CAST(extract(year FROM ${e_ts}) AS VARCHAR)`;
                            }

                            // Viability, on one probe: fewer than two
                            // entities with 4+ periods cannot clear the
                            // voting floor — the raceId-style alignment
                            // (one bucket per entity) dies here before
                            // either side is scanned wide.
                            let vkey = `viable|${maxis.label}|${alabel}|${grain}|${scope}`;
                            if !(vkey in cache) {
                                let vq = db.query(`SELECT count(*) AS v FROM
                                    (SELECT ${m_e} AS e FROM ${am_from}
                                     WHERE ${am_base} IS NOT NULL AND ${m_texpr} IS NOT NULL
                                       AND s."${column}" IS NOT NULL
                                     GROUP BY 1
                                     HAVING count(DISTINCT date_trunc('${grain}', ${m_ts})) >= 4) AS g`);
                                let v = vq.cell("v");
                                cache[vkey] = if v == () { 0 } else { v.parse_int() };
                            }
                            if cache[vkey] < 2 {
                                anchors.push(#{ event: ev,
                                    align: align_label,
                                    measure_time: maxis.label,
                                    event_time: eaxis.label,
                                    grain: grain,
                                    scope: scope,
                                    viable_entities: cache[vkey],
                                    identifier_columns: identifier_cols,
                                    verdict: "abstain",
                                    reason: "fewer than two entities carry 4+ periods on this alignment" });
                                continue;
                            }

                            // Measure-side series, cached per (axis,
                            // entity column, grain, scope) — the kernel
                            // consumes the grouped result directly.
                            let ykey = `${maxis.label}|${alabel}|${grain}|${scope}`;
                            if !(ykey in cache) {
                                cache[ykey] = db.query(`SELECT ${m_e} AS e,
                                    date_trunc('${grain}', ${m_ts}) AS b,
                                    SUM(CAST(s."${column}" AS DOUBLE)) AS yv
                                    FROM ${am_from}
                                    WHERE ${am_base} IS NOT NULL AND ${m_texpr} IS NOT NULL
                                      AND s."${column}" IS NOT NULL
                                    GROUP BY 1, 2 ORDER BY 1, 2`);
                            }
                            let yq = cache[ykey];

                            // Event-side sums for every term at once.
                            let sel = "";
                            for c in terms_pool {
                                sel += `, SUM(CAST(s."${c}" AS DOUBLE)) AS "s_${c}"`;
                            }
                            let mq = db.query(`SELECT ${e_e} AS e,
                                date_trunc('${grain}', ${e_ts}) AS b${sel}
                                FROM ${e_from}
                                WHERE s."${al.e_col}" IS NOT NULL AND ${e_texpr} IS NOT NULL
                                GROUP BY 1, 2 ORDER BY 1, 2`);

                            // The discriminator — alignment, conventions,
                            // residuals, gates — in one kernel call.
                            let rec = reconcile(yq, mq, terms_pool);
                            let n_common = rec.n_common;
                            let summaries = rec.summaries;
                            for i in 0..summaries.len() {
                                summaries[i].support =
                                    wilson_lcb(summaries[i].winners, n_common);
                            }

                            // Winner: support-first; on a support tie the
                            // fewer-term convention wins — unless the
                            // higher-arity fit is decisive, ΔBIC > 10
                            // (Kass–Raftery), v0.3's tiebreak, ported
                            // 2026-08-06. A pair difference that fits
                            // exactly beats an equally supported single
                            // that fits loosely; anything short of
                            // decisive keeps the simpler reading.
                            let winner = ();
                            for s in summaries {
                                if s.verdict == "abstain" { continue; }
                                if winner == () { winner = s; continue; }
                                if s.support > winner.support + 1.0e-9 { winner = s; continue; }
                                if abs(s.support - winner.support) <= 1.0e-9
                                    && s.terms != winner.terms {
                                    let simple = if s.terms < winner.terms { s } else { winner };
                                    let complex = if s.terms < winner.terms { winner } else { s };
                                    winner = if ("bic" in simple) && ("bic" in complex)
                                        && simple.bic - complex.bic > 10.0 {
                                        complex
                                    } else {
                                        simple
                                    };
                                }
                            }

                            // The runner-up field the judge sees beside
                            // the verdict: most-voted first, capped.
                            let alts = [];
                            let used = if winner != () { [winner.convention] } else { [] };
                            while alts.len() < 4 {
                                let best = ();
                                for s in summaries {
                                    if s.convention in used { continue; }
                                    if best == () { best = s; continue; }
                                    if s.voted > best.voted
                                        || (s.voted == best.voted && s.support > best.support) {
                                        best = s;
                                    }
                                }
                                if best == () { break; }
                                used.push(best.convention);
                                alts.push(#{ convention: best.convention,
                                             verdict: best.verdict,
                                             voted: best.voted,
                                             support: best.support });
                            }

                            let anchor = #{ event: ev,
                                            align: align_label,
                                            measure_time: maxis.label,
                                            event_time: eaxis.label,
                                            grain: grain,
                                            scope: scope,
                                            entities: n_common,
                                            identifier_columns: identifier_cols,
                                            alternatives: alts };
                            if winner != () {
                                anchor.verdict = winner.verdict;
                                anchor.convention = winner.convention;
                                anchor.voted = winner.voted;
                                anchor.agreement = winner.agreement;
                                anchor.support = winner.support;
                                if "r_flow" in winner { anchor.r_flow = winner.r_flow; }
                                if "r_stock" in winner { anchor.r_stock = winner.r_stock; }
                                // The sign partition: a mirror-heavy count
                                // says the store carries the negated
                                // convention (ledger-signed); the judge
                                // reads it before any P&L split.
                                if "sign_primary" in winner {
                                    anchor.sign = #{ primary: winner.sign_primary,
                                                     mirror: winner.sign_mirror,
                                                     both: winner.sign_both };
                                }
                            } else {
                                anchor.verdict = "abstain";
                                let any_votes = false;
                                for s in summaries { if s.voted > 0 { any_votes = true; } }
                                anchor.reason = if any_votes {
                                    "votes below the candidacy floor (2 voters, 0.8 agreement)"
                                } else {
                                    "no entity series reconciled: wrong anchor, short series, or dead values"
                                };
                            }
                            anchors.push(anchor);
                            } // for scope (flat-indented, like its body)
                            } // for grain in grains
                        }
                    }
                }
            }

            if anchors.len() == 0 {
                out = #{ applicable: false,
                         reason: `no declared relationships connect ${table} to an event table with a time axis` };
            } else {
                out = #{ applicable: true, anchors: anchors };
            }
        }
    }
}

out
