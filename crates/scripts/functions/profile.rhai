// Column profile (fixture 11): the deterministic profile plane as one
// measurement — counts, distribution shape, string lengths, top values.
// It measures the landed table — the recipe carried the casts (ruled
// 2026-08-04), so null_ratio counts what the author's try_casts
// surrendered and min/max order as their types — v0.3's gate exactly
// (statistics ran on the typed table). Numeric facts appear when the
// values read as numbers; skewness and kurtosis stayed behind: nothing
// consumed them.
let parts = subject.split(".");
let column = parts[1];
let t = db.query(`SELECT "${column}" AS v FROM "${parts[0]}"`);
let c = t.col("v");
let n = c.count();
let nulls = c.null_count();
let non_null = n - nulls;
let distinct = c.distinct();

let profile = #{
    total: n,
    null_ratio: if n == 0 { 0.0 } else { nulls.to_float() / n.to_float() },
    distinct: distinct,
    cardinality_ratio: if non_null == 0 { 0.0 } else { distinct.to_float() / non_null.to_float() },
    min: c.min(),
    max: c.max(),
    // Exact Shannon entropy (nats) over the full non-null distribution.
    // top_values is a display cap (readable buckets for the judge);
    // entropy is the statistic scores read — never the buckets
    // (2026-08-06: the truncated relevance lower bound is retired).
    entropy: c.entropy(),
    top_values: c.top_k(20),
};

let lengths = c.len_stats();
if lengths != () {
    profile.lengths = lengths;
}

let mean = c.mean();
if mean != () {
    profile.numeric = #{
        mean: mean,
        stddev: c.stddev(),
        mad: c.mad(),
        percentiles: #{
            p01: c.percentile(0.01),
            p25: c.percentile(0.25),
            p50: c.percentile(0.5),
            p75: c.percentile(0.75),
            p99: c.percentile(0.99),
        },
    };
}

// Extraction serves this alone (ruled 2026-08-14: 65 columns of full
// bodies through the door was the fan-out tax) — the judge reads the
// cached body via GLOSSARY when judging, not while warming.
profile.summary = #{
    total: n,
    null_ratio: profile.null_ratio,
    distinct: distinct,
    note: "cached — the full profile reads back via GLOSSARY(table.column::profile)",
};

profile
