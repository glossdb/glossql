---
name: glossql-dimensions
description: The dimensional read of a glossql dataset — score slice axes with dimension_relevance, judge hierarchy candidates from detect_hierarchies, and record grain-checked judged joins. Use after tables are glossed and relationships declared, before cross-table analysis or metric work.
---

# The dimensions deliverable

Three parts, one judging discipline: which axes slice the data
(inventory + relevance), how they nest (hierarchies), and the judged
joins that put judged dimensions beside each fact. Run it after the
add-source flow (roles glossed) and the relationships plane (edges
declared). The workspace ships both measurements at boot.

## 1. The vocabulary ships

The verdict aspect and its witness boot with the KPI kit — `dimension`
on columns, values `primary | supporting | none` plus `grounds`.
Nothing to declare; gloss straight into it.

`primary` and `supporting` are absolute labels, not ranks — an
ordinal priority means nothing without knowing what it is a rank *of*,
and ties at the floor sort arbitrarily. `none` is the **judged negative** — you examined the
axis and it is not one (a label, a sequence number, a join key);
grounds say why. It is a different fact from an `unassessed` row,
which only means nobody has judged yet — a reader planning slice work
needs to tell the two apart.

## 2. Inventory and relevance

For each dimension-role column (and any categorical axis worth
considering):

```glossql
SELECT profile() FROM orders.region;
SELECT dimension_relevance() FROM orders.region;
SELECT value FROM GLOSSARY(orders.region::dimension_relevance) WHERE state = 'current';
```

The score is `coverage × evenness` (Pielou), zero free parameters, on
one scale for every axis. How to read it:

- **The number answers "is this axis usable, how much does it
  resolve" — interest is yours.** Which of an even 4-way `region` and
  an even 800-way `product_id` a reader wants first is business
  judgment; the score never overrules it. Even distribution is not
  analytic interest either — a near-uniform sequence column (a round
  number, a line number) scores high and is still `none`. Gloss
  `dimension` with your verdict and grounds.
- **The score is exact.** Evenness reads the profile's exact entropy
  scalar over the full distribution; the 20 `top_values` are display
  for your judgment, never the score's input.
- **Abstentions are gates, not defects**: near-keys (fraction ≥ 0.9 of
  filled rows — a key is not an axis), null-dominated columns
  (> 0.5), constants. A null-coded binary (`{X, NULL}`) is admitted —
  NULL is a bucket — but scores low through coverage; whether the lane
  matters is your call.

## 3. Hierarchies

```glossql
SELECT detect_hierarchies() FROM orders;
SELECT value FROM GLOSSARY(orders::hierarchy_candidates) WHERE state = 'current';
```

Candidates are within-table FD screens at high recall (g3 ≤ 0.05 —
the fraction of rows breaking `from → to`). Judge each:

- **λ < 0.5 is the vacuous-skew signature.** A ≥98%-dominant dependent
  passes the FD screen vacuously — knowing `zip` "determines" a flag
  that is almost always A predicts nothing. Measured: a pre-registered
  λ floor killed 48 such false positives with zero truth lost. Treat a
  low-λ candidate as noise unless the data argues otherwise.
- **A perfect 1:1 (`kind: alias`) is a relabeling or a coincidence,
  and only meaning separates them.** A code↔label bijection
  (`city_code ↔ city`) collapses to one canonical axis; an entity key
  that happens to align with a per-row timestamp must not. Unsure?
  Leave both, say so in prose — never merge silently.
- **Same-family role columns stay apart, however cleanly they align.**
  An origin and a destination, a bill-to and a pay-to — merging them
  silently corrupts every aggregation that crosses them.
- **Reduce transitively.** `zip → city` and `city → state` imply
  `zip → state`; the measurement serves all three (recall), you
  declare the chain, not the shortcut.

Record a surviving nest as a same-table relationship, finer → coarser,
and gloss the grounds on the pair:

```glossql
DECLARE RELATIONSHIP orders.zip -> orders.city;
DECLARE RELATIONSHIP orders.city -> orders.state;
GLOSS meaning ON orders.zip -> orders.city AS $${"value": "postal drill-down; g3 0, judged non-vacuous"}$$;
```

## 4. The enriched read

The substrate does not persist views — `CREATE VIEW` is refused by
design: grain-checked joins are the construct, in the spirit of the
language. The deliverable is the *judged join*:
which joins extend a fact without corrupting it, recorded so every
later query can use them. Before trusting any join, run the
grain check — the cheapest verification of the most consequential
property a join has:

```glossql
SELECT count(*) FROM orders;
SELECT count(*) FROM orders o JOIN customers c ON o.customer_id = c.id;
```

Equal counts, exactly, or the join is not grain-preserving: a fan-out
multiplies every downstream aggregate — fail the flow rather than
ship one. In a one-hop star the probes are independent — check
each join alone. A fact-to-fact join that *drops* rows instead needs
`LEFT JOIN` to keep the fact whole; say which in the gloss. Record
the verdicts as prose on the relationship pairs (the grain-check
numbers are the grounds). Carry the dimension columns you judged
worth carrying, not everything; a conformed dimension shared across
facts needs its concept named in prose, and alias axes collapse to
one canonical column while role pairs never do.
