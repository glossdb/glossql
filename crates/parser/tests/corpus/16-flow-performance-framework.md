# 16 · Flow: the performance framework — TRANSCRIBES (the target as statements)

Source: the target scorecard
(`reports/2026-08-05-scorecard-performance-framework.md`) against the
generator's real shapes (`../dataraum-testdata/output/clean`, seed 42;
`ground_truth.yaml` is the oracle). The operating-model deliverable as
a statement sequence: fixtures 01/03/04/07's ruled shapes, exercised
end to end for the first time. Presented and ruled 2026-08-06:
windows are read policy; `read.` table functions are the
value-at-read spelling, bound when the UI transformation starts;
the scorecard runs first with the reader inlining served SQL.

The floor under it (fixtures 11 + the dimensions flow) has already
produced: `entity` glosses with verified grain, `behavior` verdicts
evidenced by `behavior_evidence`, declared relationships carrying
grain-check grounds, units and the sign convention. Every grounding
below cites that floor in its assumptions — this is what "ground the
aspects with the information we expose over functions" means in
statements.

## 1. The metric vocabulary (fixture 01/03's shape)

One QUERY aspect per concept, on the dataset (the lead's placement,
2026-08-05). Base concepts and derived metrics declare uniformly —
fixture 03's finding, a metric *is* a concept; the difference is
whether its SQL half is an extract (§2) or a formula over siblings
(§4). Four of the scorecard's roster shown:

```glossql
USE fin;

DECLARE ASPECT revenue WITH $${
  "title": "Revenue",
  "description": "Income recognized in the ledger",
  "x-kind": "measure", "x-unit": "currency"
}$$ AS QUERY ON DATASET;

DECLARE ASPECT accounts_receivable WITH $${
  "title": "Accounts Receivable",
  "description": "Open receivables — a stock at (account, period) grain",
  "x-kind": "measure", "x-unit": "currency"
}$$ AS QUERY ON DATASET;

DECLARE ASPECT dso WITH $${
  "title": "Days Sales Outstanding",
  "description": "Average days to collect a receivable",
  "x-kind": "metric", "x-unit": "days"
}$$ AS QUERY ON DATASET;

DECLARE ASPECT gross_profit WITH $${
  "title": "Gross Profit",
  "description": "Revenue less expenses",
  "x-kind": "metric", "x-unit": "currency"
}$$ AS QUERY ON DATASET;
```

## 2. Concepts ground as extracts (fixture 07's shape, taken seriously)

A grounding carries **no grain**: it is the semantic core — scoping,
signs, the grain-preserving joins composed inline (`journal_lines`
has no date; entries carry time, accounts carry type) — served as a
row-grain relation with the time axis and the judged dimensions as
columns. Everything a window or slice needs rides *on* the relation;
nothing is aggregated before a reader asks:

```glossql
GLOSS revenue ON fin AS $${
  "sql": "SELECT e.date, l.credit - l.debit AS value, l.cost_center, l.currency FROM journal_lines l JOIN journal_entries e ON l.entry_id = e.entry_id JOIN chart_of_accounts a ON l.account_id = a.account_id WHERE a.account_type = 'revenue'",
  "assumptions": [
    {"dimension": "sign", "assumption": "revenue accounts carry credit balances; value = credit - debit",
     "basis": "conventions gloss (natural balance)", "confidence": 0.95},
    {"dimension": "grain", "assumption": "both joins are grain-preserving; no header-value multiplication",
     "basis": "relationship glosses (grain-check counts on the pairs)", "confidence": 1.0},
    {"dimension": "behavior", "assumption": "a flow: sums are valid over any partition",
     "basis": "behavior_evidence on journal_lines.credit", "confidence": 0.95}
  ]
}$$;

GLOSS accounts_receivable ON fin AS $${
  "sql": "SELECT t.period, t.account_id, t.debit_balance - t.credit_balance AS value FROM trial_balance t JOIN chart_of_accounts a ON t.account_id = a.account_id WHERE a.account_type = 'asset' AND a.name LIKE '%Receivable%'",
  "assumptions": [
    {"dimension": "behavior", "assumption": "a stock at (account, period) source grain: a window takes its LAST period, never a sum across periods",
     "basis": "behavior_evidence on trial_balance.debit_balance", "confidence": 0.95},
    {"dimension": "scope", "assumption": "AR accounts selected by name within asset type",
     "basis": "chart_of_accounts meaning glosses", "confidence": 0.85}
  ]
}$$;
```

A stock's extract is bounded by its **source grain** — `trial_balance`
speaks per period; no read can answer finer, and the extract says so
by serving `period` as-is.

## 3. Grain and slice are the reader's — the period-grain fork dissolves

Grain is an open axis (decade … year, quarter, month, week, day) and
so is slice (cost center, geography, any judged dimension). Baking
any enumeration into a grounding is pre-evaluation — the exact
disease the drill re-think kills. The coherent rule:

- **Definitions are grain-free** (§2's extracts, §4's formulas).
- **Possibilities are exposed once, as judged knowledge** — what v0.3
  pre-evaluated per metric, the floor already serves per column:
  `temporal()` cadence says which time grains the data can answer,
  `dimension` verdicts (with `none`) say which axes slice, `behavior`
  says sum-vs-last, `unit` guards cross-currency sums.
- **Evaluation is composition at read**, any window, any slice
  (the §6 spelling; until the bind lands, the reader inlines the
  served SQL through the door):

```glossql
SELECT date_trunc('month', date) AS month, sum(value) FROM read.revenue() GROUP BY 1 ORDER BY 1;
SELECT date_trunc('decade', date) AS decade, sum(value) FROM read.revenue() GROUP BY 1;
SELECT cost_center, sum(value) FROM read.revenue() GROUP BY 1 ORDER BY 2 DESC;
```

The window a given reader picks is theirs — the app defaults to
month, another reader asks by day (lead, 2026-08-06); the same
definitions answer both.

A ratio is evaluated per window through its formula (§4): the agent
composes DSO at month m from the components at m — and at year, week,
or any window the source grains can answer, from the same pinned
definition. Nothing enumerates windows anywhere.

The framework's **reporting windows** are a different fact — the lead
is right that operating performance monitoring is always per time
period: the target names monthly and annual, and that is *read
policy* (the scorecard's, the app's), not the metric's definition. If
it wants a durable home, it is one FACT gloss on the dataset; until a
consumer asks, the target carries it.

**What a read proves may be recorded.** Pre-evaluation is computing
before anyone asks; *recording* is keeping what a real read proved —
the judged-join pattern. A composed evaluation the agent has verified
against the oracle may land as the metric's own QUERY gloss (the
durable executable knowledge — v0.3's snippet economy, post-record
not pre-compute), superseding as definitions change; re-windowing
recomposes from §2's extracts.

## 4. The formula DAG (formulas are FACT aspects, SPEC §5.1)

A derived metric's definition is its formula — window-generic, the
window `w` its one free variable. In-blob, one aspect — multiplicity
lives inside the schema:

```glossql
DECLARE ASPECT formulas WITH $${
  "type": "object",
  "properties": {"formulas": {"type": "object"}}
}$$ AS FACT ON DATASET;

GLOSS formulas ON fin AS $${
  "formulas": {
    "gross_profit": "revenue[w] - expenses[w]",
    "dso": "accounts_receivable[end of w] / revenue[w] * days[w]",
    "revenue_growth_pct": "(revenue[w] - revenue[w-1]) / revenue[w-1]"
  }
}$$;
```

The formula is the pinned definition the scorecard calls
definition-sensitive — it covers *every* window because it names
none. Evaluation composes it from §2's extracts at the reader's
window; a verified composition may be recorded as the metric's own
QUERY gloss (§3). Two recorded evaluations arriving at the same
number is a correct state; whether they reconcile is witness
territory (fixture 07's coexistence finding).

## 5. Validations (fixture 04's ruled shape — approved 2026-08-06)

Expectation as a FACT gloss; the check as a thin function **voice**
(`RETURNS` the aspect, full door, `ACCEPTS (imports)` for freshness);
a detector bands the slots; ATTEST is the verdict surface. Two of the
scorecard's four shown — the exact one and the one whose expectation
is deliberately not zero:

```glossql
DECLARE ASPECT journal_balanced WITH $${
  "type": "object", "required": ["outcome"],
  "properties": {
    "outcome": {"type": "string"},
    "tolerance": {"type": "number"},
    "severity": {"enum": ["critical", "warning", "info"]}
  }
}$$ AS FACT ON TABLE;

GLOSS journal_balanced ON journal_lines AS $${
  "outcome": "Total debits equal total credits, exactly.",
  "tolerance": 0.0,
  "severity": "critical"
}$$;

DECLARE ASPECT bank_reconciliation WITH $${
  "type": "object", "required": ["outcome"],
  "properties": {
    "outcome": {"type": "string"},
    "expected_rate": {"type": "number"},
    "tolerance": {"type": "number"},
    "severity": {"enum": ["critical", "warning", "info"]}
  }
}$$ AS FACT ON TABLE;

GLOSS bank_reconciliation ON bank_transactions AS $${
  "outcome": "The reconciled fraction matches the source's own dirt; 1.0 means overcleaning and is itself a failure.",
  "expected_rate": 0.895,
  "tolerance": 0.02,
  "severity": "warning"
}$$;

DECLARE FUNCTION journal_balance_check FOR fin
  AS $$/* debits equal credits, as a breach rate */$$
  ACCEPTS (imports) RETURNS journal_balanced;
DECLARE FUNCTION reconciliation_check FOR fin
  AS $$/* the reconciled fraction against the source's own dirt */$$
  ACCEPTS (imports) RETURNS bank_reconciliation;
DECLARE FUNCTION balance_bands FOR fin AS $$/* detector: bands the balance slots */$$;

DECLARE WITNESS journal_balanced_w ON journal_balanced BY (AGENT, HUMAN)
  DETECTOR balance_bands THRESHOLD 0.5;
DECLARE WITNESS bank_reconciliation_w ON bank_reconciliation BY (AGENT, HUMAN)
  DETECTOR balance_bands THRESHOLD 0.5;

SELECT journal_balance_check() FROM journal_lines;
SELECT reconciliation_check() FROM bank_transactions;
SELECT * FROM ATTEST(journal_lines::journal_balanced);
```

A check voice **speaks the aspect's schema** (fixture 06's
respelling, enforced by the engine): its output must carry `outcome`
like any slot — the verdict in words, the measurement beside it
(`{"outcome": "measured: debits equal credits", "imbalance": 0.0}`).
One schema, every speaker.

A confirmed behavior_evidence convention is a validation candidate:
`trial_balance.debit_balance = SUM(journal_lines.debit)` reconciled
at ~0 residual, so the agent may promote it to a third check — the
measurement plane produces the hypotheses, the skill teaches the
promotion.

## 6. Reading the framework — and FORK B

The framework's standing state is two reads; the app's metric DAG is
the same rows drawn:

```glossql
SELECT subject, band, score FROM ATTEST(fin) WHERE band = 'red';
SELECT subject, aspect, value FROM GLOSSARY(fin) WHERE state = 'current';
```

A metric's *value*: the reader takes the served SQL and runs it
through the door — the scorecard runs this way (run-first, ruled
2026-08-06). The bind lands when the UI transformation starts:
**value-at-read as a namespaced table function** (ruled 2026-08-06,
closing the §9-parked enhancement) — the subject aspect's current
QUERY gloss (an extract, or a recorded evaluation) expanded at read,
the value arriving as an ordinary relation the reader composes
around:

```glossql
SELECT date_trunc('week', date) AS week, sum(value) FROM read.revenue() GROUP BY 1;
SELECT * FROM read.dso();
```

`GLOSSARY` and `ATTEST` are the only FROM-position functions today,
both grammar-fixed; `read.` is the first user-named family and the
namespace prevents table collisions. The spelling parses under the
grammar already — binding is engine work on the relation-planning
path the session owns, zero grammar growth. A `::` spelling
(`metric::dso()`) was considered and rejected: vocabulary-consistent
with the aspect cast, but `::` in FROM position is not the
substrate's SQL grammar, and rewriting query text ahead of the
engine is a parallel-layer move. The division of labor: `::` names
an aspect in glossql argument position (`GLOSSARY(fin::dso)`); `.`
names a runnable relation in SQL position. The ruled line holds: no
script, no cache, no voice — running stays the reader's act, spelled
inside the reader's SQL. The seam to respect: composing around the
*output* is free, but a ratio cannot be re-scoped from its output
rows — drilling DSO by country means re-scoping its components per
the `formulas` gloss, not regrouping `read.dso()`.

## Findings

- **Exercises 01/03/04/07 end to end for the first time** — declare →
  ground → check → attest as one sequence over real shapes, no new
  constructs anywhere.
- **The checks are the first workspace-authored functions**
  (`FOR fin`, not `FOR GLOBAL`): the bootstrap ships measurements,
  the framework authors its own checks. Library content stays
  wipeable; the framework is workspace knowledge.
- **The period-grain fork dissolved** (§3) instead of being chosen:
  definitions are grain-free, possibilities are the floor's judged
  knowledge served once, evaluation is composition at read, and what
  a read proves may be recorded — pre-evaluation and recording are
  different acts. Fixture 03's parameter question closes with it: the
  parameter was the window, and the window belongs to the reader.
- **Value-at-read ruled** (§6): `read.` table functions, bound when
  the UI transformation starts; run-first until then. Fixture 15
  still holds the conformed-group fork. The prefix was `metric.`
  until 2026-08-11, renamed `read.` when it was ruled the one generic
  serving door over every QUERY gloss — no alias; the flavor lives in
  `x-kind`, never in the call syntax.
- The `expected_rate: 0.895` gloss is the anti-overcleaning stance in
  one statement: the expectation is authored, never assumed zero.
- **The pinning agenda** (added 2026-08-06, the run-7 lesson): the
  scorecard run reproduced every uniquely-defined number exactly, and
  its only misses were unflagged *definition choices* — the agent
  stated each with grounds but presented them as judgments made, not
  decisions owed. The flow now ends with every definitional
  assumption served to the user as a multiple-choice question; the
  pin is a human re-gloss (the human slot outranks the agent's — a
  tested collapse invariant). Beside the questions and the data
  findings, a third list: **world-coverage wishes** — assumptions
  only more world can settle (an opening balance sheet anchoring
  every cumulative level), named as specific asks with the numbers
  they would shift. A witness convention banding agent-only
  definitional glosses yellow was ruled *later* by the lead.
- INFORMATION LOST (accepted): v0.3's interpretation ranges
  (EXCELLENT/GOOD bands per metric) stay in the aspect's `WITH`
  annotations when wanted — rendering guidance, not glossary content.
