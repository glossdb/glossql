# 28 · Grounding keys: behaviour, row identity, slices — RULED (C: the grounding contract, the claims as glosses on the view's columns)

Source: three artifacts the corpus and the docs already hold, and one
host. The City of Baton Rouge revised budget (fixture 27): a flow booked
in the month it was authorized, sliced by fund and by object
(`segment2`); its grounding in `docs/methods/plan.md` carries
`behavior` and `axes`. The finance integration run's `ar_balance`
(fixture 26): a stock, its row identity declared as `grain`. The
harmonisation demo's service level: the share of a system's rows that
keep their required fields, by system of record — a ratio, neither
stock nor flow, which the fixed enum cannot say. The host is the DuckDB
extension (this repository), where a grounding is a view in the catalog and
a view and its columns are subjects like a table's, glossed, measured
and owed (its views suite).

What the engine reads of a grounding: `sql` or `stopped`, to build or
end the view; `assumptions`, for disclosure and the question round.
What reads `behavior`, `grain` and `axes`: the cube alone
(`crates/session/src/cube.rs`), to combine the view's rows over periods
and members and to validate the frame against its row identity.

## Transcription

Three forms for the same artifacts. All parse; the fork is where a
claim about the view lives.

**A. The fixed schema holds.** Six keys, closed, as §5.2 stands. The
budget as the docs spell it:

```glossql
GLOSS revised_budget ON brla AS $${
  "sql": "SELECT fiscalmonth, revisedbudget AS value, fund, segment2 FROM expense_budget",
  "behavior": "flow",
  "axes": ["fund", "segment2"]
}$$;
```

The service level carries no key the enum holds: `ratio` is refused at
admission, so its author writes nothing, or `flow`, which is wrong.

**B. The contract, and a grounding schema the kit declares**
(`.claude/plans/grounding.md`). The body validates against the
contract, `sql` or `stopped` and `assumptions`, then against every
aspect flavored `x-kind: grounding`; the KPI kit declares today's three
keys once, closed, and widens the enum:

```glossql
DECLARE ASPECT kpi_grounding WITH $${
  "x-kind": "grounding",
  "type": "object", "additionalProperties": false,
  "properties": {
    "sql": {"type": "string"}, "stopped": {"type": "string"},
    "assumptions": {"type": "array"},
    "behavior": {"enum": ["stock", "flow", "ratio"]},
    "grain": {"type": "array", "items": {"type": "string"}},
    "axes": {"type": "array", "items": {"type": "string"}}
  }
}$$ AS FACT ON DATASET;
```

The budget's statement is A's, unchanged. The claim still rides the
body: one slot holds `sql` and `behavior` together.

**C. The contract, and the claims as glosses on the view's columns.**
The body is `sql` or `stopped` and `assumptions`. The view is a table
of its dataset (§3), and its columns carry the kit's column vocabulary,
the words a landed table's columns carry:

```glossql
GLOSS revised_budget ON brla AS $${
  "sql": "SELECT fiscalmonth AS period, revisedbudget AS value, fund, segment2 FROM expense_budget"
}$$;

GLOSS role ON revised_budget.period AS $${"value": "timestamp"}$$;
GLOSS role ON revised_budget.value AS $${"value": "measure"}$$;
GLOSS behavior ON revised_budget.value AS $${
  "value": "flow",
  "grounds": "a row is the amount booked in its month; the year is the sum"
}$$;
GLOSS role ON revised_budget.fund AS $${"value": "dimension"}$$;
GLOSS dimension ON revised_budget.fund AS $${"value": "primary"}$$;
GLOSS role ON revised_budget.segment2 AS $${"value": "dimension"}$$;
GLOSS dimension ON revised_budget.segment2 AS $${"value": "primary"}$$;
GLOSS entity ON revised_budget AS $${
  "value": "a budget line's month",
  "grain": ["period", "fund", "segment2"]
}$$;
```

The stock says its level and its row identity the same way:

```glossql
GLOSS role ON ar_balance.date AS $${"value": "timestamp"}$$;
GLOSS behavior ON ar_balance.value AS $${"value": "stock"}$$;
GLOSS entity ON ar_balance AS $${
  "value": "the receivable at a day's end",
  "grain": ["date"]
}$$;
```

The ratio serves its halves as columns and names them, so a coarser
period or a total over members re-divides the summed halves:

```glossql
GLOSS service_level ON demo AS $${
  "sql": "SELECT system, count(*) FILTER (WHERE required_kept) AS kept, count(*) AS total, count(*) FILTER (WHERE required_kept) / count(*) AS value FROM harmonised GROUP BY system"
}$$;

GLOSS role ON service_level.value AS $${"value": "measure"}$$;
GLOSS behavior ON service_level.value AS $${
  "value": "ratio", "numerator": "kept", "denominator": "total"
}$$;
GLOSS role ON service_level.system AS $${"value": "dimension"}$$;
```

The kit's word, widened by the ratio and its halves:

```glossql
DECLARE ASPECT behavior WITH $${
  "type": "object", "required": ["value"],
  "properties": {"value": {"enum": ["stock", "flow", "ratio", "none"]},
                 "numerator": {"type": "string"},
                 "denominator": {"type": "string"},
                 "grounds": {"type": "string"}}
}$$ AS FACT ON COLUMN WHEN role = 'measure';
```

## Findings

- **A and B key a claim about one column of the view by the whole
  grounding.** One slot holds `sql` and `behavior`: a human's ruling on
  the behaviour supersedes the agent's `sql` with it, no test bands
  the behaviour alone, no measurement sits beside it as evidence, and
  a metric read as another grounding's source carries none of a
  table's facts.
- **C gives each claim the slot a landed column's claim has.** The
  test on `behavior` bands it, the behaviour evidence is a
  measurement on the column, a ruling lands on the one fact, `next`
  routes what a new metric's columns owe, and the ratio is sayable
  with its halves. The cost is statements: nine where A wrote one.
  Most are proposed rather than written: a detector reads the view's
  sources through lineage and proposes its columns' roles, and the
  test bands what it proposed.
- **The no-claim rule.** At a coarser grain than the period column's
  cadence, with no behaviour on the value column, a reader refuses and
  names the gloss. A silent default is wrong for a stock and teaches
  nothing.
- **The SPEC diff under C.** §5.1: "the standard grounding schema"
  becomes "the grounding contract". §5.2: the block keeps `oneOf`,
  `sql`, `stopped` and `assumptions`; the three bullets go, and one
  stands in their place: a grounding's view is a table of its dataset,
  its columns carry the claims a reader needs under the aspects a kit
  declares, and a reader that combines its rows refuses while the
  claim it needs is unglossed, naming the gloss. B's sentence on a
  kit-declared grounding schema stays for a key a body truly needs;
  the KPI kit declares none. Fixtures 07 and 26 and the corpus README
  rows read "grounding contract".
- **Ruled: C.** It is the structure the metrics kit is built on
  (`.claude/plans/modules-design.md` §7.1).
