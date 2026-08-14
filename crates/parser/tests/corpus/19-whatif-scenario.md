# 19 · What-if: the declared scenario — TRANSCRIBES (scenario = FACT aspect; the `whatif.` door)

Source: our own evaluation runs — the counterfactual walk in
`../dataraum-eval/tfm/PHASE2_FINDINGS.md` (Leg B: the read across
lever-varied support worlds, effect recovery 1.011 in support) and its
independent reproduction in `../tfmeval` (E4; E4b/E4c measured the
no-support regime and closed it — bare history cannot carry the read).
The product shape is the proven pipeline applied to real rows: the
scenario is declared, the server **replays the recipes** with the
override applied at a small grid of strengths, and the conditional
read runs across the replayed worlds with the factor as a feature.
Presented and ruled 2026-08-11: each scenario is its own FACT aspect
(Fork A); the shared-aspect form died on the supersession key, the
arguments-at-the-door form on the bare-call rule. Evidence and the
corrected verdict: `reports/2026-08-11-tabicl-integration.md`.

## 1. The scenario declares like a metric — one aspect per scenario

The same placement metrics have (fixture 16 §1: one QUERY aspect per
concept, served by `read.<name>()`): one FACT aspect per scenario,
served by `whatif.<name>()`. The flavor rides `x-kind`, never the
syntax (the `read.` ruling, fixture 16).

```glossql
USE fin;

DECLARE ASPECT price_hike WITH $${
  "title": "Price +15% from Jan 2027",
  "description": "List prices raised 15% across the board",
  "x-kind": "scenario",
  "type": "object",
  "required": ["overrides"],
  "properties": {"overrides": {"type": "array", "items": {
    "type": "object",
    "required": ["column", "factor", "from", "basis"],
    "properties": {
      "column": {"type": "string"},
      "factor": {"type": "number"},
      "from":   {"type": "string"},
      "basis":  {"type": "string"}
    }
  }}}
}$$ AS FACT ON DATASET;
```

## 2. The scenario body — overrides with their basis

Each override names a real column, a factor, a start, and its basis —
the assumptions discipline the grounding glosses already carry
(fixture 16 §2). A behavioral response the history never saw (demand
elasticity) is not guessed: it is either declared as a further
override, or absent — and then the read names it as an assumption.

```glossql
GLOSS price_hike ON fin AS $${
  "overrides": [
    {"column": "sales_order_lines.unit_price", "factor": 1.15, "from": "2027-01",
     "basis": "the declared lever"},
    {"column": "sales_order_lines.units", "factor": 0.95, "from": "2027-01",
     "basis": "assumed demand response, hand-declared; not in any history"}
  ]
}$$;
```

## 3. The read — one relation, narrowed by WHERE

`whatif.<scenario>()` serves one relation over every concept the
replay reaches: the time axis, the band quantiles, and a `basis`
column carrying the judgment — in-support, wide-with-reason, or the
refusal row (a concept no formula path connects to the overridden
columns gets `NULL` bands and the reason, never a silent guess).
Sweeps are `WHERE` clauses over this relation, never a special form
(the `ATTEST` rule).

```glossql
SELECT * FROM whatif.price_hike();
SELECT month, p50, basis FROM whatif.price_hike() WHERE concept = 'revenue';
```

## 4. Versioning and governance fall out of the store

Supersession key (subject, aspect, actor kind): re-glossing updates
the scenario, a human's numbers retire an agent's. A witness makes
scenario authorship a policy, with no new construct:

```glossql
DECLARE WITNESS scenario_gate ON price_hike BY (HUMAN);

GLOSS price_hike ON fin AS $${
  "overrides": [
    {"column": "sales_order_lines.unit_price", "factor": 1.10, "from": "2027-01",
     "basis": "the declared lever, revised down after review"}
  ]
}$$;
```

Removal is ordinary glossary SQL, as everywhere.

## 5. The forks that died (ruled 2026-08-11)

- **One shared `scenario` aspect, each scenario one gloss** — dies on
  the store's own key: (subject, aspect, actor kind) keeps one
  current body, so a second scenario silently retires the first; and
  the scenario's name lives inside the body, invisible to the door.
- **Overrides as arguments at the door** — dies on the bare-call rule
  (settings are context, never call arguments, ruled 2026-08-04) and
  leaves no record: not superseded, not attested, not reproducible.

## 6. Machinery, not language

None of the following appears in any statement; it is the server's
job, recorded here so the fixture stays honest about what the door
does:

- **The grid is implied by the declared factor** — bracketed, never
  extrapolated: the baseline (×1.0, the real books) plus ~5 strengths
  placed on both sides of the declared value, so the scenario's own
  read is always interpolation (the evals' shape: 1.15 held out
  *inside* the grid). Multi-override replays singles per lever plus
  the declared joint (the proven two-lever shape).
- **Reach is declared knowledge**: an override moves the metrics the
  formulas and relationships connect to the overridden columns;
  observed behavior in the replayed rows (per-invoice payment delays)
  rides along; where no path exists the read serves the refusal row
  with the reason.
- **The guard is the model's own band width** at the support
  boundary, measured trustworthy for this model specifically
  (17–24× widening out of support).
