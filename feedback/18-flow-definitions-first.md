# 18 · Flow: definitions-first onboarding — the glossary before the data

Source: the company-onboarding target below — a KPI-handbook excerpt of
the kind a medium-sized company brings to the door, constructed for
this fixture the way fixture 16's scorecard target was, awaiting its
test run. The question it puts to the language: every flow so far runs
data-first (land → measure → gloss → ground); a company arrives
definitions-first — the ontology exists in a handbook before any table
lands. Do the existing constructs carry the inverted order, and where
is the knowledge *central*?

The target, quoted:

> **KPI Handbook v3, §2 (excerpt).**
> *Net Revenue* — invoiced amounts less credit notes and rebates;
> recognized at invoice date. *Active Customer* — a customer with at
> least one paid invoice in the trailing 12 months. *Churn Rate* —
> active customers lost in period / active customers at period start.
> Definitions are owned by Finance; systems report against them, never
> the reverse.

## 1. The vocabulary declares before any source

Nothing in the grammar orders vocabulary after data. The concepts
declare as QUERY aspects on the dataset (fixture 16's shape) — thin:
schema, display label, tooling flag. The handbook content itself —
meaning, unit, owner, source — is a FACT gloss, because declarations
have no supersession and a handbook revision must land where actor,
timestamp, and contest live (ruled 2026-08-12; the glos run's stale
`x-unit` was the specimen — this section's first form put the
definition of record in the `WITH` blob, and the evaluation rejected
that home, see §4 below):

```glossql
USE co;

DECLARE ASPECT net_revenue WITH $${
  "title": "Net Revenue", "x-kind": "measure"
}$$ AS QUERY ON DATASET;

DECLARE ASPECT active_customers WITH $${
  "title": "Active Customers", "x-kind": "measure"
}$$ AS QUERY ON DATASET;

DECLARE ASPECT churn_rate WITH $${
  "title": "Churn Rate", "x-kind": "metric"
}$$ AS QUERY ON DATASET;

DECLARE ASPECT definitions WITH $${
  "type": "object", "properties": {"definitions": {"type": "object"}}
}$$ AS FACT ON DATASET;

GLOSS definitions ON co AS $${"definitions": {
  "net_revenue": {"meaning": "Invoiced amounts less credit notes and rebates; recognized at invoice date.",
                  "unit": "currency", "owner": "Finance", "source": "KPI Handbook v3 §2"},
  "active_customers": {"meaning": "Customers with at least one paid invoice in the trailing 12 months.",
                       "unit": "count", "owner": "Finance", "source": "KPI Handbook v3 §2"},
  "churn_rate": {"meaning": "Active customers lost in period / active customers at period start.",
                 "unit": "ratio", "owner": "Finance", "source": "KPI Handbook v3 §2"}
}}$$;
```

Handbook v4 is now a re-gloss — superseding, actor-stamped,
contestable — and no blob goes stale, because no field lives in two
places: the blob keeps what admission needs, the gloss keeps what
the company revises.

The formula DAG is handbook content too — a FACT gloss (fixture 16
§4), written before any table could evaluate it:

```glossql
DECLARE ASPECT formulas WITH $${
  "type": "object",
  "properties": {"formulas": {"type": "object"}}
}$$ AS FACT ON DATASET;

GLOSS formulas ON co AS $${
  "formulas": {
    "churn_rate": "lost_customers[w] / active_customers[start of w]"
  }
}$$;
```

Witnesses make the concepts *owed*, not merely named:

```glossql
DECLARE WITNESS net_revenue_w ON net_revenue BY (AGENT, HUMAN);
DECLARE WITNESS active_customers_w ON active_customers BY (AGENT, HUMAN);
DECLARE WITNESS churn_rate_w ON churn_rate BY (AGENT, HUMAN);
```

## 2. The onboarding backlog is a read

Before a single source lands, the glossary already answers "what does
this company mean and what remains ungrounded" — the `unassessed` grid
(§5.3's disclosure, doing new work):

```glossql
SELECT subject, aspect FROM GLOSSARY(co) WHERE state = 'unassessed';
SELECT count(*) FROM GLOSSARY(co) WHERE state = 'unassessed';
```

Three rows: the handbook's concepts, declared, witnessed, spoken to by
nobody. This is the fixture's central observation — **the onboarding
backlog needs no construct; it is the visible absence the collapse
already serves.** As sources land and groundings close, the count
walks to zero; a stakeholder's "how far is onboarding" is this query.

## 3. Data arrives; grounding closes concepts against the handbook

The add-source flow (fixture 11) runs unchanged — probe, recipe, land,
measure, entity verdicts, relationships. What changes is the grounding
act: the extract is judged against the *handbook's* definition, and
the assumptions cite it as a basis alongside the measured floor:

```glossql
GLOSS net_revenue ON co AS $${
  "sql": "SELECT i.invoice_date AS date, i.amount - coalesce(c.credit_amount, 0) AS value, i.customer_id FROM invoices i LEFT JOIN credit_notes c ON c.invoice_id = i.invoice_id",
  "assumptions": [
    {"dimension": "definition", "assumption": "credit notes netted at invoice grain; rebates not present in this source",
     "basis": "KPI Handbook v3 §2 (Net Revenue)", "confidence": 0.9},
    {"dimension": "grain", "assumption": "the credit-note join is grain-preserving",
     "basis": "relationship gloss (grain-check counts)", "confidence": 1.0},
    {"dimension": "behavior", "assumption": "a flow: sums valid over any partition",
     "basis": "behavior_evidence on invoices.amount", "confidence": 0.95}
  ]
}$$;
```

The handbook coverage gap surfaces exactly where fixture 16's
world-coverage wishes live: rebates are in the definition and not in
the source — a named ask (the rebate subledger), carried as a
disclosed assumption until the world arrives. The pinning agenda runs
in reverse here: where fixture 16's agent *made* definitional choices
and owed questions, this flow *receives* definitions and owes
deviations — every grounding that narrows or widens the handbook says
so in an assumption with the handbook as basis.

## 4. Where is the vocabulary central? — FORK, held open

One workspace carries one dataset (binding in the app). A company's
ERP, CRM and HR land as separate workspaces, and §1's declarations
must reach all of them. Three candidate homes:

- **Replay a folder** (exists today — fixture 01's vertical binding,
  fixture 11's "framing the vertical"): the company glossary is a
  versioned file of declarations replayed into each workspace at
  onboarding. No construct, no grammar growth. Identity across
  workspaces is by name only — nothing asserts that `co_erp`'s
  `net_revenue` and `co_crm`'s are the same concept, and a handbook
  revision is a manual re-replay everywhere. INFORMATION LOST:
  concept identity, vocabulary version.
- **Bootstrap-shipped** (the measurement library's mechanism): the
  operator ships the company vocabulary the way the reference
  functions ship — every fresh workspace receives it at boot. Central
  by construction; same identity loss as the replay, plus the
  vocabulary becomes an operator artifact outside the door.
- **A construct** — the spelling below is invented and must not
  parse; it exists to name the shape a ruling would have to give,
  not to propose it:

```glossql-gap
DECLARE VOCABULARY company_glossary FROM 'vocab/company.glossql' VERSION '2026-08';
```

Cross-workspace portability is on the held-open list and pack
envelopes were dropped by design (2026-08-03) — this fork closes only
by the project lead, against a real multi-source run. Until then the
replay is the honest answer, and its losses are named above.

## Findings

- **Definitions-first TRANSCRIBES as declaration order** — no new
  construct. Aspects, formula glosses and witnesses carry a handbook
  before any table exists; the grammar never ordered vocabulary after
  data, the flows just always ran that way.
- **The onboarding backlog is the `unassessed` read** (§2): declared +
  witnessed + unspoken = a visible row. Progress is a count walking to
  zero, served by the collapse that already exists.
- **Definitions live in glosses, not blobs** (superseding this
  fixture's first form; rejected by the glos evaluation, ruled
  2026-08-12): the definition of record is the `definitions` FACT
  gloss (§1), where supersession, actor rank, and contest apply; the
  handbook citation in a grounding stays an assumption `basis` — for
  readers, not machinery.
- **The deviation duty inverts the pinning agenda** (§3): data-first
  flows end by asking the user to pin choices; definitions-first flows
  end by disclosing every deviation from the received definition, with
  the handbook as the basis deviated from. Same mechanism (assumption
  with named basis, human re-gloss outranks), opposite direction.
- **The central-vocabulary FORK is the fixture's open half** (§4):
  replay exists and loses identity and versioning; bootstrap
  centralizes and loses the same; a construct is held-open territory.
  SEMANTICS UNDEFINED: concept identity across workspaces.
- Term-to-data lookup needs no construct at one-workspace scope:
  `GLOSSARY(co)` filtered on aspect and value is the reverse index.
  Across workspaces it inherits the §4 fork.
