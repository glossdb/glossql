# The basis vocabulary — competing forms (F4: the definition-dependency read)

Status: **Fork A ruled in structurally (2026-08-12), the kind
vocabulary deliberately unruled.** The lead's constraint: "pin" and
"judgment" as named kinds import the pin-vs-judgement split the
resolve surface exists to remove — the enum lands only after Leg D
streamlines the verbs, and this fixture's kind names are
placeholders, not vocabulary. Sequenced C → D → B (2026-08-12): the
v0.3 temporal investigation first, the resolve surface second, this
schema change last, on the streamlined words. No SPEC or schema edit
until then.

The original direction (same day): free-text basis strings are
unreliable keys — "not if we know them, eg. an ASPECT defines basis
strings. We can seed a workspace with ASPECTS, we already do that
for functions. Needs some modelling." This is that modelling.

## The problem, from today's own workspace

The lead pinned the revenue definition through the app. Which
knowledge depends on it? Two kinds of dependency exist and only one
is traceable:

- **Composition is structural**: the dso grounding reads
  `FROM read.revenue()` — a changed revenue grounding propagates into
  dso with no further act, and a text search finds the dependency.
- **Assumption bases are prose**: the same dso gloss carries
  `"basis": "behavior_evidence on journal_lines.net_amount, r_flow ~
  1e-16, 28/28 accounts"` and `"basis": "agent judgment: DSO speaks
  to trade collection"`. No read can ask *"which groundings cite the
  definitions gloss?"* over that — the blast radius of a definition
  change is traced by hand, which is F4.

What a basis actually was, in every assumption this workspace wrote
today (the empirical inventory, nothing invented):

1. another gloss — "conventions gloss on erp_export"
2. a measurement — "behavior_evidence on journal_lines.net_amount …"
3. relationship glosses (the grain-check verdicts)
4. an ad-hoc measured reconciliation (SQL run in-session)
5. the agent's own judgment
6. an engineer pin — "engineer-pinned via app"
7. a document nobody has (the world-coverage wish)

Seven shapes, five of which name something the store already holds.

## Fork A — the basis is a reference, validated by the grounding contract

No new aspect, no new statement, no new relation. The assumptions
array — already the one contract the world-model surface reads —
gains a structured `basis`: a kind from a closed set, plus the
reference the kind needs. The seeded artifact is the **grounding
schema itself** (engine-built, ships the way function declarations
ship), extended to validate the object:

```glossql
GLOSS dso ON fin AS $${
  "sql": "SELECT ... FROM balance_sheet b JOIN (SELECT ... FROM read.revenue() ...) r ON ...",
  "assumptions": [
    {"dimension": "behavior",
     "assumption": "a ratio, final at month grain",
     "basis": {"kind": "gloss", "subject": "fin", "aspect": "formulas"},
     "confidence": 1.0},
    {"dimension": "definition",
     "assumption": "numerator is Trade Receivables (1210) only",
     "basis": {"kind": "pin", "subject": "fin", "aspect": "definitions",
               "note": "engineer-pinned via app 2026-08-12"},
     "confidence": 1.0},
    {"dimension": "grain",
     "assumption": "the revenue join is month-to-month, 1:1",
     "basis": {"kind": "measurement", "function": "behavior_evidence",
               "subject": "journal_lines.net_amount"},
     "confidence": 1.0},
    {"dimension": "scope",
     "assumption": "the GL routes ~50-55% of invoiced sales through AR",
     "basis": {"kind": "document", "note": "the posting policy; no landed table carries it"},
     "confidence": 0.9}
  ]
}$$;
```

`kind: "judgment"` carries no reference — the pin marker stays the
basis, never the number. `kind: "document"` doubles as the
world-coverage wish, which today has no home at all.

The dependency read is plain SQL over the glossary, the house unnest,
no new machinery — *"what cites the definitions gloss?"*:

```glossql
SELECT g.subject, g.aspect
FROM GLOSSARY(all => true) g
CROSS JOIN generate_series(0, 19) AS i(i)
WHERE g.kind = 'query'
  AND i.i < json_length(g.body, 'assumptions')
  AND json_get_str(json_get(json_get(json_get(g.body, 'assumptions'), i.i), 'basis'), 'aspect') = 'definitions';
```

A superseded definitions slot plus this read is the re-judgement
agenda — exactly what the resolve surface (Leg D) wants to serve as
one-gesture pins.

Cost: assumption bodies grow; the free-prose evidence detail moves
into `note`. Migration is soft — a string basis stays legal as
`{"kind": "judgment"}`-equivalent prose until re-spoken (the schema
admits both during the transition, or the transition is simply the
next supersession).

## Fork B — bases stay strings; a seeded vocabulary makes them known

The lead's phrasing taken literally: an aspect defines the basis
strings. A seeded `basis_vocabulary` FACT gloss (shipped at boot,
extendable by re-speak) enumerates admissible *templates*:

```glossql
DECLARE ASPECT basis_vocabulary WITH $${"type": "object"}$$ AS FACT ON DATASET;

GLOSS basis_vocabulary ON fin AS $${
  "templates": {
    "gloss":       "gloss:<subject>:<aspect>",
    "measurement": "measurement:<function>:<subject>",
    "pin":         "pin:<subject>:<aspect>",
    "judgment":    "judgment",
    "document":    "document:<name>"
  }
}$$;

GLOSS dso ON fin AS $${
  "sql": "...",
  "assumptions": [
    {"dimension": "definition",
     "assumption": "numerator is Trade Receivables (1210) only",
     "basis": "pin:fin:definitions",
     "confidence": 1.0}
  ]
}$$;
```

Discipline is a detector, not admission: a `basis_coherence`
measurement flags bases that match no template or resolve to no
slot (recall; the agent judges). The dependency read is string
matching — `basis LIKE '%:definitions'` — workable, but the colon
convention is an unruled mini-grammar living inside strings, which
is how the "generated strings are unreliable" problem started.

## Fork C — dependency rows written at gloss time (named to reject)

A `bases` store relation, one row per citation, written when the
gloss lands. Rejected in the writing: it duplicates what the body
already carries, adds a second write surface the delete cascade must
also sweep, and the store gains machinery for what Fork A answers
with one schema field and one read.

## The lean

Fork A. The assumptions array is already the validated contract and
already the surface humans judge; making `basis` a reference makes
the same surface dependency-traceable with zero new vocabulary. The
"seeded like functions" requirement is met where it is cheapest — the
grounding schema is the seed, versioned with the binary. Fork B keeps
prose ergonomics but rebuilds references inside strings; its colon
templates are Fork A's object flattened, minus the validation.
