# Flow: source conventions — the deposit the next dataset reads

Draft fixture, unnumbered — it takes its corpus number when a ruling
and a run close it. Source: the glos onboarding run
(`2026-08-11-onboarding-run-glos.md` §2 F5, §3), whose deposits are
the real artifact. Scope narrowed by the project lead 2026-08-12:
company-wide vocabulary stays postponed; this fixture is only the
per-source half — conventions of a *source system*, deposited so
that the next dataset from the same system reads before probing.

## 1. The artifact — what one run deposited, and where it is trapped

The glos run learned, by measurement and cure, facts about the
exporting system that are true of every export it will ever produce:

- mixed German/English month abbreviations; timestamp format
  `%b %e %Y %I:%M%p`
- `1900-01-01` as the placeholder date (85% of `wunschtermin`)
- an operator-note token leaking into seven numeric columns
  (`NULLIF` before cast is the cure)
- key spelling: fact tables carry a `Z` prefix the master data omits
  (`Z120 = Z110_120`)
- a `Zeit` column on the 1899 epoch (cured by subtracting the
  column's own midnight)

Where they landed: FACT glosses and recipes in dataset `glos`. The
next export from the same ERP starts blind — the onboarding cost
curve stays flat per source (the run's §3).

Two facts about today's machinery bear directly:

- `sources` is a workspace-grain relation — `sources (name,
  settings)`, no dataset column (`store.rs:190`). A source's identity
  is already central to the workspace; only its *knowledge* is not.
- Conventions are revisable company knowledge — a wrong placeholder
  guess gets corrected, a format extends — so whatever home they get
  must be the gloss plane (supersession, actor rank, contest), never
  a declaration blob. This is the definitions ruling (2026-08-12)
  applied at source grain.

## 2. What must hold

1. Keyed by source system, not by dataset — the `Z`-prefix fact is
   about the ERP, not about `glos`.
2. Readable from any dataset's flow, before the first probe.
3. Revisable with provenance — supersession, actor, contest.
4. A promotion act: a dataset-local finding becomes a source
   convention by an explicit, witnessed act — never by harvest.
5. Dataset-local evidence stays local (the run's §3): orphan
   populations and grain verdicts would rot if centralized; the
   `basis` field is the seam — local verdicts citing the source
   convention they lean on.

## 3. Fork A — the replay (exists today, the honest interim)

A `source_conventions` FACT gloss per dataset, body keyed by source
system; a versioned file of these statements replayed into each new
dataset at onboarding:

```glossql
USE glos;

DECLARE ASPECT source_conventions WITH $${
  "type": "object", "properties": {"systems": {"type": "object"}}
}$$ AS FACT ON DATASET;

GLOSS source_conventions ON glos AS $${"systems": {
  "glos_erp": {
    "timestamp_format": "%b %e %Y %I:%M%p, month names mixed German/English",
    "placeholder_date": "1900-01-01",
    "note_leakage": "operator note token in numeric columns; NULLIF before cast",
    "key_spelling": "fact tables prefix cell keys with Z; master data omits it",
    "epoch": "Zeit rides the 1899 epoch; subtract the column's own midnight"
  }
}}$$;
```

TRANSCRIBES — no construct, no storage change. INFORMATION LOST:
identity (nothing asserts two datasets' `glos_erp` are the same
system — by-name only), version (re-replay everywhere by hand), and
trust history (actor and time reset at each replay) — the same three
losses the run charged against replay for company vocabulary.
Promotion is copy-paste into the file.

## 4. Fork B — the source as a subject (grain growth)

The source already has a central name; let the glossary speak to it.
The aspect grain vocabulary grows one word — this must not parse
today (`grammar.ebnf:106` closes the grain list at RELATIONSHIP):

```glossql-gap
DECLARE ASPECT conventions WITH $${
  "type": "object"
}$$ AS FACT ON SOURCE;
```

The gloss itself would then spell as any gloss does, the source name
in subject position (`GLOSS conventions ON glos_erp AS $$…$$`) —
that string parses under today's grammar as a path subject and fails
only at resolution, so the growth is one grain keyword plus subject
resolution reaching the `sources` relation, not a new statement
head. What it buys over Fork A, mechanically: the subject is the
workspace-grain source row, so every dataset reads the same slots —
identity by construction, supersession and contest intact, no
replay. Promotion is an ordinary act: the actor who judged the
finding re-speaks it at source grain; the local gloss cites it as
`basis` from then on.

The add-source flow's opening changes by one read:

```glossql
SELECT value FROM GLOSSARY(glos_erp) WHERE aspect = 'conventions';
```

— served before the first probe, in whichever dataset the new export
lands. (This read parses today; it resolves only under the fork.)

## 5. Persistence — where the slots live (rides the storage ruling)

Under 2026-08-11 storage integration, each dataset pairs with a
`<dataset>_meta` namespace. Source-grain slots belong to none of
them. The coherent home is one workspace-grain sibling —
`sources_meta` beside the dataset pairs — holding the source-subject
glossary rows; every dataset's flow reads it, the server is its sole
writer, snapshot expiry keeps its supersession history like any
other. No new mechanism: the same namespace convention, one more
namespace. (Interim, while the store is SQLite: the store is already
workspace-grain, so Fork B needs no storage work at all today — the
persistence question only arrives with the Iceberg move, and the
lead has asked to keep the interim straightforward.)

## 6. Findings

- **Per-source deposits TRANSCRIBE today only as replay** (Fork A),
  which loses identity, version, and trust history — the honest
  interim, not the answer.
- **The source is already a workspace-grain name** — Fork B's grain
  keyword is the smallest growth that makes the deposit central by
  construction: one word in the grain list, subject resolution to
  the `sources` relation, zero new statement heads.
- **Promotion needs no machinery under Fork B** — it is a re-speak
  at source grain by the actor who judged it, witnessed like any
  gloss; the `basis` seam keeps local evidence local.
- **FORK, open with the lead**: A (replay, no growth) vs B (SOURCE
  grain). B's cost is a grammar-adjacent change and belongs to the
  corpus-first process; this fixture is its proposal. Cross-workspace
  identity stays out — postponed with the company grain.
