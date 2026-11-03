# 09 · Answer-agent served context — DROPPED BY DESIGN (the agent experiment)

Source: `dataraum-context/packages/cockpit/src/tools/query-context.ts` (+
`query.ts:813-848`). When the answer agent gets a question, the cockpit
assembles nine context blocks — schema (preferring enriched views),
dimensions, relationships, entities, drivers, grain, vocabulary, conventions,
business concepts — and serves them as one fixed text: the same bytes every
time within a session, so the model provider caches them as a prompt prefix.

Three properties of that serving are load-bearing today:

1. **~40% of the text is hand-written instruction**, not data — "ground every
   join on a pair listed here, otherwise abstain", "(additive) is a flow:
   SUM it".
2. **Curation is disclosed** — "showing 9 of 41 catalogued dimensions; the
   other 32 were never assessed, not rejected". Without the disclosure an
   agent reads absence as nonexistence and abstains for the wrong reason.
3. **Unconfirmed knowledge is gated** — an unconfirmed alias hierarchy is
   served with "do NOT merge these columns"; once a human confirms it, the
   instruction flips to "group by canonical only".

## Transcription

None — deliberately. The language has no serving construct: reading is
`GLOSSARY()` / `ATTEST()` plus plain SQL, and context assembly is an agent
skill, not grammar. The old track's `DECLARE SERVING` policy is gone with the
serving document.

```glossql
SELECT * FROM GLOSSARY(fin.orders);
SELECT * FROM GLOSSARY(fin.orders.amount, all => true);
SELECT subject, band FROM ATTEST(fin.trial_balance) WHERE band = 'red';
```

## Findings

- **DROPPED BY DESIGN — and it is the biggest bet in the language.** The
  served context is the running system's most-consumed artifact; the bet is
  that agents can compose their own context from the reads above, guided by
  skills, without a curated serving layer.
- The three properties are the benchmark the experiment must meet: context
  stable enough to cache, curation cuts made visible, confirmation state
  respected. Disclosure stopped being experimental 2026-08-04 ("serving
  wrong information is not an experiment" — project lead): the collapsed
  read carries `state` — `unassessed` (a witnessed aspect nobody spoke to
  is a visible row, the "9 of 41" cut), `contested` (value withheld, band
  says how badly), `current`, `stale` (served and marked — the snapshot
  moved or the column's type decision postdates the gloss). Confirmation
  state stays the band from `ATTEST()`. What remains for the experiment is
  only whether agents *use* the surface — sweep `state != 'current'` and
  close what they find.
- Nothing to fix in the grammar; everything to learn in the experiment. If it
  fails, the lesson returns as hardened skills or a read-side construct — the
  read surface itself does not change.
