# Actors and supersession

An **actor** is who speaks. Every connection carries one — an
`agent_id` or a `human_id` — and the engine stamps writer and actor
kind on every statement. There is no BY clause anywhere: authorship is
transport, not syntax, so it cannot be claimed. An answer the door
elicits from a human mid-call is server-witnessed and lands with human
standing — the same rule; the transport is just not always a
connection. Normative definition: [SPEC.md §1, §5.2](../../SPEC.md).

## Supersession is a read

The supersession key is **(subject, aspect, actor kind)**. A human
re-gloss supersedes the human's value; an agent's supersedes the
agent's. Nothing is updated in place — every write appends, and the
current value is a read: the latest row per key. History is always
underneath.

The slots stay separate. Per (subject, aspect) there is one slot per
speaker: the human's gloss, the agent's gloss, and each function
voice — a function whose `RETURNS` names the aspect, computed from
data at read time ([functions](functions.md)). Disagreement between
slots is not resolved by overwriting; a witness adjudicates across
them ([validation](validation.md)).

## Precedence

The collapsed `GLOSSARY()` read serves one value per (subject,
aspect): the precedence pick — **human over agent over function** —
withheld only when the witness's detector scores the disagreement
above its threshold (`contested`: value withheld, band and score say
how badly). A human ruling therefore outranks without deleting
anything: the agent's slot stands, superseded in precedence, visible
in the raw read.

```glossql
SELECT * FROM GLOSSARY(orders.amount, all => true);
```

The raw read returns every current slot side by side —
`(subject, aspect, kind, witness, actor, body, written_at)` —
precedence between them is the reader's business.

## What this buys

- **Corrections propagate without ceremony.** One human correction is
  one `GLOSS`; every read that composes the aspect serves the new
  value from then on.
- **The record is the write.** Who said what, as which kind, when —
  every row carries it. There is no separate audit surface because
  none is needed.
- **An agent can never speak for a human.** The kinds are distinct
  slots with distinct rank; a human slot is written only over a
  human-standing transport.
