# The question round

A question is derived from the record, never stored. There is no
question object, no queue relation, no answer ledger: the workspace
derives what is worth asking from what the slots already carry, asks
it where a human is, and the answer lands as an ordinary gloss. That a
gloss was logged on the human channel is the entire record of an
answer.

## One derivation, two renderings

A question is a disclosed grounding assumption held below full
confidence, carrying its key. The derivation is one read,
`open_questions`, and both surfaces serve exactly its rows,
least-confident first. Its gates: only grounding (QUERY) aspects;
only keyed assumptions — an unkeyed one cannot be closed, so it is
never asked; no dimension a measurement settles; no claim a standing
ruling already names. An unassessed witnessed claim is not a
question — it is the agent's measurement backlog, and the shipped
functions settle it.

At the MCP door the round rides the agent's own calls: each question
is one form with three stances — stands as stated, wrong (with a
correction), unclear: ask differently — plus, where the human already
ruled the same key under another aspect, that ruling offered back as
a fourth choice, so agreeing costs a click and differing stays open.
On the app door, the docket renders the same rows as cards, with the
ruling form — the one write the app door takes.

What waits on an act is a different read, `owed` — an approved recipe
change not yet re-landed, a formula answer newer than its recorded
materialization, a contested slot, a measurement gone stale or never
made, an unfolded ruling. Those are acts, not questions.

A question belongs to one dataset: a subject name is unique within a
dataset and not across a workspace, so the same aspect on a same-named
table in two datasets is two claims, ruled separately. `open_questions`
answers for the whole workspace and carries `dataset` on every row —
the reader says which dataset they mean, and `current_dataset` names
the bound one. `owed` narrows itself, because
what waits on an act waits on someone working in one dataset.

The rows are the whole of what is owed to a human:

```sql
SELECT o.aspect, o.dimension, o.key, o.assumption, o.conf
FROM open_questions o JOIN current_dataset d ON d.dataset = o.dataset
ORDER BY o.conf ASC;
```

## The answer is a gloss

A human answer lands as the judgment alone — confirmed, corrected, or
unclear, naming the claim — in the human's slot. `unclear` defers: it
is not a judgment, and it is never offered back as one. Human outranks agent at every
collapsed read, so the answer governs immediately, and the question
retires because the derivation no longer produces it:

```glossql
GLOSS definitions ON fin AS $${"definitions": {
  "revenue": {"meaning": "Product and Service revenue families only"},
  "gross_profit": {"meaning": "revenue minus COGS"}
}}$$;
```

A ruling then owes the agent an act — the fold-in: re-record the ruled
assumption under the same key at full confidence citing the ruling, or
re-ground per the correction. Until the fold-in, the ruling keeps the
question closed for both sides; `ruling_entries` shows each ruling
with its fold-in state. When the round asks about a key the human
already ruled under another aspect, the form names that sibling
ruling — two aspects may genuinely differ, and an agent that needs the
question settled again asks again.

## Keys, not prose

Every disclosed assumption carries a **key** — a short slug authored
at disclosure. The key is the claim's identity and the only thing the
record joins on; prose is what the human reads, never what the system
compares. No wording is matched against wording anywhere in the
system. The costs are explicit: an assumption without a key is
never asked; the same claim under two keys reads as two claims; a key
dropped from the body clears its debt.

## What is never asked

A question the shipped measurements can settle is the agent's work —
stock-or-flow, sign conventions, grain. The round never serves those
dimensions; it carries judgment only: definitions, conventions,
business meaning, choices between readings. And the round is one of
two registers — forms confirm existing claims against the record;
anything that decides what the work *is* happens in prose,
conversation first.
