# 22 · The question loop — RULED: no question object; GLOSS + actor kind is the whole record

Source: our own runs. The agenda half is a pin-queue onboarding run;
the answer half is the elicitation spike (Claude Code rendered the
door's form via MRTR and the accepted answer
landed as a HUMAN gloss). The fork this fixture carried (A agenda
gloss · B per-subject gloss · C `ASK` statement · D alternatives in
bodies) closed by the project lead's verdict: **all four
were ledgers.** The ruled model:

- `GLOSS` is the only write.
- Every slot logs its speaker — `actor_kind`, `actor_id` — and human
  outranks agent at every collapsed read. Already true.
- **No additional ledger for questions or answers.** A question is
  ephemeral: it rides the interaction transport (the door's
  elicitation form, or the agent's own chat surface) and vanishes.
  That a `GLOSS` was logged as `human` is the entire record of an
  answer.

## The answer — the one thing the language records

```glossql
GLOSS definitions ON fin AS $${"definitions": {
  "revenue": {"meaning": "Product and Service revenue families only"},
  "gross_profit": {"meaning": "revenue minus COGS"}
}}$$;
```

TRANSCRIBES — spoken on the human channel, this slot outranks the
agent's at every read, and any surface that wants "what is still
open" derives it: agent slots the human has not spoken over, using
only what slots already carry (`confidence`, `grounds`, as taught).
Nothing is declared to make that read possible.

## What the fork record established, kept as evidence

- **Per-subject question glosses collapse under the supersession key**
  — two questions about one subject are two glosses on one
  (subject, aspect, actor kind); the second erases the first. Any
  question-as-slot design refutes itself here.

- **A first-class ASK is sugar at best** (must fail to parse):

```glossql-gap
ASK definitions ON fin WITH $${"question": "revenue scope: which accounts?"}$$;
```

- **Agenda artifacts are pin-shaped.** The dataset-grain
  `pin_questions` array existed so a queue frame had one slot to
  render; the body-per-option existed so a pin button had a prepared
  statement to post. Both retired with the pin surface.

DROPPED BY DESIGN: `pin_questions`, alternatives-in-body, `ASK` — the
question never enters the store in any form.
