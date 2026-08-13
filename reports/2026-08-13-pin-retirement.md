# The pin retirement — provenance is transport; the app informs

2026-08-13, ruled by the project lead on the elicitation spike's
evidence (`2026-08-13-elicitation-spike.md`): pins were app-side
duplication of standing the glossary already has. The model is
user > agent > function, carried by `actor_kind` on every slot and
the collapsed read's ranking — nothing more. What must exist:
`GLOSS`; the speaker logged; **no additional ledger for questions or
answers**. That a `GLOSS` was logged as `human` is the entire record
of an answer; the question itself is ephemeral (the door's
elicitation form, or prose in the agent's chat) and never enters the
store. Corpus fixture 22 records the verdict and the fork it closed.

## Removed

- **The write surface**: `crates/apps/src/pin.rs` (the one POST),
  `crates/apps/src/auth.rs` (HS256 sign-in — with unsigned human
  standing ruled, nothing left to sign), their routes, the door
  secret, the `who` template context, and the deps only auth used
  (`base64`, `getrandom`, `hmac`, `sha2`).
- **The pin UI**: sign-in header and modal, the question-card form
  block, their CSS, and vendored Alpine (present for exactly that
  transition; nothing else used it).
- **The conventions**: `pin_questions` retires (its only consumers
  were the pin buttons); the metrics skill's §9 now teaches the
  question round — ask through the client's question surface, the
  answer lands as the human gloss. `recipe_change` survives: it is
  the human-approval channel, data not machinery, writable from any
  session.
- **Frames**: `pins.sql` deleted; `queue.sql` and `census.sql` lose
  their agenda CTEs (the covered-assumption suppression retires with
  the agenda — the queue is simpler and more complete); `brief.sql`
  and the tiles say "human writings", not pins.

## Kept — everything derived

Queue, census, brief, dossiers, contest-as-statement: all read-only
over `GLOSSARY()`/`glossary`/`aspects`/`witnesses`/`cache`, all
derivations. The store never had pin structures — a pin was always a
human gloss — so nothing in `crates/glossary` changes beyond a
comment. The waiting-on-agent derivation (approvals unexecuted,
human formula answers newer than their materialization, contested
slots) survives unchanged and now feeds the elicitation agenda.

## The write path now

One per standing: agents write over their MCP channel; humans write
through the door's elicitation loop (the server witnesses the answer
and lands it as the anonymous `human` actor — unsigned by ruling,
identification a later question) or through any session speaking as
human (`/query`). The app carries no write route at all.
