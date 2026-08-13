# The elicitation spike — mechanics proven, the live client can't answer yet

2026-08-13. The onboarding-UX direction (discussion with the project lead,
same day): MCP elicitation is the interaction primitive — the server asks
the human mid-tool-call, the answer arrives server-side, witnessed, and
lands as a HUMAN gloss. Provenance as a transport fact, replacing the
pin surface. This spike built the mechanics and measured the one real
client against them.

## Built

- The `elicitation` rmcp feature (workspace `Cargo.toml`), a
  `--elicit-probe` boot flag, and a probe in the MCP door: one form
  question per tool call (subject, aspect, JSON body, a land-it/skip-it
  enum). An accepted dictation lands through
  `GlossqlMcp::land_human_answer` — the human's own plane channel
  (`--human`), the pin door's guards, the store's admission unchanged.
  The door composes the statement and the actor and decides nothing.
- The actor-id fix, found on the way: the sessionless transport
  synthesizes `peer_info()` (`client_info.name == "rmcp"`), so every
  2026-07-28 tool call stamped actor `rmcp` and all agents collapsed
  into one plane channel. The door now reads the request's own
  `_meta` clientInfo (`context.client_info()`); `doors.rs` asserts the
  stored actor id. The monitor line in the live run shows it working:
  `glossql <- claude-code: …`.
- A wire monitor in the door's middleware: every request prints
  `mcp <- <method> @<protocolVersion> <session|sessionless>`.

## Proven at 2025-11-25

`an_elicited_answer_lands_with_human_standing` (doors.rs): a client
initializing at protocol `2025-11-25` — the newest session-carrying
revision — receives the question on the tool call's own SSE stream,
posts the answer through the session, the handler unblocks, and the
dictated gloss lands with `actor_kind='human'`. The full loop the
design needs, green in-process.

## Measured live: Claude Code cannot complete the loop

Claude Code (2026-08-13, streamable HTTP) against the probe door:

```
mcp    <- initialize @2026-07-28 sessionless
mcp    <- tools/call @2026-07-28 sessionless
glossql <- claude-code: DECLARE DATASET fin …
glossql ?? claude-code: elicit-probe: no round-trip: request timeout after PT120S
```

Three findings, each independently blocking:

1. **Claude Code negotiates `2026-07-28`, sessionless, on every
   request.** Per SEP-2567 that revision has no transport session, and
   rmcp 3.1.2 (the newest release, checked 2026-08-13) serves it
   through a oneshot transport that discards any JSON-RPC response a
   client posts back (`tower.rs:2034`) — a posted answer has no route
   to the waiting handler.
2. **Claude Code advertises the elicitation capability but rendered no
   dialog** on this lifecycle. The capability check passed (the probe
   asked); the question went out on the POST's SSE stream; nothing
   appeared client-side. Claude Code's documentation says elicitation
   is supported (form + URL modes) — evidently not for a streamable
   HTTP server on the sessionless revision, today.
3. **The spec has the stateless answer; neither end implements it.**
   The 2026-07-28 revision carries elicitation statelessly via MRTR
   (SEP-2322); rmcp models the MRTR types but does not route the
   responses; Claude Code did not render the request.

The failure mode is graceful by design: the probe times out (120s),
the note lands in the tool result, the statements execute regardless.
A separately documented Claude Code issue (its client not echoing
`Mcp-Session-Id`) additionally clouds any forced-downgrade path.

## Options for the ruling (project lead; not decided here)

- **(a) Downgrade middleware** — rewrite a `2026-07-28` initialize to
  `2025-11-25` before rmcp classifies it, forcing the session branch.
  A protocol lie, and two unknowns: whether Claude Code accepts the
  downgrade, and whether it echoes the session id (the known issue
  says it may not).
- **(b) Upstream** — stateless MRTR response routing in rmcp, and
  elicitation rendering on this lifecycle in Claude Code. The right
  ending; not on our clock.
- **(c) Prose relay now** — the fallback that already exists: the
  agent asks in chat (its own question surface), relays, supersedes
  its own slot. Works today on every client; the statement is
  agent-actored — provenance by convention, not transport.
- **(d) A stdio side-door** — serve the same handler over rmcp's stdio
  transport for the local onboarding session. stdio is one duplex
  connection: no session routing to break, and the lifecycle problem
  does not arise. Hypothesis to test: Claude Code renders elicitation
  for stdio servers. Fits the fresh-user story (claude starts next to
  the data; the door boots locally); the HTTP door stays for `/app`,
  `/query`, and shared serving. One spike to confirm rendering.

The provenance architecture is not in question — it is proven at the
store and at the session-carrying protocol. The gap is transport
adoption, and it is measurable per client at connect time (the wire
monitor): when a client arrives on a lifecycle that can answer, the
door can elicit; until then, the relay convention stands.

## Resolution (same day): MRTR — the finding above was incomplete

The project lead pointed at the rust-sdk README's elicitation section;
options (c) and (d) were ruled out and (a) ruled in, then superseded
by what the source check found: **rmcp 3.1.2 does implement stateless
elicitation**, as MRTR (SEP-2322) — not a server→client request but an
`input_required` *result*. The handler returns
`InputRequiredResult { inputRequests, requestState }` instead of the
tool result; the client renders the embedded elicitation, then retries
the same call with `inputResponses` and the echoed state. The SDK
gates this to peers ≥ `2026-07-28` — the exact opposite gate of
`create_elicitation`, and the exact lifecycle Claude Code speaks. The
finding "neither end implements it" was wrong on both ends: the spike
had tested the old mechanism against the new lifecycle.

The probe now picks the mechanism by the negotiated version:
`2026-07-28+` gets the MRTR round; session lifecycles (≤ `2025-11-25`)
get `create_elicitation` on the call's own stream. Both are green
in-process (`an_mrtr_retry_lands_with_human_standing`,
`an_elicited_answer_lands_with_human_standing`).

Measured live (Claude Code, streamable HTTP, same day): **the loop
closes.** The form rendered, the client fulfilled and retried
unprompted, the state echoed, and the door's guards adjudicated the
answers (a free-text subject refused by the path gate; a skip
digested as such). No stall, no session, no downgrade, no protocol
lie. Ruling 1 dissolves: no option was needed — the protocol already
carried the answer, one revision ahead of where the spike first
looked.

One spike artifact to correct in the real protocol: the probe digests
the answer before the same call's statements execute, so a dictation
racing its own `USE` finds no dataset. Real questions will come from
the brief's agenda between calls, not ahead of the call that carries
their context.

Ruled on the spike's evidence (project lead, same day): **unsigned
human standing is accepted for the PoC** — human > agent > function
holds; how to identify *which* human is found out later, not faked by
a flag. The `--human` boot flag is removed; every door's human writes
land as the one anonymous `human` actor (`serverd::HUMAN`). The
question-shape design: the fixed set fixes renderable structures
(single choice, dictation, confirm), never question content — the
agent composes content as data, the door serves it through a
structure, the answer lands with human standing, and the agenda
derivation retires it. How an agent declares a question (aspect
convention vs statement) goes to the corpus, not decided here.
