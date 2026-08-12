# 2026-08-12 — How a brief reaches an agent: what the ecosystem offers

Research for the resolve-surface loop (feedback/flow-resolve-surface.md,
"the two briefs"), before deciding delivery together. The question:
how do MCP servers get "what changed" to an agent — at connect, and
on request for agents already connected.

## What the protocol offers (2026-07-28 revision)

1. **Live discovery.** The revision removed the initialize handshake;
   `server/discover` now reports capabilities, server info,
   **instructions, and cache metadata** per call — discovery is live,
   and dynamic instructions are spec-blessed with explicit cache
   control (the same caching family as the tools/list `ttlMs` we
   already inject). Delivery scope: whenever a client (re)discovers —
   in practice once per client session.
2. **`subscriptions/listen`.** The push channel that replaced the old
   GET notification stream: clients opt in per notification type.
   The right long-term shape for real-time state, but client support
   is young and unverified in the clients we face daily; rmcp-side
   support likewise unverified. Heavy for what the loop needs today.
3. **MRTR (SEP-2322)** — server-initiated asks exist only inside an
   in-flight call; not a brief channel.
4. **On request: a tool call.** The pattern the ecosystem actually
   ships today is instruction-led pull — static instructions that say
   "check X first", plus a status/context tool the agent calls. Tool
   descriptions are the agent's decision surface; a brief the agent
   can *ask for* is the only mechanism that reaches an
   already-connected agent in every client.

## What our own priors add

- MCP **resources were tried and reversed** here (2026-08-04) — the
  door tells, skills teach; resource-based delivery is out by ruling.
- The door has **one tool** by design, and live state already reads
  as plain tables through it. A brief that is a read needs no new
  door surface at all.

## The options, for deciding together

- **A (lean): the brief is a read through the one tool.** A `brief`
  relation the agent queries — at session open (skill discipline, one
  line in the static instructions: "start with the brief") and on
  request (the lead's case: a user changes something, tells the
  agent, the agent collects it — same read, any time). Zero new door
  behavior, zero client-support risk, and the app's front door
  renders the same derivation for the human.
- **B: live instructions at discover.** Spec-blessed now; a composed
  paragraph over the same reads, cache-controlled. Worth adding once
  A's content has proven itself — it is A's rendering moved earlier
  in the conversation, valuable for fresh sessions only.
- **C: `subscriptions/listen` push.** The eventual real-time channel;
  park until client support is real and the touchpoints phase
  (slack, email) makes push worth carrying.

Lean: **A now** (rides the resolve-surface build), B as follow-up
sugar over identical composition, C parked with the touchpoints.

Sources: the [2026-07-28 changelog](https://modelcontextprotocol.io/specification/2026-07-28/changelog),
the [MCP blog on the revision](https://blog.modelcontextprotocol.io/posts/2026-07-28/),
[stacktr.ee's breaking-changes guide](https://stacktr.ee/blog/mcp-2026-spec-changes),
[mcpservers.org's overview](https://blog.mcpservers.org/posts/mcp-spec-2026-07-28),
and [tool-design guidance](https://dev.to/aws-heroes/mcp-tool-design-why-your-ai-agent-is-failing-and-how-to-fix-it-40fc).
