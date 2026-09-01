---
name: glossql-prose
description: How to write prose in this repo — README, docs/, skills/. Hemingway rules from the project lead — short sentences, active verbs, simple words, no drama. Use before writing or editing any page a human or agent reads.
---

# Prose

The product is the story, not the language describing it. Write
technical docs like Ernest Hemingway: short sentences, simple words,
direct facts.

## The rules

- Cut the fat. Delete every word that does not do work.
- Use active verbs. The server processes the request. Not: the
  request is processed by the server.
- Keep sentences short. Say one thing. Then say the next thing.
- Use simple words. Say "send", not "utilize transmission protocols".
- Show the code. Let the code speak. Do not talk about the code too
  much.
- No drama. The answer is stored — it does not "join the record". A
  fixture was checked — it did not "survive". State the fact.

## What stays

- Domain terms are vocabulary, not drama. Keep them: land, gloss,
  slot, band, witness, aspect, docket, recipe, collapse,
  supersession, grain, fold-in, ruling, contested, owed.
- Precision beats brevity. The docs pages are served to agents as
  context (`doc://docs/…`). A rewrite must not drop a constraint, a
  condition, or a definition. When unsure, keep the sentence and
  simplify only its grammar.
- When editing near old dramatic prose, flatten it. Do not match it.

## Hard limits

- Do not edit fenced code blocks or tables in a prose pass. The test
  suite parses and plans every ```glossql and ```sql block under
  `docs/`, `skills/`, and SPEC.md.
- Keep every H1. The MCP door serves each docs page as a resource
  titled by its H1.
- `docs/reference/reads.md` keeps its path and the literal
  `metric_axes()` — a door test asserts both.
- Workspace `cargo test` green after any docs change.

## Example

Bad: "In order to successfully effectuate the initialization of the
application, it is required that the user input their credentials
into the designated configuration interface."

Good: "Start the app. Enter your username and password. The app sends
the data to the server. You are in."
