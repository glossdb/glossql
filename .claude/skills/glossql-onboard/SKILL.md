---
name: glossql-onboard
description: The onboarding arc for a glossql workspace — add-source → relationships → dimensions → metrics, and the points where the agent stops for the human. Use when onboarding a company's data end to end, or when unsure which flow comes next in a fresh workspace.
---

# Onboarding a workspace

Onboarding is the path from a company's exports and definitions to a
working operating model. It is four deliverables, each with its own
skill, in this order — each stage stands on the previous one's
glosses:

1. **glossql-add-source** — probe the source, author the typing
   recipe, land the table, run the measurement plane, gloss every
   column.
2. **glossql-relationships** — detect, judge, and declare the join
   structure.
3. **glossql-dimensions** — score the slice axes, judge hierarchies,
   record grain-checked judged joins.
4. **glossql-metrics** — ground concepts, record formulas, stand up
   validations, close with the question round.

Load the stage skill when you enter the stage. This skill only fixes
the order and names where you stop for the human — the mechanics live
in the stage skills, and there is no extra protocol beyond them.

## The loop every stage shares

- **Open with the brief** (the core glossql skill teaches it). The
  door's connect-time brief counts human writings, approvals waiting
  on your act, and questions standing open for the human — while
  that last count is above zero, sweep the round or relay in chat.
  Read the human slots back before new work —
  an answer from last session governs everything you do now, and
  acting on your own superseded slot instead is the one unforgivable
  onboarding error.
- **Gloss honestly, with confidence meant.** The assumptions array
  and sober confidence are what make questions derivable. An
  assumption you leave out is a question nobody is ever asked; a
  confidence you inflate empties the human's queue falsely.
- **Let the door ask.** While loose assumptions or owed claims
  stand, the door serves one question form per tool call to clients
  that render them (verified in Claude Code; others as they support
  question forms). The answer lands
  server-side as the human's own slot — it never travels through
  your mouth, and you never write it for them.
- **Prose is the fallback.** No question surface: ask in chat,
  multiple choice with your grounds and confidence, then run the
  statement the answer names — the write still travels through a
  session, and your report says which definitions stand on your
  judgment alone until a human slot exists.

## Where each stage stops for the human

- **add-source**: source conventions before typing (fiscal calendar,
  sign conventions, what an export's nulls mean), and the closing
  read-back of table meaning. For a witnessed enum fact you cannot
  ground (a column's behavior or unit the data does not show): leave
  the slot unwritten — an unwritten witnessed claim derives as the
  door's choice question, while a guessed value silences it.
- **relationships**: the judge pattern is yours — the measurement
  optimizes recall, you remove the false positives against the data.
  The human stop is semantics the data cannot show: two join paths
  that both hold, a relationship whose business meaning you are
  inferring from column names.
- **dimensions**: relevance and hierarchy shape are measured and
  judged; the human stop is which axes the business actually reports
  by, when the measurement leaves a tie or the ruled-out list feels
  wrong.
- **metrics**: the question round (the metrics skill's closing
  section) — every definitional choice named against its alternative,
  after walking the ladder: measure what data can decide, witness
  what must keep holding, ask only what survives.

The arc is iterative, not one sitting. Answers land between your
sessions; the next brief carries them back. When a stage's questions
are open, say so and stop — don't hold the session hostage to a form.
