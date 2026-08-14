---
name: glossql-onboard
description: The onboarding arc for a glossql workspace — agree the topic and the KPI cohort in chat, then add-source → relationships → dimensions → metrics, and the points where the agent stops for the human. Use when onboarding a company's data end to end, or when unsure which flow comes next in a fresh workspace.
---

# Onboarding a workspace

Onboarding is the path from a company's exports and definitions to a
working operating model. It opens with a conversation and then runs
four deliverables, each with its own skill, in this order — each
stage stands on the previous one's glosses:

0. **The topic and the cohort** — agreed in chat, before anything
   lands (below).
1. **glossql-add-source** — probe the source, author the typing
   recipe, land the tables the topic needs, run the measurement
   plane, gloss every landed column.
2. **glossql-relationships** — detect, judge, and declare the join
   structure.
3. **glossql-dimensions** — score the slice axes, judge hierarchies,
   record grain-checked judged joins.
4. **glossql-metrics** — ground the agreed cohort, record formulas,
   stand up validations, close with the question round.

Load the stage skill when you enter the stage. This skill fixes the
order and names where you stop for the human — the mechanics live in
the stage skills.

## Two registers — prose shapes the work, forms rule the record

Every human interaction in the arc is one of two kinds, and mixing
them up wastes the human either way:

- **Conversation** (the topic, the cohort, anything that shapes what
  the work *is*): stop and talk. Present the facts you have, propose
  in prose, let the human answer in prose, interpret it — the usual
  chat fair. No forms, no schemas, no machinery. There is nothing to
  confirm yet, so confirmation surfaces don't fit.
- **Rulings** (a standing assumption confirmed or corrected): the
  question round. Derived from data, served one form at a time on
  calls that read the record — never mid-landing — the answer landing
  server-side with human standing. These work because confirming a
  stated judgment with the facts on the table is easy — the human
  still decides, without authoring from nothing.

## Stage 0 — the topic, then the cohort

**Ask what this is about before anything lands.** A dataset has a
topic — working capital, sales performance, cost control — and the
topic is what makes every later choice decidable: which tables to
land, which KPIs to propose, which questions matter. Propose a topic
as prose from what you can see (the files, the user's words), let
the user shape it, then declare it:

```glossql
DECLARE DATASET fin SET (purpose: 'working capital — where cash sits and how fast it moves');
```

**Then propose the KPI cohort** — the metrics the topic implies,
including the heavy ones (a cash conversion cycle, real margins),
not just what looks easy to compute. The user prunes and extends in
prose. The agreed cohort is the contract the metrics stage grounds
against, and this conversation is where scope questions surface
while they are still cheap — "DSO over which receivables?" costs one
sentence here and a wrong dashboard later.

Aim high deliberately: **a cohort KPI the data cannot ground is a
finding, not a failure.** Name what is missing and which tables
would close it — surfacing that gap is the product working.

## Import what the topic needs — this is not ETL

Probe and recipe are already the filter. A dataset is a curated
working set for its topic, never a mirror of the export: land the
tables the cohort needs, take only the columns the recipe's SELECT
list earns, filter wide tables in the recipe's WHERE. Tables and
recipes added later are cheap — a `DECLARE RECIPE` away — so leaving
something out costs one later statement, while landing everything
costs attention on every flow that follows: more columns to gloss,
more owed slots, more noise between you and the questions that
matter. The first live run measured this: the deep scope questions
drowned in a 109-column long tail, and the wrong numbers all traced
to assumptions nobody surfaced.

## The loop every stage shares

- **Open with the brief** (the core glossql skill teaches it). The
  door's connect-time brief counts human writings, approvals waiting
  on your act, and judgment questions standing open for the human —
  while that last count is above zero, sweep the round or relay in
  chat. Read the human slots back before new work —
  an answer from last session governs everything you do now, and
  acting on your own superseded slot instead is the one unforgivable
  onboarding error.
- **Measure before anything is askable.** A statistic is never a
  human question: behavior, unit, join structure, slicing axes are
  the shipped functions' work — the core skill's function map names
  what settles what. What the human rules is judgment: definitions,
  conventions, choices between readings.
- **Gloss honestly, with confidence meant.** The assumptions array
  and sober confidence are what make questions derivable. An
  assumption you leave out is a question nobody is ever asked — the
  first validated run paid 2× on DSO for an undisclosed scope — and
  a confidence you inflate empties the human's queue falsely.
- **Let the door ask.** While judgment questions stand (assumptions
  below full confidence), the door serves one question form per
  record-reading call — a call that reads the glossary and writes
  nothing — to clients that render them (verified in Claude Code;
  others as they support question forms). Your landings and judging
  queries run uninterrupted; the stage read-back is where the forms
  engage. The answer lands server-side as the human's own slot — it
  never travels through your mouth, and you never write it for them.
- **Prose is the fallback.** No question surface: ask in chat,
  multiple choice with your grounds and confidence, then run the
  statement the answer names — the write still travels through a
  session, and your report says which definitions stand on your
  judgment alone until a human slot exists.

## Where each stage stops for the human

- **stage 0**: the whole stage is a stop — topic and cohort are the
  human's to shape, in conversation.
- **add-source**: source conventions before typing (fiscal calendar,
  sign conventions, what an export's nulls mean), and the closing
  read-back of table meaning. A column's behavior or unit is never a
  human stop — `behavior_evidence` and the profiles settle those
  after the relationships stage; leave the slot unwritten until they
  run, and relay only what the measurement abstains on, with its
  reason, as a judgment question.
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
  what must keep holding, ask only what survives. And the closing
  read-back names every cohort KPI that did not ground, with what
  would close it.

The arc is iterative, not one sitting. Answers land between your
sessions; the next brief carries them back. When a stage's questions
are open, say so and stop — don't hold the session hostage to a form.
