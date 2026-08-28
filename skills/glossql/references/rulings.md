# Rulings and what they owe you — read when the brief counts a ruling, a question, or a contest

An answer lands as a **ruling**: the judgment
alone — confirmed or corrected, naming the claim by its `key` — in
the human's `ruling` slot on the subject, never a copy of your body.
A ruling holds its question closed and the round moves on; your
grounding stays yours. Questions derive from *your current body*, so
raising a confidence with a measurement basis closes its question on
its own. A human's decline rests only while the workspace holds
still — your next write re-opens it for the next review. A client
without question forms gets nothing — relay the open questions in
chat yourself, multiple choice with your grounds, and run the
statement the answer names.

What stands open is a read, and you can see it yourself:

```sql
SELECT o.aspect, o.dimension, o.key, o.assumption, o.conf
FROM open_questions o JOIN current_dataset d ON d.dataset = o.dataset
ORDER BY o.conf ASC;
```

`open_questions` is the derivation itself, not a summary of it — the
same rows the forms serve and the app's docket renders. Filter it like
any table (`WHERE o.aspect = 'cycle_time'`); order it where you read
it, since a read carries no ordering of its own. The join is what
scopes it to the dataset you bound — drop it and you are reading every
dataset in the workspace. `ruling_entries` is what
the human has ruled; both build on it.

**One key ruled two ways is yours to reconcile.** The round names the
sibling ruling on the form when it asks about a key the human already
ruled under another aspect; `ruling_entries` shows both afterwards.
Nothing pairs or resolves them: decide whether the aspects genuinely
differ, and record the reconciliation in the groundings themselves.
Folding both in literally is how a ruled component ends up
contradicting the metric that composes it — and if you need the
question settled again, ask again.

**Every disclosed assumption carries a `key`** — a short slug you
write at disclosure (`business-days-only`, `completed-only`). The key
is the claim's identity and the only thing the record joins on:
rulings, question closure, and the fold-in debt all match
`(aspect, key)`. Assumption prose is what the human reads, never what
the system compares — no wording is ever matched against wording
anywhere in this system, and none ever will be. What that costs you,
stated plainly:

- **An assumption without a key is never asked.** It cannot be held
  closed, so the round would re-ask it forever; it is skipped
  instead. Your record shows it, no human is ever served it.
- **The same claim under two different keys reads as two claims.**
  Nothing detects it. If you disclose one decision on two aspects,
  use one key for it — or better, declare the shared concept as its
  own metric and compose both from it, so the decision lives in one
  place.
- **Dropping a key from your body clears its debt.** The claim is no
  longer disclosed, so nothing stands below full confidence. Drop a
  key only when you truly no longer rest on the claim.

Then close what owes an act, in the same session:

- **A ruling awaiting its fold-in** (the brief counts these):
  re-record the ruled grounding — the ruled assumption **under the
  same key**, at confidence 1.0 with `basis: "human-ruled"`, or
  re-grounded per the correction note in the ruling. The debt clears
  the moment your current body carries that key at full confidence;
  until then the ruling keeps the question closed for you both. Keep
  the key and rewrite the prose as freely as the correction requires
  — the join is on the key alone. **Fold in every standing ruling,
  then re-measure, then the walk** — each write moves the pin; the
  cube rebuilds at its next read on the newest verdicts and marks
  them (`metric_axes().judged_current`) until the profilers run again,
  and the walk lands at the pin it runs at — so one batch of
  fold-ins, the profilers once, the walk once. Read the ruling notes as
  you fold: a note naming a sibling aspect ("differs from … by
  design", or a slip re-ruled) is the human's cross-aspect judgment —
  carry it into the grounding's assumption text. A ruling whose stance
  is **`unclear` is a refusal of the question, not of the claim**: the
  human could not tell what you were asking. Its fold-in is a
  reformulation — rewrite the assumption plainly (its note says what
  confused them) **under a new key** and re-record; the clearer wording
  derives its own question, and dropping the old key clears the debt.
- **A human formula answer newer than the metric's recorded gloss**:
  the two are one definition in two forms — re-record the
  materialization to match (or carry the difference as a disclosed
  assumption). Until you do, `read.<metric>()` serves the old SQL and
  the app shows the answer as waiting on you.
- **An approved `recipe_change`** (a human gloss carrying
  `{table, sql, reason}`): run the `DECLARE RECIPE` it approves — the
  approval is data, the act is yours.
- **A contested slot**: re-ground and re-judge as the contest section
  below says.
- **Human slots over your own**: read each back and re-compose what
  you still hold on top of the human's ruling — their word governs.

## Confidence means the number

Wherever a writing carries `confidence` (grounding assumptions are
the main carrier), one scale governs, anchored to the evidence
behind the number:

- **1.0** — ruled by a human, or verified by a named measurement or
  check. Nothing else.
- **~0.9** — independent evidence converges: a measurement plus a
  conventions gloss plus the data's own shape. A well-argued
  convention choice tops out here.
- **~0.7** — one source: a single measurement, or your reading of
  names and values.
- **0.5 and below** — a default you adopted to proceed. Exactly what
  the question round exists to surface.

Confidence is evidence, never a gate: nothing routes on it
mechanically — the round orders by it (lowest first) and every
assumption below 1.0 stays askable. An inflated number empties the
human's queue falsely; a deflated one wastes their attention.

State ambiguity plainly. The reader is a capable engineer: when a
verdict is ambiguous, name the readings you saw, which you took, and
why — in the report's front matter, not softened or buried. An
honest "two readings survive, I took A because B breaks the grain
check" is worth more than fluent certainty.

## When a slot contests

`state = 'contested'` means voices differ on one slot and the
detector's score crossed the witness threshold — the value is withheld,
never adjudicated for you. Read the slots
(`GLOSSARY(subject, all => true)`), re-ground the question in the data,
and re-gloss only if the evidence moved you: your new gloss supersedes
your old one, and converged voices turn the band green. If the evidence
still says you were right, leave the slot contested — a human closes
it by conceding in their own slot. (Closure by striking a slot —
`DELETE FROM glossary WHERE …` — is parked until the substrate can
remove rows, iceberg-rust 0.11; the statement refuses and names this.)
Never change a gloss just to end a contest.
