# Goldens — the regression baseline for the port

What every shipped function answers today, on four workspaces chosen for
what each one breaks that the others cannot
(`reports/2026-08-17-the-foundation.md` §7). The port of stages 2–5 is
measured against these: a function that stops abstaining, starts
abstaining, or moves a number has changed behaviour, and that has to be
argued rather than discovered later.

| corpus | source | covers |
|---|---|---|
| `fin` | the finance generator, as `~/glossql-ws` | ground truth, real glosses and groundings, three workspace-authored checks |
| `rel-f1` | `dataraum-eval/corpora/relbench/rel-f1` | declared-FK truth; three tables with no time column — the borrowed-axis path; two carrying three FKs each — shared-parent alignment |
| `rel-event` | `dataraum-eval/corpora/relbench/rel-event` | keyless junction tables; a column named `Unnamed: 0` |
| `booksql` | `testdata/booksql/Tables` | composite endpoints, and `business Id` against `Business Id` across every join |

Two files per corpus shape:

- `<function>.json` — the outcome of `SELECT <fn>() FROM <subject>` for
  every subject of that function's grain, **errors included**. A refusal
  is behaviour; an abstention is behaviour.
- `_values.json` — every computed value, keyed `<subject>::<function>`.
  Extraction serves a summary where a body carries one, so this is where
  the whole value lives.

Timestamps and snapshot ids are replaced with `<volatile>`; everything
else is compared verbatim.

## Regenerating

```
cargo run --release -p glossql-serverd --example capture_goldens -- \
  <tables-dir> <dataset> <out-dir> [--setup=<file>] [--existing]
```

- `--setup=` runs glossql after landing, for a corpus that ships no FK
  truth of its own (`booksql/setup.glossql`).
- `--existing` captures a workspace that already carries glosses instead
  of landing one, which is how `fin` is taken.

RelBench corpora carry `schema.json` beside `tables/`; the FK truth in it
is declared automatically, and that is what turns the borrowed-axis and
coherence paths on.

## What the capture cost, 2026-08-17

Baseline for stage 7, and a fair statement of what the current stack
does: `fin` 412 calls in 10 s · `rel-f1` 361 in 9 s · `booksql` 387 in
30 s · `rel-event` 673 in **8 minutes**, almost all of it
`behavior_evidence` over 131 columns at roughly a second each.
