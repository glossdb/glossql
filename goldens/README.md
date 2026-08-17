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

## Determinism, and what it cost to get

The first capture was not reproducible, which stage 1 found by running
its own gate: two runs of unchanged code differed in the last digits of
`entropy`, `r_flow` and every summed `actual`. Sums over partitions do
not associate, so DataFusion may add the same values in a different
order run to run. Two normalisations settle it, both in `scrub`:

- every float rounds to **eight significant digits**. Twelve was not
  enough: rounding only settles the noise if the quantum sits well above
  it, and booksql's larger sums carry a relative noise near 3e-12, so
  values landing on a boundary flipped either way (`31996.2576049` /
  `31996.257605`, found 2026-08-17 by the first automated diff). Eight
  leaves four orders of margin, and a real change moves far more than
  1 part in 1e8;
- anything under **1e-12 absolute snaps to zero**. A residual of 2e-17
  means the two series matched exactly; its significant digits *are* the
  noise, so rounding alone cannot settle it.

A body rides as a JSON string, so the normalisation parses into it too —
otherwise it never reaches the numbers that actually move.

With both in place, two runs of the same code are byte-identical, and a
diff means behaviour changed.

## Diffing, and regenerating

`./goldens/diff.sh` re-captures into a temp dir and diffs against what is
committed. Any output means behaviour moved.

```
./goldens/diff.sh                 # all four (~10 min; rel-event is 8 of it)
./goldens/diff.sh fin rel-f1      # the fast ones
UPDATE=1 ./goldens/diff.sh        # accept the new capture as the baseline
```

It wraps one command per corpus:

```
cargo run --release -p glossql-serverd --example capture_goldens -- \
  <tables-dir> <dataset> <out-dir> [--setup=<file>] [--existing]
```

- `--setup=` runs glossql after landing, for a corpus that ships no FK
  truth of its own (`booksql/setup.glossql`).
- `--existing` captures a workspace that already carries glosses instead
  of landing one, which is how `fin` is taken. `--setup=` applies here
  too: `fin`'s FK truth and its authored vocabulary (the three checks,
  their aspects and witnesses) are re-declared on every capture, because
  the declaration relations crossed to the lake on 2026-08-17 and that
  workspace is a fixture nobody wants to re-land. Declaring is
  idempotent, so it is statements in the file rather than a one-off to
  remember.

The fin baseline was re-captured at that crossing: the migration's
re-declares sweep a function's cached rows once (by design — a
re-declared function is a different function), so verdicts the fixture
had accumulated from its own interactive reads left `_values.json`, and
one fresh recompute flipped 15 metric-cube cells on the eighth-digit
rounding boundary. Argued, not discovered: two consecutive captures on
the new stack are byte-identical.

RelBench corpora carry `schema.json` beside `tables/`; the FK truth in it
is declared automatically, and that is what turns the borrowed-axis and
coherence paths on.

## What the capture cost, 2026-08-17

Baseline for stage 7, and a fair statement of what the current stack
does: `fin` 412 calls in 10 s · `rel-f1` 361 in 9 s · `booksql` 387 in
30 s · `rel-event` 673 in **8 minutes**, almost all of it
`behavior_evidence` over 131 columns at roughly a second each.
