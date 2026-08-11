# The monitoring-application evaluation — the operating loop on fault corpora

What is graded here is the assembled application, not the engines (the
engines were graded in `../tfmeval`, FINDINGS.md): an agent builds the
framework through the skills and the MCP door on the live server, the
walk runs through `metric_bands`/`band_breach`, the investigation
through `misfit.`, and everything grades against harness ground truth
— `ground_truth.yaml` at metric grain, the same-seed clean twin at row
grain. Three legs on seed 42, m12: the null (clean), `one_column_
outliers`, `fk-shuffled`. Corpora: `../tfmeval/output/corpora/`.

## Leg 1 — the null (clean corpus, 2026-08-11): PASSED at the judge

Setup, all through the door as dataset `clean_s42` in the shared
workspace: 9 tables landed by recipe (357,203 rows total, every count
exactly the source's, zero drops, casts clean); the add-source
vocabulary; 9 relationships declared after the judge pass over
`detect_relationships` (the detector's value-coincidence echoes —
invoice amounts against GL credits, payment dates against bank dates —
rejected, the true keys kept); four metrics grounded as QUERY glosses
with cited bases: `revenue` and `expenses` (GL, account-type scoped,
posted only), `billings` (operating chain), `net_cash_flow` (bank
feed). All scoped to fiscal 2025 — the corpus runs 14 months, the last
two being settlement runoff (temporal profile), disclosed as a scope
assumption.

**The framework is exact where truth exists**: monthly `revenue`
matches `ground_truth.yaml` to the cent on all 12 months.

**The system out-judged the agent once — recorded as a win.** The
`trial_balance` columns are named balances; the first grounding of
cash read them as cumulative levels (a stock). `behavior_evidence`
had already voted **flow** (13 voters, agreement 1.0, r_flow ≈ 1e-16
against GL line sums), and the truth comparison confirmed it: the
columns carry period turnover. The gloss was corrected, cash
re-grounded as `net_cash_flow` from the bank feed — exactly the trap
the metrics skill names ("a 'trial balance' column can carry period
turnover"), caught by the measurement the skill says to read first.

**The walk** (weights provisioned, DIGESTS pinned): 4 metrics × 6
walked months (2025-07..12), trained on the full year, corridors
fitted point-in-time.

- **Pager at the standing read: GREEN** (`band_breach` score 0.476 —
  the worst current-month displacement; December continues every
  metric's story). Zero standing false alarms on clean data.
- **Walked points: 7 of 24 outside the 0.98 corridor**, all on the
  high side, all in one story: the generator's Q4 ramp (revenue
  +38% in October over the January–September plateau) plus a mild
  August. On a single-year corpus the first Q4 ever seen *must*
  breach — the corridor's fit knew only the plateau. Every flag
  disposes under the judge's coherence read: `billings` (independent
  operating chain) shows the identical ramp month by month, so the GL
  is not fabricating; `expenses` co-moves; `net_cash_flow` turns
  positive one settlement lag later. Business pattern, not defect —
  zero defects claimed on clean data.
- This reproduces the eval's own E1.1 S1 finding ("the null is NOT
  quiet" naively) product-side, and shows the product's two answers:
  the judge tier disposes onset flags with named evidence, and the
  standing pager is quiet once the pattern is in the history. A
  multi-year corpus would put the seasonality inside the corridor
  (the eval's trend-null measurement).

**Product defects the leg surfaced, both fixed in-repo same day:**

1. **The stock marker could not land.** The metrics skill and both
   readers (`metric_bands`, `whatif`) honor `"behavior": "stock"` in
   the grounding body, but the standard grounding schema
   (`additionalProperties: false`) rejected it — the stock path was
   unreachable through the door on any real workspace (the scripts
   suite hands the function fabricated bodies, so store validation
   never ran there). Fixed: SPEC §5.2 and `schemas.rs` admit the
   optional `behavior` enum — the missing half of the ruled band-walk
   item; store test added. Rides the next server rebuild. Leg 1
   proceeded correctly without it: the affected extract serves one
   row per period, so window aggregation degenerates to the level
   (disclosed in its assumptions).
2. **The weights refusal named the wrong absence.** With safetensors
   present but the pinned `DIGESTS` missing, the loader's ENOENT read
   as if the directory were missing. Fixed: the runtime names the
   missing DIGESTS and that verification is mandatory; the metrics
   skill's provisioning note now lists all three files.

## Leg 2 — one_column_outliers (2026-08-11/12): PASSED on four lanes

The fault: 5% of `journal_lines.credit` scaled ×10 (some ×−10),
stationary — every month of history corrupted equally, all account
classes. Landed as dataset `outliers_s42`, same recipes and framework
as leg 1 (relationships redeclared from the leg-1 judgment — the
corpora differ only in the injected values). Truth: 7,892 injected
rows by clean-twin diff; equivalently, exactly the rows whose
`net_amount` contradicts `debit - credit` (the injector does not
maintain the row's own arithmetic — verified 1:1).

Four lanes, in the order the operator meets them:

1. **The pager fires: red 0.998** where leg 1's identical read was
   green 0.476. Honest mechanics: a stationary fault has no
   trajectory break, but the corrupted series is noisier and December
   drew outside its own corridor — partly the draw. The lane that
   cannot miss is:
2. **Cross-chain reconciliation**: GL revenue against billings (the
   untouched operating chain) swings **−24.3% to +8.2%** monthly
   where clean it held ~0.05% constant. The framework's redundancy —
   two independently grounded chains for the same business — catches
   a stationary corruption that trajectory monitoring structurally
   cannot.
3. **The deterministic identity**: rows where `net_amount ≠ debit −
   credit` = **7,892 — the injected set to the row, precision and
   recall 1.0**, zero model cost. The judge tier's first move.
4. **The misfit ranking** (door-side grade, the identity as the
   label; frame = December revenue credit lines sampled to the
   1024-row cap in the frame SQL): full surface **AUROC 0.9863**,
   all 44 sampled positives inside the top 100 of 1,024; with
   `net_amount` withheld — the pure distribution lane, as if the
   injector had kept the arithmetic — **AUROC 0.9452**, 45 of 53 in
   the top 100. Both above the eval's recorded class for this fault
   (row AUROC 0.85–0.87).

**The performance interlude the leg forced** (2026-08-12, ruled by
the project lead after the first misfit reads pegged the CPU for
minutes): the issue was the CPU. The kernel now runs on Metal by
default (CPU fallback, `GLOSSQL_DEVICE=cpu` to force), candle work
sits on a dedicated capped pool (`GLOSSQL_CANDLE_THREADS`, default 4)
so the server never takes the machine, the chain rule's feature
conditionals run in parallel (`score_log_mean` in the port, noise
pre-drawn — bit-identical to sequential, oracle tests green), and the
row cap moved to the measured bound: 256 → 1024 rows at 1.75s on
Metal (0.47s at 256). A frame past the cap is killed and rejected
with the sampling teaching in the message. Alongside: the sibling
folder renamed to `tabicl-candle`, and weights now ride the build —
`build.rs` stages safetensors + pinned DIGESTS beside the binaries;
no manual copying, no boot copy.

## Leg 3 — fk-shuffled (2026-08-12): localization proven, detection wiring owed

(Verdict corrected same day, on the project lead's question — the
first write-up said "passed as composed", which overstated: the
standing loop never rang on this fault. What follows keeps the
original findings and adds the correction.)

The fault: 1,492 of 14,928 `payments.invoice_id` (10%) traded among
payments owing the identical amount — every value legal, no orphans,
the amounts still matching the claimed invoice on 1,489 of 1,492.
Landed as `fkshuffled_s42`, same recipes and framework. Truth by
clean-twin diff; 629 of the moved rows (42.2% — the eval's number
exactly) are dated before the invoice they claim to settle.

The lanes, in order:

- **Every value lane is blind, as the eval predicted structurally.**
  The walk is byte-equivalent to clean (pager green at the identical
  0.476), GL-vs-billings reconciliation holds at 0.01–0.03%, the
  orphan join finds zero. Nothing value-shaped can see pairings.
- **The deterministic temporal check catches its share exactly**:
  629 payments dated before their claimed invoice — equal to the
  truth's traced subset, precision 1.0. The other 863 moved rows
  leave no single-row contradiction.
- **The joined misfit frame reaches where nothing else does** (frame:
  payments⋈invoices — amount, invoiced, day_delta, amount_gap,
  terms_days — hash-sampled deterministically to 891 rows, 90 moved):
  full **AUROC 0.841**; on the trace-free subset alone — the 58% no
  rule can reach — **AUROC 0.788** (50 hidden positives vs 801
  negatives). Top-50 precision 0.36 against a 10% base rate (3.6×
  lift). Recall-oriented by design; the judge adjudicates the queue.
- **Against the eval's 0.9338**: the gap has a named cause. The eval
  scored clean-twin (reference = the clean corpus's joined rows);
  the product self-fits, and this fault contaminates 10% of the
  context's joint distribution corpus-wide — there is no clean
  period in the corpus to tilt the frame toward. Self-fit robustness
  was measured at low contamination; at 10% relational contamination
  it costs measurable AUROC. The composition lesson (now one line in
  the metrics skill): the more known-good history a frame carries
  relative to suspects, the cleaner the context — when a clean
  stretch exists, put it in the frame.

Bookkeeping: the truth table (`eval_moved_pairs`) landed only after
every detection lane had run — grading machinery, not context.

**The correction (2026-08-12).** Three facts sharpen the verdict:

1. **Nothing paged on its own.** Pager green, reconciliation clean,
   orphans zero — the temporal check was run ad hoc, knowing where
   to look. For this fault class the signal must be a **declared
   validation** (§5 flow: expectation + check voice + witness +
   ATTEST); the leg proved the check's power, not its standing
   wiring. As run, leg 3 demonstrated localization, not detection.
2. **A second deterministic lane exists and was missed**: on clean
   rows every vendor maps to exactly one bank counterparty (via the
   declared payments↔bank_transactions edge), so a claimed invoice
   whose vendor disagrees with the bank counterparty is a wrong
   pairing — 290 more moved rows, zero false flags. The two rules
   together: **788 of 1,492 = 52.8% at precision 1.0**, before any
   model. Confirmed flags are also threads (the wronged invoice's
   true payment sits elsewhere among same-amount candidates) —
   unpulled here.
3. **The remaining 704 are only probabilistically visible** (the
   0.79 queue), and for genuinely evidence-free swaps that may be
   the ceiling — the eval's own "no single-row contradiction".

Owed to close the leg properly: the two checks declared as standing
validations in the workspace, so ATTEST goes red without anyone
knowing where to look.

## The verdict across three legs

The assembled application does what the evals said the engines could,
through the product doors, at product speeds: the null passes at the
judge with a green standing pager; a stationary value fault is caught
by the framework's own redundancy (two chains), the deterministic
identity, and a 0.99/0.95 misfit ranking; a pure pairing fault —
invisible to every value lane — is localized by the composed
relationship tier (two declared-relationship rules reach 52.8% at
precision 1.0, the joined density ranks the remainder at 0.79–0.84),
but the standing loop only rings for it once those rules are wired
as validations — the wiring leg 3 still owes. Each leg also improved the
product it was grading: the stock-marker schema gap, the DIGESTS
message, build-staged weights, and the Metal/parallel/capped kernel
came out of legs 1 and 2.
