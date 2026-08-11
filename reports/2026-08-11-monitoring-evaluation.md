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

Leg 3 (fk-shuffled) follows in this file.
