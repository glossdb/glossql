# 2026-08-15 — Working-capital run on the clean export, and the four defects it found

A full onboarding run driven through the MCP door against the finance
generator's clean export (`../dataraum-testdata/output/clean`, 17 CSVs,
26 MB), on the live workspace and the shipped skills, with the project
lead answering as the human. Blind in the usual sense: the truth files
(`ground_truth.yaml`, `metadata_truth.yaml`, `entropy_map.yaml`,
`manifest.yaml`) were never opened and no CSV was read except through
the door.

The run was not blind in one respect worth stating plainly. It was
driven from inside the repo by an agent whose context already held the
codebase, so it measures whether the *system* works end to end, not
whether the skills teach a cold agent. A teaching test still wants a
scratch folder outside the repo.

## The run

Topic agreed in prose before anything landed: working capital — how much
cash sits in the operating cycle and how fast it moves. The cohort was
proposed as five base concepts and six derived, deliberately including
the cash conversion cycle, which is a metric over three metrics over
five extracts and therefore the one that exercises composition rather
than summation.

13 of 17 tables landed, 0 rows dropped, casts clean throughout. Four
files were left out and the reasons recorded: `trial_balance` (period
movement the general ledger already carries), `fx_rates` (single
currency), `bank_transactions` (liquidity, a different topic),
`stock_movements` (the level is what DIO needs). `ap_invoices` landed
16,817 rows and reported 0 dropped — the same table that produced 16,817
phantom drops before the accounting fix earlier in the day.

14 relationships declared from 34 candidates. The 20 rejected are the
textbook shapes: measure-to-measure matches (`inventory_positions.
unit_cost -> products.standard_cost`, a valuation derivation rather than
a reference), amount-to-amount and date-to-date composites, and the
reverse directions of true edges. Each declared edge was anti-joined
both ways, and three orphan populations resolved exactly: AR without a
receipt = open + overdue (3,410), AP without a payment = cancelled +
open + overdue (1,889), AP without a ledger entry = the 352 cancelled
ones. Zero residual in all three, which is what confirms an edge rather
than merely permitting it.

11 metrics grounded. All five base concepts reconcile to the ledger
exactly: revenue to accounts 4110+4120+4210, COGS to 5100, receivables
to 1210+1220, payables to 2110+2120, inventory to 1400. Three
validations stand green, two of which check the *groundings* against the
ledger rather than re-deriving them, so a drift in either half is
caught. 47 witness verdicts, one red: the bands corridor, correctly.

## Four defects, all of one kind

Every defect this run found is the same shape — a rule that was ruled
once, implemented in one place, and never carried to the others.

**1. A sliced ratio was reported as the sum of its members.**
`metric_cube` knew two behaviors: `is_stock`, and everything else is a
flow and gets summed. A ratio is neither. DSO grounded across segment
and region reported **928.3 days for a month whose true DSO was 75.6** —
twelve member ratios added together, and every member value inflated by
the other axis's multiplicity. The blast radius was the whole ratio
surface, since `metric_series()` feeds the docket's charts and
`metric_bands` walks the cube's totals.

Ruled (option 2 of three): a ratio declares itself by serving `num` and
`den` beside `value`, and the cube totals any grain as
`sum(num)/sum(den)` — exact for the headline and every member, using
only summation. Option 3, evaluating the `formulas` gloss directly, was
named and deferred: it needs the formula evaluator that F4 records as a
known gap. Option 2 is its division node, not a detour.

**2 and 3. The `whatif` door carried both defects the cube had shed.**
Testing scenarios found the stock verb keeping ONE arbitrary row per
month (`row_number() = 1`): a receivables grounding that emits a row per
open invoice replayed as **4,325 against a true 42,203,204**, and
inventory as 12,274 against 12,337,501. The cube's own comment records
shedding that exact defect on 2026-08-14. And a ratio had no verb at
all, so DSO replayed at **957 days against a true 76**.

The door is Rust and the cube is rhai, which is the whole explanation:
neither correction crossed the language boundary. Ten scenario tests
passed throughout, because every fixture has one row per month — the
shape where both wrong verbs give the right answer.

**4. A skill taught rhai that does not compile.** The functions skill's
worked example carried a multi-line `db.query("…")`, which rhai rejects:
SQL over several lines needs backticks. The standing invariant could not
see it, because a function's body is opaque text to the glossql parser —
the statement parses perfectly whatever the script says.

Each fix landed with a regression test in the shape that produced it:
four cells a month across two axes for the cube, two rows a month for
whatif, and the invariant now compiles every `DECLARE FUNCTION` body in
the skills. Each was checked to fail against the old code.

Two smaller ones, same character: the shipped vega-lite spec pinned
`$schema` to v5 against a vendored v6.4.3, warning on every page load;
and app pages carried no `Cache-Control`, so after a ruling the browser
served the pre-ruling copy under heuristic freshness — the redirect was
never wrong, the caching was.

## What the data said

The judgment findings, kept apart from the defects.

**Two scope questions closed by measurement rather than sent to the
human.** "Other Receivables" (1220) holds trade receivables: the AR
subledger reconciles to 1210+1220 exactly every month and to 1210 alone
in no month, so a trade-only DSO would have been half the truth. The
payables side mirrors it at 2110+2120, cancelled excluded.

**One measurement overruled with cited evidence.**
`behavior_evidence` returned `flow` for `inventory_positions.value`,
having aligned inventory against *sales* movement. Summed per period it
equals the ledger's inventory account exactly on all 12 months; read as
a flow the cumulative reaches 149.1M for a business carrying 12.4M. The
gloss records the disproof, not the verdict alone.

**Region and payment terms are perfectly confounded** — one term per
region, 100 customers each. Any DSO-by-region reading is arithmetically
DSO-by-payment-terms, so the cash app carries segment and says why
region is absent.

**`trial_balance` carries period movement and `balance_sheet` carries
the levels**, both verified against cumulative ledger movement. The
names are reversed, and `invoices.csv` is the payables side while
`ar_invoices.csv` is receivables. All four recorded as source
conventions, which serve every future export from this system.

**Structural limits recorded rather than worked around.** DPO carries no
axis: the payables split (goods/expense) has no counterpart in COGS, so
a per-category DPO would divide one population's payables by another's
cost. CCC carries none, its three legs being cut by different things.
And a scenario's `from` anchor needs a time column on the overridden
table, so revenue at order-line grain cannot be levered — the line
carries no date, the order does. That is the grain that buys revenue its
`product_group` axis costing it scenario reach.

**World-coverage wishes.** No opening balance (the ledger starts at zero
in January 2025, so Q1 ratios are ramp artifacts); no vendor dimension,
which is exactly the cut the payables finding wants; and 2026 is
settlement runoff, which charts as a collapse if nobody says so.

## The business reading

The cash conversion cycle improved from 85 days in September to 60 in
December, and every day of it came from paying suppliers later: DPO went
16.5 to 47.7 while DSO went 63.4 to 75.6 the wrong way. Payables rose
5.3M to 18.5M over the quarter. The bands walk flags it independently —
AP and DPO both at PIT 0.999 for three consecutive months, the one red
verdict in the workspace. Whether that is policy or a payment run that
stopped is not answerable from the ledger, and the read-back said so.

## The human loop

Six judgment questions were disclosed, all definitional or conventional;
none was a statistic. The lead ruled four in the docket mid-run and two
more after the read-back, all confirmed. Each fold-in re-recorded the
grounding citing its ruling and raised confidence to 1.0 with the rival
left disclosed and charted, and the brief's owed-count tracked down to
zero. The elicitation round fired on record reads as designed and timed
out twice with nobody at the keyboard, which is what prompted raising
the wait from 25s to a tunable 120s: the cost of absence is already
bounded by the rest-on-decline rule, so the wait only had to cover a
present person thinking.

## On method

The judge pattern earned its keep twice — once removing a measurement's
false positive (inventory as a flow), once removing 20 relationship
candidates. Both times the measurement was right to propose and wrong to
be believed.

The defects share a lesson the corpus already states and the code did
not follow: a verb ruled for one reader belongs to every reader of the
same shape. Three of the four were invisible to a green test suite
because every fixture used the degenerate shape — one row per month,
one metric per period — where the wrong rule and the right rule agree.
A fixture that cannot distinguish them is not a test of them.
