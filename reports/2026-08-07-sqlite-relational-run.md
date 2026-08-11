# The SQLite relational run — what broke

Date: 2026-08-07. Run 8: a fresh dataset `fin2` over the booksql
SQLite dump (`~/glossql-data/finance_2/accounting.sqlite`, 7 tables,
1,012,948 rows, 810,059 of them the ledger), driven through the four
skill flows end to end — add-source, relationships, dimensions,
metrics. **First run over a relational source**; the ADBC executor
landed 2026-08-06 and had not carried a full flow before.

The flows completed: 7 tables landed, 74 `meaning` glosses with none
unassessed, 7 entity verdicts, 62 columns roled, 28 dimension
verdicts, 5 relationships declared, 4 concepts grounded with no
collisions, 4 derived metrics evaluated, 3 validations passing, 80
attestations green. This record is only about what did not work.

## 1. Dates cannot land as a date type from a relational source

**The blocking finding.** Every one of the five date columns landed
`Utf8`.

The mechanism is three rulings meeting, each defensible alone:

- recipe SQL for a relational source runs **at the source**, in the
  source's dialect (`crates/import/src/adbc.rs`);
- SQLite has no date type — `CAST(x AS DATE)` takes NUMERIC affinity
  and would turn `'2010-12-27'` into `2010`, and `date()` returns
  text;
- the import lands the source's Arrow schema unconverted —
  `normalize::compat` folds only what Iceberg v2 rejects
  (`crates/import/src/lib.rs:140-154`, `normalize.rs:19-31`).

So there is no surface anywhere in the language to author a temporal
type for a source whose dialect has none. `try_to_date` exists only on
the file path (`casts::register_try_functions`, reachable at
`lib.rs:159`).

Measured consequences, both silent:

- `temporal()` abstained on all four date columns
  (`ledger.transaction_date`, `created_date`, `due_date`,
  `employees.hire_date`). Gate: `temporal.rhai:24`.
- `behavior_evidence()` abstained dataset-wide: *"no period axis: no
  date column on ledger or one declared hop away"*. Every `behavior`
  gloss in this workspace therefore rests on structure measured by
  hand (credit = quantity × rate on 487,119 lines; credits = debits
  within all 169,525 documents; `open_balance` strictly below the
  document total in all 169,525) rather than on the measurement built
  for the job.

One typing gap disabled two measurement planes. Note that
`CAST(transaction_date AS DATE)` works fine in DataFusion at read
time — the data is usable, only the *typed* contract is missing, and
everything downstream that needed it had to re-derive its own ground.

Forks, none taken (this is a ruling, not an implementation choice):
land as-is and let readers cast (what happened); a thin post-read
projection applying authored casts on our side of the wire; a
recipe-level type annotation.

## 2. `temporal()` abstains bare, and the abstention misinforms

`temporal.rhai:25` returns `#{ applicable: false }` with no `reason`.
Per the glossql skill a bare abstention means *"the subject genuinely
doesn't fit (a text column has no outliers); stop trying."*

Here the subject is a date. Only its **type** does not fit. A reader
following the skill correctly stops — and never learns that a fixable
import problem, not the data, is the cause.

The rest of the library already does this properly:
`dimension_relevance.rhai` carries four distinct reasons (`"nulls
dominate (ratio > 0.5)"`, `"near-key axis"`, `"constant axis"`,
`"empty column"`), `hierarchies.rhai` and `behavior_evidence.rhai`
likewise. `temporal.rhai` is the outlier at both its abstention sites
(`:25`, `:39`). `outliers.rhai:16` is bare too but correctly so —
a text column really has no outliers.

Cheap fix, and it converts a silent dead end into a lead.

## 3. `detect_relationships` exhausts the process file-descriptor limit

Never completed. Three invocations, three different failure points —
`query_all[54]`, `query_all[62]`, `query_all[63]` — all:

```
External error: DataInvalid => Failed to open file
  …/warehouse/fin2/ledger/data/…parquet: Too many open files (os error 24)
```

Measured rather than guessed: sampling the server's open fds at 0.5 s
during a run gave `22 → 144 → 92 → 118 → 89 → 175 → 236 → 147 → 32`.
A **concurrency burst, not a leak** — handles are released after. The
soft limit is 256 (`launchctl limit maxfiles`, the macOS launchd
default the server inherits). Peak sampled 236; the true peak crossed
256.

The corpus is small — 78 parquet files across 7 tables, 16 of them
the ledger's. The burst comes from the function's own fan-out of
sequential dataset-wide queries, each opening the full file set,
against an engine free to scan them in parallel.

This is the same root cause the f1 run recorded
(`2026-08-06-f1-run.md`: quadratic engine round-trips → timeout)
showing a second face. There the pair scan ran out of time; here it
runs out of file descriptors. The SPIDER/SINDY redesign should
retire both, but the redesign is not in yet, and **on this dataset the
function is unusable at any timeout**.

`dimension_relevance` (column grain) and `detect_hierarchies` (table
grain, including on the 810,059-row ledger) both ran fine. It is
specifically the dataset-grain fan-out.

Workaround used: judged the relationships from the source's six
declared FKs with hand-written anti-joins — which the add-source skill
already sanctions as evidence. What was lost is precisely what the
function is for: **recall over candidates nobody declared**. Two of
the six declared FKs survived judging; whether a seventh edge exists
that no FK names is unanswered for this run.

## 4. A relational recipe discloses no cast accounting at all

Every recipe reported:

```
casts unaccounted — the recipe ran at the source — its dialect owns the casts
```

`CastAccounting::Unchecked` (`crates/import/src/lib.rs:151-153`). The
reasoning is sound — the source computed it, so we cannot attribute a
NULL to a cast we did not run. But the consequence is that the
add-source flow's central safety net is absent on this path. On a file
source the landing tells you `cast-nulled cells — amount: 12 ['\N'
×10, …]`, and the skill teaches you to judge those tokens. Here:
nothing.

It cost real work. SQLite's dynamic typing meant `employees.Billing_rate`
was stored as `text` in all 50,000 rows despite its `DOUBLE`
declaration, and landed `Utf8` — caught only because I inspected the
landed types by hand. `Quantity` and `Rate` were mixed integer/text
across rows. Every sentinel (`--` in six columns, `nan` in one) had to
be found by probing rather than reported by the landing.

The honest counting is available: a source-side
`count(col) - count(cast_expr)` per `try_*` is one extra query. If the
accounting cannot be attributed, the disclosure could at least say
*which* columns went through a cast, so a reader knows where to look.

## 5. No way to read a landed table's schema

`DESCRIBE employees` is refused:

```
the substrate is not open for DESCRIBE employees — tables come from
recipes; removal is DROP TABLE (SPEC.md §3)
```

`information_schema.columns` does not exist either
(`table 'datafusion.information_schema.columns' not found`).

The add-source skill tells you to rehearse the landing identity with a
`LIMIT 0` probe — but a probe returns rows, and rows carry no types
through the JSON door. So after landing there is no way to see what
you landed. What worked:

```sql
SELECT arrow_typeof(hire_date), arrow_typeof(billing_rate) FROM employees LIMIT 1;
```

Per-column, hand-spelled, and it needs a row to exist. `DESCRIBE` is
a read, not schema-altering DDL; the refusal message argues against
DDL, which `DESCRIBE` is not.

Cost: I burned a landing to discover the typing. To learn what the
driver would hand back I declared `ledger` as a 100-row probe recipe,
read `arrow_typeof` off it, then re-declared it in full — leaning on
supersede-and-reland to undo a diagnostic. That is the wrong tool for
"what type is this column".

## 6. No `datasets` relation

`SELECT * FROM datasets` → `table 'datafusion.public.datasets' not
found`. The MCP server instructions advertise live state as
"functions, aspects, witnesses, sources, glossary, cache, imports" —
datasets are genuinely absent, not misspelled.

So an agent entering a workspace cannot ask what datasets exist; it
can only `USE` one and find out. Minor, but it is the first question
of every session, and the skill's "never guess at workspace state —
read it through the language" has no answer for it here.

## 7. An agent cannot land an engineer pin where the framework expects it

The glossql-metrics skill (now §9) closes the flow by asking the user to pin
every definitional choice, and says: *"The user's answer lands as a
re-gloss — the human slot supersedes and outranks yours in every
collapsed read, and the grounding's basis becomes `engineer-pinned`
from then on."*

The user answered four pins in this run. But actor rides the
connection (no BY clause, by design), so an agent writes only the
agent slot. The prescribed mechanism is unreachable from the side that
asks the question.

What I did instead: re-glossed my own slot with
`basis: "engineer-pinned 2026-08-07"` and the rejected alternative
named beside each. It records the decision, but it does not do what
the skill promises — nothing structurally separates a pinned
definition from an agent guess in a collapsed read, and a later
detector cannot band on the difference. The skill describes a flow the
door does not currently support.

## Not bugs — my own errors, recorded so they are not re-litigated

- **Driver name.** I declared `driver: 'adbc_driver_sqlite'`, following
  the skill's `adbc_driver_postgresql` example, and got
  `NotFound: Driver not found`. `dbc` installs drivers as manifests
  under `~/Library/Application Support/ADBC/Drivers/` where the
  registered name is the short one, `sqlite`. The error message was
  exact and the fix took one statement. Worth one clause in the
  add-source skill: the `driver:` value is whatever the local ADBC
  manifest registers, and `dbc list` prints it.
- **Grounding schema.** I put `formula` and `verified` keys in a QUERY
  gloss body and was refused —
  `GROUNDING_SCHEMA` is `additionalProperties: false` over `sql` and
  `assumptions` (`crates/glossary/src/schemas.rs:8-28`). Correct
  refusal, clear message: formulas belong in the `formulas` FACT
  aspect the skill already declares, and a verified read belongs in
  `sql`. My misreading, not a schema gap.

## The dataset, for the record

booksql's SQLite copy diverges from its CSV export
(`2026-08-05`, dataset `finance_2`) in one material way: **the
`payment_method` column is the string `nan` in all 810,059 rows**,
where the CSV run found that edge perfectly clean. Three of five
composite FKs held in the CSVs; two hold here. The customer and vendor
edges are dead in both (0.27% and 0.5% in-business resolution), and
the account-name and product-label findings reproduce exactly
(41,110 product orphans in both runs).

New from the SQLite copy, not visible in the CSV run: **five account
names are carried twice per business under two different account
types** (income and expenses; fixed assets and other expense for
`depreciation`). 89,443 ledger lines carrying $987.8M — 18% of all
credit — hang on the ambiguity, and joining for `account_type`
inflates the ledger by exactly that many rows. The ledger's own entry
side resolves it with no residue, which is what the run's revenue
grounding does.
