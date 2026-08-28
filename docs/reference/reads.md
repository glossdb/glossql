# The shipped reads

Eight relations every workspace serves, and the cube's two table
functions beside them (the last section). Seven are one `.sql` file
each, shipped in the binary; the same file answers the doors, the
built-in app's frames, and the skills' examples. Those seven names are
reserved — a read shadows both a table and a CTE of the same name,
which is what keeps the set small. `current_dataset` is the eighth and
is not a file: it serves session state, which no `.sql` file can reach,
so it is built in Rust and a CTE of that name shadows it the ordinary
way round.

A read expands as a derived relation: filter with `WHERE`, order at
the call site (an inner ordering does not survive planning — none of
the reads carries one). Formatting, glyphs, and links are always the
caller's business.

So is scope, on the reads that have one. `open_questions`,
`agent_assumptions`, `ruling_entries` and `app_parts` answer for the
whole workspace and carry `dataset` on every row, because which dataset
a caller means is the caller's to say — and the two callers of one read
often differ: the MCP question round asks across every dataset from an
unbound channel and routes each answer back by the row's own `dataset`,
while the docket asks about the one it is bound to. `current_dataset`
names the bound one. `owed` narrows itself, because what waits on an
act waits on someone working in a single dataset. The rest read
workspace vocabulary and have no dataset to narrow to.

### current_dataset

The session's bound dataset, as a relation: one row, column `dataset`,
and no rows at all while nothing is bound. It is the name SQL cannot
otherwise reach — a read written as a `.sql` file has no way to say
which session it is answering for — so a read over a workspace-wide
relation narrows itself by joining this. The empty case carries its
own meaning: nothing bound, nothing to answer, without a read having
to test for it.

### workspace_next

The map of surfaces: what kind of thing can be declared or written
here, how much of it stands, what is open on it. Not a task queue and
not an order — judgment about what to do next stays the agent's.
Columns: `surface` (sources, tables, relationships, aspects, claims,
functions, metrics, scenarios, samples, rulings, apps) · `how` (the
statement that extends the surface) · `stands` (what exists) · `open`
(what is unfinished; 0 where nothing can be owed — a function is never
"open", it computes when a read needs it). A table is open while its
landing's casts nulled cells.

### open_questions

What still stands open for a human to judge — the one derivation the
door's question round serves and the docket renders. Derived from
`agent_assumptions` (the agent's current body, never a frozen copy):
only rows below full confidence, with four gates beyond that — the
aspect is a grounding (query kind); the assumption carries a `key` (an
unkeyed assumption cannot be closed, so it is never asked — a known,
accepted gap); the dimension is not one the function map owns
(`behavior`, `sign`, `grain` are statistics — no human is asked for a
number); and no standing ruling names the same (subject, aspect, key). Columns: `dataset`, `subject`, `aspect`, `idx`,
`dimension`, `key`, `assumption`, `basis`, `conf`, plus `sibling` /
`sibling_stance` / `sibling_note` — what the human already ruled on
the same key under a different aspect, so the form can offer it back
(`unclear` is not a judgment and is never offered).

### agent_assumptions

Every assumption the agent currently discloses: the winning agent slot
per (subject, aspect) — any writing that carries an `assumptions`
array, one row per entry (the grounding gate lives in
`open_questions`). Columns: `dataset`,
`subject`, `aspect`, `idx`, `dimension`, `key`, `assumption`, `basis`,
`conf`, and `body` — the whole writing the assumption sits inside, for
re-issuing as a statement. Read the extracted columns rather than
reaching into `body`. No gates: `open_questions` decides what a human
is asked; `ruling_entries` uses the same rows to compute fold-in.

### ruling_entries

The human's standing judgments, one row per ruling entry, newest
writing per subject. Columns: `dataset`, `subject`, `idx`, `aspect`,
`key`, `stance` (`confirmed` / `corrected` / `unclear`), `dimension`,
`assumption` (the prose snapshot — never a join column), `note`,
`written_at`, `folded_in`. `key` names the claim and is the only join
column anywhere — prose is never matched against prose. `folded_in`
is the debt answered: false exactly while the ruled key is still
disclosed below full confidence in the agent's body, clearing when the
re-record lands; nothing is marked done by hand.

### owed

What stands waiting on an act, derived from the data alone — each row
is a mismatch the act itself resolves, so nothing is marked done.
Columns: `kind`, `subject`, `what`, `why`, `since`. Four kinds:
`recipe` (an approved recipe change with no import of that table since
the approval) · `formula` (a human formula answer newer than the
metric's recorded materialization) · `contest` (a slot withheld at
read — voices differ or a detector crossed) · `fold-in` (a ruling
whose key still stands below full confidence). All four answer for the
session's dataset: this is the read that narrows itself, because what
is owed is owed by someone working in one dataset.

### metric_surfaces

Every declared metric with where it stands — the record only; the
pulse list and the dossier header both render this. Columns: `metric`,
`title`, `kind` (the `x-kind` tooling flag), `unit` and `meaning`
(from the `definitions` registry — the aspect blob keeps only display
label and flag), `formula` (from the `formulas` registry, or the
stated base-concept default), `grounded`. The numbers — a metric's
latest period, its move, the axes the cube admitted — are the cube's
reads below, joined by `metric`; keeping them apart is what lets a
ruling refresh this read without touching the cube. Open counts and
ruled-at live in workspace-wide relations — callers join
`open_questions` and `ruling_entries` narrowed to a dataset, which
`current_dataset` names.

### app_parts

Apps authored as glosses, one row per file. Columns: `dataset`, `app`,
`path` (where the part goes: `index.html`, `frames/open.sql`,
`specs/series.vl.json`, or `app` for the manifest), `text` (the file
content as the gloss spelled it), `actor_kind`. Two collapses in
order: newest writing per (dataset, subject, aspect, actor kind), then
the human's over the agent's. Workspace-wide, like the three above —
`workspace_next` counts every app the workspace holds, the app door
serves one dataset's.

## The cube's reads

Two table functions over the cube — every grounded metric's cells at
its resolution, a query result computed at the read's pin from the
grounding and the judged verdicts, cached in memory, never recorded.
Both build what is not built; a cache entry is never stale, it is a
hit or a miss (a moved pin or version misses). The resolution is the
metric's judged cadence (`temporal_profile`), never finer than the
`cube` aspect's floor; the window is that aspect's rung for the
resolution, measured back from the data's own edge (see the KPI kit).

### metric_series(grain => …)

The cells: `metric`, `dimension` (`''` the total, `'alternative'` the
disclosed rival, anything else a judged dimension column), `member`,
`period` (a typed timestamp, the bucket's start), `value`, `num` /
`den` (a ratio's summed halves, NULL elsewhere), `behavior` (the verb
that made the row — `flow`, `stock` or `ratio`; a rival's may differ
from the metric's). Without a grain each metric serves at its own
resolution; with one (`minute` … `year`) every metric at or finer than
it is re-bucketed on the server by the row's verb — a flow sums, a
stock takes the bucket's last period, a ratio divides its summed
halves — and a metric coarser than the asked grain serves no rows.
The one argument is the grain; filters ride `WHERE`.

### metric_axes()

One row per current grounding — what the cube admitted and why not:
`metric`, `applicable`, `judged_current`, `reason` (the road out when
it abstains — no judged time column, no value column, a grounding the
engine refused), `behavior` and `behavior_basis` (the verb and where
it came from: `ratio` when the frame serves `num` and `den`, `marked`
when the grounding carries `behavior`, `evidence` when the
`behavior_evidence` verdict on the column the value is or sums
decided, `default` when nothing said anything and it reads as a
flow), `resolution`, `window`, `dims`, `basis`
and `admitted_by` (per admitted dimension, in `dims` order: the column
subject whose verdict admitted it, and what decided — `measurement`,
or `human` / `agent` where a `dimension` gloss did), `bucketed` (the
dimensions wider than 24 members, served as their top 23 by weight
plus `'other'`), `unadmitted` and `unadmitted_why` (every served
column that is neither the value, a ratio's half nor time-typed and
is not an axis, and at the same index what kept it out with the road
back in: no verdict on its subject — run `dimension_relevance()` over
it or gloss `dimension`; a verdict that abstained with no declared
relationship reaching a judged key; a `dimension` gloss of `none`; an
expression no verdict can reach; one member across the frame; a rank
below the cap of four), `alternative`, `alternative_error`.
Record-class: it says what the judged verdicts admitted.

The same row is what a grounding's write answers with: `GLOSS` on a
QUERY aspect returns the metric's fact at the pin the write moved to,
in this shape, in place of `{"done"}` — the plan stage alone, so
`bucketed` is empty, the member floor is not yet applied and no rival
is run; everything else is what this read will say. A grounding whose
SQL does not plan abstains there with the engine's refusal; the gloss
has landed either way. The row judges the grounding that serves, so
after a human's own grounding on the aspect it is theirs the row
describes, and a call bound to another dataset, or to none, abstains
naming the `USE` that judges it.

A served column is an axis when a verdict admits it: its own
`dimension_relevance`, or — for a label in a dimension table, a
near-key there by construction — the verdict on the key column that
reaches it through a declared relationship from a table the grounding
scans; `basis` names that key. The collapsed `dimension` gloss on the
column is the read policy over that, human over agent: `none` closes
the axis whatever was measured, `primary` admits it and ranks it
first, `supporting` admits it. The verdicts themselves are untouched —
a measurement is never glossed — and `admitted_by` says whose word the
axis stands on.

The verdicts are the newest landed per served column, whatever pin
they were judged at — served and marked, as every function voice is,
and every write moves the pin. After a ruling or an import the cube
still builds — the numbers are current — and `judged_current` is
false until the profilers run again: `temporal()` over the served
date columns, `dimension_relevance()` over the rest, or the docket's
re-measure, which re-runs every measurement standing from before the
last change.
