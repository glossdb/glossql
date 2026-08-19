# The shipped reads

Eight relations every workspace serves. Each is one `.sql` file
shipped in the binary; the same file answers the doors, the built-in
app's frames, and the skills' examples. The names are reserved — a
read shadows both a table and a CTE of the same name, which is what
keeps the set small.

A read expands as a derived relation: filter with `WHERE`, order at
the call site (an inner ordering does not survive planning — none of
the reads carries one). Formatting, glyphs, and links are always the
caller's business.

### workspace_next

The map of surfaces: what kind of thing can be declared or written
here, how much of it stands, what is open on it. Not a task queue and
not an order — judgment about what to do next stays the agent's.
Columns: `surface` (sources, tables, relationships, aspects, claims,
functions, metrics, scenarios, samples, rulings, cube, apps) · `how`
(the statement that extends the surface) · `stands` (what exists) ·
`open` (what is unfinished; 0 where nothing can be owed — a function
is never "open", it computes when a read needs it). A table is open
while its landing's casts nulled cells; the cube is open while a later
write has orphaned it.

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
whose key still stands below full confidence).

### metric_surfaces

Every declared metric with where it stands — the pulse list and the
dossier header both render this. Columns: `metric`, `title`, `kind`
(the `x-kind` tooling flag), `unit` and `meaning` (from the
`definitions` registry — the aspect blob keeps only display label and
flag), `period` / `value` / `delta` (latest cube month and the move
into it), `axes` (the dimensions the cube admitted), `formula` (from
the `formulas` registry, or the stated base-concept default),
`grounded`. Values come from the `metric_cube` measurement: a metric
with no cube rows carries nulls until the cube runs. Open counts and
ruled-at live in workspace-wide relations — callers join
`open_questions` and `ruling_entries` scoped to their own dataset.

### app_parts

Apps authored as glosses, one row per file. Columns: `app`, `path`
(where the part goes: `index.html`, `frames/open.sql`,
`specs/series.vl.json`, or `app` for the manifest), `text` (the file
content as the gloss spelled it), `actor_kind`. Two collapses in
order: newest writing per (subject, aspect, actor kind), then the
human's over the agent's.
