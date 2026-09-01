# The other doors — read for a what-if, a which-rows question, a bespoke function or an app

## Asking what would happen — the scenario door

A what-if is declared and then read, never hand-edited SQL: the
declared form is versioned, witness-gated and reproducible. One FACT
aspect per scenario, as one QUERY aspect is one metric.

```glossql
DECLARE ASPECT demand_surge WITH $${
  "title": "Orders +15% from Jan 2027",
  "x-kind": "scenario",
  "type": "object", "required": ["overrides"],
  "properties": {"overrides": {"type": "array", "items": {
    "type": "object", "required": ["column", "factor", "from", "basis"],
    "properties": {"column": {"type": "string"}, "factor": {"type": "number"},
                   "from": {"type": "string"}, "basis": {"type": "string"}}}}}
}$$ AS FACT ON DATASET;
```

Each override names a real column, a factor, a start month and its
**basis** — the same discipline the grounding assumptions carry. A
behavioral response no history ever saw is not guessed: declare it as
its own override and say so, or leave it out and let the read name it.

```glossql
GLOSS demand_surge ON ops AS $${
  "overrides": [
    {"column": "work_orders.order_count", "factor": 1.15, "from": "2027-01",
     "basis": "the declared lever"},
    {"column": "work_orders.duration_min", "factor": 1.05, "from": "2027-01",
     "basis": "assumed congestion response, hand-declared; not in any history"}
  ]
}$$;
```

`whatif.<scenario>()` then serves one relation over every concept the
replay reaches. Sweeps are `WHERE` clauses over it, never a special
form:

```sql
SELECT concept, month, replay, p05, p50, p95, basis
FROM whatif.demand_surge() WHERE concept = 'throughput' ORDER BY month
```

- **Read `basis` before the numbers.** A concept no declared path
  connects to the overridden columns comes back as a refusal row with
  its reason, not a silent guess — `detect_derivations` proposes the
  identities that would close the gap.
- `replay` is exact arithmetic at the declared factors; the bands are
  the model's. Both are served so neither hides behind the other.
- The server replays each grounding at a bracket of strengths around
  your factor, so the scenario's own point is always interpolation.
  Nothing about that grid is yours to write.

## Which rows — the sample door

When a signal fires and the question is _which rows_, author a sample
frame: a QUERY aspect with `x-kind: "sample"`, glossed with one SELECT
that holds known-good history and the suspects together, read through
`misfit.<frame>()` for a per-row score.

```sql
SELECT * FROM misfit.late_pairs() ORDER BY misfit DESC LIMIT 20
```

Pick the surface to match the suspicion: a relationship suspicion
needs the **join** in the frame — a single table cannot see wrong
pairings whose individual values are all legal. The more known-good
history the frame carries, the cleaner the ranking. Run it on a
signal, never as a routine sweep.

Both doors are on the affordance map (`SELECT * FROM workspace_next`)
as `scenarios` and `samples`, with `open` counting vocabulary that
stands without a body.

## Author what is missing

**A function** when a shipped measurement does not fit this dataset's
shape — and the measuring half of a validation, which the expectation
gloss owes (`skill://glossql-metrics/references/validate.md`). 
**`glossql-functions` teaches it**: the declaration
carries the body, so a check is writable over the door and the
shipped library reads back as worked examples
(`SELECT script FROM functions WHERE name = 'rate_tolerance'`). The
short version: a measurement's body is one SQL query the engine plans,
no `RETURNS` declares a detector (a script over slots), and a function
abstains (`applicable: false` with a reason) rather than throwing.

**An app** when someone needs to look at this — a standalone page at
`/<dataset>/app/<name>` whose URL is its whole state, so a filtered
view is a link somebody can send. **`glossql-apps` teaches it**: shape it with
the user in prose first, then write it as glosses (`app`, `app_page`,
`app_frame`, `app_spec`, one per part). Add an app beside the docket
rather than forking it. Frames are SQL and display logic is computed
there, never in a template; an app you author carries no write.
