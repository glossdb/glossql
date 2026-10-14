# The other doors — read for a which-rows question, a bespoke function or an app

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

The sample door is on the affordance map (`SELECT * FROM
workspace_next`) as `samples`, with `open` counting vocabulary that
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
