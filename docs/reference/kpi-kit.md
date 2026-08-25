# The KPI kit

The semantic vocabulary a workspace starts with, declared at boot
beside the measurement library (`kpi_kit.glossql`). The kit ships the
questions — what a column means, how a number behaves, its unit, which
axes slice, what a table is, where definitions and formulas live, how
a correction travels. The answers — the company's own metrics,
validations, scenarios — stay authored in the flows. The kit is
statements, so adapting it is an edit.

An unwritten witnessed claim is owed: `unassessed` disclosure, the
backlog, and the question round all derive from what stands unwritten.
`WHEN` narrows what a column owes to what its role makes meaningful —
a role-less column owes only `role`, and the backlog counts what is
real instead of columns × vocabulary. Statistics (`behavior`, `unit`)
are the agent's measurement backlog, never a human question; the round
asks judgment only.

## Aspects

| Aspect | Kind · grain | Holds |
| --- | --- | --- |
| `meaning` | FACT ON TABLE, COLUMN, RELATIONSHIP | `value` (prose), optional `term` |
| `role` | FACT ON COLUMN | `key` / `measure` / `dimension` / `timestamp` / `attribute` |
| `behavior` | FACT ON COLUMN WHEN `role = 'measure'` | `stock` / `flow` / `none`, with `grounds` |
| `unit` | FACT ON COLUMN WHEN `role = 'measure'` | the unit, optional `source_column` |
| `dimension` | FACT ON COLUMN WHEN `role = 'dimension'` | `primary` / `supporting` / `none`, with `grounds` |
| `entity` | FACT ON TABLE | what the table is: `value`, `role` (fact/dimension), `grain`, `time_axis`, `identity_columns` |
| `conventions` | FACT ON SOURCE | fiscal calendar, sign conventions, what an export's nulls mean — read and superseded workspace-wide |
| `formulas` | FACT ON DATASET | registry keyed by concept name: window-generic expressions over sibling concepts (`revenue[w] - expenses[w]`), the window `w` the one free variable — what a drill re-scopes from. Known gap: operands are not checked against declared concepts, so a typo is silent |
| `definitions` | FACT ON DATASET | registry keyed by concept name: the handbook content — meaning, unit, owner, source — everything the company might revise, living where supersession applies. The aspect blob keeps only `title` and `x-kind`; a field lives in exactly one place, never both |
| `cube` | FACT ON DATASET | the cube's shape: `resolution`, the floor (`minute` … `year`, default `day`) — a metric's cells stand at its judged cadence and never finer; `windows`, the retention ladder — per resolution the maximum window back from the data's edge (defaults: minute 1 day · hour 1 month · day 18 months · week 3 years · month 48 months · quarter 10 years · year 20 years). A gloss overrides the floor or any rung. Witness-free: a setting is not a claim about the data |
| `recipe_change` | FACT ON TABLE | `table`, `sql`, `reason` — the correction channel: the human gloss is the approval, the re-declare is the agent's next act |
| `ruling` | FACT (grainless) | `rulings`: a list of `{aspect, key, stance, dimension?, assumption?, note?}` with stance `confirmed` / `corrected` / `unclear`. The `key` is the only thing joined on; `assumption` is the prose snapshot the human read, never a match column. Witness-free on purpose — actor kind stamps the writer, and a witness would put an unassessed row on every subject |
| `app` / `app_page` / `app_frame` / `app_spec` | FACT (grainless) | an app's parts as glosses — `title` + optional `dataset` pin; `html`; `sql`; `spec`. One gloss per part, so an author edits one frame without rewriting the app. Grainless and witness-free: an app part is not a claim about the data, so it owes no verdict |

Names machinery reads, shipped by this file: `recipe_change` (the
brief counts pending approvals by that name), `title` / `x-kind` in
QUERY-aspect blobs and `unit` / `meaning` in `definitions` entries
(the docket's metric faces and `metric_surfaces`), `cube` (the floor
and the ladder `metric_series()` computes under), and `none` as the
judged negative in enum aspects — "examined, does not apply" is a
different fact from an unassessed row, and the one that lets the
backlog walk to zero.

## Witnesses

| Witness | On | Voices | Detector |
| --- | --- | --- | --- |
| `meaning_w` | `meaning` | AGENT, HUMAN | — |
| `entity_w` | `entity` | AGENT, HUMAN | — |
| `role_w` | `role` | AGENT, HUMAN | `slot_entropy`, threshold 0.7 |
| `behavior_w` | `behavior` | AGENT, HUMAN | `slot_entropy`, threshold 0.7 |
| `unit_w` | `unit` | AGENT, HUMAN | `slot_entropy`, threshold 0.7 |
| `dimension_w` | `dimension` | AGENT, HUMAN | — |
| `bands_w` | `metric_bands` | — | `band_breach`, threshold 0.98 |

A witness gates who may speak and makes the unwritten claim owed;
detectors adjudicate the written slots — band and score, never a
verdict of their own.
