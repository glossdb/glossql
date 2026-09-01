---
name: glossql-apps
description: Author a data app on the glossql app door — a standalone page at /<dataset>/app/<name>, shaped with the user in prose, written as glosses (app, app_page, app_frame, app_spec). Frames are SQL, tiles are macros, filters live in the URL. Use when someone needs to look at what the workspace knows, after the numbers exist.
---

# Authoring apps

A data app is **its own page, and its URL is the whole state**. It
serves at `/<dataset>/app/<name>`; the dataset is the first segment
and every filter a reader picks lands in the query string, so any
view is a link they can paste to somebody else. That is the point:
not a dashboard you log into, a page you send.

Write one when someone needs to *look* at what the workspace knows. It
is the last thing you build, not the first: an app over numbers nobody
has grounded is a picture of nothing.

**Add an app; don't fork the built-in.** The docket ships in the
binary, with tests that run against it. A workspace that wants a
different view of the same rows authors its own app beside it — the
door serves as many as you write, and yours does not inherit anyone
else's assumptions.

A part glossed under a built-in's name — `docket` — is refused: a
single part resolves the whole app, and every other page the built-in
ships would stop serving. Give yours its own name.

## Shape it with the user first, in prose

No forms anywhere in this flow — this is the conversation register.

1. **The job, in one sentence.** Who opens this page, and what single
   question does it answer? An app has a topic as a dataset does, and
   every tile earns its place against that job or gets cut. This is
   where "a monitoring dashboard" becomes "the Monday operations
   review page".
2. **The tile list, as a proposal.** Each tile named with its read, its
   slice and its chip — "cycle-time trend with the rival line ·
   throughput by region, top 8 · the open-questions count". Propose from what the
   glossary already ranks: the judged axes, the grounded surfaces.
   The user prunes and extends in words.
3. **Author, then hand over the URL.** The rendered page *is* the
   proposal made concrete. They react in prose — "swap the trend for
   by-region", "wrong grain on that number" — and you re-gloss.
4. **Close with a read-back**: what shipped, what was cut and why,
   which tiles carry disclosed assumptions. Approval is the user
   saying so, in chat.

## An app is glosses

One gloss per part, so a frame can be edited without rewriting the app
and supersession versions each part on its own. The aspect says what
kind of file it is; the subject says where it goes.

| aspect | subject | becomes |
|---|---|---|
| `app` | `delivery` | the manifest |
| `app_page` | `delivery.index` | `index.html` |
| `app_frame` | `delivery.monthly` | `frames/monthly.sql` |
| `app_spec` | `delivery.trend` | `specs/trend.vl.json` |

```glossql
GLOSS app ON delivery AS $${"title": "Monday operations"}$$;
GLOSS app_frame ON delivery.monthly AS $${"sql":
  "SELECT date_trunc('month', date) AS period, sum(value) AS value FROM read.throughput() GROUP BY 1 ORDER BY 1"}$$;
```

A manifest names no dataset — the URL binds it, so the same app serves
every dataset in the workspace and the reader switches with the picker
in the header. Read back what an app is made of:

```sql
SELECT app, path, actor_kind FROM app_parts ORDER BY app, path
```

## Frames are SQL, and they are where the thinking goes

One SELECT per frame, streamed as Arrow IPC. The browser fetches each
frame once per state and every tile bound to it shares that one table.

- **URL params bind as plan placeholders** — `$region` in the SQL,
  `?region=EMEA` on the URL. Everything arrives as Utf8, so cast
  explicitly (`CAST($from AS DATE)`), the same posture recipes take. A
  placeholder nobody bound fails the request with a message naming it;
  extra params are ignored. The dataset is not a param: your frame runs
  on a channel bound to the one the path named, so join
  `current_dataset` when a read answers for the whole workspace.
- **Compute display logic in the frame, never in the template.** A
  label, a percentage for a bar's height, a CSS class chosen by a
  verdict — all of it is a column. Templates place values; they do not
  decide them.
- **Cast view types back.** The browser's Arrow reader speaks only the
  classic types, and a `Utf8View` in a frame's schema renders as
  `Unrecognized type`. Anything built by `json_get_str`, `concat_ws`
  or `coalesce` needs `arrow_cast(x, 'Utf8')` on the way out.
- **State the cap.** A frame that shows a top-N says so in the tile's
  chip or note. A truncated list that looks complete misleads without
  anyone noticing.

A frame is planned through the same path every other read takes, so
every door is available inside one — `read.<metric>()` for a grounding,
`metric_series(grain => $grain)` for the cube's cells at a grain
(`dimension = ''` is the total, `'alternative'` the disclosed rival;
a row carries `num`/`den` for a ratio's summed halves and `behavior`,
the verb that made it), `metric_axes()` for what the cube admitted per
metric, and `whatif.<scenario>()` for a declared what-if beside the
real books:

```sql
SELECT month, replay, p05, p50, p95 FROM whatif.capacity_shift()
WHERE concept = CAST($concept AS VARCHAR) ORDER BY month
```

The one limit: a placeholder binds a **value, not a relation name** —
a door argument counts as a value (`metric_series(grain => $grain)`
works).
The scenario is fixed per frame; the URL steers the concept, the month,
the slice. A page comparing two scenarios holds two frames.

A viewer's window is ordinary params: `?grain=quarter&span=24` binds
as `$grain` and `$span`, and the frame SQL is the filter — the cube
serves the grain by the metric's verb on the server, the frame clips
the span (`dense_rank() OVER (ORDER BY period DESC)` against `$span`).
A reference's own params are the author's defaults
(`frames/trend?grain=month&span=24`); the page URL overrides them, and
only the frames whose URL changed refetch. Keep the back-control on
top: whatever narrows a view — the window, a slice picker, the crumbs
— sits above what it narrows, never below or inside it.

## Tiles place what the frame computed

Pages are tera, extending `shell.html` and importing the tile
vocabulary. Four macros and a prose block:

```html
{% extends "shell.html" %}
{% import "modules/tiles.html" as tiles %}
{% block main %}
<div class="tiles">
  {{ tiles::value(frame="frames/front", field="open", label="Open questions",
       chip="open_questions", note="what the door would ask a human") }}
  {{ tiles::chart(frame="frames/monthly", spec="specs/trend.vl.json",
       title="Throughput by month", chip="read.throughput()") }}
</div>
{% endblock %}
```

- `value(frame, field, label, …)` — one number, `format` in
  `compact | text`. Use `text` for a word (a band name); the compact
  format renders `NaN` for it.
- `chart(frame, spec, …)` — a vega-lite spec over the frame; a window
  over its series is the frame's own params.
- `gl-rows` with your own `<template>` — a row surface where you place
  each field by name (`{subj}`, `{what}`), for anything that is a list
  of matters rather than a table of numbers. `join="frames/<name>"
  on="<key>"` merges a second frame's row by key into each row — how
  a record frame and a data frame meet in one list without becoming
  one fetch (the docket's pulse: `metric_surfaces` rows joined to the
  cube's numbers by metric name).
- `<gl-window frame="frames/<axes>">` — the viewer's grain and span as
  links over the page URL; it reads the metric's resolution from a
  `metric_axes()` frame and offers the grains from there up. Member
  moves are a frame, not a component: rank them in SQL (the docket's
  `drivers.sql` is the shape) and place them with `gl-rows`.
- The **chip** is the tile's provenance: which read the number comes
  from, with `note` as its hover text — a disclosed assumption, a
  composition rule. A tile without a chip is a number with no address.

## Specs draw, they do not decide

`app_spec` bodies are vega-lite JSON over the frame's columns. Keep
them thin: encodings and marks. A spec that filters or aggregates is
doing the frame's job in a place nobody can test.

Pin `$schema` to the vega-lite major the door serves — today that is
`https://vega.github.io/schema/vega-lite/v6.json`. The v5 reflex is
outdated here: an older major renders but logs a version warning on
every load. `{"data": {"name": "frame"}}` is how the spec names the
frame the tile bound.

A corridor is a `layer`: an `area` with `y`/`y2` under the lines, and
`"color": {"datum": "as it happened"}` on each line to earn a legend
entry without hard-coding a colour.

## An app you author carries no write

The door takes exactly one write in the whole system — the docket's
ruling form, which answers a question the workspace already derived and
posts only that claim's identity. Everything else is a read.

Anything an app of yours needs to change, change with a statement. The
page is a view of the record; it is never the way into it.
