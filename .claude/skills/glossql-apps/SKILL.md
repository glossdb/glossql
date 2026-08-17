---
name: glossql-apps
description: Author a data app on the glossql app door — a standalone page at /app/<name>, shaped with the user in prose, written as glosses (app, app_page, app_frame, app_spec). Frames are SQL, tiles are macros, filters live in the URL. Use when someone needs to look at what the workspace knows, after the numbers exist.
---

# Authoring apps

A data app is **its own page, and its URL is the whole state**. It
serves at `/app/<name>`; every filter a reader picks lands in the query
string, so any view they are looking at is a link they can paste to
somebody else. That is the point of the surface — not a dashboard you
log into, a page you send.

Write one when someone needs to *look* at what the workspace knows. It
is the last thing you build, not the first: an app over numbers nobody
has grounded is a picture of nothing.

**Add an app; don't fork the built-in.** The docket ships in the binary
and is covered by tests that run against it. A workspace that wants a
different view of the same rows authors its own app beside it — the
door serves as many as you write, and yours does not inherit anyone
else's assumptions. (Forking is a later question, once the docket has
been used enough to be worth forking.)

The door holds this: a part glossed under a built-in's name — `docket`
— is refused, because a single part resolves the whole app and every
other page the built-in ships would stop serving. Give yours its own
name.

## Shape it with the user first, in prose

No forms anywhere in this flow — this is the conversation register.

1. **The job, in one sentence.** Who opens this page, and what single
   question does it answer? An app has a topic exactly as a dataset
   does, and every tile earns its place against that job or gets cut.
   This is where "a monitoring dashboard" becomes "the Monday cash
   meeting page".
2. **The tile list, as a proposal.** Each tile named with its read, its
   slice and its chip — "DSO trend with the rival line · billings by
   region, top 8 · the open-questions count". Propose from what the
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
| `app` | `cash` | the manifest |
| `app_page` | `cash.index` | `index.html` |
| `app_frame` | `cash.monthly` | `frames/monthly.sql` |
| `app_spec` | `cash.trend` | `specs/trend.vl.json` |

```glossql
GLOSS app ON cash AS $${"title": "Monday cash"}$$;
GLOSS app_frame ON cash.monthly AS $${"sql":
  "SELECT date_trunc('month', date) AS period, sum(value) AS value FROM read.revenue() GROUP BY 1 ORDER BY 1"}$$;
```

A manifest with no `dataset` binds to the workspace's sole dataset at
request time. Read back what an app is made of:

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
  extra params are ignored. `$dataset` is always bound for you.
- **Compute display logic in the frame, never in the template.** A
  label, a percentage for a bar's height, a CSS class chosen by a
  verdict — all of it is a column. Templates place values; they do not
  decide them.
- **Cast view types back.** The browser's Arrow reader speaks only the
  classic types, and a `Utf8View` in a frame's schema renders as
  `Unrecognized type`. Anything built by `json_get_str`, `concat_ws`
  or `coalesce` needs `arrow_cast(x, 'Utf8')` on the way out.
- **State the cap.** A frame that shows a top-N says so in the tile's
  chip or note. A truncated list that looks complete is the one lie a
  surface can tell without anyone noticing.

A frame is planned through the same path every other read takes, so
every door is available inside one — `read.<metric>()` for a grounding,
`metric_series()` for the measured cube's rows (`dimension = ''` is the
total, `'alternative'` the disclosed rival), and `whatif.<scenario>()`
for a declared what-if beside the real books:

```sql
SELECT month, replay, p05, p50, p95 FROM whatif.price_hike()
WHERE concept = CAST($concept AS VARCHAR) ORDER BY month
```

The one limit: a placeholder binds a **value, not a relation name**.
The scenario is fixed per frame; the URL steers the concept, the month,
the slice. A page comparing two scenarios holds two frames.

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
       title="Revenue by month", chip="read.revenue()") }}
  {{ tiles::table(frame="frames/monthly", title="The months", rows=24) }}
</div>
{% endblock %}
```

- `value(frame, field, label, …)` — one number, with `delta_field`,
  `unit`, and `format` in `compact | days | raw | month | text`. Use
  `text` for a word (a band name); the numeric formats render `NaN`
  for it.
- `chart(frame, spec, …)` — a vega-lite spec over the frame. Adding
  `drill_field` and `drill_param` makes a click navigate: the datum's
  value lands in the URL. **Drill is navigation, nothing else.**
- `table(frame, rows=…)` — the frame as a grid.
- `gl-rows` with your own `<template>` — a row surface where you place
  each field by name (`{subj}`, `{what}`), for anything that is a list
  of matters rather than a table of numbers.
- The **chip** is the tile's provenance: which read the number comes
  from, with `note` as its hover text — a disclosed assumption, a
  composition rule. A tile without a chip is a number with no address.

A tile given an `id` and a `seg_*` set renders a segmented control that
writes the URL but swaps only itself — the rest of the page never
re-renders.

## Specs draw, they do not decide

`app_spec` bodies are vega-lite JSON over the frame's columns. Keep
them thin: encodings and marks. A spec that filters or aggregates is
doing the frame's job in a place nobody can test.

Read the vendored version out of `crates/apps/assets/vendor/README.md`
and pin `$schema` to its major — a spec still claiming `v5` against a
v6 library renders but logs a version warning on every load, which
teaches readers to ignore the console. `{"data": {"name": "frame"}}` is
how the spec names the frame the tile bound.

A corridor is a `layer`: an `area` with `y`/`y2` under the lines, and
`"color": {"datum": "as it happened"}` on each line to earn a legend
entry without hard-coding a colour.

## An app you author carries no write

The door takes exactly one write in the whole system — the docket's
ruling form, which answers a question the workspace already derived and
posts only that claim's identity. Everything else is a read.

Anything an app of yours needs to change, change with a statement. The
page is a view of the record; it is never the way into it.
