---
name: glossql-apps
description: Author a server-rendered data app on the glossql app door — app.toml, a tera page, SQL frames, vega-lite specs. Use when the target wants a monitoring or exploration surface over the workspace's metrics or its glossary, after the metric framework exists.
---

# Authoring apps

Still moving as more apps are written. Three apps ground the rules —
cash (tiles over `read.billings()` / `read.dso()`), hand-written in a
workspace, and two built-ins: model (tiles over `GLOSSARY()` itself,
the verification surface) and metrics (the business surface — the
metric dossiers, slices from the cached cube through
`metric_series()`, the metric graph). A workspace `apps/<name>/`
shadows a built-in of the same name, so forking one is copying its
directory out.

## What an app is

A directory in the workspace: `apps/<name>/` holding `app.toml`
(title + dataset), `index.html` (a tera page extending `shell.html`),
`frames/*.sql`, `specs/*.vl.json`. Hot-loaded — save the files, reload
the page; only the door's own templates need a rebuild. The author
writes declarative artifacts, never code: the page states, the frames
read, the specs draw.

## Frames

- One SELECT per frame, streamed as Arrow IPC. The browser fetches
  each frame once per state and every tile bound to it shares the
  table.
- URL params bind as plan placeholders — `$from` in the SQL, `?from=`
  on the URL. Everything arrives Utf8: cast explicitly
  (`CAST($from AS DATE)`). Extra params are ignored; an unbound
  placeholder fails at read with a message naming what the URL owed.
- The frame carries the superset; the page picks the spec. Billings
  always emits its prior-year column; whether the prior line renders
  is the page choosing between two plain specs. Two plain specs beat
  one clever spec.
- Cast view types back to classic. `substr()` returns Utf8View and
  the browser's arrow reader speaks only classic types:
  `arrow_cast(substr(body, 1, 200), 'Utf8')`. `||` concatenations and
  `CAST(x AS VARCHAR)` produce view types too — arrow_cast the final
  string expression, or the tile shows `Unrecognized type` instead of
  data.
- A stated cap, never a silent one: `LIMIT` in the frame, the number
  on the tile's note.
- `GLOSSARY(all => true)` is an ordinary frame source — the model app
  is nothing but frames over it (census, ranking, dossier), joined and
  filtered with plain SQL, `json_get_*` reaching into gloss bodies.
- `metric_series()` is the cube read: the cached `metric_cube`
  measurement as long rows `(metric, dimension, member, period,
  value)` — dimension `''` is the monthly total, `'alternative'` the
  disclosed rival reading. A static frame cannot name a metric in
  FROM (`read.<name>()` is per-name), so this is how a generic frame
  slices any metric: plain value filters, `$metric` and `$dim` from
  the URL. Empty until `SELECT metric_cube() FROM <dataset>` runs —
  say so in the tile's empty text.

## Pages

- The URL is the only state. Read `state.<param>`, set defaults at
  the top of the block, derive everything else. Params are page-scoped
  flat names the author keeps unique.
- Page-global filters are boosted links — full render. Widget-local
  filters: give the tile an `id` plus the seg args and its control
  swaps only that tile while still writing the URL.
- Drill is navigation: `drill_field`/`drill_param` on a chart puts the
  clicked datum in the URL (`drill_type="date"` for temporal axes);
  the narrowed tiles render behind `{% if state.x %}`. Each back-link
  drops exactly one param and keeps the rest.
- Tera notes that bite: `{% set %}` inside `{% if %}` reaches the
  enclosing scope (the spec-choice pattern relies on it); `set` takes
  no parenthesized filter expressions — filter into a variable first,
  concatenate after; a `urlencode` filter is registered for values
  that ride into hrefs.

## Tiles

`value` / `chart` / `table` / `prose` from `modules/tiles.html`. The
chip is the tile's provenance — which surface the number comes from
(`read.dso()`, `GLOSSARY()`) — with `note` as its hover text: a
disclosed assumption, a composition rule, a stated cap. `hint` is the
teaching line and carries the back-links.

For row-shaped surfaces (queues, claim lists, worklists) there is
`gl-rows`: give it a frame and a `<template>` child, and every
`{field}` in the template's text or attributes takes the row's value.
Display logic — glyphs, css classes, drill hrefs — is the frame's
job, computed as SQL columns; the template stays dumb. An `empty`
attribute states what a zero-row frame means (absence is a claim);
a cap over `rows` gets the same honest footer a table gets. The
world model app is the reference use.

## Specs

vega-lite, data source named `frame`, width defaults to container.
Fixed pixel heights — step heights fight autosize-fit and warn.
Zero console warnings is the bar: an all-null layer, a step height,
a wrong axis type all show up there.

## A scenario ships with its tile

A what-if scenario (a FACT aspect read through `whatif.<name>()` —
the metrics skill teaches declaring one) charts through an authored
tile: the scenario's name sits in the frame's FROM clause, which no
built-in can know, so when you declare a scenario for a workspace
with apps, author its tile in the same flow. The built-in apps list
scenarios generically; the chart is yours.

The frame picks one concept and serves the door's columns:

```sql
-- frames/whatif_price_hike.sql
SELECT month, replay, p05, p10, p50, p90, p95, basis
FROM whatif.price_hike()
WHERE concept = 'revenue' ORDER BY month
```

The spec is a band fan with the replay line over it — the model's
uncertainty and the exact recomputation, visibly separate (follow the
metrics app's `specs/bands.vl.json` shape: two area layers p05/p95 and
p10/p90, a dashed p50 line, a solid `replay` line). The chip is
`whatif.price_hike()`; the `note` names the lever and its basis; the
`hint` says what the bands are and that `basis` carries refusals.
Refusal rows have NULL months — keep the `WHERE concept = …` so an
unmoved concept's refusal row doesn't feed the chart, and consider a
`gl-rows` tile beside the chart listing `DISTINCT concept, basis` so
refusals stay visible instead of filtered away.

## Apps carry no write

Every frame reads; there is no write route on the app door. Answers
travel through a session — the human tells their agent (or answers
the door's question form when the client renders one), and the
statement lands on the human channel with human standing. An open
row leaves a queue by derivation — a human slot exists on its
(subject, aspect) — never by mutation, and never by an app-side
gesture. If a page seems to need a write, it is describing a
statement; show the statement (the contest pattern) and let the
session run it.

## The misfit read stays out of frames

`misfit.<frame>()` (the metrics skill teaches it) is signal-triggered
investigation: it recomputes the density on every fetch, so a tile
bound to it turns an on-demand read into a routine sweep on every
page load. Apps surface the conclusions — the judge's glosses —
through `GLOSSARY()` frames; the ranking itself stays a session act.

## Composition honesty

Flows re-aggregate freely; ratios are final at their grain — a
different scope recomposes from the components, never regroups the
rows. Nothing is summed without a `behavior` gloss under it.

## Choosing what to build

The glossary is the design input, read with the same SQL at
generation time:

- Drill and facet columns come ranked, not guessed:

  ```sql
  SELECT subject,
         json_get_float(body, 'evenness') AS evenness,
         json_get_float(body, 'coverage') AS coverage
  FROM GLOSSARY(all => true)
  WHERE aspect = 'dimension_relevance'
    AND json_get_bool(body, 'applicable')
  ORDER BY evenness DESC
  ```

- Summability is the `behavior` gloss (flow or stock) under each
  value column; the metric surfaces are the declared QUERY aspects.

Resolve these reads while authoring and write plain artifacts. When
the model moves, regenerate — the app is cheap, the knowledge is not.
