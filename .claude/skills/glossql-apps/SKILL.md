---
name: glossql-apps
description: Author a server-rendered data app on the glossql app door — app.toml, a tera page, SQL frames, vega-lite specs. Use when the target wants a monitoring or exploration surface over the workspace's metrics or its glossary, after the metric framework exists.
---

# Authoring apps

First sketch (2026-08-09). Two hand-written apps exist — cash (tiles
over `metric.billings()` / `metric.dso()`) and model (tiles over
`GLOSSARY()` itself) — and every rule below survived building them.
Expect this to move as more apps are written.

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
(`metric.dso()`, `GLOSSARY()`) — with `note` as its hover text: a
disclosed assumption, a composition rule, a stated cap. `hint` is the
teaching line and carries the back-links.

For row-shaped surfaces (queues, claim lists, ledgers) there is
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
