# Authoring an app

An app is a named set of declarative parts — pages, frame queries,
chart specs, a manifest. Authors write templates, SQL, specs, prose;
never code. The door serves as many apps as the workspace holds.

## Three sources, one resolution order

1. **A workspace directory** — `apps/<name>/` with `app.toml`, pages
   (`*.html`, tera), `frames/*.sql`, `specs/*.vl.json`. Read fresh per
   request: save a file, reload. A directory without `app.toml` is an
   authored app that cannot serve — the door refuses it rather than
   half-shadowing a built-in.
2. **Glosses** — each part its own gloss, which is what lets an agent
   over MCP author an app at all (it has statements, no filesystem):
   `app` on `<name>` is the manifest (`title`, optional `dataset`),
   `app_page` on `<name>.<page>` a page, `app_frame` on
   `<name>.<frame>` a query, `app_spec` on `<name>.<spec>` a chart
   spec. Supersession versions each part on its own; a human's part
   wins over the agent's. The `app_parts` read shows every part as a
   file row.
3. **The built-in** — the docket ships in the binary and resolves the
   same way.

The workspace shadows the built-in **whole** — forking is copying the
directory out. A glossed part under a built-in's name is refused: one
part would resolve the whole app and the built-in's other pages would
stop serving. Add an app under its own name instead.

## The manifest

```toml
title = "Docket"
# no dataset line: binds to the workspace's sole dataset at request time
```

`dataset` pins the frames to one dataset; absent, the app binds to the
workspace's sole dataset per request. App, page, frame, and spec names
are flat segments — ASCII alphanumerics, `_`, `-`, `.`; nothing
hidden, no dot-walking.

## Frames

A frame is one SQL file served as Arrow IPC. URL params bind as typed
plan placeholders (`$from` in the SQL, `?from=…` on the URL); values
arrive as Utf8 and the frame SQL casts explicitly. `$dataset` is
reserved — always the bound dataset. Frames only read, and the browser
fetches each frame once per state, sharing the table across every tile
bound to it. The URL is the only state; drill is navigation.

Display logic — glyphs, classes, links — is computed in the frame's
SQL, not in the template. The shipped reads (`open_questions`, `owed`,
`metric_surfaces`, `metric_series()`, …) are the natural frame
sources; scope workspace-wide reads to `$dataset`.

## Pages and tiles

Pages are tera templates; pages of one app can include each other. The
door ships the assets (all vendored, the only JS): htmx, vega-lite
(vega, vega-embed), arrow-js, and the `gl-*` tiles — `gl-chart` (a
vega-lite spec over a frame), `gl-table`, `gl-value`, and `gl-rows`:

```html
<gl-rows frame="frames/open" empty="nothing stands open">
  <template>…{subject} — {assumption}…</template>
</gl-rows>
```

`gl-rows` renders the stored frame through the author's own
`<template>`: every `{field}` in text or attribute values takes the
row's value, formatted like a table cell. Substitution is literal and
the template stays dumb — a frame that feeds an `href` emits a
URL-ready value, with one guard: a substituted `href`/`src` carrying a
script-capable scheme becomes `#`. An empty frame states itself
through `empty`; a capped one gets an honest footer.

The other three tiles:

- `<gl-chart frame="frames/x" spec="specs/x.vl.json">` — a vega-lite
  view over the frame; the spec binds the named data source `frame`
  and the store supplies the rows; width defaults to the container.
  With `drill-field`, clicking a mark navigates: the clicked datum's
  field lands in the URL under `drill-param` (default: the field
  name), and `drill-type="date"` turns a temporal datum into the ISO
  day — drill is navigation, the server renders the narrowed state.
- `<gl-table frame="frames/x" rows="50">` — the frame as a plain HTML
  table, first N rows (default 50), total row count in the footer.
- `<gl-value frame="frames/kpis" field="billings"
  delta-field="billings_delta" unit="days" format="compact" good="up">`
  — one number from the frame's first row, optional delta beside it.
  `format` is `compact | days | raw | month | text` (`text` passes a
  band or verdict through unformatted); `good` says which direction
  reads as healthy — the SQL computes the delta, the element only
  shows it.

After the docket's ruling write, the door answers
`HX-Trigger: glossql:written`; the frame store drops its caches and
every connected tile refetches in place — instruments keep their DOM.

## The built-in docket

`crates/apps/builtin/docket/` — the reference app and the standing
example: what stands open for a human to judge, what has been settled,
what waits on an act, the metric surfaces and the record behind them.
Pages `index.html`, `metrics.html`, `record.html`; fifteen frames over
the shipped reads and `metric_series()`; one spec. Every built-in
frame parses under the test suite.
