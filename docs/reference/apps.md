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
```

**A manifest names no dataset.** The URL does — `/<dataset>/app/<name>` —
so one app serves every dataset in the workspace and the header's picker
switches between them. A `dataset` key is accepted and ignored. App,
page, frame, and spec names are flat segments — ASCII alphanumerics,
`_`, `-`, `.`; nothing hidden, no dot-walking.

## Frames

A frame is one SQL file served as Arrow IPC. URL params bind as typed
plan placeholders (`$from` in the SQL, `?from=…` on the URL); values
arrive as Utf8 and the frame SQL casts explicitly, and a door argument
binds the same way (`metric_series(grain => $grain)`). The bound
dataset is not a parameter: the frame's channel is bound to the URL's
dataset, and `current_dataset` names it inside the SQL. Frames only
read, and the browser
fetches each frame once per state, sharing the table across every tile
bound to it. The URL is the only state; drill is navigation, and so is
a viewer's window: `?grain=quarter&span=24` are ordinary params bound
as `$grain` and `$span`, and the frame SQL is the filter. A reference's
own params are the author's defaults (`frames/trend?grain=month&span=24`);
the page URL overrides them, and only the frames whose URL changed
refetch.

Display logic — glyphs, classes, links — is computed in the frame's
SQL, not in the template. The shipped reads (`open_questions`, `owed`,
`metric_surfaces`, `metric_series()`, …) are the natural frame
sources; narrow a workspace-wide one by joining `current_dataset`.

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
row's value, formatted like a table cell. With `join="frames/y"
on="key"` a second frame's row with the same key merges into each row
(its fields win) — how a record frame and a data frame meet in one
list without becoming one fetch. Substitution is literal and
the template stays dumb — a frame that feeds an `href` emits a
URL-ready value, with one guard: a substituted `href`/`src` carrying a
script-capable scheme becomes `#`. An empty frame states itself
through `empty`; a capped one gets an honest footer.

The other tiles:

- `<gl-chart frame="frames/x" spec="specs/x.vl.json" empty="…">` — a
  vega-lite view over the frame; the spec binds the named data source
  `frame` and the store supplies the rows; width defaults to the
  container. An empty frame states itself through `empty`, as
  `gl-rows` does, instead of drawing a blank view.
- `<gl-table frame="frames/x" rows="50">` — the frame as a plain HTML
  table, first N rows (default 50), total row count in the footer.
- `<gl-value frame="frames/kpis" field="billings"
  delta-field="billings_delta" unit="days" format="compact" good="up">`
  — one number from the frame's first row, optional delta beside it.
  `format` is `compact | days | raw | month | text` (`text` passes a
  band or verdict through unformatted); `good` says which direction
  reads as healthy — the SQL computes the delta, the element only
  shows it.
- `<gl-window frame="frames/axes">` — the viewer's grain and span as
  links over the page URL (`grain`, `span`); it reads the metric's
  resolution from a `metric_axes()` frame and offers the grains from
  there up.

After the docket's two writes the door answers with an event, never a
navigation: a ruling with `HX-Trigger: glossql:written`, on which the
frame store drops its record-class caches; re-measure (every
measurement standing from before the last change, re-run) with
`glossql:remeasured, glossql:written`, on which the data-class caches
go too — a re-measure can change the cube, a ruling cannot. Every
connected tile refetches in place; instruments keep their DOM.

## The built-in docket

`crates/apps/builtin/docket/` — the reference app and the standing
example: what stands open for a human to judge, what has been settled,
what waits on an act, the metric surfaces and the record behind them.
Pages `index.html`, `metrics.html`, `record.html`; seventeen frames
over the shipped reads and the cube's two reads; one spec. Every built-in
frame parses under the test suite.
