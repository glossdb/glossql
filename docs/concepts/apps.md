# Apps

A data app is its own page, served at `/<dataset>/app/<name>`, and
**its URL is the whole state**: the dataset is its first segment and
every filter a reader picks lands in the query string, so any view is
a link they can paste to somebody else. Not a dashboard to log into —
a page to send. An app names no dataset of its own, so one app serves
every dataset in the workspace and the picker in the header is a link
that rewrites the first segment.

## An app is glosses

A workspace authors an app as glosses — one per part, so a frame is
edited without rewriting the app, and supersession versions each part
on its own. The aspect says what kind of file the part is; the subject
says where it goes: `app` (the manifest), `app_page` (a tera page),
`app_frame` (a SQL frame), `app_spec` (a vega-lite spec).

```glossql
GLOSS app ON delivery AS $${"title": "Monday operations"}$$;
GLOSS app_frame ON delivery.monthly AS $${"sql":
  "SELECT date_trunc('month', date) AS period, sum(value) AS value FROM read.throughput() GROUP BY 1 ORDER BY 1"}$$;
```

The `app_parts` read serves what an app is made of, one row per file.

## Frames compute, tiles place, specs draw

**Frames are SQL, and they are where the thinking goes.** One SELECT
per frame, streamed as Arrow IPC; the browser fetches each frame once
per state and every tile bound to it shares that table. URL params
bind as plan placeholders (`$region` in the SQL, `?region=EMEA` on the
URL; everything arrives as text, so cast explicitly); `$dataset` is
always bound. Display logic — labels, percentages, a CSS class chosen
by a verdict — is computed in the frame as columns; templates place
values, they do not decide them. A frame plans through the same path
as every other read, so every door is available inside one:
`read.<metric>()` for a grounding, `metric_series()` for the cube's
cells at a grain, `whatif.<scenario>()` for a declared scenario beside
the real books.

**Tiles** are tera macros placing what the frame computed — a value, a
chart over a spec, a table, or a row surface with an authored
template. Each tile carries a **chip**: its provenance, which read the
number comes from. A tile without a chip is a number with no address.
A chart's drill navigates — the clicked datum's value lands in the
URL; drill is navigation, nothing else.

**Specs** are vega-lite JSON over the frame's columns, kept thin:
encodings and marks. A spec that filters or aggregates is doing the
frame's job in a place nobody can test.

## The docket

One app ships in the binary: the docket — what stands open for a human
to judge, what has been settled, what waits on an act, with the metric
surfaces and the record behind them. A workspace that wants a
different view authors its own app beside it; the door serves as many
as the workspace writes. A part glossed under a built-in's name is
refused — one part resolves the whole app, and the built-in's other
pages would stop serving. Add an app; don't fork the built-in.

## One write

The app door takes exactly one write in the whole system: the docket's
ruling form, answering a question the workspace already derived (see
[`questions.md`](questions.md)). Everything else on every app is a
read. Anything an app needs to change, change with a statement — the
page is a view of the record, never the way into it.
