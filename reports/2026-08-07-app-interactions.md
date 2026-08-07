# The interaction layer: widget-local controls on the app door

Date: 2026-08-07. The iteration the project lead asked for before the
app-generation skill exists: each addition to the hand-written cash app
forces a convention decision, and the surviving conventions are what
the skill will teach.

## The conventions, as they survived

**Page-global filters navigate.** The toolbar's segs and the date form
are boosted links — full render, view transition. Unchanged.

**Widget-local filters swap their own tile.** A tile with an `id` can
carry a segmented control in its own header (`seg_param`/`seg_options`
on the chart/table macros). Its links write the URL (`hx-push-url`)
but target only their tile (`hx-target` + `hx-select` against the full
page render) — proven live: a witness marker planted on the DSO
chart's DOM survived a compare toggle on the billings tile. No
idiomorph needed; `hx-select` is the native mechanism, and the parked
addition stays parked.

**Params are page-scoped flat names.** The author writes both the page
and its frames, and keeps names unique (`compare`, `rows`, `month`,
`customer`). Control links preserve what the URL states and nothing
more — defaults reapply on their own. Values percent-encode through a
`urlencode` filter registered in the door (tera stays
`default-features = false`; customer names carry spaces).

**The frame carries the superset; the page picks the spec.**
`billings_monthly` always emits `billings` and `prior` (year-ago,
joined over the full scan so the window's first months find their
prior). Which view renders is the page's choice between two static
specs — `billings.vl.json` and `billings_yoy.vl.json` — selected by
state. Two plain specs beat one clever spec: no conditional logic in
a spec, no all-null layers, zero vega warnings.

**Drill chains, back-links drop one param.** Month bar → customers
chart (`drill-field` on gl-chart, string datum this time) → customer
history panel behind `{% if state.customer %}`. Each back-link is an
ordinary href carrying one param fewer. Ratios still recompose
server-side; flows re-aggregate freely (standing rule).

## Notes

- Tera scoping: `{% set %}` inside `{% if %}` reaches the enclosing
  scope — the spec-choice pattern relies on it and is proven by the
  door rendering both spec refs.
- Testdata: the finance generator's `customer_name` is
  transaction-grain (person names appearing in one or two months), so
  customer histories are sparse. The drill mechanics are what this
  proves; recurring-customer shapes await better testdata.
- Live checks (release, 8115): both compare states stream, the spaced
  customer name binds and streams, the drilled page renders all three
  panels, console clean.
