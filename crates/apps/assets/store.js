// The frame store: every frame is fetched once per state as Arrow IPC
// and shared by every tile bound to it — one copy in memory, charts
// and tables read the same arrow Table. A frame reference resolves
// against the app root (body[data-approot]); params written on the
// reference are the author's defaults, and the page's query params
// override them. The URL is the only state: htmx swaps the page,
// reconnecting components ask the store, and only frames whose URL
// actually changed refetch.
(function () {
  'use strict';

  const tables = new Map(); // frame url -> Promise<Arrow.Table>
  const rowSets = new Map(); // frame url -> Promise<Array<Object>>
  const specs = new Map(); // spec url -> Promise<Object>
  // frame url -> 'record' | 'data', from the server's derived
  // glossql-frame-class header: `record` frames read the glossary
  // somewhere in their expansion and can change under a ruling; `data`
  // frames provably cannot. Metadata and data are not one pile.
  const classes = new Map();

  // The app root follows the pushed pathname: hx-boost swaps the page
  // but never re-swaps body[data-approot], so cross-app navigation
  // resolved frames against the app the tab was opened on. The
  // attribute stays as the fallback for pages outside /app/<name>.
  function approot() {
    const m = document.location.pathname.match(/^(\/app\/[^/]+)(\/|$)/);
    const root = m ? m[1] + '/' : document.body.dataset.approot || '/app/';
    return new URL(root, document.location.origin);
  }

  function frameUrl(ref) {
    const url = new URL(ref, approot());
    const page = new URLSearchParams(document.location.search);
    for (const [k, v] of page) url.searchParams.set(k, v);
    return url.toString();
  }

  async function fetchTable(url) {
    const res = await fetch(url);
    if (!res.ok) {
      let message = res.status + ' ' + res.statusText;
      try {
        message = (await res.json()).error || message;
      } catch (_) { /* not json — keep the status line */ }
      throw new Error(message);
    }
    // Absent header reads as record — evict-on-write is the safe side.
    classes.set(url, res.headers.get('glossql-frame-class') || 'record');
    return Arrow.tableFromIPC(new Uint8Array(await res.arrayBuffer()));
  }

  function table(ref) {
    const url = frameUrl(ref);
    if (!tables.has(url)) {
      tables.set(url, fetchTable(url));
    }
    return tables.get(url);
  }

  // Rows for vega: plain objects, temporal columns as Date, bigints
  // as numbers. Materialized once per frame and shared.
  function rows(ref) {
    const url = frameUrl(ref);
    if (!rowSets.has(url)) {
      rowSets.set(url, table(ref).then(toRows));
    }
    return rowSets.get(url);
  }

  function converter(type) {
    if (Arrow.DataType.isDate(type) || Arrow.DataType.isTimestamp(type)) {
      return (v) => (v == null ? null : new Date(Number(v)));
    }
    return (v) => (typeof v === 'bigint' ? Number(v) : v);
  }

  function toRows(t) {
    const names = t.schema.fields.map((f) => f.name);
    const conv = t.schema.fields.map((f) => converter(f.type));
    const out = new Array(t.numRows);
    for (let i = 0; i < t.numRows; i++) {
      const row = t.get(i);
      const o = {};
      for (let c = 0; c < names.length; c++) o[names[c]] = conv[c](row[names[c]]);
      out[i] = o;
    }
    return out;
  }

  function json(ref) {
    const url = new URL(ref, approot()).toString();
    if (!specs.has(url)) {
      specs.set(
        url,
        fetch(url).then((res) => {
          if (!res.ok) throw new Error(res.status + ' ' + res.statusText + ' — ' + url);
          return res.json();
        })
      );
    }
    // A fresh object per caller — charts mutate their spec.
    return specs.get(url).then((spec) => JSON.parse(JSON.stringify(spec)));
  }

  function errorBox(message) {
    const el = document.createElement('div');
    el.className = 'frame-error';
    el.textContent = message;
    return el;
  }

  // A write evicts what it can change, nothing else. The server
  // announces every write with `HX-Trigger: glossql:written` (the
  // ruling's 204, and the stale-tab 409 whose cause is someone else's
  // write), and it classifies every frame it serves: `record` frames
  // read the glossary somewhere in their expansion, `data` frames
  // provably do not — so the cube's cells, the trend and the slices
  // survive a ruling untouched, in cache and on screen. Capture phase, so the
  // evictions land before any panel's own listener refetches. Specs
  // stay: they are static files.
  document.addEventListener(
    'glossql:written',
    () => {
      for (const url of new Set([...tables.keys(), ...rowSets.keys()])) {
        if ((classes.get(url) || 'record') === 'record') {
          tables.delete(url);
          rowSets.delete(url);
          classes.delete(url);
        }
      }
    },
    { capture: true }
  );

  // A refused write states its reason inside the form that posted it —
  // appended as a child, so it lays out as one of the form's own rows
  // (inserted beside the form it became a grid item of the row's
  // template columns and rendered one character wide — found live,
  // 2026-08-18). The note lives inside a panel a later refetch
  // replaces, which is the right lifetime: the refreshed truth
  // supersedes the complaint.
  document.addEventListener('htmx:responseError', (e) => {
    const elt = e.detail && e.detail.elt;
    const xhr = e.detail && e.detail.xhr;
    if (!elt || !xhr) return;
    const prior = elt.querySelector('.frame-error');
    if (prior) prior.remove();
    elt.appendChild(errorBox(xhr.responseText || xhr.status + ' ' + xhr.statusText));
  });

  window.glStore = { table, rows, json, frameUrl, converter, errorBox };
})();
