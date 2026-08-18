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

  const MAX = 32;
  const tables = new Map(); // frame url -> Promise<Arrow.Table>
  const rowSets = new Map(); // frame url -> Promise<Array<Object>>
  const specs = new Map(); // spec url -> Promise<Object>

  function approot() {
    return new URL(document.body.dataset.approot || '/app/', document.location.origin);
  }

  function frameUrl(ref) {
    const url = new URL(ref, approot());
    const page = new URLSearchParams(document.location.search);
    for (const [k, v] of page) {
      url.searchParams.set(k, v);
    }
    return url.toString();
  }

  function evict(map) {
    while (map.size > MAX) map.delete(map.keys().next().value);
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
    return Arrow.tableFromIPC(new Uint8Array(await res.arrayBuffer()));
  }

  function table(ref) {
    const url = frameUrl(ref);
    if (!tables.has(url)) {
      tables.set(url, fetchTable(url));
      evict(tables);
    }
    return tables.get(url);
  }

  // Rows for vega: plain objects, temporal columns as Date, bigints
  // as numbers. Materialized once per frame and shared.
  function rows(ref) {
    const url = frameUrl(ref);
    if (!rowSets.has(url)) {
      rowSets.set(url, table(ref).then(toRows));
      evict(rowSets);
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
      evict(specs);
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

  // A write invalidates the store. The server announces every write
  // with `HX-Trigger: glossql:written` (the ruling's 204, and the
  // stale-tab 409 whose cause is someone else's write); htmx dispatches
  // the event on the posting form and it bubbles. Checked by URL alone,
  // the store kept serving the view from before the write until a hard
  // reload (found 2026-08-18, the open panel after a ruling; the
  // server's channel cache had the same defect the same week). Capture
  // phase, so the caches are empty before any component's own listener
  // refetches. Specs stay: they are static files.
  document.addEventListener(
    'glossql:written',
    () => {
      tables.clear();
      rowSets.clear();
    },
    { capture: true }
  );

  // A refused write states its reason beside the form that posted it.
  // The note lives inside a panel a later refetch replaces, which is
  // the right lifetime: the refreshed truth supersedes the complaint.
  document.addEventListener('htmx:responseError', (e) => {
    const elt = e.detail && e.detail.elt;
    const xhr = e.detail && e.detail.xhr;
    if (!elt || !xhr) return;
    const prior = elt.parentNode && elt.parentNode.querySelector('.frame-error');
    if (prior) prior.remove();
    elt.insertAdjacentElement(
      'afterend',
      errorBox(xhr.responseText || xhr.status + ' ' + xhr.statusText)
    );
  });

  window.glStore = { table, rows, json, frameUrl, converter, errorBox };
})();
