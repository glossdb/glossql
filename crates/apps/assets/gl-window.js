// <gl-window frame="frames/axes"> — the viewer's window over a
// metric: grain and span ride the URL as page params (`grain`,
// `span`), ordinary frame parameters the frames bind as $grain and
// $span. Changing one is navigation — a boosted link, like the dim
// picker — so the server re-renders the state and only the frames
// whose URL changed refetch. Nothing re-aggregates here: the cube
// serves the grain by the metric's verb on the server, and the frame
// SQL clips the span.
//
// The grains offered start at the metric's resolution, read from the
// cube's fact row (the `frame` attribute, `metric_axes()`): a finer
// grain would serve nothing. Without the row every grain is offered.
// A URL naming no grain opens at the default — and when the metric's
// resolution is coarser than that, the control replaces the URL with
// the resolution before anything is read at a grain that serves
// nothing.
(function () {
  'use strict';

  const GRAINS = ['minute', 'hour', 'day', 'week', 'month', 'quarter', 'year'];
  const SPANS = ['12', '24', 'all'];
  const DEFAULT = { grain: 'month', span: '24' };

  function state() {
    const q = new URLSearchParams(document.location.search);
    const grain = q.get('grain');
    const span = q.get('span');
    return {
      grain: GRAINS.includes(grain) ? grain : DEFAULT.grain,
      span: SPANS.includes(span) ? span : DEFAULT.span,
    };
  }

  // The page's URL with one window param changed — the rest of the
  // state (metric, dim) rides along untouched.
  function href(partial) {
    const url = new URL(document.location.href);
    const next = Object.assign(state(), partial);
    url.searchParams.set('grain', next.grain);
    url.searchParams.set('span', next.span);
    return url.pathname + url.search;
  }

  class GlWindow extends HTMLElement {
    async connectedCallback() {
      let from = GRAINS[0];
      const frame = this.getAttribute('frame');
      if (frame) {
        try {
          const rows = await glStore.rows(frame);
          if (rows.length && GRAINS.includes(rows[0].resolution)) from = rows[0].resolution;
        } catch (_) {
          // The control still renders; the chart bound to the same
          // cube states what failed.
        }
      }
      if (!this.isConnected) return;
      if (
        !new URLSearchParams(document.location.search).has('grain') &&
        GRAINS.indexOf(from) > GRAINS.indexOf(DEFAULT.grain)
      ) {
        document.location.replace(href({ grain: from }));
        return;
      }
      this.render(from);
    }

    render(from) {
      const w = state();
      const grains = GRAINS.slice(GRAINS.indexOf(from));
      const link = (key, value, on) =>
        `<a class="wbtn${on ? ' on' : ''}" href="${href({ [key]: value })}">${value}</a>`;
      this.innerHTML =
        '<div class="window-bar">' +
        '<span class="wgroup">' +
        grains.map((g) => link('grain', g, g === w.grain)).join('') +
        '</span><span class="wgroup">' +
        SPANS.map((s) => link('span', s, s === w.span)).join('') +
        '</span></div>';
      // Links inserted after htmx's initial pass: hand it the tree so
      // the window keeps the shell's boosted navigation.
      if (window.htmx) window.htmx.process(this);
    }
  }

  customElements.define('gl-window', GlWindow);
})();
