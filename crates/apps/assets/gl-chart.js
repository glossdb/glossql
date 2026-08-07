// <gl-chart frame="frames/x" spec="specs/x.vl.json"> — a vega-lite
// view over a stored frame. The spec binds the named data source
// "frame"; the store supplies the rows. With drill-field set, clicking
// a mark navigates: the clicked datum's field lands in the URL as
// drill-param (default: the field name) — drill is navigation, the
// server renders the narrowed state, the store refetches only what
// changed.
(function () {
  'use strict';

  class GlChart extends HTMLElement {
    async connectedCallback() {
      const mount = document.createElement('div');
      mount.className = 'chart-mount';
      this.replaceChildren(mount);
      try {
        const [spec, rows] = await Promise.all([
          glStore.json(this.getAttribute('spec')),
          glStore.rows(this.getAttribute('frame')),
        ]);
        spec.data = { name: 'frame' };
        if (spec.width === undefined) spec.width = 'container';
        const result = await vegaEmbed(mount, spec, { actions: false });
        if (!this.isConnected) {
          result.view.finalize();
          return;
        }
        this._view = result.view;
        result.view.data('frame', rows);
        await result.view.runAsync();
        this.drill(result.view);
      } catch (e) {
        this.replaceChildren(glStore.errorBox(e.message || String(e)));
      }
    }

    drill(view) {
      const field = this.getAttribute('drill-field');
      if (!field) return;
      const param = this.getAttribute('drill-param') || field;
      view.addEventListener('click', (_event, item) => {
        if (!item || !item.datum || item.datum[field] === undefined) return;
        let value = item.datum[field];
        if (value instanceof Date) value = value.toISOString().slice(0, 10);
        const url = new URL(document.location);
        url.searchParams.set(param, value);
        document.location.assign(url);
      });
    }

    disconnectedCallback() {
      if (this._view) {
        this._view.finalize();
        this._view = null;
      }
    }
  }

  customElements.define('gl-chart', GlChart);
})();
