// <gl-chart frame="frames/x" spec="specs/x.vl.json"> — a vega-lite
// view over a stored frame. The spec binds the named data source
// "frame"; the store supplies the rows.
(function () {
  'use strict';

  // The instrument look, matched to app.css tokens: transparent
  // ground, hairline grid, mono axes.
  const CONFIG = {
    background: 'transparent',
    view: { stroke: null },
    axis: {
      domainColor: '#2a333f',
      gridColor: '#222a35',
      tickColor: '#2a333f',
      labelColor: '#8b95a2',
      titleColor: '#8b95a2',
      labelFont: 'ui-monospace, SF Mono, Menlo, monospace',
      titleFont: 'ui-monospace, SF Mono, Menlo, monospace',
      labelFontSize: 10,
      titleFontSize: 10,
      titleFontWeight: 500,
    },
    legend: {
      labelColor: '#8b95a2',
      titleColor: '#8b95a2',
      labelFontSize: 10,
    },
  };

  class GlChart extends HTMLElement {
    async connectedCallback() {
      this._mount = document.createElement('div');
      this._mount.className = 'chart-mount';
      this.replaceChildren(this._mount);
      this.setAttribute('aria-busy', 'true');
      try {
        const [spec, rows] = await Promise.all([
          glStore.json(this.getAttribute('spec')),
          glStore.rows(this.getAttribute('frame')),
        ]);
        this._spec = spec;
        this._rows = rows;
        await this.render();
        // A `windowed` chart re-derives from rows already in hand when
        // the viewer's window changes; day and week read the `daily`
        // frame, fetched only when first asked for.
        if (this.hasAttribute('windowed')) {
          this._onWindow = () => this.render();
          document.addEventListener('glossql:window', this._onWindow);
        }
      } catch (e) {
        this.replaceChildren(glStore.errorBox(e.message || String(e)));
      } finally {
        this.removeAttribute('aria-busy');
      }
    }

    async windowedRows() {
      if (!this.hasAttribute('windowed')) return this._rows;
      const w = glWindow.state();
      const daily = this.getAttribute('daily');
      if ((w.grain === 'day' || w.grain === 'week') && daily) {
        if (!this._daily) this._daily = await glStore.rows(daily);
        return glWindow.windowed(this._daily, w);
      }
      return glWindow.windowed(this._rows, w);
    }

    async render() {
      try {
        const rows = await this.windowedRows();
        // A fresh embed per window: the ordinal scale re-derives its
        // domain, which an in-place data swap would keep stale.
        const spec = JSON.parse(JSON.stringify(this._spec));
        spec.data = { name: 'frame', values: rows };
        if (spec.width === undefined) spec.width = 'container';
        if (this._view) {
          this._view.finalize();
          this._view = null;
        }
        const result = await vegaEmbed(this._mount, spec, { actions: false, config: CONFIG });
        if (!this.isConnected) {
          result.view.finalize();
          return;
        }
        this._view = result.view;
      } catch (e) {
        this.replaceChildren(glStore.errorBox(e.message || String(e)));
      }
    }

    disconnectedCallback() {
      if (this._onWindow) {
        document.removeEventListener('glossql:window', this._onWindow);
        this._onWindow = null;
      }
      if (this._view) {
        this._view.finalize();
        this._view = null;
      }
    }
  }

  customElements.define('gl-chart', GlChart);
})();
