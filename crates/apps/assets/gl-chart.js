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
      const mount = document.createElement('div');
      mount.className = 'chart-mount';
      this.replaceChildren(mount);
      this.setAttribute('aria-busy', 'true');
      try {
        const [spec, rows] = await Promise.all([
          glStore.json(this.getAttribute('spec')),
          glStore.rows(this.getAttribute('frame')),
        ]);
        // The rows ride in at construction so vega scales once — the
        // name stays bound, so a later view.data('frame', …) still
        // updates in place.
        spec.data = { name: 'frame', values: rows };
        if (spec.width === undefined) spec.width = 'container';
        const result = await vegaEmbed(mount, spec, { actions: false, config: CONFIG });
        if (!this.isConnected) {
          result.view.finalize();
          return;
        }
        this._view = result.view;
      } catch (e) {
        this.replaceChildren(glStore.errorBox(e.message || String(e)));
      } finally {
        this.removeAttribute('aria-busy');
      }
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
