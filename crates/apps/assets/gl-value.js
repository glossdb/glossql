// <gl-value frame="frames/kpis" field="billings" delta-field="billings_delta"
//           unit="days" format="compact|days|raw" good="up|down"> — one
// number from a stored frame's first row, with an optional delta
// beside it. `good` says which direction reads as healthy; the SQL
// computes the delta, this element only shows it.
(function () {
  'use strict';

  function fmt(value, format) {
    if (value == null) return '–';
    if (format === 'month') {
      const d = value instanceof Date ? value : new Date(value);
      return d.toISOString().slice(0, 7);
    }
    if (format === 'days') return Number(value).toFixed(1);
    if (format === 'raw') return Number(value).toLocaleString();
    return new Intl.NumberFormat(undefined, {
      notation: 'compact',
      maximumFractionDigits: 1,
    }).format(Number(value));
  }

  class GlValue extends HTMLElement {
    async connectedCallback() {
      this.setAttribute('aria-busy', 'true');
      try {
        const rows = await glStore.rows(this.getAttribute('frame'));
        if (!this.isConnected) return;
        const row = rows[0] || {};
        const format = this.getAttribute('format') || 'compact';
        const unit = this.getAttribute('unit');

        const num = document.createElement('span');
        num.className = 'kpi-num';
        num.textContent = fmt(row[this.getAttribute('field')], format);
        if (unit) {
          const u = document.createElement('span');
          u.className = 'kpi-unit';
          u.textContent = unit;
          num.appendChild(u);
        }
        const parts = [num];

        const deltaField = this.getAttribute('delta-field');
        if (deltaField && row[deltaField] != null) {
          const d = Number(row[deltaField]);
          const up = d >= 0;
          const healthyUp = (this.getAttribute('good') || 'up') === 'up';
          const delta = document.createElement('span');
          delta.className = 'kpi-delta ' + (up === healthyUp ? 'pos' : 'neg');
          delta.textContent = (up ? '▲ ' : '▼ ') + fmt(Math.abs(d), format);
          delta.title = 'vs prior month';
          parts.push(delta);
        }
        this.replaceChildren(...parts);
      } catch (e) {
        this.replaceChildren(glStore.errorBox(e.message || String(e)));
      } finally {
        this.removeAttribute('aria-busy');
      }
    }
  }

  customElements.define('gl-value', GlValue);
})();
