// <gl-value frame="frames/kpis" field="billings" format="compact|text">
// — one number from a stored frame's first row.
(function () {
  'use strict';

  function fmt(value, format) {
    if (value == null) return '\u2013';
    // A value that is already a word — a band, a verdict, a state. It
    // reached the number formatter before this branch existed and the
    // docket's corridor tile rendered NaN.
    if (format === 'text') return String(value);
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
        const num = document.createElement('span');
        num.className = 'kpi-num';
        num.textContent = fmt(
          row[this.getAttribute('field')],
          this.getAttribute('format') || 'compact'
        );
        this.replaceChildren(num);
      } catch (e) {
        this.replaceChildren(glStore.errorBox(e.message || String(e)));
      } finally {
        this.removeAttribute('aria-busy');
      }
    }
  }

  customElements.define('gl-value', GlValue);
})();
