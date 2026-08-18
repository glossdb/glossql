// <gl-table frame="frames/x" rows="50"> — the stored frame as a plain
// HTML table, first N rows with an honest footer. Reads the arrow
// Table directly; no second copy of the data.
(function () {
  'use strict';

  function cell(value) {
    if (value == null) return '';
    if (value instanceof Date) {
      const iso = value.toISOString();
      return iso.endsWith('T00:00:00.000Z') ? iso.slice(0, 10) : iso;
    }
    if (typeof value === 'bigint') return value.toLocaleString();
    if (typeof value === 'number') {
      return Number.isInteger(value)
        ? value.toLocaleString()
        : value.toLocaleString(undefined, { maximumFractionDigits: 2 });
    }
    return String(value);
  }

  class GlTable extends HTMLElement {
    connectedCallback() {
      // A write refreshes the table in place — the store's capture
      // listener has already dropped its caches.
      this._written = () => this.load();
      document.addEventListener('glossql:written', this._written);
      this.load();
    }

    disconnectedCallback() {
      document.removeEventListener('glossql:written', this._written);
    }

    async load() {
      this.setAttribute('aria-busy', 'true');
      try {
        const t = await glStore.table(this.getAttribute('frame'));
        if (!this.isConnected) return;
        const cap = Number(this.getAttribute('rows')) || 50;
        const conv = t.schema.fields.map((f) => glStore.converter(f.type));
        const names = t.schema.fields.map((f) => f.name);

        const el = document.createElement('table');
        const head = el.createTHead().insertRow();
        for (const name of names) {
          const th = document.createElement('th');
          th.textContent = name;
          head.appendChild(th);
        }
        const body = el.createTBody();
        const shown = Math.min(t.numRows, cap);
        for (let i = 0; i < shown; i++) {
          const row = t.get(i);
          const tr = body.insertRow();
          for (let c = 0; c < names.length; c++) {
            const td = tr.insertCell();
            const value = conv[c](row[names[c]]);
            td.textContent = cell(value);
            if (typeof value === 'number' || typeof value === 'bigint') {
              td.className = 'num';
            }
          }
        }
        const foot = document.createElement('p');
        foot.className = 'table-foot';
        foot.textContent =
          t.numRows.toLocaleString() +
          ' rows' +
          (t.numRows > shown ? ', showing first ' + shown : '');
        this.replaceChildren(el, foot);
      } catch (e) {
        this.replaceChildren(glStore.errorBox(e.message || String(e)));
      } finally {
        this.removeAttribute('aria-busy');
      }
    }
  }

  customElements.define('gl-table', GlTable);
})();
