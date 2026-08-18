// <gl-rows frame="frames/x" rows="200" empty="…"> — the stored frame
// rendered through the author's own <template>: every {field} in text
// or attribute values takes the row's value, formatted like a table
// cell. Display logic — glyphs, classes, links — belongs to the
// frame's SQL; the template stays dumb and substitution is literal,
// so a frame that feeds an href emits a URL-ready value — with one
// guard: a substituted href/src carrying a script-capable scheme
// (javascript:, data:, vbscript:) becomes '#'. An empty
// frame states itself through `empty`; a capped one gets the same
// honest footer a table gets.
(function () {
  'use strict';

  const FIELD = /\{([A-Za-z0-9_]+)\}/g;
  // A substituted URL attribute must stay a navigation target: a raw
  // data column can carry a script-capable scheme into href/src.
  const URL_ATTR = /^(href|src|xlink:href)$/i;
  const BAD_SCHEME = /^(javascript|data|vbscript):/;

  function text(value) {
    if (value == null) return '';
    if (value instanceof Date) {
      const iso = value.toISOString();
      return iso.endsWith('T00:00:00.000Z') ? iso.slice(0, 10) : iso;
    }
    if (typeof value === 'number' && !Number.isInteger(value)) {
      return value.toLocaleString(undefined, { maximumFractionDigits: 2 });
    }
    if (typeof value === 'number' || typeof value === 'bigint') {
      return value.toLocaleString();
    }
    return String(value);
  }

  function fill(node, row) {
    const sub = (s) => s.replace(FIELD, (_, f) => (f in row ? text(row[f]) : '{' + f + '}'));
    const walker = document.createTreeWalker(node, NodeFilter.SHOW_TEXT | NodeFilter.SHOW_ELEMENT);
    for (let n = walker.currentNode; n; n = walker.nextNode()) {
      if (n.nodeType === Node.TEXT_NODE) {
        if (n.nodeValue.includes('{')) n.nodeValue = sub(n.nodeValue);
      } else if (n.attributes) {
        for (const a of n.attributes) {
          if (!a.value.includes('{')) continue;
          const v = sub(a.value);
          a.value =
            URL_ATTR.test(a.name) && BAD_SCHEME.test(v.trim().toLowerCase()) ? '#' : v;
        }
      }
    }
  }

  class GlRows extends HTMLElement {
    async connectedCallback() {
      // Upgrade during the initial parse runs before the children
      // exist — wait for the document when the template isn't there
      // yet. htmx swaps insert fully-parsed trees and skip this.
      if (!this.querySelector('template') && document.readyState === 'loading') {
        await new Promise((r) =>
          document.addEventListener('DOMContentLoaded', r, { once: true })
        );
        if (!this.isConnected) return;
      }
      const template = this.querySelector('template');
      if (!template) {
        this.replaceChildren(glStore.errorBox('gl-rows needs a <template> child'));
        return;
      }
      this.setAttribute('aria-busy', 'true');
      try {
        const rows = await glStore.rows(this.getAttribute('frame'));
        if (!this.isConnected) return;
        const cap = Number(this.getAttribute('rows')) || 200;
        const out = [];
        if (rows.length === 0) {
          // `empty=""` states nothing on purpose — a banner frame that
          // serves rows only while its condition holds stays silent
          // otherwise. Only an absent or worded attribute gets a note.
          const stated = this.getAttribute('empty');
          if (stated !== '') {
            const note = document.createElement('p');
            note.className = 'rows-empty';
            note.textContent = stated || 'nothing here';
            out.push(note);
          }
        }
        const shown = Math.min(rows.length, cap);
        for (let i = 0; i < shown; i++) {
          const piece = template.content.cloneNode(true);
          fill(piece, rows[i]);
          out.push(piece);
        }
        if (rows.length > shown) {
          const foot = document.createElement('p');
          foot.className = 'table-foot';
          foot.textContent = rows.length.toLocaleString() + ' rows, showing first ' + shown;
          out.push(foot);
        }
        this.replaceChildren(template, ...out);
        // Rows arrive after htmx's initial pass; hand it the inserted
        // tree so boosted row links keep the shell's navigation.
        if (window.htmx) window.htmx.process(this);
      } catch (e) {
        this.replaceChildren(template, glStore.errorBox(e.message || String(e)));
      } finally {
        this.removeAttribute('aria-busy');
      }
    }
  }

  customElements.define('gl-rows', GlRows);
})();
