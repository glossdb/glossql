// <gl-drivers frame="frames/drivers"> — why the metric moved: at the
// viewer's window, every admitted dimension's members ranked by their
// own move between the last two periods. Computed here from member
// cells already in the frame store — no server work, no refetch on a
// window change. Each row is the member's own move at the metric's
// verb; a ratio member's move is its own ratio shifting, never a
// share of the total (mix effects are a reading, not a cell).
(function () {
  'use strict';

  const fmt = (v) =>
    Math.abs(v) >= 1000
      ? v.toLocaleString('en-US', { maximumFractionDigits: 0 })
      : v.toLocaleString('en-US', { maximumFractionDigits: 2 });

  // Member and dimension names are data — never markup.
  const esc = (s) =>
    String(s).replace(/[&<>"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' })[c]);

  class GlDrivers extends HTMLElement {
    async connectedCallback() {
      this.setAttribute('aria-busy', 'true');
      try {
        this._rows = await glStore.rows(this.getAttribute('frame'));
        this.render();
        this._onWindow = () => this.render();
        document.addEventListener('glossql:window', this._onWindow);
      } catch (e) {
        this.replaceChildren(glStore.errorBox(e.message || String(e)));
      } finally {
        this.removeAttribute('aria-busy');
      }
    }

    disconnectedCallback() {
      if (this._onWindow) {
        document.removeEventListener('glossql:window', this._onWindow);
        this._onWindow = null;
      }
    }

    render() {
      const w = glWindow.state();
      // Members re-window per dimension: series identity is the
      // (dimension, member) pair, so one member's periods aggregate
      // among themselves.
      const grain = w.grain === 'day' || w.grain === 'week' ? 'month' : w.grain;
      const byDim = new Map();
      for (const r of this._rows) {
        if (!byDim.has(r.dimension)) byDim.set(r.dimension, []);
        byDim.get(r.dimension).push({
          period: r.period,
          series: r.member,
          value: r.value,
          num: r.num,
          den: r.den,
          behavior: r.behavior,
        });
      }
      const moves = [];
      for (const [dimension, rows] of byDim) {
        const agg = glWindow.aggregate(glWindow.clip(rows, w.span), grain);
        const periods = [...new Set(agg.map((r) => r.period))].sort();
        if (periods.length < 2) continue;
        const [prev, last] = periods.slice(-2);
        const at = new Map();
        for (const r of agg) at.set(r.series + ' ' + r.period, r.value);
        const members = new Set(agg.map((r) => r.series));
        for (const m of members) {
          const a = at.get(m + ' ' + prev);
          const b = at.get(m + ' ' + last);
          if (a == null || b == null) continue;
          moves.push({ dimension, member: m, from: a, to: b, delta: b - a, prev, last });
        }
      }
      moves.sort((x, y) => Math.abs(y.delta) - Math.abs(x.delta));
      const top = moves.slice(0, 8);
      if (!top.length) {
        this.innerHTML = '<p class="hint">no member moves in view — widen the window</p>';
        return;
      }
      this.innerHTML =
        `<p class="hint">${top[0].prev} → ${top[0].last} · each member's own move, largest first</p>` +
        top
          .map(
            (m) =>
              `<div class="rrow" style="grid-template-columns:8rem 1fr 7rem 8rem">` +
              `<span class="r-subj asp">${esc(m.dimension)}</span>` +
              `<span class="r-what">${esc(m.member)}</span>` +
              `<span class="r-num">${m.delta >= 0 ? '+' : ''}${fmt(m.delta)}</span>` +
              `<span class="r-meta">${fmt(m.from)} → ${fmt(m.to)}</span>` +
              `</div>`
          )
          .join('');
    }
  }

  customElements.define('gl-drivers', GlDrivers);
})();
