// <gl-graph nodes="frames/lineage_nodes" edges="frames/lineage_edges"
//           empty="…"> — column lineage over two stored frames. The
// nodes frame is one row per column: `node` (a table, or a metric as
// `read.<name>()`), `kind` (`table` | `metric`), `col`, `ord`, `role`.
// The edges frame is one row per edge: `src`, `dst` (column paths,
// `table.col` or `read.<name>().field`, a composite endpoint as
// `table.(a, b)`), `kind` (`m2o` | `o2o` | `reads`).
//
// dagre places the tables, reading left to right along the keys — a
// parent stands left of the table that points at it. The metrics are
// not ranked with them: they are the right-hand column, one stack
// ordered by where their sources sit, so the graph is as wide as its
// longest key chain and no wider. The element draws the rest as SVG
// in the page's own classes; nothing here is a chart.
//
// A column an edge touches, or one with a judged role, shows; the
// rest fold under a count and open on click — a disclosure, not
// state. Hover follows a column through its edges. Click puts it in
// the URL as `focus`, so the view is a link; the page renders it lit.
(function () {
  'use strict';

  const W = 190;
  const HEAD = 30;
  const ROW = 17;
  const FOLD = 17;
  const PAD = 6;
  const NS = 'http://www.w3.org/2000/svg';

  function el(tag, attrs, text) {
    const e = document.createElementNS(NS, tag);
    for (const k in attrs) e.setAttribute(k, attrs[k]);
    if (text != null) e.textContent = text;
    return e;
  }

  // `t.(a, b)` is one edge per member column; the tuple is the key,
  // the columns are where it lands.
  function endpoints(path) {
    const m = path.match(/^(.*?)\.\((.*)\)$/);
    if (!m) return [path];
    return m[2].split(',').map((c) => m[1] + '.' + c.trim());
  }

  function nodeOf(id) {
    return id.slice(0, id.lastIndexOf('.'));
  }

  class GlGraph extends HTMLElement {
    constructor() {
      super();
      this.open = new Set();
    }

    connectedCallback() {
      this._written = () => this.load();
      document.addEventListener('glossql:written', this._written);
      this.load();
    }

    disconnectedCallback() {
      document.removeEventListener('glossql:written', this._written);
    }

    async load() {
      const nodes = glStore.rows(this.getAttribute('nodes'));
      const edges = glStore.rows(this.getAttribute('edges'));
      if (nodes === this._nodes && edges === this._edges) return;
      this._nodes = nodes;
      this._edges = edges;
      this.setAttribute('aria-busy', 'true');
      try {
        const [nodeRows, edgeRows] = await Promise.all([nodes, edges]);
        if (!this.isConnected || nodes !== this._nodes) return;
        this.rows = { nodes: nodeRows, edges: edgeRows };
        this.draw();
      } catch (e) {
        this._nodes = null;
        this._edges = null;
        this.replaceChildren(glStore.errorBox(e.message || String(e)));
      } finally {
        this.removeAttribute('aria-busy');
      }
    }

    draw() {
      if (!window.dagre) {
        this.replaceChildren(glStore.errorBox('dagre did not load — the graph cannot lay out'));
        return;
      }
      const { nodes: nodeRows, edges: edgeRows } = this.rows;
      if (nodeRows.length === 0) {
        const stated = this.getAttribute('empty');
        const note = document.createElement('p');
        note.className = 'rows-empty';
        note.textContent = stated || 'nothing here';
        this.replaceChildren(note);
        return;
      }
      const focus = new URLSearchParams(document.location.search).get('focus');

      // The nodes, in the frame's order, each with its columns in
      // ordinal order.
      const byNode = new Map();
      for (const r of nodeRows) {
        if (!byNode.has(r.node)) byNode.set(r.node, { name: r.node, kind: r.kind, columns: [] });
        byNode.get(r.node).columns.push([r.col, r.role || '']);
      }
      const NODES = [...byNode.values()];

      const touched = new Set();
      const edges = [];
      for (const r of edgeRows) {
        const from = endpoints(r.src);
        const to = endpoints(r.dst);
        for (let i = 0; i < from.length; i++) {
          const e = { from: from[i], to: to[i] || to[0], kind: r.kind };
          edges.push(e);
          touched.add(e.from);
          touched.add(e.to);
        }
      }

      const shown = new Map();
      for (const n of NODES) {
        const keep = n.columns.filter(
          ([c, role]) => touched.has(n.name + '.' + c) || role || this.open.has(n.name)
        );
        shown.set(n.name, { cols: keep, hidden: n.columns.length - keep.length });
      }
      const height = (n) => {
        const s = shown.get(n.name);
        return HEAD + s.cols.length * ROW + (s.hidden ? FOLD : 0) + PAD;
      };

      const g = new dagre.graphlib.Graph();
      g.setGraph({ rankdir: 'LR', nodesep: 22, ranksep: 70, marginx: 8, marginy: 8 });
      g.setDefaultEdgeLabel(() => ({}));
      const tables = NODES.filter((n) => n.kind !== 'metric');
      const metrics = NODES.filter((n) => n.kind === 'metric');
      for (const n of tables) g.setNode(n.name, { width: W, height: height(n) });
      const seen = new Set();
      for (const e of edges) {
        if (e.kind === 'reads') continue;
        const child = nodeOf(e.from);
        const parent = nodeOf(e.to);
        const key = parent + '>' + child;
        if (child !== parent && g.hasNode(parent) && g.hasNode(child) && !seen.has(key)) {
          g.setEdge(parent, child);
          seen.add(key);
        }
      }
      dagre.layout(g);
      const pos = new Map();
      for (const n of tables) pos.set(n.name, g.node(n.name));

      // The metric column: past the widest rank, stacked in the order
      // of their sources' mean y so the dashed edges cross least.
      const tablesWidth = tables.length ? g.graph().width : 0;
      const colX = tablesWidth + (tables.length ? 70 : 8) + W / 2;
      const meanY = (m) => {
        const ys = edges
          .filter((e) => nodeOf(e.to) === m.name)
          .map((e) => pos.get(nodeOf(e.from)))
          .filter(Boolean)
          .map((p) => p.y);
        return ys.length ? ys.reduce((a, b) => a + b, 0) / ys.length : 0;
      };
      let stackY = 8;
      for (const m of metrics.slice().sort((a, b) => meanY(a) - meanY(b))) {
        const h = height(m);
        pos.set(m.name, { x: colX, y: stackY + h / 2, width: W, height: h });
        stackY += h + 22;
      }
      const totalW = (metrics.length ? colX + W / 2 : tablesWidth) + 8;
      const totalH = Math.max(tables.length ? g.graph().height : 0, stackY);

      const svg = el('svg', {
        width: totalW,
        height: totalH,
        viewBox: `0 0 ${totalW} ${totalH}`,
        role: 'img',
        'aria-label': 'column lineage',
      });
      // A graph a little wider than the band scales down to fit —
      // the whole picture beats a scrollbar as long as the type stays
      // readable; past that floor the band scrolls, as wide rows do.
      const FIT_FLOOR = 0.78;
      const room = this.clientWidth || this.parentElement?.clientWidth || 0;
      if (room && totalW > room && totalW * FIT_FLOOR <= room) {
        svg.setAttribute('width', room);
        svg.setAttribute('height', Math.round((totalH * room) / totalW));
      }
      const anchors = new Map();
      const layerE = el('g', {});
      const layerN = el('g', {});
      svg.append(layerE, layerN);

      for (const n of NODES) {
        const p = pos.get(n.name);
        const s = shown.get(n.name);
        const x = p.x - p.width / 2;
        const y = p.y - p.height / 2;
        const grp = el('g', { class: 'node', transform: `translate(${x},${y})` });
        grp.append(el('rect', { class: 'n-box ' + n.kind, width: p.width, height: p.height, rx: 2 }));
        grp.append(el('text', { class: 'n-head', x: 10, y: 19 }, n.name));
        grp.append(el('line', { class: 'n-rule', x1: 0, x2: p.width, y1: HEAD - 4, y2: HEAD - 4 }));
        s.cols.forEach(([c, role], i) => {
          const id = n.name + '.' + c;
          const cy = HEAD + i * ROW;
          const col = el('g', { class: 'col' + (touched.has(id) ? ' keyed' : ''), 'data-id': id });
          col.append(el('rect', { x: 1, y: cy - 1, width: p.width - 2, height: ROW }));
          col.append(el('text', { x: 10, y: cy + 11 }, c));
          if (role) {
            col.append(el('text', { class: 'role ' + role, x: p.width - 10, y: cy + 11, 'text-anchor': 'end' }, role));
          }
          grp.append(col);
          anchors.set(id, { x0: x, x1: x + p.width, y: y + cy + ROW / 2 });
        });
        if (s.hidden) {
          const fold = el('g', { class: 'fold', 'data-node': n.name });
          fold.append(el('text', { x: 10, y: HEAD + s.cols.length * ROW + 11 }, '+ ' + s.hidden + ' more'));
          grp.append(fold);
        }
        layerN.append(grp);
      }

      for (const e of edges) {
        const a = anchors.get(e.from);
        const b = anchors.get(e.to);
        if (!a || !b) continue;
        const ltr = a.x1 <= b.x0;
        const x0 = ltr ? a.x1 : a.x0;
        const x1 = ltr ? b.x0 : b.x1;
        const dx = Math.max(30, Math.abs(x1 - x0) / 2) * (ltr ? 1 : -1);
        const d = `M${x0},${a.y} C${x0 + dx},${a.y} ${x1 - dx},${b.y} ${x1},${b.y}`;
        layerE.append(el('path', { class: 'e ' + e.kind, d, 'data-from': e.from, 'data-to': e.to }));
        if (e.kind !== 'reads') {
          const tip = e.kind === 'o2o' ? '↔' : '→';
          layerE.append(
            el('text', {
              class: 'e-tip',
              x: (x0 + x1) / 2,
              y: (a.y + b.y) / 2 - 3,
              'text-anchor': 'middle',
              'data-from': e.from,
              'data-to': e.to,
            }, tip)
          );
        }
      }
      const line = document.createElement('p');
      line.className = 'focus-line';
      this.replaceChildren(svg, line);

      const lit = (id) => {
        const marks = svg.querySelectorAll('.e, .e-tip');
        if (!id) {
          for (const m of marks) m.classList.remove('lit', 'dim');
          for (const c of svg.querySelectorAll('.col.lit')) c.classList.remove('lit');
          line.textContent = '';
          return;
        }
        const near = new Set();
        for (const m of marks) {
          const hit = m.dataset.from === id || m.dataset.to === id;
          m.classList.toggle('lit', hit);
          m.classList.toggle('dim', !hit);
          if (hit) {
            near.add(m.dataset.from);
            near.add(m.dataset.to);
          }
        }
        for (const c of svg.querySelectorAll('.col')) c.classList.toggle('lit', near.has(c.dataset.id));
        const others = [...near].filter((t) => t !== id);
        line.replaceChildren();
        const b = document.createElement('b');
        b.textContent = id;
        line.append(b, ' — ' + (others.length ? others.join(' · ') : 'nothing attached'));
      };
      svg.addEventListener('mouseover', (ev) => {
        const c = ev.target.closest('.col');
        if (c) lit(c.dataset.id);
      });
      svg.addEventListener('mouseout', (ev) => {
        const c = ev.target.closest('.col');
        if (c) lit(focus);
      });
      svg.addEventListener('click', (ev) => {
        const f = ev.target.closest('.fold');
        if (f) {
          this.open.add(f.dataset.node);
          this.draw();
          return;
        }
        const c = ev.target.closest('.col');
        if (!c) return;
        // The focus rides the URL: the same view is a link, and the
        // frames keep their cache — nothing about them changed.
        const url = new URL(document.location.href);
        if (url.searchParams.get('focus') === c.dataset.id) {
          url.searchParams.delete('focus');
        } else {
          url.searchParams.set('focus', c.dataset.id);
        }
        document.location.assign(url.toString());
      });
      if (focus) lit(focus);
    }
  }

  customElements.define('gl-graph', GlGraph);
})();
