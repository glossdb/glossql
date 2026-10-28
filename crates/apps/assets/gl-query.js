// <gl-query dataset="fin" rows="50"> — a read of the viewer's own
// against the query door: a text area, a run button, the rows as a
// table, and the result to save as csv or as the Arrow stream the
// door sent. The pane runs reads alone — SELECT, WITH, DESCRIBE,
// SHOW, EXPLAIN — and says so for anything else; a write is a
// statement for the agent door. The door's own refusal, a scan it
// cannot plan, an expression that fails, comes back as its text.
// Nothing here touches the store: a read of one's own is never a
// frame, so a ruling's evictions leave it alone. The read rides the
// page URL as `sql`: a page opened with one runs it on load, so a
// read is a link — the Query tab's relation list is such links.
(function () {
  'use strict';

  const READ = /^\s*(select|with|describe|show|explain)\b/i;

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

  function csvField(value) {
    if (value == null) return '';
    const s = value instanceof Date ? cell(value) : String(value);
    return /[",\n]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s;
  }

  function csvOf(t) {
    const names = t.schema.fields.map((f) => f.name);
    const conv = t.schema.fields.map((f) => glStore.converter(f.type));
    const lines = [names.map(csvField).join(',')];
    for (let i = 0; i < t.numRows; i++) {
      const row = t.get(i);
      lines.push(names.map((n, c) => csvField(conv[c](row[n]))).join(','));
    }
    return lines.join('\n') + '\n';
  }

  class GlQuery extends HTMLElement {
    connectedCallback() {
      const dataset = this.getAttribute('dataset');
      this._door = '/' + dataset + '/query';
      this._cap = Number(this.getAttribute('rows')) || 50;

      const area = document.createElement('textarea');
      area.rows = 5;
      area.spellcheck = false;
      area.setAttribute('aria-label', 'a read');
      area.placeholder = 'SELECT … FROM … LIMIT 20';
      const run = document.createElement('button');
      run.type = 'button';
      run.className = 'wbtn';
      run.textContent = 'run';
      const csv = document.createElement('a');
      csv.className = 'wbtn';
      csv.textContent = 'csv';
      csv.download = 'read.csv';
      csv.hidden = true;
      const arrow = document.createElement('a');
      arrow.className = 'wbtn';
      arrow.textContent = 'arrow';
      arrow.download = 'read.arrow';
      arrow.hidden = true;
      const bar = document.createElement('div');
      bar.className = 'wgroup';
      bar.append(run, csv, arrow);
      const out = document.createElement('div');
      this.replaceChildren(area, bar, out);

      const go = () => this.run(area.value, out, csv, arrow);
      run.addEventListener('click', go);
      area.addEventListener('keydown', (e) => {
        if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') go();
      });
      const given = new URLSearchParams(document.location.search).get('sql');
      if (given) {
        area.value = given;
        go();
      }
    }

    async run(sql, out, csv, arrow) {
      for (const a of [csv, arrow]) {
        if (a.href) URL.revokeObjectURL(a.href);
        a.removeAttribute('href');
        a.hidden = true;
      }
      if (!READ.test(sql)) {
        out.replaceChildren(
          glStore.errorBox('this pane runs one read — SELECT, WITH, DESCRIBE, SHOW or EXPLAIN; a statement that writes is the agent door’s')
        );
        return;
      }
      this.setAttribute('aria-busy', 'true');
      out.replaceChildren();
      try {
        const res = await fetch(this._door, {
          method: 'POST',
          headers: { 'Content-Type': 'text/plain' },
          body: sql,
        });
        const buf = await res.arrayBuffer();
        if (!res.ok) {
          let message = res.status + ' ' + res.statusText;
          try {
            message = JSON.parse(new TextDecoder().decode(buf)).error || message;
          } catch (_) { /* not json — keep the status line */ }
          throw new Error(message);
        }
        const kind = res.headers.get('content-type') || '';
        if (!kind.includes('arrow')) {
          // Several statements answer in the wire's JSON; one read streams.
          throw new Error('one read at a time: the door answered a sequence — ' + new TextDecoder().decode(buf).slice(0, 400));
        }
        const t = Arrow.tableFromIPC(new Uint8Array(buf));
        const names = t.schema.fields.map((f) => f.name);
        const conv = t.schema.fields.map((f) => glStore.converter(f.type));
        const el = document.createElement('table');
        const head = el.createTHead().insertRow();
        for (const name of names) {
          const th = document.createElement('th');
          th.textContent = name;
          head.appendChild(th);
        }
        const body = el.createTBody();
        const shown = Math.min(t.numRows, this._cap);
        for (let i = 0; i < shown; i++) {
          const row = t.get(i);
          const tr = body.insertRow();
          for (let c = 0; c < names.length; c++) {
            const td = tr.insertCell();
            const value = conv[c](row[names[c]]);
            td.textContent = cell(value);
            if (typeof value === 'number' || typeof value === 'bigint') td.className = 'num';
          }
        }
        const foot = document.createElement('p');
        foot.className = 'table-foot';
        foot.textContent =
          t.numRows.toLocaleString() + ' rows' + (t.numRows > shown ? ', showing first ' + shown : '');
        out.replaceChildren(el, foot);
        csv.href = URL.createObjectURL(new Blob([csvOf(t)], { type: 'text/csv' }));
        csv.hidden = false;
        arrow.href = URL.createObjectURL(new Blob([buf], { type: 'application/vnd.apache.arrow.stream' }));
        arrow.hidden = false;
      } catch (e) {
        out.replaceChildren(glStore.errorBox(e.message || String(e)));
      } finally {
        this.removeAttribute('aria-busy');
      }
    }
  }

  customElements.define('gl-query', GlQuery);
})();
