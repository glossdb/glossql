# Vendored browser libraries

Embedded into the server binary and served at `/app/assets/vendor/` —
the app door works offline, no CDN. UMD builds; the globals are
`htmx`, `vega`, `vegaLite`, `vegaEmbed`, `Arrow`, `dagre`. htmx is transport;
the app door carries no local UI state (alpine left with the pin
surface, 2026-08-13). dagre is layout only: it places the lineage
graph's tables, and `gl-graph` draws them.

| file | package | version | license |
| :--- | :--- | :--- | :--- |
| htmx.min.js | htmx.org | 2.0.8 | 0BSD |
| vega.min.js | vega | 6.2.0 | BSD-3-Clause |
| vega-lite.min.js | vega-lite | 6.4.3 | BSD-3-Clause |
| vega-embed.min.js | vega-embed | 7.0.2 | BSD-3-Clause |
| arrow.min.js | apache-arrow (Arrow.es2015.min.js) | 21.0.0 | Apache-2.0 |
| dagre.min.js | @dagrejs/dagre (dist/dagre.min.js) | 3.1.1 | MIT |

To bump: download the same path from npm via a CDN mirror and update
this table. arrow-js only reads the IPC wire, which is stable across
arrow versions — it does not move in lockstep with the Rust arrow tree.
