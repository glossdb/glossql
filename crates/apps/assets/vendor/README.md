# Vendored browser libraries

Embedded into the server binary and served at `/assets/vendor/` —
the app door works offline, no CDN. UMD builds; the globals are
`htmx`, `vega`, `vegaLite`, `vegaEmbed`, `Arrow`, `dagre`. htmx is
transport; the app door carries no local UI state. dagre is layout
only: it places the lineage graph's tables, and `gl-graph` draws them.
vega-interpreter runs a spec's expressions without eval, which the
door's content security policy allows none of; it ships as an ES
module alone, so the vendored copy's one import of `vega-util` is
rewritten to `../vega-util.js`, the shim that re-exports those names
from the `vega` global, and `gl-chart` imports the module at the
first chart.

| file | package | version | license |
| :--- | :--- | :--- | :--- |
| htmx.min.js | htmx.org | 2.0.11 | 0BSD |
| vega.min.js | vega | 6.4.0 | BSD-3-Clause |
| vega-lite.min.js | vega-lite | 6.5.0 | BSD-3-Clause |
| vega-embed.min.js | vega-embed | 7.3.0 | BSD-3-Clause |
| vega-interpreter.js | vega-interpreter (build/vega-interpreter.js, import rewritten) | 2.3.2 | BSD-3-Clause |
| arrow.min.js | apache-arrow (Arrow.es2015.min.js) | 21.2.0 | Apache-2.0 |
| dagre.min.js | @dagrejs/dagre (dist/dagre.min.js) | 3.1.1 | MIT |

To bump: download the same path from npm via a CDN mirror and update
this table and `package.json`, which Dependabot watches — a release on
any line opens a pull request naming it, and the copy is this step. arrow-js only reads the IPC wire, which is stable across
arrow versions — it does not move in lockstep with the Rust arrow tree.
