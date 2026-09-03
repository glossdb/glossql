# DataFusion's SQL guide at the pin

These pages are the SQL user guide of Apache DataFusion, verbatim, at
the tag in `VERSION`. That is the version Cargo.lock resolves, the
engine behind every read on the door. The door serves them as
`doc://vendor/datafusion/sql/<page>.md`; the scalar functions are one
page per family under `sql/scalar/`. Apache License 2.0, headers kept.

What differs here, which the guide cannot say:

- The parser runs the postgres dialect. Syntax the guide shows from
  the generic dialect does not parse. `SELECT * EXCLUDE (…)` and
  `SELECT * EXCEPT (…)` are two.
- `information_schema` is off. DDL, DML and COPY are closed: tables
  come from recipes. Those pages are not vendored.
- `=>` names an argument only in the door's own table functions
  (`metric_series(grain => 'month')`). The engine's functions take
  positional arguments.

`skill://glossql/references/sql-here.md` keeps the rest: what fails at
this pin, and what lands wrong without failing.

Refresh at every pin move: `vendor/datafusion/refresh.sh <tag>`. The
serverd suite refuses a VERSION that is not the lock's.
