# 18 · App authoring — the model app as statements

Source: our own artifact (2026-08-11), the built-in world-model app
(`crates/apps/builtin/model/`) — the first app that has to become
authorable through the door. Inventory: `app.toml` (106 bytes),
`index.html` (16,832 bytes of tera), nineteen frames (378–2,507 bytes
each), one vega-lite spec — 24 files, 749 lines, ~38 KB. The constraint
that opens the gap: an app is authored, iterated, and deployed *on the
server*, through the statement door — no filesystem access, no
deployment by a third party. The files are small and the door already
takes `;`-separated sequences, so the whole app is two or three calls.

The artifacts, quoted. `crates/apps/builtin/model/app.toml`:

```toml
title = "World model"
# no dataset line: a built-in binds to the workspace's sole dataset at request time
```

`crates/apps/builtin/model/frames/travels.sql`:

```sql
-- Where the column's meaning travels: metric surfaces whose recorded
-- SQL mentions its bare name. A textual mention, stated as such — the
-- honest reach until composition is a declared relation.
SELECT q.aspect AS metric, '?metric=' || q.aspect AS link
FROM GLOSSARY(all => true) q
WHERE q.kind = 'query'
  AND strpos(json_get_str(q.body, 'sql'),
             arrow_cast(substr($subject, strpos($subject, '.') + 1), 'Utf8')) > 0
ORDER BY q.aspect
```

## Fork A — one statement per artifact

The file grain is the edit grain: an author changes one frame per turn,
so one statement carries one artifact. `SET (…)` holds what `app.toml`
holds; `$$…$$` carries content opaque, as it already does for gloss
bodies and aspect schemas.

```glossql-gap
DECLARE APP model SET (title: 'World model');

DECLARE FRAME travels ON model AS $$
SELECT q.aspect AS metric, '?metric=' || q.aspect AS link
FROM GLOSSARY(all => true) q
WHERE q.kind = 'query'
  AND strpos(json_get_str(q.body, 'sql'),
             arrow_cast(substr($subject, strpos($subject, '.') + 1), 'Utf8')) > 0
ORDER BY q.aspect
$$;

DECLARE PAGE index ON model AS $$
{% extends "shell.html" %}
… 16,832 bytes of tera, carried opaque …
$$;

DECLARE SPEC bands ON model AS $$
{ "$schema": "https://vega.github.io/schema/vega-lite/v5.json", … }
$$;

PUBLISH APP model;
```

## Fork B — the app as one envelope

```glossql-gap
DECLARE APP model AS $$
title = "World model"

[pages.index]
… the whole directory in one body …

[frames.travels]
… every frame inline …
$$;
```

## Findings

- **The write count is bootstrap-sized.** The full app is ~24 DECLAREs —
  the same order as `bootstrap.glossql`'s declaration sequence, delivered
  in two or three door calls. Authoring by statements costs nothing the
  shipped system doesn't already pay at boot.
- **Fork B forfeits the grain.** One envelope means every edit rewrites
  the app: supersession, history, and validation all lose the
  per-artifact resolution that the edit loop actually uses. Recorded as
  the losing form unless the lead rules otherwise.
- **Publish is supersession wearing a state.** The door serves the
  published version, which no further DECLARE touches — drafts stack on
  top, `PUBLISH` supersedes, and the prior publish stays history, so
  rollback is a re-read, not a mechanism.
- **The shared shell stays the door's.** `shell.html` and `modules/` are
  serving chrome, not app artifacts; only workspace artifacts ride
  statements. INFORMATION LOST: none — the `frames/`/`specs/` layout is
  carried by artifact kind.
- **Validation on declare is the mount validation.** The suite already
  requires every built-in frame to parse; a DECLARE FRAME that fails the
  same check is refused at the door.
- SEMANTICS UNDEFINED: the publish verb's family — `GLOSS` writes
  context, `PUBLISH` flips serving state, the first candidate verb since
  the simplification · draft visibility before publish (rides the
  actor/auth basket, held open).
