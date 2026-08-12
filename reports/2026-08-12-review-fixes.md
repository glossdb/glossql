# 2026-08-12 — Fixes for the adversarial review of the recent additions

Every finding in `2026-08-12-adversarial-review-recent.md` is fixed,
plus the two low app-door notes. Each fix carries a regression test
written against the finding's failure scenario; 19 new tests across
five crates. Workspace suite green (46 suites), clippy clean in every
touched file (the one remaining workspace warning,
`plane.rs:28 type_complexity`, predates this work and was left).

## The freshness layer (findings 2, 4, 5, 6 — the cluster)

**Deletion now invalidates like a write** (finding 2,
`crates/glossary/src/store.rs`). A forwarded DELETE pre-selects the
doomed rows with its own WHERE predicate, then — only if rows were
removed — applies the write-side edges: a glossary delete runs the
same ACCEPTS invalidation a gloss write runs for each affected
(dataset, aspect) and always drops the dataset's `whatif` cache rows;
a cache delete striking a function voice drops the detector verdicts
over that function's RETURNS aspect, so a struck disputed slot cannot
leave a stale contested withholding. Predicates the AST cannot
soundly re-render (USING, LIMIT, and kin) route to a blunt
whole-cache sweep, commented as such. Tests cover the targeted path,
the blunt fallback, and that another dataset's rows survive.

**The `whatif.` cache re-keyed** (finding 4,
`crates/session/src/whatif.rs:145-155`). Freshness is now the newest
write over the scenario's slots ∪ every QUERY-kind aspect's slots —
superseding a concept grounding recomputes the read. Verified no
feedback loop: the whatif cache row never surfaces in the slot read.

**Channels cross-invalidate** (finding 5). The `Lake` carries a
monotonic `data_version`, bumped on namespace create, materialization,
and `DROP TABLE`; each session's read context is tagged with the
version it was built at and rebuilds on mismatch
(`crates/catalog/src/lib.rs`, `crates/session/src/reads.rs`). The
channels test lands from one channel and asserts another channel's
collapsed read flips `current` → `stale` — with no `USE` in between,
since `USE` clearing the local cache is exactly what masked this.

**`metric_bands` sees data land** (finding 6,
`crates/scripts/functions/bootstrap.glossql`): `ACCEPTS (glossary,
imports)`, matching its siblings. Existing workspaces keep the old
declaration until wipe + re-bootstrap — the ruled mechanism; no
migration built.

## The doors

**`SELECT … INTO` refused on the streaming path** (finding 3,
`crates/session/src/session.rs`): `query_stream_with_params` now runs
the same `selects_into` guard as the execute path, before planning —
so the spelling neither mints a table nor materializes the source
past the row cap from `/query`, `/mcp`, or a frame.

**Cycle guards on `whatif.` and `misfit.`** (finding 10,
`crates/session/src/reads.rs`): the three doors share one expansion
stack with door-prefixed keys; a self-referential body errors
`read cycle: …` instead of recursing to stack overflow. The error
text gained the door prefix; the serve-door test pins the new form.

**Provider generations heal** (finding 9): the session tracks which
generation of the shared provider it mounted and re-registers the
bound dataset's schema when a namespace create rebuilt it — checked
per statement batch, one pointer compare on the fast path
(`session.rs::refresh_mount`).

## The kernel reads

**The constant-column panic** (finding 1,
`crates/scripts/src/lib.rs`): `band_grid` mirrors the kernel
preprocessor's unique filter (NaN-aware) before generating ensemble
members, so the shuffle assert can never fire; no varying feature
returns a clean error the whatif door serves as a refusal row. The
exact single-post-month frame that panicked is now a test against the
real weights.

**Support honesty and the cap** (finding 8,
`crates/session/src/whatif.rs`): worlds × post-months over 2000
refuses with the number and the fix before any world replays; `basis`
counts the worlds that actually contributed and names the skipped;
baseline-only support refuses; roster identity compares month values,
not lengths.

**Misfit refuses non-finite scores** (finding 11,
`crates/session/src/misfit.rs`), naming the ranked columns and the
complementary-null cause — the refuse-or-abstain contract holds.

**Witness thresholds pair with their own verdicts** (finding 7,
`crates/glossary/src/store.rs`): each verdict is judged against its
own witness's threshold; with plural witnesses the slot withholds
when any crosses. Interim by design — whether a second witness per
aspect should be refused outright is a language ruling still owed.

**`GLOSSQL_CANDLE_THREADS=0`** (finding 13) falls back to the default
4 instead of uncapping the pool.

## Accounting and the app door

**Import counts** (finding 12): the import keeps per-scan counts; a
multi-source recipe's outcome lists each source's rows instead of a
misleading difference; the `imports` relation serves NULL, not a
negative, when a fan-out lands more rows than it scanned. Residual,
named: a single-provider fan-out still answers "0 dropped" at the
statement while the relation says NULL — two doors, two honest
approximations of "unknown".

**gl-rows scheme guard**: values substituted into `href`/`src`
reject `javascript:`/`data:`/`vbscript:` schemes. **Partial shadow
refuses loudly**: a workspace `apps/<name>/` without `app.toml` is an
error naming the missing manifest, never a silent fall-through to the
builtin.

## Left alone, deliberately

- The plural-witness refusal question (above) — a ruling, not a fix.
- The second-scan import miscount (files appearing mid-import) —
  needs counting during the read; noted, not built.
- `plane.rs` `type_complexity` lint — predates this work.
