# Upstream issue draft — apache/iceberg-rust

**Filed 2026-08-18 as apache/iceberg-rust#3019.** Line references are
against main @ `a500a2e7`; the same shape ships in 0.10.x.

---

**Title:** datafusion: no way to set snapshot summary properties on the
`insert_into` commit path

**Labels (suggested):** `integration-datafusion`, `improvement`

---

The transaction API supports user snapshot properties on an append —
`FastAppendAction::set_snapshot_properties`
(`crates/iceberg/src/transaction/append.rs:70`), carried into the
snapshot summary and validated against reserved keys (#2744, #2725 show
this surface is maintained). The DataFusion integration cannot reach
it: `IcebergTableProvider::insert_into`
(`crates/integrations/datafusion/src/table/mod.rs:153`) builds
`IcebergWriteExec` + `IcebergCommitExec` (`table/mod.rs:221-226`), and
the commit node creates the transaction internally —

```rust
// crates/integrations/datafusion/src/physical_plan/commit.rs:243-252
let tx = Transaction::new(&table);
let action = tx.fast_append().add_data_files(data_files);
let _updated_table = action
    .apply(tx)
    .map_err(to_datafusion_error)?
    .commit(catalog.as_ref())
    .await?;
```

— with no hook between building the action and committing it, so the
properties parameter that exists one layer down is unreachable from an
`INSERT INTO`.

## Why this matters

Facts about a write are naturally snapshot summary properties: source
scan counts, rows dropped by an ingest filter, cast-failure tallies, a
recipe/job id — anything audit- or lineage-shaped that describes *this
append* rather than the table. An embedder who wants them today has to
reimplement the integration's whole write path beside the integration —
the same `RollingFileWriterBuilder` + `DataFileWriterBuilder` calls as
`physical_plan/write.rs:239`, the same `fast_append` as
`commit.rs:244` — solely to pass one `HashMap` the transaction API
already accepts. That is what we ended up doing, and the duplicated
path has to track every upstream improvement to the real one by hand.

## Proposal

The smallest API that closes the gap: a builder-style setter on the
provider, threaded through to the commit node —

```rust
let provider = IcebergTableProvider::try_new(...)
    .await?
    .with_snapshot_properties(props); // HashMap<String, String>
```

`IcebergCommitExec` gains the field and applies it:

```rust
let action = tx.fast_append()
    .add_data_files(data_files)
    .set_snapshot_properties(self.snapshot_properties.clone());
```

Granularity note: properties set this way are per-provider, which is
per-statement for embedders that register a provider per statement, and
that is the audit use case. A session/statement-option spelling (e.g.
`iceberg.snapshot-property.*`) could layer on later for SQL-level use,
but the provider hook alone removes the need to fork the write path.

Reserved-key validation stays where it is — the action already owns it,
so an invalid property fails the commit exactly as it does through the
transaction API directly.

Happy to send a PR if the shape sounds right.
