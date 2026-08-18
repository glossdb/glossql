# DELETE in Iceberg v3 (2026-08-17)

The groundwork for the glossary-strike ruling (postponed 2026-08-17,
"I have to dig into that first"). Format facts from `format/spec.md` at
apache/iceberg main (line numbers as of today); implementation facts
from the pinned iceberg-rust checkout (`ebb3f24`, our Cargo.lock).
Nothing here decides anything.

## 1. What the format says

Row-level deletes exist since v2, in two shapes (spec:110-114):

- **Position deletes** name a row by `(file_path, position)`. In v2 they
  are *position delete files* (Parquet/Avro rows of paths and
  positions). **In v3 they are deletion vectors**: "Deletion vectors are
  added in v3 and are not supported in v2 or earlier. Position delete
  files must not be added to v3 tables, but existing position delete
  files are valid" (spec:1355) — valid only through an upgrade, and
  "must be merged into the DV for a data file when one is created"
  (spec:1944).
- **Equality deletes** name rows by column values ("delete every row
  where order_id = 7"). They remain valid in v3, unchanged.

A **deletion vector** is a compressed bitmap over one data file's row
positions, stored as a `deletion-vector-v1` blob in a Puffin file. The
manifest entry that carries it must set `referenced_data_file` (which
data file it covers) and `content_offset`/`content_size_in_bytes`
matching the Puffin footer exactly (spec:752-753). One DV per data file;
a data file's whole delete state is one blob read.

**Scope rules** (spec:1072-1090) — which deletes apply to which data:

- A DV applies when the data file's sequence number is **≤** the DV's
  and the paths match.
- An equality delete applies when the data file's sequence number is
  **strictly less** — equality deletes are about *older* data only, and
  an unpartitioned equality delete is global.
- Position deletes (files and DVs) with sequence number **equal** to the
  data file's apply too: a commit may delete rows it added itself.

**Row lineage interplay** (spec:460-535) — this matters to us because
supersession orders by `(_last_updated_sequence_number, _pos)`:

- When a writer moves an existing row to a new file (compaction,
  copy-on-write), it **must copy** the row's `_row_id`, and for
  unmodified rows **must copy** `_last_updated_sequence_number`
  (spec: "row lineage assignment", rules 1–3). So maintenance rewrites
  preserve our write order.
- Equality deletes are the exception: lineage is not tracked through
  them — an equality-updated row is "completely removed and a unique
  new row was added". For a *strike* that is exactly right: struck rows
  vanish, surviving rows are untouched.

## 2. The two write disciplines

- **Copy-on-write**: the writer rewrites the affected data files without
  the deleted rows and commits an overwrite — reads stay cheap, writes
  pay.
- **Merge-on-read**: the writer commits a DV (or equality delete) beside
  the data — writes stay cheap, readers merge.

Either way, the bytes of deleted rows remain reachable through older
snapshots until `expire_snapshots` — and in iceberg-rust that action is
metadata-only: "Physical file cleanup is the responsibility of a
higher-level maintenance operation" (expire_snapshots.rs:34; puffin
cleanup likewise, :326). True byte removal is delete + expire + file GC,
under every mechanism.

## 3. What iceberg-rust can do today (checkout `ebb3f24`)

**Read side — real.** A scan plans matching delete files per data file
(delete_file_index.rs:185-213, the spec's ≤/< rules), turns equality
deletes into a row filter and position deletes into a row selection
(arrow/reader/pipeline.rs:472, :566-586). Deletes written by other
engines are applied correctly. **Deletion vectors are not read**: the
loader has `// TODO: Delete Vector loader from Puffin files`
(caching_delete_file_loader.rs:54, :146) and a DV entry would be routed
into the Parquet reader and fail as a malformed read — not skipped, not
cleanly refused.

**Write side — sealed.** The whole committable transaction surface is
eight actions (transaction/mod.rs:135-172); the only one that touches
data is `fast_append`, and it hard-rejects anything but data content:
"Only data content type is allowed for fast append"
(transaction/snapshot.rs:139-146). `SnapshotProducer` has no concept of
removed files or added delete files — the marker is
`// # TODO / Support process delete entries.` (snapshot.rs:371-372).
There is no overwrite, rewrite or replace action. An
`EqualityDeleteFileWriter` exists and produces spec-shaped delete files
(equality_delete_writer.rs:178) — with no action that can commit one.
Downstream crates cannot add an action (`TransactionAction` is
`pub(crate)`, action.rs:37) and cannot build a `TableCommit` by hand
(catalog/mod.rs:371-376, deliberately).

**Upstream tracks exactly this**: #2580 "Deletion-vector writer +
RowDelta action" (open), #2792 "[EPIC] Deletion vector read support"
(open), #2186 "Copy-on-Write and Merge-on-Read support" (open), #2201
"MERGE INTO for DataFusion" (open). The gap is acknowledged, named, and
unclaimed.

## 4. What this leaves the strike

The SQL seam is settled independently: DataFusion 54.1 plans
`DELETE FROM t WHERE …` into `TableProvider::delete_from`
(datafusion-catalog-54.1.0/src/table.rs:353), so
`DELETE FROM glossary WHERE …` keeps its surface whatever sits beneath.
The storage mechanism is the open question, and the honest options are:

1. **Upstream-first.** Contribute (or wait for) #2580's RowDelta + DV
   writer, and #2792's DV read — the read half is a prerequisite for
   ever writing DVs, or our own scans break on our own strikes. The
   complete v3-native answer; costs an upstream review cycle.
2. **Equality deletes.** v3-legal, lineage-safe for strikes (survivors
   untouched), and the writer already exists — but committing one needs
   the same missing action, so this is a smaller slice of the same
   upstream work, not an escape from it.
3. **Rebuild the table through the catalog API.** Scan survivors in
   `(seq, pos)` order, write them to `glossary_next`, drop and
   `rename_table` (in the `Catalog` trait, catalog/mod.rs:118, and
   implemented by SqlCatalog, sql/catalog.rs:932). Available today,
   entirely inside the framework — and honest about its costs: the swap
   is not atomic (a crash between drop and rename loses the name, not
   the data), snapshot history restarts, and absolute sequence numbers
   reset (relative order survives because survivors land in one commit
   in their old order, which is all supersession compares).

`glossary`'s crossing (stage 4½) is otherwise unblocked — appends,
supersession and the collapse need none of this. Only the strike waits
on the ruling.
