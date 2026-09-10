# The dev rig

The backends the server can stand on, as one compose: what the live
tests run against, and what a laptop runs the server against when the
state is to live somewhere other than the workspace directory.

- Default: Postgres 18 as the SQL catalog (`init.sql` makes three
  databases on first start — `glossql`, `glossql_az`, `lakekeeper`)
  and Azurite as an Azure warehouse (`azurite-container.py` makes the
  `lake` container).
- `--profile rest`: the REST catalog pair — SeaweedFS as the S3 store
  (`s3.json` holds its one identity, `seaweed-dev` / `seaweed-secret`)
  and Lakekeeper as the catalog, anonymous, bootstrapped with a
  `glossql` warehouse over the `lake` bucket (`lakekeeper-init.sh`).
  SeaweedFS has no STS, so Lakekeeper vends nothing and the server
  reads the keys from `AWS_*`.

```bash
docker compose -f dev/compose.yaml up -d                    # Postgres + Azurite
docker compose -f dev/compose.yaml --profile rest up -d     # and the REST pair
docker compose -f dev/compose.yaml --profile rest down -v   # everything, data included
```

Every port binds to the loopback: 5432, 10000, 8333, 8181. A container
on the compose network — the server image, say — reaches them by
service name instead, and Azurite needs its address said as object_store's
emulator mode reads it: `AZURITE_BLOB_STORAGE_URL=http://azurite:10000`
beside `AZURE_STORAGE_USE_EMULATOR=true`.

```bash
docker run --rm --network glossql-dev_default -p 127.0.0.1:8080:8080 \
  -e GLOSSQL_INSECURE_OPEN=true \
  -e GLOSSQL_CATALOG_SQL=postgres://glossql:glossql@postgres:5432/glossql_az \
  -e GLOSSQL_WAREHOUSE=abfss://lake@devstoreaccount1.dfs.core.windows.net/warehouse \
  -e AZURE_STORAGE_USE_EMULATOR=true -e AZURITE_BLOB_STORAGE_URL=http://azurite:10000 \
  glossql:dev                                              # docker build -t glossql:dev .
```

## The live tests

Each is `#[ignore]` and runs when its variables are set.

Postgres as the catalog, the warehouse on the local disk:

```bash
GLOSSQL_E2E_CATALOG_SQL=postgres://glossql:glossql@127.0.0.1:5432/glossql \
  cargo test -p glossql-catalog -p glossql-serverd live_sql -- --ignored
```

Postgres as the catalog, the warehouse on Azurite — in a catalog
database of its own, because a catalog remembers where its files are:

```bash
GLOSSQL_E2E_CATALOG_SQL=postgres://glossql:glossql@127.0.0.1:5432/glossql_az \
GLOSSQL_E2E_WAREHOUSE=abfss://lake@devstoreaccount1.dfs.core.windows.net/warehouse \
AZURE_STORAGE_USE_EMULATOR=true \
  cargo test -p glossql-catalog -p glossql-serverd live_sql -- --ignored
```

A file source in the emulator, the same client the warehouse uses: one
CSV put under the root, then listed, landed, and probed over a glob.

```bash
GLOSSQL_E2E_SOURCE=abfss://lake@devstoreaccount1.dfs.core.windows.net/sources/finance \
AZURE_STORAGE_USE_EMULATOR=true \
  cargo test -p glossql-import live_source -- --ignored
```

The REST catalog over S3:

```bash
GLOSSQL_E2E_CATALOG_URI=http://127.0.0.1:8181/catalog \
GLOSSQL_E2E_CATALOG_WAREHOUSE=glossql \
GLOSSQL_E2E_CATALOG_TOKEN=dev-anonymous \
AWS_ACCESS_KEY_ID=seaweed-dev AWS_SECRET_ACCESS_KEY=seaweed-secret \
AWS_ENDPOINT=http://127.0.0.1:8333 AWS_DEFAULT_REGION=local-01 AWS_ALLOW_HTTP=true \
  cargo test -p glossql-catalog live_catalog -- --ignored
```

The kernel service, from a glosskernels checkout running `uv run
glosskernels`:

```bash
GLOSSQL_E2E_TABICL_URL=http://127.0.0.1:8100 \
  cargo test -p glossql-scripts -- --ignored
```

The server itself takes the same variables without the `E2E_`
(`docs/start/install.md`).

## Resetting

`down -v` drops every volume. The local-disk live test keeps its
warehouse at `$TMPDIR/glossql-e2e-sql/warehouse` across runs, because
the catalog names those files; reset the two together. A catalog that
once pointed at a local warehouse is never reused with a remote one —
the objects it names are not there — which is why Azurite gets
`glossql_az`.
