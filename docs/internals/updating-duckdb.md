# Updating DuckDB

glossql is a C++ extension built against DuckDB's internal API, so it is
built for one DuckDB release at a time. Today that release is v2.0.0.

## The pins

| Where | What | Today |
|---|---|---|
| `duckdb/` (submodule) | the DuckDB source the extension builds against | `v2.0.0` |
| `extension-ci-tools/` (submodule) | DuckDB's build and CI tooling | branch `v2.0-cyanoptera` |
| `Makefile`, `OVERRIDE_GIT_DESCRIBE` | the version the build is named after; the released `duckdb` loads only a build named after its own release | `v2.0.0` |
| `.github/workflows/MainDistributionPipeline.yml` | `duckdb_version`, `ci_tools_version`, and the reusable workflows' refs | `v2.0.0`, `v2.0-cyanoptera` |

## To move to a new release

1. Check out the release tag in `duckdb/` and the matching branch in
   `extension-ci-tools/`.
2. Set `OVERRIDE_GIT_DESCRIBE` in the `Makefile` and the versions in
   `MainDistributionPipeline.yml` to the new release.
3. Build (`GEN=ninja make`) and run the suites
   (`./build/release/test/unittest "test/sql/glossql*"`).
4. Fix what the internal API changed. DuckDB's
   [release notes](https://github.com/duckdb/duckdb/releases) and the
   history of the header that changed are the places to look.

The glossdb server links DuckDB through its Rust crate and loads this
extension, so both must move to the same release together: the server
pins this repository's commit in `crates/duck/glossql.ref`.
