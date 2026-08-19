//! The relational executor: recipe and probe SQL runs **at the source**
//! over ADBC, and the driver hands back Arrow batches that land
//! unconverted (adbc arrow-array 58 is the workspace's arrow). The
//! driver is a shared library the manager resolves from the source's
//! `driver` setting — a name searched on the standard paths, or a
//! filesystem path to the library itself; it ships outside cargo, which
//! is the executor's one operational dependency.

use adbc_core::options::{AdbcVersion, OptionDatabase};
use adbc_core::{Connection as _, Database as _, Driver as _, LOAD_FLAG_DEFAULT, Statement as _};
use adbc_driver_manager::ManagedDriver;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::sql::sqlparser::ast::Statement as SQLStatement;
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;

use crate::{Error, Result, SourceSpec};

/// The loadable drivers, hardcoded from the ADBC driver index
/// (arrow.apache.org/adbc). A source's `driver`
/// setting is the index **slug** — the name the operator's install
/// registered (`dbc install <slug>`) — or a filesystem path to the
/// library. Hardcoding the list is the ruled workaround until a served
/// build ships its drivers; the operator installs them.
const KNOWN_DRIVERS: &[&str] = &[
    "bigquery",
    "clickhouse",
    "databricks",
    "datafusion",
    "duckdb",
    "exasol",
    "flightsql",
    "mssql",
    "mysql",
    "postgresql",
    "quack",
    "redshift",
    "singlestore",
    "snowflake",
    "spark",
    "sqlite",
    "trino",
];

/// What one statement returned from the source, cut at `row_cap` (one
/// row past it, so the caller can tell a truncated answer from a
/// complete one — the doors' convention).
pub(crate) struct SourceRead {
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
}

/// Execute one SQL statement at the source and pull its result. Blocking
/// (the ADBC surface is synchronous FFI) — callers wrap it in
/// `block_in_place`.
pub(crate) fn run_at_source(spec: &SourceSpec, sql: &str, row_cap: usize) -> Result<SourceRead> {
    refuse_non_query(spec, sql)?;
    // `SourceSpec::from_settings` is the one constructor and refuses a
    // relational source without a driver.
    let driver = spec
        .driver
        .as_deref()
        .expect("relational specs carry a driver");
    let adbc = |e: adbc_core::error::Error| Error::Relational {
        name: spec.name.clone(),
        detail: e.to_string(),
    };
    let mut driver =
        ManagedDriver::load_from_name(driver, None, AdbcVersion::V100, LOAD_FLAG_DEFAULT, None)
            .map_err(|e| Error::Relational {
                name: spec.name.clone(),
                detail: format!(
                    "{e} — `driver` is the ADBC index slug the operator installed \
             ({}) or a path to the driver library",
                    KNOWN_DRIVERS.join(", ")
                ),
            })?;
    // The source's location IS its URI — one setting names where a
    // source lives, whatever kind it is.
    let uri = spec.location.to_string_lossy();
    let database = driver
        .new_database_with_opts([(OptionDatabase::Uri, uri.as_ref().into())])
        .map_err(adbc)?;
    let mut connection = database.new_connection().map_err(adbc)?;
    let mut statement = connection.new_statement().map_err(adbc)?;
    statement.set_sql_query(sql).map_err(adbc)?;
    let reader = statement.execute().map_err(adbc)?;
    let schema = reader.schema();
    let mut batches = Vec::new();
    let mut rows = 0usize;
    for batch in reader {
        let batch = batch.map_err(|e| Error::Batches(e.to_string()))?;
        rows += batch.num_rows();
        batches.push(batch);
        if rows > row_cap {
            break;
        }
    }
    Ok(SourceRead { schema, batches })
}

/// Best effort, honestly so: the backend speaks its own dialect, which
/// may not parse here at all — an unparseable body passes through, and
/// the credentials the source was declared with decide what it may do.
/// What *does* parse must be a single query: the fence exists so an
/// accidental `DELETE` or a `;`-chained pair is refused at our door
/// instead of executing against someone's database.
fn refuse_non_query(spec: &SourceSpec, sql: &str) -> Result<()> {
    match Parser::parse_sql(&GenericDialect {}, sql) {
        Ok(statements) => {
            if statements.len() == 1 && matches!(statements[0], SQLStatement::Query(_)) {
                Ok(())
            } else {
                Err(Error::Relational {
                    name: spec.name.clone(),
                    detail: "a recipe or probe at a relational source is one SELECT — \
                             the source is read, never written"
                        .into(),
                })
            }
        }
        Err(_) => Ok(()),
    }
}
