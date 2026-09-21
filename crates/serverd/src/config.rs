//! What a run is told, read once before anything opens: the flags, and
//! the environment — `.env` beside a workspace, or the variables a
//! platform injects. A sizing flag has a variable named after it
//! (`--memory-limit` is `GLOSSQL_MEMORY_LIMIT`), the flag winning, so a
//! deployment sizes a box without replacing the image's command. The
//! arrangements that carry a secret are variables only, since a flag
//! sits in a process list. Every reading goes through `get`, so it is
//! testable without touching the process environment. The telemetry's
//! variables are the export SDK's own and are read where it is
//! installed ([`glossql_serverd::telemetry`]).

use std::path::PathBuf;

use glossql_serverd::{DEFAULT_ROW_CAP, DoorConfig};

pub const USAGE: &str = "usage: glossql [--workspace <dir>] [--addr <ip:port>] \
[--row-cap <n>] [--cube-cache <megabytes>] [--memory-limit <megabytes>] \
[--spill-limit <megabytes>] \
| glossql --version | glossql --help\n\
--workspace is the laptop's shape: the directory holding the catalog \
and the warehouse. GLOSSQL_CATALOG_SQL names the catalog on a Postgres server \
(postgres://…), GLOSSQL_WAREHOUSE the warehouse in an object store \
(s3://…, abfss://…), GLOSSQL_CATALOG_URI a REST catalog with both \
behind it; a deployment names them and runs without a directory.\n\
every other flag has a variable named after it, the flag winning: \
GLOSSQL_ADDR, GLOSSQL_ROW_CAP, GLOSSQL_CUBE_CACHE, GLOSSQL_MEMORY_LIMIT, \
GLOSSQL_SPILL_LIMIT.\n\
the band model behind the metric-bands walk, whatif. and misfit. is \
the kernel service named by GLOSSQL_TABICL_URL (its bearer in \
GLOSSQL_TABICL_TOKEN); unset, those three doors refuse by name and \
everything else serves.\n\
the authorization arrangement is read from .env or the environment: \
GLOSSQL_ISSUER, GLOSSQL_CLIENT_ID, GLOSSQL_CLIENT_SECRET, [GLOSSQL_AUDIENCE] \
— or GLOSSQL_INSECURE_OPEN=true serves the doors without authentication, \
every caller recorded as insecure_dev_mode (the name is the warning); \
so is the catalog connection, when there is one: GLOSSQL_CATALOG_URI, \
GLOSSQL_CATALOG_WAREHOUSE and its authentication (see .env.example)";

/// The command line as typed. `workspace` decides whether `.env` is
/// read, so the flags are parsed before the environment is.
#[derive(Debug, Default)]
pub struct Flags {
    /// The laptop's shape, and only that: the directory holding the
    /// catalog and the warehouse. A deployment names both in the
    /// environment and has no directory.
    pub workspace: Option<PathBuf>,
    addr: Option<String>,
    row_cap: Option<usize>,
    cube_cache_mb: Option<u64>,
    memory_limit_mb: Option<u64>,
    spill_limit_mb: Option<u64>,
}

pub fn parse(mut argv: impl Iterator<Item = String>) -> Result<Flags, String> {
    argv.next();
    let mut flags = Flags::default();
    while let Some(flag) = argv.next() {
        let mut value = || argv.next().ok_or(format!("{flag} needs a value"));
        match flag.as_str() {
            "--workspace" => flags.workspace = Some(PathBuf::from(value()?)),
            "--addr" => flags.addr = Some(value()?),
            "--row-cap" => flags.row_cap = Some(number(&flag, &value()?)?),
            "--cube-cache" => flags.cube_cache_mb = Some(number(&flag, &value()?)?),
            "--memory-limit" => flags.memory_limit_mb = Some(number(&flag, &value()?)?),
            "--spill-limit" => flags.spill_limit_mb = Some(number(&flag, &value()?)?),
            other => return Err(format!("unknown flag {other}")),
        }
    }
    Ok(flags)
}

/// A flag's or a variable's number; the refusal names which one.
fn number<T: std::str::FromStr<Err: std::fmt::Display>>(
    name: &str,
    value: &str,
) -> Result<T, String> {
    value.trim().parse().map_err(|e| format!("{name}: {e}"))
}

/// The run's whole configuration. Nothing past [`Config::read`] reads a
/// flag or a variable.
pub struct Config {
    pub addr: String,
    pub doors: DoorConfig,
    /// Rows a data read ships before a door declares `truncated`.
    pub row_cap: usize,
    /// The process-wide byte budget for cubes, in megabytes.
    pub cube_cache_mb: u64,
    /// The engine's memory ceiling for the whole process, in megabytes.
    /// A separate budget from the cubes: the cube cache holds its bytes
    /// outside the engine, so a deployment is sized for the sum.
    pub memory_limit_mb: u64,
    /// The disk the engine may spill onto, in megabytes — the box's
    /// disk, a number of its own; unset, twice the memory ceiling.
    pub spill_limit_mb: Option<u64>,
    /// This server's own URI — `GLOSSQL_AUDIENCE`, the API identifier a
    /// token must name (RFC 8707 §2) and the host the world reaches
    /// the doors at — defaulting to the bind address. Read under both
    /// arrangements: the open switch verifies nobody, and the door
    /// still has to know its name.
    pub audience: String,
    /// The authorization arrangement; none under the open switch.
    pub auth: Option<Auth>,
    /// The kernel service, when the environment names one.
    pub kernel: Option<Kernel>,
    pub catalog: Catalog,
}

/// The authorization arrangement: one issuer and one registered
/// application, verified against the run's audience. It comes from the
/// environment as one thing — `.env` beside the server or the variables
/// set outright.
#[derive(Debug)]
pub struct Auth {
    /// The authorization server's issuer URL, from which its keys are
    /// discovered.
    pub issuer: String,
    /// The application registered at the issuer for this server, and
    /// its secret, which only the browser login uses.
    pub client_id: String,
    pub client_secret: String,
}

/// The kernel service behind the model reads: `GLOSSQL_TABICL_URL`,
/// its bearer in `GLOSSQL_TABICL_TOKEN`.
pub struct Kernel {
    pub url: String,
    pub token: Option<String>,
}

/// The workspace data plane: the REST catalog when the environment
/// names one, the SQL catalog otherwise — on the Postgres server the
/// environment names, or the workspace directory's own SQLite file.
/// One backend serves a run. A laptop names a directory and it holds
/// the catalog and the warehouse; a deployment names both in the
/// environment and has no directory at all.
pub enum Catalog {
    #[cfg(feature = "rest")]
    Rest(glossql_catalog::rest::Connection),
    /// The catalog URI carries the credentials; the warehouse carries
    /// none.
    #[cfg(feature = "sql")]
    Sql { catalog: String, warehouse: String },
}

impl Config {
    pub fn read(flags: Flags, get: impl Fn(&str) -> Option<String>) -> Result<Config, String> {
        let var = |name: &str| get(name).filter(|v| !v.trim().is_empty());
        let sized = |name: &str| var(name).map(|v| number::<u64>(name, &v)).transpose();

        let addr = flags
            .addr
            .or_else(|| var("GLOSSQL_ADDR"))
            .unwrap_or_else(|| "127.0.0.1:8080".to_string());
        let audience = var("GLOSSQL_AUDIENCE").unwrap_or_else(|| format!("http://{addr}"));
        let doors = DoorConfig {
            allowed_hosts: allowed_hosts(&audience),
            own_uri: audience.clone(),
        };
        let row_cap = match flags.row_cap {
            Some(n) => n,
            None => var("GLOSSQL_ROW_CAP")
                .map(|v| number("GLOSSQL_ROW_CAP", &v))
                .transpose()?
                .unwrap_or(DEFAULT_ROW_CAP),
        };
        // The open switch is read where the arrangement would be: a run
        // is one or the other, and a misconfigured arrangement still
        // refuses rather than falling open.
        let auth = if open(&get) {
            None
        } else {
            Some(Auth::from(&var)?)
        };
        Ok(Config {
            cube_cache_mb: match flags.cube_cache_mb {
                Some(mb) => mb,
                None => {
                    sized("GLOSSQL_CUBE_CACHE")?.unwrap_or(glossql_session::DEFAULT_CUBE_CACHE_MB)
                }
            },
            memory_limit_mb: match flags.memory_limit_mb {
                Some(mb) => mb,
                None => sized("GLOSSQL_MEMORY_LIMIT")?
                    .unwrap_or(glossql_session::DEFAULT_MEMORY_LIMIT_MB),
            },
            spill_limit_mb: match flags.spill_limit_mb {
                Some(mb) => Some(mb),
                None => sized("GLOSSQL_SPILL_LIMIT")?,
            },
            kernel: var("GLOSSQL_TABICL_URL").map(|url| Kernel {
                url,
                token: var("GLOSSQL_TABICL_TOKEN"),
            }),
            catalog: Catalog::from(&var, flags.workspace.as_deref())?,
            addr,
            doors,
            row_cap,
            audience,
            auth,
        })
    }
}

impl Auth {
    fn from(var: &impl Fn(&str) -> Option<String>) -> Result<Auth, String> {
        let required = |name: &str| {
            var(name).ok_or_else(|| {
                format!(
                    "{name} is not set — the authorization arrangement lives in .env \
                     (see .env.example); GLOSSQL_INSECURE_OPEN=true serves open instead, \
                     every caller insecure_dev_mode"
                )
            })
        };
        Ok(Auth {
            issuer: required("GLOSSQL_ISSUER")?,
            client_id: required("GLOSSQL_CLIENT_ID")?,
            client_secret: required("GLOSSQL_CLIENT_SECRET")?,
        })
    }
}

/// The explicit way to serve without the arrangement:
/// `GLOSSQL_INSECURE_OPEN=true` opens every door, every caller
/// recorded as [`glossql_serverd::INSECURE_DEV_MODE`]. Only the literal
/// `true` counts — the switch is a statement, never a fallback:
/// anything else leaves the gate required, and the refusal names the
/// switch.
fn open(get: &impl Fn(&str) -> Option<String>) -> bool {
    get("GLOSSQL_INSECURE_OPEN").is_some_and(|v| v.trim() == "true")
}

/// Whose `Host` header the agent door answers: loopback — the
/// transport's DNS-rebinding guard, for a server on a laptop where a
/// browser could be steered at 127.0.0.1 — and the audience's host,
/// the name the world reaches this server by. A deployment names its
/// URL; one that does not answers its bind address alone, and the
/// transport says so to every real request.
fn allowed_hosts(audience: &str) -> Vec<String> {
    let mut hosts = DoorConfig::default().allowed_hosts;
    if let Some(host) = audience
        .parse::<axum::http::Uri>()
        .ok()
        .and_then(|uri| uri.host().map(str::to_string))
        && !hosts.contains(&host)
    {
        hosts.push(host);
    }
    hosts
}

impl Catalog {
    fn from(
        var: &impl Fn(&str) -> Option<String>,
        workspace: Option<&std::path::Path>,
    ) -> Result<Catalog, String> {
        #[cfg(feature = "rest")]
        if let Some(connection) = rest_from(var)? {
            return Ok(Catalog::Rest(connection));
        }
        sql_from(var, workspace)
    }
}

/// What a run without a workspace directory is told when the
/// environment does not name the state either.
#[cfg(feature = "sql")]
const NO_WORKSPACE: &str = "--workspace is required while the catalog or the warehouse lives \
in it: name both GLOSSQL_CATALOG_SQL and GLOSSQL_WAREHOUSE, or the directory";

/// The SQL catalog: on the Postgres server `GLOSSQL_CATALOG_SQL` names,
/// or the workspace directory's own SQLite file; the warehouse at the
/// location `GLOSSQL_WAREHOUSE` names in an object store, or under the
/// workspace directory. Without a directory the environment has to
/// name both.
#[cfg(feature = "sql")]
fn sql_from(
    var: &impl Fn(&str) -> Option<String>,
    workspace: Option<&std::path::Path>,
) -> Result<Catalog, String> {
    let catalog = match (var("GLOSSQL_CATALOG_SQL"), workspace) {
        (Some(uri), _) => uri.trim().to_string(),
        (None, Some(dir)) => format!("sqlite:{}?mode=rwc", dir.join("catalog.sqlite").display()),
        (None, None) => return Err(NO_WORKSPACE.into()),
    };
    let warehouse = match (var("GLOSSQL_WAREHOUSE"), workspace) {
        (Some(location), _) => location.trim().to_string(),
        (None, Some(dir)) => dir.join("warehouse").display().to_string(),
        (None, None) => return Err(NO_WORKSPACE.into()),
    };
    Ok(Catalog::Sql { catalog, warehouse })
}

#[cfg(not(feature = "sql"))]
fn sql_from(
    _var: &impl Fn(&str) -> Option<String>,
    _workspace: Option<&std::path::Path>,
) -> Result<Catalog, String> {
    Err("this build carries no local catalog — set GLOSSQL_CATALOG_URI (see .env.example)".into())
}

/// The catalog connection the environment describes, `None` without
/// `GLOSSQL_CATALOG_URI`: a URI names its warehouse and exactly one way
/// to authenticate, and a credential names where it is exchanged.
#[cfg(feature = "rest")]
fn rest_from(
    var: &impl Fn(&str) -> Option<String>,
) -> Result<Option<glossql_catalog::rest::Connection>, String> {
    use glossql_catalog::rest::{Auth as CatalogAuth, Connection};
    let Some(uri) = var("GLOSSQL_CATALOG_URI") else {
        return Ok(None);
    };
    let warehouse = var("GLOSSQL_CATALOG_WAREHOUSE").ok_or(
        "GLOSSQL_CATALOG_WAREHOUSE is not set — a REST catalog connection names its warehouse",
    )?;
    let auth = match (
        var("GLOSSQL_CATALOG_TOKEN"),
        var("GLOSSQL_CATALOG_CREDENTIAL"),
    ) {
        (Some(token), None) => CatalogAuth::Token(token),
        (None, Some(credential)) => CatalogAuth::ClientCredentials {
            credential,
            token_endpoint: var("GLOSSQL_CATALOG_TOKEN_ENDPOINT").ok_or(
                "GLOSSQL_CATALOG_TOKEN_ENDPOINT is not set — a credential is exchanged at its \
                 authorization server's token endpoint",
            )?,
            scope: var("GLOSSQL_CATALOG_SCOPE"),
        },
        (Some(_), Some(_)) => {
            return Err(
                "GLOSSQL_CATALOG_TOKEN and GLOSSQL_CATALOG_CREDENTIAL are both set — \
                 one of them authenticates the catalog connection"
                    .into(),
            );
        }
        (None, None) => {
            return Err(
                "neither GLOSSQL_CATALOG_TOKEN nor GLOSSQL_CATALOG_CREDENTIAL is set — \
                 the catalog connection has nothing to authenticate with"
                    .into(),
            );
        }
    };
    Ok(Some(Connection {
        uri,
        warehouse,
        auth,
    }))
}

#[cfg(test)]
mod tests {
    use super::{Config, allowed_hosts, parse};

    fn flags(flags: &[&str]) -> super::Flags {
        parse(
            std::iter::once("glossql")
                .chain(flags.iter().copied())
                .map(str::to_string),
        )
        .expect("the flags parse")
    }

    fn env(vars: &'static [(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        move |name: &str| {
            vars.iter()
                .find(|(k, _)| *k == name)
                .map(|(_, v)| v.to_string())
        }
    }

    /// A readable run: a workspace and the open switch, plus `vars`.
    fn read(typed: &[&str], vars: &'static [(&str, &str)]) -> Result<Config, String> {
        let base = env(&[("GLOSSQL_INSECURE_OPEN", "true")]);
        let more = env(vars);
        let mut all = vec!["--workspace", "/tmp/w"];
        all.extend(typed);
        Config::read(flags(&all), move |name| more(name).or_else(|| base(name)))
    }

    /// A configuration holds auth material, so it carries no `Debug` to
    /// unwrap through; a refusal is read off the error alone.
    fn refusal(read: Result<Config, String>) -> String {
        read.err().expect("a refusal, not a configuration")
    }

    /// The audience's host joins the transport's loopback list; a bind
    /// address as the audience adds the bind address and nothing else.
    #[test]
    fn the_agent_door_answers_loopback_and_its_own_name() {
        let named = allowed_hosts("https://glossql-trial.example.azurecontainerapps.io");
        assert!(named.contains(&"glossql-trial.example.azurecontainerapps.io".to_string()));
        assert!(named.contains(&"127.0.0.1".to_string()) && named.len() == 4);
        let bound = allowed_hosts("http://0.0.0.0:8080");
        assert!(bound.contains(&"0.0.0.0".to_string()) && bound.len() == 4);
        assert_eq!(allowed_hosts("http://127.0.0.1:8080").len(), 3);

        let named = read(
            &["--addr", "0.0.0.0:8080"],
            &[("GLOSSQL_AUDIENCE", "https://glossql.example")],
        )
        .expect("readable");
        assert_eq!(named.audience, "https://glossql.example");
        assert!(
            named
                .doors
                .allowed_hosts
                .contains(&"glossql.example".to_string())
        );
        let unnamed = read(&[], &[]).expect("readable");
        assert_eq!(
            unnamed.audience, "http://127.0.0.1:8080",
            "the audience defaults to the address"
        );
    }

    /// Without an issuer and a registered application there is nothing
    /// to verify against, and the server does not start. The refusal
    /// names the variable and where it lives, since the flags say
    /// nothing about it.
    #[test]
    fn the_arrangement_needs_an_issuer_and_an_application() {
        let gated = |vars: &'static [(&str, &str)]| {
            Config::read(flags(&["--workspace", "/tmp/w"]), env(vars))
        };
        let none = refusal(gated(&[]));
        assert!(
            none.contains("GLOSSQL_ISSUER") && none.contains(".env"),
            "{none}"
        );

        let issuer_only = refusal(gated(&[("GLOSSQL_ISSUER", "https://issuer.test")]));
        assert!(issuer_only.contains("GLOSSQL_CLIENT_ID"), "{issuer_only}");

        let no_secret = refusal(gated(&[
            ("GLOSSQL_ISSUER", "https://issuer.test"),
            ("GLOSSQL_CLIENT_ID", "app-1"),
        ]));
        assert!(no_secret.contains("GLOSSQL_CLIENT_SECRET"), "{no_secret}");

        let whole = gated(&[
            ("GLOSSQL_ISSUER", "https://issuer.test"),
            ("GLOSSQL_CLIENT_ID", "app-1"),
            ("GLOSSQL_CLIENT_SECRET", "s3cret"),
            ("GLOSSQL_AUDIENCE", "https://glossql.example"),
        ])
        .expect("readable");
        assert_eq!(whole.audience, "https://glossql.example");
        assert!(
            whole
                .auth
                .is_some_and(|a| a.issuer == "https://issuer.test")
        );
    }

    /// The open switch is a statement, not a fallback: only the
    /// literal `true` opens, anything else keeps the gate required.
    #[test]
    fn only_the_literal_true_opens_the_doors() {
        let env = |v: Option<&'static str>| move |_: &str| v.map(str::to_string);
        assert!(super::open(&env(Some("true"))));
        assert!(super::open(&env(Some(" true "))), "whitespace is trimmed");
        assert!(!super::open(&env(None)));
        assert!(!super::open(&env(Some("1"))));
        assert!(!super::open(&env(Some("TRUE"))));
        assert!(!super::open(&env(Some("false"))));
    }

    /// The catalog connection comes from the environment whole, or not
    /// at all: no URI is the local catalog, a URI must name its
    /// warehouse and exactly one way to authenticate, and a credential
    /// must name where it is exchanged. Each refusal names the missing
    /// variable.
    #[cfg(feature = "rest")]
    #[test]
    fn the_catalog_connection_is_read_whole_or_not_at_all() {
        use super::Catalog;
        use glossql_catalog::rest::Auth as CatalogAuth;

        #[cfg(feature = "sql")]
        assert!(
            matches!(
                read(&[], &[]).expect("readable").catalog,
                Catalog::Sql { .. }
            ),
            "no URI is the local catalog"
        );

        let bare = read(&[], &[("GLOSSQL_CATALOG_URI", "https://c.test")]);
        assert!(refusal(bare).contains("GLOSSQL_CATALOG_WAREHOUSE"));

        let unauthenticated = read(
            &[],
            &[
                ("GLOSSQL_CATALOG_URI", "https://c.test"),
                ("GLOSSQL_CATALOG_WAREHOUSE", "w1"),
            ],
        );
        assert!(refusal(unauthenticated).contains("GLOSSQL_CATALOG_TOKEN"));

        let token = read(
            &[],
            &[
                ("GLOSSQL_CATALOG_URI", "https://c.test"),
                ("GLOSSQL_CATALOG_WAREHOUSE", "w1"),
                ("GLOSSQL_CATALOG_TOKEN", "tok"),
            ],
        )
        .expect("readable");
        assert!(matches!(
            token.catalog,
            Catalog::Rest(c) if matches!(&c.auth, CatalogAuth::Token(t) if t == "tok")
        ));

        let endpointless = read(
            &[],
            &[
                ("GLOSSQL_CATALOG_URI", "https://c.test"),
                ("GLOSSQL_CATALOG_WAREHOUSE", "w1"),
                ("GLOSSQL_CATALOG_CREDENTIAL", "id:secret"),
            ],
        );
        assert!(refusal(endpointless).contains("GLOSSQL_CATALOG_TOKEN_ENDPOINT"));

        let both = read(
            &[],
            &[
                ("GLOSSQL_CATALOG_URI", "https://c.test"),
                ("GLOSSQL_CATALOG_WAREHOUSE", "w1"),
                ("GLOSSQL_CATALOG_TOKEN", "tok"),
                ("GLOSSQL_CATALOG_CREDENTIAL", "id:secret"),
            ],
        );
        assert!(refusal(both).contains("both set"));
    }

    /// Two budgets, two numbers, and neither borrows the other's
    /// default. The cube cache holds its bytes outside the engine, so a
    /// deployment is sized for the sum and the two have to stay
    /// separately nameable.
    #[test]
    fn the_engine_ceiling_and_the_cube_cache_are_separate_numbers() {
        let default = read(&[], &[]).expect("readable");
        assert_eq!(
            default.memory_limit_mb,
            glossql_session::DEFAULT_MEMORY_LIMIT_MB
        );
        assert_eq!(
            default.cube_cache_mb,
            glossql_session::DEFAULT_CUBE_CACHE_MB
        );

        let set = read(&["--memory-limit", "512"], &[]).expect("readable");
        assert_eq!(set.memory_limit_mb, 512);
        assert_eq!(
            set.cube_cache_mb,
            glossql_session::DEFAULT_CUBE_CACHE_MB,
            "naming one budget must not move the other"
        );
        assert_eq!(
            set.spill_limit_mb, None,
            "the disk follows the ceiling until it is named"
        );
    }

    /// A sizing flag's variable is the flag's name; the flag wins over
    /// it, and a number that does not parse refuses by the name it was
    /// given under.
    #[test]
    fn a_flag_has_a_variable_and_wins_over_it() {
        let named = read(
            &[],
            &[
                ("GLOSSQL_ADDR", "0.0.0.0:8080"),
                ("GLOSSQL_ROW_CAP", "50"),
                ("GLOSSQL_CUBE_CACHE", "64"),
                ("GLOSSQL_MEMORY_LIMIT", "512"),
                ("GLOSSQL_SPILL_LIMIT", "1024"),
            ],
        )
        .expect("readable");
        assert_eq!(named.addr, "0.0.0.0:8080");
        assert_eq!(named.audience, "http://0.0.0.0:8080");
        assert_eq!(named.row_cap, 50);
        assert_eq!(
            (
                named.cube_cache_mb,
                named.memory_limit_mb,
                named.spill_limit_mb
            ),
            (64, 512, Some(1024))
        );

        let typed = read(
            &["--memory-limit", "256", "--addr", "127.0.0.1:9000"],
            &[
                ("GLOSSQL_MEMORY_LIMIT", "512"),
                ("GLOSSQL_ADDR", "0.0.0.0:8080"),
            ],
        )
        .expect("readable");
        assert_eq!(typed.memory_limit_mb, 256);
        assert_eq!(typed.addr, "127.0.0.1:9000");

        let bad = refusal(read(&[], &[("GLOSSQL_MEMORY_LIMIT", "4g")]));
        assert!(bad.contains("GLOSSQL_MEMORY_LIMIT"), "{bad}");
    }
}
