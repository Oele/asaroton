//! Server configuration (TOML file plus environment overrides).

use std::{net::SocketAddr, path::PathBuf};

use ruma::{OwnedServerName, RoomVersionId};
use serde::Deserialize;

/// Top-level configuration.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// The server name, e.g. `example.org`. Appears in all user, room and event IDs.
    pub server_name: OwnedServerName,
    /// PostgreSQL connection URL.
    pub database_url: String,
    /// Maximum database connections.
    #[serde(default = "default_db_connections")]
    pub database_max_connections: u32,
    /// Where to store server signing keys and media. Created if missing.
    #[serde(default = "default_data_dir")]
    pub data_dir: PathBuf,
    #[serde(default)]
    pub listeners: Listeners,
    #[serde(default)]
    pub federation: FederationConfig,
    #[serde(default)]
    pub registration: RegistrationConfig,
    #[serde(default)]
    pub rate_limiting: RateLimitingConfig,
    #[serde(default)]
    pub rooms: RoomsConfig,
    #[serde(default)]
    pub logging: LoggingConfig,
    /// Admin API tokens etc. (reserved for later phases).
    #[serde(default)]
    #[allow(dead_code)]
    pub admins: Vec<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Listeners {
    #[serde(default = "default_client_listener")]
    pub client: Listener,
    #[serde(default = "default_federation_listener")]
    pub federation: Listener,
    /// Prometheus metrics listener; disabled when absent.
    #[serde(default)]
    #[allow(dead_code)]
    pub metrics: Option<Listener>,
}

impl Default for Listeners {
    fn default() -> Self {
        Self {
            client: default_client_listener(),
            federation: default_federation_listener(),
            metrics: None,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Listener {
    pub bind: SocketAddr,
    /// PEM certificate chain; enables TLS when set together with `tls_key`.
    #[serde(default)]
    pub tls_cert: Option<PathBuf>,
    #[serde(default)]
    pub tls_key: Option<PathBuf>,
    /// Set to false to disable this listener entirely.
    #[serde(default = "default_true")]
    pub enabled: bool,
}

impl Listener {
    pub fn tls(&self) -> Option<(&PathBuf, &PathBuf)> {
        self.tls_cert.as_ref().zip(self.tls_key.as_ref())
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(dead_code)]
pub struct FederationConfig {
    /// Disable all federation (the server becomes an island).
    #[serde(default)]
    pub disabled: bool,
    /// Additional CA certificates to trust for outbound federation (PEM files).
    #[serde(default)]
    pub extra_ca_certs: Vec<PathBuf>,
    /// Servers to use as key notaries (defaults to matrix.org).
    #[serde(default)]
    pub trusted_key_servers: Vec<OwnedServerName>,
    /// Allow federation over plain HTTP to these servers (testing only).
    #[serde(default)]
    pub insecure_http_servers: Vec<OwnedServerName>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegistrationConfig {
    /// Allow open registration.
    #[serde(default)]
    pub enabled: bool,
    /// Shared secret for the Synapse-compatible admin registration endpoint.
    #[serde(default)]
    pub shared_secret: Option<String>,
    /// Require a registration token (MSC3231) when `enabled`.
    #[serde(default)]
    pub require_token: bool,
    /// Allow guest accounts.
    #[serde(default)]
    pub allow_guests: bool,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RateLimitingConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
}

impl Default for RateLimitingConfig {
    fn default() -> Self {
        Self { enabled: true }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoomsConfig {
    /// Room version for new rooms when the client does not choose.
    #[serde(default = "default_room_version")]
    pub default_version: RoomVersionId,
}

impl Default for RoomsConfig {
    fn default() -> Self {
        Self {
            default_version: default_room_version(),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LoggingConfig {
    /// `tracing` filter directive, e.g. `info,tessera=debug`. `RUST_LOG` overrides.
    #[serde(default = "default_log_filter")]
    pub filter: String,
    #[serde(default)]
    pub json: bool,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            filter: default_log_filter(),
            json: false,
        }
    }
}

fn default_true() -> bool {
    true
}
fn default_db_connections() -> u32 {
    16
}
fn default_data_dir() -> PathBuf {
    PathBuf::from("./data")
}
fn default_client_listener() -> Listener {
    Listener {
        bind: "127.0.0.1:8008".parse().unwrap_or_else(|_| unreachable!()),
        tls_cert: None,
        tls_key: None,
        enabled: true,
    }
}
fn default_federation_listener() -> Listener {
    Listener {
        bind: "127.0.0.1:8448".parse().unwrap_or_else(|_| unreachable!()),
        tls_cert: None,
        tls_key: None,
        enabled: true,
    }
}
fn default_room_version() -> RoomVersionId {
    tessera_core::DEFAULT_ROOM_VERSION
}
fn default_log_filter() -> String {
    "info,tessera=debug".to_owned()
}

impl Config {
    /// Load from a TOML file.
    pub fn load(path: &std::path::Path) -> anyhow::Result<Self> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("reading config {}: {e}", path.display()))?;
        let cfg: Self = toml::from_str(&text)
            .map_err(|e| anyhow::anyhow!("parsing config {}: {e}", path.display()))?;
        Ok(cfg)
    }
}
