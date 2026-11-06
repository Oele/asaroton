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
    /// The URL clients reach the server at (`https://matrix.example.org`), announced in
    /// `.well-known/matrix/client` and login responses. Needed behind a reverse proxy; defaults
    /// to the client listener's own address.
    #[serde(default)]
    pub public_base_url: Option<String>,
    #[serde(default)]
    pub listeners: Listeners,
    #[serde(default)]
    pub federation: FederationConfig,
    #[serde(default)]
    pub registration: RegistrationConfig,
    #[serde(default)]
    pub rate_limiting: RateLimitingConfig,
    /// Server notices (admin messages delivered in a dedicated room).
    #[serde(default)]
    pub server_notices: ServerNoticesConfig,
    /// Application services.
    #[serde(default)]
    pub appservices: AppservicesConfig,
    #[serde(default)]
    pub rooms: RoomsConfig,
    /// Delegate authentication to Matrix Authentication Service (MSC3861).
    #[serde(default)]
    pub mas: MasConfig,
    /// Message retention (purging old history).
    #[serde(default)]
    pub retention: RetentionConfig,
    /// Message search.
    #[serde(default)]
    pub search: SearchConfig,
    /// Voice and video calls (`MatrixRTC` over `LiveKit`).
    #[serde(default)]
    pub rtc: RtcConfig,
    #[serde(default)]
    pub logging: LoggingConfig,
    /// User IDs made server admins at startup (they must exist).
    #[serde(default)]
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
    /// Serve `.well-known/matrix/server` pointing other servers at this host and port
    /// (`matrix.example.org:443`), so federation can run through a reverse proxy on 443.
    #[serde(default)]
    pub well_known_server: Option<String>,
    /// Allow federation over plain HTTP to these servers (testing only).
    #[serde(default)]
    pub insecure_http_servers: Vec<OwnedServerName>,
    /// Skip TLS certificate verification for federation (testing only).
    #[serde(default)]
    pub danger_accept_invalid_certs: bool,
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
    /// Take the client address from `X-Forwarded-For` (only behind a trusted reverse proxy).
    #[serde(default)]
    pub trust_x_forwarded_for: bool,
    /// Per client address.
    #[serde(default = "default_rate_logins")]
    pub logins: RateConfig,
    /// Per client address.
    #[serde(default = "default_rate_registrations")]
    pub registrations: RateConfig,
    /// Per user: sending events.
    #[serde(default = "default_rate_messages")]
    pub messages: RateConfig,
    /// Per user: joining rooms.
    #[serde(default = "default_rate_joins")]
    pub joins: RateConfig,
    /// Per user: inviting.
    #[serde(default = "default_rate_invites")]
    pub invites: RateConfig,
}

/// A token bucket: `burst` requests at once, then `per_second`.
#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RateConfig {
    pub per_second: f64,
    pub burst: u32,
}

fn default_rate_logins() -> RateConfig {
    RateConfig {
        per_second: 0.17,
        burst: 5,
    }
}
fn default_rate_registrations() -> RateConfig {
    RateConfig {
        per_second: 0.17,
        burst: 3,
    }
}
fn default_rate_messages() -> RateConfig {
    RateConfig {
        per_second: 0.2,
        burst: 10,
    }
}
fn default_rate_joins() -> RateConfig {
    RateConfig {
        per_second: 0.1,
        burst: 10,
    }
}
fn default_rate_invites() -> RateConfig {
    RateConfig {
        per_second: 0.3,
        burst: 10,
    }
}

impl Default for RateLimitingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            trust_x_forwarded_for: false,
            logins: default_rate_logins(),
            registrations: default_rate_registrations(),
            messages: default_rate_messages(),
            joins: default_rate_joins(),
            invites: default_rate_invites(),
        }
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

/// Voice and video calls: the `LiveKit` SFU this server offers (MSC4195). With a `[rtc.livekit]`
/// section the server issues the SFU tokens itself; no separate JWT service is needed.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RtcConfig {
    #[serde(default)]
    pub livekit: Option<LivekitConfig>,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LivekitConfig {
    /// The WebSocket URL clients connect to. Leave it out and clients connect to asaroton, which
    /// forwards the signalling to the SFU on `sfu.bind:sfu.port`; set it when the SFU's
    /// WebSocket is exposed some other way (`wss://livekit.example.org`).
    #[serde(default)]
    pub url: Option<String>,
    /// The HTTP URL of the SFU's API, for creating rooms and removing participants. Defaults
    /// to `url` with the scheme changed to http(s), or to `sfu.bind:sfu.port`.
    #[serde(default)]
    pub api_url: Option<String>,
    /// An API key and secret from `LiveKit`'s `keys` configuration.
    pub api_key: String,
    pub api_secret: String,
    /// Let users of other homeservers publish media on this SFU (they can always subscribe).
    #[serde(default = "default_true")]
    pub remote_can_publish: bool,
    /// How `livekit-server` itself should listen; only used by `asaroton livekit-config`.
    #[serde(default)]
    pub sfu: SfuConfig,
    /// TURN over TLS for clients on networks that block UDP; only used by
    /// `asaroton livekit-config`.
    #[serde(default)]
    pub turn: Option<TurnConfig>,
}

impl std::fmt::Debug for LivekitConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LivekitConfig")
            .field("url", &self.url)
            .field("api_url", &self.api_url)
            .field("api_key", &self.api_key)
            .field("remote_can_publish", &self.remote_can_publish)
            .field("sfu", &self.sfu)
            .field("turn", &self.turn)
            .finish_non_exhaustive()
    }
}

/// The SFU's own listeners, as written into the generated `livekit.yaml`.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SfuConfig {
    /// Address of the signalling (WebSocket and API) listener; the reverse proxy forwards
    /// `url` to it. `0.0.0.0` to listen on every interface.
    #[serde(default = "default_sfu_bind")]
    pub bind: String,
    #[serde(default = "default_sfu_port")]
    pub port: u16,
    /// Media over TCP, for clients whose UDP is blocked (open it in the firewall).
    #[serde(default = "default_sfu_tcp_port")]
    pub tcp_port: u16,
    /// Media over UDP, `[first, last]` (open the range in the firewall).
    #[serde(default = "default_udp_ports")]
    pub udp_ports: (u16, u16),
    /// Let the SFU discover its public address (needed behind NAT / cloud 1:1 NAT).
    #[serde(default = "default_true")]
    pub external_ip: bool,
}

impl Default for SfuConfig {
    fn default() -> Self {
        Self {
            bind: default_sfu_bind(),
            port: default_sfu_port(),
            tcp_port: default_sfu_tcp_port(),
            udp_ports: default_udp_ports(),
            external_ip: true,
        }
    }
}

/// TURN over TLS served by `livekit-server` itself. Clients that cannot use UDP (VPNs, strict
/// firewalls) relay their media through it, over a TLS connection to `domain:tls_port`, and
/// connect right away instead of waiting for the UDP attempts to time out.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TurnConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Host name clients connect to; the certificate must be valid for it. Defaults to the host
    /// of `url`.
    #[serde(default)]
    pub domain: Option<String>,
    /// Where `livekit-server` listens for TURN over TLS. `LiveKit` tells clients to use port 443
    /// whatever this says; asaroton's signalling proxy corrects that, so any free port works
    /// (443 itself still reaches the most restrictive networks). With an explicit `url` the
    /// correction cannot happen and this must be 443.
    #[serde(default = "default_turn_tls_port")]
    pub tls_port: u16,
    /// Certificate chain and key, readable by the user `livekit-server` runs as.
    #[serde(default = "default_turn_cert")]
    pub cert_file: PathBuf,
    #[serde(default = "default_turn_key")]
    pub key_file: PathBuf,
    /// Private addresses the relay may forward to. The relay only ever needs to reach the SFU
    /// itself, and `LiveKit` refuses private, loopback and link-local peers unless listed here;
    /// so on a host behind NAT (where the SFU's own address is private) put that address here,
    /// as `10.0.0.5/32`. A host with its public address on an interface needs nothing.
    #[serde(default)]
    pub allow_peer_cidrs: Vec<String>,
}

impl LivekitConfig {
    /// The host name of a URL, without scheme, port or path.
    pub fn host_of(url: &str) -> &str {
        let rest = url.split_once("://").map_or(url, |(_, rest)| rest);
        let host = rest.split('/').next().unwrap_or(rest);
        host.rsplit_once(':')
            .filter(|(_, port)| port.chars().all(|c| c.is_ascii_digit()))
            .map_or(host, |(h, _)| h)
    }

    /// The API URL: explicit, the WebSocket URL over HTTP, or the SFU's own listener.
    pub fn api_url(&self) -> String {
        match (&self.api_url, &self.url) {
            (Some(u), _) => u.trim_end_matches('/').to_owned(),
            (None, Some(url)) => {
                let url = url.trim_end_matches('/');
                url.strip_prefix("wss://")
                    .map(|rest| format!("https://{rest}"))
                    .or_else(|| {
                        url.strip_prefix("ws://")
                            .map(|rest| format!("http://{rest}"))
                    })
                    .unwrap_or_else(|| url.to_owned())
            }
            (None, None) => {
                let host = match self.sfu.bind.as_str() {
                    "0.0.0.0" | "::" => "127.0.0.1",
                    other => other,
                };
                format!("http://{host}:{}", self.sfu.port)
            }
        }
    }

    /// The port asaroton's signalling proxy writes into the SFU's `turns:` URLs: the TURN port
    /// when it is not the 443 `LiveKit` claims, and only when clients go through the proxy.
    pub fn advertised_turn_port(&self) -> Option<u16> {
        self.url.is_none().then_some(()).and(
            self.turn
                .as_ref()
                .filter(|t| t.enabled && t.tls_port != 443)
                .map(|t| t.tls_port),
        )
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LoggingConfig {
    /// `tracing` filter directive, e.g. `info,asaroton=debug`. `RUST_LOG` overrides.
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
fn default_sfu_bind() -> String {
    "127.0.0.1".to_owned()
}
fn default_sfu_port() -> u16 {
    7880
}
fn default_sfu_tcp_port() -> u16 {
    7881
}
fn default_udp_ports() -> (u16, u16) {
    (50000, 60000)
}
fn default_turn_tls_port() -> u16 {
    5349
}
fn default_turn_cert() -> PathBuf {
    PathBuf::from("/etc/livekit/tls/fullchain.pem")
}
fn default_turn_key() -> PathBuf {
    PathBuf::from("/etc/livekit/tls/privkey.pem")
}
fn default_db_connections() -> u32 {
    32
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
    asaroton_core::DEFAULT_ROOM_VERSION
}
fn default_log_filter() -> String {
    // ruma's state resolution logs every auth check at info level.
    "info,asaroton=debug,ruma_state_res=warn".to_owned()
}

impl Config {
    /// Load from a TOML file.
    /// The URL clients reach this server at: `public_base_url`, or derived from the client
    /// listener.
    pub fn public_base_url(&self) -> String {
        match &self.public_base_url {
            Some(url) => url.trim_end_matches('/').to_owned(),
            None if self.listeners.client.tls().is_some() => {
                format!("https://{}", self.server_name)
            }
            None => format!("http://{}", self.listeners.client.bind),
        }
    }

    pub fn load(path: &std::path::Path) -> anyhow::Result<Self> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("reading config {}: {e}", path.display()))?;
        let cfg: Self = toml::from_str(&text)
            .map_err(|e| anyhow::anyhow!("parsing config {}: {e}", path.display()))?;
        Ok(cfg)
    }
}

/// Server notices settings.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerNoticesConfig {
    #[serde(default)]
    pub enabled: bool,
    /// Localpart of the user that sends notices.
    #[serde(default = "default_notices_localpart")]
    pub user_localpart: String,
    /// Display name of that user and of the notices rooms.
    #[serde(default = "default_notices_name")]
    pub display_name: String,
}

impl Default for ServerNoticesConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            user_localpart: default_notices_localpart(),
            display_name: default_notices_name(),
        }
    }
}

fn default_notices_localpart() -> String {
    "server".to_owned()
}

fn default_notices_name() -> String {
    "Server Notices".to_owned()
}

/// Matrix Authentication Service integration. When enabled, logins, registration and
/// password changes are handled by the service; this server validates its tokens.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MasConfig {
    #[serde(default)]
    pub enabled: bool,
    /// Base URL of the service, as reachable from this server.
    #[serde(default = "default_mas_endpoint")]
    pub endpoint: String,
    /// Shared secret (`matrix.secret` in the service's configuration).
    pub secret: Option<String>,
    /// File holding the shared secret, as an alternative to `secret`.
    pub secret_file: Option<PathBuf>,
}

/// Message retention: rooms with an `m.room.retention` policy (and, with
/// `default_max_lifetime`, every room) lose old non-state events.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetentionConfig {
    #[serde(default)]
    pub enabled: bool,
    /// How often to look for events to purge, in seconds (default one hour).
    #[serde(default = "default_retention_interval_secs")]
    pub interval_secs: u64,
    /// Lifetime applied to rooms without a policy, in milliseconds.
    pub default_max_lifetime_ms: Option<i64>,
}

fn default_retention_interval_secs() -> u64 {
    3600
}

/// How `/search` matches text. `word` is PostgreSQL full-text search on English lexemes;
/// `substring` matches case-insensitive substrings of the plain text (stored from then on;
/// `asaroton search-backfill` fills in older messages); `both` is their union.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SearchConfig {
    #[serde(default)]
    pub mode: SearchMode,
    /// Substring terms shorter than this many characters are ignored (default 3, the
    /// trigram index's grain).
    #[serde(default = "default_search_min_length")]
    pub substring_min_length: usize,
}

impl Default for SearchConfig {
    fn default() -> Self {
        Self {
            mode: SearchMode::Word,
            substring_min_length: default_search_min_length(),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SearchMode {
    #[default]
    Word,
    Substring,
    Both,
}

fn default_search_min_length() -> usize {
    3
}

fn default_mas_endpoint() -> String {
    "http://localhost:8080".to_owned()
}

/// Where application service registration files live.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppservicesConfig {
    /// Individual registration files (YAML).
    #[serde(default)]
    pub registration_files: Vec<PathBuf>,
    /// A directory whose `*.yaml`/`*.yml` files are registrations; missing is fine.
    #[serde(default)]
    pub registration_dir: Option<PathBuf>,
}
