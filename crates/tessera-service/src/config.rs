//! Service-level configuration, decoupled from the on-disk format.

use ruma::{OwnedServerName, RoomVersionId};

/// What the services need to know. The binary builds this from its TOML config.
#[derive(Clone, Debug)]
pub struct Config {
    pub server_name: OwnedServerName,
    pub default_room_version: RoomVersionId,
    pub registration_enabled: bool,
    pub registration_shared_secret: Option<String>,
    pub registration_requires_token: bool,
    pub allow_guests: bool,
    pub federation_disabled: bool,
    pub trusted_key_servers: Vec<OwnedServerName>,
    pub rate_limits: RateLimits,
    /// Users made server admins at startup.
    pub admins: Vec<ruma::OwnedUserId>,
    /// Public base URL for the client API, used in well-known and login responses.
    pub public_base_url: String,
    /// Software version string reported in `/versions`.
    pub software_version: String,
    /// Directory for media files.
    pub media_dir: std::path::PathBuf,
    /// Server notices, when enabled.
    pub server_notices: Option<ServerNotices>,
    /// Application service registrations.
    pub appservices: Vec<crate::appservice::Registration>,
    /// Delegated authentication (Matrix Authentication Service), when enabled.
    pub mas: Option<MasSettings>,
}

/// Matrix Authentication Service connection.
#[derive(Clone, Debug)]
pub struct MasSettings {
    /// Base URL where the service can be reached.
    pub endpoint: String,
    /// Shared secret: we present it for introspection, the service presents it to us.
    pub secret: String,
}

/// Server notices settings.
#[derive(Clone, Debug)]
pub struct ServerNotices {
    pub user_localpart: String,
    pub display_name: String,
}

/// A token bucket: `burst` requests at once, refilled at `per_second`.
#[derive(Clone, Copy, Debug)]
pub struct Rate {
    pub per_second: f64,
    pub burst: u32,
}

/// Request rate limits by class of request.
#[derive(Clone, Debug)]
pub struct RateLimits {
    pub enabled: bool,
    /// Take the client address from `X-Forwarded-For` (behind a trusted reverse proxy).
    pub trust_x_forwarded_for: bool,
    pub logins: Rate,
    pub registrations: Rate,
    pub messages: Rate,
    pub joins: Rate,
    pub invites: Rate,
}

impl Default for RateLimits {
    fn default() -> Self {
        Self {
            enabled: true,
            trust_x_forwarded_for: false,
            logins: Rate {
                per_second: 0.17,
                burst: 5,
            },
            registrations: Rate {
                per_second: 0.17,
                burst: 3,
            },
            messages: Rate {
                per_second: 0.2,
                burst: 10,
            },
            joins: Rate {
                per_second: 0.1,
                burst: 10,
            },
            invites: Rate {
                per_second: 0.3,
                burst: 10,
            },
        }
    }
}
