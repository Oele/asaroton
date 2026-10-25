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
    pub rate_limiting: bool,
    /// Public base URL for the client API, used in well-known and login responses.
    pub public_base_url: String,
    /// Software version string reported in `/versions`.
    pub software_version: String,
    /// Directory for media files.
    pub media_dir: std::path::PathBuf,
    /// Server notices, when enabled.
    pub server_notices: Option<ServerNotices>,
}

/// Server notices settings.
#[derive(Clone, Debug)]
pub struct ServerNotices {
    pub user_localpart: String,
    pub display_name: String,
}
