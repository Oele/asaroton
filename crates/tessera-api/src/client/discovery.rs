//! `/versions`, `/capabilities`, `/.well-known/matrix/client`.

use std::collections::BTreeMap;

use axum::{Json, extract::State};
use ruma::{
    RoomVersionId,
    api::client::discovery::{
        get_capabilities::{
            self,
            v3::{Capabilities, RoomVersionStability, RoomVersionsCapability},
        },
        get_supported_versions,
    },
};

use crate::{AppState, Result, Ruma};

/// Spec versions this server implements. Kept in sync with `docs/spec-coverage.md`.
pub const SUPPORTED_VERSIONS: &[&str] = &[
    "v1.1", "v1.2", "v1.3", "v1.4", "v1.5", "v1.6", "v1.7", "v1.8", "v1.9", "v1.10", "v1.11",
    "v1.12", "v1.13", "v1.14", "v1.15", "v1.16", "v1.17", "v1.18", "v1.19",
];

pub async fn versions(
    services: AppState,
    _req: Ruma<get_supported_versions::Request>,
) -> Result<get_supported_versions::Response> {
    let mut resp = get_supported_versions::Response::new(
        SUPPORTED_VERSIONS.iter().map(|v| (*v).to_owned()).collect(),
    );
    resp.unstable_features = BTreeMap::from([
        // Simplified Sliding Sync, advertised once implemented (Phase 5).
        ("org.matrix.simplified_msc3575".to_owned(), false),
    ]);
    let _ = &services;
    Ok(resp)
}

pub async fn capabilities(
    services: AppState,
    _req: Ruma<get_capabilities::v3::Request>,
) -> Result<get_capabilities::v3::Response> {
    let mut caps = Capabilities::new();
    let mut available: BTreeMap<RoomVersionId, RoomVersionStability> = BTreeMap::new();
    for v in &tessera_core::SUPPORTED_ROOM_VERSIONS {
        available.insert(v.clone(), RoomVersionStability::Stable);
    }
    caps.room_versions =
        RoomVersionsCapability::new(services.config.default_room_version.clone(), available);
    caps.change_password.enabled = true;
    caps.thirdparty_id_changes.enabled = false;
    caps.profile_fields = Some(get_capabilities::v3::ProfileFieldsCapability::new(true));
    // Older clients still read the pre-1.16 capabilities.
    #[allow(deprecated)]
    {
        caps.set_displayname.enabled = true;
        caps.set_avatar_url.enabled = true;
    }
    Ok(get_capabilities::v3::Response::new(caps))
}

pub async fn well_known_client(State(services): State<AppState>) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "m.homeserver": { "base_url": services.config.public_base_url }
    }))
}
