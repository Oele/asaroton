//! `/versions`, `/capabilities`, `/.well-known/matrix/client`.

use std::collections::BTreeMap;

use axum::{Json, extract::State};
use ruma::api::client::discovery::{get_capabilities, get_supported_versions};

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
        // Simplified Sliding Sync (MSC4186).
        ("org.matrix.simplified_msc3575".to_owned(), true),
    ]);
    let _ = &services;
    Ok(resp)
}

pub fn capabilities(
    services: &AppState,
    req: &Ruma<get_capabilities::v3::Request>,
) -> Result<axum::response::Response> {
    use axum::response::IntoResponse;
    let _ = req.sender()?;
    let mut available = serde_json::Map::new();
    for v in &tessera_core::SUPPORTED_ROOM_VERSIONS {
        available.insert(v.to_string(), serde_json::json!("stable"));
    }
    let body = serde_json::json!({
        "capabilities": {
            "m.room_versions": {
                "default": services.config.default_room_version,
                "available": available,
            },
            "m.change_password": { "enabled": true },
            "m.set_displayname": { "enabled": true },
            "m.set_avatar_url": { "enabled": true },
            "m.3pid_changes": { "enabled": false },
            "m.get_login_token": { "enabled": false },
            "m.profile_fields": { "enabled": true },
        }
    });
    let mut resp = axum::Json(body).into_response();
    crate::extract::add_cors(resp.headers_mut());
    Ok(resp)
}

pub async fn well_known_client(State(services): State<AppState>) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "m.homeserver": { "base_url": services.config.public_base_url }
    }))
}
