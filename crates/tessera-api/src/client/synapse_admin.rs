//! Synapse-compatible shared-secret registration (`/_synapse/admin/v1/register`).
//!
//! Deployment tooling and Complement use this to create accounts (including admins) without
//! open registration. The request carries an HMAC-SHA1 over the parameters keyed with the
//! configured shared secret.

use axum::{Json, extract::State, response::IntoResponse};
use hmac::{Hmac, Mac};
use http::StatusCode;
use serde::Deserialize;
use serde_json::json;
use sha1::Sha1;
use tessera_service::account::Registration;

use crate::{AppState, Error};

#[derive(Debug, Deserialize)]
pub struct SharedSecretBody {
    nonce: String,
    username: String,
    password: String,
    #[serde(default)]
    admin: bool,
    mac: String,
    #[serde(default)]
    displayname: Option<String>,
    #[serde(default)]
    user_type: Option<String>,
}

pub async fn get_nonce(State(services): State<AppState>) -> axum::response::Response {
    if services.config.registration_shared_secret.is_none() {
        return Error::not_found("Shared secret registration is not enabled.").into_response();
    }
    let nonce = services.new_admin_nonce();
    Json(json!({ "nonce": nonce })).into_response()
}

pub async fn register(
    State(services): State<AppState>,
    body: Option<Json<SharedSecretBody>>,
) -> axum::response::Response {
    match register_inner(&services, body).await {
        Ok(v) => Json(v).into_response(),
        Err(e) => e.into_response(),
    }
}

async fn register_inner(
    services: &AppState,
    body: Option<Json<SharedSecretBody>>,
) -> Result<serde_json::Value, Error> {
    let Some(secret) = services.config.registration_shared_secret.clone() else {
        return Err(Error::not_found(
            "Shared secret registration is not enabled.",
        ));
    };
    let Some(Json(body)) = body else {
        return Err(Error::bad_json("Invalid request body."));
    };
    if !services.take_admin_nonce(&body.nonce) {
        return Err(Error::matrix(
            ruma::api::error::ErrorKind::Unknown,
            StatusCode::BAD_REQUEST,
            "unrecognised nonce",
        ));
    }

    let mut mac = Hmac::<Sha1>::new_from_slice(secret.as_bytes())
        .map_err(|e| Error::internal(e.to_string()))?;
    mac.update(body.nonce.as_bytes());
    mac.update(b"\0");
    mac.update(body.username.as_bytes());
    mac.update(b"\0");
    mac.update(body.password.as_bytes());
    mac.update(b"\0");
    mac.update(if body.admin { b"admin" } else { b"notadmin" });
    if let Some(ut) = &body.user_type {
        mac.update(b"\0");
        mac.update(ut.as_bytes());
    }
    let expected = hex::encode(mac.finalize().into_bytes());
    if !constant_time_eq(expected.as_bytes(), body.mac.to_lowercase().as_bytes()) {
        return Err(Error::matrix(
            ruma::api::error::ErrorKind::Forbidden,
            StatusCode::FORBIDDEN,
            "HMAC incorrect",
        ));
    }

    let localpart = body.username.to_lowercase();
    let user = services
        .register(Registration {
            localpart: Some(localpart),
            password: Some(body.password.clone()),
            display_name: body.displayname.clone(),
            is_guest: false,
            is_admin: body.admin,
            appservice_id: None,
        })
        .await?;
    let session = services
        .create_session(&user.user_id, None, None, false)
        .await?;
    Ok(json!({
        "user_id": session.user_id,
        "access_token": session.access_token,
        "device_id": session.device_id,
        "home_server": services.server_name(),
    }))
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}
