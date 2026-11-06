//! Synapse-compatible shared-secret registration (`/_synapse/admin/v1/register`).
//!
//! Deployment tooling and Complement use this to create accounts (including admins) without
//! open registration. The request carries an HMAC-SHA1 over the parameters keyed with the
//! configured shared secret.

use asaroton_service::account::Registration;
use axum::{Json, extract::State, response::IntoResponse};
use hmac::{Hmac, Mac};
use http::StatusCode;
use serde::Deserialize;
use serde_json::json;
use sha1::Sha1;

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

/// `POST /_synapse/admin/v1/send_server_notice` and `PUT .../{txn_id}`.
pub async fn send_server_notice(
    State(services): State<AppState>,
    txn_id: Option<axum::extract::Path<String>>,
    req: axum::extract::Request,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    match send_server_notice_inner(&services, txn_id.map(|p| p.0), req).await {
        Ok(event_id) => {
            let mut resp = axum::Json(serde_json::json!({ "event_id": event_id })).into_response();
            crate::extract::add_cors(resp.headers_mut());
            resp
        }
        Err(e) => e.into_response(),
    }
}

async fn send_server_notice_inner(
    services: &AppState,
    txn_id: Option<String>,
    req: axum::extract::Request,
) -> crate::Result<ruma::OwnedEventId> {
    let token = req
        .headers()
        .get(http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .map(|s| s.trim().to_owned())
        .ok_or_else(crate::Error::missing_token)?;
    let sender = services
        .auth
        .authenticate(&token)
        .await
        .map_err(|_| crate::Error::forbidden("Invalid access token."))?;
    let is_admin = services
        .store
        .get_user(&sender.user_id)
        .await?
        .is_some_and(|u| u.is_admin);
    if !is_admin {
        return Err(crate::Error::forbidden("You are not a server admin."));
    }
    let bytes = axum::body::to_bytes(req.into_body(), 1024 * 1024)
        .await
        .map_err(|e| crate::Error::bad_json(e.to_string()))?;
    let body: serde_json::Value =
        serde_json::from_slice(&bytes).map_err(|e| crate::Error::bad_json(e.to_string()))?;
    let user_id = body
        .get("user_id")
        .and_then(serde_json::Value::as_str)
        .and_then(|u| ruma::OwnedUserId::try_from(u).ok())
        .ok_or_else(|| crate::Error::invalid_param("Missing or invalid user_id."))?;
    let content = body
        .get("content")
        .cloned()
        .filter(serde_json::Value::is_object)
        .ok_or_else(|| crate::Error::invalid_param("Missing content."))?;
    let event_type = body
        .get("type")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("m.room.message");
    services
        .send_server_notice(&user_id, event_type, content, txn_id.as_deref())
        .await
        .map_err(Into::into)
}

/// Whether `mac` is the HMAC-SHA1 of `parts` (joined by NUL bytes, after the nonce) under the
/// registration shared secret, for a nonce handed out by `/_synapse/admin/v1/register` that
/// has not been used. This is how a tool run on the server's own host identifies itself
/// without an account: the secret is in the configuration file, which only the operator can
/// read. (Whoever holds it can register an admin account, so it grants nothing new here.)
pub(crate) fn operator_mac_is_valid(
    services: &AppState,
    nonce: &str,
    parts: &[&str],
    mac: &str,
) -> bool {
    let Some(secret) = services.config.registration_shared_secret.as_deref() else {
        return false;
    };
    if !services.take_admin_nonce(nonce) {
        return false;
    }
    let Ok(mut expected) = Hmac::<Sha1>::new_from_slice(secret.as_bytes()) else {
        return false;
    };
    expected.update(nonce.as_bytes());
    for part in parts {
        expected.update(b"\0");
        expected.update(part.as_bytes());
    }
    let expected = hex::encode(expected.finalize().into_bytes());
    constant_time_eq(expected.as_bytes(), mac.to_lowercase().as_bytes())
}
