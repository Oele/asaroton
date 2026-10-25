//! Matrix Authentication Service integration: the `/_synapse/mas/*` management API the
//! service calls (authenticated with the shared secret), and the client-facing discovery of
//! the service (`/auth_metadata`).

use std::{sync::Arc, time::Instant};

use axum::{
    Json,
    extract::{Query, State},
    http::{HeaderMap, StatusCode, header},
    response::IntoResponse,
};
use ruma::{OwnedDeviceId, OwnedUserId, UserId};
use serde::Deserialize;
use serde_json::{Value, json};
use tessera_auth::mas::MasClient;
use tessera_service::account::Registration;

use crate::{AppState, Error, Result};

type Reply = Result<(StatusCode, Value)>;

fn respond(r: Reply) -> axum::response::Response {
    match r {
        Ok((status, v)) => (status, Json(v)).into_response(),
        Err(e) => e.into_response(),
    }
}

/// Only the authentication service may call the management API.
fn require_mas(services: &AppState, headers: &HeaderMap) -> Result<Arc<MasClient>> {
    let mas = services
        .mas
        .clone()
        .ok_or_else(|| Error::not_found("Delegated authentication is not enabled."))?;
    let token = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or("");
    if !mas.is_shared_secret(token) {
        return Err(Error::forbidden("This endpoint must only be called by MAS"));
    }
    Ok(mas)
}

fn user_for(services: &AppState, localpart: &str) -> Result<OwnedUserId> {
    UserId::parse_with_server_name(localpart, &services.config.server_name)
        .map_err(|_| Error::invalid_param("Invalid localpart."))
}

async fn existing_user(services: &AppState, localpart: &str) -> Result<tessera_store::User> {
    let id = user_for(services, localpart)?;
    services
        .store
        .get_user(&id)
        .await?
        .ok_or_else(|| Error::not_found("User not found"))
}

fn body_str<'a>(body: &'a Value, key: &str) -> Result<&'a str> {
    body.get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| Error::bad_json(format!("Missing or invalid `{key}`.")))
}

#[derive(Debug, Deserialize)]
pub struct LocalpartQuery {
    localpart: String,
}

/// `GET /_synapse/mas/query_user?localpart=`
pub async fn query_user(
    State(services): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<LocalpartQuery>,
) -> axum::response::Response {
    respond(
        async {
            require_mas(&services, &headers)?;
            let u = existing_user(&services, &q.localpart).await?;
            Ok((
                StatusCode::OK,
                json!({
                    "user_id": u.user_id,
                    "display_name": u.display_name,
                    "avatar_url": u.avatar_url,
                    "is_suspended": false,
                    "is_deactivated": u.is_deactivated,
                }),
            ))
        }
        .await,
    )
}

/// `GET /_synapse/mas/is_localpart_available?localpart=`
pub async fn is_localpart_available(
    State(services): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<LocalpartQuery>,
) -> axum::response::Response {
    respond(
        async {
            require_mas(&services, &headers)?;
            let id = services.validate_localpart(&q.localpart)?;
            if services.store.user_exists(&id).await? {
                return Err(Error::matrix(
                    ruma::api::error::ErrorKind::UserInUse,
                    StatusCode::BAD_REQUEST,
                    "User ID already taken.",
                ));
            }
            Ok((StatusCode::OK, json!({})))
        }
        .await,
    )
}

/// `POST /_synapse/mas/provision_user`
pub async fn provision_user(
    State(services): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> axum::response::Response {
    respond(
        async {
            require_mas(&services, &headers)?;
            let localpart = body_str(&body, "localpart")?;
            for (unset, set) in [
                ("unset_displayname", "set_displayname"),
                ("unset_avatar_url", "set_avatar_url"),
                ("unset_emails", "set_emails"),
            ] {
                if body.get(unset).is_some() && body.get(set).is_some() {
                    return Err(Error::bad_json(format!(
                        "Cannot set both `{unset}` and `{set}`."
                    )));
                }
            }
            let set_name = body.get("set_displayname").and_then(Value::as_str);
            let unset_name = body.get("unset_displayname").and_then(Value::as_bool) == Some(true);
            let set_avatar = body.get("set_avatar_url").and_then(Value::as_str);
            let unset_avatar = body.get("unset_avatar_url").and_then(Value::as_bool) == Some(true);
            let id = user_for(&services, localpart)?;
            let (status, user_id) = match services.store.get_user(&id).await? {
                Some(u) => {
                    if let Some(n) = set_name {
                        services.set_display_name(&u.user_id, Some(n)).await?;
                    } else if unset_name {
                        services.set_display_name(&u.user_id, None).await?;
                    }
                    (StatusCode::OK, u.user_id)
                }
                None => {
                    let created = services
                        .register(Registration {
                            localpart: Some(localpart.to_owned()),
                            password: None,
                            display_name: set_name.map(str::to_owned),
                            is_guest: false,
                            is_admin: false,
                            appservice_id: None,
                        })
                        .await?;
                    (StatusCode::CREATED, created.user_id)
                }
            };
            if let Some(a) = set_avatar {
                services.set_avatar_url(&user_id, Some(a)).await?;
            } else if unset_avatar {
                services.set_avatar_url(&user_id, None).await?;
            }
            Ok((status, json!({})))
        }
        .await,
    )
}

/// `POST /_synapse/mas/delete_user`
pub async fn delete_user(
    State(services): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> axum::response::Response {
    respond(
        async {
            require_mas(&services, &headers)?;
            let u = existing_user(&services, body_str(&body, "localpart")?).await?;
            let erase = body.get("erase").and_then(Value::as_bool).unwrap_or(false);
            services.deactivate(&u.user_id).await?;
            if erase {
                services.set_display_name(&u.user_id, None).await?;
                services.set_avatar_url(&u.user_id, None).await?;
            }
            Ok((StatusCode::OK, json!({})))
        }
        .await,
    )
}

/// `POST /_synapse/mas/reactivate_user`
pub async fn reactivate_user(
    State(services): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> axum::response::Response {
    respond(
        async {
            require_mas(&services, &headers)?;
            let u = existing_user(&services, body_str(&body, "localpart")?).await?;
            services.store.set_deactivated(&u.user_id, false).await?;
            Ok((StatusCode::OK, json!({})))
        }
        .await,
    )
}

/// `POST /_synapse/mas/set_displayname`
pub async fn set_displayname(
    State(services): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> axum::response::Response {
    respond(
        async {
            require_mas(&services, &headers)?;
            let u = existing_user(&services, body_str(&body, "localpart")?).await?;
            let name = body_str(&body, "displayname")?;
            services.set_display_name(&u.user_id, Some(name)).await?;
            Ok((StatusCode::OK, json!({})))
        }
        .await,
    )
}

/// `POST /_synapse/mas/unset_displayname`
pub async fn unset_displayname(
    State(services): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> axum::response::Response {
    respond(
        async {
            require_mas(&services, &headers)?;
            let u = existing_user(&services, body_str(&body, "localpart")?).await?;
            services.set_display_name(&u.user_id, None).await?;
            Ok((StatusCode::OK, json!({})))
        }
        .await,
    )
}

/// `POST /_synapse/mas/allow_cross_signing_reset`
pub async fn allow_cross_signing_reset(
    State(services): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> axum::response::Response {
    respond(
        async {
            require_mas(&services, &headers)?;
            let u = existing_user(&services, body_str(&body, "localpart")?).await?;
            services
                .cross_signing_reset_allowed
                .insert(u.user_id, Instant::now());
            Ok((StatusCode::OK, json!({})))
        }
        .await,
    )
}

fn device_id_of(body: &Value) -> Result<OwnedDeviceId> {
    let d = body_str(body, "device_id")?;
    if d.is_empty() || d.len() > 255 {
        return Err(Error::bad_json("Invalid `device_id`."));
    }
    Ok(OwnedDeviceId::from(d))
}

/// `POST /_synapse/mas/upsert_device`
pub async fn upsert_device(
    State(services): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> axum::response::Response {
    respond(
        async {
            require_mas(&services, &headers)?;
            let u = existing_user(&services, body_str(&body, "localpart")?).await?;
            let device_id = device_id_of(&body)?;
            let display_name = body.get("display_name").and_then(Value::as_str);
            if services
                .store
                .get_device(&u.user_id, &device_id)
                .await?
                .is_some()
            {
                if let Some(n) = display_name {
                    services
                        .store
                        .update_device(&u.user_id, &device_id, Some(n))
                        .await?;
                }
                Ok((StatusCode::OK, json!({})))
            } else {
                services
                    .store
                    .create_device(&u.user_id, &device_id, display_name)
                    .await?;
                services
                    .notify_device_list_change(&u.user_id, Some(&device_id))
                    .await?;
                Ok((StatusCode::CREATED, json!({})))
            }
        }
        .await,
    )
}

/// `POST /_synapse/mas/delete_device`
pub async fn delete_device(
    State(services): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> axum::response::Response {
    respond(
        async {
            require_mas(&services, &headers)?;
            let u = existing_user(&services, body_str(&body, "localpart")?).await?;
            let device_id = device_id_of(&body)?;
            services.delete_device_fully(&u.user_id, &device_id).await?;
            Ok((StatusCode::NO_CONTENT, json!({})))
        }
        .await,
    )
}

/// `POST /_synapse/mas/update_device_display_name`
pub async fn update_device_display_name(
    State(services): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> axum::response::Response {
    respond(
        async {
            require_mas(&services, &headers)?;
            let u = existing_user(&services, body_str(&body, "localpart")?).await?;
            let device_id = device_id_of(&body)?;
            let name = body_str(&body, "display_name")?;
            if services
                .store
                .get_device(&u.user_id, &device_id)
                .await?
                .is_none()
            {
                return Err(Error::not_found("Device not found"));
            }
            services
                .store
                .update_device(&u.user_id, &device_id, Some(name))
                .await?;
            Ok((StatusCode::OK, json!({})))
        }
        .await,
    )
}

/// `POST /_synapse/mas/sync_devices`: make the user's devices exactly the given set.
pub async fn sync_devices(
    State(services): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> axum::response::Response {
    respond(
        async {
            require_mas(&services, &headers)?;
            let u = existing_user(&services, body_str(&body, "localpart")?).await?;
            let wanted: Vec<OwnedDeviceId> = body
                .get("devices")
                .and_then(Value::as_array)
                .ok_or_else(|| Error::bad_json("Missing `devices`."))?
                .iter()
                .filter_map(Value::as_str)
                .map(OwnedDeviceId::from)
                .collect();
            let existing = services.store.list_devices(&u.user_id).await?;
            for d in &existing {
                if !wanted.contains(&d.device_id) {
                    services
                        .delete_device_fully(&u.user_id, &d.device_id)
                        .await?;
                }
            }
            let mut created = false;
            for d in &wanted {
                if !existing.iter().any(|e| e.device_id == *d) {
                    services.store.create_device(&u.user_id, d, None).await?;
                    created = true;
                }
            }
            if created {
                services.notify_device_list_change(&u.user_id, None).await?;
            }
            Ok((StatusCode::OK, json!({})))
        }
        .await,
    )
}

/// `GET /_matrix/client/v1/auth_metadata`: the service's OAuth 2.0 server metadata.
pub async fn auth_metadata(State(services): State<AppState>) -> axum::response::Response {
    let Some(mas) = services.mas.clone() else {
        return Error::not_found("OIDC discovery has not been configured on this homeserver")
            .into_response();
    };
    match mas.metadata().await {
        Ok(v) => {
            let mut resp = Json(v).into_response();
            resp.headers_mut().insert(
                header::CACHE_CONTROL,
                header::HeaderValue::from_static(
                    "public, max-age=600, s-maxage=3600, stale-while-revalidate=600",
                ),
            );
            crate::extract::add_cors(resp.headers_mut());
            resp
        }
        Err(e) => Error::Service(e.into()).into_response(),
    }
}

/// `GET /_matrix/client/unstable/org.matrix.msc2965/auth_issuer` (legacy discovery).
pub async fn auth_issuer(State(services): State<AppState>) -> axum::response::Response {
    let Some(mas) = services.mas.clone() else {
        return Error::not_found("OIDC discovery has not been configured on this homeserver")
            .into_response();
    };
    match mas.metadata().await {
        Ok(v) => {
            let mut resp = Json(json!({ "issuer": v.get("issuer").cloned() })).into_response();
            crate::extract::add_cors(resp.headers_mut());
            resp
        }
        Err(e) => Error::Service(e.into()).into_response(),
    }
}
