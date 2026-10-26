//! Registration, account management, devices and profiles.

use std::time::Duration;

use ruma::api::error::ErrorKind;
use ruma::{
    MilliSecondsSinceUnixEpoch, OwnedDeviceId, OwnedUserId, UInt,
    api::client::{
        account::{
            change_password, deactivate, get_username_availability,
            register::{self, RegistrationKind},
            whoami,
        },
        device::{
            self, Device, delete_device, delete_devices, get_device, get_devices, update_device,
        },
        profile::{
            get_avatar_url, get_display_name, get_profile, set_avatar_url, set_display_name,
        },
        uiaa::{AuthData, AuthFlow, AuthType, UserIdentifier},
    },
};
use tessera_auth::{Sender, StageResult, UiaOutcome};
use tessera_service::{Services, account::Registration};

use crate::{AppState, Error, Result, Ruma};

/// Run UIA for an endpoint that authenticates the *current* user (password change, device
/// deletion, deactivation): the password stage must match the sender.
async fn uia_for_sender(
    services: &Services,
    sender: &Sender,
    auth: Option<&AuthData>,
) -> Result<()> {
    let flows = vec![AuthFlow::new(vec![AuthType::Password])];
    let outcome = services
        .uia
        .step(
            flows,
            auth,
            Some(sender.user_id.clone()),
            None,
            async |stage, data, user| check_stage(services, stage, data, user).await,
        )
        .await;
    match outcome {
        UiaOutcome::Complete { .. } => Ok(()),
        UiaOutcome::Incomplete(info) => Err(Error::Uiaa(info)),
        UiaOutcome::Rejected { kind, message } => {
            Err(Error::matrix(kind, http::StatusCode::FORBIDDEN, message))
        }
    }
}

/// Validate one UIA stage.
pub(super) async fn check_stage(
    services: &Services,
    stage: AuthType,
    data: &AuthData,
    expected_user: Option<&OwnedUserId>,
) -> StageResult {
    match (stage, data) {
        (AuthType::Dummy, _) => StageResult::Passed { user_id: None },
        (AuthType::Password, AuthData::Password(p)) => {
            let UserIdentifier::Matrix(ident) = &p.identifier else {
                return StageResult::Failed {
                    kind: ErrorKind::Forbidden,
                    message: "Unsupported identifier.".into(),
                };
            };
            let Ok(user_id) = services.resolve_user_identifier(&ident.user) else {
                return StageResult::Failed {
                    kind: ErrorKind::Forbidden,
                    message: "Invalid username or password.".into(),
                };
            };
            if let Some(expected) = expected_user
                && *expected != user_id
            {
                return StageResult::Rejected {
                    kind: ErrorKind::Forbidden,
                    message: "Password does not match the current user.".into(),
                };
            }
            match services.check_password(&user_id, &p.password).await {
                Ok(Some(_)) => StageResult::Passed {
                    user_id: Some(user_id),
                },
                _ => StageResult::Failed {
                    kind: ErrorKind::Forbidden,
                    message: "Invalid username or password.".into(),
                },
            }
        }
        (AuthType::RegistrationToken, AuthData::RegistrationToken(_)) => StageResult::Failed {
            kind: ErrorKind::Forbidden,
            message: "Registration tokens are not enabled.".into(),
        },
        _ => StageResult::Failed {
            kind: ErrorKind::Unrecognized,
            message: "Unsupported authentication stage.".into(),
        },
    }
}

pub async fn register(
    services: AppState,
    req: Ruma<register::v3::Request>,
) -> Result<register::v3::Response> {
    let body = req.body;

    // With delegated authentication only appservices register users here.
    if services.mas.is_some() && body.login_type != Some(register::LoginType::ApplicationService) {
        return Err(Error::forbidden(
            "Registration is handled by the authentication service.",
        ));
    }
    // Appservice registration: no UIA, any localpart in the namespace (namespaces come with
    // appservice support in a later phase).
    if body.login_type == Some(register::LoginType::ApplicationService) {
        let Some(sender) = req.sender.as_ref().filter(|s| s.appservice_id.is_some()) else {
            return Err(Error::missing_token());
        };
        let appservice = sender
            .appservice_id
            .as_deref()
            .and_then(|id| services.appservices.by_id(id))
            .ok_or_else(|| Error::forbidden("Unknown appservice."))?;
        if let Some(username) = &body.username {
            let candidate = services.validate_localpart(username)?;
            if !appservice.controls_user(&candidate) {
                return Err(Error::matrix(
                    ErrorKind::Exclusive,
                    http::StatusCode::BAD_REQUEST,
                    "User ID is not in the appservice's namespace.",
                ));
            }
        }
        let user = services
            .register(Registration {
                localpart: body.username.clone(),
                password: None,
                display_name: body.initial_device_display_name.clone(),
                is_guest: false,
                is_admin: false,
                appservice_id: sender.appservice_id.clone(),
            })
            .await?;
        return finish_registration(&services, user.user_id, &body).await;
    }

    if body.kind == RegistrationKind::Guest {
        if !services.config.allow_guests {
            return Err(Error::forbidden("Guest access is disabled."));
        }
        let user = services
            .register(Registration {
                is_guest: true,
                ..Default::default()
            })
            .await?;
        return finish_registration(&services, user.user_id, &body).await;
    }

    if !services.config.registration_enabled {
        return Err(Error::forbidden("Registration has been disabled."));
    }

    // Validate the username up front so clients get M_INVALID_USERNAME before UIA.
    if let Some(username) = &body.username {
        let candidate = services.validate_localpart(username)?;
        if services
            .appservices
            .exclusive_owner_of_user(&candidate)
            .is_some()
        {
            return Err(Error::matrix(
                ErrorKind::Exclusive,
                http::StatusCode::BAD_REQUEST,
                "User ID is reserved by an application service.",
            ));
        }
        if !services.is_username_available(username).await? {
            return Err(Error::matrix(
                ErrorKind::UserInUse,
                http::StatusCode::BAD_REQUEST,
                "User ID already taken.",
            ));
        }
    }

    let flows = if services.config.registration_requires_token {
        vec![AuthFlow::new(vec![AuthType::RegistrationToken])]
    } else {
        vec![AuthFlow::new(vec![AuthType::Dummy])]
    };
    let outcome = services
        .uia
        .step(
            flows,
            body.auth.as_ref(),
            None,
            None,
            async |stage, data, user| check_stage(&services, stage, data, user).await,
        )
        .await;
    match outcome {
        UiaOutcome::Complete { .. } => {}
        UiaOutcome::Incomplete(info) => return Err(Error::Uiaa(info)),
        UiaOutcome::Rejected { kind, message } => {
            return Err(Error::matrix(kind, http::StatusCode::FORBIDDEN, message));
        }
    }

    let Some(password) = body.password.clone() else {
        return Err(Error::missing_param("Missing password."));
    };
    if password.len() > 512 {
        return Err(Error::invalid_param("Password too long."));
    }
    let user = services
        .register(Registration {
            localpart: body.username.clone(),
            password: Some(password),
            display_name: None,
            is_guest: false,
            is_admin: false,
            appservice_id: None,
        })
        .await?;
    finish_registration(&services, user.user_id, &body).await
}

async fn finish_registration(
    services: &Services,
    user_id: OwnedUserId,
    body: &register::v3::Request,
) -> Result<register::v3::Response> {
    let mut resp = register::v3::Response::new(user_id.clone());
    if body.inhibit_login {
        return Ok(resp);
    }
    let session = services
        .create_session(
            &user_id,
            body.device_id.clone(),
            body.initial_device_display_name.as_deref(),
            body.refresh_token,
        )
        .await?;
    resp.access_token = Some(session.access_token);
    resp.device_id = Some(session.device_id);
    resp.refresh_token = session.refresh_token;
    resp.expires_in = session
        .expires_in_ms
        .map(|ms| Duration::from_millis(ms.unsigned_abs()));
    Ok(resp)
}

pub async fn available(
    services: AppState,
    req: Ruma<get_username_availability::v3::Request>,
) -> Result<get_username_availability::v3::Response> {
    if !services.is_username_available(&req.body.username).await? {
        return Err(Error::matrix(
            ErrorKind::UserInUse,
            http::StatusCode::BAD_REQUEST,
            "Desired user ID is already taken.",
        ));
    }
    Ok(get_username_availability::v3::Response::new(true))
}

pub async fn whoami(
    _services: AppState,
    req: Ruma<whoami::v3::Request>,
) -> Result<whoami::v3::Response> {
    let s = req.sender()?;
    let mut resp = whoami::v3::Response::new(s.user_id.clone(), s.is_guest);
    resp.device_id.clone_from(&s.device_id);
    Ok(resp)
}

pub async fn change_password(
    services: AppState,
    req: Ruma<change_password::v3::Request>,
) -> Result<change_password::v3::Response> {
    let sender = req.sender()?;
    if services.mas.is_some() {
        return Err(Error::forbidden(
            "Passwords are managed by the authentication service.",
        ));
    }
    uia_for_sender(&services, sender, req.body.auth.as_ref()).await?;
    services
        .change_password(
            &sender.user_id,
            &req.body.new_password,
            req.body.logout_devices,
            sender.device_id.as_deref(),
            Some(&sender.token),
        )
        .await?;
    Ok(change_password::v3::Response::new())
}

pub async fn deactivate(
    services: AppState,
    req: Ruma<deactivate::v3::Request>,
) -> Result<deactivate::v3::Response> {
    let sender = req.sender()?;
    uia_for_sender(&services, sender, req.body.auth.as_ref()).await?;
    services.deactivate(&sender.user_id).await?;
    Ok(deactivate::v3::Response::new(
        ruma::api::client::account::ThirdPartyIdRemovalStatus::NoSupport,
    ))
}

fn to_api_device(d: tessera_store::Device) -> Device {
    let mut out = Device::new(d.device_id);
    out.display_name = d.display_name;
    out.last_seen_ip = d.last_seen_ip;
    out.last_seen_ts = d
        .last_seen_ts
        .and_then(|ts| UInt::try_from(ts).ok())
        .map(MilliSecondsSinceUnixEpoch);
    out
}

pub async fn get_devices(
    services: AppState,
    req: Ruma<get_devices::v3::Request>,
) -> Result<get_devices::v3::Response> {
    let devices = services.devices(&req.sender()?.user_id).await?;
    Ok(get_devices::v3::Response::new(
        devices.into_iter().map(to_api_device).collect(),
    ))
}

pub async fn get_device(
    services: AppState,
    req: Ruma<get_device::v3::Request>,
) -> Result<get_device::v3::Response> {
    let d = services
        .store
        .get_device(&req.sender()?.user_id, &req.body.device_id)
        .await?
        .ok_or_else(|| Error::not_found("Device not found."))?;
    Ok(get_device::v3::Response::new(to_api_device(d)))
}

pub async fn update_device(
    services: AppState,
    req: Ruma<update_device::v3::Request>,
) -> Result<update_device::v3::Response> {
    let user_id = &req.sender()?.user_id;
    match services
        .store
        .update_device(
            user_id,
            &req.body.device_id,
            req.body.display_name.as_deref(),
        )
        .await
    {
        Ok(()) => {
            // Display names travel with device keys, so this is a device-list change.
            services
                .notify_device_list_change(user_id, Some(&req.body.device_id))
                .await?;
            Ok(update_device::v3::Response::new())
        }
        // Application services create devices for their users this way (v1.17).
        Err(tessera_store::Error::NotFound(_)) if req.sender()?.appservice_id.is_some() => {
            services
                .store
                .create_device(
                    user_id,
                    &req.body.device_id,
                    req.body.display_name.as_deref(),
                )
                .await?;
            services
                .notify_device_list_change(user_id, Some(&req.body.device_id))
                .await?;
            Ok(update_device::v3::Response::new())
        }
        Err(tessera_store::Error::NotFound(_)) => Err(Error::not_found("Device not found.")),
        Err(e) => Err(e.into()),
    }
}

pub async fn delete_device(
    services: AppState,
    req: Ruma<delete_device::v3::Request>,
) -> Result<delete_device::v3::Response> {
    let sender = req.sender()?;
    uia_for_sender(&services, sender, req.body.auth.as_ref()).await?;
    services
        .delete_device_fully(&sender.user_id, &req.body.device_id)
        .await?;
    Ok(delete_device::v3::Response::new())
}

pub async fn delete_devices(
    services: AppState,
    req: Ruma<delete_devices::v3::Request>,
) -> Result<delete_devices::v3::Response> {
    let sender = req.sender()?;
    uia_for_sender(&services, sender, req.body.auth.as_ref()).await?;
    for d in &req.body.devices {
        services.delete_device_fully(&sender.user_id, d).await?;
    }
    Ok(delete_devices::v3::Response::new())
}

async fn local_user(services: &Services, user_id: &OwnedUserId) -> Result<tessera_store::User> {
    if !services.is_local(user_id) {
        // Remote profile over federation, mapped into our user shape.
        let v = services.federation().remote_profile(user_id).await?;
        return Ok(tessera_store::User {
            user_id: user_id.clone(),
            display_name: v
                .get("displayname")
                .and_then(|d| d.as_str())
                .map(str::to_owned),
            avatar_url: v
                .get("avatar_url")
                .and_then(|d| d.as_str())
                .map(str::to_owned),
            is_admin: false,
            is_guest: false,
            is_deactivated: false,
            appservice_id: None,
            created_ts: 0,
            profile_fields: v
                .as_object()
                .map(|o| {
                    o.iter()
                        .filter(|(k, _)| k.as_str() != "displayname" && k.as_str() != "avatar_url")
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect()
                })
                .unwrap_or_default(),
        });
    }
    {
        let mut user = services.store.get_user(user_id).await?;
        if user.is_none()
            && services
                .query_appservice_user(user_id)
                .await
                .unwrap_or(false)
        {
            user = services.store.get_user(user_id).await?;
        }
        user
    }
    .ok_or_else(|| Error::not_found("Profile not found."))
}

pub async fn get_profile(
    services: AppState,
    req: Ruma<get_profile::v3::Request>,
) -> Result<get_profile::v3::Response> {
    let user = local_user(&services, &req.body.user_id).await?;
    let mut resp = get_profile::v3::Response::new();
    if let Some(name) = user.display_name {
        resp.set("displayname".to_owned(), serde_json::Value::String(name));
    }
    if let Some(url) = user.avatar_url {
        resp.set("avatar_url".to_owned(), serde_json::Value::String(url));
    }
    for (k, v) in user.profile_fields {
        resp.set(k, v);
    }
    Ok(resp)
}

/// Custom profile fields (spec v1.16): keys other than `displayname` and `avatar_url`.
const MAX_PROFILE_KEY_LEN: usize = 255;
const MAX_PROFILE_BYTES: usize = 64 * 1024;

fn check_profile_key(key: &str) -> Result<()> {
    if key.is_empty() || key.len() > MAX_PROFILE_KEY_LEN || key.contains('\0') {
        return Err(Error::matrix(
            ruma::api::error::ErrorKind::BadJson,
            http::StatusCode::BAD_REQUEST,
            "Invalid profile field name.",
        ));
    }
    Ok(())
}

/// `GET /_matrix/client/v3/profile/{userId}/{key}`
pub async fn get_profile_field(
    services: AppState,
    req: Ruma<ruma::api::client::profile::get_profile_field::v3::Request>,
) -> Result<axum::response::Response> {
    use axum::response::IntoResponse;
    let key = req.body.field.to_string();
    let user = local_user(&services, &req.body.user_id).await?;
    let value = match key.as_str() {
        "displayname" => user.display_name.map(serde_json::Value::String),
        "avatar_url" => user.avatar_url.map(serde_json::Value::String),
        k => user.profile_fields.get(k).cloned(),
    };
    let Some(value) = value else {
        return Err(Error::not_found("Profile field not set."));
    };
    let mut resp = axum::Json(serde_json::json!({ key: value })).into_response();
    crate::extract::add_cors(resp.headers_mut());
    Ok(resp)
}

/// `PUT /_matrix/client/v3/profile/{userId}/{key}` (raw: the body is `{key: value}`).
pub async fn set_profile_field(
    axum::extract::State(services): axum::extract::State<AppState>,
    headers: axum::http::HeaderMap,
    axum::extract::Path((user_id, key)): axum::extract::Path<(String, String)>,
    body: axum::body::Bytes,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    match set_profile_field_inner(&services, &headers, &user_id, &key, &body).await {
        Ok(()) => {
            let mut resp = axum::Json(serde_json::json!({})).into_response();
            crate::extract::add_cors(resp.headers_mut());
            resp
        }
        Err(e) => e.into_response(),
    }
}

async fn set_profile_field_inner(
    services: &AppState,
    headers: &axum::http::HeaderMap,
    user_id: &str,
    key: &str,
    body: &[u8],
) -> Result<()> {
    let sender = authenticate_headers(services, headers).await?;
    let user = ruma::OwnedUserId::try_from(user_id)
        .map_err(|_| Error::invalid_param("Invalid user ID."))?;
    if sender.user_id != user {
        return Err(Error::forbidden("Cannot change another user's profile."));
    }
    check_profile_key(key)?;
    if body.len() > MAX_PROFILE_BYTES {
        return Err(Error::matrix(
            ruma::api::error::ErrorKind::TooLarge,
            http::StatusCode::PAYLOAD_TOO_LARGE,
            "Profile field too large.",
        ));
    }
    let parsed: serde_json::Value =
        serde_json::from_slice(body).map_err(|e| Error::bad_json(e.to_string()))?;
    let Some(value) = parsed.get(key).cloned() else {
        return Err(Error::bad_json(format!("Body must contain `{key}`.")));
    };
    match key {
        "displayname" => {
            let name = value
                .as_str()
                .ok_or_else(|| Error::bad_json("displayname must be a string."))?;
            services.set_display_name(&user, Some(name)).await?;
        }
        "avatar_url" => {
            let url = value
                .as_str()
                .ok_or_else(|| Error::bad_json("avatar_url must be a string."))?;
            services.set_avatar_url(&user, Some(url)).await?;
        }
        _ => {
            // The whole profile stays under the size limit.
            let current = services.store.get_user(&user).await?.map_or(0, |u| {
                serde_json::Value::Object(u.profile_fields)
                    .to_string()
                    .len()
            });
            if current + body.len() > MAX_PROFILE_BYTES {
                return Err(Error::matrix(
                    ruma::api::error::ErrorKind::TooLarge,
                    http::StatusCode::PAYLOAD_TOO_LARGE,
                    "Profile too large.",
                ));
            }
            services
                .store
                .set_profile_field(&user, key, Some(&value))
                .await?;
        }
    }
    Ok(())
}

/// `DELETE /_matrix/client/v3/profile/{userId}/{key}`
pub async fn delete_profile_field(
    services: AppState,
    req: Ruma<ruma::api::client::profile::delete_profile_field::v3::Request>,
) -> Result<axum::response::Response> {
    use axum::response::IntoResponse;
    let sender = req.sender()?;
    if sender.user_id != req.body.user_id {
        return Err(Error::forbidden("Cannot change another user's profile."));
    }
    let key = req.body.field.to_string();
    match key.as_str() {
        "displayname" => services.set_display_name(&sender.user_id, None).await?,
        "avatar_url" => services.set_avatar_url(&sender.user_id, None).await?,
        k => {
            check_profile_key(k)?;
            services
                .store
                .set_profile_field(&sender.user_id, k, None)
                .await?;
        }
    }
    let mut resp = axum::Json(serde_json::json!({})).into_response();
    crate::extract::add_cors(resp.headers_mut());
    Ok(resp)
}

/// Bearer authentication for raw handlers.
pub(crate) async fn authenticate_headers(
    services: &AppState,
    headers: &axum::http::HeaderMap,
) -> Result<tessera_auth::Sender> {
    let token = headers
        .get(http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .map(str::trim)
        .ok_or_else(Error::missing_token)?;
    Ok(services.auth.authenticate(token).await?)
}

pub async fn get_display_name(
    services: AppState,
    req: Ruma<get_display_name::v3::Request>,
) -> Result<get_display_name::v3::Response> {
    let user = local_user(&services, &req.body.user_id).await?;
    Ok(get_display_name::v3::Response::new(user.display_name))
}

pub async fn set_display_name(
    services: AppState,
    req: Ruma<set_display_name::v3::Request>,
) -> Result<set_display_name::v3::Response> {
    let sender = req.sender()?;
    if sender.user_id != req.body.user_id {
        return Err(Error::forbidden("Cannot change another user's profile."));
    }
    services
        .set_display_name(&sender.user_id, req.body.displayname.as_deref())
        .await?;
    Ok(set_display_name::v3::Response::new())
}

pub async fn get_avatar_url(
    services: AppState,
    req: Ruma<get_avatar_url::v3::Request>,
) -> Result<get_avatar_url::v3::Response> {
    let user = local_user(&services, &req.body.user_id).await?;
    Ok(get_avatar_url::v3::Response::new(
        user.avatar_url.map(ruma::OwnedMxcUri::from),
    ))
}

pub async fn set_avatar_url(
    services: AppState,
    req: Ruma<set_avatar_url::v3::Request>,
) -> Result<set_avatar_url::v3::Response> {
    let sender = req.sender()?;
    if sender.user_id != req.body.user_id {
        return Err(Error::forbidden("Cannot change another user's profile."));
    }
    services
        .set_avatar_url(
            &sender.user_id,
            req.body.avatar_url.as_ref().map(|u| u.as_str()),
        )
        .await?;
    Ok(set_avatar_url::v3::Response::new())
}

/// Unused-import guard for the `device` module alias.
#[allow(dead_code)]
fn _unused(_: Option<device::Device>, _: Option<OwnedDeviceId>) {}
