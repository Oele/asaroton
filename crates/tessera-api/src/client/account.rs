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
    }
}

/// Validate one UIA stage.
async fn check_stage(
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
                return StageResult::Failed {
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

    // Appservice registration: no UIA, any localpart in the namespace (namespaces come with
    // appservice support in a later phase).
    if body.login_type == Some(register::LoginType::ApplicationService) {
        let Some(sender) = req.sender.as_ref().filter(|s| s.appservice_id.is_some()) else {
            return Err(Error::missing_token());
        };
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
        services.validate_localpart(username)?;
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
    if let UiaOutcome::Incomplete(info) = outcome {
        return Err(Error::Uiaa(info));
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
    resp.device_id = s.device_id.clone();
    Ok(resp)
}

pub async fn change_password(
    services: AppState,
    req: Ruma<change_password::v3::Request>,
) -> Result<change_password::v3::Response> {
    let sender = req.sender()?;
    uia_for_sender(&services, sender, req.body.auth.as_ref()).await?;
    services
        .change_password(
            &sender.user_id,
            &req.body.new_password,
            req.body.logout_devices,
            sender.device_id.as_deref(),
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
        Ok(()) => Ok(update_device::v3::Response::new()),
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
        .store
        .delete_device(&sender.user_id, &req.body.device_id)
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
        services.store.delete_device(&sender.user_id, d).await?;
    }
    Ok(delete_devices::v3::Response::new())
}

async fn local_user(services: &Services, user_id: &OwnedUserId) -> Result<tessera_store::User> {
    if !services.is_local(user_id) {
        // Remote profiles are fetched over federation in Phase 3.
        return Err(Error::not_found("Profile not found."));
    }
    services
        .store
        .get_user(user_id)
        .await?
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
    Ok(resp)
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
        user.avatar_url.and_then(|u| u.try_into().ok()),
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
