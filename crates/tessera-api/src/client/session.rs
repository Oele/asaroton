//! Login, logout and token refresh.

use std::time::Duration;

use ruma::api::client::{
    session::{
        get_login_types::{self, v3::LoginType},
        login::{self, v3::LoginInfo},
        logout, logout_all, refresh_token,
    },
    uiaa::UserIdentifier,
};

use crate::{AppState, Error, Result, Ruma};

pub async fn login_types(
    _services: AppState,
    _req: Ruma<get_login_types::v3::Request>,
) -> Result<get_login_types::v3::Response> {
    Ok(get_login_types::v3::Response::new(vec![
        LoginType::Password(get_login_types::v3::PasswordLoginType::new()),
        LoginType::ApplicationService(get_login_types::v3::ApplicationServiceLoginType::new()),
    ]))
}

pub async fn login(
    services: AppState,
    req: Ruma<login::v3::Request>,
) -> Result<login::v3::Response> {
    let body = req.body;
    let user_id = match &body.login_info {
        LoginInfo::Password(p) => {
            let ident = match &p.identifier {
                Some(UserIdentifier::Matrix(m)) => m.user.clone(),
                Some(_) => return Err(Error::unknown("Unsupported identifier type.")),
                None => return Err(Error::bad_json("Missing user identifier.")),
            };
            let user_id = services.resolve_user_identifier(&ident)?;
            match services.check_password(&user_id, &p.password).await? {
                Some(user) if !user.is_deactivated => user.user_id,
                Some(_) => {
                    return Err(Error::matrix(
                        ruma::api::error::ErrorKind::UserDeactivated,
                        http::StatusCode::FORBIDDEN,
                        "This account has been deactivated.",
                    ));
                }
                None => return Err(Error::forbidden("Invalid username or password.")),
            }
        }
        LoginInfo::ApplicationService(a) => {
            let Some(sender) = req.sender.as_ref().filter(|s| s.appservice_id.is_some()) else {
                return Err(Error::forbidden(
                    "Application service login requires an appservice token.",
                ));
            };
            let appservice = sender
                .appservice_id
                .as_deref()
                .and_then(|id| services.appservices.by_id(id))
                .ok_or_else(|| Error::forbidden("Unknown appservice."))?;
            let ident = match &a.identifier {
                Some(UserIdentifier::Matrix(m)) => m.user.clone(),
                _ => return Err(Error::bad_json("Missing user identifier.")),
            };
            let user_id = services.resolve_user_identifier(&ident)?;
            services
                .ensure_appservice_user(&appservice, &user_id)
                .await?;
            user_id
        }
        _ => return Err(Error::unknown("Unsupported login type.")),
    };

    let session = services
        .create_session(
            &user_id,
            body.device_id.clone(),
            body.initial_device_display_name.as_deref(),
            body.refresh_token,
        )
        .await?;
    let mut resp =
        login::v3::Response::new(session.user_id, session.access_token, session.device_id);
    resp.refresh_token = session.refresh_token;
    resp.expires_in = session
        .expires_in_ms
        .map(|ms| Duration::from_millis(ms.unsigned_abs()));
    Ok(resp)
}

pub async fn logout(
    services: AppState,
    req: Ruma<logout::v3::Request>,
) -> Result<logout::v3::Response> {
    services.logout(req.sender()?).await?;
    Ok(logout::v3::Response::new())
}

pub async fn logout_all(
    services: AppState,
    req: Ruma<logout_all::v3::Request>,
) -> Result<logout_all::v3::Response> {
    services.logout_all(&req.sender()?.user_id).await?;
    Ok(logout_all::v3::Response::new())
}

pub async fn refresh(
    services: AppState,
    req: Ruma<refresh_token::v3::Request>,
) -> Result<refresh_token::v3::Response> {
    let session = services.refresh_session(&req.body.refresh_token).await?;
    let mut resp = refresh_token::v3::Response::new(session.access_token);
    resp.refresh_token = session.refresh_token;
    resp.expires_in_ms = session
        .expires_in_ms
        .map(|ms| Duration::from_millis(ms.unsigned_abs()));
    Ok(resp)
}
