//! Account moderation by server admins (Matrix v1.18, MSC4323): locking (v1.12) and
//! suspension (v1.13) of local accounts through `GET`/`PUT /_matrix/client/v1/admin/lock/{userId}`
//! and `.../admin/suspend/{userId}` (and the MSC's unstable paths, which ruma lists). Clients
//! find out who may use them from the `m.account_moderation` capability.
//!
//! What a lock or suspension does is enforced elsewhere: the lock where access tokens are
//! resolved (asaroton-auth and the extractor), a suspension in `crate::suspension`.

use asaroton_auth::Sender;
use asaroton_store::User;
use ruma::{
    UserId,
    api::client::admin::{is_user_locked, is_user_suspended, lock_user, suspend_user},
};

use crate::{AppState, Error, Result, Ruma};

/// The account an admin looks at, in the spec's order: the caller must be a server admin,
/// checked before any account is looked up (nobody else learns which accounts exist); the user
/// must be local (400); and exist, not deactivated (404). Another admin is out of reach (403).
async fn target(services: &AppState, sender: &Sender, user_id: &UserId) -> Result<User> {
    if !sender.is_admin {
        return Err(Error::forbidden(
            "Requesting user is not a server administrator.",
        ));
    }
    if !services.is_local(user_id) {
        return Err(Error::invalid_param(
            "User does not belong to the local server.",
        ));
    }
    let user = services
        .store
        .get_user(user_id)
        .await?
        .filter(|u| !u.is_deactivated)
        .ok_or_else(|| Error::not_found("User not found."))?;
    if user.is_admin && user.user_id != sender.user_id {
        return Err(Error::forbidden(
            "The target user is another server administrator.",
        ));
    }
    Ok(user)
}

/// [`target`] for a change: also not the admin's own account (checked before the lookup,
/// like the admin check).
async fn target_to_change(services: &AppState, sender: &Sender, user_id: &UserId) -> Result<User> {
    if sender.is_admin && sender.user_id == user_id {
        return Err(Error::forbidden(
            "You cannot change this for your own account.",
        ));
    }
    target(services, sender, user_id).await
}

/// `GET /_matrix/client/v1/admin/lock/{userId}`
pub async fn get_locked(
    services: AppState,
    req: Ruma<is_user_locked::v1::Request>,
) -> Result<is_user_locked::v1::Response> {
    let user = target(&services, req.sender()?, &req.body.user_id).await?;
    Ok(is_user_locked::v1::Response::new(user.is_locked))
}

/// `PUT /_matrix/client/v1/admin/lock/{userId}`
pub async fn set_locked(
    services: AppState,
    req: Ruma<lock_user::v1::Request>,
) -> Result<lock_user::v1::Response> {
    let user = target_to_change(&services, req.sender()?, &req.body.user_id).await?;
    services.set_locked(&user.user_id, req.body.locked).await?;
    Ok(lock_user::v1::Response::new(req.body.locked))
}

/// `GET /_matrix/client/v1/admin/suspend/{userId}`
pub async fn get_suspended(
    services: AppState,
    req: Ruma<is_user_suspended::v1::Request>,
) -> Result<is_user_suspended::v1::Response> {
    let user = target(&services, req.sender()?, &req.body.user_id).await?;
    Ok(is_user_suspended::v1::Response::new(user.is_suspended))
}

/// `PUT /_matrix/client/v1/admin/suspend/{userId}`
pub async fn set_suspended(
    services: AppState,
    req: Ruma<suspend_user::v1::Request>,
) -> Result<suspend_user::v1::Response> {
    let user = target_to_change(&services, req.sender()?, &req.body.user_id).await?;
    services
        .set_suspended(&user.user_id, req.body.suspended)
        .await?;
    Ok(suspend_user::v1::Response::new(req.body.suspended))
}
