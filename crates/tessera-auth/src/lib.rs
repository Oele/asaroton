//! Authentication for the tessera homeserver: password hashing, access and refresh tokens,
//! resolving tokens to a [`Sender`], and User-Interactive Authentication sessions.
//!
//! MSC3861 (delegating auth to Matrix Authentication Service) plugs in behind the same
//! [`Authenticator`] in a later phase.

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

pub mod mas;
pub mod password;
pub mod token;
pub mod uia;

use std::sync::Arc;

use ruma::{OwnedDeviceId, OwnedUserId};
use tessera_store::{DynStore, now_ts};

pub use password::{hash_password, verify_password};
pub use token::{generate_device_id, generate_token};
pub use uia::{StageResult, UiaOutcome, UiaSessions};

/// Who is making an authenticated request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Sender {
    pub user_id: OwnedUserId,
    /// `None` for appservice senders (and MAS tokens without a device).
    pub device_id: Option<OwnedDeviceId>,
    pub is_guest: bool,
    /// The appservice this token belongs to, if any.
    pub appservice_id: Option<String>,
    /// The access token used (kept for logout).
    pub token: String,
    /// Server administrator (user flag, or an admin scope from the authentication service).
    pub is_admin: bool,
}

/// Why a token was rejected.
#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error("unknown access token")]
    UnknownToken { soft_logout: bool },
    #[error("access token expired")]
    Expired,
    #[error("user is deactivated")]
    Deactivated,
    #[error("storage error: {0}")]
    Store(#[from] tessera_store::Error),
    /// The authentication service could not be reached.
    #[error("authentication service unavailable: {0}")]
    Unavailable(String),
}

/// Resolves access tokens to senders.
#[derive(Clone)]
pub struct Authenticator {
    store: DynStore,
    appservices: Vec<AppserviceToken>,
    /// Delegated authentication: when set, every non-appservice token is introspected.
    mas: Option<Arc<mas::MasClient>>,
}

impl std::fmt::Debug for Authenticator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Authenticator")
    }
}

impl Authenticator {
    pub fn new(store: DynStore) -> Self {
        Self {
            store,
            appservices: Vec::new(),
            mas: None,
        }
    }

    /// Delegate token validation to Matrix Authentication Service.
    #[must_use]
    pub fn with_mas(mut self, mas: Option<Arc<mas::MasClient>>) -> Self {
        self.mas = mas;
        self
    }

    /// Register appservice tokens: a bearer token equal to an `as_token` authenticates as
    /// that appservice's sender user.
    #[must_use]
    pub fn with_appservices(mut self, tokens: Vec<AppserviceToken>) -> Self {
        self.appservices = tokens;
        self
    }

    /// Look up a bearer token.
    pub async fn authenticate(&self, token: &str) -> Result<Sender, AuthError> {
        if let Some(a) = self.appservices.iter().find(|a| a.token == token) {
            return Ok(Sender {
                user_id: a.user_id.clone(),
                device_id: None,
                is_guest: false,
                appservice_id: Some(a.id.clone()),
                token: token.to_owned(),
                is_admin: false,
            });
        }
        if let Some(mas) = &self.mas {
            let id = mas.introspect(token).await?;
            let Some(user) = self.store.get_user(&id.user_id).await? else {
                return Err(AuthError::UnknownToken { soft_logout: false });
            };
            if user.is_deactivated {
                return Err(AuthError::Deactivated);
            }
            if let Some(device_id) = &id.device_id {
                if self
                    .store
                    .get_device(&id.user_id, device_id)
                    .await?
                    .is_none()
                {
                    // The device was removed on the service side: the token is dead.
                    mas.forget(token);
                    return Err(AuthError::UnknownToken { soft_logout: false });
                }
                let store = Arc::clone(&self.store);
                let (u, d) = (id.user_id.clone(), device_id.clone());
                tokio::spawn(async move {
                    let _ = store.touch_device(&u, &d, None, None).await;
                });
            }
            return Ok(Sender {
                user_id: id.user_id,
                device_id: id.device_id,
                is_guest: false,
                appservice_id: user.appservice_id,
                token: token.to_owned(),
                is_admin: id.admin || user.is_admin,
            });
        }
        let Some(t) = self.store.get_access_token(token).await? else {
            return Err(AuthError::UnknownToken { soft_logout: false });
        };
        if let Some(exp) = t.expires_ts
            && exp <= now_ts()
        {
            // Expired tokens from a refreshable session are a soft logout: the client keeps
            // its device and E2EE keys and refreshes.
            return Err(AuthError::UnknownToken {
                soft_logout: t.refresh_token.is_some(),
            });
        }
        let user_id = t
            .puppets_user_id
            .clone()
            .unwrap_or_else(|| t.user_id.clone());
        let user = self.store.get_user(&user_id).await?;
        let (is_guest, appservice_id, deactivated, is_admin) = match &user {
            Some(u) => (
                u.is_guest,
                u.appservice_id.clone(),
                u.is_deactivated,
                u.is_admin,
            ),
            None => (false, None, false, false),
        };
        if deactivated {
            return Err(AuthError::Deactivated);
        }
        if let Some(device_id) = &t.device_id {
            // Best-effort last-seen update; never fails the request.
            let store = Arc::clone(&self.store);
            let (u, d) = (t.user_id.clone(), device_id.clone());
            tokio::spawn(async move {
                let _ = store.touch_device(&u, &d, None, None).await;
            });
        }
        Ok(Sender {
            user_id,
            device_id: t.device_id,
            is_guest,
            appservice_id,
            token: token.to_owned(),
            is_admin,
        })
    }
}

/// An appservice's `as_token`, the appservice it belongs to, and its sender user.
#[derive(Clone, Debug)]
pub struct AppserviceToken {
    pub token: String,
    pub id: String,
    pub user_id: OwnedUserId,
}
