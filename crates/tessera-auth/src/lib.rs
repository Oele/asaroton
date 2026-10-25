//! Authentication for the tessera homeserver: password hashing, access and refresh tokens,
//! resolving tokens to a [`Sender`], and User-Interactive Authentication sessions.
//!
//! MSC3861 (delegating auth to Matrix Authentication Service) plugs in behind the same
//! [`Authenticator`] in a later phase.

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

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
}

/// Resolves access tokens to senders.
#[derive(Clone)]
pub struct Authenticator {
    store: DynStore,
}

impl std::fmt::Debug for Authenticator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Authenticator")
    }
}

impl Authenticator {
    pub fn new(store: DynStore) -> Self {
        Self { store }
    }

    /// Look up a bearer token.
    pub async fn authenticate(&self, token: &str) -> Result<Sender, AuthError> {
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
        let (is_guest, appservice_id, deactivated) = match &user {
            Some(u) => (u.is_guest, u.appservice_id.clone(), u.is_deactivated),
            None => (false, None, false),
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
        })
    }
}
