//! Authentication for the asaroton homeserver: password hashing, access and refresh tokens,
//! resolving tokens to a [`Sender`], and User-Interactive Authentication sessions.
//!
//! MSC3861 (delegating auth to Matrix Authentication Service) plugs in behind the same
//! [`Authenticator`] in a later phase.

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

pub mod mas;
pub mod password;
pub mod token;
pub mod uia;

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use asaroton_store::{DynStore, now_ts};
use ruma::{OwnedDeviceId, OwnedUserId};

pub use password::{hash_password, is_bcrypt, verify_password};
pub use token::{generate_device_id, generate_session_id, generate_token};
pub use uia::{StageResult, UiaOutcome, UiaSessions};

/// Who is making an authenticated request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Sender {
    pub user_id: OwnedUserId,
    /// `None` for appservice senders (and MAS tokens without a device).
    pub device_id: Option<OwnedDeviceId>,
    pub is_guest: bool,
    /// The appservice whose `as_token` this is, if any. Only an `as_token` sets it: the token
    /// of a user an appservice registered is that user's own and gives no appservice powers
    /// (acting as other users with `?user_id=`, UIA-free device deletion, appservice-only
    /// endpoints). Before 2026-11-28 it was copied from the user's record, which let anyone
    /// with such a user's token act as any user in the namespace.
    pub appservice_id: Option<String>,
    /// The access token used (kept for logout).
    pub token: String,
    /// Server administrator (user flag, or an admin scope from the authentication service).
    pub is_admin: bool,
    /// The account is locked (Matrix v1.12). [`Authenticator::authenticate`] refuses such a
    /// sender; only [`Authenticator::authenticate_allowing_locked`] hands it out, for a logout.
    pub locked: bool,
    /// The account is suspended (Matrix v1.13): what it may still do is decided per request
    /// (asaroton-api's `suspension` module).
    pub suspended: bool,
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
    /// The account is locked by a server admin (`M_USER_LOCKED`, a soft logout: the token
    /// stays good for when the lock is lifted).
    #[error("account is locked")]
    Locked,
    #[error("storage error: {0}")]
    Store(#[from] asaroton_store::Error),
    /// The authentication service could not be reached.
    #[error("authentication service unavailable: {0}")]
    Unavailable(String),
}

/// Resolves access tokens to senders.
/// How long a validated token is trusted without going back to the store.
const CACHE_TTL: Duration = Duration::from_secs(30);
/// How often a device's last-seen timestamp is written.
const TOUCH_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Clone)]
pub struct Authenticator {
    store: DynStore,
    appservices: Vec<AppserviceToken>,
    /// Delegated authentication: when set, every non-appservice token is introspected.
    mas: Option<Arc<mas::MasClient>>,
    /// Recently validated tokens and until when each is trusted. Logout, device deletion and
    /// account changes drop the affected entries.
    cache: Arc<dashmap::DashMap<String, (Sender, Instant)>>,
    /// Counts the `forget_*` calls. A lookup that a forget overtook may have read the account
    /// as it was before the change (a lock, say): it answers its request but is not cached.
    generation: Arc<AtomicU64>,
    /// When each device's last-seen timestamp was last written.
    touched: Arc<dashmap::DashMap<(OwnedUserId, OwnedDeviceId), Instant>>,
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
            cache: Arc::new(dashmap::DashMap::new()),
            generation: Arc::new(AtomicU64::new(0)),
            touched: Arc::new(dashmap::DashMap::new()),
        }
    }

    /// Drops one token from the cache (logout).
    pub fn forget_token(&self, token: &str) {
        self.generation.fetch_add(1, Ordering::SeqCst);
        self.cache.remove(token);
    }

    /// Drops every cached token of a user (device deletion, deactivation, flag changes).
    pub fn forget_user(&self, user_id: &ruma::UserId) {
        // Counted before the entries go: a lookup that inserts after this sees the new count
        // and takes its entry out again; one that inserted before is removed here.
        self.generation.fetch_add(1, Ordering::SeqCst);
        self.cache.retain(|_, (s, _)| s.user_id != user_id);
    }

    /// Records that a device was seen, writing it to the store at most once a minute.
    fn touch(&self, user_id: &OwnedUserId, device_id: &OwnedDeviceId) {
        let now = Instant::now();
        let key = (user_id.clone(), device_id.clone());
        if self
            .touched
            .get(&key)
            .is_some_and(|t| now.duration_since(*t) < TOUCH_INTERVAL)
        {
            return;
        }
        self.touched.insert(key, now);
        if self.touched.len() > 100_000 {
            self.touched
                .retain(|_, t| now.duration_since(*t) < TOUCH_INTERVAL);
        }
        let store = Arc::clone(&self.store);
        let (u, d) = (user_id.clone(), device_id.clone());
        tokio::spawn(async move {
            let _ = store.touch_device(&u, &d, None, None).await;
        });
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

    /// Look up a bearer token. A locked account's token is refused with
    /// [`AuthError::Locked`] (Matrix v1.12: every request but a logout).
    pub async fn authenticate(&self, token: &str) -> Result<Sender, AuthError> {
        let sender = self.authenticate_allowing_locked(token).await?;
        if sender.locked {
            return Err(AuthError::Locked);
        }
        Ok(sender)
    }

    /// Look up a bearer token, also when its account is locked: for `POST /logout` and
    /// `POST /logout/all`, the requests a locked account may still make. Everything else goes
    /// through [`Self::authenticate`].
    pub async fn authenticate_allowing_locked(&self, token: &str) -> Result<Sender, AuthError> {
        if let Some(a) = self.appservices.iter().find(|a| a.token == token) {
            return Ok(Sender {
                user_id: a.user_id.clone(),
                device_id: None,
                is_guest: false,
                appservice_id: Some(a.id.clone()),
                token: token.to_owned(),
                is_admin: false,
                locked: false,
                suspended: false,
            });
        }
        let now = Instant::now();
        if let Some(entry) = self.cache.get(token)
            && entry.1 > now
        {
            let sender = entry.0.clone();
            drop(entry);
            if let Some(d) = &sender.device_id {
                self.touch(&sender.user_id, d);
            }
            return Ok(sender);
        }
        let generation = self.generation.load(Ordering::SeqCst);
        let (sender, valid_until) = self.authenticate_uncached(token).await?;
        if self.cache.len() > 100_000 {
            self.cache.retain(|_, (_, until)| *until > now);
        }
        self.cache.insert(
            token.to_owned(),
            (sender.clone(), valid_until.min(now + CACHE_TTL)),
        );
        if self.generation.load(Ordering::SeqCst) != generation {
            // Something was forgotten while this was read, perhaps this very account.
            self.cache.remove(token);
        }
        Ok(sender)
    }

    /// Validates a token against the store (or the authentication service); returns the sender
    /// and when the token itself expires.
    async fn authenticate_uncached(&self, token: &str) -> Result<(Sender, Instant), AuthError> {
        let far = Instant::now() + Duration::from_hours(24);
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
                self.touch(&id.user_id, device_id);
            }
            return Ok((
                Sender {
                    user_id: id.user_id,
                    device_id: id.device_id,
                    is_guest: false,
                    appservice_id: None,
                    token: token.to_owned(),
                    is_admin: id.admin || user.is_admin,
                    locked: user.is_locked,
                    suspended: user.is_suspended,
                },
                far,
            ));
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
        let (is_guest, deactivated, is_admin, locked, suspended) = match &user {
            Some(u) => (
                u.is_guest,
                u.is_deactivated,
                u.is_admin,
                u.is_locked,
                u.is_suspended,
            ),
            None => (false, false, false, false, false),
        };
        if deactivated {
            return Err(AuthError::Deactivated);
        }
        // An access token that came out of a refresh is in use, so the client has the answer
        // to that refresh: the refresh token it replaced ends here, and the access tokens
        // of that one with it. Until now they were kept, for a client that lost the answer.
        if let Some(refresh_token) = &t.refresh_token {
            for replaced in self
                .store
                .revoke_refresh_token_predecessor(refresh_token)
                .await?
            {
                self.cache.remove(&replaced);
            }
        }
        if let Some(device_id) = &t.device_id {
            self.touch(&t.user_id, device_id);
        }
        let valid_until = t.expires_ts.map_or(far, |exp| {
            let left = u64::try_from(exp.saturating_sub(now_ts())).unwrap_or(0);
            Instant::now() + Duration::from_millis(left)
        });
        Ok((
            Sender {
                user_id,
                device_id: t.device_id,
                is_guest,
                appservice_id: None,
                token: token.to_owned(),
                is_admin,
                locked,
                suspended,
            },
            valid_until,
        ))
    }
}

/// An appservice's `as_token`, the appservice it belongs to, and its sender user.
#[derive(Clone, Debug)]
pub struct AppserviceToken {
    pub token: String,
    pub id: String,
    pub user_id: OwnedUserId,
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use asaroton_store::{AccessToken, DynStore, NewUser, PostgresStore, UserStore, now_ts};
    use ruma::{device_id, user_id};

    use super::{AuthError, Authenticator};

    #[sqlx::test(migrator = "asaroton_store::postgres::MIGRATOR")]
    async fn a_lock_or_suspension_shows_once_the_user_is_forgotten(pool: sqlx::PgPool) {
        let store = Arc::new(PostgresStore::from_pool(pool));
        let alice = user_id!("@alice:example.org");
        store
            .create_user(NewUser {
                user_id: alice.to_owned(),
                password_hash: None,
                display_name: None,
                is_admin: false,
                is_guest: false,
                appservice_id: None,
            })
            .await
            .unwrap();
        store
            .create_device(alice, device_id!("DEV"), None)
            .await
            .unwrap();
        store
            .create_access_token(AccessToken {
                token: "tok".into(),
                user_id: alice.to_owned(),
                device_id: Some(device_id!("DEV").to_owned()),
                created_ts: now_ts(),
                expires_ts: None,
                refresh_token: None,
                puppets_user_id: None,
            })
            .await
            .unwrap();
        let dyn_store: DynStore = store.clone();
        let auth = Authenticator::new(dyn_store);
        let sender = auth.authenticate("tok").await.unwrap();
        assert!(!sender.locked && !sender.suspended);

        // The sender is cached: a flag changed in the store shows once the user's entries are
        // dropped, which is what the service does with every change.
        store.set_locked(alice, true).await.unwrap();
        assert!(auth.authenticate("tok").await.is_ok(), "the cached sender");
        auth.forget_user(alice);
        assert!(matches!(
            auth.authenticate("tok").await,
            Err(AuthError::Locked)
        ));
        let sender = auth.authenticate_allowing_locked("tok").await.unwrap();
        assert!(sender.locked, "a logout still gets the sender");
        assert!(
            store.get_access_token("tok").await.unwrap().is_some(),
            "a lock leaves the token in place"
        );

        store.set_locked(alice, false).await.unwrap();
        store.set_suspended(alice, true).await.unwrap();
        auth.forget_user(alice);
        let sender = auth.authenticate("tok").await.unwrap();
        assert!(!sender.locked && sender.suspended);
    }
}
