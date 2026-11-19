//! Sign-in through OAuth 2.0 (`[oauth]`): lithostroton, the authorization server that runs in
//! this process on the client listener, and what it asks of asaroton.
//!
//! asaroton stays the owner of users, devices and tokens: lithostroton checks passwords,
//! creates sessions, refreshes and ends them through [`AsarotonHost`], and the tokens it hands
//! out are asaroton's own, so the usual token check accepts them. Password login keeps working
//! beside it. lithostroton's own state (registered clients, sign-ins in progress, codes,
//! browser sessions) lives in the `lithostroton_store` table through [`KvStore`].

use std::{
    net::IpAddr,
    sync::{Arc, OnceLock},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use asaroton_service::Services;
use asaroton_store::{DynStore, now_ts};
use async_trait::async_trait;
use lithostroton::{
    Device, Embedded, Host, HostError, NewSession, Session, Store, StoreError, TokenKind,
    TokenStatus, Tokens, User,
};
use ruma::{DeviceId, OwnedDeviceId, OwnedUserId, UserId};

use crate::config::Config;

/// Starts lithostroton when `[oauth]` is enabled, and publishes its metadata for
/// `/_matrix/client/v1/auth_metadata`. The router goes on the client listener under
/// `oauth.path`.
pub async fn start(services: &Arc<Services>, config: &Config) -> anyhow::Result<Option<Embedded>> {
    if !config.oauth.enabled {
        return Ok(None);
    }
    anyhow::ensure!(
        !config.mas.enabled,
        "[oauth] and [mas] cannot both be enabled: sign-in is either served here or by the service"
    );
    let path = oauth_path(config)?;
    let issuer = url::Url::parse(&format!("{}{path}/", config.public_base_url()))?;
    let mut oauth = lithostroton::Config::new(issuer, config.server_name.clone());
    oauth.secret_key = Some(secret_key(&services.store).await?);
    oauth.device_authorization = config.oauth.device_authorization;
    let embedded = Embedded::start(
        oauth,
        Arc::new(AsarotonHost {
            services: services.clone(),
        }),
        Arc::new(KvStore {
            store: services.store.clone(),
        }),
    )
    .await?;
    let _ = services.oauth_metadata.set(embedded.metadata());
    Ok(Some(embedded))
}

/// `oauth.path` without a trailing slash, checked.
pub fn oauth_path(config: &Config) -> anyhow::Result<&str> {
    let path = config.oauth.path.trim_end_matches('/');
    anyhow::ensure!(
        path.starts_with('/') && path.len() > 1 && !path.starts_with("/_matrix"),
        "oauth.path must be an absolute path outside /_matrix, like /_lithostroton"
    );
    Ok(path)
}

/// The key lithostroton signs its form tokens with, kept in its own table so that forms open
/// across a restart keep working.
async fn secret_key(store: &DynStore) -> anyhow::Result<[u8; 32]> {
    let fresh: [u8; 32] = rand::random();
    store
        .oauth_kv_insert("asaroton", "form-key", &fresh, None)
        .await?;
    let stored = store
        .oauth_kv_get("asaroton", "form-key")
        .await?
        .ok_or_else(|| anyhow::anyhow!("the form key went missing"))?;
    stored
        .try_into()
        .map_err(|_| anyhow::anyhow!("the stored form key is not 32 bytes"))
}

/// What lithostroton asks of asaroton.
pub struct AsarotonHost {
    services: Arc<Services>,
}

fn failed(e: impl std::fmt::Display) -> HostError {
    HostError::Failed(e.to_string())
}

fn parse_user(user_id: &str) -> Option<OwnedUserId> {
    OwnedUserId::try_from(user_id).ok()
}

/// A hash to check a password against when the user does not exist, so that a refusal takes
/// as long either way.
static DUMMY_HASH: OnceLock<String> = OnceLock::new();

async fn verify_against_dummy(password: &str) {
    let password = password.to_owned();
    let _ = tokio::task::spawn_blocking(move || {
        let hash = DUMMY_HASH.get_or_init(|| {
            asaroton_auth::hash_password("lithostroton: no such user").unwrap_or_default()
        });
        asaroton_auth::verify_password(hash, &password)
    })
    .await;
}

impl AsarotonHost {
    /// Whether asaroton has a password for the user, as `check_password` looks it up (with the
    /// lower-cased localpart as a fallback).
    async fn has_password(&self, user_id: &UserId) -> Result<bool, HostError> {
        let store = &self.services.store;
        if store
            .get_password_hash(user_id)
            .await
            .map_err(failed)?
            .is_some()
        {
            return Ok(true);
        }
        let lower = user_id.localpart().to_lowercase();
        if lower == user_id.localpart() {
            return Ok(false);
        }
        let Some(folded) = parse_user(&format!("@{lower}:{}", user_id.server_name())) else {
            return Ok(false);
        };
        Ok(store
            .get_password_hash(&folded)
            .await
            .map_err(failed)?
            .is_some())
    }

    /// The session a device of a user stands for, if the device exists.
    async fn session(
        &self,
        user_id: &UserId,
        device_id: &DeviceId,
    ) -> Result<Option<Session>, HostError> {
        let Some((client_id, scope)) = self
            .services
            .store
            .device_oauth(user_id, device_id)
            .await
            .map_err(failed)?
        else {
            return Ok(None);
        };
        Ok(Some(Session {
            user_id: user_id.to_string(),
            device_id: device_id.to_string(),
            client_id,
            scope,
        }))
    }
}

fn tokens(session: asaroton_service::account::Session) -> Result<Tokens, HostError> {
    let refresh_token = session
        .refresh_token
        .ok_or_else(|| failed("a session without a refresh token"))?;
    Ok(Tokens {
        access_token: session.access_token,
        refresh_token,
        expires_in: Duration::from_millis(session.expires_in_ms.unwrap_or(0).unsigned_abs()),
    })
}

fn expired(expires_ts: Option<i64>) -> bool {
    expires_ts.is_some_and(|e| e <= now_ts())
}

#[async_trait]
impl Host for AsarotonHost {
    async fn check_password(
        &self,
        username: &str,
        password: &str,
    ) -> Result<Option<User>, HostError> {
        let Ok(user_id) = self.services.resolve_user_identifier(username.trim()) else {
            verify_against_dummy(password).await;
            return Ok(None);
        };
        if !self.has_password(&user_id).await? {
            verify_against_dummy(password).await;
            return Ok(None);
        }
        match self.services.check_password(&user_id, password).await {
            Ok(Some(u)) if !u.is_deactivated => Ok(Some(User {
                user_id: u.user_id.to_string(),
                display_name: u.display_name,
            })),
            Ok(_) => Ok(None),
            Err(e) => Err(failed(e)),
        }
    }

    async fn user(&self, user_id: &str) -> Result<Option<User>, HostError> {
        let Some(user_id) = parse_user(user_id) else {
            return Ok(None);
        };
        let user = self
            .services
            .store
            .get_user(&user_id)
            .await
            .map_err(failed)?;
        Ok(user.filter(|u| !u.is_deactivated).map(|u| User {
            user_id: u.user_id.to_string(),
            display_name: u.display_name,
        }))
    }

    async fn create_session(&self, new: NewSession) -> Result<Tokens, HostError> {
        let user_id = parse_user(&new.user_id).ok_or_else(|| failed("not a user ID"))?;
        let device_id: OwnedDeviceId = new.device_id.as_str().into();
        let store = &self.services.store;
        // A device that exists keeps its keys and loses its tokens, as the user agreed to on
        // the consent page.
        if store
            .device_oauth(&user_id, &device_id)
            .await
            .map_err(failed)?
            .is_some()
        {
            store
                .delete_access_tokens_for_device(&user_id, &device_id)
                .await
                .map_err(failed)?;
            store
                .delete_refresh_tokens_for_device(&user_id, &device_id)
                .await
                .map_err(failed)?;
            self.services.auth.forget_user(&user_id);
        }
        let session = self
            .services
            .create_session(
                &user_id,
                Some(device_id.clone()),
                new.client_name.as_deref(),
                true,
            )
            .await
            .map_err(failed)?;
        store
            .set_device_oauth(&user_id, &device_id, Some(&new.client_id), Some(&new.scope))
            .await
            .map_err(failed)?;
        tokens(session)
    }

    async fn token_status(&self, token: &str) -> Result<TokenStatus, HostError> {
        let store = &self.services.store;
        if let Some(at) = store.get_access_token(token).await.map_err(failed)? {
            let (Some(device_id), None) = (&at.device_id, &at.puppets_user_id) else {
                return Ok(TokenStatus::Unknown);
            };
            if expired(at.expires_ts) {
                return Ok(TokenStatus::Unknown);
            }
            return Ok(match self.session(&at.user_id, device_id).await? {
                Some(session) => TokenStatus::Active {
                    kind: TokenKind::Access,
                    session,
                },
                None => TokenStatus::Unknown,
            });
        }
        if let Some(rt) = store.get_refresh_token(token).await.map_err(failed)? {
            if expired(rt.expires_ts) {
                return Ok(TokenStatus::Unknown);
            }
            return Ok(match self.session(&rt.user_id, &rt.device_id).await? {
                Some(session) => TokenStatus::Active {
                    kind: TokenKind::Refresh,
                    session,
                },
                None => TokenStatus::Unknown,
            });
        }
        // A replaced refresh token is deleted once its successor is used, so it is not told
        // apart from an unknown one (the session survives a replay).
        Ok(TokenStatus::Unknown)
    }

    async fn refresh_session(&self, refresh_token: &str) -> Result<Option<Tokens>, HostError> {
        match self.services.refresh_session(refresh_token).await {
            Ok(session) => tokens(session).map(Some),
            // Unknown or expired: the client has to sign in again.
            Err(asaroton_service::Error::Matrix { status, .. }) if status.is_client_error() => {
                Ok(None)
            }
            Err(e) => Err(failed(e)),
        }
    }

    async fn end_session(&self, user_id: &str, device_id: &str) -> Result<(), HostError> {
        let Some(user_id) = parse_user(user_id) else {
            return Ok(());
        };
        let device_id: OwnedDeviceId = device_id.into();
        if self
            .services
            .store
            .device_oauth(&user_id, &device_id)
            .await
            .map_err(failed)?
            .is_none()
        {
            return Ok(());
        }
        self.services
            .delete_device_fully(&user_id, &device_id)
            .await
            .map_err(failed)
    }

    async fn devices(&self, user_id: &str) -> Result<Vec<Device>, HostError> {
        let Some(user_id) = parse_user(user_id) else {
            return Ok(Vec::new());
        };
        let store = &self.services.store;
        let mut out = Vec::new();
        for d in store.list_devices(&user_id).await.map_err(failed)? {
            let client_id = store
                .device_oauth(&user_id, &d.device_id)
                .await
                .map_err(failed)?
                .and_then(|(client, _)| client);
            out.push(Device {
                device_id: d.device_id.to_string(),
                display_name: d.display_name,
                last_seen: d
                    .last_seen_ts
                    .and_then(|ms| u64::try_from(ms).ok())
                    .map(|ms| UNIX_EPOCH + Duration::from_millis(ms)),
                last_seen_ip: d.last_seen_ip,
                client_id,
            });
        }
        Ok(out)
    }

    async fn set_display_name(
        &self,
        user_id: &str,
        display_name: Option<&str>,
    ) -> Result<(), HostError> {
        let user_id = parse_user(user_id).ok_or_else(|| failed("not a user ID"))?;
        self.services
            .set_display_name(&user_id, display_name)
            .await
            .map_err(failed)
    }

    async fn allow_cross_signing_reset(&self, user_id: &str) -> Result<(), HostError> {
        let user_id = parse_user(user_id).ok_or_else(|| failed("not a user ID"))?;
        self.services
            .cross_signing_reset_allowed
            .insert(user_id, Instant::now());
        Ok(())
    }

    /// A password change from the account pages (the current password was checked). The
    /// user's other sessions stay signed in, as a change through the API without
    /// `logout_devices` does; a password the server refuses comes back with the reason.
    async fn set_password(&self, user_id: &str, new_password: &str) -> Result<(), HostError> {
        let user_id = parse_user(user_id).ok_or_else(|| failed("not a user ID"))?;
        match self
            .services
            .change_password(&user_id, new_password, false, None, None)
            .await
        {
            Ok(()) => Ok(()),
            Err(asaroton_service::Error::Matrix {
                status, message, ..
            }) if status.is_client_error() => Err(HostError::Refused(message)),
            Err(e) => Err(failed(e)),
        }
    }

    async fn authenticate(&self, access_token: &str) -> Result<Option<String>, HostError> {
        match self.services.auth.authenticate(access_token).await {
            Ok(sender) => Ok(Some(sender.user_id.to_string())),
            Err(
                asaroton_auth::AuthError::UnknownToken { .. }
                | asaroton_auth::AuthError::Expired
                | asaroton_auth::AuthError::Deactivated,
            ) => Err(HostError::Refused(
                "The access token is unknown or has expired.".into(),
            )),
            Err(e) => Err(failed(e)),
        }
    }

    /// The last `X-Forwarded-For` entry when asaroton trusts its reverse proxy (as its own
    /// rate limits do), otherwise the connection's peer.
    fn client_ip(&self, request: &http::request::Parts) -> Option<IpAddr> {
        if self.services.config.rate_limits.trust_x_forwarded_for
            && let Some(ip) = request
                .headers
                .get("x-forwarded-for")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.rsplit(',').next())
                .and_then(|last| last.trim().parse().ok())
        {
            return Some(ip);
        }
        request
            .extensions
            .get::<axum::extract::ConnectInfo<std::net::SocketAddr>>()
            .map(|c| c.0.ip())
    }
}

/// lithostroton's key-value state in asaroton's database (`lithostroton_store`).
pub struct KvStore {
    store: DynStore,
}

fn secs(t: Option<SystemTime>) -> Option<f64> {
    t.map(|t| {
        t.duration_since(UNIX_EPOCH)
            .map_or(0.0, |d| d.as_secs_f64())
    })
}

fn store_failed(e: impl std::fmt::Display) -> StoreError {
    StoreError(e.to_string())
}

#[async_trait]
impl Store for KvStore {
    async fn insert(
        &self,
        kind: &str,
        key: &str,
        value: &[u8],
        expires_at: Option<SystemTime>,
    ) -> Result<bool, StoreError> {
        self.store
            .oauth_kv_insert(kind, key, value, secs(expires_at))
            .await
            .map_err(store_failed)
    }

    async fn get(&self, kind: &str, key: &str) -> Result<Option<Vec<u8>>, StoreError> {
        self.store
            .oauth_kv_get(kind, key)
            .await
            .map_err(store_failed)
    }

    async fn replace(
        &self,
        kind: &str,
        key: &str,
        expected: &[u8],
        value: &[u8],
        expires_at: Option<SystemTime>,
    ) -> Result<bool, StoreError> {
        self.store
            .oauth_kv_replace(kind, key, expected, value, secs(expires_at))
            .await
            .map_err(store_failed)
    }

    async fn take(&self, kind: &str, key: &str) -> Result<Option<Vec<u8>>, StoreError> {
        self.store
            .oauth_kv_take(kind, key)
            .await
            .map_err(store_failed)
    }

    async fn delete(&self, kind: &str, key: &str) -> Result<(), StoreError> {
        self.store
            .oauth_kv_delete(kind, key)
            .await
            .map_err(store_failed)
    }

    async fn purge_expired(&self) -> Result<(), StoreError> {
        self.store
            .oauth_kv_purge_expired()
            .await
            .map_err(store_failed)
    }
}
