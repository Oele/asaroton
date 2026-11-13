//! Accounts and sessions: registration, password login, devices and tokens.

use asaroton_auth::{Sender, generate_device_id, generate_token, hash_password, verify_password};
use asaroton_store::{AccessToken, Device, NewUser, RefreshToken, User, now_ts};
use ruma::{DeviceId, OwnedDeviceId, OwnedUserId, UserId};

use crate::{Error, Result, Services};

/// Access tokens from refreshable sessions live this long before a refresh is needed.
const REFRESHABLE_TOKEN_LIFETIME_MS: i64 = 60 * 60 * 1000;

/// A freshly minted session.
#[derive(Clone, Debug)]
pub struct Session {
    pub user_id: OwnedUserId,
    pub device_id: OwnedDeviceId,
    pub access_token: String,
    pub refresh_token: Option<String>,
    /// Milliseconds until the access token expires, when refreshable.
    pub expires_in_ms: Option<i64>,
}

/// Parameters for creating an account.
#[derive(Clone, Debug, Default)]
pub struct Registration {
    pub localpart: Option<String>,
    pub password: Option<String>,
    pub display_name: Option<String>,
    pub is_guest: bool,
    pub is_admin: bool,
    pub appservice_id: Option<String>,
}

impl Services {
    /// Validate a localpart per the spec's user ID grammar (historical IDs accepted on login
    /// only, not on registration).
    pub fn validate_localpart(&self, localpart: &str) -> Result<OwnedUserId> {
        // Like Synapse, capitals are folded rather than rejected.
        let lowered = localpart.to_lowercase();
        let localpart = lowered.as_str();
        if localpart.is_empty()
            || localpart.len() > 255 - self.config.server_name.as_str().len() - 2
        {
            return Err(Error::invalid_username("User ID has an invalid length."));
        }
        if !localpart.bytes().all(|b| {
            b.is_ascii_lowercase()
                || b.is_ascii_digit()
                || matches!(b, b'.' | b'_' | b'=' | b'-' | b'/' | b'+')
        }) {
            return Err(Error::invalid_username(
                "A user name is made of lowercase letters, digits and the characters . _ = - / + only.",
            ));
        }
        let id = format!("@{localpart}:{}", self.config.server_name);
        OwnedUserId::try_from(id).map_err(|e| Error::invalid_username(e.to_string()))
    }

    /// Whether a localpart is free.
    pub async fn is_username_available(&self, localpart: &str) -> Result<bool> {
        let user_id = self.validate_localpart(localpart)?;
        Ok(!self.store.user_exists(&user_id).await?)
    }

    /// Create an account. Does not create a device or token; see [`Services::create_session`].
    pub async fn register(&self, reg: Registration) -> Result<User> {
        let user_id = match &reg.localpart {
            Some(lp) => self.validate_localpart(lp)?,
            None => {
                // Pick a free random numeric localpart, like Synapse does for guests and
                // username-less registrations.
                let mut candidate;
                loop {
                    candidate = self.validate_localpart(&format!(
                        "{}",
                        rand::random::<u32>() % 1_000_000_000
                    ))?;
                    if !self.store.user_exists(&candidate).await? {
                        break;
                    }
                }
                candidate
            }
        };
        let password_hash = match &reg.password {
            Some(pw) => {
                let pw = pw.clone();
                Some(
                    tokio::task::spawn_blocking(move || hash_password(&pw))
                        .await
                        .map_err(|e| Error::internal(e.to_string()))?
                        .map_err(|e| Error::internal(e.to_string()))?,
                )
            }
            None => None,
        };
        let display_name = reg
            .display_name
            .or_else(|| Some(user_id.localpart().to_owned()));
        match self
            .store
            .create_user(NewUser {
                user_id,
                password_hash,
                display_name,
                is_admin: reg.is_admin,
                is_guest: reg.is_guest,
                appservice_id: reg.appservice_id,
            })
            .await
        {
            Ok(u) => Ok(u),
            Err(asaroton_store::Error::AlreadyExists(_)) => Err(Error::user_in_use()),
            Err(e) => Err(e.into()),
        }
    }

    /// Check a password for a user. Returns the user if it matches. A user ID whose localpart
    /// contains upper-case letters falls back to the lower-cased form, like Synapse.
    pub async fn check_password(&self, user_id: &UserId, password: &str) -> Result<Option<User>> {
        let mut candidate = user_id.to_owned();
        if self.store.get_password_hash(&candidate).await?.is_none()
            && user_id.localpart().chars().any(char::is_uppercase)
            && let Ok(folded) = OwnedUserId::try_from(format!(
                "@{}:{}",
                user_id.localpart().to_lowercase(),
                user_id.server_name()
            ))
        {
            candidate = folded;
        }
        let user_id = candidate.as_ref();
        let Some(hash) = self.store.get_password_hash(user_id).await? else {
            return Ok(None);
        };
        let password = password.to_owned();
        let legacy = asaroton_auth::is_bcrypt(&hash);
        let (ok, rehashed) = tokio::task::spawn_blocking(move || {
            let ok = verify_password(&hash, &password);
            // A bcrypt hash from an imported Synapse account becomes argon2 once it is known
            // to match.
            let rehashed = (ok && legacy)
                .then(|| hash_password(&password).ok())
                .flatten();
            (ok, rehashed)
        })
        .await
        .map_err(|e| Error::internal(e.to_string()))?;
        if !ok {
            return Ok(None);
        }
        if let Some(new_hash) = rehashed {
            self.store
                .set_password_hash(user_id, Some(&new_hash))
                .await?;
        }
        self.store.get_user(user_id).await.map_err(Into::into)
    }

    /// Resolve a login identifier (`@user:server` or bare localpart) to a local user ID.
    pub fn resolve_user_identifier(&self, ident: &str) -> Result<OwnedUserId> {
        if ident.starts_with('@') {
            let uid =
                OwnedUserId::try_from(ident).map_err(|e| Error::invalid_param(e.to_string()))?;
            if uid.server_name() != self.config.server_name {
                return Err(Error::forbidden("Invalid username or password."));
            }
            Ok(uid)
        } else {
            let lp = ident.to_lowercase();
            OwnedUserId::try_from(format!("@{lp}:{}", self.config.server_name))
                .map_err(|e| Error::invalid_param(e.to_string()))
        }
    }

    /// Create (or reuse) a device and mint tokens for it.
    pub async fn create_session(
        &self,
        user_id: &UserId,
        device_id: Option<OwnedDeviceId>,
        initial_display_name: Option<&str>,
        refreshable: bool,
    ) -> Result<Session> {
        let device_id = device_id.unwrap_or_else(generate_device_id);
        self.store
            .create_device(user_id, &device_id, initial_display_name)
            .await?;
        let now = now_ts();
        let access_token = generate_token("tsa");
        let (refresh_token, expires_ts) = if refreshable {
            let rt = generate_token("tsr");
            self.store
                .create_refresh_token(RefreshToken {
                    token: rt.clone(),
                    user_id: user_id.to_owned(),
                    device_id: device_id.clone(),
                    created_ts: now,
                    expires_ts: None,
                    next_token: None,
                })
                .await?;
            (Some(rt), Some(now + REFRESHABLE_TOKEN_LIFETIME_MS))
        } else {
            (None, None)
        };
        self.store
            .create_access_token(AccessToken {
                token: access_token.clone(),
                user_id: user_id.to_owned(),
                device_id: Some(device_id.clone()),
                created_ts: now,
                expires_ts,
                refresh_token: refresh_token.clone(),
                puppets_user_id: None,
            })
            .await?;
        Ok(Session {
            user_id: user_id.to_owned(),
            device_id,
            access_token,
            refresh_token,
            expires_in_ms: expires_ts.map(|_| REFRESHABLE_TOKEN_LIFETIME_MS),
        })
    }

    /// Exchange a refresh token for a new access + refresh token pair.
    ///
    /// The refresh token stays good until the client has used what it got for it, the new
    /// access token or the new refresh token (the spec asks for that): a client that lost
    /// the answer asks again with the token it still has. It then gets the same pair again
    /// and not a second one, so that an answer that did arrive stays good as well. The access
    /// token from before the refresh lives until then too, or until it expires.
    pub async fn refresh_session(&self, refresh_token: &str) -> Result<Session> {
        // One at a time: a second attempt that meets the first still at work waits for it.
        let _turn = self.refresh_turn(refresh_token).await;
        let Some(rt) = self.store.get_refresh_token(refresh_token).await? else {
            return Err(Error::matrix(
                ruma::api::error::ErrorKind::UnknownToken(unknown_token_data(false)),
                http::StatusCode::UNAUTHORIZED,
                "Unknown refresh token.",
            ));
        };
        if let Some(exp) = rt.expires_ts
            && exp <= now_ts()
        {
            return Err(Error::forbidden("Refresh token expired."));
        }
        // This token is in use, so the client has the answer to the refresh that handed it
        // out: the token it replaced ends here.
        for replaced in self
            .store
            .revoke_refresh_token_predecessor(&rt.token)
            .await?
        {
            self.auth.forget_token(&replaced);
        }
        let now = now_ts();
        let next = match rt.next_token {
            Some(next) if self.store.get_refresh_token(&next).await?.is_some() => next,
            _ => {
                let next = generate_token("tsr");
                self.store
                    .create_refresh_token(RefreshToken {
                        token: next.clone(),
                        user_id: rt.user_id.clone(),
                        device_id: rt.device_id.clone(),
                        created_ts: now,
                        expires_ts: None,
                        next_token: None,
                    })
                    .await?;
                self.store.set_refresh_token_next(&rt.token, &next).await?;
                next
            }
        };
        let access = match self.store.access_token_for_refresh_token(&next).await? {
            Some(access) => access,
            None => {
                let access = AccessToken {
                    token: generate_token("tsa"),
                    user_id: rt.user_id.clone(),
                    device_id: Some(rt.device_id.clone()),
                    created_ts: now,
                    expires_ts: Some(now + REFRESHABLE_TOKEN_LIFETIME_MS),
                    refresh_token: Some(next.clone()),
                    puppets_user_id: None,
                };
                self.store.create_access_token(access.clone()).await?;
                access
            }
        };
        Ok(Session {
            user_id: rt.user_id,
            device_id: rt.device_id,
            access_token: access.token,
            refresh_token: Some(next),
            expires_in_ms: access.expires_ts.map(|expires| expires - now),
        })
    }

    /// Invalidate one session (the token's device is deleted too, per spec).
    pub async fn logout(&self, sender: &Sender) -> Result<()> {
        self.store.delete_access_token(&sender.token).await?;
        self.auth.forget_token(&sender.token);
        if let Some(device_id) = &sender.device_id {
            self.delete_device_fully(&sender.user_id, device_id).await?;
        }
        Ok(())
    }

    /// Invalidate every session of the user.
    pub async fn logout_all(&self, user_id: &UserId) -> Result<()> {
        for d in self.store.list_devices(user_id).await? {
            self.store.delete_device(user_id, &d.device_id).await?;
        }
        self.store
            .delete_access_tokens_for_user(user_id, None)
            .await?;
        self.auth.forget_user(user_id);
        Ok(())
    }

    /// Change a password, optionally logging out every other device.
    pub async fn change_password(
        &self,
        user_id: &UserId,
        new_password: &str,
        logout_others: bool,
        keep_device: Option<&DeviceId>,
        keep_token: Option<&str>,
    ) -> Result<()> {
        let pw = new_password.to_owned();
        let hash = tokio::task::spawn_blocking(move || hash_password(&pw))
            .await
            .map_err(|e| Error::internal(e.to_string()))?
            .map_err(|e| Error::internal(e.to_string()))?;
        self.store.set_password_hash(user_id, Some(&hash)).await?;
        if logout_others {
            self.store
                .delete_pushers_except_token(user_id, keep_token)
                .await?;
            for d in self.store.list_devices(user_id).await? {
                if Some(d.device_id.as_ref()) != keep_device {
                    self.store.delete_device(user_id, &d.device_id).await?;
                }
            }
            self.store
                .delete_access_tokens_for_user(user_id, keep_device)
                .await?;
            self.auth.forget_user(user_id);
        }
        Ok(())
    }

    /// Deactivate an account: no more logins, all sessions gone.
    pub async fn deactivate(&self, user_id: &UserId) -> Result<()> {
        self.store.set_password_hash(user_id, None).await?;
        self.store.set_deactivated(user_id, true).await?;
        self.auth.forget_user(user_id);
        self.logout_all(user_id).await
    }

    /// Devices of a user, newest last.
    pub async fn devices(&self, user_id: &UserId) -> Result<Vec<Device>> {
        Ok(self.store.list_devices(user_id).await?)
    }
}

fn unknown_token_data(soft_logout: bool) -> ruma::api::error::UnknownTokenErrorData {
    let mut data = ruma::api::error::UnknownTokenErrorData::new();
    data.soft_logout = soft_logout;
    data
}

impl Services {
    /// Delete a device, its tokens and keys, and announce the device-list change.
    pub async fn delete_device_fully(&self, user_id: &UserId, device_id: &DeviceId) -> Result<()> {
        self.store.delete_device(user_id, device_id).await?;
        self.store.delete_device_keys(user_id, device_id).await?;
        self.auth.forget_user(user_id);
        // The device's local notification settings go with it (MSC3890).
        for ty in [
            format!("m.local_notification_settings.{device_id}"),
            format!("org.matrix.msc3890.local_notification_settings.{device_id}"),
        ] {
            if let Some(at) = self.store.delete_account_data(user_id, None, &ty).await? {
                self.wake_when_reportable(
                    asaroton_store::SyncStream::AccountData,
                    at,
                    vec![crate::Notification::User {
                        user_id: user_id.to_owned(),
                    }],
                );
            }
        }
        self.notify_device_list_change(user_id, Some(device_id))
            .await
    }
}
