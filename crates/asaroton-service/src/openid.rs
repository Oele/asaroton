//! `OpenID` tokens: a client asks for one and hands it to a third party, which asks this
//! server over federation who the token belongs to.

use std::time::Duration;

use asaroton_store::now_ts;
use ruma::{OwnedUserId, UserId};

use crate::{Error, Result, Services};

/// How long an `OpenID` token stays valid.
pub const OPENID_TOKEN_LIFETIME: Duration = Duration::from_hours(1);

impl Services {
    /// Issues an `OpenID` token for the user.
    pub async fn create_openid_token(&self, user_id: &UserId) -> Result<String> {
        let token = asaroton_auth::generate_token("tso");
        let lifetime = i64::try_from(OPENID_TOKEN_LIFETIME.as_millis()).unwrap_or(i64::MAX);
        self.store
            .add_openid_token(&token, user_id, now_ts() + lifetime)
            .await?;
        Ok(token)
    }

    /// The owner of one of this server's `OpenID` tokens, if it is valid: not expired, and its
    /// account neither locked (v1.12) nor suspended (v1.13), whose tokens prove nothing to
    /// widgets, integration managers or calls' SFUs while it lasts.
    pub async fn openid_token_owner(&self, token: &str) -> Result<Option<OwnedUserId>> {
        let Some(owner) = self.store.openid_token_owner(token, now_ts()).await? else {
            return Ok(None);
        };
        let usable = self
            .store
            .get_user(&owner)
            .await?
            .is_none_or(|u| !u.is_locked && !u.is_suspended);
        Ok(usable.then_some(owner))
    }

    /// The owner of an `OpenID` token, for the federation `openid/userinfo` endpoint.
    pub async fn openid_userinfo(&self, token: &str) -> Result<OwnedUserId> {
        self.openid_token_owner(token).await?.ok_or_else(|| {
            Error::custom(
                "M_UNKNOWN_TOKEN",
                http::StatusCode::UNAUTHORIZED,
                "Access token unknown or expired.",
            )
        })
    }
}
