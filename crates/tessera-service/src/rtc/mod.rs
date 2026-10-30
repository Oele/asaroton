//! `MatrixRTC` over `LiveKit` (MSC4195): the homeserver hands out the SFU access tokens, checks
//! room membership for them, creates the `LiveKit` rooms, exchanges tokens over federation for
//! remote SFUs, removes participants that leave the Matrix room, and keeps a delegated
//! delayed leave event alive while the participant stays connected.
//!
//! Two generations of clients are served. Current ones discover the SFU through
//! `/rtc/transports` and call the `get_token` endpoints with their access token. Older ones
//! find a `livekit_service_url` in `.well-known/matrix/client` and talk to the legacy
//! `/sfu/get`, `/get_token` and `/delegate_delayed_leave` endpoints with an `OpenID` token; this
//! server serves those too, at the same URL prefix as the new endpoints.

pub mod delegation;
pub mod jwt;
pub mod livekit;

use std::sync::Arc;

use base64::{Engine, engine::general_purpose::STANDARD_NO_PAD};
use ruma::{OwnedRoomId, OwnedServerName, OwnedUserId, RoomId, ServerName, UserId};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tessera_core::Pdu;
use tessera_store::{DelegatedLeave, RtcParticipant, now_ts};

use crate::{Error, Result, Services, config::LivekitSettings, delayed_events::UpdateAction};

/// The transport type identifier. The MSC says `m.livekit`; every implementation uses
/// `livekit`, so that is what clients look for.
pub const TRANSPORT_TYPE: &str = "livekit";

/// The slot the legacy `/sfu/get` endpoint predates: Element Call's single per-room call.
const LEGACY_SLOT_ID: &str = "m.call#ROOM";

/// The federation token endpoint, at the unstable path every current implementation serves.
pub const FEDERATION_GET_TOKEN_PATH: &str =
    "/_matrix/federation/unstable/io.element.msc4195/rtc/livekit/get_token";
/// The stable path of the federation token endpoint.
pub const FEDERATION_GET_TOKEN_STABLE_PATH: &str = "/_matrix/federation/v1/rtc/livekit/get_token";

/// How long an SFU access token stays valid for joining.
const TOKEN_TTL_SECS: i64 = 60 * 60;

/// Runtime state for the `LiveKit` integration.
#[derive(Debug, Default)]
pub struct RtcState {
    /// The SFU's management API, when a `LiveKit` transport is configured.
    pub api: Option<Arc<livekit::LivekitApi>>,
    /// Wakes the delegation worker after a delegation is added.
    pub wake: tokio::sync::Notify,
}

impl RtcState {
    pub fn new(settings: Option<&LivekitSettings>) -> Self {
        Self {
            api: settings.map(|s| {
                Arc::new(livekit::LivekitApi::new(
                    &s.api_url,
                    &s.api_key,
                    &s.api_secret,
                ))
            }),
            wake: tokio::sync::Notify::new(),
        }
    }
}

/// Unpadded base64 of the SHA-256 of the JSON array of `parts`.
fn hash_of(parts: &[&str]) -> String {
    let json = serde_json::to_vec(parts).unwrap_or_default();
    STANDARD_NO_PAD.encode(Sha256::digest(json))
}

/// The `LiveKit` room name for a slot of a Matrix room.
pub fn livekit_room_name(room_id: &RoomId, slot_id: &str) -> String {
    hash_of(&[room_id.as_str(), slot_id])
}

/// The `LiveKit` participant identity of a member of a slot.
pub fn livekit_identity(user_id: &UserId, member_id: &str) -> String {
    hash_of(&[user_id.as_str(), member_id])
}

/// The identity an earlier draft of the proposal assigned, still used by the legacy
/// `/get_token` endpoint.
pub fn legacy_hashed_identity(user_id: &UserId, device_id: &str, member_id: &str) -> String {
    hash_of(&[user_id.as_str(), device_id, member_id])
}

/// The identity the legacy `/sfu/get` endpoint assigns.
pub fn legacy_identity(user_id: &UserId, device_id: &str) -> String {
    format!("{user_id}:{device_id}")
}

/// `POST /rtc/livekit/get_token`.
#[derive(Clone, Debug, Deserialize)]
pub struct GetTokenRequest {
    /// The homeserver that operates the SFU; this server when omitted.
    #[serde(default)]
    pub server_name: Option<OwnedServerName>,
    pub url: String,
    pub room_id: OwnedRoomId,
    pub slot_id: String,
    pub member_id: String,
}

/// `POST /_matrix/federation/v1/rtc/livekit/get_token`.
#[derive(Clone, Debug, Deserialize, serde::Serialize)]
pub struct FederationGetTokenRequest {
    pub url: String,
    pub user_id: OwnedUserId,
    pub room_id: OwnedRoomId,
    pub slot_id: String,
    pub member_id: String,
}

/// `POST /rtc/livekit/delegate_delayed_leave`.
#[derive(Clone, Debug, Deserialize)]
pub struct DelegateRequest {
    pub url: String,
    pub room_id: OwnedRoomId,
    pub slot_id: String,
    pub member_id: String,
    pub delay_id: String,
}

/// An `OpenID` token as the client-server API hands it out.
#[derive(Clone, Debug, Deserialize)]
pub struct OpenIdToken {
    pub access_token: String,
    pub matrix_server_name: OwnedServerName,
}

/// `member` of the legacy `/get_token` and `/delegate_delayed_leave` bodies.
#[derive(Clone, Debug, Deserialize)]
pub struct LegacyMember {
    pub id: String,
    pub claimed_user_id: OwnedUserId,
    pub claimed_device_id: String,
}

/// The legacy `/sfu/get` body.
#[derive(Clone, Debug, Deserialize)]
pub struct LegacySfuRequest {
    pub room: OwnedRoomId,
    pub openid_token: OpenIdToken,
    pub device_id: String,
    #[serde(default)]
    pub delay_id: Option<String>,
}

/// The legacy `/get_token` body.
#[derive(Clone, Debug, Deserialize)]
pub struct LegacyGetTokenRequest {
    pub room_id: OwnedRoomId,
    pub slot_id: String,
    pub openid_token: OpenIdToken,
    pub member: LegacyMember,
    #[serde(default)]
    pub delay_id: Option<String>,
}

/// The legacy `/delegate_delayed_leave` body.
#[derive(Clone, Debug, Deserialize)]
pub struct LegacyDelegateRequest {
    pub room_id: OwnedRoomId,
    pub slot_id: String,
    pub openid_token: OpenIdToken,
    pub member: LegacyMember,
    pub delay_id: String,
}

/// The part of a member event's content the SFU integration looks at.
#[derive(Deserialize)]
struct MemberContent {
    membership: String,
}

/// A token plus the SFU it is for.
#[derive(Clone, Debug)]
pub struct SfuToken {
    pub url: String,
    pub jwt: String,
}

fn not_configured() -> Error {
    Error::not_found("This server has no LiveKit transport.")
}

fn api_error(e: &livekit::ApiError) -> Error {
    tracing::warn!(error = %e, "LiveKit API call failed");
    Error::custom(
        "M_UNKNOWN",
        http::StatusCode::BAD_GATEWAY,
        "The SFU could not be reached.",
    )
}

impl Services {
    fn livekit_settings(&self) -> Result<&LivekitSettings> {
        self.config.rtc.as_ref().ok_or_else(not_configured)
    }

    fn livekit_api(&self) -> Result<&Arc<livekit::LivekitApi>> {
        self.rtc.api.as_ref().ok_or_else(not_configured)
    }

    /// Whether a `LiveKit` transport is configured.
    pub fn has_rtc(&self) -> bool {
        self.config.rtc.is_some()
    }

    /// The transports this server offers, for `/rtc/transports`.
    pub fn rtc_transports(&self) -> Vec<Value> {
        self.config
            .rtc
            .as_ref()
            .map(|s| json!({ "type": TRANSPORT_TYPE, "url": s.url }))
            .into_iter()
            .collect()
    }

    /// The `org.matrix.msc4143.rtc_foci` entry for `.well-known/matrix/client`, for clients
    /// that predate `/rtc/transports`.
    pub fn rtc_foci(&self) -> Vec<Value> {
        self.config
            .rtc
            .as_ref()
            .map(|s| {
                json!({
                    "type": TRANSPORT_TYPE,
                    "livekit_service_url": self.legacy_service_url(),
                    "url": s.url,
                })
            })
            .into_iter()
            .collect()
    }

    /// Where the legacy service endpoints live.
    pub fn legacy_service_url(&self) -> String {
        format!(
            "{}/_matrix/client/unstable/io.element.msc4195/rtc/livekit",
            self.config.public_base_url
        )
    }

    fn require_local_sfu(&self, url: &str) -> Result<&LivekitSettings> {
        let settings = self.livekit_settings()?;
        if url.trim_end_matches('/') != settings.url.trim_end_matches('/') {
            return Err(Error::invalid_param("Not one of this server's SFUs."));
        }
        Ok(settings)
    }

    async fn require_user_joined(&self, room_id: &RoomId, user_id: &UserId) -> Result<()> {
        if self.is_joined(room_id, user_id).await? {
            Ok(())
        } else {
            Err(Error::forbidden(
                "The requesting user is not a member of the room.",
            ))
        }
    }

    async fn require_server_in_room(&self, room_id: &RoomId, server: &ServerName) -> Result<()> {
        let servers = self.store.servers_in_room(room_id).await?;
        if servers.iter().any(|s| s == server) {
            Ok(())
        } else if server == self.config.server_name {
            Err(Error::forbidden("This server is not joined to the room."))
        } else {
            Err(Error::forbidden(
                "The SFU's server is not joined to the room.",
            ))
        }
    }

    /// Signs a token for `identity` to join `lk_room`, creates the room and remembers the
    /// participant.
    async fn issue_token(
        &self,
        user_id: &UserId,
        room_id: &RoomId,
        lk_room: &str,
        identity: &str,
        can_publish: bool,
    ) -> Result<String> {
        let api = self.livekit_api()?;
        let now = livekit::now_secs();
        let video = json!({
            "room": lk_room,
            "roomJoin": true,
            "roomCreate": false,
            "canPublish": can_publish,
            "canSubscribe": true,
            "canUpdateOwnMetadata": true,
            "hidden": false,
        });
        let jwt = jwt::sign(
            api.api_secret(),
            &jwt::livekit_claims(api.api_key(), Some(identity), &video, now, TOKEN_TTL_SECS),
        );
        api.create_room(lk_room).await.map_err(|e| api_error(&e))?;
        self.store
            .add_rtc_participant(&RtcParticipant {
                lk_room: lk_room.to_owned(),
                lk_identity: identity.to_owned(),
                user_id: user_id.to_owned(),
                room_id: room_id.to_owned(),
                issued_ts: now_ts(),
            })
            .await?;
        tracing::info!(%user_id, %room_id, lk_room, identity, can_publish, "issued SFU token");
        Ok(jwt)
    }

    /// A local user asks for a token, for this server's SFU or a remote one. A server without
    /// an SFU of its own still forwards requests for remote ones.
    pub async fn rtc_get_token(&self, user_id: &UserId, req: GetTokenRequest) -> Result<String> {
        self.require_user_joined(&req.room_id, user_id).await?;
        let server = req
            .server_name
            .clone()
            .unwrap_or_else(|| self.config.server_name.clone());
        if server == self.config.server_name {
            self.require_local_sfu(&req.url)?;
            let lk_room = livekit_room_name(&req.room_id, &req.slot_id);
            let identity = livekit_identity(user_id, &req.member_id);
            return self
                .issue_token(user_id, &req.room_id, &lk_room, &identity, true)
                .await;
        }
        self.require_server_in_room(&req.room_id, &server).await?;
        let body = FederationGetTokenRequest {
            url: req.url,
            user_id: user_id.to_owned(),
            room_id: req.room_id,
            slot_id: req.slot_id,
            member_id: req.member_id,
        };
        let resp = self
            .federation()
            .remote_livekit_token(&server, serde_json::to_value(&body)?)
            .await
            .map_err(|e| match e {
                Error::Matrix { status, .. } | Error::Custom { status, .. }
                    if status == http::StatusCode::FORBIDDEN
                        || status == http::StatusCode::BAD_REQUEST =>
                {
                    e
                }
                other => {
                    tracing::warn!(%server, error = %other, "remote SFU token request failed");
                    Error::custom(
                        "M_UNKNOWN",
                        http::StatusCode::BAD_GATEWAY,
                        "The remote server could not issue a token.",
                    )
                }
            })?;
        resp.get("jwt")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .ok_or_else(|| {
                Error::custom(
                    "M_UNKNOWN",
                    http::StatusCode::BAD_GATEWAY,
                    "The remote server sent no token.",
                )
            })
    }

    /// A remote server asks for a token for one of its users, for this server's SFU.
    pub async fn rtc_federation_get_token(
        &self,
        origin: &ServerName,
        req: FederationGetTokenRequest,
    ) -> Result<String> {
        let settings = self.livekit_settings()?;
        if req.user_id.server_name() != origin {
            return Err(Error::forbidden(
                "The user_id does not belong to the origin server.",
            ));
        }
        self.require_server_in_room(&req.room_id, &self.config.server_name)
            .await?;
        self.require_user_joined(&req.room_id, &req.user_id).await?;
        self.require_local_sfu(&req.url)?;
        let lk_room = livekit_room_name(&req.room_id, &req.slot_id);
        let identity = livekit_identity(&req.user_id, &req.member_id);
        self.issue_token(
            &req.user_id,
            &req.room_id,
            &lk_room,
            &identity,
            settings.remote_can_publish,
        )
        .await
    }

    /// Registers a delayed leave event for the server to keep alive and send.
    async fn delegate(
        &self,
        user_id: &UserId,
        room_id: &RoomId,
        delay_id: &str,
        lk_room: &str,
        identity: &str,
    ) -> Result<()> {
        let Some(ev) = self.store.get_delayed_event(delay_id).await? else {
            return Err(Error::invalid_param("No such delayed event."));
        };
        if ev.user_id != user_id || ev.room_id != room_id {
            return Err(Error::invalid_param("No such delayed event."));
        }
        self.store
            .add_delegated_leave(&DelegatedLeave {
                delay_id: delay_id.to_owned(),
                user_id: user_id.to_owned(),
                room_id: room_id.to_owned(),
                lk_room: lk_room.to_owned(),
                lk_identity: identity.to_owned(),
                created_ts: now_ts(),
                connected_ts: None,
            })
            .await?;
        self.rtc.wake.notify_one();
        tracing::info!(%user_id, %room_id, delay_id, lk_room, identity, "delayed leave delegated");
        Ok(())
    }

    /// `POST /rtc/livekit/delegate_delayed_leave`.
    pub async fn rtc_delegate_delayed_leave(
        &self,
        user_id: &UserId,
        req: DelegateRequest,
    ) -> Result<()> {
        self.require_local_sfu(&req.url)?;
        self.require_user_joined(&req.room_id, user_id).await?;
        let lk_room = livekit_room_name(&req.room_id, &req.slot_id);
        let identity = livekit_identity(user_id, &req.member_id);
        self.delegate(user_id, &req.room_id, &req.delay_id, &lk_room, &identity)
            .await
    }

    // ------------------------------------------------------------ legacy (OpenID) endpoints

    /// Who an `OpenID` token belongs to, asking the issuing server when it is not us.
    async fn verify_openid(&self, token: &OpenIdToken) -> Result<OwnedUserId> {
        let owner = if token.matrix_server_name == self.config.server_name {
            self.store
                .openid_token_owner(&token.access_token, now_ts())
                .await?
        } else {
            self.federation()
                .remote_openid_userinfo(&token.matrix_server_name, &token.access_token)
                .await
                .map_err(|e| {
                    tracing::debug!(server = %token.matrix_server_name, error = %e, "OpenID userinfo lookup failed");
                    e
                })
                .ok()
        };
        let user = owner.ok_or_else(|| Error::forbidden("Invalid OpenID token."))?;
        if user.server_name() != token.matrix_server_name {
            return Err(Error::forbidden("Invalid OpenID token."));
        }
        Ok(user)
    }

    fn sfu_token(&self, jwt: String) -> Result<SfuToken> {
        Ok(SfuToken {
            url: self.livekit_settings()?.url.clone(),
            jwt,
        })
    }

    /// The legacy `/sfu/get` endpoint: a token for the room's single call, with the
    /// `user:device` identity; a `delay_id` also delegates the delayed leave.
    pub async fn rtc_legacy_sfu_get(&self, req: LegacySfuRequest) -> Result<SfuToken> {
        let settings = self.livekit_settings()?;
        let user = self.verify_openid(&req.openid_token).await?;
        let local = self.is_local(&user);
        if req.delay_id.is_some() && !local {
            return Err(Error::bad_json(
                "Delegation of delayed events is only supported for local users.",
            ));
        }
        self.require_user_joined(&req.room, &user).await?;
        let lk_room = livekit_room_name(&req.room, LEGACY_SLOT_ID);
        let identity = legacy_identity(&user, &req.device_id);
        let jwt = self
            .issue_token(
                &user,
                &req.room,
                &lk_room,
                &identity,
                local || settings.remote_can_publish,
            )
            .await?;
        if let Some(delay_id) = &req.delay_id {
            self.delegate(&user, &req.room, delay_id, &lk_room, &identity)
                .await?;
        }
        self.sfu_token(jwt)
    }

    /// The legacy `/get_token` endpoint, with the hashed `(user, device, member)` identity.
    pub async fn rtc_legacy_get_token(&self, req: LegacyGetTokenRequest) -> Result<SfuToken> {
        let settings = self.livekit_settings()?;
        let user = self.verify_openid(&req.openid_token).await?;
        if req.member.claimed_user_id != user {
            return Err(Error::invalid_param(
                "The claimed user does not match the OpenID token.",
            ));
        }
        let local = self.is_local(&user);
        if req.delay_id.is_some() && !local {
            return Err(Error::bad_json(
                "Delegation of delayed events is only supported for local users.",
            ));
        }
        self.require_user_joined(&req.room_id, &user).await?;
        let lk_room = livekit_room_name(&req.room_id, &req.slot_id);
        let identity = legacy_hashed_identity(&user, &req.member.claimed_device_id, &req.member.id);
        let jwt = self
            .issue_token(
                &user,
                &req.room_id,
                &lk_room,
                &identity,
                local || settings.remote_can_publish,
            )
            .await?;
        if let Some(delay_id) = &req.delay_id {
            self.delegate(&user, &req.room_id, delay_id, &lk_room, &identity)
                .await?;
        }
        self.sfu_token(jwt)
    }

    /// The legacy `/delegate_delayed_leave` endpoint.
    pub async fn rtc_legacy_delegate(&self, req: LegacyDelegateRequest) -> Result<()> {
        self.livekit_settings()?;
        let user = self.verify_openid(&req.openid_token).await?;
        if req.member.claimed_user_id != user {
            return Err(Error::invalid_param(
                "The claimed user does not match the OpenID token.",
            ));
        }
        if !self.is_local(&user) {
            return Err(Error::forbidden(
                "Delegation of delayed events is only supported for local users.",
            ));
        }
        self.require_user_joined(&req.room_id, &user).await?;
        let lk_room = livekit_room_name(&req.room_id, &req.slot_id);
        let identity = legacy_hashed_identity(&user, &req.member.claimed_device_id, &req.member.id);
        self.delegate(&user, &req.room_id, &req.delay_id, &lk_room, &identity)
            .await
    }

    // ------------------------------------------------------------------------ SFU webhooks

    /// A webhook from the SFU: verifies the signature over the body and reacts to
    /// participants connecting and disconnecting.
    pub async fn rtc_webhook(&self, authorization: Option<&str>, body: &[u8]) -> Result<()> {
        let api = self.livekit_api()?;
        let token = authorization
            .ok_or_else(|| Error::forbidden("Missing webhook signature."))?
            .trim();
        let token = token.strip_prefix("Bearer ").unwrap_or(token);
        let claims = jwt::verify(api.api_secret(), token, livekit::now_secs())
            .map_err(|e| Error::forbidden(format!("Bad webhook signature: {e}")))?;
        if claims.get("iss").and_then(Value::as_str) != Some(api.api_key()) {
            return Err(Error::forbidden("Webhook signed with an unknown key."));
        }
        let expected = base64::engine::general_purpose::STANDARD.encode(Sha256::digest(body));
        if claims.get("sha256").and_then(Value::as_str) != Some(expected.as_str()) {
            return Err(Error::forbidden("Webhook body hash mismatch."));
        }
        let event: Value =
            serde_json::from_slice(body).map_err(|e| Error::bad_json(e.to_string()))?;
        let kind = event.get("event").and_then(Value::as_str).unwrap_or("");
        let lk_room = event
            .get("room")
            .and_then(|r| r.get("name"))
            .and_then(Value::as_str)
            .unwrap_or("");
        let identity = event
            .get("participant")
            .and_then(|p| p.get("identity"))
            .and_then(Value::as_str)
            .unwrap_or("");
        tracing::debug!(kind, lk_room, identity, "SFU webhook");
        match kind {
            "participant_joined" => {
                if let Some(leave) = self.store.delegated_leave_for(lk_room, identity).await? {
                    self.store
                        .mark_delegated_leave_connected(&leave.delay_id, now_ts())
                        .await?;
                }
                // A participant whose Matrix membership ended before the SFU learnt about
                // it gets removed again (self-hosted LiveKit cannot revoke tokens).
                if let Some(p) = self.store.rtc_participant(lk_room, identity).await?
                    && !self.is_joined(&p.room_id, &p.user_id).await?
                {
                    tracing::info!(user_id = %p.user_id, room_id = %p.room_id, "removing SFU participant that is no longer in the room");
                    if let Err(e) = api.remove_participant(lk_room, identity).await {
                        tracing::warn!(error = %e, "removing SFU participant failed");
                    }
                }
            }
            "participant_left" | "participant_connection_aborted" => {
                if let Some(leave) = self.store.delegated_leave_for(lk_room, identity).await? {
                    self.send_delegated_leave(&leave).await;
                }
            }
            "room_finished" => {
                for leave in self.store.delegated_leaves().await? {
                    if leave.lk_room == lk_room {
                        self.send_delegated_leave(&leave).await;
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// Sends a delegated leave now and forgets the delegation.
    pub(crate) async fn send_delegated_leave(&self, leave: &DelegatedLeave) {
        tracing::info!(user_id = %leave.user_id, room_id = %leave.room_id, delay_id = %leave.delay_id, "participant disconnected: sending delegated leave");
        let _ = self.store.remove_delegated_leave(&leave.delay_id).await;
        match self
            .update_delayed_event(&leave.user_id, &leave.delay_id, UpdateAction::Send)
            .await
        {
            Ok(()) => {}
            // Already sent or cancelled by the client.
            Err(Error::Matrix { status, .. }) if status == http::StatusCode::NOT_FOUND => {}
            Err(e) => tracing::warn!(error = %e, "sending delegated leave failed"),
        }
    }

    // --------------------------------------------------------------------- membership changes

    /// A membership event was persisted: a user who left or was removed is also removed
    /// from the SFU rooms this server issued tokens for.
    pub fn rtc_on_event(&self, pdu: &Pdu) {
        let Some(api) = self.rtc.api.as_ref() else {
            return;
        };
        if pdu.event_type() != &ruma::events::TimelineEventType::RoomMember {
            return;
        }
        let Some(user_id) = pdu.state_key().and_then(|k| OwnedUserId::try_from(k).ok()) else {
            return;
        };
        let membership = pdu
            .content_as::<MemberContent>()
            .map(|c| c.membership)
            .unwrap_or_default();
        if membership == "join" {
            return;
        }
        let store = Arc::clone(&self.store);
        let api = Arc::clone(api);
        let room_id = pdu.room_id().to_owned();
        tokio::spawn(async move {
            if let Err(e) = remove_user(&*store, &api, &room_id, &user_id).await {
                tracing::warn!(error = %e, "removing SFU participants after membership change failed");
            }
        });
    }
}

/// Removes every SFU participant identity issued to the user for the room.
async fn remove_user(
    store: &dyn tessera_store::Store,
    api: &livekit::LivekitApi,
    room_id: &RoomId,
    user_id: &UserId,
) -> Result<()> {
    for p in store.rtc_participants_for(room_id, user_id).await? {
        tracing::info!(%user_id, %room_id, lk_room = %p.lk_room, "removing SFU participant: no longer in the room");
        if let Err(e) = api.remove_participant(&p.lk_room, &p.lk_identity).await {
            tracing::warn!(error = %e, "removing SFU participant failed");
        }
        store
            .remove_rtc_participant(&p.lk_room, &p.lk_identity)
            .await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_test_vectors() {
        // From the MSC4195 appendix.
        let room = ruma::room_id!("!roomid:example.com");
        assert_eq!(
            livekit_room_name(room, "slot1234"),
            "O8437W3+jmzMVjoIP3tNwbm+XxHQk2iKpOA7aqw3qSc"
        );
        let alice = ruma::user_id!("@alice:example.com");
        assert_eq!(
            livekit_identity(alice, "memberABC"),
            "M3VnsLXrkbxIDINXO64u8PZzFyD9ZYFiQULR2dshWYs"
        );
        // From the reference implementation's tests.
        assert_eq!(
            legacy_hashed_identity(alice, "DEVICE123", "memberABC"),
            "J+T45tGruxc+HrUOqJJlyQSV33m728Cme4+vt8/SWrU"
        );
        assert_eq!(legacy_identity(alice, "DEV"), "@alice:example.com:DEV");
    }
}
