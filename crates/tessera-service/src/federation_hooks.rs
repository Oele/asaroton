//! The seam between the service layer and the federation crate.
//!
//! `tessera-federation` depends on this crate, so the service layer cannot call it directly.
//! Instead the binary installs an implementation of [`FederationHooks`] at startup; with
//! federation disabled the [`NoFederation`] stand-in is used.

use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use ruma::{OwnedRoomId, OwnedServerName, OwnedUserId, RoomAliasId, RoomId, UserId};
use serde_json::Value;
use tessera_core::Pdu;

use crate::{Error, Result, Services};

/// Outbound federation operations the service layer needs.
#[async_trait]
pub trait FederationHooks: Send + Sync {
    /// A local event was persisted; send it to the other servers in the room.
    async fn on_new_event(&self, pdu: &Pdu);
    /// Send an EDU to the given servers (typing, receipts, presence, device lists, to-device).
    async fn send_edu(&self, destinations: Vec<OwnedServerName>, edu: Value);
    /// Join a room we are not in yet, via the given candidate servers.
    async fn remote_join(
        &self,
        user_id: &UserId,
        room_id: &RoomId,
        via: &[OwnedServerName],
        reason: Option<&str>,
    ) -> Result<()>;
    /// Deliver an invite event to the invitee's server; returns the event as signed by them.
    async fn remote_invite(
        &self,
        pdu: &Pdu,
        room_version: &ruma::RoomVersionId,
        stripped_state: Vec<Value>,
    ) -> Result<Pdu>;
    /// Leave a room we no longer have other members in (reject an invite remotely).
    async fn remote_leave(
        &self,
        user_id: &UserId,
        room_id: &RoomId,
        via: &[OwnedServerName],
    ) -> Result<()>;
    /// Resolve an alias on its server.
    async fn resolve_alias(
        &self,
        alias: &RoomAliasId,
    ) -> Result<(OwnedRoomId, Vec<OwnedServerName>)>;
    /// Fetch a remote user's profile (`displayname`, `avatar_url`).
    async fn remote_profile(&self, user_id: &UserId) -> Result<Value>;
    /// Fetch remote device keys (`/user/keys/query`): user -> device -> keys, plus cross-signing.
    async fn remote_query_keys(
        &self,
        by_server: Vec<(OwnedServerName, Vec<OwnedUserId>)>,
    ) -> Result<Value>;
    /// Claim remote one-time keys.
    async fn remote_claim_keys(&self, by_server: Vec<(OwnedServerName, Value)>) -> Result<Value>;
}

/// Used when federation is disabled: every remote operation fails cleanly.
#[derive(Debug)]
pub struct NoFederation;

fn disabled() -> Error {
    Error::matrix(
        ruma::api::error::ErrorKind::Forbidden,
        http::StatusCode::FORBIDDEN,
        "Federation is disabled on this server.",
    )
}

#[async_trait]
impl FederationHooks for NoFederation {
    async fn on_new_event(&self, _pdu: &Pdu) {}
    async fn send_edu(&self, _destinations: Vec<OwnedServerName>, _edu: Value) {}
    async fn remote_join(
        &self,
        _: &UserId,
        _: &RoomId,
        _: &[OwnedServerName],
        _: Option<&str>,
    ) -> Result<()> {
        Err(disabled())
    }
    async fn remote_invite(&self, _: &Pdu, _: &ruma::RoomVersionId, _: Vec<Value>) -> Result<Pdu> {
        Err(disabled())
    }
    async fn remote_leave(&self, _: &UserId, _: &RoomId, _: &[OwnedServerName]) -> Result<()> {
        Err(disabled())
    }
    async fn resolve_alias(&self, _: &RoomAliasId) -> Result<(OwnedRoomId, Vec<OwnedServerName>)> {
        Err(disabled())
    }
    async fn remote_profile(&self, _: &UserId) -> Result<Value> {
        Err(disabled())
    }
    async fn remote_query_keys(
        &self,
        _: Vec<(OwnedServerName, Vec<OwnedUserId>)>,
    ) -> Result<Value> {
        Ok(Value::Object(serde_json::Map::new()))
    }
    async fn remote_claim_keys(&self, _: Vec<(OwnedServerName, Value)>) -> Result<Value> {
        Ok(Value::Object(serde_json::Map::new()))
    }
}

/// Holder for the installed hooks.
#[derive(Default)]
pub struct HooksSlot(OnceLock<Arc<dyn FederationHooks>>);

impl std::fmt::Debug for HooksSlot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("HooksSlot")
    }
}

impl Services {
    /// Install the federation implementation. Called once at startup.
    pub fn set_federation(&self, hooks: Arc<dyn FederationHooks>) {
        let _ = self.federation_hooks.0.set(hooks);
    }

    /// The installed hooks (or the disabled stand-in).
    pub fn federation(&self) -> Arc<dyn FederationHooks> {
        self.federation_hooks
            .0
            .get()
            .cloned()
            .unwrap_or_else(|| Arc::new(NoFederation))
    }
}
