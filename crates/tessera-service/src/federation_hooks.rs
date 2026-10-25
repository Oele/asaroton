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
    /// Knock on a remote room through one of `via`.
    async fn remote_knock(
        &self,
        user_id: &UserId,
        room_id: &RoomId,
        via: &[OwnedServerName],
        reason: Option<&str>,
    ) -> Result<()>;
    /// Resolve an alias on its server.
    async fn resolve_alias(
        &self,
        alias: &RoomAliasId,
    ) -> Result<(OwnedRoomId, Vec<OwnedServerName>)>;
    /// Fetch a remote user's profile (`displayname`, `avatar_url`).
    async fn remote_profile(&self, user_id: &UserId) -> Result<Value>;
    /// A room's hierarchy summary from one of `servers` (federation `/hierarchy`), as the
    /// raw response object.
    async fn remote_hierarchy(
        &self,
        servers: &[OwnedServerName],
        room_id: &RoomId,
        suggested_only: bool,
    ) -> Result<Value>;
    /// Fetch older history for a room from other servers; returns how many events were stored.
    async fn backfill(&self, room_id: &RoomId, limit: usize) -> Result<usize>;
    /// Open Graph preview data for a URL.
    async fn preview_url(&self, url: &str) -> Result<Value>;
    /// Fetch remote device keys (`/user/keys/query`): user -> device -> keys, plus cross-signing.
    async fn remote_query_keys(
        &self,
        by_server: Vec<(OwnedServerName, Vec<OwnedUserId>)>,
    ) -> Result<Value>;
    /// Claim remote one-time keys.
    async fn remote_claim_keys(&self, by_server: Vec<(OwnedServerName, Value)>) -> Result<Value>;
    /// Download remote media: `(bytes, content_type, filename)`.
    async fn remote_media(
        &self,
        server_name: &ruma::ServerName,
        media_id: &str,
    ) -> Result<(Vec<u8>, String, Option<String>)>;
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
    async fn remote_knock(
        &self,
        _: &UserId,
        _: &RoomId,
        _: &[OwnedServerName],
        _: Option<&str>,
    ) -> Result<()> {
        Err(disabled())
    }
    async fn resolve_alias(&self, _: &RoomAliasId) -> Result<(OwnedRoomId, Vec<OwnedServerName>)> {
        Err(disabled())
    }
    async fn remote_profile(&self, _: &UserId) -> Result<Value> {
        Err(disabled())
    }
    async fn remote_hierarchy(&self, _: &[OwnedServerName], _: &RoomId, _: bool) -> Result<Value> {
        Err(disabled())
    }
    async fn backfill(&self, _: &RoomId, _: usize) -> Result<usize> {
        Ok(0)
    }
    async fn preview_url(&self, _: &str) -> Result<Value> {
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
    async fn remote_media(
        &self,
        _: &ruma::ServerName,
        _: &str,
    ) -> Result<(Vec<u8>, String, Option<String>)> {
        Err(disabled())
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

impl Services {
    /// Remote servers with a joined member in the room.
    pub async fn remote_servers_in_room(&self, room_id: &RoomId) -> Result<Vec<OwnedServerName>> {
        Ok(self
            .store
            .servers_in_room(room_id)
            .await?
            .into_iter()
            .filter(|s| *s != self.config.server_name)
            .collect())
    }

    /// Remote servers that share at least one room with the user.
    pub async fn remote_servers_sharing_rooms_with(
        &self,
        user_id: &UserId,
    ) -> Result<Vec<OwnedServerName>> {
        let mut out: Vec<OwnedServerName> = Vec::new();
        for room in self.store.rooms_for_user(user_id, "join").await? {
            for s in self.remote_servers_in_room(&room).await? {
                if !out.contains(&s) {
                    out.push(s);
                }
            }
        }
        Ok(out)
    }

    /// Queue an EDU to the given servers (no-op when there are none).
    pub fn send_edu_to(&self, destinations: Vec<OwnedServerName>, edu: Value) {
        if destinations.is_empty() {
            return;
        }
        let hooks = self.federation();
        tokio::spawn(async move { hooks.send_edu(destinations, edu).await });
    }
}
