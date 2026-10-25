//! Implementation of the service layer's [`FederationHooks`].

use std::sync::Arc;

use async_trait::async_trait;
use ruma::{
    OwnedRoomId, OwnedServerName, OwnedUserId, RoomAliasId, RoomId, UserId,
    api::federation::query::{get_profile_information, get_room_information},
};
use serde_json::Value;
use tessera_core::Pdu;
use tessera_service::federation_hooks::FederationHooks;

use crate::{Federation, sender::servers_except_self};

/// Wraps the federation handle for installation into the services.
#[derive(Debug)]
pub struct Hooks(pub Arc<Federation>);

#[async_trait]
impl FederationHooks for Hooks {
    async fn on_new_event(&self, pdu: &Pdu) {
        let fed = &self.0;
        let servers = match fed.services.store.servers_in_room(pdu.room_id()).await {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(error = %e, "servers_in_room failed");
                return;
            }
        };
        // A leave/kick must still reach the server of the user leaving.
        let mut servers = servers_except_self(servers, fed.server_name());
        if pdu.event_type() == &ruma::events::TimelineEventType::RoomMember
            && let Some(target) = pdu.state_key().and_then(|k| OwnedUserId::try_from(k).ok())
            && target.server_name() != fed.server_name()
            && !servers.iter().any(|s| *s == target.server_name())
        {
            servers.push(target.server_name().to_owned());
        }
        if !servers.is_empty() {
            fed.queue_pdu(servers.clone(), pdu.event_id()).await;
        }
        // A local user joining a federated room: the other servers need their device list.
        if pdu.event_type() == &ruma::events::TimelineEventType::RoomMember
            && let Some(target) = pdu.state_key().and_then(|k| OwnedUserId::try_from(k).ok())
            && fed.services.is_local(&target)
            && serde_json::from_str::<Value>(pdu.content().get())
                .ok()
                .and_then(|c| {
                    c.get("membership")
                        .and_then(|m| m.as_str())
                        .map(str::to_owned)
                })
                .as_deref()
                == Some("join")
        {
            let _ = fed.services.send_device_lists_to(&target, servers).await;
        }
    }

    async fn send_edu(&self, destinations: Vec<OwnedServerName>, edu: Value) {
        self.0.queue_edu(destinations, edu).await;
    }

    async fn remote_join(
        &self,
        user_id: &UserId,
        room_id: &RoomId,
        via: &[OwnedServerName],
        reason: Option<&str>,
    ) -> tessera_service::Result<()> {
        self.0
            .join_remote_room(user_id, room_id, via, reason)
            .await
            .map_err(Into::into)
    }

    async fn remote_invite(
        &self,
        pdu: &Pdu,
        room_version: &ruma::RoomVersionId,
        stripped_state: Vec<Value>,
    ) -> tessera_service::Result<Pdu> {
        self.0
            .send_invite(pdu, room_version, stripped_state)
            .await
            .map_err(Into::into)
    }

    async fn remote_leave(
        &self,
        user_id: &UserId,
        room_id: &RoomId,
        via: &[OwnedServerName],
    ) -> tessera_service::Result<()> {
        self.0
            .leave_remote_room(user_id, room_id, via)
            .await
            .map_err(Into::into)
    }

    async fn remote_knock(
        &self,
        user_id: &UserId,
        room_id: &ruma::RoomId,
        via: &[OwnedServerName],
        reason: Option<&str>,
    ) -> tessera_service::Result<()> {
        self.0
            .knock_remote_room(user_id, room_id, via, reason)
            .await
            .map_err(tessera_service::Error::from)
    }

    async fn resolve_alias(
        &self,
        alias: &RoomAliasId,
    ) -> tessera_service::Result<(OwnedRoomId, Vec<OwnedServerName>)> {
        let req = get_room_information::v1::Request::new(alias.to_owned());
        let resp = self
            .0
            .client
            .send(alias.server_name(), req)
            .await
            .map_err(tessera_service::Error::from)?;
        Ok((resp.room_id, resp.servers))
    }

    async fn remote_profile(&self, user_id: &UserId) -> tessera_service::Result<Value> {
        let req = get_profile_information::v1::Request::new(user_id.to_owned());
        let resp = self
            .0
            .client
            .send(user_id.server_name(), req)
            .await
            .map_err(tessera_service::Error::from)?;
        serde_json::to_value(resp.data).map_err(Into::into)
    }

    async fn backfill(
        &self,
        room_id: &ruma::RoomId,
        limit: usize,
    ) -> tessera_service::Result<usize> {
        self.0
            .backfill_room(room_id, limit)
            .await
            .map_err(tessera_service::Error::from)
    }

    async fn preview_url(&self, url: &str) -> tessera_service::Result<Value> {
        self.0
            .preview_url(url)
            .await
            .map_err(tessera_service::Error::from)
    }

    async fn remote_query_keys(
        &self,
        by_server: Vec<(OwnedServerName, Vec<OwnedUserId>)>,
    ) -> tessera_service::Result<Value> {
        use ruma::api::federation::keys::get_keys;
        let mut device_keys = serde_json::Map::new();
        let mut master_keys = serde_json::Map::new();
        let mut self_signing_keys = serde_json::Map::new();
        let mut failures = serde_json::Map::new();
        for (server, users) in by_server {
            let req =
                get_keys::v1::Request::new(users.into_iter().map(|u| (u, Vec::new())).collect());
            match self.0.client.send(&server, req).await {
                Ok(resp) => {
                    for (u, devs) in resp.device_keys {
                        device_keys.insert(u.to_string(), serde_json::to_value(devs)?);
                    }
                    for (u, k) in resp.master_keys {
                        master_keys.insert(u.to_string(), serde_json::to_value(k)?);
                    }
                    for (u, k) in resp.self_signing_keys {
                        self_signing_keys.insert(u.to_string(), serde_json::to_value(k)?);
                    }
                }
                Err(e) => {
                    failures.insert(
                        server.to_string(),
                        serde_json::json!({ "error": e.to_string() }),
                    );
                }
            }
        }
        Ok(serde_json::json!({
            "device_keys": device_keys, "master_keys": master_keys,
            "self_signing_keys": self_signing_keys, "failures": failures,
        }))
    }

    async fn remote_claim_keys(
        &self,
        by_server: Vec<(OwnedServerName, Value)>,
    ) -> tessera_service::Result<Value> {
        use ruma::api::federation::keys::claim_keys;
        let mut one_time_keys = serde_json::Map::new();
        let mut failures = serde_json::Map::new();
        for (server, claims) in by_server {
            let Ok(claims) = serde_json::from_value(claims) else {
                continue;
            };
            let req = claim_keys::v1::Request::new(claims);
            match self.0.client.send(&server, req).await {
                Ok(resp) => {
                    if let Ok(Value::Object(m)) = serde_json::to_value(resp.one_time_keys) {
                        for (k, v) in m {
                            one_time_keys.insert(k, v);
                        }
                    }
                }
                Err(e) => {
                    failures.insert(
                        server.to_string(),
                        serde_json::json!({ "error": e.to_string() }),
                    );
                }
            }
        }
        Ok(serde_json::json!({ "one_time_keys": one_time_keys, "failures": failures }))
    }

    async fn remote_media(
        &self,
        server_name: &ruma::ServerName,
        media_id: &str,
    ) -> tessera_service::Result<(Vec<u8>, String, Option<String>)> {
        use ruma::api::federation::authenticated_media::{FileOrLocation, get_content};
        let req = get_content::v1::Request::new(media_id.to_owned());
        let resp = self
            .0
            .client
            .send_long(server_name, req)
            .await
            .map_err(tessera_service::Error::from)?;
        match resp.content {
            FileOrLocation::File(c) => {
                let filename = c
                    .content_disposition
                    .as_ref()
                    .and_then(|d| d.filename.clone());
                Ok((
                    c.file,
                    c.content_type
                        .unwrap_or_else(|| "application/octet-stream".into()),
                    filename,
                ))
            }
            FileOrLocation::Location(url) => {
                let r = reqwest::get(&url)
                    .await
                    .map_err(|e| tessera_service::Error::internal(e.to_string()))?;
                let ct = r
                    .headers()
                    .get(http::header::CONTENT_TYPE)
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("application/octet-stream")
                    .to_owned();
                let bytes = r
                    .bytes()
                    .await
                    .map_err(|e| tessera_service::Error::internal(e.to_string()))?;
                Ok((bytes.to_vec(), ct, None))
            }
            _ => Err(tessera_service::Error::internal("unknown media response")),
        }
    }
}
