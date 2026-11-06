//! Implementation of the service layer's [`FederationHooks`].

use std::sync::Arc;

use asaroton_core::Pdu;
use asaroton_service::federation_hooks::FederationHooks;
use async_trait::async_trait;
use ruma::{
    OwnedRoomId, OwnedServerName, OwnedUserId, RoomAliasId, RoomId, UserId,
    api::federation::query::{get_profile_information, get_room_information},
};
use serde_json::Value;

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
        let servers = servers_except_self(servers, fed.server_name());
        // Servers denied by the room's ACL get nothing, except the ACL change itself.
        let is_acl_event = pdu.event_type() == &ruma::events::TimelineEventType::RoomServerAcl;
        let mut allowed = Vec::with_capacity(servers.len());
        for s in servers {
            if is_acl_event
                || fed
                    .services
                    .server_acl_allows(pdu.room_id(), &s)
                    .await
                    .unwrap_or(true)
            {
                allowed.push(s);
            }
        }
        let mut servers = allowed;
        // A leave, a kick or a ban must still reach the server of the user it is about, also
        // when that server is not in the room: someone who was only invited, or the last
        // member of their server who goes. An invitation is not sent this way. It has reached
        // the invited user's server over `/invite`, and that server, not being in the room,
        // has to refuse a copy that comes as an ordinary event.
        if pdu.event_type() == &ruma::events::TimelineEventType::RoomMember
            && let Some(target) = pdu.state_key().and_then(|k| OwnedUserId::try_from(k).ok())
            && target.server_name() != fed.server_name()
            && !servers.iter().any(|s| *s == target.server_name())
            && serde_json::from_str::<Value>(pdu.content().get())
                .ok()
                .and_then(|c| c.get("membership")?.as_str().map(str::to_owned))
                .is_some_and(|m| m == "leave" || m == "ban")
        {
            servers.push(target.server_name().to_owned());
        }
        if !servers.is_empty() {
            fed.queue_pdu(servers.clone(), pdu.event_id()).await;
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
    ) -> asaroton_service::Result<()> {
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
    ) -> asaroton_service::Result<Pdu> {
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
    ) -> asaroton_service::Result<()> {
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
    ) -> asaroton_service::Result<()> {
        self.0
            .knock_remote_room(user_id, room_id, via, reason)
            .await
            .map_err(asaroton_service::Error::from)
    }

    async fn resolve_alias(
        &self,
        alias: &RoomAliasId,
    ) -> asaroton_service::Result<(OwnedRoomId, Vec<OwnedServerName>)> {
        let req = get_room_information::v1::Request::new(alias.to_owned());
        let resp = self
            .0
            .client
            .send(alias.server_name(), req)
            .await
            .map_err(asaroton_service::Error::from)?;
        Ok((resp.room_id, resp.servers))
    }

    async fn remote_hierarchy(
        &self,
        servers: &[OwnedServerName],
        room_id: &ruma::RoomId,
        suggested_only: bool,
    ) -> asaroton_service::Result<Value> {
        use ruma::api::federation::space::get_hierarchy;
        let mut last = asaroton_service::Error::not_found("No server could provide the room.");
        for server in servers {
            let mut req = get_hierarchy::v1::Request::new(room_id.to_owned());
            req.suggested_only = suggested_only;
            match self.0.client.send(server, req).await {
                Ok(resp) => {
                    return serde_json::to_value(serde_json::json!({
                        "room": resp.room,
                        "children": resp.children,
                        "inaccessible_children": resp.inaccessible_children,
                    }))
                    .map_err(Into::into);
                }
                Err(e) => last = asaroton_service::Error::from(e),
            }
        }
        Err(last)
    }

    async fn remote_timestamp_to_event(
        &self,
        room_id: &ruma::RoomId,
        ts: u64,
        dir: asaroton_store::Direction,
    ) -> asaroton_service::Result<Option<(ruma::OwnedEventId, u64)>> {
        use ruma::api::federation::event::get_event_by_timestamp;
        let services = &self.0.services;
        let servers = servers_except_self(
            services.store.servers_in_room(room_id).await?,
            &services.config.server_name,
        );
        let Some(room) = services.store.get_room(room_id).await? else {
            return Ok(None);
        };
        let rules = asaroton_core::room_version::rules(&room.room_version)?;
        let ts_ms =
            ruma::MilliSecondsSinceUnixEpoch(ruma::UInt::try_from(ts).unwrap_or(ruma::UInt::MAX));
        let rdir = match dir {
            asaroton_store::Direction::Forwards => ruma::api::Direction::Forward,
            asaroton_store::Direction::Backwards => ruma::api::Direction::Backward,
        };
        let mut best: Option<(OwnedServerName, ruma::OwnedEventId, u64)> = None;
        for server in servers.iter().take(5) {
            let req = get_event_by_timestamp::v1::Request::new(room_id.to_owned(), ts_ms, rdir);
            match self.0.client.send(server, req).await {
                Ok(resp) => {
                    let t = u64::from(resp.origin_server_ts.0);
                    let better = match &best {
                        None => true,
                        Some((_, _, bt)) => match dir {
                            asaroton_store::Direction::Forwards => t < *bt,
                            asaroton_store::Direction::Backwards => t > *bt,
                        },
                    };
                    if better {
                        best = Some((server.clone(), resp.event_id, t));
                    }
                }
                Err(e) => tracing::debug!(%server, error = %e, "remote timestamp lookup failed"),
            }
        }
        let Some((server, event_id, t)) = best else {
            return Ok(None);
        };
        if services
            .store
            .get_event(&event_id)
            .await?
            .is_some_and(|e| !e.outlier)
        {
            return Ok(Some((event_id, t)));
        }
        // Store the event as history so the client can fetch it afterwards.
        let pdu = self
            .0
            .fetch_event(&server, &event_id, &rules)
            .await
            .map_err(asaroton_service::Error::from)?;
        let ordering = services.store.reserve_backfill_orderings(1).await?.pop();
        match self
            .0
            .process_pdu_inner(&server, pdu, &rules, true, false, ordering)
            .await
        {
            Ok(_) => Ok(Some((event_id, t))),
            Err(e) => {
                tracing::warn!(%event_id, error = %e, "could not store remotely found event");
                Ok(None)
            }
        }
    }

    async fn remote_profile(&self, user_id: &UserId) -> asaroton_service::Result<Value> {
        let req = get_profile_information::v1::Request::new(user_id.to_owned());
        let resp = self
            .0
            .client
            .send(user_id.server_name(), req)
            .await
            .map_err(asaroton_service::Error::from)?;
        serde_json::to_value(resp.data).map_err(Into::into)
    }

    async fn remote_livekit_token(
        &self,
        server: &ruma::ServerName,
        body: Value,
    ) -> asaroton_service::Result<Value> {
        self.0
            .client
            .send_signed_json(
                server,
                http::Method::POST,
                asaroton_service::rtc::FEDERATION_GET_TOKEN_PATH,
                Some(&body),
            )
            .await
            .map_err(asaroton_service::Error::from)
    }

    async fn remote_openid_userinfo(
        &self,
        server: &ruma::ServerName,
        access_token: &str,
    ) -> asaroton_service::Result<OwnedUserId> {
        let req = ruma::api::federation::openid::get_openid_userinfo::v1::Request::new(
            access_token.to_owned(),
        );
        let resp = self
            .0
            .client
            .send_unauthenticated(server, req)
            .await
            .map_err(asaroton_service::Error::from)?;
        Ok(resp.sub)
    }

    async fn backfill(
        &self,
        room_id: &ruma::RoomId,
        limit: usize,
    ) -> asaroton_service::Result<usize> {
        self.0
            .backfill_room(room_id, limit)
            .await
            .map_err(asaroton_service::Error::from)
    }

    async fn appservice_query_user(
        &self,
        appservice_id: &str,
        user: &UserId,
    ) -> asaroton_service::Result<bool> {
        self.0.appservice_query_user(appservice_id, user).await
    }

    async fn appservice_query_alias(
        &self,
        appservice_id: &str,
        alias: &ruma::RoomAliasId,
    ) -> asaroton_service::Result<bool> {
        self.0.appservice_query_alias(appservice_id, alias).await
    }

    async fn appservice_ping(
        &self,
        appservice_id: &str,
        txn_id: &str,
    ) -> asaroton_service::Result<u64> {
        self.0.appservice_ping(appservice_id, txn_id).await
    }

    async fn appservice_thirdparty(
        &self,
        appservice_id: &str,
        path: &str,
        query: &[(String, String)],
    ) -> asaroton_service::Result<Option<Value>> {
        self.0
            .appservice_thirdparty(appservice_id, path, query)
            .await
    }

    async fn preview_url(&self, url: &str) -> asaroton_service::Result<Value> {
        self.0
            .preview_url(url)
            .await
            .map_err(asaroton_service::Error::from)
    }

    async fn remote_query_keys(
        &self,
        by_server: Vec<(OwnedServerName, Vec<OwnedUserId>)>,
    ) -> asaroton_service::Result<Value> {
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
    ) -> asaroton_service::Result<Value> {
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
    ) -> asaroton_service::Result<(Vec<u8>, String, Option<String>)> {
        use ruma::api::federation::authenticated_media::{FileOrLocation, get_content};
        // How much a remote server sends is its own choice: stop reading at what a local
        // upload may be. The authenticated endpoint wraps the file in a multipart body.
        let max = usize::try_from(asaroton_service::media::MAX_UPLOAD_BYTES).unwrap_or(usize::MAX);
        let req = get_content::v1::Request::new(media_id.to_owned());
        let resp = match self
            .0
            .client
            .send_bounded(server_name, req, max.saturating_add(64 * 1024))
            .await
        {
            Ok(r) => r,
            Err(e @ crate::Error::TooLarge(..)) => return Err(e.into()),
            Err(e) => {
                // Servers without authenticated media (and some test servers) still serve the
                // legacy unauthenticated endpoint.
                tracing::debug!(%server_name, error = %e, "authenticated media fetch failed; trying legacy endpoint");
                let path = format!(
                    "/_matrix/media/v3/download/{}/{}",
                    server_name,
                    urlencoding_media(media_id)
                );
                let (bytes, content_type, disposition) = self
                    .0
                    .client
                    .get_bytes(server_name, &path, max)
                    .await
                    .map_err(asaroton_service::Error::from)?;
                let filename = disposition.and_then(|d| {
                    d.parse::<ruma::http_headers::ContentDisposition>()
                        .ok()
                        .and_then(|cd| cd.filename)
                });
                return Ok((bytes, content_type, filename));
            }
        };
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
                let (bytes, content_type) = self
                    .0
                    .outbound
                    .media_location(server_name, &url, max, crate::client::LONG_REQUEST_TIMEOUT)
                    .await
                    .map_err(asaroton_service::Error::from)?;
                Ok((bytes, content_type, None))
            }
            _ => Err(asaroton_service::Error::internal("unknown media response")),
        }
    }
}

fn urlencoding_media(s: &str) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(b as char);
        } else {
            let _ = write!(out, "%{b:02X}");
        }
    }
    out
}
