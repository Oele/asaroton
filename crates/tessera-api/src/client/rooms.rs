//! Room creation, membership, state and timeline endpoints.

use ruma::{
    OwnedRoomId, UInt,
    api::client::{
        alias,
        membership::{
            self, ban_user, forget_room, get_member_events, invite_user, join_room_by_id,
            join_room_by_id_or_alias, joined_members, joined_rooms, kick_user, leave_room,
            unban_user,
        },
        message::{get_message_events, send_message_event},
        redact::redact_event,
        room::{self, create_room, get_room_event},
        state::{get_state_event_for_key, get_state_events, send_state_event},
    },
    events::room::member::MembershipState,
    serde::Raw,
};
use serde_json::Value;
use tessera_service::rooms::{create::CreateRoomRequest, timeline::StreamToken};
use tessera_store::{Direction, StoredEvent};

use crate::{AppState, Error, Result, Ruma};

/// Render a stored event in client format with `unsigned.age`.
pub fn client_event(ev: &StoredEvent) -> Value {
    let mut v = ev.pdu.to_client_json();
    let now = tessera_store::now_ts();
    let age =
        now.saturating_sub(i64::try_from(u64::from(ev.pdu.origin_server_ts().0)).unwrap_or(0));
    let unsigned = v.as_object_mut().and_then(|o| {
        o.entry("unsigned")
            .or_insert_with(|| serde_json::json!({}))
            .as_object_mut()
    });
    if let Some(u) = unsigned {
        u.insert("age".into(), serde_json::json!(age));
    }
    v
}

pub fn raw<T>(v: &Value) -> Result<Raw<T>> {
    Ok(Raw::from_json(
        serde_json::value::to_raw_value(v).map_err(|e| Error::internal(e.to_string()))?,
    ))
}

fn json_from_raw<T>(r: &Raw<T>) -> Result<Value> {
    serde_json::from_str(r.json().get()).map_err(|e| Error::bad_json(e.to_string()))
}

fn parse_room_version(v: Option<&ruma::RoomVersionId>) -> Result<Option<ruma::RoomVersionId>> {
    match v {
        None => Ok(None),
        Some(v) if tessera_core::room_version::is_supported(v) => Ok(Some(v.clone())),
        Some(_) => Err(Error::matrix(
            ruma::api::error::ErrorKind::UnsupportedRoomVersion,
            http::StatusCode::BAD_REQUEST,
            "Unsupported room version.",
        )),
    }
}

pub async fn create(
    services: AppState,
    req: Ruma<create_room::v3::Request>,
) -> Result<create_room::v3::Response> {
    let sender = req.sender()?;
    let b = &req.body;
    let creation_content = b.creation_content.as_ref().map(json_from_raw).transpose()?;
    let pl_override = b
        .power_level_content_override
        .as_ref()
        .map(json_from_raw)
        .transpose()?;
    let initial_state = tessera_service::rooms::create::initial_state_from_raw(&b.initial_state)?;
    let room_id = services
        .create_room(CreateRoomRequest {
            creator: sender.user_id.clone(),
            room_version: parse_room_version(b.room_version.as_ref())?,
            visibility: Some(b.visibility.clone()),
            preset: b.preset.clone(),
            name: b.name.clone(),
            topic: b.topic.clone(),
            room_alias_name: b.room_alias_name.clone(),
            invite: b.invite.clone(),
            is_direct: b.is_direct,
            creation_content,
            power_level_content_override: pl_override,
            initial_state,
        })
        .await?;
    Ok(create_room::v3::Response::new(room_id))
}

pub async fn join_by_id(
    services: AppState,
    req: Ruma<join_room_by_id::v3::Request>,
) -> Result<join_room_by_id::v3::Response> {
    let sender = req.sender()?;
    join_room(
        &services,
        sender,
        &req.body.room_id,
        &[],
        req.body.reason.as_deref(),
    )
    .await?;
    Ok(join_room_by_id::v3::Response::new(req.body.room_id.clone()))
}

/// Join locally when we are already in the room, otherwise through federation.
async fn join_room(
    services: &AppState,
    sender: &tessera_auth::Sender,
    room_id: &ruma::RoomId,
    via: &[ruma::OwnedServerName],
    reason: Option<&str>,
) -> Result<()> {
    let local = services.store.room_exists(room_id).await?
        && services
            .store
            .members(room_id, Some("join"))
            .await?
            .iter()
            .any(|m| services.is_local(&m.user_id));
    if local {
        services
            .join_local_room(room_id, &sender.user_id, reason)
            .await?;
        return Ok(());
    }
    // An invited user can join through the inviter's server even without `via`.
    let mut via: Vec<ruma::OwnedServerName> = via.to_vec();
    if let Some(m) = services
        .store
        .get_membership(room_id, &sender.user_id)
        .await?
        && m.membership == ruma::events::room::member::MembershipState::Invite
        && !via.iter().any(|s| *s == m.sender.server_name())
    {
        via.push(m.sender.server_name().to_owned());
    }
    services
        .federation()
        .remote_join(&sender.user_id, room_id, &via, reason)
        .await?;
    Ok(())
}

pub async fn join_by_id_or_alias(
    services: AppState,
    req: Ruma<join_room_by_id_or_alias::v3::Request>,
) -> Result<join_room_by_id_or_alias::v3::Response> {
    let sender = req.sender()?;
    let mut via = req.body.via.clone();
    let room_id: OwnedRoomId = match <&ruma::RoomId>::try_from(req.body.room_id_or_alias.as_str()) {
        Ok(id) => id.to_owned(),
        Err(_) => {
            let alias = <&ruma::RoomAliasId>::try_from(req.body.room_id_or_alias.as_str())
                .map_err(|_| Error::invalid_param("Invalid room ID or alias."))?;
            if alias.server_name() == services.server_name() {
                services
                    .store
                    .get_alias(alias)
                    .await?
                    .ok_or_else(|| Error::not_found("Room alias not found."))?
                    .room_id
            } else {
                let (room_id, servers) = services.federation().resolve_alias(alias).await?;
                for s in servers {
                    if !via.contains(&s) {
                        via.push(s);
                    }
                }
                room_id
            }
        }
    };
    join_room(
        &services,
        sender,
        &room_id,
        &via,
        req.body.reason.as_deref(),
    )
    .await?;
    Ok(join_room_by_id_or_alias::v3::Response::new(room_id))
}

pub async fn leave(
    services: AppState,
    req: Ruma<leave_room::v3::Request>,
) -> Result<leave_room::v3::Response> {
    let sender = req.sender()?;
    let in_room = services
        .store
        .members(&req.body.room_id, Some("join"))
        .await?
        .iter()
        .any(|m| services.is_local(&m.user_id));
    if !in_room
        && let Some(m) = services
            .store
            .get_membership(&req.body.room_id, &sender.user_id)
            .await?
        && m.membership == ruma::events::room::member::MembershipState::Invite
        && !services.is_local(&m.sender)
    {
        // Rejecting a remote invite: no local members to build on, go through the inviter.
        services
            .federation()
            .remote_leave(
                &sender.user_id,
                &req.body.room_id,
                &[m.sender.server_name().to_owned()],
            )
            .await?;
        return Ok(leave_room::v3::Response::new());
    }
    services
        .leave_room(
            &req.body.room_id,
            &sender.user_id,
            req.body.reason.as_deref(),
        )
        .await?;
    Ok(leave_room::v3::Response::new())
}

pub async fn forget(
    services: AppState,
    req: Ruma<forget_room::v3::Request>,
) -> Result<forget_room::v3::Response> {
    services
        .forget_room(&req.body.room_id, &req.sender()?.user_id)
        .await?;
    Ok(forget_room::v3::Response::new())
}

pub async fn invite(
    services: AppState,
    req: Ruma<invite_user::v3::Request>,
) -> Result<invite_user::v3::Response> {
    let sender = req.sender()?;
    match &req.body.recipient {
        invite_user::v3::InvitationRecipient::UserId(r) => {
            services
                .invite_user(
                    &req.body.room_id,
                    &sender.user_id,
                    &r.user_id,
                    r.reason.as_deref(),
                    false,
                )
                .await?;
        }
        _ => {
            return Err(Error::matrix(
                ruma::api::error::ErrorKind::ThreepidDenied,
                http::StatusCode::FORBIDDEN,
                "Third-party invites are not supported.",
            ));
        }
    }
    Ok(invite_user::v3::Response::new())
}

pub async fn kick(
    services: AppState,
    req: Ruma<kick_user::v3::Request>,
) -> Result<kick_user::v3::Response> {
    let sender = req.sender()?;
    services
        .set_membership_of_other(
            &req.body.room_id,
            &sender.user_id,
            &req.body.user_id,
            "leave",
            req.body.reason.as_deref(),
        )
        .await?;
    Ok(kick_user::v3::Response::new())
}

pub async fn ban(
    services: AppState,
    req: Ruma<ban_user::v3::Request>,
) -> Result<ban_user::v3::Response> {
    let sender = req.sender()?;
    services
        .set_membership_of_other(
            &req.body.room_id,
            &sender.user_id,
            &req.body.user_id,
            "ban",
            req.body.reason.as_deref(),
        )
        .await?;
    Ok(ban_user::v3::Response::new())
}

pub async fn unban(
    services: AppState,
    req: Ruma<unban_user::v3::Request>,
) -> Result<unban_user::v3::Response> {
    let sender = req.sender()?;
    services
        .set_membership_of_other(
            &req.body.room_id,
            &sender.user_id,
            &req.body.user_id,
            "leave",
            req.body.reason.as_deref(),
        )
        .await?;
    Ok(unban_user::v3::Response::new())
}

pub async fn joined(
    services: AppState,
    req: Ruma<joined_rooms::v3::Request>,
) -> Result<joined_rooms::v3::Response> {
    Ok(joined_rooms::v3::Response::new(
        services.joined_rooms(&req.sender()?.user_id).await?,
    ))
}

pub async fn members(
    services: AppState,
    req: Ruma<get_member_events::v3::Request>,
) -> Result<get_member_events::v3::Response> {
    let sender = req.sender()?;
    let state = services
        .room_state(&req.body.room_id, &sender.user_id)
        .await?;
    let want = req.body.membership.clone();
    let not = req.body.not_membership.clone();
    let mut chunk = Vec::new();
    for ev in state
        .iter()
        .filter(|e| e.pdu.event_type() == &ruma::events::TimelineEventType::RoomMember)
    {
        let m: Option<MembershipState> = serde_json::from_str::<Value>(ev.pdu.content().get())
            .ok()
            .and_then(|v| {
                v.get("membership")
                    .and_then(|m| m.as_str())
                    .map(|m| m.to_owned().into())
            });
        if let Some(w) = &want
            && m.as_ref() != Some(w)
        {
            continue;
        }
        if let Some(n) = &not
            && m.as_ref() == Some(n)
        {
            continue;
        }
        chunk.push(raw(&client_event(ev))?);
    }
    Ok(get_member_events::v3::Response::new(chunk))
}

pub async fn joined_members(
    services: AppState,
    req: Ruma<joined_members::v3::Request>,
) -> Result<axum::response::Response> {
    use axum::response::IntoResponse;
    let sender = req.sender()?;
    services
        .require_joined(&req.body.room_id, &sender.user_id)
        .await?;
    // Hand-built so `display_name` and `avatar_url` are always present (null when unset),
    // which is what clients and the conformance tests expect.
    let mut joined = serde_json::Map::new();
    for m in services
        .store
        .members(&req.body.room_id, Some("join"))
        .await?
    {
        joined.insert(
            m.user_id.to_string(),
            serde_json::json!({ "display_name": m.display_name, "avatar_url": m.avatar_url }),
        );
    }
    let mut resp = axum::Json(serde_json::json!({ "joined": joined })).into_response();
    crate::extract::add_cors(resp.headers_mut());
    Ok(resp)
}

pub async fn send_message(
    services: AppState,
    req: Ruma<send_message_event::v3::Request>,
) -> Result<send_message_event::v3::Response> {
    let sender = req.sender()?;
    let content = json_from_raw(&req.body.body)?;
    if !content.is_object() {
        return Err(Error::bad_json("Event content must be an object."));
    }
    let event_id = services
        .send_message(
            &req.body.room_id,
            &sender.user_id,
            sender.device_id.as_deref(),
            req.body.txn_id.as_str(),
            &req.body.event_type.to_string(),
            content,
        )
        .await?;
    Ok(send_message_event::v3::Response::new(event_id))
}

pub async fn send_state(
    services: AppState,
    req: Ruma<send_state_event::v3::Request>,
) -> Result<send_state_event::v3::Response> {
    let sender = req.sender()?;
    let content = json_from_raw(&req.body.body)?;
    if !content.is_object() {
        return Err(Error::bad_json("Event content must be an object."));
    }
    let event_id = services
        .send_state(
            &req.body.room_id,
            &sender.user_id,
            &req.body.event_type.to_string(),
            &req.body.state_key,
            content,
        )
        .await?;
    Ok(send_state_event::v3::Response::new(event_id))
}

pub async fn redact(
    services: AppState,
    req: Ruma<redact_event::v3::Request>,
) -> Result<redact_event::v3::Response> {
    let sender = req.sender()?;
    let event_id = services
        .redact(
            &req.body.room_id,
            &sender.user_id,
            sender.device_id.as_deref(),
            req.body.txn_id.as_str(),
            &req.body.event_id,
            req.body.reason.as_deref(),
        )
        .await?;
    Ok(redact_event::v3::Response::new(event_id))
}

pub async fn state(
    services: AppState,
    req: Ruma<get_state_events::v3::Request>,
) -> Result<get_state_events::v3::Response> {
    let sender = req.sender()?;
    let state = services
        .room_state(&req.body.room_id, &sender.user_id)
        .await?;
    let events = state
        .iter()
        .map(|e| raw(&client_event(e)))
        .collect::<Result<Vec<_>>>()?;
    Ok(get_state_events::v3::Response::new(events))
}

pub async fn state_key(
    services: AppState,
    req: Ruma<get_state_event_for_key::v3::Request>,
) -> Result<get_state_event_for_key::v3::Response> {
    let sender = req.sender()?;
    let state = services
        .room_state(&req.body.room_id, &sender.user_id)
        .await?;
    let ev = state
        .iter()
        .find(|e| {
            e.pdu.event_type().to_string() == req.body.event_type.to_string()
                && e.pdu.state_key() == Some(req.body.state_key.as_str())
        })
        .ok_or_else(|| Error::not_found("State event not found."))?;
    let value = if req.body.format == get_state_event_for_key::v3::StateEventFormat::Event {
        client_event(ev)
    } else {
        serde_json::from_str(ev.pdu.content().get()).map_err(|e| Error::internal(e.to_string()))?
    };
    Ok(get_state_event_for_key::v3::Response::new(
        serde_json::value::to_raw_value(&value).map_err(|e| Error::internal(e.to_string()))?,
    ))
}

pub async fn messages(
    services: AppState,
    req: Ruma<get_message_events::v3::Request>,
) -> Result<get_message_events::v3::Response> {
    let sender = req.sender()?;
    let dir = match req.body.dir {
        ruma::api::Direction::Forward => Direction::Forwards,
        ruma::api::Direction::Backward => Direction::Backwards,
    };
    let from = req
        .body
        .from
        .as_deref()
        .map(|f| StreamToken::parse(f).ok_or_else(|| Error::invalid_param("Invalid from token.")))
        .transpose()?;
    let to = req
        .body
        .to
        .as_deref()
        .map(|f| StreamToken::parse(f).ok_or_else(|| Error::invalid_param("Invalid to token.")))
        .transpose()?;
    let limit = usize::try_from(u64::from(req.body.limit)).unwrap_or(10);
    let page = services
        .messages(&req.body.room_id, &sender.user_id, from, to, dir, limit)
        .await?;
    let mut resp = get_message_events::v3::Response::new();
    resp.start = page.start.to_string();
    resp.end = page.end.map(|t| t.to_string());
    resp.chunk = page
        .events
        .iter()
        .map(|e| raw(&client_event(e)))
        .collect::<Result<Vec<_>>>()?;
    // Lazy-loaded membership: the member events of everyone who sent something in the chunk.
    if matches!(
        req.body.filter.lazy_load_options,
        ruma::api::client::filter::LazyLoadOptions::Enabled { .. }
    ) {
        let senders: std::collections::HashSet<&ruma::UserId> =
            page.events.iter().map(|e| e.pdu.sender()).collect();
        let state = services.store.current_state_pdus(&req.body.room_id).await?;
        resp.state = state
            .iter()
            .filter(|e| {
                e.pdu.event_type() == &ruma::events::TimelineEventType::RoomMember
                    && e.pdu
                        .state_key()
                        .is_some_and(|k| senders.iter().any(|s| s.as_str() == k))
            })
            .map(|e| raw(&client_event(e)))
            .collect::<Result<Vec<_>>>()?;
    }
    Ok(resp)
}

pub async fn context(
    services: AppState,
    req: Ruma<room::get_room_event::v3::Request>,
) -> Result<get_room_event::v3::Response> {
    let sender = req.sender()?;
    let ev = services
        .get_room_event(&req.body.room_id, &sender.user_id, &req.body.event_id)
        .await?;
    Ok(get_room_event::v3::Response::new(raw(&client_event(&ev))?))
}

pub async fn event_context(
    services: AppState,
    req: Ruma<ruma::api::client::context::get_context::v3::Request>,
) -> Result<ruma::api::client::context::get_context::v3::Response> {
    use ruma::api::client::context::get_context;
    let sender = req.sender()?;
    let limit = usize::try_from(u64::from(req.body.limit)).unwrap_or(10);
    let (before, ev, after, start, end) = services
        .context(
            &req.body.room_id,
            &sender.user_id,
            &req.body.event_id,
            limit,
        )
        .await?;
    let mut resp = get_context::v3::Response::new();
    resp.start = Some(start.to_string());
    resp.end = Some(end.to_string());
    resp.events_before = before
        .iter()
        .map(|e| raw(&client_event(e)))
        .collect::<Result<Vec<_>>>()?;
    resp.event = Some(raw(&client_event(&ev))?);
    resp.events_after = after
        .iter()
        .map(|e| raw(&client_event(e)))
        .collect::<Result<Vec<_>>>()?;
    let state = services.store.current_state_pdus(&req.body.room_id).await?;
    resp.state = state
        .iter()
        .map(|e| raw(&client_event(e)))
        .collect::<Result<Vec<_>>>()?;
    Ok(resp)
}

// ---------------------------------------------------------------- aliases & directory

pub async fn create_alias(
    services: AppState,
    req: Ruma<alias::create_alias::v3::Request>,
) -> Result<alias::create_alias::v3::Response> {
    let sender = req.sender()?;
    if req.body.room_alias.server_name() != services.server_name() {
        return Err(Error::forbidden("Cannot create aliases on other servers."));
    }
    services
        .require_joined(&req.body.room_id, &sender.user_id)
        .await?;
    services
        .store
        .create_alias(tessera_store::RoomAlias {
            alias: req.body.room_alias.clone(),
            room_id: req.body.room_id.clone(),
            creator: Some(sender.user_id.clone()),
        })
        .await
        .map_err(|e| match e {
            tessera_store::Error::AlreadyExists(_) => Error::matrix(
                ruma::api::error::ErrorKind::Unknown,
                http::StatusCode::CONFLICT,
                "Alias already exists.",
            ),
            other => other.into(),
        })?;
    Ok(alias::create_alias::v3::Response::new())
}

pub async fn get_alias(
    services: AppState,
    req: Ruma<alias::get_alias::v3::Request>,
) -> Result<alias::get_alias::v3::Response> {
    if req.body.room_alias.server_name() != services.server_name() {
        let (room_id, servers) = services
            .federation()
            .resolve_alias(&req.body.room_alias)
            .await?;
        return Ok(alias::get_alias::v3::Response::new(room_id, servers));
    }
    let a = services
        .store
        .get_alias(&req.body.room_alias)
        .await?
        .ok_or_else(|| Error::not_found("Room alias not found."))?;
    let mut servers = services.store.servers_in_room(&a.room_id).await?;
    if servers.is_empty() {
        servers.push(services.config.server_name.clone());
    }
    Ok(alias::get_alias::v3::Response::new(a.room_id, servers))
}

pub async fn delete_alias(
    services: AppState,
    req: Ruma<alias::delete_alias::v3::Request>,
) -> Result<alias::delete_alias::v3::Response> {
    let sender = req.sender()?;
    let a = services
        .store
        .get_alias(&req.body.room_alias)
        .await?
        .ok_or_else(|| Error::not_found("Room alias not found."))?;
    let is_creator = a.creator.as_ref() == Some(&sender.user_id);
    if !is_creator {
        let ctx = services.room_context(&a.room_id).await?;
        let user = services.store.get_user(&sender.user_id).await?;
        let admin = user.is_some_and(|u| u.is_admin);
        let can = tessera_service::rooms::power::user_power_level(
            &ctx.state,
            &sender.user_id,
            &ctx.rules,
        ) >= tessera_service::rooms::power::event_level(
            &ctx.state,
            &ruma::events::TimelineEventType::RoomCanonicalAlias,
            true,
        );
        if !admin && !can {
            return Err(Error::forbidden(
                "You are not allowed to delete this alias.",
            ));
        }
    }
    services.store.delete_alias(&req.body.room_alias).await?;
    Ok(alias::delete_alias::v3::Response::new())
}

pub async fn room_aliases(
    services: AppState,
    req: Ruma<room::aliases::v3::Request>,
) -> Result<room::aliases::v3::Response> {
    let sender = req.sender()?;
    if !services
        .can_read_room(&req.body.room_id, &sender.user_id)
        .await?
    {
        return Err(Error::forbidden("You are not allowed to read this room."));
    }
    let aliases = services
        .store
        .aliases_for_room(&req.body.room_id)
        .await?
        .into_iter()
        .map(|a| a.alias)
        .collect();
    Ok(room::aliases::v3::Response::new(aliases))
}

pub async fn get_visibility(
    services: AppState,
    req: Ruma<ruma::api::client::directory::get_room_visibility::v3::Request>,
) -> Result<ruma::api::client::directory::get_room_visibility::v3::Response> {
    use ruma::api::client::{directory::get_room_visibility, room::Visibility};
    let room = services
        .store
        .get_room(&req.body.room_id)
        .await?
        .ok_or_else(|| Error::not_found("Room not found."))?;
    Ok(get_room_visibility::v3::Response::new(if room.is_public {
        Visibility::Public
    } else {
        Visibility::Private
    }))
}

pub async fn set_visibility(
    services: AppState,
    req: Ruma<ruma::api::client::directory::set_room_visibility::v3::Request>,
) -> Result<ruma::api::client::directory::set_room_visibility::v3::Response> {
    use ruma::api::client::{directory::set_room_visibility, room::Visibility};
    let sender = req.sender()?;
    services
        .require_joined(&req.body.room_id, &sender.user_id)
        .await?;
    services
        .store
        .set_room_public(&req.body.room_id, req.body.visibility == Visibility::Public)
        .await?;
    Ok(set_room_visibility::v3::Response::new())
}

pub async fn public_rooms(
    services: AppState,
    req: Ruma<ruma::api::client::directory::get_public_rooms::v3::Request>,
) -> Result<ruma::api::client::directory::get_public_rooms::v3::Response> {
    use ruma::api::client::directory::get_public_rooms;
    let limit = req
        .body
        .limit
        .map_or(100, |l| i64::try_from(u64::from(l)).unwrap_or(100))
        .clamp(1, 500);
    let offset: i64 = req
        .body
        .since
        .as_deref()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let chunk = public_rooms_chunk(&services, limit, offset).await?;
    let mut resp = get_public_rooms::v3::Response::new(chunk);
    let total = services.store.list_public_rooms(i64::MAX, 0).await?.len();
    resp.total_room_count_estimate = Some(UInt::try_from(total).unwrap_or(UInt::MAX));
    if offset + limit < i64::try_from(total).unwrap_or(i64::MAX) {
        resp.next_batch = Some((offset + limit).to_string());
    }
    if offset > 0 {
        resp.prev_batch = Some((offset - limit).max(0).to_string());
    }
    Ok(resp)
}

pub async fn public_rooms_filtered(
    services: AppState,
    req: Ruma<ruma::api::client::directory::get_public_rooms_filtered::v3::Request>,
) -> Result<ruma::api::client::directory::get_public_rooms_filtered::v3::Response> {
    use ruma::api::client::directory::get_public_rooms_filtered;
    let limit = req
        .body
        .limit
        .map_or(100, |l| i64::try_from(u64::from(l)).unwrap_or(100))
        .clamp(1, 500);
    let offset: i64 = req
        .body
        .since
        .as_deref()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let mut chunk = public_rooms_chunk(&services, limit, offset).await?;
    if let Some(term) = req
        .body
        .filter
        .generic_search_term
        .as_deref()
        .map(str::to_lowercase)
    {
        chunk.retain(|c| {
            c.name
                .as_deref()
                .is_some_and(|n| n.to_lowercase().contains(&term))
                || c.topic
                    .as_deref()
                    .is_some_and(|n| n.to_lowercase().contains(&term))
                || c.canonical_alias
                    .as_ref()
                    .is_some_and(|a| a.as_str().to_lowercase().contains(&term))
        });
    }
    let mut resp = get_public_rooms_filtered::v3::Response::new();
    resp.chunk = chunk;
    resp.total_room_count_estimate = Some(
        UInt::try_from(services.store.list_public_rooms(i64::MAX, 0).await?.len() as u64)
            .unwrap_or(UInt::MAX),
    );
    Ok(resp)
}

pub async fn public_rooms_chunk(
    services: &AppState,
    limit: i64,
    offset: i64,
) -> Result<Vec<ruma::directory::PublicRoomsChunk>> {
    let mut out = Vec::new();
    for room in services.store.list_public_rooms(limit, offset).await? {
        let ctx = services.room_context(&room.room_id).await?;
        let get = |ty: ruma::events::StateEventType, field: &str| -> Option<String> {
            ctx.get(ty, "")
                .and_then(|p| serde_json::from_str::<Value>(p.content().get()).ok())
                .and_then(|v| v.get(field).and_then(|f| f.as_str()).map(str::to_owned))
        };
        let joined = services.store.joined_member_count(&room.room_id).await?;
        let join_rule = get(ruma::events::StateEventType::RoomJoinRules, "join_rule")
            .unwrap_or_else(|| "invite".into());
        let init = ruma::directory::PublicRoomsChunkInit {
            num_joined_members: UInt::try_from(u64::try_from(joined).unwrap_or(0))
                .unwrap_or(UInt::MAX),
            room_id: room.room_id.clone(),
            world_readable: get(
                ruma::events::StateEventType::RoomHistoryVisibility,
                "history_visibility",
            )
            .as_deref()
                == Some("world_readable"),
            guest_can_join: get(
                ruma::events::StateEventType::RoomGuestAccess,
                "guest_access",
            )
            .as_deref()
                == Some("can_join"),
        };
        let mut chunk = ruma::directory::PublicRoomsChunk::from(init);
        chunk.name = get(ruma::events::StateEventType::RoomName, "name");
        chunk.topic = get(ruma::events::StateEventType::RoomTopic, "topic");
        chunk.canonical_alias = get(ruma::events::StateEventType::RoomCanonicalAlias, "alias")
            .and_then(|a| a.try_into().ok());
        chunk.avatar_url =
            get(ruma::events::StateEventType::RoomAvatar, "url").map(ruma::OwnedMxcUri::from);
        chunk.join_rule = join_rule.into();
        out.push(chunk);
    }
    Ok(out)
}

/// Silence unused-import lints for module aliases used only in paths.
#[allow(dead_code)]
fn _unused(_: Option<membership::joined_rooms::v3::Request>) {}
