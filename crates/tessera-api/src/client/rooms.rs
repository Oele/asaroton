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
    api::client::{relations, search, threads},
    events::room::member::MembershipState,
    serde::Raw,
};
use serde_json::{Value, json};
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
        extra_join_content(req.json.as_ref()),
    )
    .await?;
    Ok(join_room_by_id::v3::Response::new(req.body.room_id.clone()))
}

/// Join locally when we are already in the room, otherwise through federation.
/// Client-supplied extra membership content on `/join` (everything but the known fields).
fn extra_join_content(json: Option<&Value>) -> Option<serde_json::Map<String, Value>> {
    let obj = json?.as_object()?;
    let extra: serde_json::Map<String, Value> = obj
        .iter()
        .filter(|(k, _)| !matches!(k.as_str(), "reason" | "third_party_signed"))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    (!extra.is_empty()).then_some(extra)
}

async fn join_room(
    services: &AppState,
    sender: &tessera_auth::Sender,
    room_id: &ruma::RoomId,
    via: &[ruma::OwnedServerName],
    reason: Option<&str>,
    extra: Option<serde_json::Map<String, Value>>,
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
            .join_local_room_with(room_id, &sender.user_id, reason, extra.as_ref())
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
        extra_join_content(req.json.as_ref()),
    )
    .await?;
    Ok(join_room_by_id_or_alias::v3::Response::new(room_id))
}

pub async fn knock(
    services: AppState,
    req: Ruma<ruma::api::client::knock::knock_room::v3::Request>,
) -> Result<ruma::api::client::knock::knock_room::v3::Response> {
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
    let local = services.store.room_exists(&room_id).await?
        && services
            .store
            .members(&room_id, Some("join"))
            .await?
            .iter()
            .any(|m| services.is_local(&m.user_id));
    if local {
        services
            .knock_room(&room_id, &sender.user_id, req.body.reason.as_deref())
            .await?;
    } else {
        if via.is_empty()
            && let Some(server) = room_id.server_name()
        {
            via.push(server.to_owned());
        }
        services
            .federation()
            .remote_knock(&sender.user_id, &room_id, &via, req.body.reason.as_deref())
            .await?;
    }
    Ok(ruma::api::client::knock::knock_room::v3::Response::new(
        room_id,
    ))
}

pub async fn leave(
    services: AppState,
    req: Ruma<leave_room::v3::Request>,
) -> Result<leave_room::v3::Response> {
    let sender = req.sender()?;
    if services
        .store
        .get_membership(&req.body.room_id, &sender.user_id)
        .await?
        .is_some_and(|m| m.membership == ruma::events::room::member::MembershipState::Invite)
        && services
            .is_server_notice_room(&sender.user_id, &req.body.room_id)
            .await?
    {
        return Err(Error::matrix(
            ruma::api::error::ErrorKind::CannotLeaveServerNoticeRoom,
            http::StatusCode::FORBIDDEN,
            "You cannot reject this invite.",
        ));
    }
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
    let state = match req.body.at.as_deref() {
        Some(at) => {
            let token =
                StreamToken::parse(at).ok_or_else(|| Error::invalid_param("Invalid at token."))?;
            services
                .room_state_at(&req.body.room_id, &sender.user_id, token)
                .await?
        }
        None => {
            services
                .room_state(&req.body.room_id, &sender.user_id)
                .await?
        }
    };
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
    let f = &req.body.filter;
    let keep = |e: &StoredEvent| -> bool {
        let ty = e.pdu.event_type().to_string();
        if f.types
            .as_ref()
            .is_some_and(|t| !t.iter().any(|p| glob_match(p, &ty)))
        {
            return false;
        }
        if f.not_types.iter().any(|p| glob_match(p, &ty)) {
            return false;
        }
        if f.senders
            .as_ref()
            .is_some_and(|s| !s.iter().any(|u| u == e.pdu.sender()))
        {
            return false;
        }
        if f.not_senders.iter().any(|u| u == e.pdu.sender()) {
            return false;
        }
        if let Some(want) = f.url_filter {
            let has_url = serde_json::from_str::<Value>(e.pdu.content().get())
                .is_ok_and(|c| c.get("url").is_some());
            let want = matches!(want, ruma::api::client::filter::UrlFilter::EventsWithUrl);
            if has_url != want {
                return false;
            }
        }
        true
    };
    let page = services
        .messages(
            &req.body.room_id,
            &sender.user_id,
            from,
            to,
            dir,
            limit,
            &keep,
        )
        .await?;
    let mut resp = get_message_events::v3::Response::new();
    resp.start = page.start.to_string();
    resp.end = page.end.map(|t| t.to_string());
    let mut chunk = services
        .client_events(&page.events, Some(&sender.user_id))
        .await?;
    add_txn_ids(&services, sender, &page.events, &mut chunk).await?;
    resp.chunk = chunk.iter().map(raw).collect::<Result<Vec<_>>>()?;
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
    // Whether the room is unreadable or the event invisible, the answer is the same 404, so
    // the endpoint cannot be used to probe for rooms or events.
    let ev = services
        .get_room_event(&req.body.room_id, &sender.user_id, &req.body.event_id)
        .await
        .map_err(|_| Error::not_found("Event not found."))?;
    if !services.event_visible_to(&ev, &sender.user_id).await? {
        return Err(Error::not_found("Event not found."));
    }
    let mut v = services.client_event(&ev, Some(&sender.user_id)).await?;
    add_txn_ids(
        &services,
        sender,
        std::slice::from_ref(&ev),
        std::slice::from_mut(&mut v),
    )
    .await?;
    Ok(get_room_event::v3::Response::new(raw(&v)?))
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
    if !services.event_visible_to(&ev, &sender.user_id).await? {
        return Err(Error::not_found("Event not found."));
    }
    let before = services
        .filter_visible(&req.body.room_id, &sender.user_id, before)
        .await?;
    let after = services
        .filter_visible(&req.body.room_id, &sender.user_id, after)
        .await?;
    let mut resp = get_context::v3::Response::new();
    resp.start = Some(start.to_string());
    resp.end = Some(end.to_string());
    let user = Some(&*sender.user_id);
    resp.events_before = services
        .client_events(&before, user)
        .await?
        .iter()
        .map(raw)
        .collect::<Result<Vec<_>>>()?;
    let mut anchor = services.client_event(&ev, user).await?;
    add_txn_ids(
        &services,
        sender,
        std::slice::from_ref(&ev),
        std::slice::from_mut(&mut anchor),
    )
    .await?;
    resp.event = Some(raw(&anchor)?);
    resp.events_after = services
        .client_events(&after, user)
        .await?
        .iter()
        .map(raw)
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
    let mut found = services.store.get_alias(&req.body.room_alias).await?;
    if found.is_none()
        && services
            .query_appservice_alias(&req.body.room_alias)
            .await?
    {
        found = services.store.get_alias(&req.body.room_alias).await?;
    }
    let a = found.ok_or_else(|| Error::not_found("Room alias not found."))?;
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
    // Drop it from m.room.canonical_alias too, when the deleter may do so.
    if let Ok(Some(mut content)) = services
        .state_content(
            &a.room_id,
            &ruma::events::StateEventType::RoomCanonicalAlias,
            "",
        )
        .await
    {
        let alias_str = req.body.room_alias.as_str();
        let mut changed = false;
        if content.get("alias").and_then(Value::as_str) == Some(alias_str) {
            content.as_object_mut().map(|o| o.remove("alias"));
            changed = true;
        }
        if let Some(Value::Array(alts)) = content.get_mut("alt_aliases") {
            let before = alts.len();
            alts.retain(|v| v.as_str() != Some(alias_str));
            changed |= alts.len() != before;
        }
        if changed {
            let _ = services
                .send_state(
                    &a.room_id,
                    &sender.user_id,
                    "m.room.canonical_alias",
                    "",
                    content,
                )
                .await;
        }
    }
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

// ---------------------------------------------------------------- relations & threads

fn relation_dir(d: ruma::api::Direction) -> Direction {
    match d {
        ruma::api::Direction::Forward => Direction::Forwards,
        ruma::api::Direction::Backward => Direction::Backwards,
    }
}

fn parse_token(s: Option<&str>, what: &str) -> Result<Option<StreamToken>> {
    s.map(|t| {
        StreamToken::parse(t).ok_or_else(|| Error::invalid_param(format!("Invalid {what} token.")))
    })
    .transpose()
}

#[allow(clippy::too_many_arguments)]
async fn relations_common(
    services: &AppState,
    sender: &tessera_auth::Sender,
    room_id: &ruma::RoomId,
    event_id: &ruma::EventId,
    rel_type: Option<&str>,
    event_type: Option<&str>,
    recurse: bool,
    from: Option<&str>,
    to: Option<&str>,
    dir: ruma::api::Direction,
    limit: Option<ruma::UInt>,
) -> Result<(
    Vec<Raw<ruma::events::AnyMessageLikeEvent>>,
    Option<String>,
    Option<String>,
)> {
    let limit = limit.map_or(5, |l| usize::try_from(u64::from(l)).unwrap_or(5));
    let page = services
        .relations(
            room_id,
            &sender.user_id,
            event_id,
            rel_type,
            event_type,
            recurse,
            parse_token(from, "from")?,
            parse_token(to, "to")?,
            relation_dir(dir),
            limit,
        )
        .await?;
    let chunk = services
        .client_events(&page.events, Some(&sender.user_id))
        .await?
        .iter()
        .map(raw)
        .collect::<Result<Vec<_>>>()?;
    Ok((
        chunk,
        page.next_batch.map(|t| t.to_string()),
        page.prev_batch.map(|t| t.to_string()),
    ))
}

pub async fn relations(
    services: AppState,
    req: Ruma<relations::get_relating_events::v1::Request>,
) -> Result<relations::get_relating_events::v1::Response> {
    let sender = req.sender()?;
    let b = &req.body;
    let (chunk, next, prev) = relations_common(
        &services,
        sender,
        &b.room_id,
        &b.event_id,
        None,
        None,
        b.recurse,
        b.from.as_deref(),
        b.to.as_deref(),
        b.dir,
        b.limit,
    )
    .await?;
    let mut resp = relations::get_relating_events::v1::Response::new(chunk);
    resp.next_batch = next;
    resp.prev_batch = prev;
    resp.recursion_depth = b.recurse.then_some(ruma::UInt::from(3u32));
    Ok(resp)
}

pub async fn relations_with_rel_type(
    services: AppState,
    req: Ruma<relations::get_relating_events_with_rel_type::v1::Request>,
) -> Result<relations::get_relating_events_with_rel_type::v1::Response> {
    let sender = req.sender()?;
    let b = &req.body;
    let rel = b.rel_type.to_string();
    let (chunk, next, prev) = relations_common(
        &services,
        sender,
        &b.room_id,
        &b.event_id,
        Some(&rel),
        None,
        b.recurse,
        b.from.as_deref(),
        b.to.as_deref(),
        b.dir,
        b.limit,
    )
    .await?;
    let mut resp = relations::get_relating_events_with_rel_type::v1::Response::new(chunk);
    resp.next_batch = next;
    resp.prev_batch = prev;
    resp.recursion_depth = b.recurse.then_some(ruma::UInt::from(3u32));
    Ok(resp)
}

pub async fn relations_with_rel_and_event_type(
    services: AppState,
    req: Ruma<relations::get_relating_events_with_rel_type_and_event_type::v1::Request>,
) -> Result<relations::get_relating_events_with_rel_type_and_event_type::v1::Response> {
    let sender = req.sender()?;
    let b = &req.body;
    let rel = b.rel_type.to_string();
    let ty = b.event_type.to_string();
    let (chunk, next, prev) = relations_common(
        &services,
        sender,
        &b.room_id,
        &b.event_id,
        Some(&rel),
        Some(&ty),
        b.recurse,
        b.from.as_deref(),
        b.to.as_deref(),
        b.dir,
        b.limit,
    )
    .await?;
    let mut resp =
        relations::get_relating_events_with_rel_type_and_event_type::v1::Response::new(chunk);
    resp.next_batch = next;
    resp.prev_batch = prev;
    resp.recursion_depth = b.recurse.then_some(ruma::UInt::from(3u32));
    Ok(resp)
}

pub async fn threads(
    services: AppState,
    req: Ruma<threads::get_threads::v1::Request>,
) -> Result<threads::get_threads::v1::Response> {
    let sender = req.sender()?;
    let b = &req.body;
    let limit = b
        .limit
        .map_or(10, |l| usize::try_from(u64::from(l)).unwrap_or(10));
    let participated = matches!(
        b.include,
        threads::get_threads::v1::IncludeThreads::Participated
    );
    let (summaries, next) = services
        .threads(
            &b.room_id,
            &sender.user_id,
            participated,
            parse_token(b.from.as_deref(), "from")?,
            limit,
        )
        .await?;
    let roots: Vec<StoredEvent> = summaries.into_iter().map(|t| t.root).collect();
    let chunk = services
        .client_events(&roots, Some(&sender.user_id))
        .await?
        .iter()
        .map(raw)
        .collect::<Result<Vec<_>>>()?;
    let mut resp = threads::get_threads::v1::Response::new(chunk);
    resp.next_batch = next.map(|t| t.to_string());
    Ok(resp)
}

// ---------------------------------------------------------------- upgrade, reports, search

pub async fn upgrade(
    services: AppState,
    req: Ruma<room::upgrade_room::v3::Request>,
) -> Result<room::upgrade_room::v3::Response> {
    let sender = req.sender()?;
    if !tessera_core::room_version::is_supported(&req.body.new_version) {
        return Err(Error::matrix(
            ruma::api::error::ErrorKind::UnsupportedRoomVersion,
            http::StatusCode::BAD_REQUEST,
            "Unsupported room version.",
        ));
    }
    let new_room = services
        .upgrade_room(
            &sender.user_id,
            &req.body.room_id,
            req.body.new_version.clone(),
            req.body.additional_creators.clone(),
        )
        .await?;
    Ok(room::upgrade_room::v3::Response::new(new_room))
}

pub async fn report_event(
    services: AppState,
    req: Ruma<room::report_content::v3::Request>,
) -> Result<room::report_content::v3::Response> {
    let sender = req.sender()?;
    // The event must be visible to the reporter; otherwise 404 (after a short delay, so the
    // endpoint cannot be used to probe for events).
    if services
        .get_room_event(&req.body.room_id, &sender.user_id, &req.body.event_id)
        .await
        .is_err()
    {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        return Err(Error::not_found("Event not found."));
    }
    tracing::info!(reporter = %sender.user_id, room = %req.body.room_id, event = %req.body.event_id, reason = ?req.body.reason, "event reported");
    Ok(room::report_content::v3::Response::new())
}

pub async fn report_room(
    services: AppState,
    req: Ruma<room::report_room::v3::Request>,
) -> Result<room::report_room::v3::Response> {
    let sender = req.sender()?;
    if services.store.get_room(&req.body.room_id).await?.is_none()
        || !services
            .can_read_room(&req.body.room_id, &sender.user_id)
            .await?
    {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        return Err(Error::not_found("Room not found."));
    }
    tracing::info!(reporter = %sender.user_id, room = %req.body.room_id, reason = %req.body.reason, "room reported");
    Ok(room::report_room::v3::Response::new())
}

pub async fn report_user(
    _services: AppState,
    req: Ruma<ruma::api::client::reporting::report_user::v3::Request>,
) -> Result<ruma::api::client::reporting::report_user::v3::Response> {
    let sender = req.sender()?;
    tracing::info!(reporter = %sender.user_id, user = %req.body.user_id, reason = %req.body.reason, "user reported");
    Ok(ruma::api::client::reporting::report_user::v3::Response::new())
}

pub fn search_request(
    req: &Ruma<search::search_events::v3::Request>,
) -> Result<Option<(tessera_service::rooms::search::SearchRequest, bool)>> {
    use search::search_events::v3::OrderBy;
    use tessera_service::rooms::search::SearchRequest;
    let Some(c) = &req.body.search_categories.room_events else {
        return Ok(None);
    };
    let offset = match req.body.next_batch.as_deref() {
        Some(t) => t
            .parse::<usize>()
            .map_err(|_| Error::invalid_param("Invalid next_batch token."))?,
        None => 0,
    };
    Ok(Some((
        SearchRequest {
            term: c.search_term.clone(),
            rooms: c.filter.rooms.clone(),
            not_rooms: c.filter.not_rooms.clone(),
            senders: c.filter.senders.clone(),
            not_senders: c.filter.not_senders.clone(),
            types: c.filter.types.clone(),
            not_types: c.filter.not_types.clone(),
            order_recent: matches!(c.order_by, Some(OrderBy::Recent)),
            before_limit: usize::try_from(u64::from(c.event_context.before_limit)).unwrap_or(5),
            after_limit: usize::try_from(u64::from(c.event_context.after_limit)).unwrap_or(5),
            include_profile: c.event_context.include_profile,
            offset,
            limit: c
                .filter
                .limit
                .map_or(10, |l| usize::try_from(u64::from(l)).unwrap_or(10)),
        },
        c.include_state.unwrap_or(false),
    )))
}

/// `/search`, rendered as explicit JSON so `results` and `count` are always present.
pub async fn search(
    services: AppState,
    req: Ruma<search::search_events::v3::Request>,
) -> Result<axum::response::Response> {
    use axum::response::IntoResponse;
    let sender = req.sender()?;
    let Some((request, _include_state)) = search_request(&req)? else {
        return Ok(axum::Json(json!({ "search_categories": {} })).into_response());
    };
    let page = services.search(&sender.user_id, request).await?;
    let user = Some(&*sender.user_id);
    let mut results = Vec::with_capacity(page.results.len());
    for r in &page.results {
        let mut profile_info = serde_json::Map::new();
        for (u, name, avatar) in &r.profiles {
            let mut p = serde_json::Map::new();
            if let Some(n) = name {
                p.insert("displayname".into(), json!(n));
            }
            if let Some(a) = avatar {
                p.insert("avatar_url".into(), json!(a));
            }
            profile_info.insert(u.to_string(), Value::Object(p));
        }
        results.push(json!({
            "rank": r.rank,
            "result": services.client_event(&r.event, user).await?,
            "context": {
                "events_before": services.client_events(&r.before, user).await?,
                "events_after": services.client_events(&r.after, user).await?,
                "start": r.start.to_string(),
                "end": r.end.to_string(),
                "profile_info": profile_info,
            },
        }));
    }
    let mut room_events = json!({
        "count": page.count,
        "results": results,
        "highlights": [],
    });
    if let Some(o) = page.next_offset {
        room_events["next_batch"] = json!(o.to_string());
    }
    let mut resp =
        axum::Json(json!({ "search_categories": { "room_events": room_events } })).into_response();
    crate::extract::add_cors(resp.headers_mut());
    Ok(resp)
}

/// `unsigned.transaction_id` for events the requesting device sent itself.
async fn add_txn_ids(
    services: &AppState,
    sender: &tessera_auth::Sender,
    events: &[StoredEvent],
    values: &mut [Value],
) -> Result<()> {
    let Some(device_id) = &sender.device_id else {
        return Ok(());
    };
    for (ev, v) in events.iter().zip(values.iter_mut()) {
        if ev.pdu.sender() != sender.user_id {
            continue;
        }
        if let Some(txn) = services
            .store
            .get_event_txn_id(ev.pdu.event_id(), &sender.user_id, Some(device_id))
            .await?
            && let Some(u) = v.get_mut("unsigned").and_then(Value::as_object_mut)
        {
            u.insert("transaction_id".into(), serde_json::json!(txn));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------- URL previews

pub async fn preview_url(
    services: AppState,
    req: Ruma<ruma::api::client::authenticated_media::get_media_preview::v1::Request>,
) -> Result<ruma::api::client::authenticated_media::get_media_preview::v1::Response> {
    let _ = req.sender()?;
    let data = services.federation().preview_url(&req.body.url).await?;
    let mut resp = ruma::api::client::authenticated_media::get_media_preview::v1::Response::new();
    resp.data =
        Some(serde_json::value::to_raw_value(&data).map_err(|e| Error::internal(e.to_string()))?);
    Ok(resp)
}

#[allow(deprecated)]
pub async fn preview_url_legacy(
    services: AppState,
    req: Ruma<ruma::api::client::media::get_media_preview::v3::Request>,
) -> Result<ruma::api::client::media::get_media_preview::v3::Response> {
    let _ = req.sender()?;
    let data = services.federation().preview_url(&req.body.url).await?;
    let mut resp = ruma::api::client::media::get_media_preview::v3::Response::new();
    resp.data =
        Some(serde_json::value::to_raw_value(&data).map_err(|e| Error::internal(e.to_string()))?);
    Ok(resp)
}

/// Matrix filter globs: `*` matches anything, `?` one character.
fn glob_match(pattern: &str, value: &str) -> bool {
    fn rec(p: &[char], v: &[char]) -> bool {
        match (p.first(), v.first()) {
            (None, None) => true,
            (Some('*'), _) => rec(&p[1..], v) || (!v.is_empty() && rec(p, &v[1..])),
            (Some('?'), Some(_)) => rec(&p[1..], &v[1..]),
            (Some(a), Some(b)) if a == b => rec(&p[1..], &v[1..]),
            _ => false,
        }
    }
    let p: Vec<char> = pattern.chars().collect();
    let v: Vec<char> = value.chars().collect();
    rec(&p, &v)
}

// ---------------------------------------------------------------- spaces

pub async fn hierarchy(
    services: AppState,
    req: Ruma<ruma::api::client::space::get_hierarchy::v1::Request>,
) -> Result<axum::response::Response> {
    use axum::response::IntoResponse;
    let sender = req.sender()?;
    let b = &req.body;
    let (rooms, next_batch) = services
        .hierarchy(
            &sender.user_id,
            &b.room_id,
            b.suggested_only,
            b.max_depth
                .map(|d| usize::try_from(u64::from(d)).unwrap_or(usize::MAX)),
            b.limit
                .map(|l| usize::try_from(u64::from(l)).unwrap_or(usize::MAX)),
            b.from.as_deref(),
        )
        .await?;
    let mut body = json!({ "rooms": rooms });
    if let Some(n) = next_batch {
        body["next_batch"] = json!(n);
    }
    let mut resp = axum::Json(body).into_response();
    crate::extract::add_cors(resp.headers_mut());
    Ok(resp)
}

// ---------------------------------------------------------------- timestamp lookup, summary

pub async fn timestamp_to_event(
    services: AppState,
    req: Ruma<ruma::api::client::room::get_event_by_timestamp::v1::Request>,
) -> Result<ruma::api::client::room::get_event_by_timestamp::v1::Response> {
    let sender = req.sender()?;
    // Members only, whatever the room's visibility: this endpoint must not leak history.
    services
        .require_joined(&req.body.room_id, &sender.user_id)
        .await
        .map_err(|_| Error::forbidden("You are not in this room."))?;
    let dir = match req.body.dir {
        ruma::api::Direction::Forward => Direction::Forwards,
        ruma::api::Direction::Backward => Direction::Backwards,
    };
    let ts = i64::try_from(u64::from(req.body.ts.0)).unwrap_or(i64::MAX);
    let ev = services
        .store
        .event_at_timestamp(&req.body.room_id, ts, dir)
        .await?
        .ok_or_else(|| Error::not_found("No event found in that direction."))?;
    Ok(
        ruma::api::client::room::get_event_by_timestamp::v1::Response::new(
            ev.pdu.event_id().to_owned(),
            ev.pdu.origin_server_ts(),
        ),
    )
}

/// `/room_summary/{roomIdOrAlias}`, rendered as explicit JSON so every summary field is
/// present (the typed response drops default values such as a public join rule).
pub async fn room_summary(
    services: AppState,
    req: Ruma<ruma::api::client::room::get_summary::v1::Request>,
) -> Result<axum::response::Response> {
    use axum::response::IntoResponse;
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
                via.extend(servers);
                room_id
            }
        }
    };
    let user = req.sender.as_ref().map(|s| s.user_id.clone());
    let local = services.store.room_exists(&room_id).await?
        && !services.store.current_state(&room_id).await?.is_empty();
    let summary_json = if local {
        let accessible = match &user {
            Some(u) => services.room_accessible_to_user(&room_id, u).await?,
            None => {
                let c = services.room_summary_json(&room_id).await?;
                c.get("join_rule").and_then(Value::as_str) == Some("public")
                    || c.get("world_readable").and_then(Value::as_bool) == Some(true)
            }
        };
        if !accessible {
            return Err(Error::not_found("Room is not accessible."));
        }
        services.room_summary_json(&room_id).await?
    } else {
        if let Some(server) = room_id.server_name()
            && !via.iter().any(|s| *s == server)
        {
            via.push(server.to_owned());
        }
        let resp = services
            .federation()
            .remote_hierarchy(&via, &room_id, false)
            .await
            .map_err(|_| Error::not_found("Room is not accessible."))?;
        let mut room = resp
            .get("room")
            .cloned()
            .ok_or_else(|| Error::not_found("Room is not accessible."))?;
        if let Some(o) = room.as_object_mut() {
            o.remove("children_state");
        }
        room
    };
    let mut body = summary_json;
    if let Some(u) = &user
        && let Some(m) = services.store.get_membership(&room_id, u).await?
    {
        body["membership"] = json!(m.membership);
    }
    let mut resp = axum::Json(body).into_response();
    crate::extract::add_cors(resp.headers_mut());
    Ok(resp)
}

/// The public rooms response as explicit JSON: the typed response drops a `public` join rule
/// as a default, but clients expect it.
pub fn public_rooms_json(
    chunk: &[ruma::directory::PublicRoomsChunk],
    total: Option<UInt>,
    next_batch: Option<&str>,
    prev_batch: Option<&str>,
) -> Value {
    let rooms: Vec<Value> = chunk
        .iter()
        .map(|c| {
            let mut v = serde_json::to_value(c).unwrap_or_default();
            if v.get("join_rule").is_none() {
                v["join_rule"] = json!("public");
            }
            v
        })
        .collect();
    let mut body = json!({ "chunk": rooms });
    if let Some(t) = total {
        body["total_room_count_estimate"] = json!(t);
    }
    if let Some(n) = next_batch {
        body["next_batch"] = json!(n);
    }
    if let Some(p) = prev_batch {
        body["prev_batch"] = json!(p);
    }
    body
}
