//! `GET /sync` (classic). The response is assembled by hand so every field the spec
//! requires is present (ruma's types omit several when empty or false).

use std::time::Duration;

use axum::{Json, response::IntoResponse};
use ruma::api::client::{
    filter::{FilterDefinition, LazyLoadOptions},
    sync::sync_events::v3::{self as sync_v3, Filter},
};
use serde_json::{Map, Value, json};
use tessera_service::sync::{RoomSync, SyncFilter, SyncRequest, SyncToken};
use tessera_store::StoredEvent;

use crate::{AppState, Error, Result, Ruma, client::rooms::client_event};

/// Turn a ruma filter into the subset we apply.
pub fn sync_filter(f: &FilterDefinition) -> SyncFilter {
    let mut out = SyncFilter::default();
    let tl = &f.room.timeline;
    if let Some(l) = tl.limit {
        out.timeline_limit = usize::try_from(u64::from(l)).unwrap_or(10);
    }
    out.unread_thread_notifications = tl.unread_thread_notifications;
    {}
    out.timeline_types.clone_from(&tl.types);
    out.timeline_not_types.clone_from(&tl.not_types);
    out.timeline_senders.clone_from(&tl.senders);
    out.timeline_not_senders.clone_from(&tl.not_senders);
    out.state_types.clone_from(&f.room.state.types);
    out.state_not_types.clone_from(&f.room.state.not_types);
    out.lazy_load_members = matches!(
        f.room.state.lazy_load_options,
        LazyLoadOptions::Enabled { .. }
    ) || matches!(tl.lazy_load_options, LazyLoadOptions::Enabled { .. });
    out.include_redundant_members = matches!(
        f.room.state.lazy_load_options,
        LazyLoadOptions::Enabled {
            include_redundant_members: true
        }
    );
    out.include_leave = f.room.include_leave;
    out.rooms.clone_from(&f.room.rooms);
    out.not_rooms.clone_from(&f.room.not_rooms);
    out.presence_enabled = f.presence.types.as_ref().is_none_or(|t| !t.is_empty())
        && !f
            .presence
            .not_types
            .iter()
            .any(|t| t == "m.presence" || t == "*");
    out
}

async fn events_json(
    services: &AppState,
    user: &ruma::UserId,
    events: &[StoredEvent],
    room: &RoomSync,
    aggregations: bool,
) -> Result<Vec<Value>> {
    let mut out = if aggregations {
        services.client_events(events, Some(user)).await?
    } else {
        events.iter().map(client_event).collect()
    };
    for (v, e) in out.iter_mut().zip(events) {
        if let Some(txn) = room.txn_ids.get(e.pdu.event_id())
            && let Some(u) = v.get_mut("unsigned").and_then(|u| u.as_object_mut())
        {
            u.insert("transaction_id".into(), json!(txn));
        }
    }
    Ok(out)
}

async fn room_json(
    services: &AppState,
    user: &ruma::UserId,
    room: &RoomSync,
    joined: bool,
) -> Result<Value> {
    let mut v = Map::new();
    let mut timeline = json!({
        "events": events_json(services, user, &room.timeline, room, true).await?,
        "limited": room.limited,
    });
    if let Some(pb) = &room.prev_batch {
        timeline["prev_batch"] = json!(pb);
    }
    v.insert("timeline".into(), timeline);
    match &room.state_after {
        Some(after) => {
            v.insert(
                "state_after".into(),
                json!({ "events": events_json(services, user, after, room, false).await? }),
            );
        }
        None => {
            v.insert(
                "state".into(),
                json!({ "events": events_json(services, user, &room.state, room, false).await? }),
            );
        }
    }
    v.insert(
        "account_data".into(),
        json!({ "events": room.account_data }),
    );
    if joined {
        v.insert("ephemeral".into(), json!({ "events": room.ephemeral }));
        let mut summary = json!({
            "m.joined_member_count": room.joined_count,
            "m.invited_member_count": room.invited_count,
        });
        if !room.heroes.is_empty() {
            summary["m.heroes"] = json!(room.heroes);
        }
        v.insert("summary".into(), summary);
        v.insert(
            "unread_notifications".into(),
            json!({ "notification_count": room.unread_notifications, "highlight_count": room.unread_highlights }),
        );
        if !room.unread_threads.is_empty() {
            let threads: Map<String, Value> = room
                .unread_threads
                .iter()
                .map(|(k, (n, h))| {
                    (
                        k.to_string(),
                        json!({ "notification_count": n, "highlight_count": h }),
                    )
                })
                .collect();
            v.insert("unread_thread_notifications".into(), Value::Object(threads));
        }
    }
    Ok(Value::Object(v))
}

pub async fn sync(
    services: AppState,
    req: Ruma<sync_v3::Request>,
) -> Result<axum::response::Response> {
    let sender = req.sender()?;
    let since = match req.body.since.as_deref() {
        None | Some("") => None,
        Some(s) => {
            Some(SyncToken::parse(s).ok_or_else(|| Error::invalid_param("Invalid since token."))?)
        }
    };

    let filter: Option<FilterDefinition> = match &req.body.filter {
        Some(Filter::FilterDefinition(f)) => Some(f.clone()),
        Some(Filter::FilterId(id)) => {
            let id: i64 = id
                .parse()
                .map_err(|_| Error::invalid_param("Invalid filter ID."))?;
            services
                .store
                .get_filter(&sender.user_id, id)
                .await?
                .map(serde_json::from_value)
                .transpose()
                .map_err(|e| Error::internal(e.to_string()))?
        }
        _ => None,
    };
    let filter = filter.as_ref().map(sync_filter).unwrap_or_default();

    let result = services
        .sync(SyncRequest {
            user_id: sender.user_id.clone(),
            device_id: sender.device_id.clone(),
            since,
            timeout: req
                .body
                .timeout
                .unwrap_or(Duration::ZERO)
                .min(Duration::from_secs(300)),
            full_state: req.body.full_state,
            filter,
            set_presence: req.body.set_presence.clone(),
            use_state_after: req.body.use_state_after,
        })
        .await?;

    let mut join = Map::new();
    for (room_id, r) in &result.join {
        join.insert(
            room_id.to_string(),
            room_json(&services, &sender.user_id, r, true).await?,
        );
    }
    let mut leave = Map::new();
    for (room_id, r) in &result.leave {
        leave.insert(
            room_id.to_string(),
            room_json(&services, &sender.user_id, r, false).await?,
        );
    }
    let mut invite = Map::new();
    for (room_id, state) in &result.invite {
        invite.insert(
            room_id.to_string(),
            json!({ "invite_state": { "events": state } }),
        );
    }
    let mut knock = Map::new();
    for (room_id, state) in &result.knock {
        knock.insert(
            room_id.to_string(),
            json!({ "knock_state": { "events": state } }),
        );
    }
    let otk: Map<String, Value> = result
        .one_time_key_counts
        .iter()
        .map(|(k, v)| (k.clone(), json!(v)))
        .collect();
    let body = json!({
        "next_batch": result.next_batch.to_string(),
        "rooms": { "join": join, "leave": leave, "invite": invite, "knock": knock },
        "presence": { "events": result.presence },
        "account_data": { "events": result.account_data },
        "to_device": { "events": result.to_device },
        "device_lists": { "changed": result.device_lists_changed, "left": result.device_lists_left },
        "device_one_time_keys_count": otk,
        "device_unused_fallback_key_types": result.unused_fallback_key_types,
        "org.matrix.msc2732.device_unused_fallback_key_types": result.unused_fallback_key_types,
    });
    let mut resp = Json(body).into_response();
    crate::extract::add_cors(resp.headers_mut());
    Ok(resp)
}
