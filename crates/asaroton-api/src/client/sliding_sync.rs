//! `POST /_matrix/client/unstable/org.matrix.simplified_msc3575/sync` (Simplified Sliding
//! Sync, MSC4186). The response is built by hand so every field is present.

use std::time::Duration;

use asaroton_service::sliding_sync::{
    ExtensionScope, ExtensionsRequest, ListFilters, ListRequest, RoomResult,
    RoomSubscriptionRequest, ScopeRoom, SlidingSyncRequest, ToDeviceRequest,
};
use axum::{Json, response::IntoResponse};
use ruma::api::client::sync::sync_events::v5;
use serde_json::{Map, Value, json};

use crate::{AppState, Result, Ruma, client::rooms::client_event};

fn room_type_filter(f: &ruma::directory::RoomTypeFilter) -> Option<String> {
    match serde_json::to_value(f) {
        Ok(Value::String(s)) => Some(s),
        _ => None,
    }
}

fn required_state(v: &[(ruma::events::StateEventType, String)]) -> Vec<(String, String)> {
    v.iter().map(|(t, k)| (t.to_string(), k.clone())).collect()
}

fn limit(v: ruma::UInt) -> usize {
    usize::try_from(u64::from(v)).unwrap_or(usize::MAX)
}

fn scope(
    lists: Option<&Vec<String>>,
    rooms: Option<&Vec<v5::request::ExtensionRoomConfig>>,
) -> ExtensionScope {
    ExtensionScope {
        lists: lists.cloned(),
        rooms: rooms.map(|r| {
            r.iter()
                .map(|e| match e {
                    v5::request::ExtensionRoomConfig::Room(id) => ScopeRoom::Room(id.clone()),
                    _ => ScopeRoom::AllSubscribed,
                })
                .collect()
        }),
    }
}

async fn room_json(services: &AppState, user: &ruma::UserId, r: &RoomResult) -> Result<Value> {
    let mut v = Map::new();
    if r.initial {
        v.insert("initial".into(), json!(true));
    }
    if let Some(n) = &r.name {
        v.insert("name".into(), json!(n));
    }
    if let Some(a) = &r.avatar {
        v.insert("avatar".into(), json!(a));
    }
    if r.is_dm {
        v.insert("is_dm".into(), json!(true));
    }
    v.insert("bump_stamp".into(), json!(r.bump_stamp));
    if let Some(inv) = &r.invite_state {
        v.insert("invite_state".into(), json!(inv));
        return Ok(Value::Object(v));
    }
    let mut timeline = services.client_events(&r.timeline, Some(user)).await?;
    for (ev, e) in timeline.iter_mut().zip(&r.timeline) {
        if let Some(txn) = r.txn_ids.get(e.pdu.event_id())
            && let Some(u) = ev.get_mut("unsigned").and_then(Value::as_object_mut)
        {
            u.insert("transaction_id".into(), json!(txn));
        }
    }
    let required_state: Vec<Value> = r.required_state.iter().map(client_event).collect();
    v.insert("required_state".into(), Value::Array(required_state));
    v.insert("timeline".into(), Value::Array(timeline));
    if let Some(pb) = &r.prev_batch {
        v.insert("prev_batch".into(), json!(pb));
    }
    v.insert("limited".into(), json!(r.limited));
    v.insert("num_live".into(), json!(r.num_live));
    v.insert("joined_count".into(), json!(r.joined_count));
    v.insert("invited_count".into(), json!(r.invited_count));
    v.insert("notification_count".into(), json!(r.notification_count));
    v.insert("highlight_count".into(), json!(r.highlight_count));
    v.insert(
        "unread_notifications".into(),
        json!({ "notification_count": r.notification_count, "highlight_count": r.highlight_count }),
    );
    if !r.heroes.is_empty() {
        let heroes: Vec<Value> = r
            .heroes
            .iter()
            .map(|h| {
                let mut o = json!({ "user_id": h.user_id });
                if let Some(n) = &h.name {
                    o["name"] = json!(n);
                }
                if let Some(a) = &h.avatar {
                    o["avatar"] = json!(a);
                }
                o
            })
            .collect();
        v.insert("heroes".into(), Value::Array(heroes));
    }
    Ok(Value::Object(v))
}

pub async fn sliding_sync(
    services: AppState,
    req: Ruma<v5::Request>,
) -> Result<axum::response::Response> {
    let sender = req.sender()?;
    let b = &req.body;
    let lists = b
        .lists
        .iter()
        .map(|(name, l)| {
            (
                name.clone(),
                ListRequest {
                    ranges: l
                        .ranges
                        .iter()
                        .map(|(a, z)| (u64::from(*a), u64::from(*z)))
                        .collect(),
                    required_state: required_state(&l.room_details.required_state),
                    timeline_limit: limit(l.room_details.timeline_limit),
                    filters: l.filters.as_ref().map(|f| ListFilters {
                        is_dm: f.is_dm,
                        is_encrypted: f.is_encrypted,
                        is_invite: f.is_invite,
                        room_types: if f.room_types.is_empty() {
                            None
                        } else {
                            Some(f.room_types.iter().map(room_type_filter).collect())
                        },
                        not_room_types: f.not_room_types.iter().map(room_type_filter).collect(),
                    }),
                },
            )
        })
        .collect();
    let room_subscriptions = b
        .room_subscriptions
        .iter()
        .map(|(id, s)| {
            (
                id.clone(),
                RoomSubscriptionRequest {
                    required_state: required_state(&s.required_state),
                    timeline_limit: limit(s.timeline_limit),
                },
            )
        })
        .collect();
    let x = &b.extensions;
    let extensions = ExtensionsRequest {
        to_device: (x.to_device.enabled == Some(true)).then(|| ToDeviceRequest {
            since: x.to_device.since.clone(),
            limit: x.to_device.limit.map_or(0, limit),
        }),
        e2ee: x.e2ee.enabled == Some(true),
        account_data: (x.account_data.enabled == Some(true))
            .then(|| scope(x.account_data.lists.as_ref(), x.account_data.rooms.as_ref())),
        receipts: (x.receipts.enabled == Some(true))
            .then(|| scope(x.receipts.lists.as_ref(), x.receipts.rooms.as_ref())),
        typing: (x.typing.enabled == Some(true))
            .then(|| scope(x.typing.lists.as_ref(), x.typing.rooms.as_ref())),
        thread_subscriptions: (x.thread_subscriptions.enabled == Some(true))
            .then(|| x.thread_subscriptions.limit.map_or(100, limit)),
    };
    let result = services
        .sliding_sync(SlidingSyncRequest {
            user_id: sender.user_id.clone(),
            device_id: sender.device_id.clone(),
            conn_id: b.conn_id.clone(),
            pos: b.pos.clone(),
            timeout: b
                .timeout
                .unwrap_or(Duration::ZERO)
                .min(Duration::from_secs(300)),
            set_presence: b.set_presence.clone(),
            lists,
            room_subscriptions,
            extensions,
        })
        .await?;

    let mut rooms = Map::new();
    for (room_id, r) in &result.rooms {
        rooms.insert(
            room_id.to_string(),
            room_json(&services, &sender.user_id, r).await?,
        );
    }
    let lists: Map<String, Value> = result
        .lists
        .iter()
        .map(|(name, count)| (name.clone(), json!({ "count": count })))
        .collect();
    let ext = &result.extensions;
    let mut extensions = Map::new();
    if let Some((next_batch, events)) = &ext.to_device {
        extensions.insert(
            "to_device".into(),
            json!({ "next_batch": next_batch, "events": events }),
        );
    }
    if let Some(e) = &ext.e2ee {
        extensions.insert(
            "e2ee".into(),
            json!({
                "device_lists": { "changed": e.changed, "left": e.left },
                "device_one_time_keys_count": e.one_time_key_counts,
                "device_unused_fallback_key_types": e.unused_fallback_key_types,
            }),
        );
    }
    if let Some(a) = &ext.account_data {
        extensions.insert(
            "account_data".into(),
            json!({ "global": a.global, "rooms": a.rooms }),
        );
    }
    if !ext.receipts.is_empty() {
        extensions.insert("receipts".into(), json!({ "rooms": ext.receipts }));
    }
    if !ext.typing.is_empty() {
        let typing: Map<String, Value> = ext
            .typing
            .iter()
            .map(|(room, users)| {
                (
                    room.to_string(),
                    json!({ "type": "m.typing", "content": { "user_ids": users } }),
                )
            })
            .collect();
        extensions.insert("typing".into(), json!({ "rooms": typing }));
    }
    if let Some(t) = &ext.thread_subscriptions {
        extensions.insert(
            "io.element.msc4308.thread_subscriptions".into(),
            super::thread_subscriptions::changes_json(t),
        );
    }
    let mut body = json!({
        "pos": result.pos,
        "lists": lists,
        "rooms": rooms,
        "extensions": extensions,
    });
    if let Some(txn) = &b.txn_id {
        body["txn_id"] = json!(txn);
    }
    let mut resp = Json(body).into_response();
    crate::extract::add_cors(resp.headers_mut());
    Ok(resp)
}
