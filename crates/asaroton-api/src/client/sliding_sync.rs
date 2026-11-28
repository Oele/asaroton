//! `POST /_matrix/client/unstable/org.matrix.simplified_msc3575/sync` (Simplified Sliding
//! Sync, MSC4186). The response is built by hand so every field is present.

use std::time::Duration;

use asaroton_service::sliding_sync::{
    ExtensionScope, ExtensionsRequest, ListFilters, ListRequest, ProfilesRequest, RoomResult,
    RoomSubscriptionRequest, ScopeRoom, SlidingSyncRequest, StickyRequest, ToDeviceRequest,
};
use axum::{Json, response::IntoResponse};
use ruma::api::client::sync::sync_events::msc4186;
use serde_json::{Map, Value, json};

use crate::{AppState, Result, Ruma, client::rooms::client_event};

/// Where sliding sync is served. The router needs to know it apart from the other requests
/// that are not a GET: this one waits for news and ends when its client hangs up.
pub(crate) const PATH: &str = "/_matrix/client/unstable/org.matrix.simplified_msc3575/sync";

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

/// The names the sticky events extension (MSC4480) goes by: the unstable one matrix-js-sdk
/// uses, and the one the proposal will have once stable. The answer uses the name asked by.
const STICKY_EXTENSIONS: [&str; 2] = ["org.matrix.msc4354.sticky_events", "sticky_events"];

/// The sticky events extension from the raw body, and the name it was asked by. Both shapes
/// are read: MSC4480's (`enabled`, `limit`, `lists`, `rooms`) and the older one matrix-js-sdk
/// still sends, whose `since` is ignored (the connection's `pos` says what was delivered).
fn sticky_request(raw: Option<&Value>) -> Option<(StickyRequest, &'static str)> {
    let extensions = raw?.get("extensions")?;
    let (name, x) = STICKY_EXTENSIONS
        .iter()
        .find_map(|n| extensions.get(*n).map(|x| (*n, x)))?;
    if x.get("enabled").and_then(Value::as_bool) != Some(true) {
        return None;
    }
    let limit = x
        .get("limit")
        .and_then(Value::as_u64)
        .and_then(|l| usize::try_from(l).ok())
        .unwrap_or(100);
    Some((
        StickyRequest {
            scope: scope_from_raw(x),
            limit,
        },
        name,
    ))
}

/// An extension's `lists` and `rooms` from its raw JSON (`*` among `rooms` stands for the
/// subscribed rooms).
fn scope_from_raw(x: &Value) -> ExtensionScope {
    let strings = |key: &str| {
        x.get(key).and_then(Value::as_array).map(|a| {
            a.iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect::<Vec<_>>()
        })
    };
    let rooms = strings("rooms").map(|rooms| {
        rooms
            .into_iter()
            .filter_map(|r| {
                if r == "*" {
                    Some(ScopeRoom::AllSubscribed)
                } else {
                    ruma::OwnedRoomId::try_from(r).ok().map(ScopeRoom::Room)
                }
            })
            .collect()
    });
    ExtensionScope {
        lists: strings("lists"),
        rooms,
    }
}

/// The profiles extension (MSC4262) under its unstable name, read from the raw body: the ruma
/// request type predates it. `lists` and `rooms` scope it like the other extensions (`*` for
/// the subscribed rooms); `fields` names the profile fields, all of them when absent.
fn profiles_request(raw: Option<&Value>) -> Option<ProfilesRequest> {
    let p = raw?.get("extensions")?.get("org.matrix.msc4262.profiles")?;
    if p.get("enabled").and_then(Value::as_bool) != Some(true) {
        return None;
    }
    let fields = p.get("fields").and_then(Value::as_array).map(|a| {
        a.iter()
            .filter_map(Value::as_str)
            .map(str::to_owned)
            .collect::<Vec<_>>()
    });
    Some(ProfilesRequest {
        scope: scope_from_raw(p),
        fields,
    })
}

fn scope(
    lists: Option<&Vec<String>>,
    rooms: Option<&Vec<msc4186::request::ExtensionRoomConfig>>,
) -> ExtensionScope {
    ExtensionScope {
        lists: lists.cloned(),
        rooms: rooms.map(|r| {
            r.iter()
                .map(|e| match e {
                    msc4186::request::ExtensionRoomConfig::Room(id) => ScopeRoom::Room(id.clone()),
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
        if let Some(delay_id) = r.delay_ids.get(e.pdu.event_id())
            && let Some(u) = ev.get_mut("unsigned").and_then(Value::as_object_mut)
        {
            u.insert("delay_id".into(), json!(delay_id));
        }
    }
    let required_state: Vec<Value> = r.required_state.iter().map(client_event).collect();
    v.insert("required_state".into(), Value::Array(required_state));
    v.insert("timeline".into(), Value::Array(timeline));
    if let Some(pb) = &r.prev_batch {
        v.insert("prev_batch".into(), json!(pb));
    }
    v.insert("limited".into(), json!(r.limited));
    if r.expanded_timeline {
        v.insert("expanded_timeline".into(), json!(true));
    }
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
                // MSC4186 names these as profiles do. (`name` and `avatar` were the old
                // sliding sync proxy's names; clients built on the Rust SDK do not read them
                // and then call a direct chat "2 people".)
                let mut o = json!({ "user_id": h.user_id });
                if let Some(n) = &h.name {
                    o["displayname"] = json!(n);
                }
                if let Some(a) = &h.avatar {
                    o["avatar_url"] = json!(a);
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
    req: Ruma<msc4186::Request>,
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
    let sticky = sticky_request(req.json.as_ref());
    let extensions = ExtensionsRequest {
        to_device: x.to_device.enabled.then(|| ToDeviceRequest {
            since: x.to_device.since.clone(),
            limit: x.to_device.limit.map_or(0, limit),
        }),
        e2ee: x.e2ee.enabled,
        account_data: (x.account_data.enabled == Some(true))
            .then(|| scope(x.account_data.lists.as_ref(), x.account_data.rooms.as_ref())),
        receipts: (x.receipts.enabled == Some(true))
            .then(|| scope(x.receipts.lists.as_ref(), x.receipts.rooms.as_ref())),
        typing: (x.typing.enabled == Some(true))
            .then(|| scope(x.typing.lists.as_ref(), x.typing.rooms.as_ref())),
        thread_subscriptions: x
            .thread_subscriptions
            .enabled
            .then(|| x.thread_subscriptions.limit.map_or(100, limit)),
        profiles: profiles_request(req.json.as_ref()),
        sticky: sticky.as_ref().map(|(s, _)| s.clone()),
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
    if let Some((_, name)) = &sticky
        && !ext.sticky.is_empty()
    {
        let rooms: Map<String, Value> = ext
            .sticky
            .iter()
            .map(|(room, events)| {
                (
                    room.to_string(),
                    json!({ "events": events.iter().map(client_event).collect::<Vec<_>>() }),
                )
            })
            .collect();
        extensions.insert((*name).into(), json!({ "rooms": rooms }));
    }
    if !ext.profiles.is_empty() {
        let users: Map<String, Value> = ext
            .profiles
            .iter()
            .map(|(who, delta)| {
                let entry = delta.as_ref().map_or(
                    Value::Null,
                    |d| json!({ "updated": d.updated, "removed": d.removed }),
                );
                (who.to_string(), entry)
            })
            .collect();
        extensions.insert(
            "org.matrix.msc4262.profiles".into(),
            json!({ "users": users }),
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
