//! `GET /sync` (classic). The response is assembled by hand so every field the spec
//! requires is present (ruma's types omit several when empty or false).

use std::time::Duration;

use asaroton_service::sync::{RoomSync, SyncFilter, SyncRequest};
use asaroton_store::StoredEvent;
use axum::{Json, response::IntoResponse};
use ruma::api::client::{
    filter::{FilterDefinition, LazyLoadOptions},
    sync::sync_events::v3::{self as sync_v3, Filter},
};
use serde_json::{Map, Value, json};

use crate::{AppState, Error, Result, Ruma, client::rooms::client_event};

/// The `filter` query parameter when it is a filter definition written out as JSON.
fn inline_filter(query: &str) -> Option<Value> {
    let raw = query.split('&').find_map(|kv| kv.strip_prefix("filter="))?;
    // Form encoding: `+` is a space.
    let decoded = percent_encoding::percent_decode_str(&raw.replace('+', " "))
        .decode_utf8_lossy()
        .into_owned();
    serde_json::from_str::<Value>(&decoded)
        .ok()
        .filter(Value::is_object)
}

/// The profile fields a filter asks updates for (MSC4429): `profile_fields.ids`, under its
/// stable name or the proposal's unstable one. Nothing is the default: a client has to ask.
pub fn profile_field_ids(filter: &Value) -> Vec<String> {
    let mut ids: Vec<String> = Vec::new();
    for key in ["profile_fields", "org.matrix.msc4429.profile_fields"] {
        let listed = filter
            .get(key)
            .and_then(|f| f.get("ids"))
            .and_then(Value::as_array);
        for id in listed.into_iter().flatten().filter_map(Value::as_str) {
            if !ids.iter().any(|known| known == id) {
                ids.push(id.to_owned());
            }
        }
    }
    ids
}

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
        Some(s) => Some(
            services
                .resolve_since_token(s)
                .await?
                .ok_or_else(|| Error::invalid_param("Invalid since token."))?,
        ),
    };

    // The filter as JSON as well as typed: the typed form drops what it does not know, and
    // the profile fields a client wants updates for (MSC4429) are not in it.
    let mut filter_json: Option<Value> = None;
    let filter: Option<FilterDefinition> = match &req.body.filter {
        Some(Filter::FilterDefinition(f)) => {
            filter_json = req.query.as_deref().and_then(inline_filter);
            Some(f.clone())
        }
        Some(Filter::FilterId(id)) => {
            let id: i64 = id
                .parse()
                .map_err(|_| Error::invalid_param("Invalid filter ID."))?;
            filter_json = services.store.get_filter(&sender.user_id, id).await?;
            filter_json
                .clone()
                .map(serde_json::from_value)
                .transpose()
                .map_err(|e| Error::internal(e.to_string()))?
        }
        _ => None,
    };
    let mut filter = filter.as_ref().map(sync_filter).unwrap_or_default();
    filter.profile_fields = filter_json
        .as_ref()
        .map(profile_field_ids)
        .unwrap_or_default();

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
    // Empty membership sections are left out, as clients (and Complement) treat a present
    // `invite` as "there are invites".
    let mut rooms = Map::new();
    for (key, map) in [
        ("join", join),
        ("leave", leave),
        ("invite", invite),
        ("knock", knock),
    ] {
        if !map.is_empty() {
            rooms.insert(key.to_owned(), Value::Object(map));
        }
    }
    let mut body = json!({
        "next_batch": result.next_batch.to_string(),
        "rooms": rooms,
        "presence": { "events": result.presence },
        "account_data": { "events": result.account_data },
        "to_device": { "events": result.to_device },
        "device_lists": { "changed": result.device_lists_changed, "left": result.device_lists_left },
        "device_one_time_keys_count": otk,
        "device_unused_fallback_key_types": result.unused_fallback_key_types,
        "org.matrix.msc2732.device_unused_fallback_key_types": result.unused_fallback_key_types,
    });
    // Profile updates (MSC4429), under the proposal's unstable name while it is unstable.
    if !result.profile_updates.is_empty() {
        let users: Map<String, Value> = result
            .profile_updates
            .iter()
            .map(|(user, fields)| (user.to_string(), json!({ "profile_updates": fields })))
            .collect();
        body["org.matrix.msc4429.users"] = Value::Object(users);
    }
    let mut resp = Json(body).into_response();
    crate::extract::add_cors(resp.headers_mut());
    Ok(resp)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn profile_fields_have_to_be_asked_for() {
        let none: Vec<String> = Vec::new();
        assert_eq!(profile_field_ids(&json!({})), none);
        assert_eq!(
            profile_field_ids(&json!({ "room": { "timeline": { "limit": 5 } } })),
            none
        );
        assert_eq!(profile_field_ids(&json!({ "profile_fields": {} })), none);
        assert_eq!(
            profile_field_ids(&json!({ "profile_fields": { "ids": ["m.status", "displayname"] } })),
            ["m.status", "displayname"]
        );
        // The proposal's unstable name counts too; a client may send both.
        assert_eq!(
            profile_field_ids(&json!({
                "profile_fields": { "ids": ["m.status"] },
                "org.matrix.msc4429.profile_fields": { "ids": ["m.status", "m.tz", 7] },
            })),
            ["m.status", "m.tz"]
        );
    }

    #[test]
    fn a_filter_written_out_in_the_query_is_read_as_json() {
        let query = "timeout=0&filter=%7B%22profile_fields%22%3A%7B%22ids%22%3A%5B%22m.status%22%5D%7D%7D&since=s1";
        let filter = inline_filter(query).unwrap_or_default();
        assert_eq!(profile_field_ids(&filter), ["m.status"]);
        // A filter ID is not JSON.
        assert_eq!(inline_filter("filter=12&timeout=0"), None);
        assert_eq!(inline_filter("timeout=0"), None);
    }
}
