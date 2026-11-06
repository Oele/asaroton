//! Push gateway delivery: events that notify a user are sent to the user's HTTP pushers.

use std::{
    collections::HashSet,
    sync::Arc,
    time::{Duration, Instant},
};

use asaroton_service::{Notification, Result as ServiceResult, Services};
use asaroton_store::{Pusher, StoredEvent};
use ruma::{OwnedUserId, RoomId, events::StateEventType};
use serde_json::{Value, json};

use crate::{Federation, url_preview::plain_http};

pub fn start(fed: &Arc<Federation>) {
    let fed = Arc::clone(fed);
    tokio::spawn(async move { worker(&fed).await });
}

async fn worker(fed: &Arc<Federation>) {
    let services: &Services = &fed.services;
    let mut rx = services.notifier.subscribe();
    // Most users have no pusher; the set of those who do is refreshed every few seconds
    // rather than queried per member per event.
    let mut with_pushers: HashSet<OwnedUserId> = HashSet::new();
    let mut refreshed: Option<Instant> = None;
    loop {
        let n = match rx.recv().await {
            Ok(n) => n,
            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
            Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
        };
        let Notification::RoomEvent {
            room_id,
            stream_ordering,
        } = &*n
        else {
            continue;
        };
        if refreshed.is_none_or(|t| t.elapsed() > Duration::from_secs(10)) {
            match services.store.users_with_pushers().await {
                Ok(users) => with_pushers = users.into_iter().collect(),
                Err(e) => tracing::warn!(error = %e, "could not list users with pushers"),
            }
            refreshed = Some(Instant::now());
        }
        if with_pushers.is_empty() {
            continue;
        }
        if let Err(e) = handle_event(services, room_id, *stream_ordering, &with_pushers).await {
            tracing::warn!(%room_id, error = %e, "push delivery failed");
        }
    }
}

async fn handle_event(
    services: &Services,
    room_id: &RoomId,
    stream_ordering: i64,
    with_pushers: &HashSet<OwnedUserId>,
) -> ServiceResult<()> {
    let Some(ev) = services
        .store
        .events_after(stream_ordering - 1, 1)
        .await?
        .into_iter()
        .find(|e| e.stream_ordering == stream_ordering)
    else {
        return Ok(());
    };
    for m in services.store.members(room_id, Some("join")).await? {
        if !services.is_local(&m.user_id)
            || m.user_id == ev.pdu.sender()
            || !with_pushers.contains(&m.user_id)
        {
            continue;
        }
        let pushers: Vec<Pusher> = services
            .store
            .pushers(&m.user_id)
            .await?
            .into_iter()
            .filter(|p| p.kind == "http")
            .collect();
        if pushers.is_empty() {
            continue;
        }
        let (ruleset, ctx) = services.push_rule_context(room_id, &m.user_id).await?;
        let Some(highlight) = services.push_actions_for(&ruleset, &ctx, &ev).await else {
            continue;
        };
        let unread = unread_total(services, &m.user_id).await;
        let notification =
            build_notification(services, room_id, &ev, &m.user_id, highlight, unread).await?;
        for pusher in pushers {
            deliver(services, &pusher, &notification).await;
        }
    }
    Ok(())
}

/// Unread notifications across every joined room (the badge count).
async fn unread_total(services: &Services, user: &ruma::UserId) -> u64 {
    let Ok(rooms) = services.store.rooms_for_user(user, "join").await else {
        return 0;
    };
    let Ok(upto) = services.store.max_stream_ordering().await else {
        return 0;
    };
    let mut total = 0;
    for r in rooms {
        if let Ok(c) = services.unread_counts(&r, user, upto).await {
            total += c.total.notification_count;
        }
    }
    total
}

async fn build_notification(
    services: &Services,
    room_id: &RoomId,
    ev: &StoredEvent,
    user: &ruma::UserId,
    highlight: bool,
    unread: u64,
) -> ServiceResult<Value> {
    let content: Value = serde_json::from_str(ev.pdu.content().get()).unwrap_or_default();
    let (sender_name, _) = services.member_profile(room_id, ev.pdu.sender()).await?;
    let room_name = services
        .state_content(room_id, &StateEventType::RoomName, "")
        .await?
        .and_then(|c| c.get("name").and_then(Value::as_str).map(str::to_owned));
    let room_alias = services
        .state_content(room_id, &StateEventType::RoomCanonicalAlias, "")
        .await?
        .and_then(|c| c.get("alias").and_then(Value::as_str).map(str::to_owned));
    let mut n = json!({
        "event_id": ev.pdu.event_id(),
        "room_id": room_id,
        "type": ev.pdu.event_type(),
        "sender": ev.pdu.sender(),
        "prio": if highlight { "high" } else { "low" },
        "content": content,
        "counts": { "unread": unread },
        "user_is_target": ev.pdu.state_key() == Some(user.as_str()),
    });
    if let Some(name) = sender_name {
        n["sender_display_name"] = json!(name);
    }
    if let Some(name) = room_name {
        n["room_name"] = json!(name);
    }
    if let Some(alias) = room_alias {
        n["room_alias"] = json!(alias);
    }
    if let Some(key) = ev.pdu.state_key() {
        n["state_key"] = json!(key);
    }
    n["tweaks"] = json!({ "highlight": highlight });
    Ok(n)
}

/// POST the notification to one pusher's gateway; a rejected pushkey removes the pusher.
async fn deliver(services: &Services, pusher: &Pusher, notification: &Value) {
    let Some(url) = pusher.data.get("url").and_then(Value::as_str) else {
        return;
    };
    let format = pusher.data.get("format").and_then(Value::as_str);
    let mut n = notification.clone();
    if format == Some("event_id_only")
        && let Some(o) = n.as_object_mut()
    {
        o.retain(|k, _| matches!(k.as_str(), "event_id" | "room_id" | "counts"));
    }
    let mut data = pusher.data.clone();
    if let Some(o) = data.as_object_mut() {
        o.remove("url");
        o.remove("format");
    }
    let tweaks = n
        .as_object_mut()
        .and_then(|o| o.remove("tweaks"))
        .unwrap_or_else(|| json!({}));
    n["devices"] = json!([{
        "app_id": pusher.app_id,
        "pushkey": pusher.pushkey,
        "pushkey_ts": 0,
        "data": data,
        "tweaks": tweaks,
    }]);
    let body = json!({ "notification": n });
    let client = plain_http();
    let mut delay = Duration::from_secs(1);
    for attempt in 0..3 {
        match client.post(url).json(&body).send().await {
            Ok(resp) if resp.status().is_success() => {
                let rejected: Vec<String> = resp
                    .json::<Value>()
                    .await
                    .ok()
                    .and_then(|v| v.get("rejected").cloned())
                    .and_then(|r| serde_json::from_value(r).ok())
                    .unwrap_or_default();
                if rejected.contains(&pusher.pushkey) {
                    tracing::info!(user = %pusher.user_id, app = %pusher.app_id, "push gateway rejected pushkey; removing pusher");
                    let _ = services
                        .store
                        .delete_pusher(&pusher.user_id, &pusher.app_id, &pusher.pushkey)
                        .await;
                }
                return;
            }
            Ok(resp) => {
                tracing::debug!(url, status = %resp.status(), attempt, "push gateway error");
            }
            Err(e) => {
                tracing::debug!(url, error = %e, attempt, "push gateway unreachable");
            }
        }
        tokio::time::sleep(delay).await;
        delay *= 2;
    }
}
