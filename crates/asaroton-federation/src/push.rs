//! Push gateway delivery: events that notify a user are sent to the user's HTTP pushers.
//!
//! One worker reads the room events in order and works out who is to be notified of what.
//! It does not wait for gateways: each pusher has a queue of its own, drained by a task that
//! runs while the queue has something in it, so a slow or dead gateway (three attempts over
//! seven seconds) delays only that pusher's notifications, and each pusher still gets its
//! notifications in the order of the events.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    hash::Hash,
    sync::Arc,
    time::{Duration, Instant},
};

use asaroton_service::{Notification, Result as ServiceResult, Services};
use asaroton_store::{Pusher, StoredEvent};
use ruma::{OwnedUserId, RoomId, events::StateEventType};
use serde_json::{Value, json};

use crate::outbound::Outbound;

pub fn start(services: Arc<Services>, outbound: Outbound) {
    tokio::spawn(async move { worker(services, outbound).await });
}

/// How many notifications wait for one pusher at most. When a gateway falls this far behind
/// (it is down: each notification then takes seven seconds of attempts), the oldest go: a
/// notification is only worth something while it is new.
const MOST_QUEUED_PER_PUSHER: usize = 100;

/// Queues of work per key, each drained by one task at a time.
struct PerKeyQueues<K, J> {
    queues: parking_lot::Mutex<HashMap<K, VecDeque<J>>>,
    cap: usize,
}

impl<K: Eq + Hash + Clone, J> PerKeyQueues<K, J> {
    fn new(cap: usize) -> Self {
        Self {
            queues: parking_lot::Mutex::new(HashMap::new()),
            cap,
        }
    }

    /// Queues a job; true when no task is draining this key's queue, so the caller starts
    /// one. Past the cap the oldest waiting job is dropped (the second value says so).
    fn push(&self, key: &K, job: J) -> (bool, bool) {
        let mut queues = self.queues.lock();
        match queues.get_mut(key) {
            Some(queue) => {
                queue.push_back(job);
                let dropped = queue.len() > self.cap;
                if dropped {
                    queue.pop_front();
                }
                (false, dropped)
            }
            None => {
                queues.insert(key.clone(), VecDeque::from([job]));
                (true, false)
            }
        }
    }

    /// The next job for a key's draining task. The key stays known (so a push meanwhile
    /// joins the queue instead of starting a second task) until the task asks again and
    /// finds the queue empty: then the key is forgotten and `None` tells the task to end.
    fn next(&self, key: &K) -> Option<J> {
        let mut queues = self.queues.lock();
        let job = queues.get_mut(key).and_then(VecDeque::pop_front);
        if job.is_none() {
            queues.remove(key);
        }
        job
    }
}

/// A pusher, as its queue knows it.
type PusherKey = (OwnedUserId, String, String);

/// What a pusher's queue holds: the pusher as it was when the notification was made, and the
/// notification.
type PushJob = (Arc<Pusher>, Arc<Value>);

async fn worker(services: Arc<Services>, outbound: Outbound) {
    let queues: Arc<PerKeyQueues<PusherKey, PushJob>> =
        Arc::new(PerKeyQueues::new(MOST_QUEUED_PER_PUSHER));
    let mut rx = services.notifier.subscribe();
    // Most users have no pusher; the set of those who do is kept here rather than queried
    // per member per event. It is read again when a pusher is set or removed, and every few
    // seconds for changes that do not announce themselves (a logout drops its pushers).
    let mut with_pushers: HashSet<OwnedUserId> = HashSet::new();
    let mut refreshed: Option<Instant> = None;
    let mut generation = 0;
    loop {
        let n = match rx.recv().await {
            Ok(n) => n,
            Err(tokio::sync::broadcast::error::RecvError::Lagged(lost)) => {
                tracing::warn!(
                    lost,
                    "the push worker fell behind: notifications of these events are not pushed"
                );
                continue;
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
        };
        let Notification::RoomEvent {
            room_id,
            stream_ordering,
        } = &*n
        else {
            continue;
        };
        let current = services
            .pushers_generation
            .load(std::sync::atomic::Ordering::Relaxed);
        if current != generation || refreshed.is_none_or(|t| t.elapsed() > Duration::from_secs(10))
        {
            generation = current;
            match services.store.users_with_pushers().await {
                Ok(users) => with_pushers = users.into_iter().collect(),
                Err(e) => tracing::warn!(error = %e, "could not list users with pushers"),
            }
            refreshed = Some(Instant::now());
        }
        if with_pushers.is_empty() {
            continue;
        }
        let enqueue = |pusher: Pusher, notification: Arc<Value>| {
            let key: PusherKey = (
                pusher.user_id.clone(),
                pusher.app_id.clone(),
                pusher.pushkey.clone(),
            );
            let (start, dropped) = queues.push(&key, (Arc::new(pusher), notification));
            if dropped {
                tracing::warn!(user = %key.0, app = %key.1, "push gateway too far behind: its oldest notification was dropped");
            }
            if start {
                let (services, outbound, queues) =
                    (Arc::clone(&services), outbound.clone(), Arc::clone(&queues));
                tokio::spawn(async move {
                    while let Some((pusher, notification)) = queues.next(&key) {
                        deliver(&services, &outbound, &pusher, &notification).await;
                    }
                });
            }
        };
        if let Err(e) =
            handle_event(&services, room_id, *stream_ordering, &with_pushers, enqueue).await
        {
            tracing::warn!(%room_id, error = %e, "push delivery failed");
        }
    }
}

/// Works out who of the room's members is to be notified of the event, and hands each of
/// their HTTP pushers the notification (`enqueue`, which does not wait for the gateway).
async fn handle_event(
    services: &Services,
    room_id: &RoomId,
    stream_ordering: i64,
    with_pushers: &HashSet<OwnedUserId>,
    enqueue: impl Fn(Pusher, Arc<Value>),
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
        // Nobody is woken for an event of someone they ignore (as Synapse does; tracker
        // issue 68). State events are not exempt here: they do not push anyway unless a rule
        // says so, and a member event of an ignored user is no reason to.
        if services
            .ignored_users(&m.user_id)
            .await?
            .contains(ev.pdu.sender())
        {
            continue;
        }
        let (ruleset, ctx) = services.push_rule_context(room_id, &m.user_id).await?;
        let Some(highlight) = services.push_actions_for(&ruleset, &ctx, &ev).await else {
            continue;
        };
        let unread = unread_total(services, &m.user_id).await;
        let notification = Arc::new(
            build_notification(services, room_id, &ev, &m.user_id, highlight, unread).await?,
        );
        for pusher in pushers {
            enqueue(pusher, Arc::clone(&notification));
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
async fn deliver(services: &Services, outbound: &Outbound, pusher: &Pusher, notification: &Value) {
    let Some(url) = pusher.data.get("url").and_then(Value::as_str) else {
        return;
    };
    // The gateway URL is the user's choice: it may only lead to a public address (or one
    // the operator exempted).
    let target = match outbound.parse(url) {
        Ok(target) => target,
        Err(e) => {
            tracing::warn!(user = %pusher.user_id, app = %pusher.app_id, url, error = %e, "not pushing to this gateway");
            return;
        }
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
    let mut delay = Duration::from_secs(1);
    for attempt in 0..3 {
        match outbound
            .request(reqwest::Method::POST, target.clone())
            .json(&body)
            .send()
            .await
        {
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

#[cfg(test)]
mod tests {
    use super::PerKeyQueues;

    #[test]
    fn a_queue_per_key_drained_in_order_by_one_task() {
        let q: PerKeyQueues<&str, u32> = PerKeyQueues::new(2);
        // the first job of a key starts a task; more jobs join its queue
        assert_eq!(q.push(&"a", 1), (true, false));
        assert_eq!(q.push(&"a", 2), (false, false));
        assert_eq!(
            q.push(&"b", 10),
            (true, false),
            "another key has a task of its own"
        );
        // the task takes the jobs in order; while it works on the last one, the key stays
        // known and a new job joins the queue rather than starting a second task
        assert_eq!(q.next(&"a"), Some(1));
        assert_eq!(q.next(&"a"), Some(2));
        assert_eq!(q.push(&"a", 3), (false, false));
        assert_eq!(q.next(&"a"), Some(3));
        // empty: the task ends and the key is forgotten, the next job starts a new task
        assert_eq!(q.next(&"a"), None);
        assert_eq!(q.push(&"a", 4), (true, false));
        // past the cap the oldest waiting job goes, not the one being worked on
        assert_eq!(q.next(&"b"), Some(10));
        assert_eq!(q.push(&"b", 11), (false, false));
        assert_eq!(q.push(&"b", 12), (false, false));
        assert_eq!(q.push(&"b", 13), (false, true));
        assert_eq!(q.next(&"b"), Some(12));
        assert_eq!(q.next(&"b"), Some(13));
        assert_eq!(q.next(&"b"), None);
    }
}
