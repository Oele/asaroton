//! Delayed events (MSC4140): events scheduled to be sent after a timeout unless they are
//! cancelled, restarted or sent early. Element Call uses them to expire call membership state
//! when a client drops off.

use std::{collections::HashSet, sync::Arc, time::Duration};

use asaroton_store::{DelayedEvent, now_ts};
use ruma::{DeviceId, OwnedEventId, RoomId, UserId};
use serde_json::{Value, json};

use crate::{Error, Result, Services};

/// The longest delay accepted: 24 hours.
pub const MAX_DELAY_MS: i64 = 24 * 60 * 60 * 1000;
/// How many delayed events one user may have scheduled at once. A call needs one or two per
/// device; this leaves ample room and still bounds what one account can queue.
pub const MAX_SCHEDULED: usize = 100;
/// How long sent and cancelled delayed events are kept for `GET /delayed_events/{delayId}`
/// (the spec suggests 7 days), and at most how many per user (it suggests 1000).
const KEEP_FINALISED_MS: i64 = 7 * 24 * 60 * 60 * 1000;
const KEEP_FINALISED_PER_USER: i64 = 1000;
/// How often the worker prunes them.
const PRUNE_EVERY: Duration = Duration::from_hours(1);

/// Which API a request came through: the merged spec's stable endpoints (v1.20) answer some
/// cases differently from the proposal's unstable ones, which keep their old behaviour.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DelayedApi {
    Unstable,
    Stable,
}

/// Scheduler state shared between the API and the worker.
///
/// A delayed state event is deliberately *not* cancelled when a state event with the same key
/// is sent in the meantime: Element Call schedules its delayed leave first and sends its join
/// membership for the same key right after, and the merged proposal (like Synapse) keeps the
/// delayed event scheduled.
#[derive(Debug, Default)]
pub struct DelayedEvents {
    /// Woken whenever the schedule changes so the worker recomputes its next deadline.
    pub(crate) wake: tokio::sync::Notify,
    /// The delayed events being sent this moment. One is marked as finalised before it is
    /// sent (so that it is sent once) and gets its outcome after; in between it has neither
    /// an event nor an error, like a cancelled one, and is shown as still scheduled.
    sending: parking_lot::Mutex<HashSet<String>>,
}

/// A delayed event in [`DelayedEvents::sending`] for as long as this lives.
struct Sending<'a>(&'a DelayedEvents, String);

impl<'a> Sending<'a> {
    fn start(delayed: &'a DelayedEvents, delay_id: &str) -> Self {
        delayed.sending.lock().insert(delay_id.to_owned());
        Self(delayed, delay_id.to_owned())
    }
}

impl Drop for Sending<'_> {
    fn drop(&mut self) {
        self.0.sending.lock().remove(&self.1);
    }
}

/// What `POST /delayed_events/{delay_id}` may do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpdateAction {
    Send,
    Cancel,
    Restart,
}

impl std::str::FromStr for UpdateAction {
    type Err = ();
    fn from_str(s: &str) -> std::result::Result<Self, ()> {
        match s {
            "send" => Ok(Self::Send),
            "cancel" => Ok(Self::Cancel),
            "restart" => Ok(Self::Restart),
            _ => Err(()),
        }
    }
}

impl Services {
    /// Schedules an event; returns its delay ID (the same one for a repeated transaction ID).
    #[allow(clippy::too_many_arguments)]
    pub async fn schedule_delayed_event(
        &self,
        user_id: &UserId,
        device_id: Option<&DeviceId>,
        room_id: &RoomId,
        event_type: &str,
        state_key: Option<String>,
        content: Value,
        delay_ms: i64,
        txn_id: Option<&str>,
        api: DelayedApi,
        sticky: Option<asaroton_core::sticky::Sticky>,
    ) -> Result<String> {
        if sticky.is_some() && state_key.is_some() {
            return Err(Error::invalid_param("A state event cannot be sticky."));
        }
        if delay_ms <= 0 {
            return Err(Error::invalid_param(
                "The delay must be a positive number of milliseconds.",
            ));
        }
        if delay_ms > MAX_DELAY_MS {
            // The proposal's own error code, under its unstable name while it is unstable.
            // Clients take it as the cue to read the limit from the capabilities and retry.
            return Err(Error::custom(
                match api {
                    DelayedApi::Unstable => "ORG.MATRIX.MSC4140_DELAY_TOO_LARGE",
                    DelayedApi::Stable => "M_DELAY_TOO_LARGE",
                },
                http::StatusCode::BAD_REQUEST,
                format!("The delay must not exceed {MAX_DELAY_MS} ms."),
            ));
        }
        if !content.is_object() {
            return Err(Error::bad_json("Event content must be an object."));
        }
        // A second attempt with the same transaction ID waits until the first has scheduled
        // its event, and then gets that one.
        let _turn = match txn_id {
            Some(txn) => Some(
                self.txn_turn(user_id, device_id, &format!("delayed:{txn}"))
                    .await,
            ),
            None => None,
        };
        if let Some(txn) = txn_id
            && let Some(existing) = self
                .store
                .delayed_event_by_txn(user_id, device_id, txn)
                .await?
        {
            return Ok(existing.delay_id);
        }
        self.require_joined(room_id, user_id).await?;
        let scheduled = self.store.delayed_events_for_user(user_id).await?;
        if scheduled.len() >= MAX_SCHEDULED {
            // Room frees up when the next of the user's events is sent.
            let next_due = scheduled
                .iter()
                .map(|e| e.running_since.saturating_add(e.delay_ms))
                .min()
                .unwrap_or_else(now_ts);
            let wait_ms = u64::try_from(next_due - now_ts()).unwrap_or(0);
            let mut limit = ruma::api::error::LimitExceededErrorData::new();
            limit.retry_after = Some(ruma::api::error::RetryAfter::Delay(Duration::from_secs(
                wait_ms.div_ceil(1000).max(1),
            )));
            return Err(Error::matrix(
                ruma::api::error::ErrorKind::LimitExceeded(limit),
                http::StatusCode::TOO_MANY_REQUESTS,
                "The maximum number of delayed events has been reached.",
            ));
        }
        let ev = DelayedEvent {
            delay_id: asaroton_auth::generate_session_id(),
            user_id: user_id.to_owned(),
            device_id: device_id.map(ToOwned::to_owned),
            room_id: room_id.to_owned(),
            event_type: event_type.to_owned(),
            state_key,
            content,
            delay_ms,
            running_since: now_ts(),
            txn_id: txn_id.map(ToOwned::to_owned),
            finalised: None,
            sticky,
        };
        self.store.add_delayed_event(&ev).await?;
        self.delayed.wake.notify_one();
        Ok(ev.delay_id)
    }

    /// The user's pending delayed events as the API shows them.
    pub async fn delayed_events_for(&self, user_id: &UserId) -> Result<Vec<Value>> {
        let evs = self.store.delayed_events_for_user(user_id).await?;
        Ok(evs
            .into_iter()
            .map(|ev| {
                let mut v = json!({
                    "delay_id": ev.delay_id,
                    "room_id": ev.room_id,
                    "type": ev.event_type,
                    "delay": ev.delay_ms,
                    "running_since": ev.running_since,
                    "content": ev.content,
                });
                if let Some(k) = ev.state_key {
                    v["state_key"] = Value::String(k);
                }
                v
            })
            .collect())
    }

    /// One of the user's delayed events as `GET /v1/delayed_events/{delayId}` shows it,
    /// pending or kept after it was sent or cancelled.
    pub async fn delayed_event_view(&self, user_id: &UserId, delay_id: &str) -> Result<Value> {
        let ev = self
            .store
            .get_delayed_event_any(delay_id)
            .await?
            .filter(|ev| ev.user_id == user_id)
            .ok_or_else(|| Error::not_found("No such delayed event."))?;
        let mut v = json!({
            "delay_id": ev.delay_id,
            "room_id": ev.room_id,
            "type": ev.event_type,
            "delay_ms": ev.delay_ms,
            "delayed_since_ts": ev.running_since,
            "content": ev.content,
        });
        if let Some(k) = ev.state_key {
            v["state_key"] = Value::String(k);
        }
        if let Some(f) = ev.finalised.filter(|f| {
            f.event_id.is_some()
                || f.error.is_some()
                || !self.delayed.sending.lock().contains(delay_id)
        }) {
            let mut fin = json!({ "finalised_ts": f.ts });
            if let Some(id) = f.event_id {
                fin["event_id"] = Value::String(id);
            }
            if let Some(err) = f.error {
                fin["error"] = err;
            }
            v["finalised"] = fin;
        }
        Ok(v)
    }

    /// Cancels, restarts or sends one of the user's delayed events.
    ///
    /// Through the unstable API an event that was already sent or cancelled is gone (404), as
    /// it always was. Through the stable one it is kept for a while: sending a sent one or
    /// cancelling a cancelled one succeeds, the opposite or a restart is a 409. A send that
    /// fails answers with the error of sending the event and leaves it scheduled.
    pub async fn update_delayed_event(
        &self,
        user_id: &UserId,
        delay_id: &str,
        action: UpdateAction,
        api: DelayedApi,
    ) -> Result<()> {
        // A second round only when another request or the worker finalised the event between
        // reading it and acting on it.
        for _ in 0..2 {
            let ev = self
                .store
                .get_delayed_event_any(delay_id)
                .await?
                .filter(|ev| ev.user_id == user_id)
                .ok_or_else(|| Error::not_found("No such delayed event."))?;
            if let Some(fin) = &ev.finalised {
                if api == DelayedApi::Unstable {
                    return Err(Error::not_found("No such delayed event."));
                }
                let conflict = |msg: &str| {
                    Error::custom("M_UNKNOWN", http::StatusCode::CONFLICT, msg.to_owned())
                };
                return match (action, fin.event_id.is_some()) {
                    (UpdateAction::Send, true) | (UpdateAction::Cancel, false) => Ok(()),
                    (UpdateAction::Send, false) => Err(conflict(
                        "The delayed event was cancelled and cannot be sent.",
                    )),
                    (UpdateAction::Cancel, true) => Err(conflict(
                        "The delayed event was sent and cannot be cancelled.",
                    )),
                    (UpdateAction::Restart, _) => Err(conflict(
                        "The delayed event was sent or cancelled and cannot be restarted.",
                    )),
                };
            }
            let done = match action {
                UpdateAction::Cancel => {
                    self.store
                        .finalise_delayed_event(delay_id, now_ts())
                        .await?
                }
                UpdateAction::Restart => {
                    self.store.restart_delayed_event(delay_id, now_ts()).await?
                }
                UpdateAction::Send => {
                    let _sending = Sending::start(&self.delayed, delay_id);
                    if self
                        .store
                        .finalise_delayed_event(delay_id, now_ts())
                        .await?
                    {
                        match self.fire_delayed(ev).await {
                            Ok(event_id) => {
                                self.store
                                    .record_delayed_event_outcome(delay_id, Some(&event_id), None)
                                    .await?;
                            }
                            Err(e) => {
                                self.store.unfinalise_delayed_event(delay_id).await?;
                                self.delayed.wake.notify_one();
                                return Err(e);
                            }
                        }
                        true
                    } else {
                        false
                    }
                }
            };
            if done {
                self.delayed.wake.notify_one();
                return Ok(());
            }
        }
        Err(Error::not_found("No such delayed event."))
    }

    /// Sends a due event (the worker): records the event it became, or cancels it with the
    /// error that kept it from being sent, as the spec requires.
    async fn send_due(&self, ev: DelayedEvent) -> Result<()> {
        let delay_id = ev.delay_id.clone();
        let _sending = Sending::start(&self.delayed, &delay_id);
        if !self
            .store
            .finalise_delayed_event(&delay_id, now_ts())
            .await?
        {
            return Ok(());
        }
        match self.fire_delayed(ev).await {
            Ok(event_id) => {
                self.store
                    .record_delayed_event_outcome(&delay_id, Some(&event_id), None)
                    .await?;
            }
            Err(e) => {
                tracing::debug!(delay_id, error = %e, "delayed event not sent: cancelled");
                self.store
                    .record_delayed_event_outcome(&delay_id, None, Some(&e.to_error_json()))
                    .await?;
            }
        }
        Ok(())
    }

    async fn fire_delayed(&self, ev: DelayedEvent) -> Result<OwnedEventId> {
        // An account suspended after it scheduled this (Matrix v1.13) sends only what ends
        // something: an emptied event (how a call's membership ends) or its own leave. Anything
        // else would reach the room as if the suspension were not there.
        if self
            .store
            .get_user(&ev.user_id)
            .await?
            .is_some_and(|u| u.is_suspended)
            && !ends_something(&ev)
        {
            return Err(Error::matrix(
                ruma::api::error::ErrorKind::UserSuspended,
                http::StatusCode::FORBIDDEN,
                "You cannot perform this action while suspended.",
            ));
        }
        match ev.state_key {
            Some(key) => {
                self.send_state(
                    &ev.room_id,
                    &ev.user_id,
                    &ev.event_type,
                    &key,
                    ev.content,
                    None,
                )
                .await
            }
            None => {
                self.send_message(
                    &ev.room_id,
                    &ev.user_id,
                    ev.device_id.as_deref(),
                    &format!("delayed:{}", ev.delay_id),
                    &ev.event_type,
                    ev.content,
                    None,
                    ev.sticky,
                )
                .await
            }
        }
    }
}

/// Whether a delayed event only ends something: empty content (a call membership's delayed
/// leave) or its sender's own membership set to `leave`. The one kind a suspended account
/// still sends.
fn ends_something(ev: &DelayedEvent) -> bool {
    let empty = ev
        .content
        .as_object()
        .is_some_and(serde_json::Map::is_empty);
    let own_leave = ev.event_type == "m.room.member"
        && ev.state_key.as_deref() == Some(ev.user_id.as_str())
        && ev.content.get("membership").and_then(Value::as_str) == Some("leave");
    empty || own_leave
}

/// Starts the worker that sends delayed events when their timeout passes, and prunes the
/// sent and cancelled ones it keeps.
pub fn start_worker(services: Arc<Services>) {
    tokio::spawn(async move {
        let mut last_prune: Option<tokio::time::Instant> = None;
        loop {
            match services.store.due_delayed_events(now_ts()).await {
                Ok(due) => {
                    for ev in due {
                        let delay_id = ev.delay_id.clone();
                        if let Err(e) = services.send_due(ev).await {
                            tracing::warn!(delay_id, error = %e, "sending a due delayed event failed");
                        }
                    }
                }
                Err(e) => tracing::warn!(error = %e, "loading due delayed events failed"),
            }
            if last_prune.is_none_or(|t| t.elapsed() >= PRUNE_EVERY) {
                last_prune = Some(tokio::time::Instant::now());
                match services
                    .store
                    .prune_finalised_delayed_events(
                        now_ts() - KEEP_FINALISED_MS,
                        KEEP_FINALISED_PER_USER,
                    )
                    .await
                {
                    Ok(0) => {}
                    Ok(n) => tracing::debug!(n, "pruned sent and cancelled delayed events"),
                    Err(e) => tracing::warn!(error = %e, "pruning delayed events failed"),
                }
            }
            let next = services.store.next_delayed_event_due().await.ok().flatten();
            let wait = next.map_or(Duration::from_secs(60), |due| {
                Duration::from_millis(u64::try_from(due - now_ts()).unwrap_or(0))
            });
            tokio::select! {
                () = tokio::time::sleep(wait.min(Duration::from_secs(60))) => {}
                () = services.delayed.wake.notified() => {}
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use asaroton_store::DelayedEvent;
    use ruma::{owned_room_id, owned_user_id};
    use serde_json::json;

    fn delayed(
        event_type: &str,
        state_key: Option<&str>,
        content: serde_json::Value,
    ) -> DelayedEvent {
        DelayedEvent {
            delay_id: "d".into(),
            user_id: owned_user_id!("@alice:example.org"),
            device_id: None,
            room_id: owned_room_id!("!room:example.org"),
            event_type: event_type.into(),
            state_key: state_key.map(str::to_owned),
            content,
            delay_ms: 1000,
            running_since: 0,
            txn_id: None,
            finalised: None,
            sticky: None,
        }
    }

    #[test]
    fn a_suspended_account_still_ends_its_call_and_leaves() {
        let ends = |ev: &DelayedEvent| super::ends_something(ev);
        assert!(ends(&delayed(
            "org.matrix.msc3401.call.member",
            Some("_@alice:example.org_DEV"),
            json!({})
        )));
        assert!(ends(&delayed("m.rtc.member", None, json!({}))));
        assert!(ends(&delayed(
            "m.room.member",
            Some("@alice:example.org"),
            json!({"membership": "leave"})
        )));
        assert!(!ends(&delayed(
            "m.room.message",
            None,
            json!({"msgtype": "m.text", "body": "hi"})
        )));
        assert!(!ends(&delayed(
            "m.room.topic",
            Some(""),
            json!({"topic": "spam"})
        )));
        assert!(!ends(&delayed(
            "m.room.member",
            Some("@bob:example.org"),
            json!({"membership": "leave"})
        )));
        assert!(!ends(&delayed(
            "m.room.member",
            Some("@alice:example.org"),
            json!({"membership": "join"})
        )));
    }
}
