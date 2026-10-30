//! Delayed events (MSC4140): events scheduled to be sent after a timeout unless they are
//! cancelled, restarted or sent early. Element Call uses them to expire call membership state
//! when a client drops off.

use std::{sync::Arc, time::Duration};

use ruma::{DeviceId, OwnedEventId, RoomId, UserId};
use serde_json::{Value, json};
use tessera_store::{DelayedEvent, now_ts};

use crate::{Error, Result, Services};

/// The longest delay accepted: 24 hours.
pub const MAX_DELAY_MS: i64 = 24 * 60 * 60 * 1000;

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
    ) -> Result<String> {
        if delay_ms <= 0 {
            return Err(Error::invalid_param(
                "The delay must be a positive number of milliseconds.",
            ));
        }
        if delay_ms > MAX_DELAY_MS {
            return Err(Error::invalid_param(format!(
                "The delay must not exceed {MAX_DELAY_MS} ms."
            )));
        }
        if !content.is_object() {
            return Err(Error::bad_json("Event content must be an object."));
        }
        if let Some(txn) = txn_id
            && let Some(existing) = self
                .store
                .delayed_event_by_txn(user_id, device_id, txn)
                .await?
        {
            return Ok(existing.delay_id);
        }
        self.require_joined(room_id, user_id).await?;
        let ev = DelayedEvent {
            delay_id: tessera_auth::generate_session_id(),
            user_id: user_id.to_owned(),
            device_id: device_id.map(ToOwned::to_owned),
            room_id: room_id.to_owned(),
            event_type: event_type.to_owned(),
            state_key,
            content,
            delay_ms,
            running_since: now_ts(),
            txn_id: txn_id.map(ToOwned::to_owned),
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

    /// Cancels, restarts or sends one of the user's delayed events.
    pub async fn update_delayed_event(
        &self,
        user_id: &UserId,
        delay_id: &str,
        action: UpdateAction,
    ) -> Result<()> {
        let ev = self
            .store
            .get_delayed_event(delay_id)
            .await?
            .filter(|ev| ev.user_id == user_id)
            .ok_or_else(|| Error::not_found("No such delayed event."))?;
        match action {
            UpdateAction::Cancel => {
                self.remove_delayed(&ev).await?;
            }
            UpdateAction::Restart => {
                self.store.restart_delayed_event(delay_id, now_ts()).await?;
            }
            UpdateAction::Send => {
                if self.remove_delayed(&ev).await? {
                    self.fire_delayed(ev).await?;
                }
            }
        }
        self.delayed.wake.notify_one();
        Ok(())
    }

    async fn remove_delayed(&self, ev: &DelayedEvent) -> Result<bool> {
        Ok(self.store.remove_delayed_event(&ev.delay_id).await?)
    }

    async fn fire_delayed(&self, ev: DelayedEvent) -> Result<OwnedEventId> {
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
                )
                .await
            }
        }
    }
}

/// Starts the worker that sends delayed events when their timeout passes.
pub fn start_worker(services: Arc<Services>) {
    tokio::spawn(async move {
        loop {
            match services.store.due_delayed_events(now_ts()).await {
                Ok(due) => {
                    for ev in due {
                        let delay_id = ev.delay_id.clone();
                        match services.remove_delayed(&ev).await {
                            Ok(true) => {
                                if let Err(e) = services.fire_delayed(ev).await {
                                    tracing::debug!(delay_id, error = %e, "delayed event not sent");
                                }
                            }
                            Ok(false) => {}
                            Err(e) => {
                                tracing::warn!(delay_id, error = %e, "delayed event removal failed");
                            }
                        }
                    }
                }
                Err(e) => tracing::warn!(error = %e, "loading due delayed events failed"),
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
