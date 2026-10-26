//! Thread subscriptions (MSC4306) and their change stream (MSC4308).

use std::collections::BTreeMap;

use ruma::{EventId, OwnedEventId, OwnedRoomId, RoomId, UserId};
use serde_json::Value;
use tessera_store::{StoredEvent, ThreadSubscription};

use crate::{Error, Notification, Result, Services};

pub const ERR_NOT_IN_THREAD: &str = "IO.ELEMENT.MSC4306.M_NOT_IN_THREAD";
pub const ERR_CONFLICTING_UNSUBSCRIPTION: &str = "IO.ELEMENT.MSC4306.M_CONFLICTING_UNSUBSCRIPTION";

/// A page of subscription changes, as sliding sync and `/thread_subscriptions` report them.
#[derive(Clone, Debug, Default)]
pub struct ThreadSubscriptionChanges {
    /// `room -> thread root -> (automatic, bump_stamp)`.
    pub subscribed: BTreeMap<OwnedRoomId, BTreeMap<OwnedEventId, (bool, i64)>>,
    /// `room -> thread root -> bump_stamp`.
    pub unsubscribed: BTreeMap<OwnedRoomId, BTreeMap<OwnedEventId, i64>>,
    /// Where to continue when the page was cut off by the limit.
    pub prev_batch: Option<String>,
}

impl ThreadSubscriptionChanges {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.subscribed.is_empty() && self.unsubscribed.is_empty()
    }

    fn from_rows(rows: Vec<ThreadSubscription>, has_more: bool) -> Self {
        let mut out = Self::default();
        let mut lowest = i64::MAX;
        for s in rows {
            lowest = lowest.min(s.stream_id);
            if s.subscribed {
                out.subscribed
                    .entry(s.room_id)
                    .or_default()
                    .insert(s.thread_root, (s.automatic, s.stream_id));
            } else {
                out.unsubscribed
                    .entry(s.room_id)
                    .or_default()
                    .insert(s.thread_root, s.stream_id);
            }
        }
        if has_more {
            out.prev_batch = Some(lowest.to_string());
        }
        out
    }
}

impl Services {
    async fn thread_root_in_room(&self, room_id: &RoomId, root: &EventId) -> Result<StoredEvent> {
        self.store
            .get_event(root)
            .await?
            .filter(|ev| ev.pdu.room_id() == room_id && !ev.pdu.rejected())
            .ok_or_else(|| Error::not_found("Thread root not found."))
    }

    /// Subscribes the user to a thread, manually or because of `automatic` (an event in the
    /// thread, such as a mention) which may not override a later unsubscription.
    pub async fn subscribe_thread(
        &self,
        user_id: &UserId,
        room_id: &RoomId,
        root: &EventId,
        automatic: Option<&EventId>,
    ) -> Result<()> {
        self.require_joined(room_id, user_id).await?;
        self.thread_root_in_room(room_id, root).await?;
        let existing = self
            .store
            .get_thread_subscription(user_id, room_id, root)
            .await?;
        let automatic_flag = match automatic {
            None => false,
            Some(cause) => {
                let in_thread = self
                    .store
                    .get_event(cause)
                    .await?
                    .filter(|ev| ev.pdu.room_id() == room_id)
                    .filter(|ev| {
                        serde_json::from_str::<Value>(ev.pdu.content().get()).is_ok_and(|c| {
                            let rel = c.get("m.relates_to");
                            rel.and_then(|r| r.get("rel_type")).and_then(Value::as_str)
                                == Some("m.thread")
                                && rel.and_then(|r| r.get("event_id")).and_then(Value::as_str)
                                    == Some(root.as_str())
                        })
                    });
                let Some(cause_ev) = in_thread else {
                    return Err(Error::custom(
                        ERR_NOT_IN_THREAD,
                        http::StatusCode::BAD_REQUEST,
                        "The automatic subscription cause is not an event in this thread.",
                    ));
                };
                if let Some(ex) = &existing {
                    if ex.subscribed && !ex.automatic {
                        // A manual subscription is never downgraded.
                        return Ok(());
                    }
                    if !ex.subscribed
                        && ex
                            .unsubscribed_at
                            .is_some_and(|at| cause_ev.stream_ordering <= at)
                    {
                        return Err(Error::custom(
                            ERR_CONFLICTING_UNSUBSCRIPTION,
                            http::StatusCode::CONFLICT,
                            "The user unsubscribed from the thread after that event.",
                        ));
                    }
                }
                true
            }
        };
        if existing
            .as_ref()
            .is_some_and(|ex| ex.subscribed && ex.automatic == automatic_flag)
        {
            return Ok(());
        }
        self.store
            .subscribe_thread(user_id, room_id, root, automatic_flag)
            .await?;
        self.notifier.notify(Notification::User {
            user_id: user_id.to_owned(),
        });
        Ok(())
    }

    /// Unsubscribes; succeeds even without a subscription, as the position still matters for
    /// later automatic subscriptions.
    pub async fn unsubscribe_thread(
        &self,
        user_id: &UserId,
        room_id: &RoomId,
        root: &EventId,
    ) -> Result<()> {
        self.thread_root_in_room(room_id, root).await?;
        let at = self.store.max_stream_ordering().await?;
        self.store
            .unsubscribe_thread(user_id, room_id, root, at)
            .await?;
        self.notifier.notify(Notification::User {
            user_id: user_id.to_owned(),
        });
        Ok(())
    }

    /// `Some(automatic)` when the user is subscribed to the thread.
    pub async fn thread_subscription(
        &self,
        user_id: &UserId,
        room_id: &RoomId,
        root: &EventId,
    ) -> Result<Option<bool>> {
        self.thread_root_in_room(room_id, root).await?;
        Ok(self
            .store
            .get_thread_subscription(user_id, room_id, root)
            .await?
            .filter(|s| s.subscribed)
            .map(|s| s.automatic))
    }

    /// Changes with bump stamps in `(above, below)`, newest first, at most `limit`; an initial
    /// page lists current subscriptions only.
    pub async fn thread_subscription_changes(
        &self,
        user_id: &UserId,
        above: Option<i64>,
        below: Option<i64>,
        limit: usize,
        initial: bool,
    ) -> Result<ThreadSubscriptionChanges> {
        let limit = limit.clamp(1, 1000);
        let mut rows = self
            .store
            .thread_subscription_changes(user_id, above, below, limit + 1, initial)
            .await?;
        let has_more = rows.len() > limit;
        rows.truncate(limit);
        Ok(ThreadSubscriptionChanges::from_rows(rows, has_more))
    }
}
