//! Sending and reading timeline/state events as a local user.

use ruma::{
    DeviceId, EventId, OwnedEventId, RoomId, UserId,
    events::{StateEventType, TimelineEventType, room::member::MembershipState},
};
use serde_json::Value;
use tessera_store::{Direction, StoredEvent};

use crate::{
    Error, Result, Services,
    rooms::{pipeline::LocalEvent, power},
};

/// A pagination token: stream position, rendered as `s<N>`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StreamToken(pub i64);

impl StreamToken {
    /// Accepts both bare `s<N>` tokens and full sync tokens (`s<N>_<...>`), since clients
    /// hand `prev_batch`/`next_batch` from `/sync` to `/messages`.
    pub fn parse(s: &str) -> Option<Self> {
        let n = s.strip_prefix('s')?;
        let n = n.split('_').next()?;
        n.parse().ok().map(Self)
    }
}

impl std::fmt::Display for StreamToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "s{}", self.0)
    }
}

/// A page of timeline events.
#[derive(Debug)]
pub struct TimelinePage {
    pub events: Vec<StoredEvent>,
    pub start: StreamToken,
    /// `None` when the end of history (in that direction) was reached.
    pub end: Option<StreamToken>,
}

impl Services {
    /// Send a message-like event, de-duplicated by transaction ID.
    pub async fn send_message(
        &self,
        room_id: &RoomId,
        sender: &UserId,
        device_id: Option<&DeviceId>,
        txn_id: &str,
        event_type: &str,
        content: Value,
    ) -> Result<OwnedEventId> {
        // Transaction IDs are scoped to the device *and* the endpoint path (room + type).
        let txn_key = format!("send:{room_id}:{event_type}:{txn_id}");
        if let Some(existing) = self
            .store
            .get_txn_event(sender, device_id, &txn_key)
            .await?
        {
            return Ok(existing);
        }
        self.require_joined(room_id, sender).await?;
        let ev = LocalEvent::new(room_id.to_owned(), sender.to_owned(), event_type.to_owned())
            .content(&content)?;
        let stored = self.send_local_event(ev).await?;
        self.store
            .set_txn_event(sender, device_id, &txn_key, stored.pdu.event_id())
            .await?;
        self.store
            .set_event_txn_id(stored.pdu.event_id(), sender, device_id, txn_id)
            .await?;
        Ok(stored.pdu.event_id().to_owned())
    }

    /// Send a state event.
    pub async fn send_state(
        &self,
        room_id: &RoomId,
        sender: &UserId,
        event_type: &str,
        state_key: &str,
        content: Value,
    ) -> Result<OwnedEventId> {
        self.require_joined(room_id, sender).await?;
        // Membership changes via /state go through the membership logic so profile fields
        // and invite handling stay consistent.
        if event_type == "m.room.member" {
            let target = ruma::OwnedUserId::try_from(state_key)
                .map_err(|e| Error::invalid_param(e.to_string()))?;
            let membership = content
                .get("membership")
                .and_then(|m| m.as_str())
                .unwrap_or("");
            let reason = content.get("reason").and_then(|r| r.as_str());
            let ev = match (membership, target == sender) {
                ("join", true) => self.join_local_room(room_id, sender, reason).await?,
                ("leave", true) => self
                    .leave_room(room_id, sender, reason)
                    .await?
                    .ok_or_else(|| Error::forbidden("Not in room."))?,
                ("invite", false) => {
                    self.invite_user(room_id, sender, &target, reason, false)
                        .await?
                }
                ("leave" | "ban", false) => {
                    self.set_membership_of_other(room_id, sender, &target, membership, reason)
                        .await?
                }
                _ => return Err(Error::forbidden("Unsupported membership change.")),
            };
            return Ok(ev.pdu.event_id().to_owned());
        }
        let ev = LocalEvent::new(room_id.to_owned(), sender.to_owned(), event_type.to_owned())
            .state_key(state_key)
            .content(&content)?;
        let sent = self.send_local_event(ev).await.map_err(|e| match e {
            // Malformed power-level content fails the auth rules; clients expect 400 for that.
            Error::Matrix {
                kind: ruma::api::error::ErrorKind::Forbidden,
                message,
                ..
            } if event_type == "m.room.power_levels" && is_content_error(&message) => {
                Error::matrix(
                    ruma::api::error::ErrorKind::BadJson,
                    http::StatusCode::BAD_REQUEST,
                    message,
                )
            }
            other => other,
        })?;
        Ok(sent.pdu.event_id().to_owned())
    }

    /// Redact an event.
    pub async fn redact(
        &self,
        room_id: &RoomId,
        sender: &UserId,
        device_id: Option<&DeviceId>,
        txn_id: &str,
        target: &EventId,
        reason: Option<&str>,
    ) -> Result<OwnedEventId> {
        let txn_key = format!("redact:{room_id}:{target}:{txn_id}");
        if let Some(existing) = self
            .store
            .get_txn_event(sender, device_id, &txn_key)
            .await?
        {
            return Ok(existing);
        }
        self.require_joined(room_id, sender).await?;
        let ctx = self.room_context(room_id).await?;
        let target_ev = self.store.get_event(target).await?;
        let own = target_ev.as_ref().is_some_and(|t| t.pdu.sender() == sender);
        if !own && !power::can(&ctx.state, sender, "redact", &ctx.rules) {
            return Err(Error::forbidden(
                "You don't have permission to redact events in this room.",
            ));
        }
        let mut content = serde_json::Map::new();
        if let Some(r) = reason {
            content.insert("reason".into(), Value::String(r.to_owned()));
        }
        let mut ev = LocalEvent::new(
            room_id.to_owned(),
            sender.to_owned(),
            TimelineEventType::RoomRedaction,
        )
        .content(&Value::Object(content))?;
        ev.redacts = Some(target.to_owned());
        let stored = self.send_local_event(ev).await?;
        self.store
            .set_txn_event(sender, device_id, &txn_key, stored.pdu.event_id())
            .await?;
        self.store
            .set_event_txn_id(stored.pdu.event_id(), sender, device_id, txn_id)
            .await?;
        Ok(stored.pdu.event_id().to_owned())
    }

    /// Error unless the user is joined.
    pub async fn require_joined(&self, room_id: &RoomId, user: &UserId) -> Result<()> {
        if !self.store.room_exists(room_id).await? {
            return Err(Error::not_found("Room not found."));
        }
        if self.is_joined(room_id, user).await? {
            Ok(())
        } else {
            Err(Error::forbidden("You are not joined to this room."))
        }
    }

    /// Whether the user may read the room's history right now: joined, or left/banned with
    /// history visibility permitting (simplified: members and former members may read).
    pub async fn can_read_room(&self, room_id: &RoomId, user: &UserId) -> Result<bool> {
        if let Some(m) = self.store.get_membership(room_id, user).await? {
            if m.forgotten {
                return Ok(false);
            }
            return Ok(matches!(
                m.membership,
                MembershipState::Join
                    | MembershipState::Leave
                    | MembershipState::Invite
                    | MembershipState::Ban
            ));
        }
        // World-readable rooms.
        let ctx = self.room_context(room_id).await?;
        let hv = ctx
            .get(StateEventType::RoomHistoryVisibility, "")
            .and_then(|p| serde_json::from_str::<Value>(p.content().get()).ok())
            .and_then(|v| {
                v.get("history_visibility")
                    .and_then(|h| h.as_str())
                    .map(str::to_owned)
            });
        Ok(hv.as_deref() == Some("world_readable"))
    }

    /// `/messages`: paginate the room timeline.
    pub async fn messages(
        &self,
        room_id: &RoomId,
        user: &UserId,
        from: Option<StreamToken>,
        to: Option<StreamToken>,
        direction: Direction,
        limit: usize,
    ) -> Result<TimelinePage> {
        if !self.can_read_room(room_id, user).await? {
            return Err(Error::forbidden("You are not allowed to read this room."));
        }
        let limit = limit.clamp(1, 1000);
        let from = match (from, direction) {
            (Some(t), _) => t,
            (None, Direction::Backwards) => {
                StreamToken(self.store.max_stream_ordering().await? + 1)
            }
            (None, Direction::Forwards) => StreamToken(i64::MIN + 1),
        };
        let events = self
            .store
            .room_events_in_range(room_id, from.0, to.map(|t| t.0), direction, limit)
            .await?;
        let end = if events.len() < limit {
            None
        } else {
            events.last().map(|e| StreamToken(e.stream_ordering))
        };
        Ok(TimelinePage {
            events,
            start: from,
            end,
        })
    }

    /// `/context`: events around one event.
    pub async fn context(
        &self,
        room_id: &RoomId,
        user: &UserId,
        event_id: &EventId,
        limit: usize,
    ) -> Result<(
        Vec<StoredEvent>,
        StoredEvent,
        Vec<StoredEvent>,
        StreamToken,
        StreamToken,
    )> {
        if !self.can_read_room(room_id, user).await? {
            return Err(Error::forbidden("You are not allowed to read this room."));
        }
        let ev = self
            .store
            .get_event(event_id)
            .await?
            .filter(|e| e.pdu.room_id() == room_id && !e.outlier)
            .ok_or_else(|| Error::not_found("Event not found."))?;
        let half = limit.max(1).div_ceil(2);
        let before = self
            .store
            .room_events_in_range(
                room_id,
                ev.stream_ordering,
                None,
                Direction::Backwards,
                half,
            )
            .await?;
        let after = self
            .store
            .room_events_in_range(room_id, ev.stream_ordering, None, Direction::Forwards, half)
            .await?;
        let start = StreamToken(
            before
                .last()
                .map_or(ev.stream_ordering, |e| e.stream_ordering),
        );
        let end = StreamToken(
            after
                .last()
                .map_or(ev.stream_ordering, |e| e.stream_ordering),
        );
        Ok((before, ev, after, start, end))
    }

    /// A single event, with a visibility check.
    pub async fn get_room_event(
        &self,
        room_id: &RoomId,
        user: &UserId,
        event_id: &EventId,
    ) -> Result<StoredEvent> {
        if !self.can_read_room(room_id, user).await? {
            return Err(Error::forbidden("You are not allowed to read this room."));
        }
        self.store
            .get_event(event_id)
            .await?
            .filter(|e| e.pdu.room_id() == room_id)
            .ok_or_else(|| Error::not_found("Event not found."))
    }

    /// Current state of a room as stored events (visibility checked).
    pub async fn room_state(&self, room_id: &RoomId, user: &UserId) -> Result<Vec<StoredEvent>> {
        if !self.can_read_room(room_id, user).await? {
            return Err(Error::forbidden("You are not allowed to read this room."));
        }
        // Users who left see the state as of their departure.
        if let Some(m) = self.store.get_membership(room_id, user).await?
            && matches!(m.membership, MembershipState::Leave | MembershipState::Ban)
            && let Some(leave) = self.store.get_event(&m.event_id).await?
            && let Some(group) = leave.state_group
        {
            let ids: Vec<ruma::OwnedEventId> = self
                .store
                .state_at_group(group)
                .await?
                .into_iter()
                .map(|e| e.event_id)
                .collect();
            return Ok(self.store.get_events(&ids).await?);
        }
        Ok(self.store.current_state_pdus(room_id).await?)
    }
}

/// Whether an auth-rule failure message describes malformed content rather than missing power.
fn is_content_error(message: &str) -> bool {
    let m = message.to_lowercase();
    m.contains("creator user ids")
        || m.contains("integer")
        || m.contains("invalid")
        || m.contains("must be")
}
