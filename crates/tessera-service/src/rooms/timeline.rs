//! Sending and reading timeline/state events as a local user.

use ruma::{
    DeviceId, EventId, OwnedEventId, RoomId, UserId,
    events::{StateEventType, TimelineEventType, room::member::MembershipState},
};
use serde_json::Value;
use std::collections::HashMap;
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
        // Re-sending the exact current state is idempotent: return the existing event.
        if let Some(current_id) = self
            .store
            .current_state_event(room_id, &StateEventType::from(event_type), state_key)
            .await?
            && let Some(current) = self.store.get_event(&current_id).await?
            && serde_json::from_str::<Value>(current.pdu.content().get())
                .ok()
                .as_ref()
                == Some(&content)
            && current.pdu.sender() == sender
        {
            return Ok(current_id);
        }
        if event_type == "m.room.canonical_alias" {
            self.validate_canonical_alias(room_id, &content).await?;
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
        if !self.store.room_exists(room_id).await? {
            return Ok(false);
        }
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

    /// `/messages`: paginate the room timeline. `keep` is the client's event filter; history
    /// visibility is applied here too. Pages are filled up to `limit` matching events as long
    /// as more history exists (fetched over federation when we run out locally).
    #[allow(clippy::too_many_arguments)]
    pub async fn messages(
        &self,
        room_id: &RoomId,
        user: &UserId,
        from: Option<StreamToken>,
        to: Option<StreamToken>,
        direction: Direction,
        limit: usize,
        keep: &(dyn Fn(&StoredEvent) -> bool + Sync),
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
        let mut events: Vec<StoredEvent> = Vec::new();
        let mut cursor = from;
        let mut exhausted = false;
        let mut backfills = 0;
        for _ in 0..20 {
            let batch = self
                .store
                .room_events_in_range(room_id, cursor.0, to.map(|t| t.0), direction, limit)
                .await?;
            let batch_len = batch.len();
            if let Some(last) = batch.last() {
                cursor = StreamToken(last.stream_ordering);
            }
            let visible = self.filter_visible(room_id, user, batch).await?;
            events.extend(visible.into_iter().filter(|e| keep(e)));
            if events.len() >= limit {
                break;
            }
            if batch_len < limit {
                // Out of local history: paginating backwards may pull more over federation.
                if direction == Direction::Backwards && to.is_none() && backfills < 3 {
                    backfills += 1;
                    let got = self
                        .federation()
                        .backfill(room_id, limit.max(100))
                        .await
                        .unwrap_or_else(|e| {
                            tracing::debug!(%room_id, error = %e, "backfill failed");
                            0
                        });
                    if got > 0 {
                        continue;
                    }
                }
                exhausted = true;
                break;
            }
        }
        events.truncate(limit);
        let end = if exhausted { None } else { Some(cursor) };
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

impl Services {
    /// `m.room.canonical_alias`: every alias named must be well-formed and point at this room.
    async fn validate_canonical_alias(&self, room_id: &RoomId, content: &Value) -> Result<()> {
        let mut aliases: Vec<&str> = Vec::new();
        if let Some(a) = content.get("alias") {
            match a {
                Value::Null => {}
                Value::String(s) if s.is_empty() => {}
                Value::String(s) => aliases.push(s),
                _ => return Err(Error::bad_json("`alias` must be a string.")),
            }
        }
        if let Some(alt) = content.get("alt_aliases") {
            let Some(list) = alt.as_array() else {
                return Err(Error::bad_json("`alt_aliases` must be an array."));
            };
            for a in list {
                match a.as_str() {
                    Some(s) => aliases.push(s),
                    None => return Err(Error::bad_json("`alt_aliases` entries must be strings.")),
                }
            }
        }
        for a in aliases {
            let alias = ruma::OwnedRoomAliasId::try_from(a)
                .map_err(|_| Error::invalid_param(format!("Invalid room alias: {a}")))?;
            let target = if alias.server_name() == self.config.server_name {
                self.store.get_alias(&alias).await?.map(|a| a.room_id)
            } else {
                self.federation()
                    .resolve_alias(&alias)
                    .await
                    .ok()
                    .map(|(r, _)| r)
            };
            if target.as_deref() != Some(room_id) {
                return Err(Error::matrix(
                    ruma::api::error::ErrorKind::BadAlias,
                    http::StatusCode::BAD_REQUEST,
                    format!("Room alias {a} does not point to this room."),
                ));
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------- history visibility

/// What decides whether a user may see an event: the room's history visibility and the
/// user's membership as of that event.
#[derive(Debug, Clone)]
struct VisibilityContext {
    history_visibility: String,
    membership: Option<MembershipState>,
}

impl Services {
    /// The state group holding the state *before* `ev` (for state events) or at it.
    async fn state_group_for_visibility(
        &self,
        ev: &StoredEvent,
    ) -> Result<Option<tessera_store::StateGroupId>> {
        if ev.pdu.is_state() {
            Ok(self.store.state_group_before(ev.pdu.event_id()).await?)
        } else {
            Ok(ev.state_group)
        }
    }

    async fn visibility_context_at(
        &self,
        group: tessera_store::StateGroupId,
        user: &UserId,
    ) -> Result<VisibilityContext> {
        let entries = self.store.state_at_group(group).await?;
        let mut hv = "shared".to_owned();
        let mut membership = None;
        for e in entries {
            if e.event_type == StateEventType::RoomHistoryVisibility && e.state_key.is_empty() {
                if let Some(ev) = self.store.get_event(&e.event_id).await?
                    && let Ok(c) = serde_json::from_str::<Value>(ev.pdu.content().get())
                    && let Some(v) = c.get("history_visibility").and_then(Value::as_str)
                {
                    v.clone_into(&mut hv);
                }
            } else if e.event_type == StateEventType::RoomMember
                && e.state_key == user.as_str()
                && let Some(ev) = self.store.get_event(&e.event_id).await?
                && let Ok(c) = serde_json::from_str::<Value>(ev.pdu.content().get())
                && let Some(m) = c.get("membership").and_then(Value::as_str)
            {
                membership = Some(MembershipState::from(m.to_owned()));
            }
        }
        Ok(VisibilityContext {
            history_visibility: hv,
            membership,
        })
    }

    /// Whether `user` may see `ev` under the room's history visibility rules.
    pub async fn event_visible_to(&self, ev: &StoredEvent, user: &UserId) -> Result<bool> {
        let currently_joined = self.is_joined(ev.pdu.room_id(), user).await?;
        let mut cache = HashMap::new();
        self.event_visible_cached(ev, user, currently_joined, &mut cache)
            .await
    }

    async fn event_visible_cached(
        &self,
        ev: &StoredEvent,
        user: &UserId,
        currently_joined: bool,
        cache: &mut HashMap<tessera_store::StateGroupId, VisibilityContext>,
    ) -> Result<bool> {
        // A user always sees their own membership changes.
        if ev.pdu.event_type() == &TimelineEventType::RoomMember
            && ev.pdu.state_key() == Some(user.as_str())
        {
            return Ok(true);
        }
        let Some(group) = self.state_group_for_visibility(ev).await? else {
            return Ok(false);
        };
        let ctx = match cache.get(&group) {
            Some(c) => c.clone(),
            None => {
                let c = self.visibility_context_at(group, user).await?;
                cache.insert(group, c.clone());
                c
            }
        };
        let visible = match ctx.history_visibility.as_str() {
            "world_readable" => true,
            "shared" => currently_joined || ctx.membership == Some(MembershipState::Join),
            "invited" => matches!(
                ctx.membership,
                Some(MembershipState::Join | MembershipState::Invite)
            ),
            _ => ctx.membership == Some(MembershipState::Join),
        };
        Ok(visible)
    }

    /// Keep only the events `user` may see.
    pub async fn filter_visible(
        &self,
        room_id: &RoomId,
        user: &UserId,
        events: Vec<StoredEvent>,
    ) -> Result<Vec<StoredEvent>> {
        let currently_joined = self.is_joined(room_id, user).await?;
        let mut cache = HashMap::new();
        let mut out = Vec::with_capacity(events.len());
        for ev in events {
            if self
                .event_visible_cached(&ev, user, currently_joined, &mut cache)
                .await?
            {
                out.push(ev);
            }
        }
        Ok(out)
    }

    /// The user's membership as of each event, as the `unsigned.membership` field: the
    /// membership before the event, except for the user's own membership events where it is
    /// the new membership. Absent membership reads as `leave`.
    pub async fn membership_at_events(
        &self,
        user: &UserId,
        events: &[StoredEvent],
    ) -> Result<Vec<String>> {
        let mut cache: HashMap<tessera_store::StateGroupId, String> = HashMap::new();
        let mut out = Vec::with_capacity(events.len());
        for ev in events {
            if ev.pdu.event_type() == &TimelineEventType::RoomMember
                && ev.pdu.state_key() == Some(user.as_str())
            {
                let m = serde_json::from_str::<Value>(ev.pdu.content().get())
                    .ok()
                    .and_then(|c| {
                        c.get("membership")
                            .and_then(Value::as_str)
                            .map(str::to_owned)
                    })
                    .unwrap_or_else(|| "leave".to_owned());
                out.push(m);
                continue;
            }
            let Some(group) = self.state_group_for_visibility(ev).await? else {
                out.push("leave".to_owned());
                continue;
            };
            let m = match cache.get(&group) {
                Some(m) => m.clone(),
                None => {
                    let ctx = self.visibility_context_at(group, user).await?;
                    let m = ctx
                        .membership
                        .map_or_else(|| "leave".to_owned(), |m| m.to_string());
                    cache.insert(group, m.clone());
                    m
                }
            };
            out.push(m);
        }
        Ok(out)
    }

    /// Room state as of a stream token (`/members?at=`), with the same visibility rules as
    /// `room_state`.
    pub async fn room_state_at(
        &self,
        room_id: &RoomId,
        user: &UserId,
        at: StreamToken,
    ) -> Result<Vec<StoredEvent>> {
        if !self.can_read_room(room_id, user).await? {
            return Err(Error::forbidden("You are not allowed to read this room."));
        }
        let mut bound = at.0;
        if let Some(m) = self.store.get_membership(room_id, user).await?
            && m.membership != MembershipState::Join
        {
            bound = bound.min(m.stream_ordering);
        }
        let latest = self
            .store
            .room_events_in_range(room_id, bound + 1, None, Direction::Backwards, 1)
            .await?;
        let Some(ev) = latest.into_iter().next() else {
            return Ok(Vec::new());
        };
        let Some(group) = ev.state_group else {
            return Ok(Vec::new());
        };
        let ids: Vec<ruma::OwnedEventId> = self
            .store
            .state_at_group(group)
            .await?
            .into_iter()
            .map(|e| e.event_id)
            .collect();
        self.store.get_events(&ids).await.map_err(Into::into)
    }
}
