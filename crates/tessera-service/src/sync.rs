//! Classic `/sync` (v3).
//!
//! The sync token is a tuple of stream positions: events, account data, receipts and typing.
//! An incremental sync reports everything that moved past the token's positions; a long poll
//! subscribes to the notifier *before* reading so no change can slip between read and wait.

use std::{collections::BTreeMap, time::Duration};

use ruma::{
    OwnedEventId, OwnedRoomId, OwnedUserId, RoomId, UserId,
    events::{StateEventType, room::member::MembershipState},
};
use serde_json::{Value, json};
use tessera_store::{Direction, Membership, StoredEvent};

use crate::{Error, Notification, Result, Services, notifier};

/// Positions across all streams.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SyncToken {
    pub events: i64,
    pub account_data: i64,
    pub receipts: i64,
    pub typing: i64,
}

impl SyncToken {
    pub fn parse(s: &str) -> Option<Self> {
        let rest = s.strip_prefix('s')?;
        let mut it = rest.split('_').map(|p| p.parse::<i64>().ok());
        let events = it.next()??;
        let account_data = it.next().flatten().unwrap_or(0);
        let receipts = it.next().flatten().unwrap_or(0);
        let typing = it.next().flatten().unwrap_or(0);
        Some(Self {
            events,
            account_data,
            receipts,
            typing,
        })
    }
}

impl std::fmt::Display for SyncToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "s{}_{}_{}_{}",
            self.events, self.account_data, self.receipts, self.typing
        )
    }
}

/// What the caller asked for.
#[derive(Clone, Debug)]
pub struct SyncRequest {
    pub user_id: OwnedUserId,
    pub since: Option<SyncToken>,
    pub timeout: Duration,
    pub full_state: bool,
    pub timeline_limit: usize,
}

/// One joined room's delta.
#[derive(Debug, Default)]
pub struct JoinedRoomSync {
    pub state: Vec<StoredEvent>,
    pub timeline: Vec<StoredEvent>,
    pub limited: bool,
    pub prev_batch: Option<String>,
    pub account_data: Vec<Value>,
    pub ephemeral: Vec<Value>,
    pub joined_count: u64,
    pub invited_count: u64,
    pub heroes: Vec<OwnedUserId>,
}

#[derive(Debug, Default)]
pub struct LeftRoomSync {
    pub state: Vec<StoredEvent>,
    pub timeline: Vec<StoredEvent>,
    pub limited: bool,
    pub prev_batch: Option<String>,
}

#[derive(Debug, Default)]
pub struct InvitedRoomSync {
    /// Stripped state events (client format, no `event_id`).
    pub invite_state: Vec<Value>,
}

/// The computed sync response, in domain terms; the API layer renders it.
#[derive(Debug, Default)]
pub struct SyncResult {
    pub next_batch: SyncToken,
    pub join: BTreeMap<OwnedRoomId, JoinedRoomSync>,
    pub leave: BTreeMap<OwnedRoomId, LeftRoomSync>,
    pub invite: BTreeMap<OwnedRoomId, InvitedRoomSync>,
    pub account_data: Vec<Value>,
}

impl SyncResult {
    fn is_empty(&self) -> bool {
        self.join.is_empty()
            && self.leave.is_empty()
            && self.invite.is_empty()
            && self.account_data.is_empty()
    }
}

impl Services {
    /// Run a sync, long-polling up to `timeout` for an incremental sync with nothing new.
    pub async fn sync(&self, req: SyncRequest) -> Result<SyncResult> {
        let mut rx = self.notifier.subscribe();
        let mut result = self.compute_sync(&req).await?;
        if req.since.is_some() && result.is_empty() && !req.timeout.is_zero() {
            let user_id = req.user_id.clone();
            let rooms: Vec<OwnedRoomId> = self
                .store
                .memberships_for_user(&user_id)
                .await?
                .into_iter()
                .map(|m| m.room_id)
                .collect();
            let woke = notifier::wait_for(&mut rx, req.timeout, |n| match n {
                Notification::RoomEvent { room_id, .. }
                | Notification::RoomEphemeral { room_id } => rooms.contains(room_id),
                Notification::User { user_id: u } => *u == user_id,
            })
            .await;
            if woke {
                result = self.compute_sync(&req).await?;
            }
        }
        Ok(result)
    }

    async fn current_token(&self) -> Result<SyncToken> {
        Ok(SyncToken {
            events: self.store.max_stream_ordering().await?,
            account_data: self.store.max_account_data_stream_id().await?,
            receipts: self.store.max_receipt_stream_id().await?,
            typing: self.typing.position(),
        })
    }

    async fn compute_sync(&self, req: &SyncRequest) -> Result<SyncResult> {
        let now = self.current_token().await?;
        let since = req.since;
        if let Some(s) = since
            && s.events > now.events
        {
            return Err(Error::matrix(
                ruma::api::error::ErrorKind::UnknownPos,
                http::StatusCode::BAD_REQUEST,
                "Unknown sync token.",
            ));
        }
        let mut out = SyncResult {
            next_batch: now,
            ..Default::default()
        };
        let user = &req.user_id;
        let limit = req.timeline_limit.clamp(1, 1000);

        let memberships: Vec<Membership> = self
            .store
            .memberships_for_user(user)
            .await?
            .into_iter()
            .filter(|m| !m.forgotten)
            .collect();

        for m in &memberships {
            let changed_in_window = since.is_none_or(|s| m.stream_ordering > s.events);
            match m.membership {
                MembershipState::Join => {
                    // A join that happened inside the window gets the full state treatment.
                    let full = req.full_state || since.is_none() || changed_in_window;
                    if let Some(room) = self
                        .sync_joined_room(user, &m.room_id, since, now, full, limit)
                        .await?
                    {
                        out.join.insert(m.room_id.clone(), room);
                    }
                }
                MembershipState::Invite if changed_in_window || since.is_none() => {
                    out.invite.insert(
                        m.room_id.clone(),
                        self.sync_invited_room(&m.room_id, user).await?,
                    );
                }
                MembershipState::Leave | MembershipState::Ban
                    if changed_in_window && since.is_some() =>
                {
                    out.leave.insert(
                        m.room_id.clone(),
                        self.sync_left_room(&m.room_id, m, since, limit).await?,
                    );
                }
                _ => {}
            }
        }

        // Global account data.
        let ad_since = since.map_or(0, |s| s.account_data);
        for ad in self.store.account_data_since(user, ad_since).await? {
            if ad.stream_id > now.account_data {
                continue;
            }
            match ad.room_id {
                None => out
                    .account_data
                    .push(json!({ "type": ad.data_type, "content": ad.content })),
                Some(room_id) => {
                    if let Some(room) = out.join.get_mut(&room_id) {
                        room.account_data
                            .push(json!({ "type": ad.data_type, "content": ad.content }));
                    }
                }
            }
        }

        Ok(out)
    }

    async fn sync_joined_room(
        &self,
        user: &UserId,
        room_id: &RoomId,
        since: Option<SyncToken>,
        now: SyncToken,
        full_state: bool,
        limit: usize,
    ) -> Result<Option<JoinedRoomSync>> {
        let mut room = JoinedRoomSync::default();

        // Timeline: newest `limit` events in (since, now], oldest first.
        let lower = since.map(|s| s.events);
        let mut events = self
            .store
            .room_events_in_range(
                room_id,
                now.events + 1,
                lower.map(|l| l + 1),
                Direction::Backwards,
                limit + 1,
            )
            .await?;
        // `to` is inclusive on the store side; drop anything at or below `since`.
        if let Some(l) = lower {
            events.retain(|e| e.stream_ordering > l);
        }
        let limited = events.len() > limit;
        events.truncate(limit);
        events.reverse();
        let timeline_start = events.first().map(|e| e.stream_ordering);
        room.limited = limited || (since.is_none() && !events.is_empty());
        if let Some(first) = events.first() {
            room.prev_batch =
                Some(super::rooms::timeline::StreamToken(first.stream_ordering).to_string());
        }
        room.timeline = events;

        // State.
        if full_state {
            room.state = self.store.current_state_pdus(room_id).await?;
            if let Some(start) = timeline_start {
                // State events inside the timeline are not repeated in `state`.
                room.state.retain(|s| s.stream_ordering < start);
            }
        } else if limited && let (Some(s), Some(start)) = (since, timeline_start) {
            room.state = self.state_delta(room_id, s.events, start).await?;
        }

        // Ephemeral: receipts and typing.
        let rc_since = since.map_or(0, |s| s.receipts);
        let receipts = self.store.receipts_for_room(room_id, rc_since).await?;
        if !receipts.is_empty() {
            let mut content: serde_json::Map<String, Value> = serde_json::Map::new();
            for r in receipts {
                if r.receipt_type == "m.read.private" && r.user_id != user {
                    continue;
                }
                let entry = content
                    .entry(r.event_id.to_string())
                    .or_insert_with(|| json!({}))
                    .as_object_mut()
                    .map(|o| o.entry(r.receipt_type.clone()).or_insert_with(|| json!({})));
                if let Some(Value::Object(users)) = entry {
                    let mut data = r.data.clone();
                    if let Some(t) = &r.thread_id {
                        data["thread_id"] = json!(t);
                    }
                    users.insert(r.user_id.to_string(), data);
                }
            }
            room.ephemeral
                .push(json!({ "type": "m.receipt", "content": content }));
        }
        let typing_since = since.map_or(-1, |s| s.typing);
        if self.typing.changed_since(room_id, typing_since) || since.is_none() {
            let users = self.typing.typing_users(room_id);
            if since.is_some() || !users.is_empty() {
                room.ephemeral
                    .push(json!({ "type": "m.typing", "content": { "user_ids": users } }));
            }
        }

        // Summary.
        let members = self.store.members(room_id, None).await?;
        room.joined_count = members
            .iter()
            .filter(|m| m.membership == MembershipState::Join)
            .count() as u64;
        room.invited_count = members
            .iter()
            .filter(|m| m.membership == MembershipState::Invite)
            .count() as u64;
        let has_name = self
            .store
            .current_state_event(room_id, &StateEventType::RoomName, "")
            .await?
            .is_some()
            || self
                .store
                .current_state_event(room_id, &StateEventType::RoomCanonicalAlias, "")
                .await?
                .is_some();
        if !has_name {
            room.heroes = members
                .iter()
                .filter(|m| {
                    m.user_id != user
                        && matches!(
                            m.membership,
                            MembershipState::Join | MembershipState::Invite
                        )
                })
                .take(5)
                .map(|m| m.user_id.clone())
                .collect();
        }

        let nothing_new = since.is_some()
            && !full_state
            && room.timeline.is_empty()
            && room.state.is_empty()
            && room.ephemeral.is_empty();
        if nothing_new {
            return Ok(None);
        }
        Ok(Some(room))
    }

    /// State events that changed between the state at `since` and the state before the event
    /// at stream position `start`.
    async fn state_delta(
        &self,
        room_id: &RoomId,
        since: i64,
        start: i64,
    ) -> Result<Vec<StoredEvent>> {
        let at_since = self
            .store
            .room_events_in_range(room_id, since + 1, None, Direction::Backwards, 1)
            .await?;
        let at_start = self
            .store
            .room_events_in_range(room_id, start, None, Direction::Backwards, 1)
            .await?;
        let (Some(a), Some(b)) = (at_since.first(), at_start.first()) else {
            return Ok(Vec::new());
        };
        let (Some(ga), Some(gb)) = (a.state_group, b.state_group) else {
            return Ok(Vec::new());
        };
        if ga == gb {
            return Ok(Vec::new());
        }
        let before: BTreeMap<(StateEventType, String), OwnedEventId> = self
            .store
            .state_at_group(ga)
            .await?
            .into_iter()
            .map(|e| ((e.event_type, e.state_key), e.event_id))
            .collect();
        let after = self.store.state_at_group(gb).await?;
        let changed: Vec<OwnedEventId> = after
            .into_iter()
            .filter(|e| {
                before.get(&(e.event_type.clone(), e.state_key.clone())) != Some(&e.event_id)
            })
            .map(|e| e.event_id)
            .collect();
        Ok(self.store.get_events(&changed).await?)
    }

    async fn sync_left_room(
        &self,
        room_id: &RoomId,
        m: &Membership,
        since: Option<SyncToken>,
        limit: usize,
    ) -> Result<LeftRoomSync> {
        let lower = since.map(|s| s.events);
        let mut events = self
            .store
            .room_events_in_range(
                room_id,
                m.stream_ordering + 1,
                lower.map(|l| l + 1),
                Direction::Backwards,
                limit + 1,
            )
            .await?;
        if let Some(l) = lower {
            events.retain(|e| e.stream_ordering > l);
        }
        let limited = events.len() > limit;
        events.truncate(limit);
        events.reverse();
        let prev_batch = events
            .first()
            .map(|e| super::rooms::timeline::StreamToken(e.stream_ordering).to_string());
        Ok(LeftRoomSync {
            state: Vec::new(),
            timeline: events,
            limited,
            prev_batch,
        })
    }

    async fn sync_invited_room(&self, room_id: &RoomId, user: &UserId) -> Result<InvitedRoomSync> {
        Ok(InvitedRoomSync {
            invite_state: self.stripped_state(room_id, Some(user)).await?,
        })
    }

    /// Stripped state for invites/knocks: a fixed subset of state in client format without
    /// event IDs.
    pub async fn stripped_state(
        &self,
        room_id: &RoomId,
        invitee: Option<&UserId>,
    ) -> Result<Vec<Value>> {
        let state = self.store.current_state_pdus(room_id).await?;
        let mut out = Vec::new();
        for se in state {
            let keep = match se.pdu.state_tuple() {
                Some((
                    StateEventType::RoomCreate
                    | StateEventType::RoomJoinRules
                    | StateEventType::RoomName
                    | StateEventType::RoomAvatar
                    | StateEventType::RoomCanonicalAlias
                    | StateEventType::RoomTopic
                    | StateEventType::RoomEncryption,
                    _,
                )) => true,
                Some((StateEventType::RoomMember, key)) => {
                    // the invitee's own event and its sender
                    invitee.is_some_and(|u| u.as_str() == key)
                        || invitee
                            .is_some_and(|u| se.pdu.sender() != u && is_inviter_of(&se.pdu, u))
                }
                _ => false,
            };
            if keep {
                let mut v = se.pdu.to_client_json();
                if let Value::Object(o) = &mut v {
                    o.retain(|k, _| {
                        matches!(k.as_str(), "type" | "state_key" | "content" | "sender")
                    });
                }
                out.push(v);
            }
        }
        // Also include the inviter's member event explicitly.
        if let Some(u) = invitee
            && let Some(m) = self.store.get_membership(room_id, u).await?
            && let Some(inviter) = self.store.get_membership(room_id, &m.sender).await?
            && let Some(ev) = self.store.get_event(&inviter.event_id).await?
        {
            let mut v = ev.pdu.to_client_json();
            if let Value::Object(o) = &mut v {
                o.retain(|k, _| matches!(k.as_str(), "type" | "state_key" | "content" | "sender"));
            }
            if !out
                .iter()
                .any(|x| x["state_key"] == v["state_key"] && x["type"] == v["type"])
            {
                out.push(v);
            }
        }
        Ok(out)
    }
}

fn is_inviter_of(_member_event: &tessera_core::Pdu, _user: &UserId) -> bool {
    false
}
