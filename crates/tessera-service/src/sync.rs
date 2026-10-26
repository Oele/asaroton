//! Classic `/sync` (v3).
//!
//! The sync token is a tuple of stream positions. An incremental sync reports everything that
//! moved past the token's positions; a long poll subscribes to the notifier *before* reading
//! so no change can slip between read and wait. The API layer renders the result as JSON.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    time::Duration,
};

use ruma::{
    OwnedDeviceId, OwnedEventId, OwnedRoomId, OwnedUserId, RoomId, UserId,
    events::{StateEventType, room::member::MembershipState},
    presence::PresenceState,
};
use serde_json::{Value, json};
use tessera_store::{Direction, Membership, StoredEvent};

use crate::{Error, Notification, Result, Services, notifier, presence::presence_event};

/// Positions across all streams.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SyncToken {
    pub events: i64,
    pub account_data: i64,
    pub receipts: i64,
    pub typing: i64,
    pub to_device: i64,
    pub device_lists: i64,
    pub presence: i64,
    pub thread_subscriptions: i64,
}

impl SyncToken {
    pub fn parse(s: &str) -> Option<Self> {
        let rest = s.strip_prefix('s')?;
        let mut it = rest.split('_').map(|p| p.parse::<i64>().ok());
        let events = it.next()??;
        let mut next = || it.next().flatten().unwrap_or(0);
        Some(Self {
            events,
            account_data: next(),
            receipts: next(),
            typing: next(),
            to_device: next(),
            device_lists: next(),
            presence: next(),
            thread_subscriptions: next(),
        })
    }
}

impl std::fmt::Display for SyncToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "s{}_{}_{}_{}_{}_{}_{}_{}",
            self.events,
            self.account_data,
            self.receipts,
            self.typing,
            self.to_device,
            self.device_lists,
            self.presence,
            self.thread_subscriptions
        )
    }
}

/// The subset of a sync filter the server applies.
#[derive(Clone, Debug)]
pub struct SyncFilter {
    pub timeline_limit: usize,
    pub timeline_types: Option<Vec<String>>,
    pub timeline_not_types: Vec<String>,
    pub timeline_senders: Option<Vec<OwnedUserId>>,
    pub timeline_not_senders: Vec<OwnedUserId>,
    pub state_types: Option<Vec<String>>,
    pub state_not_types: Vec<String>,
    pub lazy_load_members: bool,
    pub include_redundant_members: bool,
    pub include_leave: bool,
    pub unread_thread_notifications: bool,
    pub rooms: Option<Vec<OwnedRoomId>>,
    pub not_rooms: Vec<OwnedRoomId>,
    pub presence_enabled: bool,
}

impl Default for SyncFilter {
    fn default() -> Self {
        Self {
            timeline_limit: 10,
            timeline_types: None,
            timeline_not_types: Vec::new(),
            timeline_senders: None,
            timeline_not_senders: Vec::new(),
            state_types: None,
            state_not_types: Vec::new(),
            lazy_load_members: false,
            include_redundant_members: false,
            include_leave: false,
            unread_thread_notifications: false,
            rooms: None,
            not_rooms: Vec::new(),
            presence_enabled: true,
        }
    }
}

/// Glob match for event type filters (`m.room.*`).
pub fn type_matches(pattern: &str, ty: &str) -> bool {
    match pattern.strip_suffix('*') {
        Some(prefix) => ty.starts_with(prefix),
        None => pattern == ty,
    }
}

impl SyncFilter {
    fn timeline_allows(&self, ev: &StoredEvent) -> bool {
        let ty = ev.pdu.event_type().to_string();
        if let Some(types) = &self.timeline_types
            && !types.iter().any(|p| type_matches(p, &ty))
        {
            return false;
        }
        if self.timeline_not_types.iter().any(|p| type_matches(p, &ty)) {
            return false;
        }
        if let Some(senders) = &self.timeline_senders
            && !senders.iter().any(|s| s == ev.pdu.sender())
        {
            return false;
        }
        if self
            .timeline_not_senders
            .iter()
            .any(|s| s == ev.pdu.sender())
        {
            return false;
        }
        true
    }

    fn state_allows(&self, ev: &StoredEvent) -> bool {
        let ty = ev.pdu.event_type().to_string();
        if let Some(types) = &self.state_types
            && !types.iter().any(|p| type_matches(p, &ty))
        {
            return false;
        }
        !self.state_not_types.iter().any(|p| type_matches(p, &ty))
    }

    fn room_allowed(&self, room_id: &RoomId) -> bool {
        if let Some(rooms) = &self.rooms
            && !rooms.iter().any(|r| r == room_id)
        {
            return false;
        }
        !self.not_rooms.iter().any(|r| r == room_id)
    }
}

/// What the caller asked for.
#[derive(Clone, Debug)]
pub struct SyncRequest {
    pub user_id: OwnedUserId,
    pub device_id: Option<OwnedDeviceId>,
    pub since: Option<SyncToken>,
    pub timeout: Duration,
    pub full_state: bool,
    pub filter: SyncFilter,
    pub set_presence: PresenceState,
    /// `use_state_after`: report the state at the end of the timeline (`state_after`) instead
    /// of the state at its start.
    pub use_state_after: bool,
}

/// A room timeline/state delta.
#[derive(Debug, Default)]
pub struct RoomSync {
    pub state: Vec<StoredEvent>,
    /// `state_after` (MSC4222 / v1.16) when requested: state changes up to the end of the
    /// timeline, including those the timeline carries.
    pub state_after: Option<Vec<StoredEvent>>,
    pub timeline: Vec<StoredEvent>,
    pub limited: bool,
    pub prev_batch: Option<String>,
    pub account_data: Vec<Value>,
    pub ephemeral: Vec<Value>,
    pub joined_count: u64,
    pub invited_count: u64,
    pub heroes: Vec<OwnedUserId>,
    /// `event_id -> transaction_id` for events this device sent.
    pub txn_ids: HashMap<OwnedEventId, String>,
    pub unread_notifications: u64,
    pub unread_highlights: u64,
    /// Per-thread counts, only when the filter asks for `unread_thread_notifications`.
    pub unread_threads: std::collections::BTreeMap<OwnedEventId, (u64, u64)>,
}

/// The computed sync response, in domain terms; the API layer renders it.
#[derive(Debug, Default)]
pub struct SyncResult {
    pub next_batch: SyncToken,
    pub join: BTreeMap<OwnedRoomId, RoomSync>,
    pub leave: BTreeMap<OwnedRoomId, RoomSync>,
    /// Stripped state per invited room.
    pub invite: BTreeMap<OwnedRoomId, Vec<Value>>,
    /// Stripped state per room the user has knocked on.
    pub knock: BTreeMap<OwnedRoomId, Vec<Value>>,
    pub account_data: Vec<Value>,
    pub presence: Vec<Value>,
    pub to_device: Vec<Value>,
    pub device_lists_changed: Vec<OwnedUserId>,
    pub device_lists_left: Vec<OwnedUserId>,
    pub one_time_key_counts: BTreeMap<String, u64>,
    pub unused_fallback_key_types: Vec<String>,
}

impl SyncResult {
    fn is_empty(&self) -> bool {
        self.join.is_empty()
            && self.leave.is_empty()
            && self.invite.is_empty()
            && self.account_data.is_empty()
            && self.presence.is_empty()
            && self.to_device.is_empty()
            && self.device_lists_changed.is_empty()
    }
}

impl Services {
    /// Run a sync, long-polling up to `timeout` for an incremental sync with nothing new.
    pub async fn sync(&self, req: SyncRequest) -> Result<SyncResult> {
        // Syncing marks the user active with the requested presence.
        if req.set_presence != PresenceState::Offline || self.presence.get(&req.user_id).is_some() {
            self.set_presence(&req.user_id, req.set_presence.clone(), None)
                .await;
        }
        self.presence.touch(&req.user_id);

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
                // Cheap to re-check; the sync itself filters to relevant users.
                Notification::DeviceLists { .. } | Notification::Presence { .. } => true,
            })
            .await;
            if woke {
                result = self.compute_sync(&req).await?;
            }
        }
        Ok(result)
    }

    pub(crate) async fn current_token(&self) -> Result<SyncToken> {
        Ok(SyncToken {
            events: self.store.max_stream_ordering().await?,
            account_data: self.store.max_account_data_stream_id().await?,
            receipts: self.store.max_receipt_stream_id().await?,
            typing: self.typing.position(),
            to_device: self.store.max_to_device_stream_id().await?,
            device_lists: self.store.max_device_list_stream_id().await?,
            presence: self.presence.position(),
            thread_subscriptions: self.store.max_thread_subscription_stream_id().await?,
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
        let filter = &req.filter;

        // Forgotten rooms are hidden, except that a leave inside the window is still reported.
        let memberships: Vec<Membership> = self
            .store
            .memberships_for_user(user)
            .await?
            .into_iter()
            .filter(|m| !m.forgotten || since.is_some_and(|s| m.stream_ordering > s.events))
            .filter(|m| filter.room_allowed(&m.room_id))
            .collect();

        let ignored = self.ignored_users(user).await?;
        // Incremental syncs: rooms with nothing new (no events, receipts or typing) are not
        // visited at all; two set queries decide that for every joined room at once.
        let joined_ids: Vec<OwnedRoomId> = memberships
            .iter()
            .filter(|m| m.membership == MembershipState::Join)
            .map(|m| m.room_id.clone())
            .collect();
        let (active_events, active_receipts): (HashSet<OwnedRoomId>, HashSet<OwnedRoomId>) =
            match since {
                Some(s) if !req.full_state => (
                    self.store
                        .rooms_with_events_in_range(&joined_ids, s.events, now.events)
                        .await?
                        .into_iter()
                        .collect(),
                    self.store
                        .rooms_with_receipts_since(&joined_ids, s.receipts)
                        .await?
                        .into_iter()
                        .collect(),
                ),
                _ => (HashSet::new(), HashSet::new()),
            };
        let mut newly_joined: Vec<OwnedRoomId> = Vec::new();
        for m in &memberships {
            let changed_in_window = since.is_none_or(|s| m.stream_ordering > s.events);
            match m.membership {
                MembershipState::Join => {
                    let full = req.full_state || since.is_none() || changed_in_window;
                    if changed_in_window && since.is_some() {
                        newly_joined.push(m.room_id.clone());
                    }
                    if let Some(s) = since
                        && !full
                        && !active_events.contains(&m.room_id)
                        && !active_receipts.contains(&m.room_id)
                        && !self.typing.changed_since(&m.room_id, s.typing)
                    {
                        continue;
                    }
                    if let Some(room) = self
                        .sync_room(
                            req,
                            &m.room_id,
                            since,
                            now,
                            full,
                            None,
                            changed_in_window && since.is_some(),
                            &ignored,
                        )
                        .await?
                    {
                        out.join.insert(m.room_id.clone(), room);
                    }
                }
                MembershipState::Invite if changed_in_window && !ignored.contains(&m.sender) => {
                    out.invite.insert(
                        m.room_id.clone(),
                        self.stripped_state(&m.room_id, Some(user)).await?,
                    );
                }
                MembershipState::Knock if changed_in_window => {
                    out.knock.insert(
                        m.room_id.clone(),
                        self.stripped_state(&m.room_id, Some(user)).await?,
                    );
                }
                MembershipState::Leave | MembershipState::Ban
                    if (changed_in_window && since.is_some())
                        || (since.is_none() && filter.include_leave) =>
                {
                    if let Some(room) = self
                        .sync_room(
                            req,
                            &m.room_id,
                            since,
                            now,
                            since.is_none(),
                            Some(m.stream_ordering),
                            false,
                            &ignored,
                        )
                        .await?
                    {
                        out.leave.insert(m.room_id.clone(), room);
                    }
                }
                _ => {}
            }
        }

        // Global account data, including push rules which live in their own table.
        let ad_since = since.map_or(0, |s| s.account_data);
        let pr_stream = self.push_rules_stream_id(user).await?;
        if since.is_none() || (pr_stream > ad_since && pr_stream <= now.account_data) {
            out.account_data
                .push(json!({ "type": "m.push_rules", "content": self.push_rules_account_data(user).await? }));
        }
        for ad in self.store.account_data_since(user, ad_since).await? {
            if ad.stream_id > now.account_data || (since.is_none() && ad.is_deleted()) {
                continue;
            }
            let ev = json!({ "type": ad.data_type, "content": ad.content });
            match ad.room_id {
                None => out.account_data.push(ev),
                Some(room_id) => {
                    if let Some(room) = out.join.get_mut(&room_id) {
                        room.account_data.push(ev);
                    } else if joined_ids.contains(&room_id) {
                        // A joined room with nothing else new still reports its account data.
                        out.join.entry(room_id).or_default().account_data.push(ev);
                    }
                }
            }
        }

        // Presence of users we share rooms with.
        if filter.presence_enabled {
            out.presence = self.sync_presence(user, since, &out, &newly_joined).await?;
        }

        // To-device messages and device lists (per device).
        if let Some(device_id) = &req.device_id {
            let td_since = since.map_or(0, |s| s.to_device);
            if td_since > 0 {
                self.store
                    .delete_to_device_messages_up_to(user, device_id, td_since)
                    .await?;
            }
            let msgs = self
                .store
                .to_device_messages(user, device_id, td_since, now.to_device, 100)
                .await?;
            out.to_device = msgs
                .into_iter()
                .map(|m| json!({ "type": m.event_type, "sender": m.sender, "content": m.content }))
                .collect();
            out.one_time_key_counts = self.one_time_key_counts(user, device_id).await?;
            out.unused_fallback_key_types =
                self.unused_fallback_key_algorithms(user, device_id).await?;
        }
        if let Some(s) = since {
            let (changed, left) = self
                .device_list_updates(user, s, now, &memberships, &newly_joined)
                .await?;
            out.device_lists_changed = changed;
            out.device_lists_left = left;
        }

        Ok(out)
    }

    /// One room's delta. `leave_at` bounds the timeline for rooms the user left.
    #[allow(clippy::too_many_arguments)]
    async fn sync_room(
        &self,
        req: &SyncRequest,
        room_id: &RoomId,
        since: Option<SyncToken>,
        now: SyncToken,
        full_state: bool,
        leave_at: Option<i64>,
        newly_joined: bool,
        ignored: &HashSet<OwnedUserId>,
    ) -> Result<Option<RoomSync>> {
        let user = &req.user_id;
        let filter = &req.filter;
        let limit = filter.timeline_limit.min(1000);
        let mut room = RoomSync::default();

        // Timeline: newest `limit` matching events in (since, upper], oldest first.
        let upper = leave_at.unwrap_or(now.events);
        // A room joined inside this window shows its recent history from the start, and is
        // always marked limited so clients paginate (as the reference implementation does).
        let lower = if newly_joined {
            None
        } else {
            since.map(|s| s.events)
        };
        let mut events: Vec<StoredEvent> = Vec::new();
        let mut cursor = upper + 1;
        let mut exhausted = false;
        let mut scanned = 0usize;
        // Set when the topological window ended before the room's history did.
        let mut more_beyond = false;
        if lower.is_none() {
            // A first view of the room: the newest events in topological order, so a state
            // event that arrived late (on a fork) is reported in `state` rather than
            // displacing the recent timeline.
            let want = (limit * 3).clamp(50, 1000);
            let candidates = self
                .store
                .room_events_topological(room_id, upper, want)
                .await?;
            exhausted = candidates.len() < want;
            for ev in candidates {
                if filter.timeline_allows(&ev)
                    && (ev.pdu.is_state() || !ignored.contains(ev.pdu.sender()))
                {
                    events.push(ev);
                    if events.len() > limit {
                        break;
                    }
                }
            }
            more_beyond = !exhausted && events.len() <= limit;
            exhausted = true;
        }
        while events.len() <= limit && !exhausted && scanned < 5000 {
            let batch = self
                .store
                .room_events_in_range(
                    room_id,
                    cursor,
                    lower.map(|l| l + 1),
                    Direction::Backwards,
                    100,
                )
                .await?;
            if batch.is_empty() {
                break;
            }
            scanned += batch.len();
            cursor = batch.last().map_or(cursor, |e| e.stream_ordering);
            for ev in batch {
                if let Some(l) = lower
                    && ev.stream_ordering <= l
                {
                    exhausted = true;
                    break;
                }
                if filter.timeline_allows(&ev)
                    && (ev.pdu.is_state() || !ignored.contains(ev.pdu.sender()))
                {
                    events.push(ev);
                    if events.len() > limit {
                        break;
                    }
                }
            }
        }
        let mut limited = events.len() > limit || more_beyond;
        events.truncate(limit);
        events.reverse();
        // A gap (events arrived whose history we do not have) cuts the timeline: only the
        // events from the gap onwards are shown, marked limited so the client paginates.
        let gap = match lower {
            Some(l) if !events.is_empty() => self.store.gap_position_since(room_id, l).await?,
            _ => None,
        };
        if let Some(g) = gap {
            events.retain(|e| e.stream_ordering >= g);
            limited = true;
        }
        let timeline_start = events.first().map(|e| e.stream_ordering);
        room.limited = limited || newly_joined;
        if let Some(oldest) = events.iter().map(|e| e.stream_ordering).min() {
            // A first view that holds the room's whole history gets the sync position as
            // `prev_batch`, as the reference implementation does: `/members?at=` with it then
            // reads as "the members at the time of this sync" rather than "before the room".
            let token = if lower.is_none() && !room.limited {
                upper
            } else {
                oldest - 1
            };
            room.prev_batch = Some(super::rooms::timeline::StreamToken(token).to_string());
        }
        if let Some(device_id) = &req.device_id {
            for ev in &events {
                if ev.pdu.sender() == user
                    && let Some(txn) = self
                        .store
                        .get_event_txn_id(ev.pdu.event_id(), user, Some(device_id))
                        .await?
                {
                    room.txn_ids.insert(ev.pdu.event_id().to_owned(), txn);
                }
            }
        }
        // Our own leave (or a kick/rejection) of a room we never had state for is an outlier,
        // which the timeline query skips; the client still needs to see it.
        if leave_at.is_some()
            && !events.iter().any(|e| {
                e.pdu.event_type() == &ruma::events::TimelineEventType::RoomMember
                    && e.pdu.state_key() == Some(user.as_str())
            })
            && let Some(m) = self.store.get_membership(room_id, user).await?
            && let Some(ev) = self.store.get_event(&m.event_id).await?
            && ev.outlier
        {
            events.push(ev);
        }
        room.timeline = events;

        // State.
        let mut state: Vec<StoredEvent> = if full_state {
            let mut s = match leave_at {
                // A room we left: the state as of the leave event (which it includes).
                Some(leave_stream) => {
                    let leave = self
                        .store
                        .room_events_in_range(
                            room_id,
                            leave_stream + 1,
                            None,
                            Direction::Backwards,
                            1,
                        )
                        .await?;
                    match leave.first().and_then(|e| e.state_group) {
                        Some(g) => {
                            let ids: Vec<OwnedEventId> = self
                                .store
                                .state_at_group(g)
                                .await?
                                .into_iter()
                                .map(|e| e.event_id)
                                .collect();
                            self.store.get_events(&ids).await?
                        }
                        None => self.store.current_state_pdus(room_id).await?,
                    }
                }
                None => self.store.current_state_pdus(room_id).await?,
            };
            // Everything the timeline already carries is left out of the state block; the
            // rest is current state as of the timeline's end (state events may arrive with a
            // later stream position than events they precede topologically).
            let in_timeline: HashSet<OwnedEventId> = room
                .timeline
                .iter()
                .map(|e| e.pdu.event_id().to_owned())
                .collect();
            let _ = timeline_start;
            s.retain(|e| !in_timeline.contains(e.pdu.event_id()));
            s
        } else if limited && let (Some(s), Some(start)) = (since, timeline_start) {
            self.state_delta(room_id, s.events, start).await?
        } else {
            Vec::new()
        };
        if req.use_state_after {
            // State at the end of the timeline: everything for a full view, otherwise what
            // changed since the token (timeline events included, as the spec requires).
            let mut after: Vec<StoredEvent> = if full_state {
                match leave_at {
                    Some(_) => {
                        let mut s = state.clone();
                        s.extend(room.timeline.iter().filter(|e| e.pdu.is_state()).cloned());
                        s
                    }
                    None => self.store.current_state_pdus(room_id).await?,
                }
            } else if let Some(s) = since {
                self.state_changes_since(room_id, s.events).await?
            } else {
                Vec::new()
            };
            after.retain(|e| filter.state_allows(e));
            room.state_after = Some(after);
            state.clear();
        }
        state.retain(|e| filter.state_allows(e));
        if filter.lazy_load_members && !filter.include_redundant_members {
            let senders: HashSet<&UserId> = room.timeline.iter().map(|e| e.pdu.sender()).collect();
            state.retain(|e| {
                e.pdu.event_type() != &ruma::events::TimelineEventType::RoomMember
                    || e.pdu.state_key().is_some_and(|k| {
                        senders.iter().any(|s| s.as_str() == k) || k == user.as_str()
                    })
            });
        }
        room.state = state;

        // Ephemeral: receipts and typing (joined rooms only).
        if leave_at.is_none() {
            let rc_since = since.map_or(0, |s| s.receipts);
            let receipts = self.store.receipts_for_room(room_id, rc_since).await?;
            if !receipts.is_empty() {
                let content = receipts_content(receipts, user);
                room.ephemeral
                    .push(json!({ "type": "m.receipt", "content": content }));
            }
            let typing_since = since.map_or(-1, |s| s.typing);
            if since.is_none() || self.typing.changed_since(room_id, typing_since) {
                let users = self.typing.typing_users(room_id);
                if since.is_some() || !users.is_empty() {
                    room.ephemeral
                        .push(json!({ "type": "m.typing", "content": { "user_ids": users } }));
                }
            }

            // An idle room in an incremental sync: nothing to report, so skip the counting and
            // summary queries (most rooms are idle most of the time).
            if since.is_some()
                && !full_state
                && room.timeline.is_empty()
                && room.state.is_empty()
                && room.ephemeral.is_empty()
            {
                return Ok(None);
            }
            // Unread notification counts.
            match self.unread_counts(room_id, user, upper).await {
                Ok(counts) => {
                    if filter.unread_thread_notifications {
                        room.unread_notifications = counts.main.notification_count;
                        room.unread_highlights = counts.main.highlight_count;
                        room.unread_threads = counts
                            .threads
                            .into_iter()
                            .map(|(k, c)| (k, (c.notification_count, c.highlight_count)))
                            .collect();
                    } else {
                        room.unread_notifications = counts.total.notification_count;
                        room.unread_highlights = counts.total.highlight_count;
                    }
                }
                Err(e) => tracing::warn!(%room_id, error = %e, "unread counts failed"),
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
                        m.user_id != *user
                            && matches!(
                                m.membership,
                                MembershipState::Join | MembershipState::Invite
                            )
                    })
                    .take(5)
                    .map(|m| m.user_id.clone())
                    .collect();
            }
        }

        let nothing_new = since.is_some()
            && !full_state
            && leave_at.is_none()
            && room.timeline.is_empty()
            && room.state.is_empty()
            && room.state_after.as_ref().is_none_or(Vec::is_empty)
            && room.ephemeral.is_empty();
        if nothing_new {
            return Ok(None);
        }
        Ok(Some(room))
    }

    /// Presence events for the sync: everyone sharing a room (initial / newly joined rooms),
    /// otherwise those whose presence changed since the token.
    async fn sync_presence(
        &self,
        user: &UserId,
        since: Option<SyncToken>,
        out: &SyncResult,
        newly_joined: &[OwnedRoomId],
    ) -> Result<Vec<Value>> {
        let mut interesting: HashSet<OwnedUserId> = HashSet::new();
        let rooms: Vec<OwnedRoomId> = if since.is_none() {
            out.join.keys().cloned().collect()
        } else {
            newly_joined.to_vec()
        };
        for room in &rooms {
            for m in self.store.members(room, Some("join")).await? {
                interesting.insert(m.user_id);
            }
        }
        let mut events = Vec::new();
        let mut emitted: HashSet<OwnedUserId> = HashSet::new();
        for u in &interesting {
            if let Some(e) = self.presence.get(u) {
                events.push(presence_event(u, &e));
                emitted.insert(u.clone());
            }
        }
        if let Some(s) = since {
            // Members who joined our rooms in the window.
            let my_rooms = self.store.rooms_for_user(user, "join").await?;
            for m in self
                .store
                .memberships_changed_in_rooms(&my_rooms, s.events)
                .await?
            {
                if m.membership == MembershipState::Join
                    && m.user_id != *user
                    && !emitted.contains(&m.user_id)
                    && let Some(e) = self.presence.get(&m.user_id)
                {
                    events.push(presence_event(&m.user_id, &e));
                    emitted.insert(m.user_id.clone());
                }
            }
            // Changed presence of anyone we share a room with (only when any presence changed
            // at all since the token: the scan over every room's members is not free).
            if self.presence.position() <= s.presence {
                return Ok(events);
            }
            let my_rooms = self.store.rooms_for_user(user, "join").await?;
            let mut shared: HashSet<OwnedUserId> = HashSet::new();
            for room in &my_rooms {
                for m in self.store.members(room, Some("join")).await? {
                    shared.insert(m.user_id);
                }
            }
            for u in shared {
                if emitted.contains(&u) {
                    continue;
                }
                if let Some(e) = self.presence.get(&u)
                    && e.stream > s.presence
                {
                    events.push(presence_event(&u, &e));
                }
            }
        }
        Ok(events)
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

    /// The stripped-state subset of a room's current state as full events, for
    /// `invite_room_state` / `knock_room_state` over federation.
    pub async fn stripped_state_full(&self, room_id: &RoomId) -> Result<Vec<Value>> {
        let state = self.store.current_state_pdus(room_id).await?;
        Ok(state
            .iter()
            .filter(|se| is_stripped_state_type(se.pdu.state_tuple().map(|(t, _)| t).as_ref()))
            .map(|se| se.pdu.to_client_json())
            .collect())
    }

    /// Stripped state for invites/knocks: a fixed subset of state in client format without
    /// event IDs, plus the inviter's membership.
    pub async fn stripped_state(
        &self,
        room_id: &RoomId,
        invitee: Option<&UserId>,
    ) -> Result<Vec<Value>> {
        // For an invite or knock received over federation the remote server sent us the
        // stripped state; our own copy of the room state is incomplete.
        let own_event = match invitee {
            Some(u) => match self.store.get_membership(room_id, u).await? {
                Some(m) => self.store.get_event(&m.event_id).await?,
                None => None,
            },
            None => None,
        };
        if let Some(u) = invitee
            && own_event.as_ref().is_some_and(|e| e.outlier)
            && let Some(mut stored) = self.store.get_invite_state(room_id, u).await?
        {
            let has_own_membership = stored.iter().any(|e| {
                e.get("type").and_then(Value::as_str) == Some("m.room.member")
                    && e.get("state_key").and_then(Value::as_str) == Some(u.as_str())
            });
            if !has_own_membership && let Some(ev) = own_event {
                stored.push(ev.pdu.to_client_json());
            }
            // Servers may send full events; clients get the stripped form.
            for v in &mut stored {
                if let Value::Object(o) = v {
                    o.retain(|k, _| {
                        matches!(k.as_str(), "type" | "state_key" | "content" | "sender")
                    });
                }
            }
            return Ok(stored);
        }
        let state = self.store.current_state_pdus(room_id).await?;
        let inviter = match invitee {
            Some(u) => self
                .store
                .get_membership(room_id, u)
                .await?
                .map(|m| m.sender),
            None => None,
        };
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
                    invitee.is_some_and(|u| u.as_str() == key)
                        || inviter.as_ref().is_some_and(|i| i.as_str() == key)
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
        Ok(out)
    }
}

impl Services {
    /// The users listed in the user's `m.ignored_user_list` account data.
    pub async fn ignored_users(&self, user: &UserId) -> Result<HashSet<OwnedUserId>> {
        let Some(data) = self
            .store
            .get_account_data(user, None, "m.ignored_user_list")
            .await?
        else {
            return Ok(HashSet::new());
        };
        Ok(data
            .content
            .get("ignored_users")
            .and_then(Value::as_object)
            .map(|m| {
                m.keys()
                    .filter_map(|k| OwnedUserId::try_from(k.as_str()).ok())
                    .collect()
            })
            .unwrap_or_default())
    }
}

/// Event types that belong in stripped state.
pub fn is_stripped_state_type(ty: Option<&StateEventType>) -> bool {
    matches!(
        ty,
        Some(
            StateEventType::RoomCreate
                | StateEventType::RoomJoinRules
                | StateEventType::RoomName
                | StateEventType::RoomAvatar
                | StateEventType::RoomCanonicalAlias
                | StateEventType::RoomTopic
                | StateEventType::RoomEncryption
        )
    )
}

/// The content of an `m.receipt` ephemeral event for a room's receipts, as seen by `user`
/// (other users' private receipts are left out).
pub fn receipts_content(
    receipts: Vec<tessera_store::Receipt>,
    user: &UserId,
) -> serde_json::Map<String, Value> {
    let mut content: serde_json::Map<String, Value> = serde_json::Map::new();
    for r in receipts {
        if r.receipt_type == "m.read.private" && r.user_id != *user {
            continue;
        }
        let by_type = content
            .entry(r.event_id.to_string())
            .or_insert_with(|| json!({}));
        let users = by_type.as_object_mut().and_then(|o| {
            o.entry(r.receipt_type.clone())
                .or_insert_with(|| json!({}))
                .as_object_mut()
        });
        if let Some(users) = users {
            let mut data = r.data.clone();
            if let Some(t) = &r.thread_id {
                data["thread_id"] = json!(t);
            }
            // MSC4102: an unthreaded receipt takes precedence over a threaded one.
            let existing_unthreaded = users
                .get(r.user_id.as_str())
                .is_some_and(|d| d.get("thread_id").is_none());
            if !(existing_unthreaded && r.thread_id.is_some()) {
                users.insert(r.user_id.to_string(), data);
            }
        }
    }
    content
}

impl Services {
    /// Device-list `changed`/`left` for an incremental sync between `s` and `now`.
    pub(crate) async fn device_list_updates(
        &self,
        user: &UserId,
        s: SyncToken,
        now: SyncToken,
        memberships: &[Membership],
        newly_joined: &[OwnedRoomId],
    ) -> Result<(Vec<OwnedUserId>, Vec<OwnedUserId>)> {
        let (mut changed, mut left) = self
            .device_list_changes_for(user, s.device_lists, now.device_lists)
            .await?;
        // Users who joined or left rooms we are in during the window.
        let all_my_rooms = self.store.rooms_for_user(user, "join").await?;
        for m in self
            .store
            .memberships_changed_in_rooms(&all_my_rooms, s.events)
            .await?
        {
            if m.user_id == *user || m.stream_ordering > now.events {
                continue;
            }
            match m.membership {
                MembershipState::Join => {
                    if !changed.contains(&m.user_id) {
                        changed.push(m.user_id.clone());
                    }
                }
                MembershipState::Leave | MembershipState::Ban => {
                    let still_shared = self
                        .store
                        .rooms_for_user(&m.user_id, "join")
                        .await?
                        .iter()
                        .any(|r| all_my_rooms.contains(r));
                    if !still_shared && !left.contains(&m.user_id) {
                        left.push(m.user_id.clone());
                    }
                }
                _ => {}
            }
        }
        // Rooms we left in the window: their members, unless still shared elsewhere.
        for m in memberships {
            if !matches!(m.membership, MembershipState::Leave | MembershipState::Ban)
                || m.stream_ordering <= s.events
                || m.stream_ordering > now.events
            {
                continue;
            }
            for other in self.store.members(&m.room_id, Some("join")).await? {
                if other.user_id == *user || left.contains(&other.user_id) {
                    continue;
                }
                let still_shared = self
                    .store
                    .rooms_for_user(&other.user_id, "join")
                    .await?
                    .iter()
                    .any(|r| all_my_rooms.contains(r));
                if !still_shared {
                    left.push(other.user_id);
                }
            }
        }
        // Members of newly joined rooms need their device lists fetched.
        for room in newly_joined {
            for m in self.store.members(room, Some("join")).await? {
                if !changed.contains(&m.user_id) {
                    changed.push(m.user_id);
                }
            }
        }
        Ok((changed, left))
    }
}
