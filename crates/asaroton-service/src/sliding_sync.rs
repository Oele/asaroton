//! Simplified Sliding Sync (MSC4186).
//!
//! The client describes lists of rooms (sorted by recency, filtered, windowed by ranges) and
//! explicit room subscriptions. The server remembers, per connection, what it has sent, so
//! every response after the first carries only what changed since the last position.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    hash::{DefaultHasher, Hash, Hasher},
    time::Duration,
};

use asaroton_store::{Direction, Membership, StateGroupId, StoredEvent};
use dashmap::DashMap;
use ruma::{
    OwnedDeviceId, OwnedEventId, OwnedRoomId, OwnedUserId, RoomId, UserId,
    events::{StateEventType, TimelineEventType, room::member::MembershipState},
    presence::PresenceState,
};
use serde_json::{Value, json};

use crate::{
    Error, Result, Services,
    notifier::{self, Notification},
    rooms::timeline::StreamToken,
    sync::{SyncToken, receipts_content},
};

/// Event types whose arrival moves a room up the list (`bump_stamp`).
const BUMP_EVENT_TYPES: &[&str] = &[
    "m.room.create",
    "m.room.message",
    "m.room.encrypted",
    "m.sticker",
    "m.call.invite",
    "m.poll.start",
    "m.beacon_info",
];
const MAX_TIMELINE_LIMIT: usize = 1000;
const DEFAULT_TO_DEVICE_LIMIT: usize = 100;

// ------------------------------------------------------------------ requests

#[derive(Clone, Debug, Default)]
pub struct ListRequest {
    pub ranges: Vec<(u64, u64)>,
    pub required_state: Vec<(String, String)>,
    pub timeline_limit: usize,
    pub filters: Option<ListFilters>,
}

#[derive(Clone, Debug, Default)]
pub struct ListFilters {
    pub is_dm: Option<bool>,
    pub is_encrypted: Option<bool>,
    pub is_invite: Option<bool>,
    /// `None` entries stand for rooms without a type.
    pub room_types: Option<Vec<Option<String>>>,
    pub not_room_types: Vec<Option<String>>,
}

#[derive(Clone, Debug, Default)]
pub struct RoomSubscriptionRequest {
    pub required_state: Vec<(String, String)>,
    pub timeline_limit: usize,
}

/// Which rooms an extension applies to.
#[derive(Clone, Debug)]
pub enum ScopeRoom {
    AllSubscribed,
    Room(OwnedRoomId),
}

#[derive(Clone, Debug, Default)]
pub struct ExtensionScope {
    pub lists: Option<Vec<String>>,
    pub rooms: Option<Vec<ScopeRoom>>,
}

#[derive(Clone, Debug, Default)]
pub struct ToDeviceRequest {
    pub since: Option<String>,
    pub limit: usize,
}

#[derive(Clone, Debug, Default)]
pub struct ExtensionsRequest {
    pub to_device: Option<ToDeviceRequest>,
    pub e2ee: bool,
    pub account_data: Option<ExtensionScope>,
    pub receipts: Option<ExtensionScope>,
    pub typing: Option<ExtensionScope>,
    /// Thread subscriptions (MSC4308), with the page limit.
    pub thread_subscriptions: Option<usize>,
}

#[derive(Clone, Debug)]
pub struct SlidingSyncRequest {
    pub user_id: OwnedUserId,
    pub device_id: Option<OwnedDeviceId>,
    pub conn_id: Option<String>,
    pub pos: Option<String>,
    pub timeout: Duration,
    pub set_presence: PresenceState,
    pub lists: BTreeMap<String, ListRequest>,
    pub room_subscriptions: BTreeMap<OwnedRoomId, RoomSubscriptionRequest>,
    pub extensions: ExtensionsRequest,
}

// ------------------------------------------------------------------ results

#[derive(Clone, Debug)]
pub struct Hero {
    pub user_id: OwnedUserId,
    pub name: Option<String>,
    pub avatar: Option<String>,
}

#[derive(Clone, Debug)]
pub struct RoomResult {
    pub membership: MembershipState,
    pub initial: bool,
    pub name: Option<String>,
    pub avatar: Option<String>,
    pub is_dm: bool,
    pub invite_state: Option<Vec<Value>>,
    pub required_state: Vec<StoredEvent>,
    pub timeline: Vec<StoredEvent>,
    pub prev_batch: Option<String>,
    pub limited: bool,
    /// The timeline holds older events again because the client raised its `timeline_limit`.
    pub expanded_timeline: bool,
    pub joined_count: u64,
    pub invited_count: u64,
    pub num_live: u64,
    pub bump_stamp: u64,
    pub heroes: Vec<Hero>,
    pub notification_count: u64,
    pub highlight_count: u64,
    pub txn_ids: HashMap<OwnedEventId, String>,
}

#[derive(Clone, Debug, Default)]
pub struct E2eeResult {
    pub changed: Vec<OwnedUserId>,
    pub left: Vec<OwnedUserId>,
    pub one_time_key_counts: BTreeMap<String, u64>,
    pub unused_fallback_key_types: Vec<String>,
}

#[derive(Clone, Debug, Default)]
pub struct AccountDataResult {
    pub global: Vec<Value>,
    pub rooms: BTreeMap<OwnedRoomId, Vec<Value>>,
}

#[derive(Clone, Debug, Default)]
pub struct ExtensionsResult {
    /// `(next_batch, events)`.
    pub to_device: Option<(String, Vec<Value>)>,
    pub e2ee: Option<E2eeResult>,
    pub account_data: Option<AccountDataResult>,
    pub receipts: BTreeMap<OwnedRoomId, Value>,
    pub typing: BTreeMap<OwnedRoomId, Vec<OwnedUserId>>,
    pub thread_subscriptions: Option<crate::thread_subscriptions::ThreadSubscriptionChanges>,
}

impl ExtensionsResult {
    fn is_quiet(&self) -> bool {
        self.to_device.as_ref().is_none_or(|(_, e)| e.is_empty())
            && self
                .e2ee
                .as_ref()
                .is_none_or(|e| e.changed.is_empty() && e.left.is_empty())
            && self
                .account_data
                .as_ref()
                .is_none_or(|a| a.global.is_empty() && a.rooms.is_empty())
            && self.receipts.is_empty()
            && self.typing.is_empty()
            && self
                .thread_subscriptions
                .as_ref()
                .is_none_or(crate::thread_subscriptions::ThreadSubscriptionChanges::is_empty)
    }
}

#[derive(Clone, Debug, Default)]
pub struct SlidingSyncResult {
    pub pos: String,
    pub lists: BTreeMap<String, u64>,
    pub rooms: BTreeMap<OwnedRoomId, RoomResult>,
    pub extensions: ExtensionsResult,
    /// Nothing changed since the previous position.
    pub quiet: bool,
}

// ------------------------------------------------------------------ connections

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct ConnKey {
    user: OwnedUserId,
    device: Option<OwnedDeviceId>,
    conn_id: String,
}

/// What a connection has been told about one room.
#[derive(Clone, Debug)]
struct RoomTrack {
    timeline_upto: i64,
    required_state: BTreeSet<(String, String)>,
    timeline_limit: usize,
    membership: MembershipState,
    notification_count: u64,
    highlight_count: u64,
    /// Fingerprint of the stripped state last sent for an invite, so the invite is sent again
    /// only when it changes. Without this an invited room was part of every response and the
    /// request never long-polled.
    invite: Option<u64>,
}

/// An order-independent fingerprint of an invite's stripped state.
fn invite_fingerprint(stripped: &[Value]) -> u64 {
    let mut events: Vec<String> = stripped.iter().map(Value::to_string).collect();
    events.sort_unstable();
    let mut hasher = DefaultHasher::new();
    events.hash(&mut hasher);
    hasher.finish()
}

#[derive(Clone, Debug)]
struct ConnState {
    seq: u64,
    token: SyncToken,
    rooms: HashMap<OwnedRoomId, RoomTrack>,
    list_counts: BTreeMap<String, u64>,
}

/// The last state the client acknowledged, and the one produced by the latest response (which
/// the client may still fail to receive, in which case it retries with the older position).
#[derive(Debug, Default)]
struct Connection {
    confirmed: Option<ConnState>,
    pending: Option<ConnState>,
}

/// Per-connection state of every sliding sync client.
#[derive(Debug, Default)]
pub struct SlidingSyncConnections {
    conns: DashMap<ConnKey, Connection>,
}

impl SlidingSyncConnections {
    fn resolve(&self, key: &ConnKey, pos: Option<&str>) -> Result<Option<ConnState>> {
        let mut entry = self.conns.entry(key.clone()).or_default();
        let conn = &mut *entry;
        match pos {
            None => {
                conn.confirmed = None;
                conn.pending = None;
                Ok(None)
            }
            Some(p) => {
                let seq: u64 = p.parse().map_err(|_| unknown_pos())?;
                if conn.pending.as_ref().is_some_and(|s| s.seq == seq) {
                    conn.confirmed = conn.pending.take();
                } else if conn.confirmed.as_ref().is_some_and(|s| s.seq == seq) {
                    conn.pending = None;
                } else {
                    return Err(unknown_pos());
                }
                Ok(conn.confirmed.clone())
            }
        }
    }

    fn store_pending(&self, key: &ConnKey, state: ConnState) {
        if self.conns.len() > 10_000 {
            self.conns.clear();
        }
        self.conns.entry(key.clone()).or_default().pending = Some(state);
    }
}

fn unknown_pos() -> Error {
    Error::matrix(
        ruma::api::error::ErrorKind::UnknownPos,
        http::StatusCode::BAD_REQUEST,
        "Unknown or expired sliding sync position.",
    )
}

// ------------------------------------------------------------------ selection

struct RoomFacts {
    encrypted: bool,
    room_type: Option<String>,
}

impl ListFilters {
    fn matches(&self, m: &Membership, facts: &RoomFacts, is_dm: bool) -> bool {
        if self.is_dm.is_some_and(|v| v != is_dm) {
            return false;
        }
        if self.is_encrypted.is_some_and(|v| v != facts.encrypted) {
            return false;
        }
        if self
            .is_invite
            .is_some_and(|v| v != (m.membership == MembershipState::Invite))
        {
            return false;
        }
        if let Some(types) = &self.room_types
            && !types.contains(&facts.room_type)
        {
            return false;
        }
        if self.not_room_types.contains(&facts.room_type) {
            return false;
        }
        true
    }
}

/// A room to report, with the union of every list's and subscription's demands on it.
struct RoomSelection {
    room_id: OwnedRoomId,
    membership: Membership,
    required_state: BTreeSet<(String, String)>,
    timeline_limit: usize,
    lists: Vec<String>,
    subscribed: bool,
}

impl ExtensionScope {
    fn applies(&self, room: &RoomSelection) -> bool {
        if self.lists.is_none() && self.rooms.is_none() {
            return true;
        }
        if let Some(l) = &self.lists
            && room.lists.iter().any(|n| l.contains(n))
        {
            return true;
        }
        if let Some(r) = &self.rooms {
            for e in r {
                match e {
                    ScopeRoom::AllSubscribed if room.subscribed => return true,
                    ScopeRoom::Room(id) if *id == room.room_id => return true,
                    _ => {}
                }
            }
        }
        false
    }
}

fn select(
    selections: &mut BTreeMap<OwnedRoomId, RoomSelection>,
    m: &Membership,
    required_state: &[(String, String)],
    timeline_limit: usize,
    list: Option<&str>,
    subscribed: bool,
) {
    let e = selections
        .entry(m.room_id.clone())
        .or_insert_with(|| RoomSelection {
            room_id: m.room_id.clone(),
            membership: m.clone(),
            required_state: BTreeSet::new(),
            timeline_limit: 0,
            lists: Vec::new(),
            subscribed: false,
        });
    e.required_state.extend(required_state.iter().cloned());
    e.timeline_limit = e.timeline_limit.max(timeline_limit);
    if let Some(l) = list {
        e.lists.push(l.to_owned());
    }
    e.subscribed |= subscribed;
}

/// Whether a state event is wanted by a `required_state` list (`*`, `$ME` and `$LAZY`
/// wildcards included).
fn required_state_matches(
    patterns: &BTreeSet<(String, String)>,
    ev: &StoredEvent,
    user: &UserId,
    senders: &HashSet<OwnedUserId>,
) -> bool {
    let Some((ty, key)) = ev.pdu.state_tuple() else {
        return false;
    };
    let ty = ty.to_string();
    patterns.iter().any(|(pt, pk)| {
        (pt == "*" || *pt == ty)
            && match pk.as_str() {
                "*" => true,
                "$ME" => key == user.as_str(),
                "$LAZY" => {
                    ty == "m.room.member"
                        && (key == user.as_str() || senders.iter().any(|s| s.as_str() == key))
                }
                other => other == key,
            }
    })
}

// ------------------------------------------------------------------ the sync

impl Services {
    /// One sliding sync request: resolve the connection position, compute the response, long
    /// poll when nothing changed, and remember what was sent.
    pub async fn sliding_sync(&self, req: SlidingSyncRequest) -> Result<SlidingSyncResult> {
        // `offline` on a sync means "do not affect my presence" (a background sync).
        if req.set_presence != PresenceState::Offline {
            self.set_presence(
                &req.user_id,
                req.device_id.as_deref(),
                req.set_presence.clone(),
                None,
            )
            .await;
        }
        self.presence.touch(&req.user_id);

        let key = ConnKey {
            user: req.user_id.clone(),
            device: req.device_id.clone(),
            conn_id: req.conn_id.clone().unwrap_or_default(),
        };
        let base = self.sliding_sync.resolve(&key, req.pos.as_deref())?;
        let mut rx = self.notifier.subscribe();
        let (mut result, mut state) = self.compute_sliding_sync(&req, base.as_ref()).await?;
        if base.is_some() && result.quiet && !req.timeout.is_zero() {
            let user_id = req.user_id.clone();
            let rooms: Vec<OwnedRoomId> = self
                .store
                .memberships_for_user(&user_id)
                .await?
                .into_iter()
                .map(|m| m.room_id)
                .collect();
            let deadline = tokio::time::Instant::now() + req.timeout;
            // Wake on anything that might matter; unrelated wake-ups just loop until the
            // deadline.
            for _ in 0..20 {
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                if remaining.is_zero() {
                    break;
                }
                let woke = notifier::wait_for(&mut rx, remaining, |n| match n {
                    Notification::RoomEvent { room_id, .. }
                    | Notification::RoomEphemeral { room_id } => rooms.contains(room_id),
                    Notification::User { user_id: u } => *u == user_id,
                    Notification::DeviceLists { .. } | Notification::Presence { .. } => true,
                })
                .await;
                if !woke {
                    break;
                }
                (result, state) = self.compute_sliding_sync(&req, base.as_ref()).await?;
                if !result.quiet {
                    break;
                }
            }
        }
        self.sliding_sync.store_pending(&key, state);
        Ok(result)
    }

    async fn compute_sliding_sync(
        &self,
        req: &SlidingSyncRequest,
        base: Option<&ConnState>,
    ) -> Result<(SlidingSyncResult, ConnState)> {
        let now = self.current_token().await?;
        let since = base.map(|b| b.token);
        let user = &req.user_id;
        // Invites are hidden while the user refuses all invites.
        let invites_blocked = self.invites_blocked(user).await?;
        let memberships: Vec<Membership> = self
            .store
            .memberships_for_user(user)
            .await?
            .into_iter()
            .filter(|m| !m.forgotten)
            .filter(|m| !(invites_blocked && m.membership == MembershipState::Invite))
            .collect();
        let ignored = self.ignored_users(user).await?;
        let direct = self.direct_rooms(user).await?;

        // Recency: the newest event in each joined room, and the newest "bump" event.
        let joined_ids: Vec<OwnedRoomId> = memberships
            .iter()
            .filter(|m| m.membership == MembershipState::Join)
            .map(|m| m.room_id.clone())
            .collect();
        let latest = self
            .store
            .latest_stream_orderings(&joined_ids, None)
            .await?;
        let bump_types: Vec<String> = BUMP_EVENT_TYPES.iter().map(|t| (*t).to_owned()).collect();
        let bumps = self
            .store
            .latest_stream_orderings(&joined_ids, Some(&bump_types))
            .await?;
        let recency = |m: &Membership| -> i64 {
            if m.membership == MembershipState::Join {
                latest.get(&m.room_id).copied().unwrap_or(m.stream_ordering)
            } else {
                m.stream_ordering
            }
        };
        let mut candidates: Vec<&Membership> = memberships
            .iter()
            .filter(|m| match m.membership {
                MembershipState::Join => true,
                MembershipState::Invite => !ignored.contains(&m.sender),
                _ => false,
            })
            .collect();
        candidates.sort_by(|a, b| {
            recency(b)
                .cmp(&recency(a))
                .then_with(|| a.room_id.cmp(&b.room_id))
        });
        let mut facts: HashMap<OwnedRoomId, RoomFacts> = HashMap::new();
        for m in &candidates {
            facts.insert(m.room_id.clone(), self.room_facts(&m.room_id).await?);
        }

        // Lists, subscriptions, and rooms left since the last position (reported once more so
        // the client can drop them).
        let mut selections: BTreeMap<OwnedRoomId, RoomSelection> = BTreeMap::new();
        let mut list_counts: BTreeMap<String, u64> = BTreeMap::new();
        for (name, list) in &req.lists {
            let filtered: Vec<&Membership> = candidates
                .iter()
                .copied()
                .filter(|m| {
                    list.filters.as_ref().is_none_or(|f| {
                        f.matches(m, &facts[&m.room_id], direct.contains(&m.room_id))
                    })
                })
                .collect();
            list_counts.insert(name.clone(), filtered.len() as u64);
            for (start, end) in &list.ranges {
                let start = usize::try_from(*start).unwrap_or(usize::MAX);
                let end = usize::try_from(*end).unwrap_or(usize::MAX);
                if end < start {
                    continue;
                }
                let take = end.saturating_sub(start).saturating_add(1);
                for m in filtered.iter().skip(start).take(take) {
                    select(
                        &mut selections,
                        m,
                        &list.required_state,
                        list.timeline_limit,
                        Some(name),
                        false,
                    );
                }
            }
        }
        for (room_id, sub) in &req.room_subscriptions {
            if let Some(m) = memberships.iter().find(|m| m.room_id == *room_id) {
                select(
                    &mut selections,
                    m,
                    &sub.required_state,
                    sub.timeline_limit,
                    None,
                    true,
                );
            }
        }
        if let (Some(s), Some(b)) = (since, base) {
            for m in &memberships {
                if matches!(m.membership, MembershipState::Leave | MembershipState::Ban)
                    && m.stream_ordering > s.events
                    && !selections.contains_key(&m.room_id)
                    && let Some(t) = b.rooms.get(&m.room_id)
                {
                    let rs: Vec<(String, String)> = t.required_state.iter().cloned().collect();
                    select(
                        &mut selections,
                        m,
                        &rs,
                        t.timeline_limit.max(1),
                        None,
                        false,
                    );
                }
            }
        }

        // Rooms.
        let mut rooms: BTreeMap<OwnedRoomId, RoomResult> = BTreeMap::new();
        let mut tracks: HashMap<OwnedRoomId, RoomTrack> =
            base.map(|b| b.rooms.clone()).unwrap_or_default();
        for (room_id, sel) in &selections {
            let track = base.and_then(|b| b.rooms.get(room_id));
            let bump = bumps
                .get(room_id)
                .or_else(|| latest.get(room_id))
                .copied()
                .unwrap_or(sel.membership.stream_ordering);
            let r = self
                .sliding_room(
                    req,
                    sel,
                    track,
                    now,
                    since,
                    &ignored,
                    direct.contains(room_id),
                    bump,
                )
                .await?;
            let invite = r.invite_state.as_deref().map(invite_fingerprint);
            let unchanged = track.is_some_and(|t| {
                !r.initial
                    && r.timeline.is_empty()
                    && r.required_state.is_empty()
                    && t.invite == invite
                    && t.membership == sel.membership.membership
                    && t.notification_count == r.notification_count
                    && t.highlight_count == r.highlight_count
            });
            tracks.insert(
                room_id.clone(),
                RoomTrack {
                    timeline_upto: now.events,
                    required_state: sel.required_state.clone(),
                    // The longest timeline the connection was given, so that dropping a
                    // subscription and taking it again does not send the same events twice.
                    timeline_limit: sel
                        .timeline_limit
                        .max(track.map_or(0, |t| t.timeline_limit)),
                    membership: sel.membership.membership.clone(),
                    notification_count: r.notification_count,
                    highlight_count: r.highlight_count,
                    invite,
                },
            );
            if !unchanged {
                rooms.insert(room_id.clone(), r);
            }
        }

        // Extensions.
        let mut ext = ExtensionsResult::default();
        let newly_joined: Vec<OwnedRoomId> = since
            .map(|s| {
                memberships
                    .iter()
                    .filter(|m| {
                        m.membership == MembershipState::Join && m.stream_ordering > s.events
                    })
                    .map(|m| m.room_id.clone())
                    .collect()
            })
            .unwrap_or_default();
        if let (Some(td), Some(device_id)) = (&req.extensions.to_device, &req.device_id) {
            let since_td: i64 = td
                .since
                .as_deref()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
            if since_td > 0 {
                self.store
                    .delete_to_device_messages_up_to(user, device_id, since_td)
                    .await?;
            }
            let limit = if td.limit == 0 {
                DEFAULT_TO_DEVICE_LIMIT
            } else {
                td.limit.min(MAX_TIMELINE_LIMIT)
            };
            let msgs = self
                .store
                .to_device_messages(user, device_id, since_td, now.to_device, limit)
                .await?;
            let next = if msgs.len() >= limit {
                msgs.last().map_or(now.to_device, |m| m.stream_id)
            } else {
                now.to_device
            };
            let events = msgs
                .into_iter()
                .map(|m| json!({ "type": m.event_type, "sender": m.sender, "content": m.content }))
                .collect();
            ext.to_device = Some((next.to_string(), events));
        }
        if req.extensions.e2ee
            && let Some(device_id) = &req.device_id
        {
            let (changed, left) = match since {
                Some(s) => {
                    let all_my_rooms = self.store.rooms_for_user(user, "join").await?;
                    let changed_in_my_rooms = self
                        .store
                        .memberships_changed_in_rooms(&all_my_rooms, s.events)
                        .await?;
                    self.device_list_updates(
                        user,
                        s,
                        now,
                        &memberships,
                        &newly_joined,
                        &all_my_rooms,
                        &changed_in_my_rooms,
                    )
                    .await?
                }
                None => (Vec::new(), Vec::new()),
            };
            ext.e2ee = Some(E2eeResult {
                changed,
                left,
                one_time_key_counts: self.one_time_key_counts(user, device_id).await?,
                unused_fallback_key_types: self
                    .unused_fallback_key_algorithms(user, device_id)
                    .await?,
            });
        }
        if let Some(scope) = &req.extensions.account_data {
            let ad_since = since.map_or(0, |s| s.account_data);
            let mut global = Vec::new();
            let mut per_room: BTreeMap<OwnedRoomId, Vec<Value>> = BTreeMap::new();
            let pr_stream = self.push_rules_stream_id(user).await?;
            if since.is_none() || (pr_stream > ad_since && pr_stream <= now.account_data) {
                global.push(
                    json!({ "type": "m.push_rules", "content": self.push_rules_account_data(user).await? }),
                );
            }
            // Rooms sent for the first time get all their account data, others the changes.
            let initial_rooms: HashSet<&OwnedRoomId> = rooms
                .iter()
                .filter(|(_, r)| r.initial)
                .map(|(id, _)| id)
                .collect();
            let from = if initial_rooms.is_empty() {
                ad_since
            } else {
                0
            };
            for ad in self.store.account_data_since(user, from).await? {
                if ad.stream_id > now.account_data {
                    continue;
                }
                // A deletion (MSC3391) is only news to a connection that saw the data.
                let fresh = ad.stream_id > ad_since && !(ad_since == 0 && ad.is_deleted());
                let ev = json!({ "type": ad.data_type, "content": ad.content });
                match &ad.room_id {
                    None => {
                        if fresh {
                            global.push(ev);
                        }
                    }
                    Some(r) => {
                        let Some(sel) = selections.get(r) else {
                            continue;
                        };
                        if !scope.applies(sel) {
                            continue;
                        }
                        if fresh || (initial_rooms.contains(r) && !ad.is_deleted()) {
                            per_room.entry(r.clone()).or_default().push(ev);
                        }
                    }
                }
            }
            ext.account_data = Some(AccountDataResult {
                global,
                rooms: per_room,
            });
        }
        if let Some(limit) = req.extensions.thread_subscriptions {
            let changes = self
                .thread_subscription_changes(
                    user,
                    since.map(|s| s.thread_subscriptions),
                    Some(now.thread_subscriptions + 1),
                    limit,
                    since.is_none(),
                )
                .await?;
            ext.thread_subscriptions = Some(changes);
        }
        if let Some(scope) = &req.extensions.receipts {
            for (room_id, sel) in &selections {
                if sel.membership.membership != MembershipState::Join || !scope.applies(sel) {
                    continue;
                }
                let initial = base.is_none_or(|b| !b.rooms.contains_key(room_id));
                let receipts: Vec<asaroton_store::Receipt> = if initial {
                    // Receipts for the events the client is being shown.
                    let ids: HashSet<OwnedEventId> = rooms
                        .get(room_id)
                        .map(|r| {
                            r.timeline
                                .iter()
                                .map(|e| e.pdu.event_id().to_owned())
                                .collect()
                        })
                        .unwrap_or_default();
                    self.store
                        .receipts_for_room(room_id, 0)
                        .await?
                        .into_iter()
                        .filter(|r| ids.contains(&r.event_id))
                        .collect()
                } else {
                    self.store
                        .receipts_for_room(room_id, since.map_or(0, |s| s.receipts))
                        .await?
                };
                let content = receipts_content(receipts, user);
                if !content.is_empty() {
                    ext.receipts.insert(
                        room_id.clone(),
                        json!({ "type": "m.receipt", "room_id": room_id, "content": content }),
                    );
                }
            }
        }
        if let Some(scope) = &req.extensions.typing {
            for (room_id, sel) in &selections {
                if sel.membership.membership != MembershipState::Join || !scope.applies(sel) {
                    continue;
                }
                let initial = base.is_none_or(|b| !b.rooms.contains_key(room_id));
                let changed =
                    initial || since.is_none_or(|s| self.typing.changed_since(room_id, s.typing));
                if !changed {
                    continue;
                }
                let users = self.typing.typing_users(room_id);
                if initial && users.is_empty() {
                    continue;
                }
                ext.typing.insert(room_id.clone(), users);
            }
        }

        let quiet = base.is_some_and(|b| b.list_counts == list_counts)
            && rooms.is_empty()
            && ext.is_quiet();
        let seq = base.map_or(1, |b| b.seq + 1);
        let result = SlidingSyncResult {
            pos: seq.to_string(),
            lists: list_counts.clone(),
            rooms,
            extensions: ext,
            quiet,
        };
        let state = ConnState {
            seq,
            token: now,
            rooms: tracks,
            list_counts,
        };
        Ok((result, state))
    }

    /// One room's entry: timeline and state since what the connection already has (or the
    /// most recent slice, for a room sent for the first time).
    #[allow(clippy::too_many_arguments)]
    async fn sliding_room(
        &self,
        req: &SlidingSyncRequest,
        sel: &RoomSelection,
        track: Option<&RoomTrack>,
        now: SyncToken,
        since: Option<SyncToken>,
        ignored: &HashSet<OwnedUserId>,
        is_dm: bool,
        bump: i64,
    ) -> Result<RoomResult> {
        let user = &req.user_id;
        let room_id = &sel.room_id;
        let m = &sel.membership;
        // A room this connection has only been sent as an invitation or a knock starts over
        // when the user joins: all it got was stripped state, and now there is history to
        // read. Sent as an ordinary update, the join would look like the start of the room
        // and the client would have no token to page back with.
        let initial = track.is_none_or(|t| {
            matches!(
                t.membership,
                MembershipState::Invite | MembershipState::Knock
            ) && m.membership == MembershipState::Join
        });
        let mut out = RoomResult {
            membership: m.membership.clone(),
            initial,
            name: None,
            avatar: None,
            is_dm,
            invite_state: None,
            required_state: Vec::new(),
            timeline: Vec::new(),
            prev_batch: None,
            limited: false,
            expanded_timeline: false,
            joined_count: 0,
            invited_count: 0,
            num_live: 0,
            bump_stamp: u64::try_from(bump).unwrap_or(0),
            heroes: Vec::new(),
            notification_count: 0,
            highlight_count: 0,
            txn_ids: HashMap::new(),
        };
        // An invite or a pending knock shows the room's stripped state and nothing else: the
        // user is not in the room, so its timeline and state are not theirs to read. (A knock
        // used to fall through to the joined path below, which let anyone who could knock on
        // a room read it through a room subscription.)
        if matches!(
            m.membership,
            MembershipState::Invite | MembershipState::Knock
        ) {
            out.invite_state = Some(self.stripped_state(room_id, Some(user)).await?);
            return Ok(out);
        }
        // Anything but `join` from here on is a room the user is no longer in (left, kicked or
        // banned). Such a room is frozen at the membership event that took them out: the
        // timeline ends there, and so do its state, name and avatar.
        let left = m.membership != MembershipState::Join;
        let upper = if left { m.stream_ordering } else { now.events };
        let lower = if initial {
            None
        } else {
            track.map(|t| t.timeline_upto)
        };
        // The client now asks for a longer timeline than this connection was given for the
        // room (typically a subscription to a room it knows from a list): send the newest
        // events again, up to the new limit, instead of only what is new.
        let limit = sel.timeline_limit.min(MAX_TIMELINE_LIMIT);
        let widened = !initial
            && !left
            && track.is_some_and(|t| limit > t.timeline_limit.min(MAX_TIMELINE_LIMIT));
        let timeline_lower = if widened { None } else { lower };

        // Timeline: the newest `limit` events in (timeline_lower, upper], oldest first.
        let mut events: Vec<StoredEvent> = Vec::new();
        let mut more = false;
        if limit > 0 {
            let batch = self
                .store
                .room_events_in_range(
                    room_id,
                    upper + 1,
                    timeline_lower.map(|l| l + 1),
                    Direction::Backwards,
                    limit + 1,
                )
                .await?;
            for ev in batch {
                if let Some(l) = timeline_lower
                    && ev.stream_ordering <= l
                {
                    break;
                }
                if ev.pdu.is_state() || !ignored.contains(ev.pdu.sender()) {
                    events.push(ev);
                }
            }
            more = events.len() > limit;
            events.truncate(limit);
            events.reverse();
            if let Some(l) = timeline_lower
                && let Some(g) = self.store.gap_position_since(room_id, l).await?
            {
                events.retain(|e| e.stream_ordering >= g);
                more = true;
            }
            // History visibility: under `invited` or `joined` a member does not see what was
            // said before they were invited or joined.
            events = self.filter_visible(room_id, user, events).await?;
            // A leave stored as an outlier is not in the timeline query's result.
            if left
                && !events.iter().any(|e| {
                    e.pdu.event_type() == &TimelineEventType::RoomMember
                        && e.pdu.state_key() == Some(user.as_str())
                })
                && let Some(ev) = self.store.get_event(&m.event_id).await?
                && ev.outlier
            {
                events.push(ev);
            }
        }
        out.limited = more;
        out.expanded_timeline = widened && !events.is_empty();
        if (initial || more || widened)
            && let Some(first) = events.first()
        {
            out.prev_batch = Some(StreamToken(first.stream_ordering - 1).to_string());
        }
        // Events newer than the client's position are live, also in a room that is sent in
        // full: a room that enters a list because of a new message carries that message next
        // to older ones.
        if let Some(s) = since {
            out.num_live = events
                .iter()
                .filter(|e| e.stream_ordering > s.events)
                .count() as u64;
        }
        if let Some(device_id) = &req.device_id {
            for ev in &events {
                if ev.pdu.sender() == user
                    && let Some(txn) = self
                        .store
                        .get_event_txn_id(ev.pdu.event_id(), user, Some(device_id))
                        .await?
                {
                    out.txn_ids.insert(ev.pdu.event_id().to_owned(), txn);
                }
            }
        }

        // Required state: the whole current state for a first (or widened) request, afterwards
        // the changes since the last position. Events the timeline also carries are included
        // either way, as Synapse does: the Rust SDK reads room state from `required_state` only
        // and ignores state events in the timeline.
        let expanded = track.is_some_and(|t| !sel.required_state.is_subset(&t.required_state));
        let full = initial || expanded;
        let senders: HashSet<OwnedUserId> =
            events.iter().map(|e| e.pdu.sender().to_owned()).collect();
        // The state group right after the leave, for someone who left as a member. After a
        // declined invite or a withdrawn knock the user was never in the room and there is no
        // state to show.
        let leave_state = if left && self.left_as_member(room_id, user).await? {
            self.store
                .room_events_in_range(room_id, upper + 1, None, Direction::Backwards, 1)
                .await?
                .first()
                .and_then(|e| e.state_group)
        } else {
            None
        };
        let pool: Vec<StoredEvent> = if sel.required_state.is_empty() {
            Vec::new()
        } else if left {
            match (leave_state, lower) {
                (Some(group), _) if full => self.state_events_at(group).await?,
                (Some(group), Some(l)) if l < upper => {
                    self.state_changes_until(room_id, l, group).await?
                }
                // Never a member: no room state, but the user's own membership event is
                // always theirs. Clients built on the Rust SDK learn that an invite was
                // declined from this event in `required_state`, not from the timeline.
                (None, _) if full || lower.is_some_and(|l| l < upper) => self
                    .store
                    .get_event(&m.event_id)
                    .await?
                    .into_iter()
                    .collect(),
                _ => Vec::new(),
            }
        } else if full {
            self.store.current_state_pdus(room_id).await?
        } else if let Some(l) = lower {
            self.state_changes_since(room_id, l).await?
        } else {
            Vec::new()
        };
        out.required_state = pool
            .into_iter()
            .filter(|e| required_state_matches(&sel.required_state, e, user, &senders))
            .collect();

        // Name, avatar, counts and heroes. A former member gets the name and avatar as they
        // were when they left; who is in the room now is none of their business.
        if left {
            if let Some(group) = leave_state {
                out.name = self
                    .state_content_at(group, StateEventType::RoomName)
                    .await?
                    .and_then(|c| c.get("name").and_then(Value::as_str).map(str::to_owned))
                    .filter(|n| !n.is_empty());
                out.avatar = self
                    .state_content_at(group, StateEventType::RoomAvatar)
                    .await?
                    .and_then(|c| c.get("url").and_then(Value::as_str).map(str::to_owned));
            }
            out.timeline = events;
            return Ok(out);
        }
        out.name = self
            .state_content(room_id, &StateEventType::RoomName, "")
            .await?
            .and_then(|c| c.get("name").and_then(Value::as_str).map(str::to_owned))
            .filter(|n| !n.is_empty());
        out.avatar = self
            .state_content(room_id, &StateEventType::RoomAvatar, "")
            .await?
            .and_then(|c| c.get("url").and_then(Value::as_str).map(str::to_owned));
        let members = self.store.members(room_id, None).await?;
        out.joined_count = members
            .iter()
            .filter(|x| x.membership == MembershipState::Join)
            .count() as u64;
        out.invited_count = members
            .iter()
            .filter(|x| x.membership == MembershipState::Invite)
            .count() as u64;
        let has_alias = self
            .store
            .current_state_event(room_id, &StateEventType::RoomCanonicalAlias, "")
            .await?
            .is_some();
        if out.name.is_none() && !has_alias {
            out.heroes = members
                .iter()
                .filter(|x| {
                    x.user_id != *user
                        && matches!(
                            x.membership,
                            MembershipState::Join | MembershipState::Invite
                        )
                })
                .take(5)
                .map(|x| Hero {
                    user_id: x.user_id.clone(),
                    name: x.display_name.clone(),
                    avatar: x.avatar_url.clone(),
                })
                .collect();
        }
        match self.unread_counts(room_id, user, upper).await {
            Ok(c) => {
                out.notification_count = c.total.notification_count;
                out.highlight_count = c.total.highlight_count;
            }
            Err(e) => self.unread_counts_failed(room_id, &e),
        }
        out.timeline = events;
        Ok(out)
    }

    /// The full state at a state group, as events.
    async fn state_events_at(&self, group: StateGroupId) -> Result<Vec<StoredEvent>> {
        let ids: Vec<OwnedEventId> = self
            .store
            .state_at_group(group)
            .await?
            .into_iter()
            .map(|e| e.event_id)
            .collect();
        Ok(self.store.get_events(&ids).await?)
    }

    /// The content of the state event `(ty, "")` at a state group.
    async fn state_content_at(
        &self,
        group: StateGroupId,
        ty: StateEventType,
    ) -> Result<Option<Value>> {
        let key = [(ty, String::new())];
        let Some(entry) = self
            .store
            .state_at_group_filtered(group, &key)
            .await?
            .into_iter()
            .next()
        else {
            return Ok(None);
        };
        Ok(self
            .store
            .get_event(&entry.event_id)
            .await?
            .and_then(|ev| serde_json::from_str(ev.pdu.content().get()).ok()))
    }

    /// State events that differ between the state at stream position `since` and the state
    /// group `until`: the changes a room saw up to a fixed point, where
    /// [`Services::state_changes_since`] compares with the current state.
    pub(crate) async fn state_changes_until(
        &self,
        room_id: &RoomId,
        since: i64,
        until: StateGroupId,
    ) -> Result<Vec<StoredEvent>> {
        let at = self
            .store
            .room_events_in_range(room_id, since + 1, None, Direction::Backwards, 1)
            .await?;
        let before: HashMap<(StateEventType, String), OwnedEventId> =
            match at.first().and_then(|e| e.state_group) {
                Some(group) if group == until => return Ok(Vec::new()),
                Some(group) => self
                    .store
                    .state_at_group(group)
                    .await?
                    .into_iter()
                    .map(|e| ((e.event_type, e.state_key), e.event_id))
                    .collect(),
                None => HashMap::new(),
            };
        let changed: Vec<OwnedEventId> = self
            .store
            .state_at_group(until)
            .await?
            .into_iter()
            .filter(|e| {
                before.get(&(e.event_type.clone(), e.state_key.clone())) != Some(&e.event_id)
            })
            .map(|e| e.event_id)
            .collect();
        Ok(self.store.get_events(&changed).await?)
    }

    /// Current state events that differ from the state as of stream position `since`.
    pub(crate) async fn state_changes_since(
        &self,
        room_id: &RoomId,
        since: i64,
    ) -> Result<Vec<StoredEvent>> {
        let at = self
            .store
            .room_events_in_range(room_id, since + 1, None, Direction::Backwards, 1)
            .await?;
        let current = self.store.current_state_pdus(room_id).await?;
        let Some(group) = at.first().and_then(|e| e.state_group) else {
            return Ok(current);
        };
        let before: HashMap<(StateEventType, String), String> = self
            .store
            .state_at_group(group)
            .await?
            .into_iter()
            .map(|e| ((e.event_type, e.state_key), e.event_id.to_string()))
            .collect();
        Ok(current
            .into_iter()
            .filter(|e| {
                e.pdu.state_tuple().is_none_or(|(t, k)| {
                    before.get(&(t, k.to_owned())).map(String::as_str)
                        != Some(e.pdu.event_id().as_str())
                })
            })
            .collect())
    }

    /// Rooms marked as direct chats in the user's `m.direct` account data.
    async fn direct_rooms(&self, user: &UserId) -> Result<HashSet<OwnedRoomId>> {
        let mut out = HashSet::new();
        if let Some(ad) = self.store.get_account_data(user, None, "m.direct").await?
            && let Some(map) = ad.content.as_object()
        {
            for rooms in map.values() {
                for r in rooms.as_array().into_iter().flatten() {
                    if let Some(id) = r.as_str().and_then(|s| OwnedRoomId::try_from(s).ok()) {
                        out.insert(id);
                    }
                }
            }
        }
        Ok(out)
    }

    async fn room_facts(&self, room_id: &RoomId) -> Result<RoomFacts> {
        let encrypted = self
            .store
            .current_state_event(room_id, &StateEventType::RoomEncryption, "")
            .await?
            .is_some();
        let room_type = self
            .state_content(room_id, &StateEventType::RoomCreate, "")
            .await?
            .and_then(|c| c.get("type").and_then(Value::as_str).map(str::to_owned));
        Ok(RoomFacts {
            encrypted,
            room_type,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invite_fingerprint_ignores_order_and_sees_changes() {
        let name = json!({ "type": "m.room.name", "state_key": "", "content": { "name": "a" } });
        let create = json!({ "type": "m.room.create", "state_key": "", "content": {} });
        let renamed = json!({ "type": "m.room.name", "state_key": "", "content": { "name": "b" } });
        assert_eq!(
            invite_fingerprint(&[name.clone(), create.clone()]),
            invite_fingerprint(&[create.clone(), name])
        );
        assert_ne!(
            invite_fingerprint(&[create.clone(), renamed]),
            invite_fingerprint(&[create])
        );
    }
}
