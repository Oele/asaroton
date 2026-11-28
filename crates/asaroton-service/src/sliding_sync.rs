//! Simplified Sliding Sync (MSC4186).
//!
//! The client describes lists of rooms (sorted by recency, filtered, windowed by ranges) and
//! explicit room subscriptions. The server remembers, per connection, what it has sent, so
//! every response after the first carries only what changed since the last position.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    hash::{DefaultHasher, Hash, Hasher},
    sync::Arc,
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
/// How many state events of one room a connection is remembered to have been given (see
/// [`StateGiven`]). Past this the memberships are forgotten, except those of the people the
/// newest timeline is about; the others are sent again when a timeline is about them.
const MAX_STATE_REMEMBERED: usize = 1000;
/// When there is too much to remember: how many state keys of one type other than memberships
/// are still worth remembering.
const MAX_STATE_PER_TYPE: usize = 100;

mod required_state;
pub use required_state::{KeyMatch, RequiredState, RequiredStateSpec, StateMatcher};

// ------------------------------------------------------------------ requests

#[derive(Clone, Debug, Default)]
pub struct ListRequest {
    pub ranges: Vec<(u64, u64)>,
    pub required_state: RequiredStateSpec,
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
    /// Only the rooms that these spaces name as children (`m.space.child` with a `via`), of
    /// the spaces the user has joined; empty for no such filter. Only `/v4/sync` sends it.
    pub spaces: Vec<OwnedRoomId>,
    /// Only rooms with one of these tags (`m.tag` room account data); empty for no filter.
    pub tags: Vec<String>,
    /// No rooms with any of these tags; wins over `tags`.
    pub not_tags: Vec<String>,
}

#[derive(Clone, Debug, Default)]
pub struct RoomSubscriptionRequest {
    pub required_state: RequiredStateSpec,
    pub timeline_limit: usize,
}

/// Which rooms an extension applies to.
#[derive(Clone, Debug)]
pub enum ScopeRoom {
    AllSubscribed,
    /// This room, whether or not it is among the subscriptions (the unstable endpoint).
    Room(OwnedRoomId),
    /// This room when it is among the request's subscriptions: `/v4/sync`'s `rooms` names
    /// subscriptions only (MSC4508).
    Subscribed(OwnedRoomId),
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
    /// Profile field updates of the people one shares a room with (MSC4262).
    pub profiles: Option<ProfilesRequest>,
    /// Sticky events (MSC4480, for MSC4354).
    pub sticky: Option<StickyRequest>,
}

/// The sticky events picked for one response (see `Services::sticky_pick`).
#[derive(Default)]
struct StickyPick {
    events: BTreeMap<OwnedRoomId, Vec<StoredEvent>>,
    /// The senders of `events`, per room.
    senders: HashMap<OwnedRoomId, HashSet<OwnedUserId>>,
    /// Per room, the position up to which its sticky events are handed out.
    upto: HashMap<OwnedRoomId, i64>,
    /// Some room had more than its limit.
    pending: bool,
}

/// The most sticky events per room that one response of the extension carries.
const STICKY_LIMIT_MAX: usize = 100;

/// The sticky events extension (MSC4480): which rooms, and at most how many events per room
/// in one response.
#[derive(Clone, Debug, Default)]
pub struct StickyRequest {
    pub scope: ExtensionScope,
    pub limit: usize,
}

/// The profiles extension (MSC4262): the members of which rooms, and which profile fields
/// (`None`: all of them).
#[derive(Clone, Debug, Default)]
pub struct ProfilesRequest {
    pub scope: ExtensionScope,
    pub fields: Option<Vec<String>>,
}

/// What changed in one user's profile: fields with their new values, and fields cleared.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ProfileDelta {
    pub updated: BTreeMap<String, Value>,
    pub removed: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct SlidingSyncRequest {
    /// Which endpoint the request came to.
    pub dialect: Dialect,
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
    /// The keys of the lists the room is in (not the subscriptions), each once.
    pub lists: Vec<String>,
    pub initial: bool,
    pub name: Option<String>,
    pub avatar: Option<String>,
    /// The connection was given a name, and the room has none any more (`/v4/sync` says so
    /// with `name: null`; an omitted name means "unchanged").
    pub name_cleared: bool,
    /// The same for the avatar.
    pub avatar_cleared: bool,
    pub is_dm: bool,
    /// `is_dm` differs from what the connection was given (`/v4/sync` then sends it, also
    /// when it is false).
    pub is_dm_changed: bool,
    /// State the connection has that is gone from the room (a state reset): `(type,
    /// state_key)`, sent by `/v4/sync` as a `StateStub` in `required_state`.
    pub stubs: Vec<(String, String)>,
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
    /// `event_id -> delay_id` for the user's events that were sent from a delayed event.
    pub delay_ids: HashMap<OwnedEventId, String>,
    /// The state the connection has of the room once this answer arrives. Kept with the
    /// connection, not sent.
    pub(crate) state_given: Arc<StateGiven>,
    /// The stream position up to which the room was read: where the next answer goes on
    /// from. Kept with the connection, not sent.
    pub(crate) read_upto: i64,
}

impl RoomResult {
    /// An entry that says nothing yet, for a room where the user's membership is `membership`.
    #[must_use]
    pub fn new(membership: MembershipState) -> Self {
        Self {
            membership,
            lists: Vec::new(),
            initial: false,
            name: None,
            avatar: None,
            name_cleared: false,
            avatar_cleared: false,
            is_dm: false,
            is_dm_changed: false,
            stubs: Vec::new(),
            invite_state: None,
            required_state: Vec::new(),
            timeline: Vec::new(),
            prev_batch: None,
            limited: false,
            expanded_timeline: false,
            joined_count: 0,
            invited_count: 0,
            num_live: 0,
            bump_stamp: 0,
            heroes: Vec::new(),
            notification_count: 0,
            highlight_count: 0,
            txn_ids: HashMap::new(),
            delay_ids: HashMap::new(),
            state_given: Arc::default(),
            read_upto: 0,
        }
    }
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
    /// Per room, the content of an `m.receipt` event (the unstable endpoint wraps it in one).
    pub receipts: BTreeMap<OwnedRoomId, Value>,
    pub typing: BTreeMap<OwnedRoomId, Vec<OwnedUserId>>,
    pub thread_subscriptions: Option<crate::thread_subscriptions::ThreadSubscriptionChanges>,
    /// Per user: what changed, or `None` when they no longer share a room in scope (MSC4262).
    pub profiles: BTreeMap<OwnedUserId, Option<ProfileDelta>>,
    /// Per room, the sticky events to deliver, oldest first (MSC4480).
    pub sticky: BTreeMap<OwnedRoomId, Vec<StoredEvent>>,
    /// Some room had more sticky events than its limit: the rest follows in the next
    /// responses, which therefore do not wait.
    pub sticky_pending: bool,
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
            && self.profiles.is_empty()
            && self.sticky.is_empty()
            && !self.sticky_pending
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

/// The two sliding sync endpoints: the unstable one (MSC4186 as first implemented,
/// `org.matrix.simplified_msc3575`) and `/v4/sync` (MSC4186 accepted, with its extension
/// MSCs). One engine serves both; the dialect decides the few places where they differ, and
/// connections of one are never taken up by the other.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum Dialect {
    #[default]
    Unstable,
    V4,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct ConnKey {
    dialect: Dialect,
    user: OwnedUserId,
    device: Option<OwnedDeviceId>,
    conn_id: String,
}

/// What a connection has been told about one room.
#[derive(Clone, Debug)]
struct RoomTrack {
    timeline_upto: i64,
    /// The position of the user's membership event that the connection was told about.
    membership_at: i64,
    required_state: RequiredState,
    timeline_limit: usize,
    membership: MembershipState,
    notification_count: u64,
    highlight_count: u64,
    /// What `/v4/sync` showed of the room besides its timeline and state; `None` on the
    /// unstable endpoint, which does not compare these.
    shown: Option<Shown>,
    /// Fingerprint of the stripped state last sent for an invite, so the invite is sent again
    /// only when it changes. Without this an invited room was part of every response and the
    /// request never long-polled.
    invite: Option<u64>,
    /// The state this connection has of the room.
    state_given: Arc<StateGiven>,
}

/// What a `/v4/sync` connection was shown of a room besides its timeline and state: its name,
/// avatar and DM flag (so that it can be told when they change or go, see
/// `RoomResult::name_cleared`), and the lists it is in (so that it is told when the room moves
/// between them).
#[derive(Clone, Debug, PartialEq, Eq)]
struct Shown {
    name: Option<String>,
    /// An avatar with an empty URL is none.
    avatar: Option<String>,
    is_dm: bool,
    lists: Vec<String>,
}

impl Shown {
    fn of(r: &RoomResult) -> Self {
        Self {
            name: r.name.clone(),
            avatar: r.avatar.clone().filter(|a| !a.is_empty()),
            is_dm: r.is_dm,
            lists: r.lists.clone(),
        }
    }
}

/// The state a connection has of one room: for each state key, the event it was sent as
/// required state.
///
/// Two things rest on it. State the connection has is not sent again, however often the
/// comparison of the room's state then and now comes up with it: in a room with two loose
/// ends (events of two servers that did not know of each other) that comparison keeps finding
/// the same change until the ends are tied, and an answer that repeated it each time would
/// never wait. And with lazily loaded members (`$LAZY`) it says whose membership the
/// connection has: someone it lacks gets theirs sent along when a timeline is about them, and
/// a change to someone it has is sent whether the timeline shows it or not.
#[derive(Clone, Debug, Default)]
pub(crate) struct StateGiven(BTreeMap<String, BTreeMap<String, OwnedEventId>>);

impl StateGiven {
    fn has_member(&self, user: &UserId) -> bool {
        self.0
            .get("m.room.member")
            .is_some_and(|members| members.contains_key(user.as_str()))
    }

    /// Whether the connection has an event for `(ty, key)`.
    fn has_key(&self, ty: &str, key: &str) -> bool {
        self.0.get(ty).is_some_and(|keys| keys.contains_key(key))
    }

    /// What the connection has once it was told that these keys are gone.
    fn without(given: &Arc<Self>, gone: &[(String, String)]) -> Arc<Self> {
        if gone.is_empty() {
            return Arc::clone(given);
        }
        let mut next = (**given).clone();
        for (ty, key) in gone {
            if let Some(keys) = next.0.get_mut(ty) {
                keys.remove(key);
                if keys.is_empty() {
                    next.0.remove(ty);
                }
            }
        }
        Arc::new(next)
    }

    /// Whether the connection has exactly this event for its state key.
    fn has(&self, ev: &StoredEvent) -> bool {
        ev.pdu.state_tuple().is_some_and(|(ty, key)| {
            self.0
                .get(ty.to_string().as_str())
                .and_then(|keys| keys.get(key))
                .is_some_and(|id| id == ev.pdu.event_id())
        })
    }

    /// Whether the connection has an event for this state key, and it is another one.
    fn has_another(&self, ev: &StoredEvent) -> bool {
        ev.pdu.state_tuple().is_some_and(|(ty, key)| {
            self.0
                .get(ty.to_string().as_str())
                .and_then(|keys| keys.get(key))
                .is_some_and(|id| id != ev.pdu.event_id())
        })
    }

    fn len(&self) -> usize {
        self.0.values().map(BTreeMap::len).sum()
    }

    /// What the connection has after it was sent `sent` on top of `given`. `about` and `user`
    /// are whose memberships are kept when there is too much to remember.
    fn with(
        given: Option<&Arc<Self>>,
        sent: &[StoredEvent],
        about: &HashSet<OwnedUserId>,
        user: &UserId,
    ) -> Arc<Self> {
        if sent.is_empty() {
            return given.cloned().unwrap_or_default();
        }
        let mut next = given.map_or_else(Self::default, |g| (**g).clone());
        for ev in sent {
            if let Some((ty, key)) = ev.pdu.state_tuple() {
                next.0
                    .entry(ty.to_string())
                    .or_default()
                    .insert(key.to_owned(), ev.pdu.event_id().to_owned());
            }
        }
        if next.len() > MAX_STATE_REMEMBERED {
            // Too much to remember. First the kinds of state that a client asked for with a
            // wildcard and that a room has no end of (the children of a space, the members
            // of a call): they are not remembered at all, and are sent again whenever the
            // comparison comes up with them, as everything was before this existed.
            next.0
                .retain(|ty, keys| ty == "m.room.member" || keys.len() <= MAX_STATE_PER_TYPE);
        }
        if next.len() > MAX_STATE_REMEMBERED
            && let Some(members) = next.0.get_mut("m.room.member")
        {
            // Then the memberships, except those of the people the newest timeline is about.
            let keep: HashSet<&str> = about.iter().map(|u| u.as_str()).collect();
            members.retain(|k, _| k == user.as_str() || keep.contains(k.as_str()));
        }
        Arc::new(next)
    }
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
    /// The users whose profiles the profiles extension has handed out (MSC4262); `None` while
    /// the extension is off on this connection.
    profile_users: Option<BTreeSet<OwnedUserId>>,
    /// For each kick or ban of the user: whether it put them out as a member. Such rooms stay
    /// in the lists (MSC4186); what an event did never changes, so it is worked out once.
    put_out: HashMap<OwnedEventId, bool>,
    /// For each room in scope of the sticky events extension, the stream position up to which
    /// its sticky events were handed out (MSC4480); `None` while the extension is off.
    sticky_upto: Option<HashMap<OwnedRoomId, i64>>,
    /// `/v4/sync`: what the account data, receipts and typing extensions have handed out, so
    /// that a room entering their scope, for the first time or again, gets what MSC4548,
    /// MSC4551 and MSC4508 say (see [`ScopeTrack`]). `None` while the extension is off, and
    /// always on the unstable endpoint, which keeps its own rules.
    account_data_upto: Option<ScopeTrack>,
    receipts_upto: Option<ScopeTrack>,
    /// The rooms in scope of the typing extension at this answer.
    typing_scope: Option<HashSet<OwnedRoomId>>,
}

/// For each room that has been in scope of an extension on a connection: the stream position up
/// to which the room's data was handed out. A room in scope gets what changed after its
/// position; a room without an entry is entering scope for the first time and gets its initial
/// data; a room that drops out of scope keeps its entry as it was, so that when it comes back it
/// gets exactly what changed while it was out. The map grows at most to the user's rooms.
type ScopeTrack = HashMap<OwnedRoomId, i64>;

/// The last state the client acknowledged, and the one produced by the latest response (which
/// the client may still fail to receive, in which case it retries with the older position).
#[derive(Debug, Default)]
struct Connection {
    confirmed: Option<ConnState>,
    pending: Option<ConnState>,
    /// Chosen afresh whenever the connection starts over; every `pos` carries it, so that a
    /// position from before (or from another device's connection) is not taken for one of
    /// this connection's.
    nonce: String,
    /// How many requests were made on this connection. Only the newest may leave its state
    /// behind as `pending` (see `SlidingSyncConnections::store_pending`).
    requests: u64,
}

/// A connection's position as clients see it: `<sync token>~<nonce>~<number>`. The sync token
/// is the stream positions the answer covered, so the `pos` also serves where a `/sync` token
/// does (`/messages`, `/keys/changes`: their parsers stop at the `~`); the nonce ties it to the
/// connection as it was started; the number is the answer's on the connection.
fn format_pos(token: &SyncToken, nonce: &str, seq: u64) -> String {
    format!("{token}~{nonce}~{seq}")
}

/// The nonce and the number of a `pos` (see [`format_pos`]).
fn parse_pos(pos: &str) -> Option<(&str, u64)> {
    let mut parts = pos.rsplitn(3, '~');
    let seq = parts.next()?.parse().ok()?;
    let nonce = parts.next()?;
    parts.next()?;
    Some((nonce, seq))
}

/// Per-connection state of every sliding sync client.
#[derive(Debug, Default)]
pub struct SlidingSyncConnections {
    conns: DashMap<ConnKey, Connection>,
}

impl SlidingSyncConnections {
    /// The state a request with position `pos` starts from, the request's number on its
    /// connection (to be handed back to `store_pending`) and the connection's nonce (for the
    /// answer's `pos`, see [`format_pos`]).
    fn resolve(
        &self,
        key: &ConnKey,
        pos: Option<&str>,
    ) -> Result<(Option<ConnState>, u64, String)> {
        let mut entry = self.conns.entry(key.clone()).or_default();
        let conn = &mut *entry;
        conn.requests += 1;
        let request = conn.requests;
        match pos {
            None => {
                conn.confirmed = None;
                conn.pending = None;
                conn.nonce = asaroton_auth::generate_session_id()
                    .chars()
                    .take(8)
                    .collect();
                Ok((None, request, conn.nonce.clone()))
            }
            Some(p) => {
                let (nonce, seq) = parse_pos(p).ok_or_else(unknown_pos)?;
                if conn.nonce.is_empty() || nonce != conn.nonce {
                    return Err(unknown_pos());
                }
                if conn.pending.as_ref().is_some_and(|s| s.seq == seq) {
                    conn.confirmed = conn.pending.take();
                } else if conn.confirmed.as_ref().is_some_and(|s| s.seq == seq) {
                    conn.pending = None;
                } else {
                    return Err(unknown_pos());
                }
                Ok((conn.confirmed.clone(), request, conn.nonce.clone()))
            }
        }
    }

    /// Remembers what the answer to request number `request` tells the client, unless a
    /// newer request has been made on the connection in the meantime. A client that gives up
    /// on a waiting request and asks again is answered by the new one; the old one may still
    /// be running here (behind a reverse proxy it is not dropped), and when it finishes its
    /// answer goes nowhere. Were its state kept, the client's next position would confirm
    /// events and state that it was never sent.
    fn store_pending(&self, key: &ConnKey, state: ConnState, request: u64, nonce: &str) {
        if self.conns.len() > 10_000 {
            self.conns.clear();
        }
        match self.conns.entry(key.clone()) {
            dashmap::Entry::Occupied(mut conn) => {
                if conn.get().requests == request {
                    conn.get_mut().pending = Some(state);
                }
            }
            // Dropped with all the others just now: this request starts the connection anew.
            dashmap::Entry::Vacant(slot) => {
                slot.insert(Connection {
                    confirmed: None,
                    pending: Some(state),
                    requests: request,
                    nonce: nonce.to_owned(),
                });
            }
        }
    }
}

/// A to-device `since` on `/v4/sync` (MSC4538): a position this server could have handed out
/// as `next_batch`, else `M_INVALID_PARAM`. None given: nothing delivered yet.
fn to_device_since(since: Option<&str>, now: i64) -> Result<i64> {
    let Some(s) = since else {
        return Ok(0);
    };
    s.parse::<i64>()
        .ok()
        .filter(|n| (0..=now).contains(n))
        .ok_or_else(|| {
            Error::invalid_param("The to-device since is not a position this server gave out.")
        })
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

/// What the `spaces`, `tags` and `not_tags` filters look at, read once per request and only
/// when some list asks for them.
#[derive(Default)]
struct UserFacts {
    /// The tags of each room the user has tagged.
    tags: HashMap<OwnedRoomId, HashSet<String>>,
    /// For each space a list names and the user has joined: the rooms it names as children.
    space_children: HashMap<OwnedRoomId, HashSet<OwnedRoomId>>,
}

impl ListFilters {
    fn matches(&self, m: &Membership, facts: &RoomFacts, is_dm: bool, user: &UserFacts) -> bool {
        if self.is_dm.is_some_and(|v| v != is_dm) {
            return false;
        }
        // A space the user has not joined, or does not know, names no rooms here: a filter
        // whose spaces are all like that leaves the list empty (open question 7 of the v4
        // plan: the filter is not dropped).
        if !self.spaces.is_empty()
            && !self.spaces.iter().any(|space| {
                user.space_children
                    .get(space)
                    .is_some_and(|children| children.contains(&m.room_id))
            })
        {
            return false;
        }
        let tags = user.tags.get(&m.room_id);
        let tagged = |t: &String| tags.is_some_and(|have| have.contains(t));
        if self.not_tags.iter().any(tagged) {
            return false;
        }
        if !self.tags.is_empty() && !self.tags.iter().any(tagged) {
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
        !self.not_room_types.contains(&facts.room_type)
    }
}

/// A room to report, with the union of every list's and subscription's demands on it.
struct RoomSelection {
    room_id: OwnedRoomId,
    membership: Membership,
    required_state: RequiredState,
    timeline_limit: usize,
    lists: Vec<String>,
    subscribed: bool,
}

impl ExtensionScope {
    fn applies(&self, room: &RoomSelection) -> bool {
        self.applies_to(&room.room_id, &room.lists, room.subscribed)
    }

    /// Whether the extension covers a room that is in `lists` and, when `subscribed`, among
    /// the request's room subscriptions.
    fn applies_to(&self, room_id: &RoomId, lists: &[String], subscribed: bool) -> bool {
        // Each of the two is "all of them" when it is left out (MSC4186): a request that names
        // only `rooms` still means the rooms of every list, and the other way round. `*` in
        // `lists` says the same in so many words.
        let in_a_wanted_list = match &self.lists {
            None => !lists.is_empty(),
            Some(wanted) => lists
                .iter()
                .any(|n| wanted.contains(n) || wanted.iter().any(|w| w == "*")),
        };
        if in_a_wanted_list || (self.rooms.is_none() && subscribed) {
            return true;
        }
        if let Some(r) = &self.rooms {
            for e in r {
                match e {
                    ScopeRoom::AllSubscribed if subscribed => return true,
                    ScopeRoom::Room(id) if id == room_id => return true,
                    ScopeRoom::Subscribed(id) if subscribed && id == room_id => return true,
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
    required_state: &RequiredState,
    timeline_limit: usize,
    list: Option<&str>,
    subscribed: bool,
) {
    let e = selections
        .entry(m.room_id.clone())
        .or_insert_with(|| RoomSelection {
            room_id: m.room_id.clone(),
            membership: m.clone(),
            required_state: RequiredState::default(),
            timeline_limit: 0,
            lists: Vec::new(),
            subscribed: false,
        });
    e.required_state.extend(required_state);
    e.timeline_limit = e.timeline_limit.max(timeline_limit);
    if let Some(l) = list {
        e.lists.push(l.to_owned());
    }
    e.subscribed |= subscribed;
}

/// Whether a state event is wanted by a room's `required_state`. `lazy_member` says whose
/// membership lazy loading wants, beside the user's own.
fn required_state_matches(
    wanted: &RequiredState,
    ev: &StoredEvent,
    user: &UserId,
    lazy_member: impl Fn(&UserId) -> bool,
) -> bool {
    let Some((ty, key)) = ev.pdu.state_tuple() else {
        return false;
    };
    wanted.matches(&ty.to_string(), key, user, lazy_member)
}

/// Whether the room's name and its avatar went since the connection was shown the room
/// (`shown_before`): it had one, and `out` has none. An avatar with an empty URL is none.
fn cleared(shown_before: Option<&Shown>, out: &RoomResult) -> (bool, bool) {
    let set = |v: &Option<String>| v.as_deref().is_some_and(|s| !s.is_empty());
    shown_before.map_or((false, false), |t| {
        (
            set(&t.name) && !set(&out.name),
            set(&t.avatar) && !set(&out.avatar),
        )
    })
}

/// Whether lazy loading sends every membership that changed since the last answer, beside
/// those of the people the timeline is about (MSC4186, rule 3): only on `/v4/sync`, and only
/// for an answer that goes on from the last one (not `full`: a room sent anew or with more
/// state) with no gap in its timeline. A timeline limit of 0 never shows whether there was a
/// gap, so it does not count as without one: a room that sat in such a list for days would
/// get every membership change of those days.
fn sends_every_membership_change(
    dialect: Dialect,
    full: bool,
    timeline_limit: usize,
    limited: bool,
) -> bool {
    dialect == Dialect::V4 && !full && timeline_limit > 0 && !limited
}

/// Whom a timeline is about: whoever sent one of its events, and whoever one of its
/// membership events is about (the person invited, removed or banned).
fn timeline_members(events: &[StoredEvent]) -> HashSet<OwnedUserId> {
    let mut members: HashSet<OwnedUserId> =
        events.iter().map(|e| e.pdu.sender().to_owned()).collect();
    members.extend(
        events
            .iter()
            .filter(|e| e.pdu.event_type() == &TimelineEventType::RoomMember)
            .filter_map(|e| e.pdu.state_key())
            .filter_map(|key| OwnedUserId::try_from(key).ok()),
    );
    members
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
            // Only a sync that affects presence counts as activity: a user who keeps their
            // presence to themselves (offline) must not show as "last active just now".
            self.presence.touch(&req.user_id);
        }

        let key = ConnKey {
            dialect: req.dialect,
            user: req.user_id.clone(),
            device: req.device_id.clone(),
            conn_id: req.conn_id.clone().unwrap_or_default(),
        };
        let began = std::time::Instant::now();
        let (base, request, nonce) = self.sliding_sync.resolve(&key, req.pos.as_deref())?;
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
                let news = notifier::wait_for(&mut rx, remaining, |n| match n {
                    Notification::RoomEvent { room_id, .. }
                    | Notification::RoomEphemeral { room_id } => rooms.contains(room_id),
                    Notification::User { user_id: u } => *u == user_id,
                    Notification::DeviceLists { .. } | Notification::Presence { .. } => true,
                    // A profile change matters to a connection that hands out that user's
                    // profile (MSC4262).
                    Notification::Profile { user_id: u } => base
                        .as_ref()
                        .and_then(|b| b.profile_users.as_ref())
                        .is_some_and(|users| users.contains(u)),
                });
                // A server that stops answers with what it has: nothing.
                let woke = tokio::select! {
                    woke = news => woke,
                    () = self.closing.begun() => false,
                };
                if !woke {
                    break;
                }
                (result, state) = self.compute_sliding_sync(&req, base.as_ref()).await?;
                if !result.quiet {
                    break;
                }
            }
        }
        result.pos = format_pos(&state.token, &nonce, state.seq);
        // What each answer carried, by count: when a client reports that something did not
        // reach it, this says whether the server sent it (the bodies are logged nowhere).
        tracing::debug!(
            user = %req.user_id,
            dialect = ?req.dialect,
            conn = %key.conn_id,
            from = req.pos.as_deref().unwrap_or("-"),
            pos = %result.pos,
            waited_ms = u64::try_from(began.elapsed().as_millis()).unwrap_or(u64::MAX),
            quiet = result.quiet,
            rooms = result.rooms.len(),
            account_data = result
                .extensions
                .account_data
                .as_ref()
                .map_or(0, |a| a.global.len() + a.rooms.values().map(Vec::len).sum::<usize>()),
            // The account-data stream positions the answer covered, after `from` up to `upto`:
            // compared with the stream of a write ("global account data set"), they say
            // whether an answer should have carried it (tracker issue 56).
            ad_from = base.as_ref().map_or(0, |b| b.token.account_data),
            ad_upto = state.token.account_data,
            receipts = result.extensions.receipts.len(),
            typing = result.extensions.typing.len(),
            to_device = result
                .extensions
                .to_device
                .as_ref()
                .map_or(0, |(_, events)| events.len()),
            "sliding sync answered"
        );
        self.sliding_sync
            .store_pending(&key, state, request, &nonce);
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
        let mut memberships: Vec<Membership> = self
            .store
            .memberships_for_user(user)
            .await?
            .into_iter()
            .filter(|m| !m.forgotten)
            .filter(|m| !(invites_blocked && m.membership == MembershipState::Invite))
            .collect();
        // A room that `createRoom` is still writing is not shown yet: asked after the
        // memberships were read, as `rooms::making` explains. The room is new to the
        // connection when it does come, and is then sent in full.
        let unfinished = self.rooms_in_the_making.under_way(user);
        memberships.retain(|m| !unfinished.iter().any(|u| u.room_id == m.room_id));
        let ignored = self.ignored_users(user).await?;
        let direct = self.direct_rooms(user).await?;

        // Kicks and bans: whether each put the user out as a member (not a ban of someone who
        // was never in, nor a withdrawn invite or a refused knock).
        let mut put_out: HashMap<OwnedEventId, bool> = HashMap::new();
        let mut before_cache = HashMap::new();
        for m in &memberships {
            let by_other = match m.membership {
                MembershipState::Ban => true,
                MembershipState::Leave => m.sender != *user,
                _ => false,
            };
            if !by_other {
                continue;
            }
            let was_member = match base.and_then(|b| b.put_out.get(&m.event_id)) {
                Some(known) => *known,
                None => match self.store.get_event(&m.event_id).await? {
                    Some(ev) => {
                        self.membership_before(user, &ev, &mut before_cache).await? == "join"
                    }
                    None => false,
                },
            };
            put_out.insert(m.event_id.clone(), was_member);
        }

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
                // A pending knock is in the lists too (as Synapse has it), as stripped state:
                // the knocker sees the room they asked to join, and later its answer.
                MembershipState::Join | MembershipState::Knock => true,
                MembershipState::Invite => !ignored.contains(&m.sender),
                // "Rooms the user has been kicked or banned from will always be included"
                // (MSC4186, Lists), when they were in it: otherwise a user who joins and is
                // put out before an answer sent the room as joined is never told. A room the
                // user left themselves is reported once, below, and then drops out.
                MembershipState::Leave | MembershipState::Ban => {
                    put_out.get(&m.event_id).copied().unwrap_or(false)
                }
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
        let user_facts = self.user_facts(user, req, &memberships).await?;

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
                        f.matches(
                            m,
                            &facts[&m.room_id],
                            direct.contains(&m.room_id),
                            &user_facts,
                        )
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
                        &RequiredState::of(&list.required_state),
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
                    &RequiredState::of(&sub.required_state),
                    sub.timeline_limit,
                    None,
                    true,
                );
            }
        }
        // A room the user left themselves is reported once more, outside the lists, and is
        // then dropped. MSC4186 keeps such a room in the server's list for the rest of the
        // connection once it was sent; asaroton keeps its own treatment on both endpoints
        // (open question 2 of the v4 plan), so on `/v4/sync` it comes without `lists`.
        if let (Some(s), Some(b)) = (since, base) {
            for m in &memberships {
                if matches!(m.membership, MembershipState::Leave | MembershipState::Ban)
                    && m.stream_ordering > s.events
                    && !selections.contains_key(&m.room_id)
                    && let Some(t) = b.rooms.get(&m.room_id)
                {
                    select(
                        &mut selections,
                        m,
                        &t.required_state,
                        t.timeline_limit.max(1),
                        None,
                        false,
                    );
                }
            }
        }

        // Sticky events first: their senders count for the rooms' lazily loaded members.
        let sticky = match &req.extensions.sticky {
            Some(s) => Some(
                self.sticky_pick(s, &selections, base, now, &ignored)
                    .await?,
            ),
            None => None,
        };
        let no_one = HashSet::new();

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
                    latest.get(room_id).copied(),
                    sticky
                        .as_ref()
                        .and_then(|p| p.senders.get(room_id))
                        .unwrap_or(&no_one),
                )
                .await?;
            let invite = r.invite_state.as_deref().map(invite_fingerprint);
            let unchanged = track.is_some_and(|t| {
                !r.initial
                    && r.timeline.is_empty()
                    && r.required_state.is_empty()
                    && t.invite == invite
                    && t.membership == sel.membership.membership
                    && match req.dialect {
                        Dialect::Unstable => {
                            t.notification_count == r.notification_count
                                && t.highlight_count == r.highlight_count
                        }
                        // `/v4/sync` has no notification counts (MSC4186; open question 9),
                        // and says when the name, the avatar or the DM flag change and when
                        // state is gone. An invite or a knock shows only its stripped state.
                        // A room that moved into another list, or out of one, is sent with
                        // its new `lists`, also when nothing else changed (open question 3 of
                        // the v4 plan): with the order kept on the server, the counts alone
                        // do not tell the client which room moved.
                        Dialect::V4 => {
                            let now = Shown::of(&r);
                            r.stubs.is_empty()
                                && t.shown.as_ref().is_some_and(|was| {
                                    was.lists == now.lists
                                        && (r.invite_state.is_some() || *was == now)
                                })
                        }
                    }
            });
            tracks.insert(
                room_id.clone(),
                RoomTrack {
                    // Not always this answer's position: a room the user is out of was read
                    // up to their leave, and a joined room may have been read further (see
                    // `sliding_room`). Reading on from the position would send those events
                    // a second time.
                    timeline_upto: r.read_upto,
                    membership_at: sel.membership.stream_ordering,
                    required_state: sel.required_state.clone(),
                    // The longest timeline the connection was given, so that dropping a
                    // subscription and taking it again does not send the same events twice.
                    // A room that starts over has been given what it is sent now, and so has
                    // a room sent with a gap (`limited`): the client holds the latest events
                    // from here on, not the older ones from before the gap (matrix-rust-sdk
                    // drops them), so a longer limit later has to send them again (MSC4186,
                    // "Changes to timeline_limit"; tracker #103).
                    timeline_limit: if r.initial || r.limited {
                        sel.timeline_limit
                    } else {
                        sel.timeline_limit
                            .max(track.map_or(0, |t| t.timeline_limit))
                    },
                    membership: sel.membership.membership.clone(),
                    notification_count: r.notification_count,
                    highlight_count: r.highlight_count,
                    shown: (req.dialect == Dialect::V4).then(|| Shown::of(&r)),
                    invite,
                    state_given: Arc::clone(&r.state_given),
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
            let since_td: i64 = match req.dialect {
                // The unstable endpoint reads a `since` it cannot use as nothing delivered.
                Dialect::Unstable => td
                    .since
                    .as_deref()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0),
                Dialect::V4 => to_device_since(td.since.as_deref(), now.to_device)?,
            };
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
        let mut account_data_upto = None;
        if let Some(scope) = &req.extensions.account_data
            && req.dialect == Dialect::V4
        {
            let (result, upto) = self
                .account_data_v4(
                    user,
                    scope,
                    &selections,
                    base.and_then(|b| b.account_data_upto.as_ref()),
                    since,
                    now,
                )
                .await?;
            ext.account_data = Some(result);
            account_data_upto = Some(upto);
        } else if let Some(scope) = &req.extensions.account_data {
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
        let mut receipts_upto = None;
        if let Some(scope) = &req.extensions.receipts
            && req.dialect == Dialect::V4
        {
            let (receipts, upto) = self
                .receipts_v4(
                    user,
                    scope,
                    &selections,
                    &rooms,
                    base.and_then(|b| b.receipts_upto.as_ref()),
                    now,
                )
                .await?;
            ext.receipts = receipts;
            receipts_upto = Some(upto);
        } else if let Some(scope) = &req.extensions.receipts {
            for (room_id, sel) in &selections {
                if sel.membership.membership != MembershipState::Join || !scope.applies(sel) {
                    continue;
                }
                let initial = base.is_none_or(|b| !b.rooms.contains_key(room_id))
                    || rooms.get(room_id).is_some_and(|r| r.initial);
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
                    ext.receipts.insert(room_id.clone(), Value::Object(content));
                }
            }
        }
        let mut typing_scope = None;
        if let Some(scope) = &req.extensions.typing
            && req.dialect == Dialect::V4
        {
            let (typing, in_scope) = self.typing_v4(
                scope,
                &selections,
                &rooms,
                base.and_then(|b| b.typing_scope.as_ref()),
                since,
            );
            ext.typing = typing;
            typing_scope = Some(in_scope);
        } else if let Some(scope) = &req.extensions.typing {
            for (room_id, sel) in &selections {
                if sel.membership.membership != MembershipState::Join || !scope.applies(sel) {
                    continue;
                }
                let initial = base.is_none_or(|b| !b.rooms.contains_key(room_id))
                    || rooms.get(room_id).is_some_and(|r| r.initial);
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

        // The sticky events picked before the rooms, minus those the room's timeline carries.
        let sticky_upto = sticky.map(|pick| {
            for (room_id, events) in pick.events {
                let shown: HashSet<&ruma::EventId> = rooms
                    .get(&room_id)
                    .map(|r| r.timeline.iter().map(StoredEvent::event_id).collect())
                    .unwrap_or_default();
                let events: Vec<StoredEvent> = events
                    .into_iter()
                    .filter(|e| !shown.contains(e.event_id()))
                    .collect();
                if !events.is_empty() {
                    ext.sticky.insert(room_id, events);
                }
            }
            ext.sticky_pending = pick.pending;
            pick.upto
        });

        let mut profile_users = None;
        if let Some(p) = &req.extensions.profiles {
            let (delta, users) = self
                .profile_extension(user, p, &selections, base, now)
                .await?;
            ext.profiles = delta;
            profile_users = Some(users);
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
            profile_users,
            put_out,
            sticky_upto,
            account_data_upto,
            receipts_upto,
            typing_scope,
        };
        Ok((result, state))
    }

    /// The account data extension on `/v4/sync` (MSC4548). `previous` is what the connection
    /// was given (`None`: the extension is enabled for the first time, or again, or the request
    /// has no `pos`). Global account data: all of it the first time, afterwards what changed
    /// after `pos`. A room in scope: all its account data when it enters scope for the first
    /// time, afterwards what changed after its position (for a room coming back into scope,
    /// what changed while it was out); any membership counts. One entry per type.
    async fn account_data_v4(
        &self,
        user: &UserId,
        scope: &ExtensionScope,
        selections: &BTreeMap<OwnedRoomId, RoomSelection>,
        previous: Option<&ScopeTrack>,
        since: Option<SyncToken>,
        now: SyncToken,
    ) -> Result<(AccountDataResult, ScopeTrack)> {
        let global_from = match previous {
            Some(_) => since.map_or(0, |s| s.account_data),
            None => 0,
        };
        let from: HashMap<&OwnedRoomId, i64> = selections
            .iter()
            .filter(|(_, sel)| scope.applies(sel))
            .map(|(room_id, _)| {
                let at = previous.and_then(|p| p.get(room_id)).copied().unwrap_or(0);
                (room_id, at)
            })
            .collect();
        let lowest = from.values().copied().fold(global_from, i64::min);
        let mut global: Vec<Value> = Vec::new();
        let mut rooms: BTreeMap<OwnedRoomId, Vec<Value>> = BTreeMap::new();
        let pr_stream = self.push_rules_stream_id(user).await?;
        if global_from == 0 || (pr_stream > global_from && pr_stream <= now.account_data) {
            global.push(
                json!({ "type": "m.push_rules", "content": self.push_rules_account_data(user).await? }),
            );
        }
        for ad in self.store.account_data_since(user, lowest).await? {
            if ad.stream_id > now.account_data {
                continue;
            }
            let after = match &ad.room_id {
                None => global_from,
                Some(r) => match from.get(r) {
                    Some(at) => *at,
                    None => continue,
                },
            };
            // A deletion (MSC3391) is only news to a connection that saw the data. The push
            // rules come from their own table, above.
            if ad.stream_id <= after
                || (after == 0 && ad.is_deleted())
                || (ad.room_id.is_none() && ad.data_type == "m.push_rules")
            {
                continue;
            }
            let list = match ad.room_id {
                None => &mut global,
                Some(r) => rooms.entry(r).or_default(),
            };
            // One entry per type (MSC4548); the rows come oldest first.
            list.retain(|e| e.get("type").and_then(Value::as_str) != Some(ad.data_type.as_str()));
            list.push(json!({ "type": ad.data_type, "content": ad.content }));
        }
        let mut upto = previous.cloned().unwrap_or_default();
        for room_id in from.keys() {
            upto.insert((*room_id).clone(), now.account_data);
        }
        Ok((AccountDataResult { global, rooms }, upto))
    }

    /// The receipts extension on `/v4/sync` (MSC4551). Only joined rooms are in scope. A room
    /// entering scope for the first time (also when the extension is first enabled, and when
    /// the user rejoins: the room is `initial`) gets its initial receipts: everyone's on the
    /// events of this answer's timeline, and all the user's own. A room that was in scope gets
    /// what changed after its position, which for a room coming back into scope is what changed
    /// while it was out. A room sent with an expanded timeline also gets the receipts on its
    /// timeline. Other users' private receipts are never sent. Returns bare `m.receipt`
    /// contents.
    async fn receipts_v4(
        &self,
        user: &UserId,
        scope: &ExtensionScope,
        selections: &BTreeMap<OwnedRoomId, RoomSelection>,
        rooms: &BTreeMap<OwnedRoomId, RoomResult>,
        previous: Option<&ScopeTrack>,
        now: SyncToken,
    ) -> Result<(BTreeMap<OwnedRoomId, Value>, ScopeTrack)> {
        let mut upto = previous.cloned().unwrap_or_default();
        let mut out = BTreeMap::new();
        for (room_id, sel) in selections {
            if sel.membership.membership != MembershipState::Join {
                // Out of scope; forgotten, so that a return to the room counts as a first
                // entry.
                upto.remove(room_id);
                continue;
            }
            if !scope.applies(sel) {
                continue;
            }
            let shown = rooms.get(room_id);
            let on_timeline: HashSet<&ruma::EventId> = shown
                .map(|r| r.timeline.iter().map(StoredEvent::event_id).collect())
                .unwrap_or_default();
            let at = previous
                .and_then(|p| p.get(room_id))
                .copied()
                .filter(|_| shown.is_none_or(|r| !r.initial));
            let receipts: Vec<asaroton_store::Receipt> = match at {
                None => self
                    .store
                    .receipts_for_room(room_id, 0)
                    .await?
                    .into_iter()
                    .filter(|r| r.user_id == *user || on_timeline.contains(&*r.event_id))
                    .collect(),
                Some(at) => {
                    let mut changed = self.store.receipts_for_room(room_id, at).await?;
                    if shown.is_some_and(|r| r.expanded_timeline) && !on_timeline.is_empty() {
                        changed.extend(
                            self.store
                                .receipts_for_room(room_id, 0)
                                .await?
                                .into_iter()
                                .filter(|r| r.stream_id <= at)
                                .filter(|r| on_timeline.contains(&*r.event_id)),
                        );
                    }
                    changed
                }
            };
            let content = receipts_content(receipts, user);
            if !content.is_empty() {
                out.insert(room_id.clone(), Value::Object(content));
            }
            upto.insert(room_id.clone(), now.receipts);
        }
        Ok((out, upto))
    }

    /// The typing extension on `/v4/sync` (MSC4508). Only joined rooms are in scope. A room
    /// entering scope (for the first time or again) gets its current typing state, left out
    /// when nobody types; a room that stays gets it when it changed after `pos`. Returns the
    /// rooms in scope as well.
    fn typing_v4(
        &self,
        scope: &ExtensionScope,
        selections: &BTreeMap<OwnedRoomId, RoomSelection>,
        rooms: &BTreeMap<OwnedRoomId, RoomResult>,
        previous: Option<&HashSet<OwnedRoomId>>,
        since: Option<SyncToken>,
    ) -> (
        BTreeMap<OwnedRoomId, Vec<OwnedUserId>>,
        HashSet<OwnedRoomId>,
    ) {
        let mut out = BTreeMap::new();
        let mut in_scope = HashSet::new();
        for (room_id, sel) in selections {
            if sel.membership.membership != MembershipState::Join || !scope.applies(sel) {
                continue;
            }
            in_scope.insert(room_id.clone());
            let entering = previous.is_none_or(|p| !p.contains(room_id))
                || rooms.get(room_id).is_some_and(|r| r.initial);
            if entering {
                let users = self.typing.typing_users(room_id);
                if !users.is_empty() {
                    out.insert(room_id.clone(), users);
                }
            } else if since.is_some_and(|s| self.typing.changed_since(room_id, s.typing)) {
                out.insert(room_id.clone(), self.typing.typing_users(room_id));
            }
        }
        (out, in_scope)
    }

    /// The sticky events extension (MSC4480), picked before the rooms are worked out, so that
    /// their senders count as people the room's answer is about (lazily loaded members). For
    /// each joined room in scope: a room that enters scope (new on the connection or to the
    /// extension, the extension just enabled, the user newly joined) gets all its unexpired
    /// sticky events, any other the ones since its last position; the oldest `limit` of them,
    /// the rest in later responses. Those of ignored users are left out. Rooms that left scope
    /// are forgotten, so they get everything when they return.
    async fn sticky_pick(
        &self,
        req: &StickyRequest,
        selections: &BTreeMap<OwnedRoomId, RoomSelection>,
        base: Option<&ConnState>,
        now: SyncToken,
        ignored: &HashSet<OwnedUserId>,
    ) -> Result<StickyPick> {
        let previous = base.and_then(|b| b.sticky_upto.as_ref());
        let limit = req.limit.clamp(1, STICKY_LIMIT_MAX);
        // Events that run out within a second are not handed out (they must have time left).
        let alive_at = asaroton_store::now_ts() + 1000;
        let mut pick = StickyPick::default();
        for (room_id, sel) in selections {
            if sel.membership.membership != MembershipState::Join || !req.scope.applies(sel) {
                continue;
            }
            // For a joined room this is `sliding_room`'s `initial`: the connection did not
            // have the room, or not as joined.
            let entering = base
                .and_then(|b| b.rooms.get(room_id))
                .is_none_or(|t| t.membership != MembershipState::Join);
            let after = match previous.and_then(|p| p.get(room_id)) {
                Some(&at) if !entering => at,
                _ => i64::MIN,
            };
            let fetched = self
                .store
                .sticky_events(room_id, alive_at, after, now.events, limit)
                .await?;
            let reached = fetched.len() >= limit;
            let delivered = match fetched.last() {
                Some(last) if reached => {
                    pick.pending = true;
                    last.stream_ordering
                }
                _ => now.events,
            };
            pick.upto.insert(room_id.clone(), delivered);
            let events: Vec<StoredEvent> = fetched
                .into_iter()
                .filter(|e| !ignored.contains(e.pdu.sender()))
                .collect();
            if !events.is_empty() {
                pick.senders.insert(
                    room_id.clone(),
                    events.iter().map(|e| e.pdu.sender().to_owned()).collect(),
                );
                pick.events.insert(room_id.clone(), events);
            }
        }
        Ok(pick)
    }

    /// The profiles extension (MSC4262). Who: the local members of the joined rooms in scope
    /// (with no scope, everyone who shares a room with `user`, and `user`). A user the
    /// connection has not had yet gets every requested field that is set; one it has gets the
    /// fields that changed since its last position, with the new value or as removed; one that
    /// left the scope gets `None`. Returns the answer and the users the connection now has.
    /// Profiles of users on other servers are not known here, as in classic sync (MSC4429).
    async fn profile_extension(
        &self,
        user: &UserId,
        p: &ProfilesRequest,
        selections: &BTreeMap<OwnedRoomId, RoomSelection>,
        base: Option<&ConnState>,
        now: SyncToken,
    ) -> Result<(
        BTreeMap<OwnedUserId, Option<ProfileDelta>>,
        BTreeSet<OwnedUserId>,
    )> {
        let mut users: BTreeSet<OwnedUserId> = BTreeSet::new();
        if p.scope.lists.is_none() && p.scope.rooms.is_none() {
            users.extend(self.store.users_sharing_rooms_with(user).await?);
            users.insert(user.to_owned());
        } else {
            for (room_id, sel) in selections {
                if sel.membership.membership != MembershipState::Join || !p.scope.applies(sel) {
                    continue;
                }
                users.extend(
                    self.store
                        .members(room_id, Some("join"))
                        .await?
                        .into_iter()
                        .map(|m| m.user_id),
                );
            }
        }
        users.retain(|u| self.is_local(u));

        let wanted = |field: &str| {
            p.fields
                .as_ref()
                .is_none_or(|f| f.iter().any(|x| x == field))
        };
        let value_of = |profile: &asaroton_store::User, field: &str| -> Option<Value> {
            match field {
                "displayname" => profile.display_name.clone().map(Value::String),
                "avatar_url" => profile.avatar_url.clone().map(Value::String),
                other => profile.profile_fields.get(other).cloned(),
            }
        };
        let previous = base.and_then(|b| b.profile_users.as_ref());
        let mut out: BTreeMap<OwnedUserId, Option<ProfileDelta>> = BTreeMap::new();

        for who in &users {
            if previous.is_some_and(|prev| prev.contains(who)) {
                continue;
            }
            let Some(profile) = self.store.get_user(who).await? else {
                continue;
            };
            let mut updated = BTreeMap::new();
            for field in ["displayname", "avatar_url"]
                .into_iter()
                .chain(profile.profile_fields.keys().map(String::as_str))
            {
                if wanted(field)
                    && let Some(v) = value_of(&profile, field)
                {
                    updated.insert(field.to_owned(), v);
                }
            }
            if !updated.is_empty() {
                out.insert(
                    who.clone(),
                    Some(ProfileDelta {
                        updated,
                        removed: Vec::new(),
                    }),
                );
            }
        }

        if let (Some(prev), Some(b)) = (previous, base) {
            let changed = self
                .store
                .profile_updates_between(b.token.profile, now.profile, p.fields.as_deref())
                .await?;
            let mut profiles: HashMap<OwnedUserId, Option<asaroton_store::User>> = HashMap::new();
            for (who, field) in changed {
                if !users.contains(&who) || !prev.contains(&who) {
                    continue;
                }
                if !profiles.contains_key(&who) {
                    profiles.insert(who.clone(), self.store.get_user(&who).await?);
                }
                let value = profiles
                    .get(&who)
                    .and_then(Option::as_ref)
                    .and_then(|profile| value_of(profile, &field));
                if let Some(delta) = out
                    .entry(who)
                    .or_insert_with(|| Some(ProfileDelta::default()))
                {
                    delta.removed.retain(|f| *f != field);
                    delta.updated.remove(&field);
                    match value {
                        Some(v) => {
                            delta.updated.insert(field, v);
                        }
                        None => delta.removed.push(field),
                    }
                }
            }
            for gone in prev.difference(&users) {
                out.insert(gone.clone(), None);
            }
        }
        Ok((out, users))
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
        newest: Option<i64>,
        sticky_senders: &HashSet<OwnedUserId>,
    ) -> Result<RoomResult> {
        let user = &req.user_id;
        let room_id = &sel.room_id;
        let m = &sel.membership;
        // A room this connection has only been sent as an invitation or a knock starts over
        // when the user joins: all it got was stripped state, and now there is history to
        // read. Sent as an ordinary update, the join would look like the start of the room
        // and the client would have no token to page back with. Someone who comes back to a
        // room they had left starts over as well: what the connection has of the room is the
        // room as it was when they went, and what happened since is not all theirs to read.
        let initial = track.is_none_or(|t| {
            t.membership != MembershipState::Join && m.membership == MembershipState::Join
        });
        // The same for someone who came back and went again between two answers: the
        // connection knows them as gone, and they are gone, but by a later event than the
        // one it was told about, and in between they were a member. The room is theirs as it
        // was at this later leave.
        let back_and_gone = !initial
            && track.is_some_and(|t| {
                t.membership != MembershipState::Join
                    && matches!(m.membership, MembershipState::Leave | MembershipState::Ban)
                    && m.stream_ordering > t.membership_at
            })
            && self.left_as_member(room_id, user).await?;
        let initial = initial || back_and_gone;
        let mut lists: Vec<String> = Vec::with_capacity(sel.lists.len());
        for name in &sel.lists {
            if !lists.contains(name) {
                lists.push(name.clone());
            }
        }
        let mut out = RoomResult {
            lists,
            initial,
            is_dm,
            is_dm_changed: !initial
                && track
                    .and_then(|t| t.shown.as_ref())
                    .is_some_and(|s| s.is_dm != is_dm),
            bump_stamp: u64::try_from(bump).unwrap_or(0),
            read_upto: now.events,
            ..RoomResult::new(m.membership.clone())
        };
        // What the connection was given of the room as a room (not an invite's or a knock's
        // stripped state), for telling it that the name or the avatar went.
        let shown_before = track
            .filter(|t| {
                !initial
                    && !matches!(
                        t.membership,
                        MembershipState::Invite | MembershipState::Knock
                    )
            })
            .and_then(|t| t.shown.as_ref());
        // An invite or a pending knock shows the room's stripped state and nothing else: the
        // user is not in the room, so its timeline and state are not theirs to read. (A knock
        // used to fall through to the joined path below, which let anyone who could knock on
        // a room read it through a room subscription.)
        if matches!(
            m.membership,
            MembershipState::Invite | MembershipState::Knock
        ) {
            let stripped = self.stripped_state(room_id, Some(user)).await?;
            // The room's name and avatar from what the user may see of it (clients name an
            // invited or knocked room from these, as from a joined room's).
            let content_of = |ty: &str, field: &str| {
                stripped
                    .iter()
                    .find(|e| e.get("type").and_then(Value::as_str) == Some(ty))
                    .and_then(|e| e.get("content")?.get(field)?.as_str())
                    .filter(|s| !s.is_empty())
                    .map(str::to_owned)
            };
            out.name = content_of("m.room.name", "name");
            out.avatar = content_of("m.room.avatar", "url");
            out.invite_state = Some(stripped);
            return Ok(out);
        }
        // Anything but `join` from here on is a room the user is no longer in (left, kicked or
        // banned). Such a room is frozen at the membership event that took them out: the
        // timeline ends there, and so do its state, name and avatar.
        let left = m.membership != MembershipState::Join;
        // The room is shown as of one point, the same for its timeline and its state, at
        // which the user is in it as the answer says: the answer's position, or the user's
        // membership if that came later (the memberships are read after the position). A
        // room sent for the first time goes on to its newest event as read with the
        // memberships, so that a room `createRoom` just finished comes whole. A change that
        // commits while the answer is built comes in the next one, timeline and state
        // together; were the state read as it is by then, a leave could reach
        // `required_state` in this answer and be held back from the next, which reports the
        // room as left (tracker #104).
        let upper = if left {
            m.stream_ordering
        } else if initial {
            now.events.max(m.stream_ordering).max(newest.unwrap_or(0))
        } else {
            now.events.max(m.stream_ordering)
        };
        out.read_upto = now.events.max(upper);
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
        // The user's own events: `unsigned.delay_id` for those sent from a delayed event (to
        // every device), otherwise `unsigned.transaction_id` for the device that sent them.
        let own: Vec<OwnedEventId> = events
            .iter()
            .filter(|ev| ev.pdu.sender() == user)
            .map(|ev| ev.pdu.event_id().to_owned())
            .collect();
        out.delay_ids = self.store.delay_ids_for_events(user, &own).await?;
        if let Some(device_id) = &req.device_id {
            for ev in &events {
                if ev.pdu.sender() == user
                    && !out.delay_ids.contains_key(ev.pdu.event_id())
                    && let Some(txn) = self
                        .store
                        .get_event_txn_id(ev.pdu.event_id(), user, Some(device_id))
                        .await?
                {
                    out.txn_ids.insert(ev.pdu.event_id().to_owned(), txn);
                }
            }
        }

        // Required state: the whole current state for a first request, or for one that asks
        // for more state than before; afterwards the changes since the last position. Events
        // the timeline also carries are included either way, as Synapse does: the Rust SDK
        // reads room state from `required_state` only and ignores state events in the
        // timeline.
        let expanded = track.is_some_and(|t| sel.required_state.expands(&t.required_state));
        let full = initial || expanded;
        // What the connection has of the room's state. A room that starts over starts with
        // nothing.
        let given = track.filter(|_| !initial).map(|t| &t.state_given);
        // Lazily loaded members: the people this timeline is about, and the people the
        // connection was given before, whose changes it gets from then on.
        // Senders of sticky events count as timeline senders (MSC4480).
        let mut about = timeline_members(&events);
        about.extend(sticky_senders.iter().cloned());
        let lazy = sel.required_state.loads_members_lazily();
        // `/v4/sync` adds a third kind (see `sends_every_membership_change`). The pool of an
        // answer that is not full holds only what changed.
        let every_change = sends_every_membership_change(req.dialect, full, limit, more);
        let lazy_member = |u: &UserId| {
            every_change || about.contains(u) || given.is_some_and(|g| g.has_member(u))
        };
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
        // State the connection has that the room no longer has, for `/v4/sync`'s stubs. A
        // room the user is out of is shown as it was at their leave, and gets none.
        let stub_given = given.filter(|_| req.dialect == Dialect::V4 && !left);
        let mut gone: Vec<(String, String)> = Vec::new();
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
            let current = self.store.current_state_pdus(room_id).await?;
            // Sent in full again (more state asked for): what the connection has and the
            // room lost goes as stubs here too, or it would stay with the client for good.
            if let Some(g) = stub_given {
                gone = self.gone_keys(room_id, g, &current).await?;
            }
            self.state_as_of(room_id, upper, current).await?
        } else if let Some(l) = lower {
            // What changed since the last position, and what differs from what the
            // connection holds. The second covers state that fell back to an earlier event:
            // in a room with two loose ends the connection may have been given the outcome
            // of one resolution, and a later event tips it back to what the room's newest
            // event already had, so that "then" and "now" look the same.
            let (changed, removed) = self
                .state_changes_and_removed(
                    room_id,
                    l,
                    |e| given.is_some_and(|g| g.has_another(e)),
                    stub_given.map(|g| &**g),
                )
                .await?;
            gone = removed;
            self.state_as_of(room_id, upper, changed).await?
        } else {
            Vec::new()
        };
        let mut state: Vec<StoredEvent> = pool
            .into_iter()
            .filter(|e| required_state_matches(&sel.required_state, e, user, lazy_member))
            .collect();
        if lazy && !full {
            // Someone the timeline is about whose membership the connection does not have
            // gets it sent along, also when it did not change: a person who speaks for the
            // first time, whoever removed the user, or the people of older events when the
            // timeline got longer. (In a full answer the filter above has seen to that.)
            let have: HashSet<&str> = state
                .iter()
                .filter(|e| e.pdu.event_type() == &TimelineEventType::RoomMember)
                .filter_map(|e| e.pdu.state_key())
                .collect();
            let wanted: Vec<(StateEventType, String)> = about
                .iter()
                .filter(|u| !given.is_some_and(|g| g.has_member(u)) && !have.contains(u.as_str()))
                .map(|u| (StateEventType::RoomMember, u.to_string()))
                .collect();
            if wanted.is_empty() {
            } else if !left {
                let found = self.store.current_state_pdus_for(room_id, &wanted).await?;
                state.extend(self.state_as_of(room_id, upper, found).await?);
            } else if let Some(group) = leave_state {
                // A room the user is out of is not read past their leave.
                let ids: Vec<OwnedEventId> = self
                    .store
                    .state_at_group_filtered(group, &wanted)
                    .await?
                    .into_iter()
                    .map(|e| e.event_id)
                    .collect();
                state.extend(self.store.get_events(&ids).await?);
            }
        }
        // The answer that reports the room as left, or as left again, carries the user's own
        // membership event when the client asks for it, whatever the connection was given
        // before: clients built on the Rust SDK read their membership from `required_state`
        // only, and take a room without it as joined.
        let own = |e: &StoredEvent| {
            e.pdu.event_type() == &TimelineEventType::RoomMember
                && e.pdu.state_key() == Some(user.as_str())
        };
        let reported_gone = left && (initial || track.is_none_or(|t| t.membership != m.membership));
        if reported_gone
            && !state.iter().any(&own)
            && let Some(ev) = self.store.get_event(&m.event_id).await?
            && required_state_matches(&sel.required_state, &ev, user, |_| false)
        {
            state.push(ev);
        }
        // State the connection has is not sent again (see `StateGiven`).
        if let Some(g) = given {
            state.retain(|e| !g.has(e) || (reported_gone && own(e)));
        }
        // State that is gone, that the connection has and that the client asks for: a stub
        // tells the client to drop it (`/v4/sync` only; `gone` is empty otherwise).
        if let Some(g) = given {
            out.stubs = gone
                .into_iter()
                .filter(|(ty, key)| {
                    g.has_key(ty, key)
                        && sel.required_state.matches(ty, key, user, lazy_member)
                        && !state.iter().any(|e| {
                            e.pdu
                                .state_tuple()
                                .is_some_and(|(t, k)| t.to_string() == *ty && k == key)
                        })
                })
                .collect();
        }
        out.state_given = StateGiven::with(given, &state, &about, user);
        out.state_given = StateGiven::without(&out.state_given, &out.stubs);
        out.required_state = state;

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
            (out.name_cleared, out.avatar_cleared) = cleared(shown_before, &out);
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
        (out.name_cleared, out.avatar_cleared) = cleared(shown_before, &out);
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
        // `/v4/sync` sends no notification counts (open question 9 of the v4 plan).
        if req.dialect == Dialect::Unstable {
            match self.unread_counts(room_id, user, upper).await {
                Ok(c) => {
                    out.notification_count = c.total.notification_count;
                    out.highlight_count = c.total.highlight_count;
                }
                Err(e) => self.unread_counts_failed(room_id, &e),
            }
        }
        out.timeline = events;
        Ok(out)
    }

    /// State events read from the current state, as they were at stream position `upper`: an
    /// entry written after it (a change that committed while the answer was being built) is
    /// replaced by the room's entry at `upper`, or left out when it had none then. The entry
    /// at `upper` is the state after the room's newest event up to there.
    async fn state_as_of(
        &self,
        room_id: &RoomId,
        upper: i64,
        events: Vec<StoredEvent>,
    ) -> Result<Vec<StoredEvent>> {
        if events.iter().all(|e| e.stream_ordering <= upper) {
            return Ok(events);
        }
        let (mut kept, late): (Vec<StoredEvent>, Vec<StoredEvent>) =
            events.into_iter().partition(|e| e.stream_ordering <= upper);
        let keys: Vec<(StateEventType, String)> = late
            .iter()
            .filter_map(|e| e.pdu.state_tuple().map(|(t, k)| (t, k.to_owned())))
            .collect();
        let group = self
            .store
            .room_events_in_range(room_id, upper + 1, None, Direction::Backwards, 1)
            .await?
            .first()
            .and_then(|e| e.state_group);
        if let Some(group) = group {
            let ids: Vec<OwnedEventId> = self
                .store
                .state_at_group_filtered(group, &keys)
                .await?
                .into_iter()
                .map(|e| e.event_id)
                .filter(|id| !kept.iter().any(|e| e.pdu.event_id() == id))
                .collect();
            kept.extend(self.store.get_events(&ids).await?);
        }
        Ok(kept)
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
        self.state_changes_and(room_id, since, |_| false).await
    }

    /// Current state events that differ from the state as of stream position `since`, and
    /// those that `also` picks.
    async fn state_changes_and(
        &self,
        room_id: &RoomId,
        since: i64,
        also: impl Fn(&StoredEvent) -> bool,
    ) -> Result<Vec<StoredEvent>> {
        Ok(self
            .state_changes_and_removed(room_id, since, also, None)
            .await?
            .0)
    }

    /// [`Services::state_changes_and`], and, given what a connection has (`gone_of`), the
    /// state keys of it that the current state no longer has (see [`Services::gone_keys`]).
    async fn state_changes_and_removed(
        &self,
        room_id: &RoomId,
        since: i64,
        also: impl Fn(&StoredEvent) -> bool,
        gone_of: Option<&StateGiven>,
    ) -> Result<(Vec<StoredEvent>, Vec<(String, String)>)> {
        let at = self
            .store
            .room_events_in_range(room_id, since + 1, None, Direction::Backwards, 1)
            .await?;
        let current = self.store.current_state_pdus(room_id).await?;
        let gone = match gone_of {
            Some(given) => self.gone_keys(room_id, given, &current).await?,
            None => Vec::new(),
        };
        let Some(group) = at.first().and_then(|e| e.state_group) else {
            return Ok((current, gone));
        };
        let before: HashMap<(StateEventType, String), String> = self
            .store
            .state_at_group(group)
            .await?
            .into_iter()
            .map(|e| ((e.event_type, e.state_key), e.event_id.to_string()))
            .collect();
        let changed = current
            .into_iter()
            .filter(|e| {
                also(e)
                    || e.pdu.state_tuple().is_none_or(|(t, k)| {
                        before.get(&(t, k.to_owned())).map(String::as_str)
                            != Some(e.pdu.event_id().as_str())
                    })
            })
            .collect();
        Ok((changed, gone))
    }

    /// The state keys a connection has (`given`) that the room's current state (`current`, as
    /// read) no longer has: state a state reset took away. Each is read once more by itself:
    /// `current` is joined with the events table, and an entry missing there is no reason to
    /// tell a client that the state is gone.
    async fn gone_keys(
        &self,
        room_id: &RoomId,
        given: &StateGiven,
        current: &[StoredEvent],
    ) -> Result<Vec<(String, String)>> {
        let mut now: HashMap<String, HashSet<&str>> = HashMap::new();
        for e in current {
            if let Some((ty, key)) = e.pdu.state_tuple() {
                now.entry(ty.to_string()).or_default().insert(key);
            }
        }
        let mut gone = Vec::new();
        for (ty, keys) in &given.0 {
            for key in keys.keys() {
                if now.get(ty).is_some_and(|k| k.contains(key.as_str())) {
                    continue;
                }
                if self
                    .store
                    .current_state_event(room_id, &StateEventType::from(ty.as_str()), key)
                    .await?
                    .is_none()
                {
                    gone.push((ty.clone(), key.clone()));
                }
            }
        }
        Ok(gone)
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

    /// The tags and the space children that the request's list filters look at (see
    /// [`UserFacts`]); nothing is read for a request whose lists do not filter by them.
    async fn user_facts(
        &self,
        user: &UserId,
        req: &SlidingSyncRequest,
        memberships: &[Membership],
    ) -> Result<UserFacts> {
        let filters = || req.lists.values().filter_map(|l| l.filters.as_ref());
        let mut out = UserFacts::default();
        if filters().any(|f| !f.tags.is_empty() || !f.not_tags.is_empty()) {
            for ad in self.store.account_data_of_type(user, "m.tag").await? {
                let Some(room_id) = ad.room_id else {
                    continue;
                };
                let tags: HashSet<String> = ad
                    .content
                    .get("tags")
                    .and_then(Value::as_object)
                    .map(|t| t.keys().cloned().collect())
                    .unwrap_or_default();
                if !tags.is_empty() {
                    out.tags.insert(room_id, tags);
                }
            }
        }
        let spaces: BTreeSet<&OwnedRoomId> = filters().flat_map(|f| &f.spaces).collect();
        for space in spaces {
            // Only spaces the user has joined count (MSC4186); their children are the rooms
            // named by `m.space.child` with a `via`, and not the children of subspaces.
            if !memberships
                .iter()
                .any(|m| m.room_id == *space && m.membership == MembershipState::Join)
            {
                continue;
            }
            let children: HashSet<OwnedRoomId> = self
                .store
                .current_state_pdus_of_type(space, &StateEventType::SpaceChild)
                .await?
                .into_iter()
                .filter(|e| {
                    serde_json::from_str::<Value>(e.pdu.content().get())
                        .ok()
                        .and_then(|c| c.get("via")?.as_array().map(|v| !v.is_empty()))
                        .unwrap_or(false)
                })
                .filter_map(|e| OwnedRoomId::try_from(e.pdu.state_key()?).ok())
                .collect();
            out.space_children.insert(space.clone(), children);
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
    fn an_extension_covers_all_lists_and_all_subscriptions_unless_it_says_otherwise() {
        let room = OwnedRoomId::try_from("!r:x.test").unwrap();
        let other = OwnedRoomId::try_from("!o:x.test").unwrap();
        let in_list = ["all_rooms".to_owned()];
        let scope = |lists: Option<&[&str]>, rooms: Option<Vec<ScopeRoom>>| ExtensionScope {
            lists: lists.map(|l| l.iter().map(|s| (*s).to_owned()).collect()),
            rooms,
        };
        // Nothing said: every room of the request.
        assert!(scope(None, None).applies_to(&room, &in_list, false));
        assert!(scope(None, None).applies_to(&room, &[], true));
        // Only `rooms` said (what the Rust SDK and the report's client send): the lists are
        // still all of them. This is the case that left a list's rooms without receipts.
        let all_subscribed = || Some(vec![ScopeRoom::AllSubscribed]);
        assert!(scope(None, all_subscribed()).applies_to(&room, &in_list, false));
        assert!(scope(None, all_subscribed()).applies_to(&room, &[], true));
        // Only `lists` said: the subscriptions are still all of them.
        assert!(scope(Some(&["all_rooms"]), None).applies_to(&room, &[], true));
        assert!(scope(Some(&["*"]), None).applies_to(&room, &in_list, false));
        // Said and not matching.
        assert!(!scope(Some(&["favourites"]), None).applies_to(&room, &in_list, false));
        assert!(!scope(Some(&[]), Some(vec![])).applies_to(&room, &in_list, true));
        let only_other = Some(vec![ScopeRoom::Room(other.clone())]);
        assert!(!scope(Some(&[]), only_other).applies_to(&room, &in_list, true));
        assert!(
            scope(Some(&[]), Some(vec![ScopeRoom::Room(room.clone())])).applies_to(
                &room,
                &[],
                false
            )
        );
    }

    #[test]
    fn v4_rooms_name_subscriptions_only() {
        let room = OwnedRoomId::try_from("!r:x.test").unwrap();
        let in_list = ["all_rooms".to_owned()];
        let scope = ExtensionScope {
            lists: Some(Vec::new()),
            rooms: Some(vec![ScopeRoom::Subscribed(room.clone())]),
        };
        assert!(scope.applies_to(&room, &[], true), "subscribed");
        assert!(
            !scope.applies_to(&room, &in_list, false),
            "in a list but not subscribed: no (MSC4508)"
        );
        // The unstable endpoint's `Room` counts either way.
        let unstable = ExtensionScope {
            lists: Some(Vec::new()),
            rooms: Some(vec![ScopeRoom::Room(room.clone())]),
        };
        assert!(unstable.applies_to(&room, &in_list, false));
    }

    #[test]
    fn a_v4_to_device_since_is_one_the_server_gave_out() {
        assert_eq!(to_device_since(None, 5).ok(), Some(0));
        assert_eq!(to_device_since(Some("0"), 5).ok(), Some(0));
        assert_eq!(to_device_since(Some("5"), 5).ok(), Some(5));
        for bad in ["6", "-1", "abc", "", "1.5"] {
            assert!(to_device_since(Some(bad), 5).is_err(), "{bad:?}");
        }
    }

    /// A state event for these tests: only its type, state key and ID matter.
    fn state_event(ty: &str, key: &str, n: u32) -> StoredEvent {
        let rules = asaroton_core::room_version::rules(&ruma::RoomVersionId::V11).unwrap();
        let key_pair = asaroton_core::SigningKey::generate("t").unwrap();
        let pdu = asaroton_core::builder::PduBuilder::new(
            OwnedUserId::try_from("@a:x.test").unwrap(),
            TimelineEventType::from(ty),
        )
        .room_id(OwnedRoomId::try_from("!r:x.test").unwrap())
        .state_key(key)
        .content(&json!({ "n": n }))
        .unwrap()
        .depth(ruma::UInt::from(n))
        .build(&rules, &key_pair, ruma::server_name!("x.test"))
        .unwrap();
        StoredEvent {
            pdu,
            stream_ordering: i64::from(n),
            state_group: None,
            soft_failed: false,
            outlier: false,
            redacted: false,
            received_ts: 0,
        }
    }

    #[test]
    fn a_connection_knows_which_event_it_holds_for_a_state_key() {
        let user = OwnedUserId::try_from("@a:x.test").unwrap();
        let (name, renamed) = (
            state_event("m.room.name", "", 1),
            state_event("m.room.name", "", 2),
        );
        let topic = state_event("m.room.topic", "", 3);
        let given = StateGiven::with(None, std::slice::from_ref(&name), &HashSet::new(), &user);
        assert!(given.has(&name) && !given.has(&renamed) && !given.has(&topic));
        // Another event for a key it holds, as against a key it holds nothing for.
        assert!(given.has_another(&renamed) && !given.has_another(&name));
        assert!(!given.has_another(&topic));
        let given = StateGiven::with(
            Some(&given),
            std::slice::from_ref(&renamed),
            &HashSet::new(),
            &user,
        );
        assert!(given.has(&renamed) && given.has_another(&name));
    }

    #[test]
    fn what_a_connection_remembers_is_bounded() {
        let user = OwnedUserId::try_from("@a:x.test").unwrap();
        let speaker = OwnedUserId::try_from("@speaker:x.test").unwrap();
        let about: HashSet<OwnedUserId> = [speaker.clone()].into();
        let name = state_event("m.room.name", "", 1);
        // A space with more children than there is room for, asked for with a wildcard.
        let more_than_fits = u32::try_from(MAX_STATE_REMEMBERED).unwrap() + 10;
        let children: Vec<StoredEvent> = (0..more_than_fits)
            .map(|n| state_event("m.space.child", &format!("!child{n}:x.test"), n + 10))
            .collect();
        let members = [
            state_event("m.room.member", user.as_str(), 2),
            state_event("m.room.member", speaker.as_str(), 3),
            state_event("m.room.member", "@quiet:x.test", 4),
        ];
        let mut sent = vec![name.clone()];
        sent.extend(members.iter().cloned());
        sent.extend(children.iter().cloned());
        let given = StateGiven::with(None, &sent, &about, &user);
        assert!(!given.has(&children[0]), "the children are not remembered");
        assert!(given.has(&name), "the name is");
        assert!(
            members.iter().all(|m| given.has(m)),
            "and so are the members: forgetting the children made room"
        );

        // More members than there is room for: those of the newest timeline stay.
        let crowd: Vec<StoredEvent> = (0..more_than_fits)
            .map(|n| state_event("m.room.member", &format!("@u{n}:x.test"), n + 10))
            .collect();
        let given = StateGiven::with(Some(&given), &crowd, &about, &user);
        assert!(given.has(&members[0]) && given.has(&members[1]) && given.has(&name));
        assert!(!given.has(&members[2]) && !given.has(&crowd[0]));
    }

    #[test]
    fn a_name_or_avatar_that_went_is_cleared() {
        let now = |name: Option<&str>, avatar: Option<&str>| {
            let mut r = RoomResult::new(MembershipState::Join);
            r.name = name.map(str::to_owned);
            r.avatar = avatar.map(str::to_owned);
            r
        };
        let shown = |name, avatar| Shown::of(&now(name, avatar));
        let had = shown(Some("Room"), Some("mxc://x.test/a"));
        assert_eq!(cleared(Some(&had), &now(None, None)), (true, true));
        assert_eq!(cleared(Some(&had), &now(None, Some(""))), (true, true));
        assert_eq!(
            cleared(Some(&had), &now(Some("Other"), Some("mxc://x.test/b"))),
            (false, false)
        );
        assert_eq!(
            cleared(Some(&shown(None, None)), &now(None, None)),
            (false, false)
        );
        assert_eq!(cleared(None, &now(None, None)), (false, false));
        // An avatar with an empty URL is shown as none, so it compares equal to none.
        assert_eq!(shown(None, Some("")), shown(None, None));
        assert_ne!(shown(None, Some("mxc://x.test/a")), shown(None, None));
        let mut in_lists = now(None, None);
        in_lists.lists = vec!["fav".to_owned()];
        assert_ne!(Shown::of(&in_lists), shown(None, None), "lists count");
    }

    #[test]
    fn every_membership_change_only_on_v4_answers_without_a_gap() {
        // dialect, full, timeline limit, limited
        let rule = sends_every_membership_change;
        assert!(
            rule(Dialect::V4, false, 5, false),
            "an incremental answer, no gap"
        );
        assert!(!rule(Dialect::V4, false, 5, true), "limited");
        assert!(
            !rule(Dialect::V4, true, 5, false),
            "full: initial or expanded"
        );
        assert!(!rule(Dialect::V4, false, 0, false), "a timeline limit of 0");
        for (full, limit, limited) in [(false, 5, false), (false, 0, false), (true, 1, true)] {
            assert!(
                !rule(Dialect::Unstable, full, limit, limited),
                "the unstable endpoint keeps its rule"
            );
        }
    }

    #[test]
    fn only_the_newest_request_leaves_its_state_behind() {
        let state = |seq| ConnState {
            seq,
            token: SyncToken::default(),
            rooms: HashMap::new(),
            list_counts: BTreeMap::new(),
            profile_users: None,
            put_out: HashMap::new(),
            sticky_upto: None,
            account_data_upto: None,
            receipts_upto: None,
            typing_scope: None,
        };
        let conns = SlidingSyncConnections::default();
        let key = ConnKey {
            dialect: Dialect::Unstable,
            user: OwnedUserId::try_from("@a:x.test").unwrap(),
            device: None,
            conn_id: "c".to_owned(),
        };
        let (base, first, nonce) = conns.resolve(&key, None).unwrap();
        assert!(base.is_none());
        conns.store_pending(&key, state(1), first, &nonce);
        let pos = |seq| format_pos(&SyncToken::default(), &nonce, seq);

        // A request that waits, and one that the client makes after giving up on it.
        let (_, waiting, _) = conns.resolve(&key, Some(&pos(1))).unwrap();
        let (_, newer, _) = conns.resolve(&key, Some(&pos(1))).unwrap();
        conns.store_pending(&key, state(2), newer, &nonce);
        // The first one finishes late, with other content under the same number.
        let mut late = state(2);
        late.list_counts.insert("late".to_owned(), 1);
        conns.store_pending(&key, late, waiting, &nonce);
        let (confirmed, _, _) = conns.resolve(&key, Some(&pos(2))).unwrap();
        assert!(
            confirmed.is_some_and(|s| s.list_counts.is_empty()),
            "the client confirms what the newer request told it"
        );
    }

    #[test]
    fn a_pos_is_tied_to_its_connection_and_reads_as_a_sync_token() {
        let conns = SlidingSyncConnections::default();
        let key = ConnKey {
            dialect: Dialect::Unstable,
            user: OwnedUserId::try_from("@a:x.test").unwrap(),
            device: None,
            conn_id: "c".to_owned(),
        };
        let token = SyncToken {
            events: 42,
            ..SyncToken::default()
        };
        let (_, first, nonce) = conns.resolve(&key, None).unwrap();
        let mut state = ConnState {
            seq: 1,
            token,
            rooms: HashMap::new(),
            list_counts: BTreeMap::new(),
            profile_users: None,
            put_out: HashMap::new(),
            sticky_upto: None,
            account_data_upto: None,
            receipts_upto: None,
            typing_scope: None,
        };
        conns.store_pending(&key, state.clone(), first, &nonce);
        let pos = format_pos(&token, &nonce, 1);
        assert_eq!(SyncToken::parse(&pos).map(|t| t.events), Some(42));
        assert_eq!(
            crate::rooms::timeline::StreamToken::parse(&pos).map(|t| t.0),
            Some(42)
        );
        assert!(conns.resolve(&key, Some(&pos)).is_ok());
        // A bare number, another nonce, or the position of a connection since started over.
        assert!(conns.resolve(&key, Some("1")).is_err());
        assert!(
            conns
                .resolve(&key, Some(&format_pos(&token, "other", 1)))
                .is_err()
        );
        let (_, again, _) = conns.resolve(&key, None).unwrap();
        state.seq = 1;
        conns.store_pending(&key, state, again, &nonce);
        assert!(conns.resolve(&key, Some(&pos)).is_err());
    }

    #[test]
    fn list_filters_by_space_and_by_tag() {
        let room = |id: &str| OwnedRoomId::try_from(id).unwrap();
        let member = |id: &str| Membership {
            room_id: room(id),
            user_id: OwnedUserId::try_from("@a:x.test").unwrap(),
            membership: MembershipState::Join,
            event_id: OwnedEventId::try_from("$e").unwrap(),
            sender: OwnedUserId::try_from("@a:x.test").unwrap(),
            display_name: None,
            avatar_url: None,
            stream_ordering: 1,
            forgotten: false,
        };
        let facts = RoomFacts {
            encrypted: false,
            room_type: None,
        };
        let (child, other) = (member("!child:x.test"), member("!other:x.test"));
        let mut user = UserFacts::default();
        user.space_children
            .insert(room("!space:x.test"), [room("!child:x.test")].into());
        user.tags.insert(
            room("!child:x.test"),
            ["m.favourite".to_owned(), "u.work".to_owned()].into(),
        );
        user.tags
            .insert(room("!other:x.test"), ["m.favourite".to_owned()].into());

        let spaces = |s: &[&str]| ListFilters {
            spaces: s.iter().map(|id| room(id)).collect(),
            ..ListFilters::default()
        };
        assert!(spaces(&["!space:x.test"]).matches(&child, &facts, false, &user));
        assert!(!spaces(&["!space:x.test"]).matches(&other, &facts, false, &user));
        // A space the user has not joined (it has no entry) names no rooms: the list is
        // empty rather than unfiltered.
        assert!(!spaces(&["!unjoined:x.test"]).matches(&child, &facts, false, &user));

        let tags = |t: &[&str], not: &[&str]| ListFilters {
            tags: t.iter().map(|s| (*s).to_owned()).collect(),
            not_tags: not.iter().map(|s| (*s).to_owned()).collect(),
            ..ListFilters::default()
        };
        assert!(tags(&["m.favourite"], &[]).matches(&other, &facts, false, &user));
        assert!(tags(&["m.lowpriority", "u.work"], &[]).matches(&child, &facts, false, &user));
        assert!(!tags(&["u.work"], &[]).matches(&other, &facts, false, &user));
        // `not_tags` wins over `tags`.
        assert!(!tags(&["m.favourite"], &["u.work"]).matches(&child, &facts, false, &user));
        assert!(tags(&[], &["u.work"]).matches(&other, &facts, false, &user));
        let untagged = member("!plain:x.test");
        assert!(tags(&[], &["m.favourite"]).matches(&untagged, &facts, false, &user));
        assert!(!tags(&["m.favourite"], &[]).matches(&untagged, &facts, false, &user));
    }

    #[test]
    fn a_connection_forgets_state_it_was_told_is_gone() {
        let user = OwnedUserId::try_from("@a:x.test").unwrap();
        let topic = state_event("m.room.topic", "", 1);
        let name = state_event("m.room.name", "", 2);
        let given = StateGiven::with(None, &[topic.clone(), name.clone()], &HashSet::new(), &user);
        assert!(given.has_key("m.room.topic", "") && given.has_key("m.room.name", ""));
        let after = StateGiven::without(&given, &[("m.room.topic".to_owned(), String::new())]);
        assert!(!after.has_key("m.room.topic", "") && !after.has(&topic));
        assert!(after.has(&name));
        assert!(
            !after.0.contains_key("m.room.topic"),
            "no empty type is left behind"
        );
    }

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
