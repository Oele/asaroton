//! Domain types exchanged with the store. No SQL leaks past this module.

use ruma::{
    MilliSecondsSinceUnixEpoch, OwnedDeviceId, OwnedEventId, OwnedRoomAliasId, OwnedRoomId,
    OwnedServerName, OwnedServerSigningKeyId, OwnedUserId, RoomVersionId, UInt,
    events::StateEventType,
};
use tessera_core::Pdu;

/// Position in the event stream. Positive for live events, negative for backfilled ones.
pub type StreamOrdering = i64;

/// Identifier of a state group.
pub type StateGroupId = i64;

/// Milliseconds since the epoch, as stored.
pub type Ts = i64;

/// Current wall-clock time in Matrix milliseconds.
pub fn now_ts() -> Ts {
    i64::try_from(u64::from(MilliSecondsSinceUnixEpoch::now().0)).unwrap_or(i64::MAX)
}

// ------------------------------------------------------------------ users

/// A local user account.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct User {
    pub user_id: OwnedUserId,
    pub display_name: Option<String>,
    pub avatar_url: Option<String>,
    pub is_admin: bool,
    pub is_guest: bool,
    pub is_deactivated: bool,
    pub appservice_id: Option<String>,
    pub created_ts: Ts,
}

/// Parameters for creating a user.
#[derive(Clone, Debug)]
pub struct NewUser {
    pub user_id: OwnedUserId,
    pub password_hash: Option<String>,
    pub display_name: Option<String>,
    pub is_admin: bool,
    pub is_guest: bool,
    pub appservice_id: Option<String>,
}

/// A device belonging to a user.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Device {
    pub user_id: OwnedUserId,
    pub device_id: OwnedDeviceId,
    pub display_name: Option<String>,
    pub created_ts: Ts,
    pub last_seen_ts: Option<Ts>,
    pub last_seen_ip: Option<String>,
    pub last_seen_user_agent: Option<String>,
}

/// An access token and what it grants.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AccessToken {
    pub token: String,
    pub user_id: OwnedUserId,
    pub device_id: Option<OwnedDeviceId>,
    pub created_ts: Ts,
    pub expires_ts: Option<Ts>,
    pub refresh_token: Option<String>,
    pub puppets_user_id: Option<OwnedUserId>,
}

/// A refresh token.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RefreshToken {
    pub token: String,
    pub user_id: OwnedUserId,
    pub device_id: OwnedDeviceId,
    pub created_ts: Ts,
    pub expires_ts: Option<Ts>,
    pub next_token: Option<String>,
}

// ------------------------------------------------------------------ rooms

/// Room metadata.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RoomInfo {
    pub room_id: OwnedRoomId,
    pub room_version: RoomVersionId,
    pub creator: Option<OwnedUserId>,
    pub is_public: bool,
    pub created_ts: Ts,
}

/// A room alias mapping.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RoomAlias {
    pub alias: OwnedRoomAliasId,
    pub room_id: OwnedRoomId,
    pub creator: Option<OwnedUserId>,
}

// ------------------------------------------------------------------ events

/// An event as stored, with its position and persistence flags.
#[derive(Clone, Debug)]
pub struct StoredEvent {
    pub pdu: Pdu,
    pub stream_ordering: StreamOrdering,
    /// The state group *after* this event; `None` for outliers.
    pub state_group: Option<StateGroupId>,
    pub soft_failed: bool,
    pub outlier: bool,
    /// Set once the event has been redacted: the redacted PDU is what `pdu` holds, and this
    /// is the original.
    pub redacted: bool,
}

impl StoredEvent {
    /// The event ID.
    pub fn event_id(&self) -> &ruma::EventId {
        self.pdu.event_id()
    }
}

/// A `(type, state_key)` to event ID mapping.
pub type StateDelta = Vec<((StateEventType, String), OwnedEventId)>;

/// How to assign a state group to an event being persisted.
#[derive(Clone, Debug)]
pub enum EventState {
    /// Outlier: we know nothing about the state around this event.
    Outlier,
    /// Reuse an existing state group (non-state events share their prev event's group).
    Existing(StateGroupId),
    /// Create a new group as a delta on top of `prev` (or a full state if `prev` is `None`).
    New {
        prev: Option<StateGroupId>,
        delta: StateDelta,
    },
}

/// A change to the room's current state resulting from persisting an event.
#[derive(Clone, Debug, Default)]
pub struct CurrentStateDelta {
    /// Entries to insert or replace.
    pub upsert: StateDelta,
    /// Entries to remove (only happens after state resolution reverts something).
    pub remove: Vec<(StateEventType, String)>,
}

/// Everything the store needs to persist one event atomically. The service layer computes all
/// of it (auth, state, extremities); the store only writes.
#[derive(Clone, Debug)]
pub struct PersistEvent {
    pub pdu: Pdu,
    pub state: EventState,
    pub soft_failed: bool,
    /// Backfilled events get a negative stream ordering so they never show up as "new".
    pub backfilled: bool,
    /// Changes to current state; `None` if the event does not affect it (rejected, soft-failed,
    /// outlier, or a non-state event).
    pub current_state_delta: Option<CurrentStateDelta>,
    /// New set of forward extremities for the room, if changed.
    pub forward_extremities: Option<Vec<OwnedEventId>>,
}

/// What the store reports after persisting an event.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PersistedEvent {
    pub event_id: OwnedEventId,
    pub stream_ordering: StreamOrdering,
    pub state_group: Option<StateGroupId>,
}

/// A row of current state, including the denormalised membership.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CurrentStateEntry {
    pub event_type: StateEventType,
    pub state_key: String,
    pub event_id: OwnedEventId,
}

/// A user's membership in a room.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Membership {
    pub room_id: OwnedRoomId,
    pub user_id: OwnedUserId,
    pub membership: ruma::events::room::member::MembershipState,
    pub event_id: OwnedEventId,
    pub sender: OwnedUserId,
    pub display_name: Option<String>,
    pub avatar_url: Option<String>,
    pub stream_ordering: StreamOrdering,
    /// The user asked to forget this room (hidden from sync until they rejoin).
    pub forgotten: bool,
}

/// A read receipt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Receipt {
    pub room_id: OwnedRoomId,
    pub user_id: OwnedUserId,
    pub receipt_type: String,
    /// `None` = unthreaded.
    pub thread_id: Option<String>,
    pub event_id: OwnedEventId,
    pub data: serde_json::Value,
    pub stream_id: i64,
}

/// Direction for paginating a timeline.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Direction {
    Forwards,
    Backwards,
}

// ------------------------------------------------------------------ account data

/// One account data entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AccountData {
    pub room_id: Option<OwnedRoomId>,
    pub data_type: String,
    pub content: serde_json::Value,
    pub stream_id: i64,
}

// ------------------------------------------------------------------ keys

/// One of this server's own signing keys.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalSigningKey {
    pub key_id: OwnedServerSigningKeyId,
    pub seed_b64: String,
    pub created_ts: Ts,
    pub expired_ts: Option<Ts>,
}

/// A cached verify key of a remote server.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteVerifyKey {
    pub server_name: OwnedServerName,
    pub key_id: OwnedServerSigningKeyId,
    pub verify_key_b64: String,
    pub valid_until_ts: Ts,
    pub expired_ts: Option<Ts>,
    pub fetched_ts: Ts,
    pub fetched_from: OwnedServerName,
    pub key_json: String,
}

// ------------------------------------------------------------------ e2ee & to-device

/// A one-time or fallback key as uploaded.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OneTimeKeyRow {
    pub algorithm: String,
    pub key_id: String,
    pub key: serde_json::Value,
}

/// A to-device message waiting for a device.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ToDeviceMessage {
    pub stream_id: i64,
    pub sender: OwnedUserId,
    pub event_type: String,
    pub content: serde_json::Value,
}

/// A pusher.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Pusher {
    pub user_id: OwnedUserId,
    pub app_id: String,
    pub pushkey: String,
    pub kind: String,
    pub app_display_name: String,
    pub device_display_name: String,
    pub profile_tag: Option<String>,
    pub lang: String,
    pub data: serde_json::Value,
}

// ------------------------------------------------------------------ federation

/// A queued outbound federation item.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutboxItem {
    pub id: i64,
    pub destination: OwnedServerName,
    pub kind: OutboxKind,
}

/// What an outbox row carries.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OutboxKind {
    Pdu(OwnedEventId),
    Edu(serde_json::Value),
}

/// Retry state for a remote server.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DestinationState {
    pub failure_ts: Option<Ts>,
    pub retry_last_ts: Option<Ts>,
    pub retry_interval_ms: i64,
    pub last_successful_ts: Option<Ts>,
}

/// Cached server discovery result.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DiscoveryEntry {
    pub server_name: OwnedServerName,
    pub host: String,
    pub port: u16,
    pub host_header: String,
    pub tls_name: String,
    pub expires_ts: Ts,
}

/// Convert a `UInt` depth/timestamp into the `i64` the database wants.
pub fn uint_to_i64(v: UInt) -> i64 {
    i64::try_from(u64::from(v)).unwrap_or(i64::MAX)
}
