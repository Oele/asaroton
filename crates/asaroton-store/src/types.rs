//! Domain types exchanged with the store. No SQL leaks past this module.

use asaroton_core::Pdu;
use ruma::{
    MilliSecondsSinceUnixEpoch, OwnedDeviceId, OwnedEventId, OwnedRoomAliasId, OwnedRoomId,
    OwnedServerName, OwnedServerSigningKeyId, OwnedUserId, RoomVersionId, UInt,
    events::StateEventType,
};

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
    /// Custom profile fields (anything besides `displayname` and `avatar_url`).
    pub profile_fields: serde_json::Map<String, serde_json::Value>,
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
    /// A pre-reserved stream ordering (see `reserve_backfill_orderings`); otherwise the next
    /// value of the live or backfill sequence is used.
    pub stream_ordering: Option<StreamOrdering>,
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

impl AccountData {
    /// Deleted account data (MSC3391) is kept as an empty object so that incremental syncs can
    /// report the deletion.
    #[must_use]
    pub fn is_deleted(&self) -> bool {
        self.content
            .as_object()
            .is_some_and(serde_json::Map::is_empty)
    }
}

// ------------------------------------------------------------------ stream positions

/// The newest position of every stream a sync token tracks, read in one query.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StreamPositions {
    pub events: i64,
    pub account_data: i64,
    pub receipts: i64,
    pub to_device: i64,
    pub device_lists: i64,
    pub thread_subscriptions: i64,
}

// ------------------------------------------------------------------ thread subscriptions

/// A user's subscription state for one thread (MSC4306).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ThreadSubscription {
    pub room_id: OwnedRoomId,
    pub thread_root: OwnedEventId,
    pub subscribed: bool,
    pub automatic: bool,
    /// Bump stamp: when this state was last changed.
    pub stream_id: i64,
    /// The newest event position at the time of the last unsubscription.
    pub unsubscribed_at: Option<i64>,
}

// ------------------------------------------------------------------ delayed events

/// An event scheduled to be sent later (MSC4140).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DelayedEvent {
    pub delay_id: String,
    pub user_id: OwnedUserId,
    pub device_id: Option<OwnedDeviceId>,
    pub room_id: OwnedRoomId,
    pub event_type: String,
    pub state_key: Option<String>,
    pub content: serde_json::Value,
    pub delay_ms: i64,
    /// When the current timeout started (ms); the event is due at `running_since + delay_ms`.
    pub running_since: i64,
    pub txn_id: Option<String>,
}

// ------------------------------------------------------------------ rtc (MSC4195)

/// A participant this server issued a `LiveKit` token to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RtcParticipant {
    pub lk_room: String,
    pub lk_identity: String,
    pub user_id: OwnedUserId,
    pub room_id: OwnedRoomId,
    pub issued_ts: i64,
}

/// A delayed leave event whose restarting the server took over from the client.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DelegatedLeave {
    pub delay_id: String,
    pub user_id: OwnedUserId,
    pub room_id: OwnedRoomId,
    pub lk_room: String,
    pub lk_identity: String,
    pub created_ts: i64,
    /// When the participant was last seen connected to the SFU; `None` until first seen.
    pub connected_ts: Option<i64>,
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
    /// Access token that created the pusher (if known).
    pub access_token: Option<String>,
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

// ------------------------------------------------------------------ media

/// Stored media metadata.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MediaInfo {
    pub server_name: OwnedServerName,
    pub media_id: String,
    pub user_id: Option<OwnedUserId>,
    pub content_type: Option<String>,
    pub size: Option<i64>,
    pub upload_name: Option<String>,
    pub sha256: Option<String>,
    pub created_ts: Ts,
    /// `None` while an async upload is still pending.
    pub uploaded_ts: Option<Ts>,
    pub quarantined: bool,
}

/// A generated thumbnail.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ThumbnailInfo {
    pub width: u32,
    pub height: u32,
    pub method: String,
    pub content_type: String,
    pub size: i64,
}

/// Convert a `UInt` depth/timestamp into the `i64` the database wants.
pub fn uint_to_i64(v: UInt) -> i64 {
    i64::try_from(u64::from(v)).unwrap_or(i64::MAX)
}

/// One relation (`m.relates_to`) pointing at an event.
#[derive(Debug, Clone)]
pub struct RelationRow {
    pub event_id: OwnedEventId,
    pub rel_type: String,
    pub event_type: String,
    pub sender: OwnedUserId,
    pub origin_server_ts: i64,
    pub stream_ordering: StreamOrdering,
}

/// A thread root with the position of its latest reply.
#[derive(Debug, Clone)]
pub struct ThreadSummary {
    pub root: StoredEvent,
    pub latest_stream_ordering: StreamOrdering,
}

/// How `/search` matches text.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SearchMode {
    /// Whole words via the lexeme vector (stemmed, stop words dropped).
    #[default]
    Word,
    /// Case-insensitive substrings of the plain text.
    Substring,
    /// The union of both.
    Both,
}

/// A `/search` request as the store sees it.
#[derive(Clone, Debug)]
pub struct SearchQuery<'a> {
    pub term: &'a str,
    pub mode: SearchMode,
    /// Substring terms shorter than this are ignored.
    pub min_length: usize,
    pub order_recent: bool,
    pub offset: usize,
    pub limit: usize,
}

/// A full-text search hit.
#[derive(Debug, Clone)]
pub struct SearchHit {
    pub event: StoredEvent,
    pub rank: f64,
}

/// A key backup version.
#[derive(Debug, Clone)]
pub struct BackupVersion {
    pub version: String,
    /// `{"algorithm": ..., "auth_data": ...}` exactly as the client sent it.
    pub algorithm: serde_json::Value,
    pub etag: i64,
    pub count: i64,
}

/// One backed-up session key.
#[derive(Debug, Clone)]
pub struct BackupKey {
    pub room_id: OwnedRoomId,
    pub session_id: String,
    pub first_message_index: i64,
    pub forwarded_count: i64,
    pub is_verified: bool,
    /// The full `KeyBackupData` JSON.
    pub data: serde_json::Value,
}

impl BackupKey {
    /// Whether `new` should replace `self` (spec: verified beats unverified, then lower
    /// `first_message_index`, then lower `forwarded_count`).
    pub fn should_be_replaced_by(&self, new: &BackupKey) -> bool {
        if new.is_verified != self.is_verified {
            return new.is_verified;
        }
        if new.first_message_index != self.first_message_index {
            return new.first_message_index < self.first_message_index;
        }
        new.forwarded_count < self.forwarded_count
    }
}

/// Filters for the admin user listing.
#[derive(Clone, Debug, Default)]
pub struct UserListQuery {
    /// Substring of the user ID or display name.
    pub name: Option<String>,
    pub include_guests: bool,
    pub include_deactivated: bool,
    pub admins_only: bool,
    pub limit: i64,
    pub offset: i64,
}
