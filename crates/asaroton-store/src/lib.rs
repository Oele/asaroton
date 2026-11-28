//! Storage trait and PostgreSQL backend for the asaroton homeserver.
//!
//! Everything outside this crate talks to a `dyn Store` in domain types; SQL never leaks out.
//! The traits are split by area so each backend file stays readable; [`Store`] is the sum.

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

mod error;
pub mod postgres;
pub mod types;

use std::sync::Arc;

use asaroton_core::Pdu;
use async_trait::async_trait;
use ruma::{
    DeviceId, EventId, OwnedDeviceId, OwnedEventId, OwnedRoomId, OwnedServerName, OwnedUserId,
    RoomAliasId, RoomId, ServerName, UserId, events::StateEventType,
};

pub use error::{Error, Result};
pub use postgres::PostgresStore;
pub use types::*;

/// Shared handle to a store.
pub type DynStore = Arc<dyn Store>;

/// Users, devices and tokens.
#[async_trait]
pub trait UserStore: Send + Sync {
    async fn create_user(&self, user: NewUser) -> Result<User>;
    async fn get_user(&self, user_id: &UserId) -> Result<Option<User>>;
    async fn user_exists(&self, user_id: &UserId) -> Result<bool>;
    async fn count_users(&self) -> Result<i64>;
    async fn get_password_hash(&self, user_id: &UserId) -> Result<Option<String>>;
    async fn set_password_hash(&self, user_id: &UserId, hash: Option<&str>) -> Result<()>;
    async fn set_profile(
        &self,
        user_id: &UserId,
        display_name: Option<Option<&str>>,
        avatar_url: Option<Option<&str>>,
    ) -> Result<()>;
    /// Set (`Some`) or remove (`None`) one custom profile field.
    async fn set_profile_field(
        &self,
        user_id: &UserId,
        key: &str,
        value: Option<&serde_json::Value>,
    ) -> Result<()>;
    /// Note that a profile field of a local user changed (was set or cleared), for syncs
    /// that follow profile updates. Returns the new position.
    async fn record_profile_update(&self, user_id: &UserId, field: &str) -> Result<i64>;
    /// The profile fields, among `fields` (all of them when `None`), that changed after `since`
    /// and up to `until`: user and field, each pair once.
    async fn profile_updates_between(
        &self,
        since: i64,
        until: i64,
        fields: Option<&[String]>,
    ) -> Result<Vec<(OwnedUserId, String)>>;
    async fn set_deactivated(&self, user_id: &UserId, deactivated: bool) -> Result<()>;
    /// Marks an account as erased (`erase: true` on `/account/deactivate`) at the current end
    /// of the events stream, and returns that position. An account erased before keeps the
    /// position of its first erasure.
    async fn mark_erased(&self, user_id: &UserId, erased_ts: Ts) -> Result<StreamOrdering>;
    /// Every erased account with the events stream position of its erasure.
    async fn erased_users(&self) -> Result<Vec<(OwnedUserId, StreamOrdering)>>;
    async fn set_admin(&self, user_id: &UserId, admin: bool) -> Result<()>;
    /// A page of users (admin listing) and the total matching count.
    async fn list_users(&self, query: &UserListQuery) -> Result<(Vec<User>, i64)>;

    async fn create_device(
        &self,
        user_id: &UserId,
        device_id: &DeviceId,
        display_name: Option<&str>,
    ) -> Result<Device>;
    async fn get_device(&self, user_id: &UserId, device_id: &DeviceId) -> Result<Option<Device>>;
    async fn list_devices(&self, user_id: &UserId) -> Result<Vec<Device>>;
    async fn update_device(
        &self,
        user_id: &UserId,
        device_id: &DeviceId,
        display_name: Option<&str>,
    ) -> Result<()>;
    async fn touch_device(
        &self,
        user_id: &UserId,
        device_id: &DeviceId,
        ip: Option<&str>,
        user_agent: Option<&str>,
    ) -> Result<()>;
    /// Delete the device and every token bound to it.
    async fn delete_device(&self, user_id: &UserId, device_id: &DeviceId) -> Result<()>;

    async fn create_access_token(&self, token: AccessToken) -> Result<()>;
    async fn get_access_token(&self, token: &str) -> Result<Option<AccessToken>>;
    async fn delete_access_token(&self, token: &str) -> Result<()>;
    async fn delete_access_tokens_for_user(
        &self,
        user_id: &UserId,
        except_device: Option<&DeviceId>,
    ) -> Result<()>;
    async fn delete_access_tokens_for_device(
        &self,
        user_id: &UserId,
        device_id: &DeviceId,
    ) -> Result<()>;
    async fn create_refresh_token(&self, token: RefreshToken) -> Result<()>;
    async fn get_refresh_token(&self, token: &str) -> Result<Option<RefreshToken>>;
    async fn set_refresh_token_next(&self, token: &str, next: &str) -> Result<()>;
    async fn delete_refresh_token(&self, token: &str) -> Result<()>;
    /// The access token that was handed out with this refresh token and has not expired,
    /// the newest if there are several.
    async fn access_token_for_refresh_token(
        &self,
        refresh_token: &str,
    ) -> Result<Option<AccessToken>>;
    /// Revokes what this refresh token replaced: the refresh token before it, and the access
    /// tokens that were handed out with that one. Returns those access tokens. Called when
    /// this refresh token, or an access token handed out with it, is used.
    async fn revoke_refresh_token_predecessor(&self, refresh_token: &str) -> Result<Vec<String>>;
}

/// Rooms, aliases, and the event graph.
#[async_trait]
pub trait RoomStore: Send + Sync {
    /// Registers a room. Returns whether it is new: a room that is known already is left as
    /// it is.
    async fn create_room(&self, room: RoomInfo) -> Result<bool>;
    async fn get_room(&self, room_id: &RoomId) -> Result<Option<RoomInfo>>;
    async fn room_exists(&self, room_id: &RoomId) -> Result<bool>;
    async fn set_room_public(&self, room_id: &RoomId, public: bool) -> Result<()>;
    async fn list_public_rooms(&self, limit: i64, offset: i64) -> Result<Vec<RoomInfo>>;
    /// Name, topic and canonical alias of the public rooms (all of them, or those in `rooms`),
    /// in the order of [`list_public_rooms`](Self::list_public_rooms): what a directory search
    /// matches on.
    async fn public_rooms_text(&self, rooms: Option<&[OwnedRoomId]>)
    -> Result<Vec<PublicRoomText>>;
    /// Rooms with a state event or a redaction at a stream position after `since` and up to
    /// `upto`: those whose name, topic or canonical alias may have changed.
    async fn rooms_with_state_changes(&self, since: i64, upto: i64) -> Result<Vec<OwnedRoomId>>;
    /// A page of all rooms (admin listing, optionally filtered by ID/alias/name substring
    /// via `search`), newest first, and the total count.
    async fn list_rooms(
        &self,
        search: Option<&str>,
        limit: i64,
        offset: i64,
    ) -> Result<(Vec<RoomInfo>, i64)>;
    /// Rooms whose current state has an event of this type.
    async fn rooms_with_state_type(&self, event_type: &str) -> Result<Vec<OwnedRoomId>>;
    /// Remove every trace of a room: events, state, memberships, aliases, receipts...
    async fn purge_room(&self, room_id: &RoomId) -> Result<()>;
    /// Delete non-state events of a room with a stream position below `before` (keeping
    /// forward extremities); returns how many were deleted.
    async fn purge_history(&self, room_id: &RoomId, before: StreamOrdering) -> Result<u64>;

    async fn create_alias(&self, alias: RoomAlias) -> Result<()>;
    async fn get_alias(&self, alias: &RoomAliasId) -> Result<Option<RoomAlias>>;
    async fn delete_alias(&self, alias: &RoomAliasId) -> Result<()>;
    async fn aliases_for_room(&self, room_id: &RoomId) -> Result<Vec<RoomAlias>>;

    /// Rooms where `user_id` currently has membership `membership` ("join", "invite", ...).
    async fn rooms_for_user(&self, user_id: &UserId, membership: &str) -> Result<Vec<OwnedRoomId>>;
    async fn get_membership(
        &self,
        room_id: &RoomId,
        user_id: &UserId,
    ) -> Result<Option<Membership>>;
    async fn members(&self, room_id: &RoomId, membership: Option<&str>) -> Result<Vec<Membership>>;
    /// Servers with at least one joined user in the room.
    async fn servers_in_room(&self, room_id: &RoomId) -> Result<Vec<ruma::OwnedServerName>>;
    async fn joined_member_count(&self, room_id: &RoomId) -> Result<i64>;
    /// Everyone joined to a room the user is joined to (the user included).
    async fn users_sharing_rooms_with(&self, user_id: &UserId) -> Result<Vec<OwnedUserId>>;
    /// All memberships of a user (any state), for sync.
    async fn memberships_for_user(&self, user_id: &UserId) -> Result<Vec<Membership>>;
    /// For each of `room_ids` where `user_id` was ever joined or invited: the stream position
    /// of the first such membership event (account erasure: who was there before it).
    async fn first_member_positions(
        &self,
        user_id: &UserId,
        room_ids: &[OwnedRoomId],
    ) -> Result<std::collections::HashMap<OwnedRoomId, StreamOrdering>>;
    /// The stream position of the first join or invite of any user of `server` in the room.
    async fn first_server_member_position(
        &self,
        room_id: &RoomId,
        server: &ServerName,
    ) -> Result<Option<StreamOrdering>>;
    /// Memberships of a user whose current membership event is newer than `since`.
    async fn memberships_changed_since(
        &self,
        user_id: &UserId,
        since: StreamOrdering,
    ) -> Result<Vec<Membership>>;
    /// Memberships in the given rooms whose current membership event is newer than `since`.
    async fn memberships_changed_in_rooms(
        &self,
        room_ids: &[OwnedRoomId],
        since: StreamOrdering,
    ) -> Result<Vec<Membership>>;
    async fn set_forgotten(
        &self,
        room_id: &RoomId,
        user_id: &UserId,
        forgotten: bool,
    ) -> Result<()>;

    /// Transaction-ID de-duplication for client sends.
    async fn get_txn_event(
        &self,
        user_id: &UserId,
        device_id: Option<&DeviceId>,
        txn_id: &str,
    ) -> Result<Option<OwnedEventId>>;
    /// Records the de-duplication key and, when given, the client's transaction ID to echo
    /// back in `unsigned` for the event.
    async fn set_txn_event(
        &self,
        user_id: &UserId,
        device_id: Option<&DeviceId>,
        txn_key: &str,
        event_id: &EventId,
        client_txn_id: Option<&str>,
    ) -> Result<()>;
    async fn get_event_txn_id(
        &self,
        event_id: &EventId,
        user_id: &UserId,
        device_id: Option<&DeviceId>,
    ) -> Result<Option<String>>;

    /// Receipts.
    async fn set_receipt(&self, receipt: Receipt) -> Result<i64>;
    async fn receipts_for_room(&self, room_id: &RoomId, since: i64) -> Result<Vec<Receipt>>;
    /// Receipts in any room newer than `since`, oldest first (bounded by `limit`).
    async fn receipts_since(&self, since: i64, limit: usize) -> Result<Vec<Receipt>>;
    /// Of the given rooms, those with receipts newer than `since`.
    async fn rooms_with_receipts_since(
        &self,
        room_ids: &[OwnedRoomId],
        since: i64,
    ) -> Result<Vec<OwnedRoomId>>;
    /// One user's receipts in a room (their read positions).
    async fn user_receipts(&self, room_id: &RoomId, user_id: &UserId) -> Result<Vec<Receipt>>;
    /// One user's read receipts (`m.read`, `m.read.private`) in every room, as the stream
    /// position of the event each points at: `(room, thread, position)`. Receipts for events
    /// this server does not have are left out.
    async fn user_read_positions(
        &self,
        user_id: &UserId,
    ) -> Result<Vec<(OwnedRoomId, Option<String>, StreamOrdering)>>;
    async fn max_receipt_stream_id(&self) -> Result<i64>;
}

/// Events and state.
#[async_trait]
pub trait EventStore: Send + Sync {
    /// The room's sticky events (MSC4354) that are still sticky at `now`, with a stream
    /// position in `(after, upto]`, oldest first, at most `limit`.
    async fn sticky_events(
        &self,
        room_id: &RoomId,
        now: Ts,
        after: StreamOrdering,
        upto: StreamOrdering,
        limit: usize,
    ) -> Result<Vec<StoredEvent>>;
    /// Forget the sticky events that expired before `now`; returns how many.
    async fn prune_expired_sticky(&self, now: Ts) -> Result<u64>;
    /// The room's soft-failed sticky events that are still within their sticky time, oldest
    /// first: to be checked again against the room's current state.
    async fn soft_failed_sticky_events(
        &self,
        room_id: &RoomId,
        now: Ts,
    ) -> Result<Vec<StoredEvent>>;
    /// Accept a soft-failed sticky event after all (its authorising state changed): it gets a
    /// fresh place in the stream, as a newly arrived event would. `None` when it was no longer
    /// soft-failed.
    async fn accept_soft_failed_sticky(&self, ev: &StoredEvent) -> Result<Option<StreamOrdering>>;

    /// Persist one event with everything that goes with it, atomically.
    async fn persist_event(&self, event: PersistEvent) -> Result<PersistedEvent>;
    /// Persist several events in one transaction, in order.
    async fn persist_events(&self, events: Vec<PersistEvent>) -> Result<Vec<PersistedEvent>>;

    /// The event closest to `ts` in the given direction (forwards: the earliest event at or
    /// after `ts`; backwards: the latest at or before), ties broken by stream order.
    async fn event_at_timestamp(
        &self,
        room_id: &RoomId,
        ts: i64,
        direction: Direction,
    ) -> Result<Option<StoredEvent>>;
    /// Live (non-backfilled, non-outlier, non-rejected) events after a global stream position.
    async fn events_after(&self, after: StreamOrdering, limit: usize) -> Result<Vec<StoredEvent>>;
    /// Newest live stream ordering per room, optionally counting only the given event types.
    async fn latest_stream_orderings(
        &self,
        room_ids: &[OwnedRoomId],
        event_types: Option<&[String]>,
    ) -> Result<std::collections::HashMap<OwnedRoomId, StreamOrdering>>;
    /// The last stream position delivered to an appservice (0 if none).
    async fn appservice_position(&self, appservice_id: &str) -> Result<StreamOrdering>;
    async fn set_appservice_position(
        &self,
        appservice_id: &str,
        position: StreamOrdering,
    ) -> Result<()>;
    /// Reserve `n` stream orderings from the backfill sequence (descending), so a batch of
    /// backfilled events can be stored oldest-first yet ordered correctly.
    async fn reserve_backfill_orderings(&self, n: usize) -> Result<Vec<StreamOrdering>>;

    /// The position of every stream that a sync can be told: the newest one below which no
    /// write is still under way.
    async fn stream_positions(&self) -> Result<StreamPositions>;
    /// Whether a sync can be told about `position` of `stream`, where the caller has just
    /// written a row, right now. It cannot while an earlier write to the stream is still
    /// under way: the stream's position is held below that write. Whoever wakes syncs for
    /// the row has to wait for this, or the syncs find nothing and go back to waiting.
    fn stream_position_reportable(&self, stream: SyncStream, position: i64) -> bool;
    /// Returns once `stream_position_reportable` holds.
    async fn stream_position_settles(&self, stream: SyncStream, position: i64);
    /// How many live, accepted events of the room have `from <= stream_ordering <= to`.
    async fn room_event_count_in_range(&self, room_id: &RoomId, from: i64, to: i64) -> Result<i64>;
    async fn get_event(&self, event_id: &EventId) -> Result<Option<StoredEvent>>;
    /// The event at a stream position, outliers included (an invitation from another server).
    async fn event_at_stream_ordering(&self, stream_ordering: i64) -> Result<Option<StoredEvent>>;
    async fn get_events(&self, event_ids: &[OwnedEventId]) -> Result<Vec<StoredEvent>>;
    /// Take back the rejection of events that turn out to pass their checks after all: they
    /// had been rejected because something they depend on could not be obtained or verified
    /// at the time. Returns how many rows changed.
    async fn clear_rejected(&self, event_ids: &[OwnedEventId]) -> Result<u64>;
    /// The rejected events in a room's timeline (outliers are not part of it) that were
    /// received at or after `received_since`, oldest first.
    async fn rejected_timeline_events(
        &self,
        room_id: &RoomId,
        received_since: Ts,
    ) -> Result<Vec<StoredEvent>>;
    /// Note that an event was stored as rejected only because an auth event it cites could
    /// not be obtained, so that the check is tried again from `next_try_ts` on. An event
    /// that is listed already keeps its entry.
    async fn await_auth(
        &self,
        event_id: &EventId,
        room_id: &RoomId,
        origin: &ServerName,
        now: Ts,
        next_try_ts: Ts,
    ) -> Result<()>;
    /// The listed events whose next try is due, the longest waiting first.
    async fn events_awaiting_auth_due(&self, now: Ts, limit: i64) -> Result<Vec<AwaitingAuth>>;
    /// Count a failed try and set the time of the next one.
    async fn reschedule_awaiting_auth(&self, event_id: &EventId, next_try_ts: Ts) -> Result<()>;
    /// The listed events of a room, oldest first.
    async fn events_set_aside(&self, room_id: &RoomId) -> Result<Vec<StoredEvent>>;
    /// Make the listed events of a room due now: something happened that may let them pass
    /// (the room was joined again, which brings in its state afresh).
    async fn hasten_awaiting_auth(&self, room_id: &RoomId, now: Ts) -> Result<()>;
    /// Take an event off the list: it has been checked, or is given up on.
    async fn forget_awaiting_auth(&self, event_id: &EventId) -> Result<()>;
    async fn have_events(&self, event_ids: &[OwnedEventId]) -> Result<Vec<OwnedEventId>>;
    /// Whether an accepted event in the room's timeline names this event as a prev event:
    /// then it is no tip of the room, however late it is accepted itself.
    async fn has_accepted_child(&self, event_id: &EventId) -> Result<bool>;
    /// Replace the stored JSON with its redacted form (keeps the original in `redacted_json`).
    async fn redact_event(&self, event_id: &EventId, redacted: &Pdu) -> Result<()>;
    /// Mark an outlier as a normal event with the given state group.
    async fn de_outlier(&self, event_id: &EventId, state_group: StateGroupId) -> Result<()>;
    /// Add a signature to a stored event, beside the others (in its pre-redaction copy too):
    /// a policy server's signature obtained after the event was first stored. Signatures
    /// take no part in the event ID or the hashes. Nothing happens for an unknown event.
    async fn add_event_signature(
        &self,
        event_id: &EventId,
        server: &str,
        key_id: &str,
        signature: &str,
    ) -> Result<()>;

    async fn forward_extremities(&self, room_id: &RoomId) -> Result<Vec<OwnedEventId>>;
    async fn set_forward_extremities(
        &self,
        room_id: &RoomId,
        event_ids: &[OwnedEventId],
    ) -> Result<()>;
    /// Prev events that are referenced but not stored.
    async fn missing_prev_events(&self, room_id: &RoomId) -> Result<Vec<OwnedEventId>>;
    /// The earliest event stored after `since` whose prev events we do not have (or hold only
    /// as outliers): the start of a gap in the timeline clients should paginate.
    async fn gap_position_since(
        &self,
        room_id: &RoomId,
        since: StreamOrdering,
    ) -> Result<Option<StreamOrdering>>;
    /// Oldest events in the room that have missing prev events (backfill points).
    async fn backward_extremities(&self, room_id: &RoomId) -> Result<Vec<OwnedEventId>>;
    /// Missing prev events of the room's *timeline* (events that are not outliers), oldest gap
    /// first: where backfill should continue. Prev events of outlier state events are not
    /// history and are left out.
    async fn backfill_points(&self, room_id: &RoomId, limit: usize) -> Result<Vec<OwnedEventId>>;

    /// The full auth chain (transitive `auth_events`) of the given events, excluding them.
    async fn auth_chain(&self, event_ids: &[OwnedEventId]) -> Result<Vec<OwnedEventId>>;
    /// Walk back from `latest` over `prev_events` for up to `limit` events, newest first.
    async fn backfill_from(
        &self,
        room_id: &RoomId,
        latest: &[OwnedEventId],
        limit: usize,
    ) -> Result<Vec<StoredEvent>>;
    /// Events relating to `event_id`, optionally filtered by relation and event type, paged
    /// by stream ordering. With `recurse`, relations of relations (up to depth 3) count too.
    #[allow(clippy::too_many_arguments)]
    async fn relations(
        &self,
        room_id: &RoomId,
        event_id: &EventId,
        rel_type: Option<&str>,
        event_type: Option<&str>,
        recurse: bool,
        from: Option<StreamOrdering>,
        to: Option<StreamOrdering>,
        direction: Direction,
        limit: usize,
    ) -> Result<Vec<StoredEvent>>;
    /// Every relation pointing at `event_id`, oldest first.
    async fn relation_summary(&self, event_id: &EventId) -> Result<Vec<RelationRow>>;
    /// `relation_summary` for many events in one query, keyed by the related-to event.
    async fn relation_summaries(
        &self,
        event_ids: &[OwnedEventId],
    ) -> Result<std::collections::HashMap<OwnedEventId, Vec<RelationRow>>>;
    /// Thread roots in a room ordered by latest activity (newest first), paged by the latest
    /// reply's stream ordering.
    async fn threads(
        &self,
        room_id: &RoomId,
        participant: Option<&UserId>,
        before: Option<StreamOrdering>,
        limit: usize,
    ) -> Result<Vec<ThreadSummary>>;
    /// Full-text search over message bodies, names and topics in the given rooms. Returns the
    /// page plus the total hit count.
    async fn search_events(
        &self,
        room_ids: &[OwnedRoomId],
        query: &SearchQuery<'_>,
    ) -> Result<(Vec<SearchHit>, usize)>;
    /// Whether newly persisted events keep their plain text for substring search.
    fn set_search_plaintext(&self, enabled: bool);
    /// Fills the plain text of up to `limit` searchable events after stream position `after`
    /// (older rows get the text they were missing). Returns how many rows were written and the
    /// last position looked at, `None` when there is nothing left.
    async fn search_backfill_batch(&self, after: i64, limit: usize)
    -> Result<(usize, Option<i64>)>;
    /// Up to `limit` events found by walking prev events back from `latest` (exclusive)
    /// without crossing `earliest` (exclusive): breadth-first, the nearest to `latest` first,
    /// as `/get_missing_events` defines it. In no particular order.
    async fn missing_events_between(
        &self,
        room_id: &RoomId,
        earliest: &[OwnedEventId],
        latest: &[OwnedEventId],
        limit: usize,
        min_depth: i64,
    ) -> Result<Vec<StoredEvent>>;

    /// Timeline events in stream order within `(from, to]` (forwards) or `[to, from)`
    /// (backwards), newest-first when backwards. Excludes rejected events and outliers, and
    /// soft-failed events: those are kept for state resolution and for other servers, but
    /// are not shown to clients (a banned user's server can go on sending events that build
    /// on the room as it was before the ban). The same holds for every other query here that
    /// reads events for clients.
    async fn room_events_in_range(
        &self,
        room_id: &RoomId,
        from: StreamOrdering,
        to: Option<StreamOrdering>,
        direction: Direction,
        limit: usize,
    ) -> Result<Vec<StoredEvent>>;
    /// The newest events of a room at or before `upto`, in topological order (depth, then
    /// stream position), newest first. Initial syncs show history this way, like the
    /// reference implementation, so state that arrived late sits in `state`, not the timeline.
    async fn room_events_topological(
        &self,
        room_id: &RoomId,
        upto: StreamOrdering,
        limit: usize,
    ) -> Result<Vec<StoredEvent>>;
    /// Latest stream ordering of any event, i.e. the current sync position.
    async fn max_stream_ordering(&self) -> Result<StreamOrdering>;
    /// Events in `(since, upto]` in stream order, at most `limit`, for the push worker to
    /// catch up with: the rooms' events and the outlier member events (invitations from other
    /// servers), neither rejected nor soft-failed.
    async fn events_for_push(
        &self,
        since: StreamOrdering,
        upto: StreamOrdering,
        limit: i64,
    ) -> Result<Vec<StoredEvent>>;
    /// Rooms that had events in `(from, to]`.
    async fn rooms_with_events_in_range(
        &self,
        room_ids: &[OwnedRoomId],
        from: StreamOrdering,
        to: StreamOrdering,
    ) -> Result<Vec<OwnedRoomId>>;
    /// The event at `stream_ordering` plus its depth, for topological tokens.
    async fn stream_to_topological(
        &self,
        event_id: &EventId,
    ) -> Result<Option<(i64, StreamOrdering)>>;
}

/// State groups and current state.
#[async_trait]
pub trait StateStore: Send + Sync {
    /// Full state `(type, state_key)` -> event id at a state group.
    async fn state_at_group(&self, group: StateGroupId) -> Result<Vec<CurrentStateEntry>>;
    /// Subset of the state at a group, by `(type, state_key)` keys.
    async fn state_at_group_filtered(
        &self,
        group: StateGroupId,
        keys: &[(StateEventType, String)],
    ) -> Result<Vec<CurrentStateEntry>>;
    /// State group *before* an event: for a state event the parent of its group, otherwise
    /// its own group.
    async fn state_group_before(&self, event_id: &EventId) -> Result<Option<StateGroupId>>;
    async fn create_state_group(
        &self,
        room_id: &RoomId,
        prev: Option<StateGroupId>,
        delta: &StateDelta,
    ) -> Result<StateGroupId>;

    async fn current_state(&self, room_id: &RoomId) -> Result<Vec<CurrentStateEntry>>;
    async fn current_state_event(
        &self,
        room_id: &RoomId,
        event_type: &StateEventType,
        state_key: &str,
    ) -> Result<Option<OwnedEventId>>;
    /// Current state as full PDUs.
    async fn current_state_pdus(&self, room_id: &RoomId) -> Result<Vec<StoredEvent>>;
    /// The current state events of one type.
    async fn current_state_pdus_of_type(
        &self,
        room_id: &RoomId,
        event_type: &StateEventType,
    ) -> Result<Vec<StoredEvent>>;
    /// The current state events for the given keys only.
    async fn current_state_pdus_for(
        &self,
        room_id: &RoomId,
        keys: &[(StateEventType, String)],
    ) -> Result<Vec<StoredEvent>>;
    async fn apply_current_state_delta(
        &self,
        room_id: &RoomId,
        delta: &CurrentStateDelta,
    ) -> Result<()>;
}

/// Account data and filters.
#[async_trait]
pub trait AccountDataStore: Send + Sync {
    async fn set_account_data(
        &self,
        user_id: &UserId,
        room_id: Option<&RoomId>,
        data_type: &str,
        content: &serde_json::Value,
    ) -> Result<i64>;
    async fn get_account_data(
        &self,
        user_id: &UserId,
        room_id: Option<&RoomId>,
        data_type: &str,
    ) -> Result<Option<AccountData>>;
    /// Deletes account data (MSC3391) by replacing it with empty content, which incremental
    /// syncs report; returns the new stream id when there was something to delete.
    async fn delete_account_data(
        &self,
        user_id: &UserId,
        room_id: Option<&RoomId>,
        data_type: &str,
    ) -> Result<Option<i64>>;
    /// All account data for the user with stream id > `since`.
    async fn account_data_since(&self, user_id: &UserId, since: i64) -> Result<Vec<AccountData>>;
    /// The user's account data of one type, global and in every room, without deleted entries.
    async fn account_data_of_type(
        &self,
        user_id: &UserId,
        data_type: &str,
    ) -> Result<Vec<AccountData>>;
    async fn max_account_data_stream_id(&self) -> Result<i64>;
    /// Removes all of a user's account data, global and per room, their push rules and their
    /// filters, for good (account erasure; nobody syncs as the user any more, so nothing is
    /// left behind to report the deletion).
    async fn erase_account_data(&self, user_id: &UserId) -> Result<()>;
    async fn create_filter(&self, user_id: &UserId, filter: &serde_json::Value) -> Result<i64>;
    async fn get_filter(
        &self,
        user_id: &UserId,
        filter_id: i64,
    ) -> Result<Option<serde_json::Value>>;
}

/// Thread subscriptions (MSC4306).
#[async_trait]
pub trait ThreadSubscriptionStore: Send + Sync {
    async fn get_thread_subscription(
        &self,
        user_id: &UserId,
        room_id: &RoomId,
        thread_root: &EventId,
    ) -> Result<Option<ThreadSubscription>>;
    /// Subscribes (replacing any earlier state); returns the new bump stamp.
    async fn subscribe_thread(
        &self,
        user_id: &UserId,
        room_id: &RoomId,
        thread_root: &EventId,
        automatic: bool,
    ) -> Result<i64>;
    /// Unsubscribes, remembering the newest event position `at`; returns the new bump stamp.
    async fn unsubscribe_thread(
        &self,
        user_id: &UserId,
        room_id: &RoomId,
        thread_root: &EventId,
        at: i64,
    ) -> Result<i64>;
    /// Subscription changes with `above < stream_id < below` (either bound optional), newest
    /// first, at most `limit`; `subscribed_only` drops unsubscriptions.
    async fn thread_subscription_changes(
        &self,
        user_id: &UserId,
        above: Option<i64>,
        below: Option<i64>,
        limit: usize,
        subscribed_only: bool,
    ) -> Result<Vec<ThreadSubscription>>;
    async fn max_thread_subscription_stream_id(&self) -> Result<i64>;
}

/// Delayed events (MSC4140).
#[async_trait]
pub trait DelayedEventStore: Send + Sync {
    async fn add_delayed_event(&self, event: &DelayedEvent) -> Result<()>;
    /// A pending delayed event (not yet sent or cancelled).
    async fn get_delayed_event(&self, delay_id: &str) -> Result<Option<DelayedEvent>>;
    /// A delayed event, pending or kept after it was sent or cancelled.
    async fn get_delayed_event_any(&self, delay_id: &str) -> Result<Option<DelayedEvent>>;
    /// The delayed event a device already scheduled under this transaction ID, if any (also
    /// one that was sent or cancelled since, so a retried request gets the same delay ID).
    async fn delayed_event_by_txn(
        &self,
        user_id: &UserId,
        device_id: Option<&DeviceId>,
        txn_id: &str,
    ) -> Result<Option<DelayedEvent>>;
    /// The user's pending delayed events.
    async fn delayed_events_for_user(&self, user_id: &UserId) -> Result<Vec<DelayedEvent>>;
    /// Marks a pending event as sent or cancelled at `now`; `false` if it was not pending (so
    /// of two callers only one goes on to send it). The outcome is recorded separately.
    async fn finalise_delayed_event(&self, delay_id: &str, now: i64) -> Result<bool>;
    /// Makes an event pending again after an explicit send failed (the spec keeps it scheduled).
    async fn unfinalise_delayed_event(&self, delay_id: &str) -> Result<()>;
    /// Records how a finalised event ended: the event it became (also kept in
    /// `event_delay_ids` for `unsigned.delay_id`), or the error that cancelled it.
    async fn record_delayed_event_outcome(
        &self,
        delay_id: &str,
        event_id: Option<&ruma::EventId>,
        error: Option<&serde_json::Value>,
    ) -> Result<()>;
    /// Deletes finalised events older than `before`, and beyond the newest `keep_per_user` of
    /// each user; returns how many.
    async fn prune_finalised_delayed_events(&self, before: i64, keep_per_user: i64) -> Result<u64>;
    /// The delay IDs of those of `event_ids` that `user_id` sent from a delayed event.
    async fn delay_ids_for_events(
        &self,
        user_id: &UserId,
        event_ids: &[ruma::OwnedEventId],
    ) -> Result<std::collections::HashMap<ruma::OwnedEventId, String>>;
    /// Starts the timeout of a pending event over from `now`; `false` if it is not pending.
    async fn restart_delayed_event(&self, delay_id: &str, now: i64) -> Result<bool>;
    /// Pending events whose timeout has passed at `now`, oldest first.
    async fn due_delayed_events(&self, now: i64) -> Result<Vec<DelayedEvent>>;
    /// When the next pending event is due (ms), if any.
    async fn next_delayed_event_due(&self) -> Result<Option<i64>>;
    /// Cancels pending delayed state events for a state tuple; returns how many.
    async fn remove_delayed_state_events(
        &self,
        room_id: &RoomId,
        event_type: &str,
        state_key: &str,
    ) -> Result<u64>;
    /// How many pending delayed state events there are.
    async fn count_delayed_state_events(&self) -> Result<u64>;
}

/// `OpenID` tokens and `MatrixRTC` over `LiveKit` (MSC4195).
#[async_trait]
pub trait RtcStore: Send + Sync {
    async fn add_openid_token(&self, token: &str, user_id: &UserId, expires_ts: i64) -> Result<()>;
    /// The owner of a still valid `OpenID` token.
    async fn openid_token_owner(&self, token: &str, now: i64) -> Result<Option<OwnedUserId>>;
    async fn purge_expired_openid_tokens(&self, now: i64) -> Result<u64>;

    /// Records (or refreshes) a participant a token was issued to.
    async fn add_rtc_participant(&self, participant: &RtcParticipant) -> Result<()>;
    async fn rtc_participant(
        &self,
        lk_room: &str,
        lk_identity: &str,
    ) -> Result<Option<RtcParticipant>>;
    /// Every SFU participant identity issued to a user in a room.
    async fn rtc_participants_for(
        &self,
        room_id: &RoomId,
        user_id: &UserId,
    ) -> Result<Vec<RtcParticipant>>;
    async fn remove_rtc_participant(&self, lk_room: &str, lk_identity: &str) -> Result<()>;
    async fn purge_rtc_participants_before(&self, issued_before: i64) -> Result<u64>;

    /// Registers a delegated leave, replacing an earlier one for the same participant; `false`
    /// when the delayed event no longer exists.
    async fn add_delegated_leave(&self, leave: &DelegatedLeave) -> Result<bool>;
    async fn delegated_leaves(&self) -> Result<Vec<DelegatedLeave>>;
    async fn delegated_leave_for(
        &self,
        lk_room: &str,
        lk_identity: &str,
    ) -> Result<Option<DelegatedLeave>>;
    async fn mark_delegated_leave_connected(&self, delay_id: &str, now: i64) -> Result<()>;
    async fn remove_delegated_leave(&self, delay_id: &str) -> Result<bool>;
}

/// Signing keys: ours and cached remote ones.
#[async_trait]
pub trait KeyStore: Send + Sync {
    async fn local_signing_keys(&self) -> Result<Vec<LocalSigningKey>>;
    async fn add_local_signing_key(&self, key: LocalSigningKey) -> Result<()>;
    async fn expire_local_signing_key(&self, key_id: &str, expired_ts: Ts) -> Result<()>;
    async fn remote_verify_keys(&self, server_name: &ServerName) -> Result<Vec<RemoteVerifyKey>>;
    async fn remote_verify_keys_for(
        &self,
        server_name: &ServerName,
        key_ids: &[String],
    ) -> Result<Vec<RemoteVerifyKey>>;
    async fn store_remote_verify_keys(&self, keys: Vec<RemoteVerifyKey>) -> Result<()>;
}

/// End-to-end encryption keys, to-device messages, device list stream, push.
#[async_trait]
pub trait KeyBackupStore: Send + Sync {
    /// Create a new backup version; returns its version string.
    async fn create_backup_version(
        &self,
        user_id: &UserId,
        algorithm: &serde_json::Value,
    ) -> Result<String>;
    /// A backup version (`None` = latest), if it exists and is not deleted.
    async fn get_backup_version(
        &self,
        user_id: &UserId,
        version: Option<&str>,
    ) -> Result<Option<BackupVersion>>;
    /// Replace the algorithm/auth data of a version.
    async fn update_backup_version(
        &self,
        user_id: &UserId,
        version: &str,
        algorithm: &serde_json::Value,
    ) -> Result<bool>;
    /// Delete a version and its keys.
    async fn delete_backup_version(&self, user_id: &UserId, version: &str) -> Result<bool>;
    /// Store keys, applying the replacement rules; returns the new etag and count.
    async fn put_backup_keys(
        &self,
        user_id: &UserId,
        version: &str,
        keys: &[BackupKey],
    ) -> Result<(i64, i64)>;
    /// Keys of a version, optionally narrowed to a room and session.
    async fn get_backup_keys(
        &self,
        user_id: &UserId,
        version: &str,
        room_id: Option<&RoomId>,
        session_id: Option<&str>,
    ) -> Result<Vec<BackupKey>>;
    /// Delete keys (all, a room's, or one session's); returns the new etag and count.
    async fn delete_backup_keys(
        &self,
        user_id: &UserId,
        version: &str,
        room_id: Option<&RoomId>,
        session_id: Option<&str>,
    ) -> Result<(i64, i64)>;
}

#[async_trait]
pub trait E2eeStore: Send + Sync {
    async fn set_device_keys(
        &self,
        user_id: &UserId,
        device_id: &DeviceId,
        keys: &serde_json::Value,
    ) -> Result<()>;
    async fn get_device_keys(
        &self,
        user_id: &UserId,
        device_id: &DeviceId,
    ) -> Result<Option<serde_json::Value>>;
    async fn all_device_keys(
        &self,
        user_id: &UserId,
    ) -> Result<Vec<(OwnedDeviceId, serde_json::Value)>>;
    async fn delete_device_keys(&self, user_id: &UserId, device_id: &DeviceId) -> Result<()>;

    async fn add_one_time_keys(
        &self,
        user_id: &UserId,
        device_id: &DeviceId,
        keys: &[OneTimeKeyRow],
    ) -> Result<()>;
    /// Count of unclaimed one-time keys per algorithm.
    async fn count_one_time_keys(
        &self,
        user_id: &UserId,
        device_id: &DeviceId,
    ) -> Result<Vec<(String, i64)>>;
    /// Atomically take one key of the algorithm; falls back to the fallback key (marking it used).
    async fn claim_one_time_key(
        &self,
        user_id: &UserId,
        device_id: &DeviceId,
        algorithm: &str,
    ) -> Result<Option<OneTimeKeyRow>>;
    async fn set_fallback_keys(
        &self,
        user_id: &UserId,
        device_id: &DeviceId,
        keys: &[OneTimeKeyRow],
    ) -> Result<()>;
    /// Algorithms that have an unused fallback key.
    async fn unused_fallback_algorithms(
        &self,
        user_id: &UserId,
        device_id: &DeviceId,
    ) -> Result<Vec<String>>;

    async fn set_cross_signing_key(
        &self,
        user_id: &UserId,
        key_type: &str,
        key: &serde_json::Value,
    ) -> Result<()>;
    async fn get_cross_signing_key(
        &self,
        user_id: &UserId,
        key_type: &str,
    ) -> Result<Option<serde_json::Value>>;
    async fn add_key_signature(
        &self,
        target_user: &UserId,
        target_key: &str,
        signing_user: &UserId,
        signing_key: &str,
        signature: &str,
    ) -> Result<()>;
    async fn key_signatures(
        &self,
        target_user: &UserId,
        target_key: &str,
    ) -> Result<Vec<(OwnedUserId, String, String)>>;

    async fn add_device_list_change(
        &self,
        user_id: &UserId,
        device_id: Option<&DeviceId>,
    ) -> Result<i64>;
    async fn device_list_changes_since(&self, since: i64, to: i64) -> Result<Vec<OwnedUserId>>;
    async fn max_device_list_stream_id(&self) -> Result<i64>;
    /// The newest change to one of `user_id`'s devices below `before`: what an
    /// `m.device_list_update` with stream ID `before` names as its `prev_id`. Changes that
    /// concern no device (cross-signing keys) are not part of that chain.
    async fn previous_device_change(&self, user_id: &UserId, before: i64) -> Result<Option<i64>>;
    /// A stream ID for each of `user_id`'s devices, distinct per device: the device's newest
    /// change, or a fresh number for a device that has none (devices imported from Synapse).
    async fn device_stream_ids(&self, user_id: &UserId) -> Result<Vec<(OwnedDeviceId, i64)>>;
    /// Every device that carries a signature by its owner (cross-signing), on any server.
    async fn owner_signed_devices(&self) -> Result<Vec<(OwnedUserId, OwnedDeviceId)>>;

    async fn add_to_device_message(
        &self,
        user_id: &UserId,
        device_id: &DeviceId,
        sender: &UserId,
        event_type: &str,
        content: &serde_json::Value,
    ) -> Result<i64>;
    async fn to_device_messages(
        &self,
        user_id: &UserId,
        device_id: &DeviceId,
        since: i64,
        to: i64,
        limit: usize,
    ) -> Result<Vec<ToDeviceMessage>>;
    async fn delete_to_device_messages_up_to(
        &self,
        user_id: &UserId,
        device_id: &DeviceId,
        up_to: i64,
    ) -> Result<()>;
    async fn max_to_device_stream_id(&self) -> Result<i64>;

    /// Makes `device_id`, which has to exist, the user's dehydrated device (MSC3814), in the
    /// place of any other.
    async fn set_dehydrated_device(
        &self,
        user_id: &UserId,
        device_id: &DeviceId,
        device_data: &serde_json::Value,
    ) -> Result<()>;
    /// The user's dehydrated device and its data.
    async fn dehydrated_device(
        &self,
        user_id: &UserId,
    ) -> Result<Option<(OwnedDeviceId, serde_json::Value)>>;

    async fn get_push_rules(&self, user_id: &UserId) -> Result<Option<(serde_json::Value, i64)>>;
    /// Store the ruleset and return the account-data stream id it was published at.
    async fn set_push_rules(&self, user_id: &UserId, ruleset: &serde_json::Value) -> Result<i64>;
    async fn set_pusher(&self, pusher: Pusher) -> Result<()>;
    async fn delete_pusher(&self, user_id: &UserId, app_id: &str, pushkey: &str) -> Result<()>;
    /// Delete every pusher of the user except those created with `keep_token`.
    async fn delete_pushers_except_token(
        &self,
        user_id: &UserId,
        keep_token: Option<&str>,
    ) -> Result<()>;
    async fn pushers(&self, user_id: &UserId) -> Result<Vec<Pusher>>;
    /// Every user with at least one HTTP pusher.
    async fn users_with_pushers(&self) -> Result<Vec<OwnedUserId>>;
}

/// Federation queues and caches.
#[async_trait]
pub trait FederationStore: Send + Sync {
    /// Queue a PDU for every destination; returns the first new row id.
    async fn enqueue_pdu(&self, destinations: &[OwnedServerName], event_id: &EventId)
    -> Result<()>;
    async fn enqueue_edu(
        &self,
        destinations: &[OwnedServerName],
        edu: &serde_json::Value,
    ) -> Result<()>;
    /// Pending items for a destination: its oldest PDUs, followed by its oldest EDUs with
    /// those of the `first_edu_types` ahead of the rest. PDUs and EDUs are limited
    /// separately, so that neither kind waits behind a backlog of the other.
    async fn outbox_for(
        &self,
        destination: &ServerName,
        max_pdus: usize,
        max_edus: usize,
        first_edu_types: &[&str],
    ) -> Result<Vec<OutboxItem>>;
    async fn delete_outbox_items(&self, ids: &[i64]) -> Result<()>;
    /// Destinations that have anything queued.
    async fn outbox_destinations(&self) -> Result<Vec<OwnedServerName>>;
    /// Thin out the queues of destinations that have been failing since before `failing_since`:
    /// their EDUs go, and of their PDUs only the newest per room stays, which is enough for a
    /// server that returns to fetch what it missed. Returns the number of rows deleted.
    async fn prune_dead_outbox(&self, failing_since: Ts) -> Result<u64>;
    async fn destination_state(&self, destination: &ServerName) -> Result<DestinationState>;
    async fn set_destination_state(
        &self,
        destination: &ServerName,
        state: &DestinationState,
    ) -> Result<()>;

    async fn received_transaction(
        &self,
        origin: &ServerName,
        txn_id: &str,
    ) -> Result<Option<serde_json::Value>>;
    async fn store_received_transaction(
        &self,
        origin: &ServerName,
        txn_id: &str,
        response: &serde_json::Value,
    ) -> Result<()>;
    /// Forget inbound transactions received before `before`; their stored answers only serve
    /// to answer a retransmission. Returns the number of rows deleted.
    async fn prune_received_transactions(&self, before: Ts) -> Result<u64>;

    async fn get_discovery(&self, server_name: &ServerName) -> Result<Option<DiscoveryEntry>>;
    async fn set_discovery(&self, entry: &DiscoveryEntry) -> Result<()>;

    /// Stripped state from a remote invite.
    async fn set_invite_state(
        &self,
        room_id: &RoomId,
        user_id: &UserId,
        state: &[serde_json::Value],
    ) -> Result<()>;
    async fn get_invite_state(
        &self,
        room_id: &RoomId,
        user_id: &UserId,
    ) -> Result<Option<Vec<serde_json::Value>>>;
    async fn delete_invite_state(&self, room_id: &RoomId, user_id: &UserId) -> Result<()>;
}

/// Media metadata.
#[async_trait]
pub trait MediaStore: Send + Sync {
    async fn create_media(&self, info: &MediaInfo) -> Result<()>;
    async fn update_media(&self, info: &MediaInfo) -> Result<()>;
    async fn get_media(
        &self,
        server_name: &ServerName,
        media_id: &str,
    ) -> Result<Option<MediaInfo>>;
    async fn add_thumbnail(
        &self,
        server_name: &ServerName,
        media_id: &str,
        thumb: &ThumbnailInfo,
    ) -> Result<()>;
    async fn thumbnails(
        &self,
        server_name: &ServerName,
        media_id: &str,
    ) -> Result<Vec<ThumbnailInfo>>;
}

/// Sign-in through OAuth 2.0: which client a device's tokens were granted to, and the
/// embedded authorization server's key-value state (`lithostroton_store`). Expiry is in
/// seconds since the epoch; an entry past it counts as absent everywhere.
#[async_trait]
pub trait OauthStore: Send + Sync {
    /// Remember the OAuth client and scope a device's tokens were granted to (`None` for a
    /// password login).
    async fn set_device_oauth(
        &self,
        user_id: &UserId,
        device_id: &DeviceId,
        client_id: Option<&str>,
        scope: Option<&str>,
    ) -> Result<()>;
    /// The OAuth client and scope of a device: `None` if there is no such device.
    async fn device_oauth(
        &self,
        user_id: &UserId,
        device_id: &DeviceId,
    ) -> Result<Option<(Option<String>, Option<String>)>>;
    /// Delete a device's refresh tokens (its access tokens go with
    /// `delete_access_tokens_for_device`).
    async fn delete_refresh_tokens_for_device(
        &self,
        user_id: &UserId,
        device_id: &DeviceId,
    ) -> Result<()>;
    /// Add an entry unless a live one exists; whether it was added.
    async fn oauth_kv_insert(
        &self,
        kind: &str,
        key: &str,
        value: &[u8],
        expires_at: Option<f64>,
    ) -> Result<bool>;
    async fn oauth_kv_get(&self, kind: &str, key: &str) -> Result<Option<Vec<u8>>>;
    /// Replace a live entry's value and expiry if its value is `expected`; whether it was.
    async fn oauth_kv_replace(
        &self,
        kind: &str,
        key: &str,
        expected: &[u8],
        value: &[u8],
        expires_at: Option<f64>,
    ) -> Result<bool>;
    /// Remove an entry, returning its value if it was live.
    async fn oauth_kv_take(&self, kind: &str, key: &str) -> Result<Option<Vec<u8>>>;
    async fn oauth_kv_delete(&self, kind: &str, key: &str) -> Result<()>;
    async fn oauth_kv_purge_expired(&self) -> Result<()>;
}

/// Email addresses on accounts, and their verification.
#[async_trait]
pub trait ThreepidStore: Send + Sync {
    /// Who has this address, if anyone.
    async fn threepid_owner(&self, medium: &str, address: &str) -> Result<Option<OwnedUserId>>;
    async fn user_threepids(&self, user_id: &UserId) -> Result<Vec<UserThreepid>>;
    /// Add an address to an account; `false` if another account (or this one) has it.
    async fn add_user_threepid(&self, threepid: UserThreepid) -> Result<bool>;
    /// Remove an address from an account; whether it was there.
    async fn delete_user_threepid(
        &self,
        user_id: &UserId,
        medium: &str,
        address: &str,
    ) -> Result<bool>;
    async fn delete_user_threepids(&self, user_id: &UserId) -> Result<()>;
    /// The verification session with this client secret for this address, if any.
    async fn threepid_session_for(
        &self,
        client_secret: &str,
        medium: &str,
        address: &str,
    ) -> Result<Option<ThreepidSession>>;
    async fn threepid_session(&self, sid: &str) -> Result<Option<ThreepidSession>>;
    /// Insert a session, or replace the one with the same client secret and address.
    async fn put_threepid_session(&self, session: ThreepidSession) -> Result<()>;
    /// Mark a session validated (`Some`) or count a wrong code (`None`).
    async fn update_threepid_session(&self, sid: &str, validated_at: Option<Ts>) -> Result<()>;
    async fn delete_threepid_session(&self, sid: &str) -> Result<()>;
    async fn purge_expired_threepid_sessions(&self, now: Ts) -> Result<()>;
}

/// The full store.
pub trait Store:
    UserStore
    + RoomStore
    + EventStore
    + StateStore
    + AccountDataStore
    + DelayedEventStore
    + RtcStore
    + ThreadSubscriptionStore
    + KeyStore
    + E2eeStore
    + KeyBackupStore
    + FederationStore
    + MediaStore
    + OauthStore
    + ThreepidStore
{
    /// Run pending migrations.
    fn migrate(&self) -> futures::future::BoxFuture<'_, Result<()>>;
}

/// Convenience for building a `(type, state_key)` key.
pub fn state_key(event_type: StateEventType, state_key: &str) -> (StateEventType, String) {
    (event_type, state_key.to_owned())
}

/// Helper to turn an owned user id and the Rust-side membership string into a store filter.
pub fn membership_str(m: &ruma::events::room::member::MembershipState) -> String {
    m.to_string()
}

/// Build an `OwnedUserId`-keyed list from raw strings, skipping corrupt rows with a warning.
pub(crate) fn parse_ids<T: TryFrom<String>>(raw: Vec<String>) -> Vec<T>
where
    T::Error: std::fmt::Display,
{
    raw.into_iter()
        .filter_map(|s| match T::try_from(s) {
            Ok(v) => Some(v),
            Err(e) => {
                tracing::warn!(error = %e, "skipping corrupt identifier in database");
                None
            }
        })
        .collect()
}
