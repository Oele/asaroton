//! Storage trait and PostgreSQL backend for the tessera homeserver.
//!
//! Everything outside this crate talks to a `dyn Store` in domain types; SQL never leaks out.
//! The traits are split by area so each backend file stays readable; [`Store`] is the sum.

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

mod error;
pub mod postgres;
pub mod types;

use std::sync::Arc;

use async_trait::async_trait;
use ruma::{
    DeviceId, EventId, OwnedDeviceId, OwnedEventId, OwnedRoomId, OwnedServerName, OwnedUserId,
    RoomAliasId, RoomId, ServerName, UserId, events::StateEventType,
};
use tessera_core::Pdu;

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
    async fn set_deactivated(&self, user_id: &UserId, deactivated: bool) -> Result<()>;
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
}

/// Rooms, aliases, and the event graph.
#[async_trait]
pub trait RoomStore: Send + Sync {
    async fn create_room(&self, room: RoomInfo) -> Result<()>;
    async fn get_room(&self, room_id: &RoomId) -> Result<Option<RoomInfo>>;
    async fn room_exists(&self, room_id: &RoomId) -> Result<bool>;
    async fn set_room_public(&self, room_id: &RoomId, public: bool) -> Result<()>;
    async fn list_public_rooms(&self, limit: i64, offset: i64) -> Result<Vec<RoomInfo>>;
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
    async fn max_receipt_stream_id(&self) -> Result<i64>;
}

/// Events and state.
#[async_trait]
pub trait EventStore: Send + Sync {
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

    /// The newest position of every stream, for building a sync token.
    async fn stream_positions(&self) -> Result<StreamPositions>;
    /// How many live, accepted events of the room have `from <= stream_ordering <= to`.
    async fn room_event_count_in_range(&self, room_id: &RoomId, from: i64, to: i64) -> Result<i64>;
    async fn get_event(&self, event_id: &EventId) -> Result<Option<StoredEvent>>;
    async fn get_events(&self, event_ids: &[OwnedEventId]) -> Result<Vec<StoredEvent>>;
    async fn have_events(&self, event_ids: &[OwnedEventId]) -> Result<Vec<OwnedEventId>>;
    /// Replace the stored JSON with its redacted form (keeps the original in `redacted_json`).
    async fn redact_event(&self, event_id: &EventId, redacted: &Pdu) -> Result<()>;
    /// Mark an outlier as a normal event with the given state group.
    async fn de_outlier(&self, event_id: &EventId, state_group: StateGroupId) -> Result<()>;

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
    /// Events between `earliest` (exclusive) and `latest` (inclusive) by walking prev events.
    async fn missing_events_between(
        &self,
        room_id: &RoomId,
        earliest: &[OwnedEventId],
        latest: &[OwnedEventId],
        limit: usize,
        min_depth: i64,
    ) -> Result<Vec<StoredEvent>>;

    /// Timeline events in stream order within `(from, to]` (forwards) or `[to, from)`
    /// (backwards), newest-first when backwards. Excludes rejected and outliers.
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
    async fn max_account_data_stream_id(&self) -> Result<i64>;
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
    async fn get_delayed_event(&self, delay_id: &str) -> Result<Option<DelayedEvent>>;
    /// The delayed event a device already scheduled under this transaction ID, if any.
    async fn delayed_event_by_txn(
        &self,
        user_id: &UserId,
        device_id: Option<&DeviceId>,
        txn_id: &str,
    ) -> Result<Option<DelayedEvent>>;
    async fn delayed_events_for_user(&self, user_id: &UserId) -> Result<Vec<DelayedEvent>>;
    /// Removes the event; `false` if it was not there.
    async fn remove_delayed_event(&self, delay_id: &str) -> Result<bool>;
    /// Starts the timeout over from `now`; `false` if the event was not there.
    async fn restart_delayed_event(&self, delay_id: &str, now: i64) -> Result<bool>;
    /// Events whose timeout has passed at `now`, oldest first.
    async fn due_delayed_events(&self, now: i64) -> Result<Vec<DelayedEvent>>;
    /// When the next event is due (ms), if any is pending.
    async fn next_delayed_event_due(&self) -> Result<Option<i64>>;
    /// Drops delayed state events for a state tuple that was just overwritten; returns how many.
    async fn remove_delayed_state_events(
        &self,
        room_id: &RoomId,
        event_type: &str,
        state_key: &str,
    ) -> Result<u64>;
    async fn count_delayed_state_events(&self) -> Result<u64>;
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
    /// Oldest pending items for a destination.
    async fn outbox_for(&self, destination: &ServerName, limit: usize) -> Result<Vec<OutboxItem>>;
    async fn delete_outbox_items(&self, ids: &[i64]) -> Result<()>;
    /// Destinations that have anything queued.
    async fn outbox_destinations(&self) -> Result<Vec<OwnedServerName>>;
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

/// The full store.
pub trait Store:
    UserStore
    + RoomStore
    + EventStore
    + StateStore
    + AccountDataStore
    + DelayedEventStore
    + ThreadSubscriptionStore
    + KeyStore
    + E2eeStore
    + KeyBackupStore
    + FederationStore
    + MediaStore
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
