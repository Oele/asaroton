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
    async fn set_deactivated(&self, user_id: &UserId, deactivated: bool) -> Result<()>;
    async fn set_admin(&self, user_id: &UserId, admin: bool) -> Result<()>;

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
    async fn set_txn_event(
        &self,
        user_id: &UserId,
        device_id: Option<&DeviceId>,
        txn_id: &str,
        event_id: &EventId,
    ) -> Result<()>;
    /// Remember the client transaction ID of an event, to echo it back in `unsigned`.
    async fn set_event_txn_id(
        &self,
        event_id: &EventId,
        user_id: &UserId,
        device_id: Option<&DeviceId>,
        txn_id: &str,
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
    async fn max_receipt_stream_id(&self) -> Result<i64>;
}

/// Events and state.
#[async_trait]
pub trait EventStore: Send + Sync {
    /// Persist one event with everything that goes with it, atomically.
    async fn persist_event(&self, event: PersistEvent) -> Result<PersistedEvent>;
    /// Persist several events in one transaction, in order.
    async fn persist_events(&self, events: Vec<PersistEvent>) -> Result<Vec<PersistedEvent>>;

    /// Reserve `n` stream orderings from the backfill sequence (descending), so a batch of
    /// backfilled events can be stored oldest-first yet ordered correctly.
    async fn reserve_backfill_orderings(&self, n: usize) -> Result<Vec<StreamOrdering>>;

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
    /// Oldest events in the room that have missing prev events (backfill points).
    async fn backward_extremities(&self, room_id: &RoomId) -> Result<Vec<OwnedEventId>>;

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
        term: &str,
        order_recent: bool,
        offset: usize,
        limit: usize,
    ) -> Result<(Vec<SearchHit>, usize)>;
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
    + KeyStore
    + E2eeStore
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
