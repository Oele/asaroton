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
    DeviceId, EventId, OwnedEventId, OwnedRoomId, RoomAliasId, RoomId, ServerName, UserId,
    events::StateEventType,
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
}

/// Events and state.
#[async_trait]
pub trait EventStore: Send + Sync {
    /// Persist one event with everything that goes with it, atomically.
    async fn persist_event(&self, event: PersistEvent) -> Result<PersistedEvent>;
    /// Persist several events in one transaction, in order.
    async fn persist_events(&self, events: Vec<PersistEvent>) -> Result<Vec<PersistedEvent>>;

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

/// The full store.
pub trait Store:
    UserStore + RoomStore + EventStore + StateStore + AccountDataStore + KeyStore
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
