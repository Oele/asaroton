//! Server administration: room deletion, history purging and retention.

use std::time::Duration;

use ruma::{OwnedRoomId, OwnedUserId, RoomId, events::StateEventType};
use serde_json::Value;
use tessera_store::{Direction, StreamOrdering};

use crate::{Result, Services};

/// What deleting a room did.
#[derive(Clone, Debug, Default)]
pub struct RoomDeletion {
    pub kicked_users: Vec<OwnedUserId>,
    pub failed_to_kick_users: Vec<OwnedUserId>,
    pub local_aliases: Vec<String>,
}

impl Services {
    /// Make every local member leave, drop the room's aliases and directory listing, and
    /// (with `purge`) remove it from the database entirely.
    pub async fn delete_room(&self, room_id: &RoomId, purge: bool) -> Result<RoomDeletion> {
        let mut out = RoomDeletion::default();
        for m in self.store.members(room_id, None).await? {
            if !self.is_local(&m.user_id)
                || !matches!(
                    m.membership,
                    ruma::events::room::member::MembershipState::Join
                        | ruma::events::room::member::MembershipState::Invite
                        | ruma::events::room::member::MembershipState::Knock
                )
            {
                continue;
            }
            match self
                .leave_room(
                    room_id,
                    &m.user_id,
                    Some("room deleted by an administrator"),
                )
                .await
            {
                Ok(_) => out.kicked_users.push(m.user_id),
                Err(e) => {
                    tracing::warn!(user = %m.user_id, error = %e, "could not remove user from deleted room");
                    out.failed_to_kick_users.push(m.user_id);
                }
            }
        }
        for a in self.store.aliases_for_room(room_id).await? {
            out.local_aliases.push(a.alias.to_string());
            self.store.delete_alias(&a.alias).await?;
        }
        if self.store.room_exists(room_id).await? {
            self.store.set_room_public(room_id, false).await?;
        }
        if purge {
            self.store.purge_room(room_id).await?;
        }
        Ok(out)
    }

    /// Delete the room's non-state history before a stream position.
    pub async fn purge_history_before(
        &self,
        room_id: &RoomId,
        before: StreamOrdering,
    ) -> Result<u64> {
        let _guard = self.lock_room(room_id).await;
        Ok(self.store.purge_history(room_id, before).await?)
    }

    /// Delete the room's non-state history older than `ts` (milliseconds).
    pub async fn purge_history_older_than(&self, room_id: &RoomId, ts: i64) -> Result<u64> {
        let Some(ev) = self
            .store
            .event_at_timestamp(room_id, ts, Direction::Backwards)
            .await?
        else {
            return Ok(0);
        };
        self.purge_history_before(room_id, ev.stream_ordering + 1)
            .await
    }

    /// One pass of message retention: rooms with an `m.room.retention` policy lose events
    /// older than its `max_lifetime`.
    pub async fn apply_retention_policies(
        &self,
        default_max_lifetime_ms: Option<i64>,
    ) -> Result<u64> {
        let mut total = 0;
        let mut rooms: Vec<OwnedRoomId> =
            self.store.rooms_with_state_type("m.room.retention").await?;
        if default_max_lifetime_ms.is_some() {
            // A server-wide default applies to every room we know.
            let (all, _) = self.store.list_rooms(None, i64::MAX, 0).await?;
            for r in all {
                if !rooms.contains(&r.room_id) {
                    rooms.push(r.room_id);
                }
            }
        }
        let now = tessera_store::now_ts();
        for room_id in rooms {
            let policy = self
                .state_content(&room_id, &StateEventType::from("m.room.retention"), "")
                .await?;
            let max_lifetime = policy
                .as_ref()
                .and_then(|p| p.get("max_lifetime"))
                .and_then(Value::as_i64)
                .or(default_max_lifetime_ms);
            let Some(max_lifetime) = max_lifetime.filter(|l| *l > 0) else {
                continue;
            };
            match self
                .purge_history_older_than(&room_id, now - max_lifetime)
                .await
            {
                Ok(n) => total += n,
                Err(e) => tracing::warn!(%room_id, error = %e, "retention purge failed"),
            }
        }
        Ok(total)
    }
}

/// Run retention periodically.
pub fn start_retention(
    services: std::sync::Arc<Services>,
    interval: Duration,
    default_max_lifetime_ms: Option<i64>,
) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(interval).await;
            match services
                .apply_retention_policies(default_max_lifetime_ms)
                .await
            {
                Ok(0) => {}
                Ok(n) => tracing::info!(events = n, "retention purged old events"),
                Err(e) => tracing::warn!(error = %e, "retention pass failed"),
            }
        }
    });
}
