use async_trait::async_trait;
use ruma::{
    OwnedEventId, OwnedRoomAliasId, OwnedRoomId, OwnedServerName, OwnedUserId, RoomAliasId, RoomId,
    RoomVersionId, UserId, events::room::member::MembershipState,
};
use sqlx::Row;

use super::PostgresStore;
use crate::{
    Error, Membership, Result, RoomAlias, RoomInfo, RoomStore, StreamOrdering, now_ts, parse_ids,
};

#[derive(sqlx::FromRow)]
struct RoomRow {
    room_id: String,
    room_version: String,
    creator: Option<String>,
    is_public: bool,
    created_ts: i64,
}

impl TryFrom<RoomRow> for RoomInfo {
    type Error = Error;
    fn try_from(r: RoomRow) -> Result<Self> {
        Ok(RoomInfo {
            room_id: OwnedRoomId::try_from(r.room_id)
                .map_err(|e| Error::Corrupt(format!("rooms.room_id: {e}")))?,
            room_version: RoomVersionId::try_from(r.room_version.as_str())
                .map_err(|e| Error::Corrupt(format!("rooms.room_version: {e}")))?,
            // Informational, and not always known: Synapse stores an empty string for a room
            // it first heard of through an invite, and the importer used to copy that. A
            // creator that cannot be read must not make the room unloadable.
            creator: r.creator.and_then(|c| OwnedUserId::try_from(c).ok()),
            is_public: r.is_public,
            created_ts: r.created_ts,
        })
    }
}

#[derive(sqlx::FromRow)]
pub(crate) struct MembershipRow {
    room_id: String,
    user_id: String,
    membership: String,
    event_id: String,
    sender: String,
    display_name: Option<String>,
    avatar_url: Option<String>,
    stream_ordering: i64,
    forgotten: bool,
}

impl TryFrom<MembershipRow> for Membership {
    type Error = Error;
    fn try_from(r: MembershipRow) -> Result<Self> {
        let corrupt = |field: &'static str| {
            move |e: ruma::IdParseError| Error::Corrupt(format!("room_memberships.{field}: {e}"))
        };
        Ok(Membership {
            room_id: OwnedRoomId::try_from(r.room_id).map_err(corrupt("room_id"))?,
            user_id: OwnedUserId::try_from(r.user_id).map_err(corrupt("user_id"))?,
            membership: MembershipState::from(r.membership),
            event_id: OwnedEventId::try_from(r.event_id).map_err(corrupt("event_id"))?,
            sender: OwnedUserId::try_from(r.sender).map_err(corrupt("sender"))?,
            display_name: r.display_name,
            avatar_url: r.avatar_url,
            stream_ordering: r.stream_ordering,
            forgotten: r.forgotten,
        })
    }
}

const ROOM_COLUMNS: &str = "room_id, room_version, creator, is_public, created_ts";
pub(crate) const MEMBERSHIP_COLUMNS: &str = "room_id, user_id, membership, event_id, sender, display_name, avatar_url, stream_ordering, forgotten";

#[async_trait]
impl RoomStore for PostgresStore {
    async fn create_room(&self, room: RoomInfo) -> Result<bool> {
        let done = sqlx::query(
            "INSERT INTO rooms (room_id, room_version, creator, is_public, created_ts) VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (room_id) DO NOTHING",
        )
        .bind(room.room_id.as_str())
        .bind(room.room_version.as_str())
        .bind(room.creator.as_ref().map(|u| u.as_str()))
        .bind(room.is_public)
        .bind(room.created_ts)
        .execute(&self.pool)
        .await?;
        Ok(done.rows_affected() == 1)
    }

    async fn get_room(&self, room_id: &RoomId) -> Result<Option<RoomInfo>> {
        let row: Option<RoomRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT {ROOM_COLUMNS} FROM rooms WHERE room_id = $1"
        )))
        .bind(room_id.as_str())
        .fetch_optional(&self.pool)
        .await?;
        row.map(RoomInfo::try_from).transpose()
    }

    async fn room_exists(&self, room_id: &RoomId) -> Result<bool> {
        Ok(sqlx::query("SELECT 1 FROM rooms WHERE room_id = $1")
            .bind(room_id.as_str())
            .fetch_optional(&self.pool)
            .await?
            .is_some())
    }

    async fn set_room_public(&self, room_id: &RoomId, public: bool) -> Result<()> {
        sqlx::query("UPDATE rooms SET is_public = $2 WHERE room_id = $1")
            .bind(room_id.as_str())
            .bind(public)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn list_public_rooms(&self, limit: i64, offset: i64) -> Result<Vec<RoomInfo>> {
        let rows: Vec<RoomRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT {ROOM_COLUMNS} FROM rooms WHERE is_public ORDER BY created_ts LIMIT $1 OFFSET $2"
        )))
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(RoomInfo::try_from).collect()
    }

    async fn list_rooms(
        &self,
        search: Option<&str>,
        limit: i64,
        offset: i64,
    ) -> Result<(Vec<RoomInfo>, i64)> {
        let pattern = search.map(|n| format!("%{n}%"));
        let where_clause = "WHERE $1::text IS NULL OR room_id ILIKE $1
              OR EXISTS (SELECT 1 FROM room_aliases a WHERE a.room_id = rooms.room_id AND a.alias ILIKE $1)
              OR EXISTS (SELECT 1 FROM current_state_events c JOIN events e ON e.event_id = c.event_id
                         WHERE c.room_id = rooms.room_id AND c.event_type = 'm.room.name'
                           AND e.json ILIKE $1)";
        let rows: Vec<RoomRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT {ROOM_COLUMNS} FROM rooms {where_clause} ORDER BY created_ts DESC LIMIT $2 OFFSET $3"
        )))
        .bind(&pattern)
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await?;
        let total = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT count(*) FROM rooms {where_clause}"
        )))
        .bind(&pattern)
        .fetch_one(&self.pool)
        .await?
        .get::<i64, _>(0);
        Ok((
            rows.into_iter()
                .map(RoomInfo::try_from)
                .collect::<Result<Vec<_>>>()?,
            total,
        ))
    }

    async fn rooms_with_state_type(&self, event_type: &str) -> Result<Vec<OwnedRoomId>> {
        let rows = sqlx::query(
            "SELECT DISTINCT room_id FROM current_state_events WHERE event_type = $1 AND state_key = ''",
        )
        .bind(event_type)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .filter_map(|r| OwnedRoomId::try_from(r.get::<String, _>(0)).ok())
            .collect())
    }

    async fn purge_room(&self, room_id: &RoomId) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let r = room_id.as_str();
        // Tables keyed by event: find the room's event IDs first.
        for sql in [
            "DELETE FROM event_edges WHERE event_id IN (SELECT event_id FROM events WHERE room_id = $1)",
            "DELETE FROM event_auth WHERE event_id IN (SELECT event_id FROM events WHERE room_id = $1)",
            "DELETE FROM event_txn_ids WHERE event_id IN (SELECT event_id FROM events WHERE room_id = $1)",
            "DELETE FROM federation_outbox WHERE event_id IN (SELECT event_id FROM events WHERE room_id = $1)",
            "DELETE FROM event_relations WHERE room_id = $1",
            "DELETE FROM event_search WHERE room_id = $1",
            "DELETE FROM receipts WHERE room_id = $1",
            "DELETE FROM missing_events WHERE room_id = $1",
            "DELETE FROM forward_extremities WHERE room_id = $1",
            "DELETE FROM current_state_events WHERE room_id = $1",
            "DELETE FROM room_memberships WHERE room_id = $1",
            "DELETE FROM invite_state WHERE room_id = $1",
            "DELETE FROM account_data WHERE room_id = $1",
            "DELETE FROM room_aliases WHERE room_id = $1",
            "UPDATE events SET state_group = NULL WHERE room_id = $1",
            "DELETE FROM state_groups WHERE room_id = $1",
            "DELETE FROM events WHERE room_id = $1",
            "DELETE FROM rooms WHERE room_id = $1",
        ] {
            sqlx::query(sql).bind(r).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn purge_history(&self, room_id: &RoomId, before: StreamOrdering) -> Result<u64> {
        let mut tx = self.pool.begin().await?;
        let r = room_id.as_str();
        // Non-state events only: state events are needed to authorise what remains.
        let selector = "SELECT event_id FROM events WHERE room_id = $1 AND stream_ordering < $2
             AND state_key IS NULL
             AND event_id NOT IN (SELECT event_id FROM forward_extremities WHERE room_id = $1)";
        for table in [
            "event_edges",
            "event_auth",
            "event_txn_ids",
            "federation_outbox",
            "event_relations",
            "event_search",
            "receipts",
        ] {
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "DELETE FROM {table} WHERE event_id IN ({selector})"
            )))
            .bind(r)
            .bind(before)
            .execute(&mut *tx)
            .await?;
        }
        let deleted = sqlx::query(sqlx::AssertSqlSafe(format!(
            "DELETE FROM events WHERE event_id IN ({selector})"
        )))
        .bind(r)
        .bind(before)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        tx.commit().await?;
        Ok(deleted)
    }

    async fn create_alias(&self, alias: RoomAlias) -> Result<()> {
        let res = sqlx::query(
            "INSERT INTO room_aliases (alias, room_id, creator, created_ts) VALUES ($1, $2, $3, $4)",
        )
        .bind(alias.alias.as_str())
        .bind(alias.room_id.as_str())
        .bind(alias.creator.as_ref().map(|u| u.as_str()))
        .bind(now_ts())
        .execute(&self.pool)
        .await;
        match res {
            Ok(_) => Ok(()),
            Err(e) => {
                let err = Error::from(e);
                Err(if err.is_unique_violation() {
                    Error::AlreadyExists("alias")
                } else {
                    err
                })
            }
        }
    }

    async fn get_alias(&self, alias: &RoomAliasId) -> Result<Option<RoomAlias>> {
        let row = sqlx::query("SELECT alias, room_id, creator FROM room_aliases WHERE alias = $1")
            .bind(alias.as_str())
            .fetch_optional(&self.pool)
            .await?;
        row.as_ref().map(alias_from_row).transpose()
    }

    async fn delete_alias(&self, alias: &RoomAliasId) -> Result<()> {
        sqlx::query("DELETE FROM room_aliases WHERE alias = $1")
            .bind(alias.as_str())
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn aliases_for_room(&self, room_id: &RoomId) -> Result<Vec<RoomAlias>> {
        let rows = sqlx::query("SELECT alias, room_id, creator FROM room_aliases WHERE room_id = $1 ORDER BY created_ts")
            .bind(room_id.as_str())
            .fetch_all(&self.pool)
            .await?;
        rows.iter().map(alias_from_row).collect()
    }

    async fn rooms_for_user(&self, user_id: &UserId, membership: &str) -> Result<Vec<OwnedRoomId>> {
        let rows = sqlx::query(
            "SELECT room_id FROM room_memberships WHERE user_id = $1 AND membership = $2",
        )
        .bind(user_id.as_str())
        .bind(membership)
        .fetch_all(&self.pool)
        .await?;
        Ok(parse_ids(
            rows.into_iter().map(|r| r.get::<String, _>(0)).collect(),
        ))
    }

    async fn get_membership(
        &self,
        room_id: &RoomId,
        user_id: &UserId,
    ) -> Result<Option<Membership>> {
        let row: Option<MembershipRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT {MEMBERSHIP_COLUMNS} FROM room_memberships WHERE room_id = $1 AND user_id = $2"
        )))
        .bind(room_id.as_str())
        .bind(user_id.as_str())
        .fetch_optional(&self.pool)
        .await?;
        row.map(Membership::try_from).transpose()
    }

    async fn members(&self, room_id: &RoomId, membership: Option<&str>) -> Result<Vec<Membership>> {
        let rows: Vec<MembershipRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT {MEMBERSHIP_COLUMNS} FROM room_memberships
             WHERE room_id = $1 AND ($2::text IS NULL OR membership = $2) ORDER BY stream_ordering"
        )))
        .bind(room_id.as_str())
        .bind(membership)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(Membership::try_from).collect()
    }

    async fn servers_in_room(&self, room_id: &RoomId) -> Result<Vec<OwnedServerName>> {
        // Server names may contain ':' (ports), so derive them from parsed user IDs.
        let mut out = std::collections::BTreeSet::new();
        let rows = sqlx::query(
            "SELECT user_id FROM room_memberships WHERE room_id = $1 AND membership = 'join'",
        )
        .bind(room_id.as_str())
        .fetch_all(&self.pool)
        .await?;
        for r in rows {
            if let Ok(uid) = OwnedUserId::try_from(r.get::<String, _>(0)) {
                out.insert(uid.server_name().to_owned());
            }
        }
        Ok(out.into_iter().collect())
    }

    async fn memberships_for_user(&self, user_id: &UserId) -> Result<Vec<Membership>> {
        let rows: Vec<MembershipRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT {MEMBERSHIP_COLUMNS} FROM room_memberships WHERE user_id = $1 ORDER BY stream_ordering"
        )))
        .bind(user_id.as_str())
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(Membership::try_from).collect()
    }

    async fn memberships_changed_since(
        &self,
        user_id: &UserId,
        since: i64,
    ) -> Result<Vec<Membership>> {
        let rows: Vec<MembershipRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT {MEMBERSHIP_COLUMNS} FROM room_memberships WHERE user_id = $1 AND stream_ordering > $2 ORDER BY stream_ordering"
        )))
        .bind(user_id.as_str())
        .bind(since)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(Membership::try_from).collect()
    }

    async fn memberships_changed_in_rooms(
        &self,
        room_ids: &[OwnedRoomId],
        since: i64,
    ) -> Result<Vec<Membership>> {
        let rows: Vec<MembershipRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT {MEMBERSHIP_COLUMNS} FROM room_memberships WHERE room_id = ANY($1) AND stream_ordering > $2 ORDER BY stream_ordering"
        )))
        .bind(super::strings(room_ids))
        .bind(since)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(Membership::try_from).collect()
    }

    async fn set_forgotten(
        &self,
        room_id: &RoomId,
        user_id: &UserId,
        forgotten: bool,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE room_memberships SET forgotten = $3 WHERE room_id = $1 AND user_id = $2",
        )
        .bind(room_id.as_str())
        .bind(user_id.as_str())
        .bind(forgotten)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn get_txn_event(
        &self,
        user_id: &UserId,
        device_id: Option<&ruma::DeviceId>,
        txn_id: &str,
    ) -> Result<Option<OwnedEventId>> {
        let row = sqlx::query("SELECT event_id FROM event_txn_ids WHERE user_id = $1 AND device_id = $2 AND txn_id = $3")
            .bind(user_id.as_str())
            .bind(device_id.map_or("", ruma::DeviceId::as_str))
            .bind(txn_id)
            .fetch_optional(&self.pool)
            .await?;
        row.map(|r| {
            OwnedEventId::try_from(r.get::<String, _>(0)).map_err(|e| Error::Corrupt(e.to_string()))
        })
        .transpose()
    }

    async fn set_txn_event(
        &self,
        user_id: &UserId,
        device_id: Option<&ruma::DeviceId>,
        txn_key: &str,
        event_id: &ruma::EventId,
        client_txn_id: Option<&str>,
    ) -> Result<()> {
        // The echo row keys the client's transaction ID by event: `event:<id> -> txn`.
        sqlx::query(
            "INSERT INTO event_txn_ids (user_id, device_id, txn_id, event_id, created_ts)
             SELECT $1, $2, k, v, $5 FROM UNNEST($3::text[], $4::text[]) AS kv(k, v)
             ON CONFLICT DO NOTHING",
        )
        .bind(user_id.as_str())
        .bind(device_id.map_or("", ruma::DeviceId::as_str))
        .bind(
            std::iter::once(txn_key.to_owned())
                .chain(client_txn_id.map(|_| format!("event:{event_id}")))
                .collect::<Vec<_>>(),
        )
        .bind(
            std::iter::once(event_id.to_string())
                .chain(client_txn_id.map(str::to_owned))
                .collect::<Vec<_>>(),
        )
        .bind(now_ts())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn get_event_txn_id(
        &self,
        event_id: &ruma::EventId,
        user_id: &UserId,
        device_id: Option<&ruma::DeviceId>,
    ) -> Result<Option<String>> {
        let row = sqlx::query("SELECT event_id FROM event_txn_ids WHERE user_id = $1 AND device_id = $2 AND txn_id = $3")
            .bind(user_id.as_str())
            .bind(device_id.map_or("", ruma::DeviceId::as_str))
            .bind(format!("event:{event_id}"))
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(|r| r.get::<String, _>(0)))
    }

    async fn set_receipt(&self, receipt: crate::Receipt) -> Result<i64> {
        let row = sqlx::query(
            "INSERT INTO receipts (room_id, user_id, receipt_type, thread_id, event_id, data, stream_id)
             VALUES ($1, $2, $3, $4, $5, $6, nextval('receipts_seq'))
             ON CONFLICT (room_id, user_id, receipt_type, thread_id) DO UPDATE SET event_id = EXCLUDED.event_id,
                data = EXCLUDED.data, stream_id = EXCLUDED.stream_id
             RETURNING stream_id",
        )
        .bind(receipt.room_id.as_str())
        .bind(receipt.user_id.as_str())
        .bind(&receipt.receipt_type)
        .bind(receipt.thread_id.as_deref().unwrap_or(""))
        .bind(receipt.event_id.as_str())
        .bind(&receipt.data)
        .fetch_one(&self.pool)
        .await?;
        Ok(row.get::<i64, _>(0))
    }

    async fn receipts_since(&self, since: i64, limit: usize) -> Result<Vec<crate::Receipt>> {
        let rows = sqlx::query(
            "SELECT room_id, user_id, receipt_type, thread_id, event_id, data, stream_id FROM receipts
             WHERE stream_id > $1 ORDER BY stream_id LIMIT $2",
        )
        .bind(since)
        .bind(i64::try_from(limit).unwrap_or(i64::MAX))
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(receipt_from_row).collect()
    }

    async fn rooms_with_receipts_since(
        &self,
        room_ids: &[OwnedRoomId],
        since: i64,
    ) -> Result<Vec<OwnedRoomId>> {
        if room_ids.is_empty() {
            return Ok(Vec::new());
        }
        let ids: Vec<String> = room_ids.iter().map(ToString::to_string).collect();
        let rows = sqlx::query(
            "SELECT DISTINCT room_id FROM receipts WHERE room_id = ANY($1) AND stream_id > $2",
        )
        .bind(&ids)
        .bind(since)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .filter_map(|r| OwnedRoomId::try_from(r.get::<String, _>(0)).ok())
            .collect())
    }

    async fn user_receipts(
        &self,
        room_id: &RoomId,
        user_id: &UserId,
    ) -> Result<Vec<crate::Receipt>> {
        let rows = sqlx::query(
            "SELECT room_id, user_id, receipt_type, thread_id, event_id, data, stream_id FROM receipts
             WHERE room_id = $1 AND user_id = $2",
        )
        .bind(room_id.as_str())
        .bind(user_id.as_str())
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(receipt_from_row).collect()
    }

    async fn receipts_for_room(&self, room_id: &RoomId, since: i64) -> Result<Vec<crate::Receipt>> {
        let rows = sqlx::query(
            "SELECT room_id, user_id, receipt_type, thread_id, event_id, data, stream_id FROM receipts
             WHERE room_id = $1 AND stream_id > $2 ORDER BY stream_id",
        )
        .bind(room_id.as_str())
        .bind(since)
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(receipt_from_row).collect()
    }

    async fn max_receipt_stream_id(&self) -> Result<i64> {
        let row = sqlx::query("SELECT COALESCE(MAX(stream_id), 0) FROM receipts")
            .fetch_one(&self.pool)
            .await?;
        Ok(row.get::<i64, _>(0))
    }

    async fn users_sharing_rooms_with(&self, user_id: &UserId) -> Result<Vec<OwnedUserId>> {
        let rows = sqlx::query(
            "SELECT DISTINCT m2.user_id FROM room_memberships m1
             JOIN room_memberships m2 ON m2.room_id = m1.room_id AND m2.membership = 'join'
             WHERE m1.user_id = $1 AND m1.membership = 'join'",
        )
        .bind(user_id.as_str())
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|r| {
                OwnedUserId::try_from(r.get::<String, _>(0))
                    .map_err(|e| Error::Corrupt(e.to_string()))
            })
            .collect()
    }

    async fn joined_member_count(&self, room_id: &RoomId) -> Result<i64> {
        let row = sqlx::query(
            "SELECT count(*) FROM room_memberships WHERE room_id = $1 AND membership = 'join'",
        )
        .bind(room_id.as_str())
        .fetch_one(&self.pool)
        .await?;
        Ok(row.get::<i64, _>(0))
    }
}

fn alias_from_row(r: &sqlx::postgres::PgRow) -> Result<RoomAlias> {
    let corrupt = |e: ruma::IdParseError| Error::Corrupt(e.to_string());
    Ok(RoomAlias {
        alias: OwnedRoomAliasId::try_from(r.get::<String, _>("alias")).map_err(corrupt)?,
        room_id: OwnedRoomId::try_from(r.get::<String, _>("room_id")).map_err(corrupt)?,
        creator: r
            .get::<Option<String>, _>("creator")
            .map(OwnedUserId::try_from)
            .transpose()
            .map_err(corrupt)?,
    })
}

fn receipt_from_row(r: &sqlx::postgres::PgRow) -> Result<crate::Receipt> {
    let corrupt = |e: ruma::IdParseError| Error::Corrupt(e.to_string());
    let thread: String = r.get("thread_id");
    Ok(crate::Receipt {
        room_id: OwnedRoomId::try_from(r.get::<String, _>("room_id")).map_err(corrupt)?,
        user_id: OwnedUserId::try_from(r.get::<String, _>("user_id")).map_err(corrupt)?,
        receipt_type: r.get("receipt_type"),
        thread_id: if thread.is_empty() {
            None
        } else {
            Some(thread)
        },
        event_id: OwnedEventId::try_from(r.get::<String, _>("event_id")).map_err(corrupt)?,
        data: r.get("data"),
        stream_id: r.get("stream_id"),
    })
}
