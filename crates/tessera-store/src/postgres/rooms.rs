use async_trait::async_trait;
use ruma::{
    OwnedEventId, OwnedRoomAliasId, OwnedRoomId, OwnedServerName, OwnedUserId, RoomAliasId, RoomId,
    RoomVersionId, UserId, events::room::member::MembershipState,
};
use sqlx::Row;

use super::PostgresStore;
use crate::{Error, Membership, Result, RoomAlias, RoomInfo, RoomStore, now_ts, parse_ids};

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
            room_id: OwnedRoomId::try_from(r.room_id).map_err(|e| Error::Corrupt(e.to_string()))?,
            room_version: RoomVersionId::try_from(r.room_version.as_str())
                .map_err(|e| Error::Corrupt(e.to_string()))?,
            creator: r
                .creator
                .map(OwnedUserId::try_from)
                .transpose()
                .map_err(|e| Error::Corrupt(e.to_string()))?,
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
}

impl TryFrom<MembershipRow> for Membership {
    type Error = Error;
    fn try_from(r: MembershipRow) -> Result<Self> {
        let corrupt = |e: ruma::IdParseError| Error::Corrupt(e.to_string());
        Ok(Membership {
            room_id: OwnedRoomId::try_from(r.room_id).map_err(corrupt)?,
            user_id: OwnedUserId::try_from(r.user_id).map_err(corrupt)?,
            membership: MembershipState::from(r.membership),
            event_id: OwnedEventId::try_from(r.event_id).map_err(corrupt)?,
            sender: OwnedUserId::try_from(r.sender).map_err(corrupt)?,
            display_name: r.display_name,
            avatar_url: r.avatar_url,
            stream_ordering: r.stream_ordering,
        })
    }
}

const ROOM_COLUMNS: &str = "room_id, room_version, creator, is_public, created_ts";
pub(crate) const MEMBERSHIP_COLUMNS: &str =
    "room_id, user_id, membership, event_id, sender, display_name, avatar_url, stream_ordering";

#[async_trait]
impl RoomStore for PostgresStore {
    async fn create_room(&self, room: RoomInfo) -> Result<()> {
        sqlx::query(
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
        Ok(())
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
