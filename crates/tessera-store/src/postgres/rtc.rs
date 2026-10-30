//! `OpenID` tokens and `MatrixRTC` over `LiveKit` (MSC4195).

use async_trait::async_trait;
use ruma::{OwnedUserId, RoomId, UserId};
use sqlx::Row;

use super::PostgresStore;
use crate::{DelegatedLeave, Error, Result, RtcParticipant, RtcStore};

fn participant_from_row(r: &sqlx::postgres::PgRow) -> Result<RtcParticipant> {
    let corrupt = |e: ruma::IdParseError| Error::Corrupt(e.to_string());
    Ok(RtcParticipant {
        lk_room: r.get("lk_room"),
        lk_identity: r.get("lk_identity"),
        user_id: OwnedUserId::try_from(r.get::<String, _>("user_id")).map_err(corrupt)?,
        room_id: ruma::OwnedRoomId::try_from(r.get::<String, _>("room_id")).map_err(corrupt)?,
        issued_ts: r.get("issued_ts"),
    })
}

fn leave_from_row(r: &sqlx::postgres::PgRow) -> Result<DelegatedLeave> {
    let corrupt = |e: ruma::IdParseError| Error::Corrupt(e.to_string());
    Ok(DelegatedLeave {
        delay_id: r.get("delay_id"),
        user_id: OwnedUserId::try_from(r.get::<String, _>("user_id")).map_err(corrupt)?,
        room_id: ruma::OwnedRoomId::try_from(r.get::<String, _>("room_id")).map_err(corrupt)?,
        lk_room: r.get("lk_room"),
        lk_identity: r.get("lk_identity"),
        created_ts: r.get("created_ts"),
        connected_ts: r.get("connected_ts"),
    })
}

const LEAVE_COLUMNS: &str =
    "delay_id, user_id, room_id, lk_room, lk_identity, created_ts, connected_ts";

#[async_trait]
impl RtcStore for PostgresStore {
    async fn add_openid_token(&self, token: &str, user_id: &UserId, expires_ts: i64) -> Result<()> {
        sqlx::query("INSERT INTO openid_tokens (token, user_id, expires_ts) VALUES ($1, $2, $3)")
            .bind(token)
            .bind(user_id.as_str())
            .bind(expires_ts)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn openid_token_owner(&self, token: &str, now: i64) -> Result<Option<OwnedUserId>> {
        let row =
            sqlx::query("SELECT user_id FROM openid_tokens WHERE token = $1 AND expires_ts > $2")
                .bind(token)
                .bind(now)
                .fetch_optional(&self.pool)
                .await?;
        row.map(|r| {
            OwnedUserId::try_from(r.get::<String, _>("user_id"))
                .map_err(|e| Error::Corrupt(e.to_string()))
        })
        .transpose()
    }

    async fn purge_expired_openid_tokens(&self, now: i64) -> Result<u64> {
        let res = sqlx::query("DELETE FROM openid_tokens WHERE expires_ts <= $1")
            .bind(now)
            .execute(&self.pool)
            .await?;
        Ok(res.rows_affected())
    }

    async fn add_rtc_participant(&self, p: &RtcParticipant) -> Result<()> {
        sqlx::query(
            "INSERT INTO rtc_participants (lk_room, lk_identity, user_id, room_id, issued_ts)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (lk_room, lk_identity) DO UPDATE
                SET user_id = EXCLUDED.user_id, room_id = EXCLUDED.room_id,
                    issued_ts = EXCLUDED.issued_ts",
        )
        .bind(&p.lk_room)
        .bind(&p.lk_identity)
        .bind(p.user_id.as_str())
        .bind(p.room_id.as_str())
        .bind(p.issued_ts)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn rtc_participant(
        &self,
        lk_room: &str,
        lk_identity: &str,
    ) -> Result<Option<RtcParticipant>> {
        let row = sqlx::query(
            "SELECT lk_room, lk_identity, user_id, room_id, issued_ts FROM rtc_participants
             WHERE lk_room = $1 AND lk_identity = $2",
        )
        .bind(lk_room)
        .bind(lk_identity)
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(participant_from_row).transpose()
    }

    async fn rtc_participants_for(
        &self,
        room_id: &RoomId,
        user_id: &UserId,
    ) -> Result<Vec<RtcParticipant>> {
        let rows = sqlx::query(
            "SELECT lk_room, lk_identity, user_id, room_id, issued_ts FROM rtc_participants
             WHERE room_id = $1 AND user_id = $2",
        )
        .bind(room_id.as_str())
        .bind(user_id.as_str())
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(participant_from_row).collect()
    }

    async fn remove_rtc_participant(&self, lk_room: &str, lk_identity: &str) -> Result<()> {
        sqlx::query("DELETE FROM rtc_participants WHERE lk_room = $1 AND lk_identity = $2")
            .bind(lk_room)
            .bind(lk_identity)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn purge_rtc_participants_before(&self, issued_before: i64) -> Result<u64> {
        let res = sqlx::query("DELETE FROM rtc_participants WHERE issued_ts < $1")
            .bind(issued_before)
            .execute(&self.pool)
            .await?;
        Ok(res.rows_affected())
    }

    async fn add_delegated_leave(&self, leave: &DelegatedLeave) -> Result<bool> {
        let mut tx = self.pool.begin().await?;
        // One delegation per participant: a newer delay ID replaces the old one.
        sqlx::query(
            "DELETE FROM rtc_delegated_leaves WHERE lk_room = $1 AND lk_identity = $2 AND delay_id <> $3",
        )
        .bind(&leave.lk_room)
        .bind(&leave.lk_identity)
        .bind(&leave.delay_id)
        .execute(&mut *tx)
        .await?;
        let res = sqlx::query(
            "INSERT INTO rtc_delegated_leaves
                (delay_id, user_id, room_id, lk_room, lk_identity, created_ts, connected_ts)
             SELECT $1, $2, $3, $4, $5, $6, $7
             WHERE EXISTS (SELECT 1 FROM delayed_events WHERE delay_id = $1)
             ON CONFLICT (delay_id) DO UPDATE
                SET lk_room = EXCLUDED.lk_room, lk_identity = EXCLUDED.lk_identity,
                    created_ts = EXCLUDED.created_ts, connected_ts = EXCLUDED.connected_ts",
        )
        .bind(&leave.delay_id)
        .bind(leave.user_id.as_str())
        .bind(leave.room_id.as_str())
        .bind(&leave.lk_room)
        .bind(&leave.lk_identity)
        .bind(leave.created_ts)
        .bind(leave.connected_ts)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(res.rows_affected() > 0)
    }

    async fn delegated_leaves(&self) -> Result<Vec<DelegatedLeave>> {
        let rows = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT {LEAVE_COLUMNS} FROM rtc_delegated_leaves ORDER BY created_ts"
        )))
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(leave_from_row).collect()
    }

    async fn delegated_leave_for(
        &self,
        lk_room: &str,
        lk_identity: &str,
    ) -> Result<Option<DelegatedLeave>> {
        let row = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT {LEAVE_COLUMNS} FROM rtc_delegated_leaves WHERE lk_room = $1 AND lk_identity = $2"
        )))
        .bind(lk_room)
        .bind(lk_identity)
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(leave_from_row).transpose()
    }

    async fn mark_delegated_leave_connected(&self, delay_id: &str, now: i64) -> Result<()> {
        sqlx::query("UPDATE rtc_delegated_leaves SET connected_ts = $2 WHERE delay_id = $1")
            .bind(delay_id)
            .bind(now)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn remove_delegated_leave(&self, delay_id: &str) -> Result<bool> {
        let res = sqlx::query("DELETE FROM rtc_delegated_leaves WHERE delay_id = $1")
            .bind(delay_id)
            .execute(&self.pool)
            .await?;
        Ok(res.rows_affected() > 0)
    }
}
