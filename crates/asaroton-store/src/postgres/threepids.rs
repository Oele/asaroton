//! Email addresses on accounts, and their verification.

use async_trait::async_trait;
use ruma::{OwnedUserId, UserId};
use sqlx::Row;

use super::PostgresStore;
use crate::{Error, Result, ThreepidSession, ThreepidStore, Ts, UserThreepid};

const SESSION_COLUMNS: &str = "sid, client_secret, medium, address, token_hash, send_attempt, \
     next_link, created_ts, last_sent_ts, expires_ts, validated_at, failures";

fn session_from(r: &sqlx::postgres::PgRow) -> ThreepidSession {
    ThreepidSession {
        sid: r.get("sid"),
        client_secret: r.get("client_secret"),
        medium: r.get("medium"),
        address: r.get("address"),
        token_hash: r.get("token_hash"),
        send_attempt: r.get("send_attempt"),
        next_link: r.get("next_link"),
        created_ts: r.get("created_ts"),
        last_sent_ts: r.get("last_sent_ts"),
        expires_ts: r.get("expires_ts"),
        validated_at: r.get("validated_at"),
        failures: r.get("failures"),
    }
}

fn user_id(s: String) -> Result<OwnedUserId> {
    OwnedUserId::try_from(s).map_err(|e| Error::Corrupt(format!("user_threepids.user_id: {e}")))
}

#[async_trait]
impl ThreepidStore for PostgresStore {
    async fn threepid_owner(&self, medium: &str, address: &str) -> Result<Option<OwnedUserId>> {
        let row =
            sqlx::query("SELECT user_id FROM user_threepids WHERE medium = $1 AND address = $2")
                .bind(medium)
                .bind(address)
                .fetch_optional(&self.pool)
                .await?;
        row.map(|r| user_id(r.get(0))).transpose()
    }

    async fn user_threepids(&self, user: &UserId) -> Result<Vec<UserThreepid>> {
        let rows = sqlx::query(
            "SELECT user_id, medium, address, validated_at, added_at FROM user_threepids
              WHERE user_id = $1 ORDER BY added_at",
        )
        .bind(user.as_str())
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|r| {
                Ok(UserThreepid {
                    user_id: user_id(r.get(0))?,
                    medium: r.get(1),
                    address: r.get(2),
                    validated_at: r.get(3),
                    added_at: r.get(4),
                })
            })
            .collect()
    }

    async fn add_user_threepid(&self, t: UserThreepid) -> Result<bool> {
        let done = sqlx::query(
            "INSERT INTO user_threepids (user_id, medium, address, validated_at, added_at)
             VALUES ($1, $2, $3, $4, $5) ON CONFLICT (medium, address) DO NOTHING",
        )
        .bind(t.user_id.as_str())
        .bind(&t.medium)
        .bind(&t.address)
        .bind(t.validated_at)
        .bind(t.added_at)
        .execute(&self.pool)
        .await?;
        Ok(done.rows_affected() == 1)
    }

    async fn delete_user_threepid(
        &self,
        user: &UserId,
        medium: &str,
        address: &str,
    ) -> Result<bool> {
        let done = sqlx::query(
            "DELETE FROM user_threepids WHERE user_id = $1 AND medium = $2 AND address = $3",
        )
        .bind(user.as_str())
        .bind(medium)
        .bind(address)
        .execute(&self.pool)
        .await?;
        Ok(done.rows_affected() > 0)
    }

    async fn delete_user_threepids(&self, user: &UserId) -> Result<()> {
        sqlx::query("DELETE FROM user_threepids WHERE user_id = $1")
            .bind(user.as_str())
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn threepid_session_for(
        &self,
        client_secret: &str,
        medium: &str,
        address: &str,
    ) -> Result<Option<ThreepidSession>> {
        let row = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT {SESSION_COLUMNS} FROM threepid_sessions
              WHERE client_secret = $1 AND medium = $2 AND address = $3"
        )))
        .bind(client_secret)
        .bind(medium)
        .bind(address)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.as_ref().map(session_from))
    }

    async fn threepid_session(&self, sid: &str) -> Result<Option<ThreepidSession>> {
        let row = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT {SESSION_COLUMNS} FROM threepid_sessions WHERE sid = $1"
        )))
        .bind(sid)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.as_ref().map(session_from))
    }

    async fn put_threepid_session(&self, s: ThreepidSession) -> Result<()> {
        sqlx::query(
            "INSERT INTO threepid_sessions (sid, client_secret, medium, address, token_hash,
                 send_attempt, next_link, created_ts, last_sent_ts, expires_ts, validated_at, failures)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
             ON CONFLICT (client_secret, medium, address) DO UPDATE SET
                 sid = EXCLUDED.sid, token_hash = EXCLUDED.token_hash,
                 send_attempt = EXCLUDED.send_attempt, next_link = EXCLUDED.next_link,
                 created_ts = EXCLUDED.created_ts, last_sent_ts = EXCLUDED.last_sent_ts,
                 expires_ts = EXCLUDED.expires_ts, validated_at = EXCLUDED.validated_at,
                 failures = EXCLUDED.failures",
        )
        .bind(&s.sid)
        .bind(&s.client_secret)
        .bind(&s.medium)
        .bind(&s.address)
        .bind(&s.token_hash)
        .bind(s.send_attempt)
        .bind(&s.next_link)
        .bind(s.created_ts)
        .bind(s.last_sent_ts)
        .bind(s.expires_ts)
        .bind(s.validated_at)
        .bind(s.failures)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn update_threepid_session(&self, sid: &str, validated_at: Option<Ts>) -> Result<()> {
        let sql = if validated_at.is_some() {
            "UPDATE threepid_sessions SET validated_at = $2 WHERE sid = $1"
        } else {
            "UPDATE threepid_sessions SET failures = failures + 1 WHERE sid = $1"
        };
        sqlx::query(sql)
            .bind(sid)
            .bind(validated_at)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn delete_threepid_session(&self, sid: &str) -> Result<()> {
        sqlx::query("DELETE FROM threepid_sessions WHERE sid = $1")
            .bind(sid)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn purge_expired_threepid_sessions(&self, now: Ts) -> Result<()> {
        sqlx::query("DELETE FROM threepid_sessions WHERE expires_ts <= $1")
            .bind(now)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}
