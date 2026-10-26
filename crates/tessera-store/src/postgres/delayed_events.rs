use async_trait::async_trait;
use ruma::{DeviceId, RoomId, UserId};
use sqlx::Row;

use super::PostgresStore;
use crate::{DelayedEvent, DelayedEventStore, Error, Result, now_ts};

const COLUMNS: &str = "delay_id, user_id, device_id, room_id, event_type, state_key, content, delay_ms, running_since, txn_id";

fn from_row(r: &sqlx::postgres::PgRow) -> Result<DelayedEvent> {
    let corrupt = |e: ruma::IdParseError| Error::Corrupt(e.to_string());
    Ok(DelayedEvent {
        delay_id: r.get("delay_id"),
        user_id: ruma::OwnedUserId::try_from(r.get::<String, _>("user_id")).map_err(corrupt)?,
        device_id: r
            .get::<Option<String>, _>("device_id")
            .map(ruma::OwnedDeviceId::from),
        room_id: ruma::OwnedRoomId::try_from(r.get::<String, _>("room_id")).map_err(corrupt)?,
        event_type: r.get("event_type"),
        state_key: r.get("state_key"),
        content: r.get("content"),
        delay_ms: r.get("delay_ms"),
        running_since: r.get("running_since"),
        txn_id: r.get("txn_id"),
    })
}

#[async_trait]
impl DelayedEventStore for PostgresStore {
    async fn add_delayed_event(&self, ev: &DelayedEvent) -> Result<()> {
        sqlx::query(
            "INSERT INTO delayed_events (delay_id, user_id, device_id, room_id, event_type, state_key,
                content, delay_ms, running_since, txn_id, created_ts)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
        )
        .bind(&ev.delay_id)
        .bind(ev.user_id.as_str())
        .bind(ev.device_id.as_deref().map(DeviceId::as_str))
        .bind(ev.room_id.as_str())
        .bind(&ev.event_type)
        .bind(&ev.state_key)
        .bind(&ev.content)
        .bind(ev.delay_ms)
        .bind(ev.running_since)
        .bind(&ev.txn_id)
        .bind(now_ts())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn get_delayed_event(&self, delay_id: &str) -> Result<Option<DelayedEvent>> {
        let row = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT {COLUMNS} FROM delayed_events WHERE delay_id = $1"
        )))
        .bind(delay_id)
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(from_row).transpose()
    }

    async fn delayed_event_by_txn(
        &self,
        user_id: &UserId,
        device_id: Option<&DeviceId>,
        txn_id: &str,
    ) -> Result<Option<DelayedEvent>> {
        let row = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT {COLUMNS} FROM delayed_events
             WHERE user_id = $1 AND COALESCE(device_id, '') = $2 AND txn_id = $3"
        )))
        .bind(user_id.as_str())
        .bind(device_id.map_or("", DeviceId::as_str))
        .bind(txn_id)
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(from_row).transpose()
    }

    async fn delayed_events_for_user(&self, user_id: &UserId) -> Result<Vec<DelayedEvent>> {
        let rows = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT {COLUMNS} FROM delayed_events WHERE user_id = $1 ORDER BY created_ts, delay_id"
        )))
        .bind(user_id.as_str())
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(from_row).collect()
    }

    async fn remove_delayed_event(&self, delay_id: &str) -> Result<bool> {
        let n = sqlx::query("DELETE FROM delayed_events WHERE delay_id = $1")
            .bind(delay_id)
            .execute(&self.pool)
            .await?
            .rows_affected();
        Ok(n > 0)
    }

    async fn restart_delayed_event(&self, delay_id: &str, now: i64) -> Result<bool> {
        let n = sqlx::query("UPDATE delayed_events SET running_since = $2 WHERE delay_id = $1")
            .bind(delay_id)
            .bind(now)
            .execute(&self.pool)
            .await?
            .rows_affected();
        Ok(n > 0)
    }

    async fn due_delayed_events(&self, now: i64) -> Result<Vec<DelayedEvent>> {
        let rows = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT {COLUMNS} FROM delayed_events WHERE running_since + delay_ms <= $1
             ORDER BY running_since + delay_ms, created_ts"
        )))
        .bind(now)
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(from_row).collect()
    }

    async fn next_delayed_event_due(&self) -> Result<Option<i64>> {
        let row = sqlx::query("SELECT MIN(running_since + delay_ms) FROM delayed_events")
            .fetch_one(&self.pool)
            .await?;
        Ok(row.get::<Option<i64>, _>(0))
    }

    async fn remove_delayed_state_events(
        &self,
        room_id: &RoomId,
        event_type: &str,
        state_key: &str,
    ) -> Result<u64> {
        Ok(sqlx::query(
            "DELETE FROM delayed_events WHERE room_id = $1 AND event_type = $2 AND state_key = $3",
        )
        .bind(room_id.as_str())
        .bind(event_type)
        .bind(state_key)
        .execute(&self.pool)
        .await?
        .rows_affected())
    }

    async fn count_delayed_state_events(&self) -> Result<u64> {
        let row = sqlx::query("SELECT COUNT(*) FROM delayed_events WHERE state_key IS NOT NULL")
            .fetch_one(&self.pool)
            .await?;
        Ok(u64::try_from(row.get::<i64, _>(0)).unwrap_or(0))
    }
}
