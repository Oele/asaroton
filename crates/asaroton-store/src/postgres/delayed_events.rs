use asaroton_core::sticky::{Sticky, StickyMarker};
use async_trait::async_trait;
use ruma::{DeviceId, RoomId, UserId};
use sqlx::Row;

use super::PostgresStore;
use crate::{DelayedEvent, DelayedEventFinalised, DelayedEventStore, Error, Result, now_ts};

const COLUMNS: &str = "delay_id, user_id, device_id, room_id, event_type, state_key, content, delay_ms, running_since, txn_id, finalised_ts, finalised_event_id, finalised_error, sticky_marker, sticky_duration_ms";

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
        finalised: r
            .get::<Option<i64>, _>("finalised_ts")
            .map(|ts| DelayedEventFinalised {
                ts,
                event_id: r.get("finalised_event_id"),
                error: r.get("finalised_error"),
            }),
        sticky: sticky_from_row(r),
    })
}

fn sticky_from_row(r: &sqlx::postgres::PgRow) -> Option<Sticky> {
    let marker = match r.get::<Option<String>, _>("sticky_marker")?.as_str() {
        m if m == StickyMarker::Stable.key() => StickyMarker::Stable,
        m if m == StickyMarker::Unstable.key() => StickyMarker::Unstable,
        _ => return None,
    };
    let duration_ms = r.get::<Option<i64>, _>("sticky_duration_ms")?;
    (duration_ms > 0).then_some(Sticky {
        marker,
        duration_ms,
    })
}

#[async_trait]
impl DelayedEventStore for PostgresStore {
    async fn add_delayed_event(&self, ev: &DelayedEvent) -> Result<()> {
        sqlx::query(
            "INSERT INTO delayed_events (delay_id, user_id, device_id, room_id, event_type, state_key,
                content, delay_ms, running_since, txn_id, created_ts, sticky_marker,
                sticky_duration_ms)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)",
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
        .bind(ev.sticky.map(|s| s.marker.key()))
        .bind(ev.sticky.map(|s| s.duration_ms))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn get_delayed_event(&self, delay_id: &str) -> Result<Option<DelayedEvent>> {
        let row = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT {COLUMNS} FROM delayed_events WHERE delay_id = $1 AND finalised_ts IS NULL"
        )))
        .bind(delay_id)
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(from_row).transpose()
    }

    async fn get_delayed_event_any(&self, delay_id: &str) -> Result<Option<DelayedEvent>> {
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
            "SELECT {COLUMNS} FROM delayed_events WHERE user_id = $1 AND finalised_ts IS NULL
             ORDER BY created_ts, delay_id"
        )))
        .bind(user_id.as_str())
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(from_row).collect()
    }

    async fn finalise_delayed_event(&self, delay_id: &str, now: i64) -> Result<bool> {
        let n = sqlx::query(
            "UPDATE delayed_events SET finalised_ts = $2 WHERE delay_id = $1 AND finalised_ts IS NULL",
        )
        .bind(delay_id)
        .bind(now)
        .execute(&self.pool)
        .await?
        .rows_affected();
        Ok(n > 0)
    }

    async fn unfinalise_delayed_event(&self, delay_id: &str) -> Result<()> {
        sqlx::query(
            "UPDATE delayed_events SET finalised_ts = NULL, finalised_event_id = NULL, finalised_error = NULL
             WHERE delay_id = $1",
        )
        .bind(delay_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn record_delayed_event_outcome(
        &self,
        delay_id: &str,
        event_id: Option<&ruma::EventId>,
        error: Option<&serde_json::Value>,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let user: Option<String> = sqlx::query(
            "UPDATE delayed_events SET finalised_event_id = $2, finalised_error = $3
             WHERE delay_id = $1 RETURNING user_id",
        )
        .bind(delay_id)
        .bind(event_id.map(ruma::EventId::as_str))
        .bind(error)
        .fetch_optional(&mut *tx)
        .await?
        .map(|r| r.get(0));
        if let (Some(user), Some(event_id)) = (user, event_id) {
            sqlx::query(
                "INSERT INTO event_delay_ids (event_id, user_id, delay_id) VALUES ($1, $2, $3)
                 ON CONFLICT (event_id) DO NOTHING",
            )
            .bind(event_id.as_str())
            .bind(user)
            .bind(delay_id)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn prune_finalised_delayed_events(&self, before: i64, keep_per_user: i64) -> Result<u64> {
        let old = sqlx::query(
            "DELETE FROM delayed_events WHERE finalised_ts IS NOT NULL AND finalised_ts < $1",
        )
        .bind(before)
        .execute(&self.pool)
        .await?
        .rows_affected();
        let surplus = sqlx::query(
            "DELETE FROM delayed_events WHERE delay_id IN (
                SELECT delay_id FROM (
                    SELECT delay_id, row_number() OVER (PARTITION BY user_id ORDER BY finalised_ts DESC) AS n
                      FROM delayed_events WHERE finalised_ts IS NOT NULL) x
                 WHERE n > $1)",
        )
        .bind(keep_per_user)
        .execute(&self.pool)
        .await?
        .rows_affected();
        Ok(old + surplus)
    }

    async fn delay_ids_for_events(
        &self,
        user_id: &UserId,
        event_ids: &[ruma::OwnedEventId],
    ) -> Result<std::collections::HashMap<ruma::OwnedEventId, String>> {
        if event_ids.is_empty() {
            return Ok(std::collections::HashMap::new());
        }
        let ids: Vec<&str> = event_ids.iter().map(|e| e.as_str()).collect();
        let rows = sqlx::query(
            "SELECT event_id, delay_id FROM event_delay_ids WHERE user_id = $1 AND event_id = ANY($2)",
        )
        .bind(user_id.as_str())
        .bind(&ids)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .filter_map(|r| {
                let id = ruma::OwnedEventId::try_from(r.get::<String, _>(0)).ok()?;
                Some((id, r.get::<String, _>(1)))
            })
            .collect())
    }

    async fn restart_delayed_event(&self, delay_id: &str, now: i64) -> Result<bool> {
        let n = sqlx::query(
            "UPDATE delayed_events SET running_since = $2 WHERE delay_id = $1 AND finalised_ts IS NULL",
        )
            .bind(delay_id)
            .bind(now)
            .execute(&self.pool)
            .await?
            .rows_affected();
        Ok(n > 0)
    }

    async fn due_delayed_events(&self, now: i64) -> Result<Vec<DelayedEvent>> {
        let rows = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT {COLUMNS} FROM delayed_events
             WHERE finalised_ts IS NULL AND running_since + delay_ms <= $1
             ORDER BY running_since + delay_ms, created_ts"
        )))
        .bind(now)
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(from_row).collect()
    }

    async fn next_delayed_event_due(&self) -> Result<Option<i64>> {
        let row = sqlx::query(
            "SELECT MIN(running_since + delay_ms) FROM delayed_events WHERE finalised_ts IS NULL",
        )
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
            "UPDATE delayed_events SET finalised_ts = $4
             WHERE room_id = $1 AND event_type = $2 AND state_key = $3 AND finalised_ts IS NULL",
        )
        .bind(room_id.as_str())
        .bind(event_type)
        .bind(state_key)
        .bind(now_ts())
        .execute(&self.pool)
        .await?
        .rows_affected())
    }

    async fn count_delayed_state_events(&self) -> Result<u64> {
        let row = sqlx::query(
            "SELECT COUNT(*) FROM delayed_events WHERE state_key IS NOT NULL AND finalised_ts IS NULL",
        )
            .fetch_one(&self.pool)
            .await?;
        Ok(u64::try_from(row.get::<i64, _>(0)).unwrap_or(0))
    }
}
