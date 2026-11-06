use async_trait::async_trait;
use ruma::{EventId, RoomId, UserId};
use sqlx::Row;

use super::PostgresStore;
use super::streams::Stream;
use crate::{Error, Result, ThreadSubscription, ThreadSubscriptionStore};

const COLUMNS: &str = "room_id, thread_root, subscribed, automatic, stream_id, unsubscribed_at";

fn from_row(r: &sqlx::postgres::PgRow) -> Result<ThreadSubscription> {
    let corrupt = |e: ruma::IdParseError| Error::Corrupt(e.to_string());
    Ok(ThreadSubscription {
        room_id: ruma::OwnedRoomId::try_from(r.get::<String, _>("room_id")).map_err(corrupt)?,
        thread_root: ruma::OwnedEventId::try_from(r.get::<String, _>("thread_root"))
            .map_err(corrupt)?,
        subscribed: r.get("subscribed"),
        automatic: r.get("automatic"),
        stream_id: r.get("stream_id"),
        unsubscribed_at: r.get("unsubscribed_at"),
    })
}

#[async_trait]
impl ThreadSubscriptionStore for PostgresStore {
    async fn get_thread_subscription(
        &self,
        user_id: &UserId,
        room_id: &RoomId,
        thread_root: &EventId,
    ) -> Result<Option<ThreadSubscription>> {
        let row = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT {COLUMNS} FROM thread_subscriptions
             WHERE user_id = $1 AND room_id = $2 AND thread_root = $3"
        )))
        .bind(user_id.as_str())
        .bind(room_id.as_str())
        .bind(thread_root.as_str())
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(from_row).transpose()
    }

    async fn subscribe_thread(
        &self,
        user_id: &UserId,
        room_id: &RoomId,
        thread_root: &EventId,
        automatic: bool,
    ) -> Result<i64> {
        let writing = self.streams().await?.writing(Stream::ThreadSubscriptions);
        let row = sqlx::query(
            "INSERT INTO thread_subscriptions (user_id, room_id, thread_root, subscribed, automatic, stream_id, unsubscribed_at)
             VALUES ($1, $2, $3, TRUE, $4, nextval('thread_subscriptions_seq'), NULL)
             ON CONFLICT (user_id, room_id, thread_root) DO UPDATE SET subscribed = TRUE,
                automatic = EXCLUDED.automatic, stream_id = EXCLUDED.stream_id, unsubscribed_at = NULL
             RETURNING stream_id",
        )
        .bind(user_id.as_str())
        .bind(room_id.as_str())
        .bind(thread_root.as_str())
        .bind(automatic)
        .fetch_one(&self.pool)
        .await?;
        let position = row.get::<i64, _>(0);
        writing.done(position).await;
        Ok(position)
    }

    async fn unsubscribe_thread(
        &self,
        user_id: &UserId,
        room_id: &RoomId,
        thread_root: &EventId,
        at: i64,
    ) -> Result<i64> {
        let writing = self.streams().await?.writing(Stream::ThreadSubscriptions);
        let row = sqlx::query(
            "INSERT INTO thread_subscriptions (user_id, room_id, thread_root, subscribed, automatic, stream_id, unsubscribed_at)
             VALUES ($1, $2, $3, FALSE, FALSE, nextval('thread_subscriptions_seq'), $4)
             ON CONFLICT (user_id, room_id, thread_root) DO UPDATE SET subscribed = FALSE,
                automatic = FALSE, stream_id = EXCLUDED.stream_id, unsubscribed_at = EXCLUDED.unsubscribed_at
             RETURNING stream_id",
        )
        .bind(user_id.as_str())
        .bind(room_id.as_str())
        .bind(thread_root.as_str())
        .bind(at)
        .fetch_one(&self.pool)
        .await?;
        let position = row.get::<i64, _>(0);
        writing.done(position).await;
        Ok(position)
    }

    async fn thread_subscription_changes(
        &self,
        user_id: &UserId,
        above: Option<i64>,
        below: Option<i64>,
        limit: usize,
        subscribed_only: bool,
    ) -> Result<Vec<ThreadSubscription>> {
        let rows = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT {COLUMNS} FROM thread_subscriptions
             WHERE user_id = $1 AND ($2::bigint IS NULL OR stream_id > $2)
               AND ($3::bigint IS NULL OR stream_id < $3) AND (NOT $4 OR subscribed)
             ORDER BY stream_id DESC LIMIT $5"
        )))
        .bind(user_id.as_str())
        .bind(above)
        .bind(below)
        .bind(subscribed_only)
        .bind(i64::try_from(limit).unwrap_or(i64::MAX))
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(from_row).collect()
    }

    async fn max_thread_subscription_stream_id(&self) -> Result<i64> {
        let row = sqlx::query("SELECT COALESCE(MAX(stream_id), 0) FROM thread_subscriptions")
            .fetch_one(&self.pool)
            .await?;
        Ok(row.get::<i64, _>(0))
    }
}
