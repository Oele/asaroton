use async_trait::async_trait;
use ruma::{OwnedRoomId, RoomId, UserId};
use sqlx::Row;

use super::PostgresStore;
use crate::{AccountData, AccountDataStore, Error, Result};

#[async_trait]
impl AccountDataStore for PostgresStore {
    async fn set_account_data(
        &self,
        user_id: &UserId,
        room_id: Option<&RoomId>,
        data_type: &str,
        content: &serde_json::Value,
    ) -> Result<i64> {
        let row = sqlx::query(
            "INSERT INTO account_data (user_id, room_id, data_type, content, stream_id)
             VALUES ($1, $2, $3, $4, nextval('account_data_seq'))
             ON CONFLICT (user_id, room_id, data_type) DO UPDATE SET content = EXCLUDED.content, stream_id = EXCLUDED.stream_id
             RETURNING stream_id",
        )
        .bind(user_id.as_str())
        .bind(room_id.map_or("", RoomId::as_str))
        .bind(data_type)
        .bind(content)
        .fetch_one(&self.pool)
        .await?;
        Ok(row.get::<i64, _>(0))
    }

    async fn get_account_data(
        &self,
        user_id: &UserId,
        room_id: Option<&RoomId>,
        data_type: &str,
    ) -> Result<Option<AccountData>> {
        let row = sqlx::query(
            "SELECT room_id, data_type, content, stream_id FROM account_data
             WHERE user_id = $1 AND room_id = $2 AND data_type = $3",
        )
        .bind(user_id.as_str())
        .bind(room_id.map_or("", RoomId::as_str))
        .bind(data_type)
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(account_data_from_row).transpose()
    }

    async fn account_data_since(&self, user_id: &UserId, since: i64) -> Result<Vec<AccountData>> {
        let rows = sqlx::query(
            "SELECT room_id, data_type, content, stream_id FROM account_data
             WHERE user_id = $1 AND stream_id > $2 ORDER BY stream_id",
        )
        .bind(user_id.as_str())
        .bind(since)
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(account_data_from_row).collect()
    }

    async fn max_account_data_stream_id(&self) -> Result<i64> {
        let row = sqlx::query("SELECT COALESCE(MAX(stream_id), 0) FROM account_data")
            .fetch_one(&self.pool)
            .await?;
        Ok(row.get::<i64, _>(0))
    }

    async fn create_filter(&self, user_id: &UserId, filter: &serde_json::Value) -> Result<i64> {
        let row = sqlx::query(
            "INSERT INTO filters (user_id, filter_id, filter_json)
             VALUES ($1, (SELECT COALESCE(MAX(filter_id), 0) + 1 FROM filters WHERE user_id = $1), $2)
             RETURNING filter_id",
        )
        .bind(user_id.as_str())
        .bind(filter)
        .fetch_one(&self.pool)
        .await?;
        Ok(row.get::<i64, _>(0))
    }

    async fn get_filter(
        &self,
        user_id: &UserId,
        filter_id: i64,
    ) -> Result<Option<serde_json::Value>> {
        let row =
            sqlx::query("SELECT filter_json FROM filters WHERE user_id = $1 AND filter_id = $2")
                .bind(user_id.as_str())
                .bind(filter_id)
                .fetch_optional(&self.pool)
                .await?;
        Ok(row.map(|r| r.get::<serde_json::Value, _>(0)))
    }
}

fn account_data_from_row(r: &sqlx::postgres::PgRow) -> Result<AccountData> {
    let room_id: String = r.get("room_id");
    Ok(AccountData {
        room_id: if room_id.is_empty() {
            None
        } else {
            Some(OwnedRoomId::try_from(room_id).map_err(|e| Error::Corrupt(e.to_string()))?)
        },
        data_type: r.get("data_type"),
        content: r.get("content"),
        stream_id: r.get("stream_id"),
    })
}
