//! Key backups.

use async_trait::async_trait;
use ruma::{OwnedRoomId, RoomId, UserId};
use sqlx::Row;

use super::PostgresStore;
use crate::{BackupKey, BackupVersion, Error, KeyBackupStore, Result};

fn parse_version(v: &str) -> Option<i64> {
    v.parse::<i64>().ok().filter(|n| *n > 0)
}

impl PostgresStore {
    async fn backup_row(
        &self,
        user_id: &UserId,
        version: Option<i64>,
    ) -> Result<Option<BackupVersion>> {
        let row = sqlx::query(
            "SELECT v.version, v.algorithm, v.etag,
                    (SELECT COUNT(*) FROM key_backup_keys k
                      WHERE k.user_id = v.user_id AND k.version = v.version) AS count
               FROM key_backup_versions v
              WHERE v.user_id = $1 AND NOT v.deleted AND ($2::bigint IS NULL OR v.version = $2)
              ORDER BY v.version DESC LIMIT 1",
        )
        .bind(user_id.as_str())
        .bind(version)
        .fetch_optional(&self.pool)
        .await?;
        row.map(|r| {
            let algorithm: String = r.get("algorithm");
            Ok(BackupVersion {
                version: r.get::<i64, _>("version").to_string(),
                algorithm: serde_json::from_str(&algorithm)
                    .map_err(|e| Error::Corrupt(format!("backup algorithm: {e}")))?,
                etag: r.get("etag"),
                count: r.get("count"),
            })
        })
        .transpose()
    }

    async fn bump_etag(&self, user_id: &UserId, version: i64) -> Result<(i64, i64)> {
        let row = sqlx::query(
            "UPDATE key_backup_versions SET etag = etag + 1 WHERE user_id = $1 AND version = $2
             RETURNING etag, (SELECT COUNT(*) FROM key_backup_keys k WHERE k.user_id = $1 AND k.version = $2)",
        )
        .bind(user_id.as_str())
        .bind(version)
        .fetch_one(&self.pool)
        .await?;
        Ok((row.get(0), row.get(1)))
    }
}

#[async_trait]
impl KeyBackupStore for PostgresStore {
    async fn create_backup_version(
        &self,
        user_id: &UserId,
        algorithm: &serde_json::Value,
    ) -> Result<String> {
        let row = sqlx::query(
            "INSERT INTO key_backup_versions (user_id, version, algorithm)
             VALUES ($1, (SELECT COALESCE(MAX(version), 0) + 1 FROM key_backup_versions WHERE user_id = $1), $2)
             RETURNING version",
        )
        .bind(user_id.as_str())
        .bind(algorithm.to_string())
        .fetch_one(&self.pool)
        .await?;
        Ok(row.get::<i64, _>(0).to_string())
    }

    async fn get_backup_version(
        &self,
        user_id: &UserId,
        version: Option<&str>,
    ) -> Result<Option<BackupVersion>> {
        match version {
            Some(v) => match parse_version(v) {
                Some(n) => self.backup_row(user_id, Some(n)).await,
                None => Ok(None),
            },
            None => self.backup_row(user_id, None).await,
        }
    }

    async fn update_backup_version(
        &self,
        user_id: &UserId,
        version: &str,
        algorithm: &serde_json::Value,
    ) -> Result<bool> {
        let Some(n) = parse_version(version) else {
            return Ok(false);
        };
        let res = sqlx::query(
            "UPDATE key_backup_versions SET algorithm = $3 WHERE user_id = $1 AND version = $2 AND NOT deleted",
        )
        .bind(user_id.as_str())
        .bind(n)
        .bind(algorithm.to_string())
        .execute(&self.pool)
        .await?;
        Ok(res.rows_affected() > 0)
    }

    async fn delete_backup_version(&self, user_id: &UserId, version: &str) -> Result<bool> {
        let Some(n) = parse_version(version) else {
            return Ok(false);
        };
        let res = sqlx::query(
            "UPDATE key_backup_versions SET deleted = TRUE WHERE user_id = $1 AND version = $2 AND NOT deleted",
        )
        .bind(user_id.as_str())
        .bind(n)
        .execute(&self.pool)
        .await?;
        sqlx::query("DELETE FROM key_backup_keys WHERE user_id = $1 AND version = $2")
            .bind(user_id.as_str())
            .bind(n)
            .execute(&self.pool)
            .await?;
        Ok(res.rows_affected() > 0)
    }

    async fn put_backup_keys(
        &self,
        user_id: &UserId,
        version: &str,
        keys: &[BackupKey],
    ) -> Result<(i64, i64)> {
        let n = parse_version(version).ok_or_else(|| Error::NotFound("backup version"))?;
        let mut tx = self.pool.begin().await?;
        for key in keys {
            let existing = sqlx::query(
                "SELECT first_message_index, forwarded_count, is_verified, session_data
                   FROM key_backup_keys
                  WHERE user_id = $1 AND version = $2 AND room_id = $3 AND session_id = $4 FOR UPDATE",
            )
            .bind(user_id.as_str())
            .bind(n)
            .bind(key.room_id.as_str())
            .bind(&key.session_id)
            .fetch_optional(&mut *tx)
            .await?;
            if let Some(row) = existing {
                let current = BackupKey {
                    room_id: key.room_id.clone(),
                    session_id: key.session_id.clone(),
                    first_message_index: row.get("first_message_index"),
                    forwarded_count: row.get("forwarded_count"),
                    is_verified: row.get("is_verified"),
                    data: serde_json::Value::Null,
                };
                if !current.should_be_replaced_by(key) {
                    continue;
                }
            }
            sqlx::query(
                "INSERT INTO key_backup_keys (user_id, version, room_id, session_id, first_message_index, forwarded_count, is_verified, session_data)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                 ON CONFLICT (user_id, version, room_id, session_id) DO UPDATE SET
                    first_message_index = EXCLUDED.first_message_index,
                    forwarded_count = EXCLUDED.forwarded_count,
                    is_verified = EXCLUDED.is_verified,
                    session_data = EXCLUDED.session_data",
            )
            .bind(user_id.as_str())
            .bind(n)
            .bind(key.room_id.as_str())
            .bind(&key.session_id)
            .bind(key.first_message_index)
            .bind(key.forwarded_count)
            .bind(key.is_verified)
            .bind(key.data.to_string())
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        self.bump_etag(user_id, n).await
    }

    async fn get_backup_keys(
        &self,
        user_id: &UserId,
        version: &str,
        room_id: Option<&RoomId>,
        session_id: Option<&str>,
    ) -> Result<Vec<BackupKey>> {
        let Some(n) = parse_version(version) else {
            return Ok(Vec::new());
        };
        let rows = sqlx::query(
            "SELECT room_id, session_id, first_message_index, forwarded_count, is_verified, session_data
               FROM key_backup_keys
              WHERE user_id = $1 AND version = $2
                AND ($3::text IS NULL OR room_id = $3) AND ($4::text IS NULL OR session_id = $4)
              ORDER BY room_id, session_id",
        )
        .bind(user_id.as_str())
        .bind(n)
        .bind(room_id.map(RoomId::as_str))
        .bind(session_id)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|r| {
                let data: String = r.get("session_data");
                Ok(BackupKey {
                    room_id: OwnedRoomId::try_from(r.get::<String, _>("room_id"))
                        .map_err(|e| Error::Corrupt(format!("room id: {e}")))?,
                    session_id: r.get("session_id"),
                    first_message_index: r.get("first_message_index"),
                    forwarded_count: r.get("forwarded_count"),
                    is_verified: r.get("is_verified"),
                    data: serde_json::from_str(&data)
                        .map_err(|e| Error::Corrupt(format!("backup key: {e}")))?,
                })
            })
            .collect()
    }

    async fn delete_backup_keys(
        &self,
        user_id: &UserId,
        version: &str,
        room_id: Option<&RoomId>,
        session_id: Option<&str>,
    ) -> Result<(i64, i64)> {
        let n = parse_version(version).ok_or_else(|| Error::NotFound("backup version"))?;
        sqlx::query(
            "DELETE FROM key_backup_keys WHERE user_id = $1 AND version = $2
                AND ($3::text IS NULL OR room_id = $3) AND ($4::text IS NULL OR session_id = $4)",
        )
        .bind(user_id.as_str())
        .bind(n)
        .bind(room_id.map(RoomId::as_str))
        .bind(session_id)
        .execute(&self.pool)
        .await?;
        self.bump_etag(user_id, n).await
    }
}
