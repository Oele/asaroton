//! Sign-in through OAuth 2.0: the OAuth client and scope of a device, and the embedded
//! authorization server's key-value state (`lithostroton_store`).

use async_trait::async_trait;
use ruma::{DeviceId, UserId};
use sqlx::Row;

use super::PostgresStore;
use crate::{OauthStore, Result};

/// The condition for an entry that counts: not expired.
const LIVE: &str = "(expires_at IS NULL OR expires_at > now())";

#[async_trait]
impl OauthStore for PostgresStore {
    async fn set_device_oauth(
        &self,
        user_id: &UserId,
        device_id: &DeviceId,
        client_id: Option<&str>,
        scope: Option<&str>,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE devices SET oauth_client_id = $3, oauth_scope = $4
              WHERE user_id = $1 AND device_id = $2",
        )
        .bind(user_id.as_str())
        .bind(device_id.as_str())
        .bind(client_id)
        .bind(scope)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn device_oauth(
        &self,
        user_id: &UserId,
        device_id: &DeviceId,
    ) -> Result<Option<(Option<String>, Option<String>)>> {
        let row = sqlx::query(
            "SELECT oauth_client_id, oauth_scope FROM devices
              WHERE user_id = $1 AND device_id = $2",
        )
        .bind(user_id.as_str())
        .bind(device_id.as_str())
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| (r.get(0), r.get(1))))
    }

    async fn delete_refresh_tokens_for_device(
        &self,
        user_id: &UserId,
        device_id: &DeviceId,
    ) -> Result<()> {
        sqlx::query("DELETE FROM refresh_tokens WHERE user_id = $1 AND device_id = $2")
            .bind(user_id.as_str())
            .bind(device_id.as_str())
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn oauth_kv_insert(
        &self,
        kind: &str,
        key: &str,
        value: &[u8],
        expires_at: Option<f64>,
    ) -> Result<bool> {
        // An expired entry under the key does not count: it is overwritten.
        let row = sqlx::query(
            "INSERT INTO lithostroton_store (kind, key, value, expires_at)
             VALUES ($1, $2, $3, to_timestamp($4))
             ON CONFLICT (kind, key) DO UPDATE
                SET value = EXCLUDED.value, expires_at = EXCLUDED.expires_at
              WHERE lithostroton_store.expires_at IS NOT NULL
                AND lithostroton_store.expires_at <= now()
             RETURNING 1",
        )
        .bind(kind)
        .bind(key)
        .bind(value)
        .bind(expires_at)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.is_some())
    }

    async fn oauth_kv_get(&self, kind: &str, key: &str) -> Result<Option<Vec<u8>>> {
        let row = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT value FROM lithostroton_store WHERE kind = $1 AND key = $2 AND {LIVE}"
        )))
        .bind(kind)
        .bind(key)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| r.get(0)))
    }

    async fn oauth_kv_replace(
        &self,
        kind: &str,
        key: &str,
        expected: &[u8],
        value: &[u8],
        expires_at: Option<f64>,
    ) -> Result<bool> {
        let row = sqlx::query(sqlx::AssertSqlSafe(format!(
            "UPDATE lithostroton_store SET value = $4, expires_at = to_timestamp($5)
              WHERE kind = $1 AND key = $2 AND value = $3 AND {LIVE}
             RETURNING 1"
        )))
        .bind(kind)
        .bind(key)
        .bind(expected)
        .bind(value)
        .bind(expires_at)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.is_some())
    }

    async fn oauth_kv_take(&self, kind: &str, key: &str) -> Result<Option<Vec<u8>>> {
        let row = sqlx::query(sqlx::AssertSqlSafe(format!(
            "DELETE FROM lithostroton_store WHERE kind = $1 AND key = $2 AND {LIVE}
             RETURNING value"
        )))
        .bind(kind)
        .bind(key)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| r.get(0)))
    }

    async fn oauth_kv_delete(&self, kind: &str, key: &str) -> Result<()> {
        sqlx::query("DELETE FROM lithostroton_store WHERE kind = $1 AND key = $2")
            .bind(kind)
            .bind(key)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn oauth_kv_purge_expired(&self) -> Result<()> {
        sqlx::query("DELETE FROM lithostroton_store WHERE expires_at <= now()")
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}
