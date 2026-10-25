use async_trait::async_trait;
use ruma::{OwnedServerName, OwnedServerSigningKeyId, ServerName};
use sqlx::Row;

use super::PostgresStore;
use crate::{Error, KeyStore, LocalSigningKey, RemoteVerifyKey, Result, Ts};

const REMOTE_COLUMNS: &str = "server_name, key_id, verify_key_b64, valid_until_ts, expired_ts, fetched_ts, fetched_from, key_json";

fn remote_from_row(r: &sqlx::postgres::PgRow) -> Result<RemoteVerifyKey> {
    let corrupt = |e: ruma::IdParseError| Error::Corrupt(e.to_string());
    Ok(RemoteVerifyKey {
        server_name: OwnedServerName::try_from(r.get::<String, _>("server_name"))
            .map_err(corrupt)?,
        key_id: OwnedServerSigningKeyId::try_from(r.get::<String, _>("key_id")).map_err(corrupt)?,
        verify_key_b64: r.get("verify_key_b64"),
        valid_until_ts: r.get("valid_until_ts"),
        expired_ts: r.get("expired_ts"),
        fetched_ts: r.get("fetched_ts"),
        fetched_from: OwnedServerName::try_from(r.get::<String, _>("fetched_from"))
            .map_err(corrupt)?,
        key_json: r.get("key_json"),
    })
}

#[async_trait]
impl KeyStore for PostgresStore {
    async fn local_signing_keys(&self) -> Result<Vec<LocalSigningKey>> {
        let rows = sqlx::query("SELECT key_id, seed_b64, created_ts, expired_ts FROM server_signing_keys ORDER BY created_ts")
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter()
            .map(|r| {
                Ok(LocalSigningKey {
                    key_id: OwnedServerSigningKeyId::try_from(r.get::<String, _>("key_id"))
                        .map_err(|e| Error::Corrupt(e.to_string()))?,
                    seed_b64: r.get("seed_b64"),
                    created_ts: r.get("created_ts"),
                    expired_ts: r.get("expired_ts"),
                })
            })
            .collect()
    }

    async fn add_local_signing_key(&self, key: LocalSigningKey) -> Result<()> {
        sqlx::query(
            "INSERT INTO server_signing_keys (key_id, seed_b64, created_ts, expired_ts) VALUES ($1, $2, $3, $4)
             ON CONFLICT (key_id) DO NOTHING",
        )
        .bind(key.key_id.as_str())
        .bind(&key.seed_b64)
        .bind(key.created_ts)
        .bind(key.expired_ts)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn expire_local_signing_key(&self, key_id: &str, expired_ts: Ts) -> Result<()> {
        sqlx::query("UPDATE server_signing_keys SET expired_ts = $2 WHERE key_id = $1")
            .bind(key_id)
            .bind(expired_ts)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn remote_verify_keys(&self, server_name: &ServerName) -> Result<Vec<RemoteVerifyKey>> {
        let rows = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT {REMOTE_COLUMNS} FROM remote_server_keys WHERE server_name = $1"
        )))
        .bind(server_name.as_str())
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(remote_from_row).collect()
    }

    async fn remote_verify_keys_for(
        &self,
        server_name: &ServerName,
        key_ids: &[String],
    ) -> Result<Vec<RemoteVerifyKey>> {
        let rows = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT {REMOTE_COLUMNS} FROM remote_server_keys WHERE server_name = $1 AND key_id = ANY($2)"
        )))
        .bind(server_name.as_str())
        .bind(key_ids)
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(remote_from_row).collect()
    }

    async fn store_remote_verify_keys(&self, keys: Vec<RemoteVerifyKey>) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        for k in keys {
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "INSERT INTO remote_server_keys ({REMOTE_COLUMNS}) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                 ON CONFLICT (server_name, key_id) DO UPDATE SET verify_key_b64 = EXCLUDED.verify_key_b64,
                    valid_until_ts = GREATEST(remote_server_keys.valid_until_ts, EXCLUDED.valid_until_ts),
                    expired_ts = EXCLUDED.expired_ts, fetched_ts = EXCLUDED.fetched_ts,
                    fetched_from = EXCLUDED.fetched_from, key_json = EXCLUDED.key_json"
            )))
            .bind(k.server_name.as_str())
            .bind(k.key_id.as_str())
            .bind(&k.verify_key_b64)
            .bind(k.valid_until_ts)
            .bind(k.expired_ts)
            .bind(k.fetched_ts)
            .bind(k.fetched_from.as_str())
            .bind(&k.key_json)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }
}
