use async_trait::async_trait;
use ruma::{DeviceId, OwnedDeviceId, OwnedUserId, UserId};
use sqlx::Row;

use super::PostgresStore;
use crate::SyncStream as Stream;
use crate::{E2eeStore, Error, OneTimeKeyRow, Pusher, Result, ToDeviceMessage, now_ts, parse_ids};

#[async_trait]
impl E2eeStore for PostgresStore {
    async fn set_device_keys(
        &self,
        user_id: &UserId,
        device_id: &DeviceId,
        keys: &serde_json::Value,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO device_keys (user_id, device_id, key_json, updated_ts) VALUES ($1, $2, $3, $4)
             ON CONFLICT (user_id, device_id) DO UPDATE SET key_json = EXCLUDED.key_json, updated_ts = EXCLUDED.updated_ts",
        )
        .bind(user_id.as_str())
        .bind(device_id.as_str())
        .bind(keys)
        .bind(now_ts())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn get_device_keys(
        &self,
        user_id: &UserId,
        device_id: &DeviceId,
    ) -> Result<Option<serde_json::Value>> {
        let row =
            sqlx::query("SELECT key_json FROM device_keys WHERE user_id = $1 AND device_id = $2")
                .bind(user_id.as_str())
                .bind(device_id.as_str())
                .fetch_optional(&self.pool)
                .await?;
        Ok(row.map(|r| r.get::<serde_json::Value, _>(0)))
    }

    async fn all_device_keys(
        &self,
        user_id: &UserId,
    ) -> Result<Vec<(OwnedDeviceId, serde_json::Value)>> {
        let rows = sqlx::query(
            "SELECT device_id, key_json FROM device_keys WHERE user_id = $1 ORDER BY device_id",
        )
        .bind(user_id.as_str())
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|r| {
                (
                    OwnedDeviceId::from(r.get::<String, _>(0)),
                    r.get::<serde_json::Value, _>(1),
                )
            })
            .collect())
    }

    async fn delete_device_keys(&self, user_id: &UserId, device_id: &DeviceId) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        for table in ["device_keys", "one_time_keys", "fallback_keys"] {
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "DELETE FROM {table} WHERE user_id = $1 AND device_id = $2"
            )))
            .bind(user_id.as_str())
            .bind(device_id.as_str())
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn add_one_time_keys(
        &self,
        user_id: &UserId,
        device_id: &DeviceId,
        keys: &[OneTimeKeyRow],
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        for k in keys {
            sqlx::query(
                "INSERT INTO one_time_keys (user_id, device_id, algorithm, key_id, key_json, created_ts)
                 VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT DO NOTHING",
            )
            .bind(user_id.as_str())
            .bind(device_id.as_str())
            .bind(&k.algorithm)
            .bind(&k.key_id)
            .bind(&k.key)
            .bind(now_ts())
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn count_one_time_keys(
        &self,
        user_id: &UserId,
        device_id: &DeviceId,
    ) -> Result<Vec<(String, i64)>> {
        let rows = sqlx::query(
            "SELECT algorithm, count(*) FROM one_time_keys WHERE user_id = $1 AND device_id = $2 GROUP BY algorithm",
        )
        .bind(user_id.as_str())
        .bind(device_id.as_str())
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|r| (r.get::<String, _>(0), r.get::<i64, _>(1)))
            .collect())
    }

    async fn claim_one_time_key(
        &self,
        user_id: &UserId,
        device_id: &DeviceId,
        algorithm: &str,
    ) -> Result<Option<OneTimeKeyRow>> {
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query(
            "DELETE FROM one_time_keys WHERE (user_id, device_id, algorithm, key_id) = (
                SELECT user_id, device_id, algorithm, key_id FROM one_time_keys
                WHERE user_id = $1 AND device_id = $2 AND algorithm = $3
                ORDER BY created_ts, key_id LIMIT 1 FOR UPDATE SKIP LOCKED)
             RETURNING key_id, key_json",
        )
        .bind(user_id.as_str())
        .bind(device_id.as_str())
        .bind(algorithm)
        .fetch_optional(&mut *tx)
        .await?;
        let out = if let Some(r) = row {
            Some(OneTimeKeyRow {
                algorithm: algorithm.to_owned(),
                key_id: r.get(0),
                key: r.get(1),
            })
        } else {
            let fb = sqlx::query(
                "UPDATE fallback_keys SET used = TRUE WHERE user_id = $1 AND device_id = $2 AND algorithm = $3
                 RETURNING key_id, key_json",
            )
            .bind(user_id.as_str())
            .bind(device_id.as_str())
            .bind(algorithm)
            .fetch_optional(&mut *tx)
            .await?;
            fb.map(|r| OneTimeKeyRow {
                algorithm: algorithm.to_owned(),
                key_id: r.get(0),
                key: r.get(1),
            })
        };
        tx.commit().await?;
        Ok(out)
    }

    async fn set_fallback_keys(
        &self,
        user_id: &UserId,
        device_id: &DeviceId,
        keys: &[OneTimeKeyRow],
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        for k in keys {
            sqlx::query(
                "INSERT INTO fallback_keys (user_id, device_id, algorithm, key_id, key_json, used)
                 VALUES ($1, $2, $3, $4, $5, FALSE)
                 ON CONFLICT (user_id, device_id, algorithm) DO UPDATE SET key_id = EXCLUDED.key_id,
                    key_json = EXCLUDED.key_json, used = FALSE",
            )
            .bind(user_id.as_str())
            .bind(device_id.as_str())
            .bind(&k.algorithm)
            .bind(&k.key_id)
            .bind(&k.key)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn unused_fallback_algorithms(
        &self,
        user_id: &UserId,
        device_id: &DeviceId,
    ) -> Result<Vec<String>> {
        let rows = sqlx::query("SELECT algorithm FROM fallback_keys WHERE user_id = $1 AND device_id = $2 AND NOT used")
            .bind(user_id.as_str())
            .bind(device_id.as_str())
            .fetch_all(&self.pool)
            .await?;
        Ok(rows.into_iter().map(|r| r.get::<String, _>(0)).collect())
    }

    async fn set_cross_signing_key(
        &self,
        user_id: &UserId,
        key_type: &str,
        key: &serde_json::Value,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO cross_signing_keys (user_id, key_type, key_json, updated_ts) VALUES ($1, $2, $3, $4)
             ON CONFLICT (user_id, key_type) DO UPDATE SET key_json = EXCLUDED.key_json, updated_ts = EXCLUDED.updated_ts",
        )
        .bind(user_id.as_str())
        .bind(key_type)
        .bind(key)
        .bind(now_ts())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn get_cross_signing_key(
        &self,
        user_id: &UserId,
        key_type: &str,
    ) -> Result<Option<serde_json::Value>> {
        let row = sqlx::query(
            "SELECT key_json FROM cross_signing_keys WHERE user_id = $1 AND key_type = $2",
        )
        .bind(user_id.as_str())
        .bind(key_type)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| r.get::<serde_json::Value, _>(0)))
    }

    async fn add_key_signature(
        &self,
        target_user: &UserId,
        target_key: &str,
        signing_user: &UserId,
        signing_key: &str,
        signature: &str,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO key_signatures (target_user_id, target_key_id, signing_user_id, signing_key_id, signature)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (target_user_id, target_key_id, signing_user_id, signing_key_id) DO UPDATE SET signature = EXCLUDED.signature",
        )
        .bind(target_user.as_str())
        .bind(target_key)
        .bind(signing_user.as_str())
        .bind(signing_key)
        .bind(signature)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn key_signatures(
        &self,
        target_user: &UserId,
        target_key: &str,
    ) -> Result<Vec<(OwnedUserId, String, String)>> {
        let rows = sqlx::query(
            "SELECT signing_user_id, signing_key_id, signature FROM key_signatures WHERE target_user_id = $1 AND target_key_id = $2",
        )
        .bind(target_user.as_str())
        .bind(target_key)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|r| {
                Ok((
                    OwnedUserId::try_from(r.get::<String, _>(0))
                        .map_err(|e| Error::Corrupt(e.to_string()))?,
                    r.get::<String, _>(1),
                    r.get::<String, _>(2),
                ))
            })
            .collect()
    }

    async fn add_device_list_change(
        &self,
        user_id: &UserId,
        device_id: Option<&DeviceId>,
    ) -> Result<i64> {
        let (user, device) = (user_id.to_string(), device_id.map(ToString::to_string));
        let position = self
            .write_to_stream(Stream::DeviceLists, move |conn| {
                Box::pin(async move {
                    let row = sqlx::query(
                        "INSERT INTO device_list_changes (stream_id, user_id, device_id, changed_ts)
                         VALUES (nextval('device_lists_seq'), $1, $2, $3) RETURNING stream_id",
                    )
                    .bind(user)
                    .bind(device)
                    .bind(now_ts())
                    .fetch_one(conn)
                    .await?;
                    Ok(Some(row.get::<i64, _>(0)))
                })
            })
            .await?;
        Ok(position.unwrap_or_default())
    }

    async fn device_list_changes_since(&self, since: i64, to: i64) -> Result<Vec<OwnedUserId>> {
        let rows = sqlx::query("SELECT DISTINCT user_id FROM device_list_changes WHERE stream_id > $1 AND stream_id <= $2")
            .bind(since)
            .bind(to)
            .fetch_all(&self.pool)
            .await?;
        Ok(parse_ids(
            rows.into_iter().map(|r| r.get::<String, _>(0)).collect(),
        ))
    }

    async fn max_device_list_stream_id(&self) -> Result<i64> {
        let row = sqlx::query("SELECT COALESCE(MAX(stream_id), 0) FROM device_list_changes")
            .fetch_one(&self.pool)
            .await?;
        Ok(row.get::<i64, _>(0))
    }

    async fn previous_device_change(&self, user_id: &UserId, before: i64) -> Result<Option<i64>> {
        let row = sqlx::query(
            "SELECT MAX(stream_id) FROM device_list_changes
             WHERE user_id = $1 AND device_id IS NOT NULL AND stream_id < $2",
        )
        .bind(user_id.as_str())
        .bind(before)
        .fetch_one(&self.pool)
        .await?;
        Ok(row.get::<Option<i64>, _>(0))
    }

    async fn device_stream_ids(&self, user_id: &UserId) -> Result<Vec<(OwnedDeviceId, i64)>> {
        // COALESCE evaluates nextval only for a device without a change. The number is not
        // written anywhere: it only has to differ from every other ID of this user's.
        let rows = sqlx::query(
            "SELECT d.device_id,
                    COALESCE((SELECT MAX(c.stream_id) FROM device_list_changes c
                              WHERE c.user_id = d.user_id AND c.device_id = d.device_id),
                             nextval('device_lists_seq'))
             FROM devices d WHERE d.user_id = $1 ORDER BY d.device_id",
        )
        .bind(user_id.as_str())
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|r| {
                (
                    OwnedDeviceId::from(r.get::<String, _>(0)),
                    r.get::<i64, _>(1),
                )
            })
            .collect())
    }

    async fn owner_signed_devices(&self) -> Result<Vec<(OwnedUserId, OwnedDeviceId)>> {
        let rows = sqlx::query(
            "SELECT DISTINCT s.target_user_id, s.target_key_id
             FROM key_signatures s
             JOIN device_keys k ON k.user_id = s.target_user_id AND k.device_id = s.target_key_id
             WHERE s.signing_user_id = s.target_user_id
             ORDER BY 1, 2",
        )
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|r| {
                Ok((
                    OwnedUserId::try_from(r.get::<String, _>(0))
                        .map_err(|e| Error::Corrupt(e.to_string()))?,
                    OwnedDeviceId::from(r.get::<String, _>(1)),
                ))
            })
            .collect()
    }

    async fn add_to_device_message(
        &self,
        user_id: &UserId,
        device_id: &DeviceId,
        sender: &UserId,
        event_type: &str,
        content: &serde_json::Value,
    ) -> Result<i64> {
        let (user, device, sender) = (
            user_id.to_string(),
            device_id.to_string(),
            sender.to_string(),
        );
        let (event_type, content) = (event_type.to_owned(), content.clone());
        let position = self
            .write_to_stream(Stream::ToDevice, move |conn| {
                Box::pin(async move {
                    let row = sqlx::query(
                        "INSERT INTO to_device_messages (stream_id, user_id, device_id, sender, event_type, content, created_ts)
                         VALUES (nextval('to_device_seq'), $1, $2, $3, $4, $5, $6) RETURNING stream_id",
                    )
                    .bind(user)
                    .bind(device)
                    .bind(sender)
                    .bind(event_type)
                    .bind(content)
                    .bind(now_ts())
                    .fetch_one(conn)
                    .await?;
                    Ok(Some(row.get::<i64, _>(0)))
                })
            })
            .await?;
        Ok(position.unwrap_or_default())
    }

    async fn to_device_messages(
        &self,
        user_id: &UserId,
        device_id: &DeviceId,
        since: i64,
        to: i64,
        limit: usize,
    ) -> Result<Vec<ToDeviceMessage>> {
        let rows = sqlx::query(
            "SELECT stream_id, sender, event_type, content FROM to_device_messages
             WHERE user_id = $1 AND device_id = $2 AND stream_id > $3 AND stream_id <= $4 ORDER BY stream_id LIMIT $5",
        )
        .bind(user_id.as_str())
        .bind(device_id.as_str())
        .bind(since)
        .bind(to)
        .bind(i64::try_from(limit).unwrap_or(i64::MAX))
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|r| {
                Ok(ToDeviceMessage {
                    stream_id: r.get(0),
                    sender: OwnedUserId::try_from(r.get::<String, _>(1))
                        .map_err(|e| Error::Corrupt(e.to_string()))?,
                    event_type: r.get(2),
                    content: r.get(3),
                })
            })
            .collect()
    }

    async fn delete_to_device_messages_up_to(
        &self,
        user_id: &UserId,
        device_id: &DeviceId,
        up_to: i64,
    ) -> Result<()> {
        sqlx::query("DELETE FROM to_device_messages WHERE user_id = $1 AND device_id = $2 AND stream_id <= $3")
            .bind(user_id.as_str())
            .bind(device_id.as_str())
            .bind(up_to)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn max_to_device_stream_id(&self) -> Result<i64> {
        let row = sqlx::query("SELECT COALESCE(MAX(stream_id), 0) FROM to_device_messages")
            .fetch_one(&self.pool)
            .await?;
        Ok(row.get::<i64, _>(0))
    }

    async fn get_push_rules(&self, user_id: &UserId) -> Result<Option<(serde_json::Value, i64)>> {
        let row = sqlx::query("SELECT ruleset, stream_id FROM push_rules WHERE user_id = $1")
            .bind(user_id.as_str())
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(|r| (r.get::<serde_json::Value, _>(0), r.get::<i64, _>(1))))
    }

    async fn set_push_rules(&self, user_id: &UserId, ruleset: &serde_json::Value) -> Result<i64> {
        let (user, ruleset) = (user_id.to_string(), ruleset.clone());
        let position = self
            .write_to_stream(Stream::AccountData, move |conn| {
                Box::pin(async move {
                    let row = sqlx::query(
                        "INSERT INTO push_rules (user_id, ruleset, stream_id) VALUES ($1, $2, nextval('account_data_seq'))
                         ON CONFLICT (user_id) DO UPDATE SET ruleset = EXCLUDED.ruleset, stream_id = EXCLUDED.stream_id
                         RETURNING stream_id",
                    )
                    .bind(user)
                    .bind(ruleset)
                    .fetch_one(conn)
                    .await?;
                    Ok(Some(row.get::<i64, _>(0)))
                })
            })
            .await?;
        Ok(position.unwrap_or_default())
    }

    async fn set_pusher(&self, p: Pusher) -> Result<()> {
        sqlx::query(
            "INSERT INTO pushers (user_id, app_id, pushkey, kind, app_display_name, device_display_name, profile_tag, lang, data, created_ts, access_token)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
             ON CONFLICT (user_id, app_id, pushkey) DO UPDATE SET kind = EXCLUDED.kind, app_display_name = EXCLUDED.app_display_name,
                device_display_name = EXCLUDED.device_display_name, profile_tag = EXCLUDED.profile_tag, lang = EXCLUDED.lang,
                data = EXCLUDED.data, access_token = EXCLUDED.access_token",
        )
        .bind(p.user_id.as_str())
        .bind(&p.app_id)
        .bind(&p.pushkey)
        .bind(&p.kind)
        .bind(&p.app_display_name)
        .bind(&p.device_display_name)
        .bind(&p.profile_tag)
        .bind(&p.lang)
        .bind(&p.data)
        .bind(now_ts())
        .bind(&p.access_token)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn delete_pushers_except_token(
        &self,
        user_id: &UserId,
        keep_token: Option<&str>,
    ) -> Result<()> {
        sqlx::query("DELETE FROM pushers WHERE user_id = $1 AND ($2::text IS NULL OR access_token IS DISTINCT FROM $2)")
            .bind(user_id.as_str())
            .bind(keep_token)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn delete_pusher(&self, user_id: &UserId, app_id: &str, pushkey: &str) -> Result<()> {
        sqlx::query("DELETE FROM pushers WHERE user_id = $1 AND app_id = $2 AND pushkey = $3")
            .bind(user_id.as_str())
            .bind(app_id)
            .bind(pushkey)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn users_with_pushers(&self) -> Result<Vec<OwnedUserId>> {
        let rows = sqlx::query("SELECT DISTINCT user_id FROM pushers WHERE kind = 'http'")
            .fetch_all(&self.pool)
            .await?;
        Ok(rows
            .into_iter()
            .filter_map(|r| OwnedUserId::try_from(r.get::<String, _>(0)).ok())
            .collect())
    }

    async fn pushers(&self, user_id: &UserId) -> Result<Vec<Pusher>> {
        let rows = sqlx::query(
            "SELECT user_id, app_id, pushkey, kind, app_display_name, device_display_name, profile_tag, lang, data, access_token FROM pushers WHERE user_id = $1",
        )
        .bind(user_id.as_str())
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|r| {
                Ok(Pusher {
                    user_id: OwnedUserId::try_from(r.get::<String, _>("user_id"))
                        .map_err(|e| Error::Corrupt(e.to_string()))?,
                    app_id: r.get("app_id"),
                    pushkey: r.get("pushkey"),
                    kind: r.get("kind"),
                    app_display_name: r.get("app_display_name"),
                    device_display_name: r.get("device_display_name"),
                    profile_tag: r.get("profile_tag"),
                    lang: r.get("lang"),
                    data: r.get("data"),
                    access_token: r.get("access_token"),
                })
            })
            .collect()
    }
}
