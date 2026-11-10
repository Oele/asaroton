use async_trait::async_trait;
use ruma::{DeviceId, OwnedDeviceId, OwnedUserId, UserId};
use sqlx::Row;

use super::PostgresStore;
use crate::SyncStream as Stream;
use crate::{
    AccessToken, Device, Error, NewUser, RefreshToken, Result, User, UserListQuery, UserStore,
    now_ts,
};

#[derive(sqlx::FromRow)]
struct UserRow {
    user_id: String,
    display_name: Option<String>,
    avatar_url: Option<String>,
    is_admin: bool,
    is_guest: bool,
    is_deactivated: bool,
    appservice_id: Option<String>,
    created_ts: i64,
    profile_fields: serde_json::Value,
}

impl TryFrom<UserRow> for User {
    type Error = Error;
    fn try_from(r: UserRow) -> Result<Self> {
        Ok(User {
            user_id: OwnedUserId::try_from(r.user_id).map_err(|e| Error::Corrupt(e.to_string()))?,
            display_name: r.display_name,
            avatar_url: r.avatar_url,
            is_admin: r.is_admin,
            is_guest: r.is_guest,
            is_deactivated: r.is_deactivated,
            appservice_id: r.appservice_id,
            created_ts: r.created_ts,
            profile_fields: match r.profile_fields {
                serde_json::Value::Object(m) => m,
                _ => serde_json::Map::new(),
            },
        })
    }
}

#[derive(sqlx::FromRow)]
struct DeviceRow {
    user_id: String,
    device_id: String,
    display_name: Option<String>,
    created_ts: i64,
    last_seen_ts: Option<i64>,
    last_seen_ip: Option<String>,
    last_seen_user_agent: Option<String>,
}

impl TryFrom<DeviceRow> for Device {
    type Error = Error;
    fn try_from(r: DeviceRow) -> Result<Self> {
        Ok(Device {
            user_id: OwnedUserId::try_from(r.user_id).map_err(|e| Error::Corrupt(e.to_string()))?,
            device_id: OwnedDeviceId::from(r.device_id),
            display_name: r.display_name,
            created_ts: r.created_ts,
            last_seen_ts: r.last_seen_ts,
            last_seen_ip: r.last_seen_ip,
            last_seen_user_agent: r.last_seen_user_agent,
        })
    }
}

#[derive(sqlx::FromRow)]
struct TokenRow {
    token: String,
    user_id: String,
    device_id: Option<String>,
    created_ts: i64,
    expires_ts: Option<i64>,
    refresh_token: Option<String>,
    puppets_user_id: Option<String>,
}

impl TryFrom<TokenRow> for AccessToken {
    type Error = Error;
    fn try_from(r: TokenRow) -> Result<Self> {
        Ok(AccessToken {
            token: r.token,
            user_id: OwnedUserId::try_from(r.user_id).map_err(|e| Error::Corrupt(e.to_string()))?,
            device_id: r.device_id.map(OwnedDeviceId::from),
            created_ts: r.created_ts,
            expires_ts: r.expires_ts,
            refresh_token: r.refresh_token,
            puppets_user_id: r
                .puppets_user_id
                .map(OwnedUserId::try_from)
                .transpose()
                .map_err(|e| Error::Corrupt(e.to_string()))?,
        })
    }
}

const USER_COLUMNS: &str = "user_id, display_name, avatar_url, is_admin, is_guest, is_deactivated, appservice_id, created_ts, profile_fields";
const DEVICE_COLUMNS: &str = "user_id, device_id, display_name, created_ts, last_seen_ts, last_seen_ip, last_seen_user_agent";
const TOKEN_COLUMNS: &str =
    "token, user_id, device_id, created_ts, expires_ts, refresh_token, puppets_user_id";

#[async_trait]
impl UserStore for PostgresStore {
    async fn create_user(&self, user: NewUser) -> Result<User> {
        let now = now_ts();
        let res = sqlx::query(
            "INSERT INTO users (user_id, password_hash, display_name, is_admin, is_guest, appservice_id, created_ts)
             VALUES ($1, $2, $3, $4, $5, $6, $7)",
        )
        .bind(user.user_id.as_str())
        .bind(&user.password_hash)
        .bind(&user.display_name)
        .bind(user.is_admin)
        .bind(user.is_guest)
        .bind(&user.appservice_id)
        .bind(now)
        .execute(&self.pool)
        .await;
        match res {
            Ok(_) => {}
            Err(e) => {
                let err = Error::from(e);
                return Err(if err.is_unique_violation() {
                    Error::AlreadyExists("user")
                } else {
                    err
                });
            }
        }
        Ok(User {
            user_id: user.user_id,
            display_name: user.display_name,
            avatar_url: None,
            is_admin: user.is_admin,
            is_guest: user.is_guest,
            is_deactivated: false,
            appservice_id: user.appservice_id,
            created_ts: now,
            profile_fields: serde_json::Map::new(),
        })
    }

    async fn get_user(&self, user_id: &UserId) -> Result<Option<User>> {
        let row: Option<UserRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT {USER_COLUMNS} FROM users WHERE user_id = $1"
        )))
        .bind(user_id.as_str())
        .fetch_optional(&self.pool)
        .await?;
        row.map(User::try_from).transpose()
    }

    async fn list_users(&self, q: &UserListQuery) -> Result<(Vec<User>, i64)> {
        let pattern = q.name.as_ref().map(|n| format!("%{n}%"));
        let where_clause = "WHERE ($1::text IS NULL OR user_id ILIKE $1 OR display_name ILIKE $1)
              AND ($2 OR NOT is_guest) AND ($3 OR NOT is_deactivated) AND (NOT $4 OR is_admin)";
        let rows: Vec<UserRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT {USER_COLUMNS} FROM users {where_clause} ORDER BY user_id LIMIT $5 OFFSET $6"
        )))
        .bind(&pattern)
        .bind(q.include_guests)
        .bind(q.include_deactivated)
        .bind(q.admins_only)
        .bind(q.limit)
        .bind(q.offset)
        .fetch_all(&self.pool)
        .await?;
        let total = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT count(*) FROM users {where_clause}"
        )))
        .bind(&pattern)
        .bind(q.include_guests)
        .bind(q.include_deactivated)
        .bind(q.admins_only)
        .fetch_one(&self.pool)
        .await?
        .get::<i64, _>(0);
        Ok((
            rows.into_iter()
                .map(User::try_from)
                .collect::<Result<Vec<_>>>()?,
            total,
        ))
    }

    async fn user_exists(&self, user_id: &UserId) -> Result<bool> {
        let row = sqlx::query("SELECT 1 FROM users WHERE user_id = $1")
            .bind(user_id.as_str())
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.is_some())
    }

    async fn count_users(&self) -> Result<i64> {
        let row =
            sqlx::query("SELECT count(*) FROM users WHERE NOT is_guest AND appservice_id IS NULL")
                .fetch_one(&self.pool)
                .await?;
        Ok(row.get::<i64, _>(0))
    }

    async fn get_password_hash(&self, user_id: &UserId) -> Result<Option<String>> {
        let row = sqlx::query("SELECT password_hash FROM users WHERE user_id = $1")
            .bind(user_id.as_str())
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.and_then(|r| r.get::<Option<String>, _>(0)))
    }

    async fn set_password_hash(&self, user_id: &UserId, hash: Option<&str>) -> Result<()> {
        sqlx::query("UPDATE users SET password_hash = $2 WHERE user_id = $1")
            .bind(user_id.as_str())
            .bind(hash)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn set_profile_field(
        &self,
        user_id: &UserId,
        key: &str,
        value: Option<&serde_json::Value>,
    ) -> Result<()> {
        let res = match value {
            Some(v) => {
                sqlx::query(
                    "UPDATE users SET profile_fields = profile_fields || jsonb_build_object($2::text, $3::jsonb) WHERE user_id = $1",
                )
                .bind(user_id.as_str())
                .bind(key)
                .bind(v)
                .execute(&self.pool)
                .await?
            }
            None => {
                sqlx::query("UPDATE users SET profile_fields = profile_fields - $2::text WHERE user_id = $1")
                    .bind(user_id.as_str())
                    .bind(key)
                    .execute(&self.pool)
                    .await?
            }
        };
        if res.rows_affected() == 0 {
            return Err(Error::NotFound("user"));
        }
        Ok(())
    }

    async fn record_profile_update(&self, user_id: &UserId, field: &str) -> Result<i64> {
        let (user, field) = (user_id.to_string(), field.to_owned());
        let position = self
            .write_to_stream(Stream::ProfileUpdates, move |conn| {
                Box::pin(async move {
                    let row = sqlx::query(
                        "INSERT INTO profile_updates (user_id, field_id, stream_id)
                         VALUES ($1, $2, nextval('profile_updates_seq'))
                         ON CONFLICT (user_id, field_id) DO UPDATE SET stream_id = EXCLUDED.stream_id
                         RETURNING stream_id",
                    )
                    .bind(user)
                    .bind(field)
                    .fetch_one(conn)
                    .await?;
                    Ok(Some(row.get::<i64, _>(0)))
                })
            })
            .await?;
        Ok(position.unwrap_or_default())
    }

    async fn profile_updates_between(
        &self,
        since: i64,
        until: i64,
        fields: &[String],
    ) -> Result<Vec<(OwnedUserId, String)>> {
        let rows = sqlx::query(
            "SELECT user_id, field_id FROM profile_updates
             WHERE stream_id > $1 AND stream_id <= $2 AND field_id = ANY($3)
             ORDER BY stream_id",
        )
        .bind(since)
        .bind(until)
        .bind(fields)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|r| {
                let user = OwnedUserId::try_from(r.get::<String, _>("user_id"))
                    .map_err(|e| Error::Corrupt(format!("profile_updates.user_id: {e}")))?;
                Ok((user, r.get("field_id")))
            })
            .collect()
    }

    async fn set_profile(
        &self,
        user_id: &UserId,
        display_name: Option<Option<&str>>,
        avatar_url: Option<Option<&str>>,
    ) -> Result<()> {
        if let Some(name) = display_name {
            sqlx::query("UPDATE users SET display_name = $2 WHERE user_id = $1")
                .bind(user_id.as_str())
                .bind(name)
                .execute(&self.pool)
                .await?;
        }
        if let Some(url) = avatar_url {
            sqlx::query("UPDATE users SET avatar_url = $2 WHERE user_id = $1")
                .bind(user_id.as_str())
                .bind(url)
                .execute(&self.pool)
                .await?;
        }
        Ok(())
    }

    async fn set_deactivated(&self, user_id: &UserId, deactivated: bool) -> Result<()> {
        sqlx::query("UPDATE users SET is_deactivated = $2 WHERE user_id = $1")
            .bind(user_id.as_str())
            .bind(deactivated)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn set_admin(&self, user_id: &UserId, admin: bool) -> Result<()> {
        sqlx::query("UPDATE users SET is_admin = $2 WHERE user_id = $1")
            .bind(user_id.as_str())
            .bind(admin)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn create_device(
        &self,
        user_id: &UserId,
        device_id: &DeviceId,
        display_name: Option<&str>,
    ) -> Result<Device> {
        let now = now_ts();
        sqlx::query(
            "INSERT INTO devices (user_id, device_id, display_name, created_ts) VALUES ($1, $2, $3, $4)
             ON CONFLICT (user_id, device_id) DO UPDATE SET display_name = COALESCE(EXCLUDED.display_name, devices.display_name)",
        )
        .bind(user_id.as_str())
        .bind(device_id.as_str())
        .bind(display_name)
        .bind(now)
        .execute(&self.pool)
        .await?;
        self.get_device(user_id, device_id)
            .await?
            .ok_or(Error::NotFound("device"))
    }

    async fn get_device(&self, user_id: &UserId, device_id: &DeviceId) -> Result<Option<Device>> {
        let row: Option<DeviceRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT {DEVICE_COLUMNS} FROM devices WHERE user_id = $1 AND device_id = $2"
        )))
        .bind(user_id.as_str())
        .bind(device_id.as_str())
        .fetch_optional(&self.pool)
        .await?;
        row.map(Device::try_from).transpose()
    }

    async fn list_devices(&self, user_id: &UserId) -> Result<Vec<Device>> {
        let rows: Vec<DeviceRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT {DEVICE_COLUMNS} FROM devices WHERE user_id = $1 AND NOT hidden ORDER BY created_ts"
        )))
        .bind(user_id.as_str())
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(Device::try_from).collect()
    }

    async fn update_device(
        &self,
        user_id: &UserId,
        device_id: &DeviceId,
        display_name: Option<&str>,
    ) -> Result<()> {
        let res = sqlx::query(
            "UPDATE devices SET display_name = $3 WHERE user_id = $1 AND device_id = $2",
        )
        .bind(user_id.as_str())
        .bind(device_id.as_str())
        .bind(display_name)
        .execute(&self.pool)
        .await?;
        if res.rows_affected() == 0 {
            return Err(Error::NotFound("device"));
        }
        Ok(())
    }

    async fn touch_device(
        &self,
        user_id: &UserId,
        device_id: &DeviceId,
        ip: Option<&str>,
        user_agent: Option<&str>,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE devices SET last_seen_ts = $3, last_seen_ip = COALESCE($4, last_seen_ip),
                    last_seen_user_agent = COALESCE($5, last_seen_user_agent)
             WHERE user_id = $1 AND device_id = $2",
        )
        .bind(user_id.as_str())
        .bind(device_id.as_str())
        .bind(now_ts())
        .bind(ip)
        .bind(user_agent)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn delete_device(&self, user_id: &UserId, device_id: &DeviceId) -> Result<()> {
        let mut tx = self.begin().await?;
        sqlx::query("DELETE FROM access_tokens WHERE user_id = $1 AND device_id = $2")
            .bind(user_id.as_str())
            .bind(device_id.as_str())
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM refresh_tokens WHERE user_id = $1 AND device_id = $2")
            .bind(user_id.as_str())
            .bind(device_id.as_str())
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM devices WHERE user_id = $1 AND device_id = $2")
            .bind(user_id.as_str())
            .bind(device_id.as_str())
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn create_access_token(&self, token: AccessToken) -> Result<()> {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO access_tokens ({TOKEN_COLUMNS}) VALUES ($1, $2, $3, $4, $5, $6, $7)"
        )))
        .bind(&token.token)
        .bind(token.user_id.as_str())
        .bind(token.device_id.as_ref().map(|d| d.as_str()))
        .bind(token.created_ts)
        .bind(token.expires_ts)
        .bind(&token.refresh_token)
        .bind(token.puppets_user_id.as_ref().map(|u| u.as_str()))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn get_access_token(&self, token: &str) -> Result<Option<AccessToken>> {
        let row: Option<TokenRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT {TOKEN_COLUMNS} FROM access_tokens WHERE token = $1"
        )))
        .bind(token)
        .fetch_optional(&self.pool)
        .await?;
        row.map(AccessToken::try_from).transpose()
    }

    async fn delete_access_token(&self, token: &str) -> Result<()> {
        sqlx::query("DELETE FROM access_tokens WHERE token = $1")
            .bind(token)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn delete_access_tokens_for_user(
        &self,
        user_id: &UserId,
        except_device: Option<&DeviceId>,
    ) -> Result<()> {
        sqlx::query(
            "DELETE FROM access_tokens WHERE user_id = $1 AND ($2::text IS NULL OR device_id IS DISTINCT FROM $2)",
        )
        .bind(user_id.as_str())
        .bind(except_device.map(DeviceId::as_str))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn delete_access_tokens_for_device(
        &self,
        user_id: &UserId,
        device_id: &DeviceId,
    ) -> Result<()> {
        sqlx::query("DELETE FROM access_tokens WHERE user_id = $1 AND device_id = $2")
            .bind(user_id.as_str())
            .bind(device_id.as_str())
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn create_refresh_token(&self, token: RefreshToken) -> Result<()> {
        sqlx::query(
            "INSERT INTO refresh_tokens (token, user_id, device_id, created_ts, expires_ts, next_token)
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(&token.token)
        .bind(token.user_id.as_str())
        .bind(token.device_id.as_str())
        .bind(token.created_ts)
        .bind(token.expires_ts)
        .bind(&token.next_token)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn get_refresh_token(&self, token: &str) -> Result<Option<RefreshToken>> {
        let row = sqlx::query(
            "SELECT token, user_id, device_id, created_ts, expires_ts, next_token FROM refresh_tokens WHERE token = $1",
        )
        .bind(token)
        .fetch_optional(&self.pool)
        .await?;
        row.map(|r| {
            Ok(RefreshToken {
                token: r.get("token"),
                user_id: OwnedUserId::try_from(r.get::<String, _>("user_id"))
                    .map_err(|e| Error::Corrupt(e.to_string()))?,
                device_id: OwnedDeviceId::from(r.get::<String, _>("device_id")),
                created_ts: r.get("created_ts"),
                expires_ts: r.get("expires_ts"),
                next_token: r.get("next_token"),
            })
        })
        .transpose()
    }

    async fn access_token_for_refresh_token(
        &self,
        refresh_token: &str,
    ) -> Result<Option<AccessToken>> {
        let row: Option<TokenRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT {TOKEN_COLUMNS} FROM access_tokens
             WHERE refresh_token = $1 AND (expires_ts IS NULL OR expires_ts > $2)
             ORDER BY created_ts DESC LIMIT 1"
        )))
        .bind(refresh_token)
        .bind(now_ts())
        .fetch_optional(&self.pool)
        .await?;
        row.map(AccessToken::try_from).transpose()
    }

    async fn revoke_refresh_token_predecessor(&self, refresh_token: &str) -> Result<Vec<String>> {
        // One statement, so that the refresh token does not go without its access tokens.
        let rows = sqlx::query(
            "WITH replaced AS (DELETE FROM refresh_tokens WHERE next_token = $1 RETURNING token)
             DELETE FROM access_tokens WHERE refresh_token IN (SELECT token FROM replaced)
             RETURNING token",
        )
        .bind(refresh_token)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.iter().map(|r| r.get(0)).collect())
    }

    async fn set_refresh_token_next(&self, token: &str, next: &str) -> Result<()> {
        sqlx::query("UPDATE refresh_tokens SET next_token = $2 WHERE token = $1")
            .bind(token)
            .bind(next)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn delete_refresh_token(&self, token: &str) -> Result<()> {
        sqlx::query("DELETE FROM refresh_tokens WHERE token = $1")
            .bind(token)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}
