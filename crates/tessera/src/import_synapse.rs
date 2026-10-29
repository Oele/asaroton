//! `tessera import-synapse`: copy a Synapse homeserver's data into an empty tessera database.
//!
//! The selection follows docs/migration-from-synapse.md: every user row (deactivated accounts as
//! names only), the rooms a non-deactivated local account is or was a member of, every event of
//! those rooms, and the sessions, keys, account data, pushers and media of the active accounts.
//! Stream orderings and state group ids are kept, so the imported history keeps its order and
//! the sequences continue past the Synapse maxima.

#![allow(
    clippy::cast_possible_truncation,
    clippy::many_single_char_names,
    clippy::too_many_lines,
    clippy::map_unwrap_or,
    clippy::bool_to_int_with_if,
    clippy::type_complexity
)]

use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
};

use anyhow::Context;
use ruma::{OwnedEventId, OwnedRoomId, RoomVersionId};
use serde_json::{Value, json};
use sqlx::{PgPool, Row, postgres::PgPoolOptions};
use tessera_core::Pdu;

use crate::config::Config;

#[derive(Debug, clap::Args)]
pub struct ImportArgs {
    /// Synapse's PostgreSQL URL (read only), e.g. `postgres:///synapse?host=/var/run/postgresql`.
    #[arg(long)]
    pub from: String,
    /// Synapse's `signing.key` file; its key becomes this server's signing key.
    #[arg(long)]
    pub signing_key: Option<PathBuf>,
    /// Synapse's `media_store_path`; local uploads are hard-linked (or copied) into `data_dir`.
    #[arg(long)]
    pub media_src: Option<PathBuf>,
    /// Import into a database that already holds users (for re-runs on a rehearsal copy).
    #[arg(long)]
    pub force: bool,
}

struct Importer {
    src: PgPool,
    dst: PgPool,
    server_name: String,
    media_dir: PathBuf,
    now: i64,
    active_users: Vec<String>,
    rooms: Vec<String>,
    rules: HashMap<String, ruma::room_version_rules::RoomVersionRules>,
}

pub async fn run(config: Config, args: ImportArgs) -> anyhow::Result<()> {
    let store = crate::server::connect_store(&config).await?;
    let dst = store.pool().clone();
    let src = PgPoolOptions::new()
        .max_connections(4)
        .connect(&args.from)
        .await
        .context("connecting to the Synapse database")?;
    let existing: i64 = sqlx::query("SELECT count(*) FROM users")
        .fetch_one(&dst)
        .await?
        .get(0);
    if existing > 0 && !args.force {
        anyhow::bail!(
            "the tessera database already holds {existing} users; use --force to import anyway"
        );
    }
    let mut imp = Importer {
        src,
        dst,
        server_name: config.server_name.to_string(),
        media_dir: config.data_dir.join("media"),
        now: tessera_store::now_ts(),
        active_users: Vec::new(),
        rooms: Vec::new(),
        rules: HashMap::new(),
    };
    imp.select().await?;
    imp.users().await?;
    imp.sessions().await?;
    imp.e2ee().await?;
    imp.account_data().await?;
    imp.push_rules().await?;
    imp.pushers().await?;
    imp.rooms().await?;
    imp.events().await?;
    imp.state().await?;
    imp.memberships_and_extremities().await?;
    imp.receipts().await?;
    imp.media(args.media_src.as_deref()).await?;
    if let Some(path) = &args.signing_key {
        imp.signing_key(path).await?;
    }
    imp.sequences().await?;
    println!("import finished");
    Ok(())
}

/// Synapse stores JSON as text; a few rows carry a NUL escape PostgreSQL's jsonb rejects.
fn to_jsonb(text: &str) -> Value {
    serde_json::from_str::<Value>(text).unwrap_or(Value::Null)
}

fn strip_nul(s: &str) -> String {
    s.replace('\0', "")
}

impl Importer {
    async fn select(&mut self) -> anyhow::Result<()> {
        let rows = sqlx::query("SELECT name FROM users WHERE deactivated = 0")
            .fetch_all(&self.src)
            .await?;
        self.active_users = rows.iter().map(|r| r.get::<String, _>(0)).collect();
        let rows = sqlx::query(
            "SELECT DISTINCT room_id FROM local_current_membership
              WHERE user_id = ANY($1) AND room_id NOT IN (SELECT room_id FROM partial_state_rooms)
              ORDER BY room_id",
        )
        .bind(&self.active_users)
        .fetch_all(&self.src)
        .await?;
        self.rooms = rows.iter().map(|r| r.get::<String, _>(0)).collect();
        println!(
            "selected {} active users and {} rooms",
            self.active_users.len(),
            self.rooms.len()
        );
        Ok(())
    }

    async fn users(&self) -> anyhow::Result<()> {
        let rows = sqlx::query(
            "SELECT u.name, u.password_hash, u.creation_ts, u.admin, u.is_guest, u.appservice_id,
                    u.deactivated, p.displayname, p.avatar_url
               FROM users u LEFT JOIN profiles p ON p.full_user_id = u.name",
        )
        .fetch_all(&self.src)
        .await?;
        let mut n = 0usize;
        for chunk in rows.chunks(1000) {
            let (
                mut ids,
                mut hashes,
                mut created,
                mut admins,
                mut guests,
                mut appservices,
                mut deact,
                mut names,
                mut avatars,
            ) = (
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
            );
            for r in chunk {
                let deactivated = r.get::<i16, _>("deactivated") != 0;
                ids.push(r.get::<String, _>("name"));
                // Deactivated accounts keep no credentials.
                hashes.push(if deactivated {
                    None
                } else {
                    r.get::<Option<String>, _>("password_hash")
                });
                created.push(r.get::<Option<i64>, _>("creation_ts").unwrap_or(0) * 1000);
                admins.push(r.get::<Option<i16>, _>("admin").unwrap_or(0) != 0);
                guests.push(r.get::<Option<i16>, _>("is_guest").unwrap_or(0) != 0);
                appservices.push(r.get::<Option<String>, _>("appservice_id"));
                deact.push(deactivated);
                names.push(if deactivated {
                    None
                } else {
                    r.get::<Option<String>, _>("displayname")
                });
                avatars.push(if deactivated {
                    None
                } else {
                    r.get::<Option<String>, _>("avatar_url")
                });
            }
            n += sqlx::query(
                "INSERT INTO users (user_id, password_hash, display_name, avatar_url, is_admin, is_guest, is_deactivated, appservice_id, created_ts)
                 SELECT * FROM UNNEST($1::text[], $2::text[], $3::text[], $4::text[], $5::bool[], $6::bool[], $7::bool[], $8::text[], $9::bigint[])
                 ON CONFLICT (user_id) DO UPDATE SET password_hash = EXCLUDED.password_hash, display_name = EXCLUDED.display_name,
                    avatar_url = EXCLUDED.avatar_url, is_admin = EXCLUDED.is_admin, is_deactivated = EXCLUDED.is_deactivated",
            )
            .bind(&ids).bind(&hashes).bind(&names).bind(&avatars).bind(&admins).bind(&guests).bind(&deact).bind(&appservices).bind(&created)
            .execute(&self.dst)
            .await?
            .rows_affected() as usize;
        }
        println!("users: {n}");
        Ok(())
    }

    async fn sessions(&self) -> anyhow::Result<()> {
        let devices = sqlx::query(
            "SELECT user_id, device_id, display_name, last_seen, ip, user_agent, COALESCE(hidden, false) AS hidden
               FROM devices WHERE user_id = ANY($1)",
        )
        .bind(&self.active_users)
        .fetch_all(&self.src)
        .await?;
        let mut nd = 0usize;
        for r in &devices {
            nd += sqlx::query(
                "INSERT INTO devices (user_id, device_id, display_name, created_ts, last_seen_ts, last_seen_ip, last_seen_user_agent, hidden)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT DO NOTHING",
            )
            .bind(r.get::<String, _>("user_id"))
            .bind(r.get::<String, _>("device_id"))
            .bind(r.get::<Option<String>, _>("display_name"))
            .bind(r.get::<Option<i64>, _>("last_seen").unwrap_or(self.now))
            .bind(r.get::<Option<i64>, _>("last_seen"))
            .bind(r.get::<Option<String>, _>("ip"))
            .bind(r.get::<Option<String>, _>("user_agent"))
            .bind(r.get::<bool, _>("hidden"))
            .execute(&self.dst)
            .await?
            .rows_affected() as usize;
        }
        // Refresh tokens first: access tokens reference them by value.
        let refresh = sqlx::query(
            "SELECT id, user_id, device_id, token, next_token_id, expiry_ts FROM refresh_tokens WHERE user_id = ANY($1)",
        )
        .bind(&self.active_users)
        .fetch_all(&self.src)
        .await?;
        let token_by_id: HashMap<i64, String> = refresh
            .iter()
            .map(|r| (r.get::<i64, _>("id"), r.get::<String, _>("token")))
            .collect();
        let mut nr = 0usize;
        for r in &refresh {
            nr += sqlx::query(
                "INSERT INTO refresh_tokens (token, user_id, device_id, created_ts, expires_ts, next_token)
                 VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT DO NOTHING",
            )
            .bind(r.get::<String, _>("token"))
            .bind(r.get::<String, _>("user_id"))
            .bind(r.get::<String, _>("device_id"))
            .bind(self.now)
            .bind(r.get::<Option<i64>, _>("expiry_ts"))
            .bind(r.get::<Option<i64>, _>("next_token_id").and_then(|id| token_by_id.get(&id).cloned()))
            .execute(&self.dst)
            .await?
            .rows_affected() as usize;
        }
        let tokens = sqlx::query(
            "SELECT id, user_id, device_id, token, valid_until_ms, puppets_user_id, last_validated, refresh_token_id
               FROM access_tokens WHERE user_id = ANY($1)",
        )
        .bind(&self.active_users)
        .fetch_all(&self.src)
        .await?;
        let mut na = 0usize;
        for r in &tokens {
            na += sqlx::query(
                "INSERT INTO access_tokens (token, user_id, device_id, created_ts, expires_ts, last_used_ts, refresh_token, puppets_user_id)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT DO NOTHING",
            )
            .bind(r.get::<String, _>("token"))
            .bind(r.get::<String, _>("user_id"))
            .bind(r.get::<Option<String>, _>("device_id"))
            .bind(r.get::<Option<i64>, _>("last_validated").unwrap_or(self.now))
            .bind(r.get::<Option<i64>, _>("valid_until_ms"))
            .bind(r.get::<Option<i64>, _>("last_validated"))
            .bind(r.get::<Option<i64>, _>("refresh_token_id").and_then(|id| token_by_id.get(&id).cloned()))
            .bind(r.get::<Option<String>, _>("puppets_user_id"))
            .execute(&self.dst)
            .await?
            .rows_affected() as usize;
        }
        // Pushers reference access tokens by id in Synapse; remember the mapping for later.
        println!("devices: {nd}, refresh tokens: {nr}, access tokens: {na}");
        Ok(())
    }

    async fn e2ee(&self) -> anyhow::Result<()> {
        let mut counts = Vec::new();
        let rows = sqlx::query("SELECT user_id, device_id, ts_added_ms, key_json FROM e2e_device_keys_json WHERE user_id = ANY($1)")
            .bind(&self.active_users).fetch_all(&self.src).await?;
        let mut n = 0usize;
        for r in &rows {
            n += sqlx::query("INSERT INTO device_keys (user_id, device_id, key_json, updated_ts) VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING")
                .bind(r.get::<String, _>("user_id")).bind(r.get::<String, _>("device_id"))
                .bind(to_jsonb(&r.get::<String, _>("key_json"))).bind(r.get::<Option<i64>, _>("ts_added_ms").unwrap_or(self.now))
                .execute(&self.dst).await?.rows_affected() as usize;
        }
        counts.push(("device keys", n));

        let rows = sqlx::query("SELECT user_id, device_id, algorithm, key_id, ts_added_ms, key_json FROM e2e_one_time_keys_json WHERE user_id = ANY($1)")
            .bind(&self.active_users).fetch_all(&self.src).await?;
        let mut n = 0usize;
        for r in &rows {
            n += sqlx::query("INSERT INTO one_time_keys (user_id, device_id, algorithm, key_id, key_json, created_ts) VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT DO NOTHING")
                .bind(r.get::<String, _>("user_id")).bind(r.get::<String, _>("device_id")).bind(r.get::<String, _>("algorithm"))
                .bind(r.get::<String, _>("key_id")).bind(to_jsonb(&r.get::<String, _>("key_json"))).bind(r.get::<Option<i64>, _>("ts_added_ms").unwrap_or(self.now))
                .execute(&self.dst).await?.rows_affected() as usize;
        }
        counts.push(("one-time keys", n));

        let rows = sqlx::query("SELECT user_id, device_id, algorithm, key_id, key_json, COALESCE(used, false) AS used FROM e2e_fallback_keys_json WHERE user_id = ANY($1)")
            .bind(&self.active_users).fetch_all(&self.src).await?;
        let mut n = 0usize;
        for r in &rows {
            n += sqlx::query("INSERT INTO fallback_keys (user_id, device_id, algorithm, key_id, key_json, used) VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT DO NOTHING")
                .bind(r.get::<String, _>("user_id")).bind(r.get::<String, _>("device_id")).bind(r.get::<String, _>("algorithm"))
                .bind(r.get::<String, _>("key_id")).bind(to_jsonb(&r.get::<String, _>("key_json"))).bind(r.get::<bool, _>("used"))
                .execute(&self.dst).await?.rows_affected() as usize;
        }
        counts.push(("fallback keys", n));

        let rows = sqlx::query("SELECT user_id, keytype, keydata FROM e2e_cross_signing_keys WHERE user_id = ANY($1) ORDER BY stream_id")
            .bind(&self.active_users).fetch_all(&self.src).await?;
        let mut n = 0usize;
        for r in &rows {
            n += sqlx::query("INSERT INTO cross_signing_keys (user_id, key_type, key_json, updated_ts) VALUES ($1, $2, $3, $4)
                              ON CONFLICT (user_id, key_type) DO UPDATE SET key_json = EXCLUDED.key_json")
                .bind(r.get::<String, _>("user_id")).bind(r.get::<String, _>("keytype"))
                .bind(to_jsonb(&r.get::<String, _>("keydata"))).bind(self.now)
                .execute(&self.dst).await?.rows_affected() as usize;
        }
        counts.push(("cross-signing keys", n));

        let rows = sqlx::query("SELECT user_id, key_id, target_user_id, target_device_id, signature FROM e2e_cross_signing_signatures WHERE user_id = ANY($1)")
            .bind(&self.active_users).fetch_all(&self.src).await?;
        let mut n = 0usize;
        for r in &rows {
            n += sqlx::query("INSERT INTO key_signatures (target_user_id, target_key_id, signing_user_id, signing_key_id, signature) VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING")
                .bind(r.get::<String, _>("target_user_id")).bind(r.get::<String, _>("target_device_id"))
                .bind(r.get::<String, _>("user_id")).bind(r.get::<String, _>("key_id")).bind(r.get::<String, _>("signature"))
                .execute(&self.dst).await?.rows_affected() as usize;
        }
        counts.push(("key signatures", n));

        let rows = sqlx::query("SELECT user_id, version, algorithm, auth_data, COALESCE(deleted, 0) AS deleted, COALESCE(etag, 0) AS etag FROM e2e_room_keys_versions WHERE user_id = ANY($1)")
            .bind(&self.active_users).fetch_all(&self.src).await?;
        let mut n = 0usize;
        for r in &rows {
            let algorithm = json!({ "algorithm": r.get::<String, _>("algorithm"), "auth_data": to_jsonb(&r.get::<String, _>("auth_data")) });
            n += sqlx::query("INSERT INTO key_backup_versions (user_id, version, algorithm, etag, deleted) VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING")
                .bind(r.get::<String, _>("user_id")).bind(r.get::<i64, _>("version")).bind(algorithm.to_string())
                .bind(r.get::<i64, _>("etag")).bind(r.get::<i16, _>("deleted") != 0)
                .execute(&self.dst).await?.rows_affected() as usize;
        }
        counts.push(("backup versions", n));

        let rows = sqlx::query("SELECT user_id, room_id, session_id, version, first_message_index, forwarded_count, is_verified, session_data FROM e2e_room_keys WHERE user_id = ANY($1)")
            .bind(&self.active_users).fetch_all(&self.src).await?;
        let mut n = 0usize;
        for r in &rows {
            n += sqlx::query("INSERT INTO key_backup_keys (user_id, version, room_id, session_id, first_message_index, forwarded_count, is_verified, session_data) VALUES ($1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT DO NOTHING")
                .bind(r.get::<String, _>("user_id")).bind(r.get::<i64, _>("version")).bind(r.get::<String, _>("room_id")).bind(r.get::<String, _>("session_id"))
                .bind(i64::from(r.get::<Option<i32>, _>("first_message_index").unwrap_or(0))).bind(i64::from(r.get::<Option<i32>, _>("forwarded_count").unwrap_or(0)))
                .bind(r.get::<Option<bool>, _>("is_verified").unwrap_or(false)).bind(r.get::<String, _>("session_data"))
                .execute(&self.dst).await?.rows_affected() as usize;
        }
        counts.push(("backup keys", n));

        // Pending to-device messages (room keys for devices that were offline).
        let rows = sqlx::query("SELECT user_id, device_id, message_json FROM device_inbox WHERE user_id = ANY($1) ORDER BY stream_id")
            .bind(&self.active_users).fetch_all(&self.src).await?;
        let mut n = 0usize;
        for r in &rows {
            let m = to_jsonb(&r.get::<String, _>("message_json"));
            let (Some(ty), Some(sender)) = (
                m.get("type").and_then(Value::as_str),
                m.get("sender").and_then(Value::as_str),
            ) else {
                continue;
            };
            n += sqlx::query("INSERT INTO to_device_messages (stream_id, user_id, device_id, sender, event_type, content, created_ts) VALUES (nextval('to_device_seq'), $1, $2, $3, $4, $5, $6)")
                .bind(r.get::<String, _>("user_id")).bind(r.get::<String, _>("device_id")).bind(sender).bind(ty)
                .bind(m.get("content").cloned().unwrap_or(Value::Null)).bind(self.now)
                .execute(&self.dst).await?.rows_affected() as usize;
        }
        counts.push(("pending to-device messages", n));

        // The device-list stream: one entry per active user at Synapse's position, and the
        // sequence continues from there so remote servers keep accepting updates.
        let max: Option<i64> = sqlx::query("SELECT max(stream_id) FROM device_lists_stream")
            .fetch_one(&self.src)
            .await?
            .get(0);
        let rows = sqlx::query("SELECT user_id, max(stream_id) AS stream_id FROM device_lists_stream WHERE user_id = ANY($1) GROUP BY user_id")
            .bind(&self.active_users).fetch_all(&self.src).await?;
        for r in &rows {
            sqlx::query("INSERT INTO device_list_changes (stream_id, user_id, device_id, changed_ts) VALUES ($1, $2, NULL, $3) ON CONFLICT DO NOTHING")
                .bind(r.get::<i64, _>("stream_id")).bind(r.get::<String, _>("user_id")).bind(self.now)
                .execute(&self.dst).await?;
        }
        if let Some(max) = max {
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "ALTER SEQUENCE device_lists_seq RESTART WITH {}",
                max + 1
            )))
            .execute(&self.dst)
            .await?;
        }
        counts.push(("device-list positions", rows.len()));
        println!(
            "e2ee: {}",
            counts
                .iter()
                .map(|(k, v)| format!("{k} {v}"))
                .collect::<Vec<_>>()
                .join(", ")
        );
        Ok(())
    }

    async fn account_data(&self) -> anyhow::Result<()> {
        let mut n = 0usize;
        let rows = sqlx::query(
            "SELECT user_id, account_data_type, content FROM account_data WHERE user_id = ANY($1)",
        )
        .bind(&self.active_users)
        .fetch_all(&self.src)
        .await?;
        for r in &rows {
            n += self
                .put_account_data(
                    &r.get::<String, _>("user_id"),
                    "",
                    &r.get::<String, _>("account_data_type"),
                    to_jsonb(&r.get::<String, _>("content")),
                )
                .await?;
        }
        let rows = sqlx::query("SELECT user_id, room_id, account_data_type, content FROM room_account_data WHERE user_id = ANY($1) AND room_id = ANY($2)")
            .bind(&self.active_users).bind(&self.rooms).fetch_all(&self.src).await?;
        for r in &rows {
            n += self
                .put_account_data(
                    &r.get::<String, _>("user_id"),
                    &r.get::<String, _>("room_id"),
                    &r.get::<String, _>("account_data_type"),
                    to_jsonb(&r.get::<String, _>("content")),
                )
                .await?;
        }
        // Room tags are account data in the client API; Synapse keeps them apart.
        let rows = sqlx::query("SELECT user_id, room_id, tag, content FROM room_tags WHERE user_id = ANY($1) AND room_id = ANY($2) ORDER BY user_id, room_id")
            .bind(&self.active_users).bind(&self.rooms).fetch_all(&self.src).await?;
        let mut tags: HashMap<(String, String), serde_json::Map<String, Value>> = HashMap::new();
        for r in &rows {
            tags.entry((r.get("user_id"), r.get("room_id")))
                .or_default()
                .insert(
                    r.get::<String, _>("tag"),
                    to_jsonb(&r.get::<String, _>("content")),
                );
        }
        for ((user, room), map) in tags {
            n += self
                .put_account_data(&user, &room, "m.tag", json!({ "tags": map }))
                .await?;
        }
        let rows = sqlx::query("SELECT user_id, filter_id, convert_from(filter_json, 'UTF8') AS filter_json FROM user_filters WHERE full_user_id = ANY($1)")
            .bind(&self.active_users).fetch_all(&self.src).await?;
        let mut nf = 0usize;
        for r in &rows {
            nf += sqlx::query("INSERT INTO filters (user_id, filter_id, filter_json) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING")
                .bind(r.get::<String, _>("user_id")).bind(r.get::<i64, _>("filter_id")).bind(to_jsonb(&r.get::<String, _>("filter_json")))
                .execute(&self.dst).await?.rows_affected() as usize;
        }
        println!("account data: {n}, filters: {nf}");
        Ok(())
    }

    async fn put_account_data(
        &self,
        user: &str,
        room: &str,
        ty: &str,
        content: Value,
    ) -> anyhow::Result<usize> {
        if content.is_null() {
            return Ok(0);
        }
        Ok(sqlx::query(
            "INSERT INTO account_data (user_id, room_id, data_type, content, stream_id) VALUES ($1, $2, $3, $4, nextval('account_data_seq'))
             ON CONFLICT (user_id, room_id, data_type) DO UPDATE SET content = EXCLUDED.content, stream_id = EXCLUDED.stream_id",
        )
        .bind(user).bind(room).bind(ty).bind(content)
        .execute(&self.dst).await?.rows_affected() as usize)
    }

    async fn push_rules(&self) -> anyhow::Result<()> {
        let rules = sqlx::query("SELECT user_name, rule_id, priority_class, priority, conditions, actions FROM push_rules WHERE user_name = ANY($1) ORDER BY user_name, priority_class, priority DESC")
            .bind(&self.active_users).fetch_all(&self.src).await?;
        let enables = sqlx::query(
            "SELECT user_name, rule_id, enabled FROM push_rules_enable WHERE user_name = ANY($1)",
        )
        .bind(&self.active_users)
        .fetch_all(&self.src)
        .await?;
        let mut per_user: HashMap<String, Vec<&sqlx::postgres::PgRow>> = HashMap::new();
        for r in &rules {
            per_user.entry(r.get("user_name")).or_default().push(r);
        }
        let mut disabled: HashMap<String, HashSet<String>> = HashMap::new();
        let mut enabled_custom: HashMap<(String, String), bool> = HashMap::new();
        for r in &enables {
            let on = r.get::<Option<i16>, _>("enabled").unwrap_or(1) != 0;
            let user: String = r.get("user_name");
            let rule: String = r.get("rule_id");
            if !on {
                disabled
                    .entry(user.clone())
                    .or_default()
                    .insert(rule.clone());
            }
            enabled_custom.insert((user, rule), on);
        }
        let mut n = 0usize;
        for user in &self.active_users {
            let Ok(uid) = ruma::OwnedUserId::try_from(user.as_str()) else {
                continue;
            };
            let custom = per_user.get(user).map(Vec::as_slice).unwrap_or(&[]);
            let off = disabled.get(user).cloned().unwrap_or_default();
            if custom.is_empty() && off.is_empty() {
                continue;
            }
            let mut ruleset = serde_json::to_value(tessera_service::push::default_ruleset(&uid))?;
            for kind in ["override", "content", "room", "sender", "underride"] {
                if let Some(list) = ruleset.get_mut(kind).and_then(Value::as_array_mut) {
                    for rule in list.iter_mut() {
                        if let Some(id) = rule.get("rule_id").and_then(Value::as_str)
                            && off.contains(id)
                        {
                            rule["enabled"] = json!(false);
                        }
                    }
                }
            }
            for r in custom {
                let rule_id: String = r.get("rule_id");
                let class: i16 = r.get("priority_class");
                let conditions = to_jsonb(&r.get::<String, _>("conditions"));
                let actions = to_jsonb(&r.get::<String, _>("actions"));
                let enabled = enabled_custom
                    .get(&(user.clone(), rule_id.clone()))
                    .copied()
                    .unwrap_or(true);
                let (kind, rule) = match class {
                    5 => (
                        "override",
                        json!({ "rule_id": rule_id, "default": false, "enabled": enabled, "conditions": conditions, "actions": actions }),
                    ),
                    4 => {
                        let pattern = conditions
                            .as_array()
                            .and_then(|c| {
                                c.iter().find(|x| {
                                    x.get("key").and_then(Value::as_str) == Some("content.body")
                                })
                            })
                            .and_then(|x| x.get("pattern").cloned())
                            .unwrap_or(Value::String(rule_id.clone()));
                        (
                            "content",
                            json!({ "rule_id": rule_id, "default": false, "enabled": enabled, "pattern": pattern, "actions": actions }),
                        )
                    }
                    3 => (
                        "room",
                        json!({ "rule_id": rule_id, "default": false, "enabled": enabled, "actions": actions }),
                    ),
                    2 => (
                        "sender",
                        json!({ "rule_id": rule_id, "default": false, "enabled": enabled, "actions": actions }),
                    ),
                    _ => (
                        "underride",
                        json!({ "rule_id": rule_id, "default": false, "enabled": enabled, "conditions": conditions, "actions": actions }),
                    ),
                };
                if let Some(list) = ruleset.get_mut(kind).and_then(Value::as_array_mut) {
                    // User rules precede the server defaults of their kind, except the master
                    // switch which always comes first.
                    let at = if kind == "override"
                        && list
                            .first()
                            .and_then(|f| f.get("rule_id"))
                            .and_then(Value::as_str)
                            == Some(".m.rule.master")
                    {
                        1
                    } else {
                        0
                    };
                    let at = at.min(list.len());
                    list.insert(at, rule);
                }
            }
            // Round-trip through ruma so only a ruleset tessera can load is stored.
            let parsed: ruma::push::Ruleset = match serde_json::from_value(ruleset.clone()) {
                Ok(p) => p,
                Err(e) => {
                    eprintln!("push rules of {user} not importable ({e}); defaults kept");
                    continue;
                }
            };
            sqlx::query("INSERT INTO push_rules (user_id, ruleset, stream_id) VALUES ($1, $2, nextval('account_data_seq')) ON CONFLICT (user_id) DO UPDATE SET ruleset = EXCLUDED.ruleset, stream_id = EXCLUDED.stream_id")
                .bind(user).bind(serde_json::to_value(parsed)?)
                .execute(&self.dst).await?;
            n += 1;
        }
        println!("push rulesets: {n}");
        Ok(())
    }

    async fn pushers(&self) -> anyhow::Result<()> {
        let rows = sqlx::query(
            "SELECT p.user_name, p.kind, p.app_id, p.app_display_name, p.device_display_name, p.pushkey, p.ts, p.lang, p.data, p.profile_tag,
                    p.last_stream_ordering, t.token
               FROM pushers p LEFT JOIN access_tokens t ON t.id = p.access_token
              WHERE p.user_name = ANY($1) AND COALESCE(p.enabled, true)",
        )
        .bind(&self.active_users).fetch_all(&self.src).await?;
        let mut n = 0usize;
        for r in &rows {
            n += sqlx::query(
                "INSERT INTO pushers (user_id, app_id, pushkey, kind, app_display_name, device_display_name, profile_tag, lang, data, created_ts, last_stream_ordering, access_token)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12) ON CONFLICT DO NOTHING",
            )
            .bind(r.get::<String, _>("user_name")).bind(r.get::<String, _>("app_id")).bind(r.get::<String, _>("pushkey")).bind(r.get::<String, _>("kind"))
            .bind(r.get::<Option<String>, _>("app_display_name").unwrap_or_default()).bind(r.get::<Option<String>, _>("device_display_name").unwrap_or_default())
            .bind(r.get::<Option<String>, _>("profile_tag")).bind(r.get::<Option<String>, _>("lang").unwrap_or_else(|| "en".into()))
            .bind(to_jsonb(&r.get::<Option<String>, _>("data").unwrap_or_else(|| "{}".into()))).bind(r.get::<Option<i64>, _>("ts").unwrap_or(self.now))
            .bind(r.get::<Option<i64>, _>("last_stream_ordering").unwrap_or(0)).bind(r.get::<Option<String>, _>("token"))
            .execute(&self.dst).await?.rows_affected() as usize;
        }
        println!("pushers: {n}");
        Ok(())
    }

    async fn rooms(&mut self) -> anyhow::Result<()> {
        let rows = sqlx::query("SELECT room_id, room_version, creator, COALESCE(is_public, false) AS is_public, COALESCE(has_auth_chain_index, false) AS hac FROM rooms WHERE room_id = ANY($1)")
            .bind(&self.rooms).fetch_all(&self.src).await?;
        let mut n = 0usize;
        for r in &rows {
            let room_id: String = r.get("room_id");
            let version: String = r
                .get::<Option<String>, _>("room_version")
                .unwrap_or_else(|| "1".into());
            let rv = RoomVersionId::try_from(version.as_str())
                .map_err(|e| anyhow::anyhow!("{room_id}: room version {version}: {e}"))?;
            let rules = tessera_core::room_version::rules(&rv)
                .map_err(|e| anyhow::anyhow!("{room_id}: {e}"))?;
            self.rules.insert(room_id.clone(), rules);
            n += sqlx::query("INSERT INTO rooms (room_id, room_version, creator, is_public, has_auth_chain_index, created_ts) VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT DO NOTHING")
                .bind(&room_id).bind(&version).bind(r.get::<Option<String>, _>("creator")).bind(r.get::<bool, _>("is_public")).bind(r.get::<bool, _>("hac")).bind(self.now)
                .execute(&self.dst).await?.rows_affected() as usize;
        }
        let rows = sqlx::query(
            "SELECT room_alias, room_id, creator FROM room_aliases WHERE room_id = ANY($1)",
        )
        .bind(&self.rooms)
        .fetch_all(&self.src)
        .await?;
        let mut na = 0usize;
        for r in &rows {
            na += sqlx::query("INSERT INTO room_aliases (alias, room_id, creator, created_ts) VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING")
                .bind(r.get::<String, _>("room_alias")).bind(r.get::<String, _>("room_id")).bind(r.get::<Option<String>, _>("creator")).bind(self.now)
                .execute(&self.dst).await?.rows_affected() as usize;
        }
        println!("rooms: {n}, aliases: {na}");
        Ok(())
    }

    async fn events(&self) -> anyhow::Result<()> {
        // Redactions that apply within the selected rooms: target -> redaction event.
        let rows = sqlx::query(
            "SELECT r.redacts, r.event_id FROM redactions r JOIN events e ON e.event_id = r.event_id
              WHERE e.room_id = ANY($1) AND NOT EXISTS (SELECT 1 FROM rejections x WHERE x.event_id = r.event_id)",
        )
        .bind(&self.rooms).fetch_all(&self.src).await?;
        let redactions: HashMap<String, String> = rows
            .iter()
            .map(|r| (r.get("redacts"), r.get("event_id")))
            .collect();
        let mut after = i64::MIN;
        let (mut total, mut skipped, mut redacted, mut rel, mut searchable) =
            (0usize, 0usize, 0usize, 0usize, 0usize);
        loop {
            let rows = sqlx::query(
                "SELECT e.event_id, e.room_id, e.type, e.state_key, e.sender, e.origin_server_ts, e.depth, e.stream_ordering,
                        COALESCE(e.outlier, false) AS outlier, e.received_ts, j.json, j.internal_metadata,
                        (x.event_id IS NOT NULL) AS rejected, sg.state_group
                   FROM events e JOIN event_json j ON j.event_id = e.event_id
                   LEFT JOIN rejections x ON x.event_id = e.event_id
                   LEFT JOIN event_to_state_groups sg ON sg.event_id = e.event_id
                  WHERE e.room_id = ANY($1) AND e.stream_ordering > $2
                  ORDER BY e.stream_ordering LIMIT 2000",
            )
            .bind(&self.rooms).bind(after)
            .fetch_all(&self.src).await?;
            if rows.is_empty() {
                break;
            }
            let (
                mut so,
                mut ids,
                mut rooms,
                mut types,
                mut keys,
                mut senders,
                mut ts,
                mut depths,
                mut jsons,
                mut originals,
                mut groups,
                mut rej,
                mut soft,
                mut outl,
                mut recv,
            ) = (
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
            );
            let (mut edge_e, mut edge_p, mut auth_e, mut auth_a) =
                (Vec::new(), Vec::new(), Vec::new(), Vec::new());
            let (mut rel_rows, mut search_rows): (
                Vec<(String, String, String, String, String, String, i64, i64)>,
                Vec<(String, String, i64, i64, String)>,
            ) = (Vec::new(), Vec::new());
            for r in &rows {
                let stream: i64 = r.get("stream_ordering");
                after = stream;
                let event_id: String = r.get("event_id");
                let room_id: String = r.get("room_id");
                let Some(rules) = self.rules.get(&room_id) else {
                    continue;
                };
                let raw = match tessera_core::canonical::from_str(&strip_nul(
                    &r.get::<String, _>("json"),
                )) {
                    Ok(o) => o,
                    Err(e) => {
                        eprintln!("{event_id}: unparsable JSON ({e}); skipped");
                        skipped += 1;
                        continue;
                    }
                };
                let Ok(eid) = OwnedEventId::try_from(event_id.as_str()) else {
                    skipped += 1;
                    continue;
                };
                let pdu = match Pdu::from_canonical_trusted(raw, rules, eid.clone()) {
                    Ok(p) => p,
                    Err(e) => {
                        eprintln!("{event_id}: not a valid PDU ({e}); skipped");
                        skipped += 1;
                        continue;
                    }
                };
                let mut json_obj = pdu.raw().clone();
                json_obj.remove("unsigned");
                let original = tessera_core::canonical::to_string(&json_obj);
                let (json, original_json) = match redactions.get(&event_id) {
                    Some(by) => {
                        let because = self.load_pdu(by, rules).await;
                        match tessera_core::redaction::redact_pdu(&pdu, rules, because.as_ref()) {
                            Ok(red) => {
                                redacted += 1;
                                let mut o = red.raw().clone();
                                o.remove("unsigned");
                                (
                                    tessera_core::canonical::to_string(&o),
                                    Some(original.clone()),
                                )
                            }
                            Err(_) => (original.clone(), None),
                        }
                    }
                    None => (original.clone(), None),
                };
                let meta = to_jsonb(
                    &r.get::<Option<String>, _>("internal_metadata")
                        .unwrap_or_else(|| "{}".into()),
                );
                let is_rejected: bool = r.get("rejected");
                let is_outlier: bool = r.get("outlier");
                so.push(stream);
                ids.push(event_id.clone());
                rooms.push(room_id.clone());
                types.push(pdu.event_type().to_string());
                keys.push(pdu.state_key().map(str::to_owned));
                senders.push(pdu.sender().to_string());
                ts.push(tessera_store::uint_to_i64(pdu.origin_server_ts().0));
                depths.push(tessera_store::uint_to_i64(pdu.depth()));
                jsons.push(json);
                let was_redacted = original_json.is_some();
                originals.push(original_json);
                groups.push(if is_outlier {
                    None
                } else {
                    r.get::<Option<i64>, _>("state_group")
                });
                rej.push(is_rejected);
                soft.push(
                    meta.get("soft_failed")
                        .and_then(Value::as_bool)
                        .unwrap_or(false),
                );
                outl.push(is_outlier);
                recv.push(r.get::<Option<i64>, _>("received_ts").unwrap_or(self.now));
                for p in pdu.prev_events() {
                    edge_e.push(event_id.clone());
                    edge_p.push(p.to_string());
                }
                for a in pdu.auth_events() {
                    auth_e.push(event_id.clone());
                    auth_a.push(a.to_string());
                }
                if !is_rejected && !was_redacted {
                    let content: Value =
                        serde_json::from_str(pdu.content().get()).unwrap_or_default();
                    if let Some(relm) = content.get("m.relates_to")
                        && let (Some(rt), Some(target)) = (
                            relm.get("rel_type").and_then(Value::as_str),
                            relm.get("event_id").and_then(Value::as_str),
                        )
                        && ruma::EventId::parse(target).is_ok()
                    {
                        rel_rows.push((
                            event_id.clone(),
                            room_id.clone(),
                            target.to_owned(),
                            rt.to_owned(),
                            pdu.event_type().to_string(),
                            pdu.sender().to_string(),
                            tessera_store::uint_to_i64(pdu.origin_server_ts().0),
                            stream,
                        ));
                    }
                    let key = match pdu.event_type().to_string().as_str() {
                        "m.room.message" => Some("body"),
                        "m.room.name" => Some("name"),
                        "m.room.topic" => Some("topic"),
                        _ => None,
                    };
                    if let Some(k) = key
                        && let Some(text) = content.get(k).and_then(Value::as_str)
                    {
                        search_rows.push((
                            event_id.clone(),
                            room_id.clone(),
                            stream,
                            tessera_store::uint_to_i64(pdu.origin_server_ts().0),
                            strip_nul(text),
                        ));
                    }
                }
            }
            let mut tx = self.dst.begin().await?;
            total += sqlx::query(
                "INSERT INTO events (stream_ordering, event_id, room_id, event_type, state_key, sender, origin_server_ts, depth, json, original_json, state_group, rejected, soft_failed, outlier, received_ts)
                 SELECT * FROM UNNEST($1::bigint[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[], $7::bigint[], $8::bigint[], $9::text[], $10::text[], $11::bigint[], $12::bool[], $13::bool[], $14::bool[], $15::bigint[])
                 ON CONFLICT (event_id) DO NOTHING",
            )
            .bind(&so).bind(&ids).bind(&rooms).bind(&types).bind(&keys).bind(&senders).bind(&ts).bind(&depths).bind(&jsons).bind(&originals).bind(&groups).bind(&rej).bind(&soft).bind(&outl).bind(&recv)
            .execute(&mut *tx).await?.rows_affected() as usize;
            sqlx::query("INSERT INTO event_edges (event_id, prev_event_id) SELECT * FROM UNNEST($1::text[], $2::text[]) ON CONFLICT DO NOTHING")
                .bind(&edge_e).bind(&edge_p).execute(&mut *tx).await?;
            sqlx::query("INSERT INTO event_auth (event_id, auth_event_id) SELECT * FROM UNNEST($1::text[], $2::text[]) ON CONFLICT DO NOTHING")
                .bind(&auth_e).bind(&auth_a).execute(&mut *tx).await?;
            if !rel_rows.is_empty() {
                let (a, b, c, d, e, f, g, h): (
                    Vec<_>,
                    Vec<_>,
                    Vec<_>,
                    Vec<_>,
                    Vec<_>,
                    Vec<_>,
                    Vec<_>,
                    Vec<_>,
                ) = rel_rows
                    .iter()
                    .cloned()
                    .map(|t| (t.0, t.1, t.2, t.3, t.4, t.5, t.6, t.7))
                    .fold(
                        Default::default(),
                        |mut acc: (
                            Vec<String>,
                            Vec<String>,
                            Vec<String>,
                            Vec<String>,
                            Vec<String>,
                            Vec<String>,
                            Vec<i64>,
                            Vec<i64>,
                        ),
                         t| {
                            acc.0.push(t.0);
                            acc.1.push(t.1);
                            acc.2.push(t.2);
                            acc.3.push(t.3);
                            acc.4.push(t.4);
                            acc.5.push(t.5);
                            acc.6.push(t.6);
                            acc.7.push(t.7);
                            acc
                        },
                    );
                rel += sqlx::query("INSERT INTO event_relations (event_id, room_id, relates_to, rel_type, event_type, sender, origin_server_ts, stream_ordering) SELECT * FROM UNNEST($1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[], $7::bigint[], $8::bigint[]) ON CONFLICT DO NOTHING")
                    .bind(&a).bind(&b).bind(&c).bind(&d).bind(&e).bind(&f).bind(&g).bind(&h).execute(&mut *tx).await?.rows_affected() as usize;
            }
            if !search_rows.is_empty() {
                let (a, b, c, d, e): (Vec<String>, Vec<String>, Vec<i64>, Vec<i64>, Vec<String>) =
                    search_rows.iter().cloned().fold(
                        Default::default(),
                        |mut acc: (Vec<String>, Vec<String>, Vec<i64>, Vec<i64>, Vec<String>),
                         t| {
                            acc.0.push(t.0);
                            acc.1.push(t.1);
                            acc.2.push(t.2);
                            acc.3.push(t.3);
                            acc.4.push(t.4);
                            acc
                        },
                    );
                searchable += sqlx::query("INSERT INTO event_search (event_id, room_id, stream_ordering, origin_server_ts, vector, value) SELECT id, room, so, ts, to_tsvector('english', txt), txt FROM UNNEST($1::text[], $2::text[], $3::bigint[], $4::bigint[], $5::text[]) AS u(id, room, so, ts, txt) ON CONFLICT DO NOTHING")
                    .bind(&a).bind(&b).bind(&c).bind(&d).bind(&e).execute(&mut *tx).await?.rows_affected() as usize;
            }
            tx.commit().await?;
            if total % 20000 < 2000 {
                println!("events: {total} so far (stream position {after})");
            }
        }
        println!(
            "events: {total} imported, {skipped} skipped, {redacted} redacted, {rel} relations, {searchable} searchable"
        );
        Ok(())
    }

    async fn load_pdu(
        &self,
        event_id: &str,
        rules: &ruma::room_version_rules::RoomVersionRules,
    ) -> Option<Pdu> {
        let row = sqlx::query("SELECT json FROM event_json WHERE event_id = $1")
            .bind(event_id)
            .fetch_optional(&self.src)
            .await
            .ok()??;
        let raw =
            tessera_core::canonical::from_str(&strip_nul(&row.get::<String, _>("json"))).ok()?;
        let eid = OwnedEventId::try_from(event_id).ok()?;
        Pdu::from_canonical_trusted(raw, rules, eid).ok()
    }

    async fn state(&self) -> anyhow::Result<()> {
        // Groups of the selected rooms, with their delta chain. Ids are kept.
        let groups = sqlx::query("SELECT g.id, g.room_id, e.prev_state_group FROM state_groups g LEFT JOIN state_group_edges e ON e.state_group = g.id WHERE g.room_id = ANY($1) ORDER BY g.id")
            .bind(&self.rooms).fetch_all(&self.src).await?;
        let known: HashSet<i64> = groups.iter().map(|r| r.get::<i64, _>("id")).collect();
        let mut n = 0usize;
        for chunk in groups.chunks(5000) {
            let ids: Vec<i64> = chunk.iter().map(|r| r.get("id")).collect();
            let rooms: Vec<String> = chunk.iter().map(|r| r.get("room_id")).collect();
            // A previous group outside the selection (should not happen) breaks the chain
            // rather than the import.
            let prevs: Vec<Option<i64>> = chunk
                .iter()
                .map(|r| {
                    r.get::<Option<i64>, _>("prev_state_group")
                        .filter(|p| known.contains(p))
                })
                .collect();
            n += sqlx::query("INSERT INTO state_groups (id, room_id, prev_group) SELECT * FROM UNNEST($1::bigint[], $2::text[], $3::bigint[]) ON CONFLICT DO NOTHING")
                .bind(&ids).bind(&rooms).bind(&prevs).execute(&self.dst).await?.rows_affected() as usize;
        }
        let mut after = i64::MIN;
        let mut rows_n = 0usize;
        loop {
            let rows = sqlx::query("SELECT state_group, type, state_key, event_id FROM state_groups_state WHERE room_id = ANY($1) AND state_group > $2 ORDER BY state_group LIMIT 20000")
                .bind(&self.rooms).bind(after).fetch_all(&self.src).await?;
            if rows.is_empty() {
                break;
            }
            // Keep every row of the last group in the batch together with its group.
            let last: i64 = rows.last().map(|r| r.get("state_group")).unwrap_or(after);
            let mut rows: Vec<_> = rows;
            if rows.len() == 20000 {
                rows.retain(|r| r.get::<i64, _>("state_group") < last);
                after = last - 1;
            } else {
                after = last;
            }
            let g: Vec<i64> = rows.iter().map(|r| r.get("state_group")).collect();
            let t: Vec<String> = rows.iter().map(|r| r.get("type")).collect();
            let k: Vec<String> = rows.iter().map(|r| r.get("state_key")).collect();
            let e: Vec<String> = rows.iter().map(|r| r.get("event_id")).collect();
            rows_n += sqlx::query("INSERT INTO state_groups_state (state_group, event_type, state_key, event_id) SELECT * FROM UNNEST($1::bigint[], $2::text[], $3::text[], $4::text[]) ON CONFLICT DO NOTHING")
                .bind(&g).bind(&t).bind(&k).bind(&e).execute(&self.dst).await?.rows_affected() as usize;
        }
        let max: Option<i64> = sqlx::query("SELECT max(id) FROM state_groups")
            .fetch_one(&self.dst)
            .await?
            .get(0);
        if let Some(m) = max {
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "ALTER SEQUENCE state_groups_id_seq RESTART WITH {}",
                m + 1
            )))
            .execute(&self.dst)
            .await?;
        }
        println!("state groups: {n}, state rows: {rows_n}");
        Ok(())
    }

    async fn memberships_and_extremities(&self) -> anyhow::Result<()> {
        let rows = sqlx::query("SELECT room_id, type, state_key, event_id, membership FROM current_state_events WHERE room_id = ANY($1)")
            .bind(&self.rooms).fetch_all(&self.src).await?;
        let mut n = 0usize;
        for chunk in rows.chunks(5000) {
            let r: Vec<String> = chunk.iter().map(|x| x.get("room_id")).collect();
            let t: Vec<String> = chunk.iter().map(|x| x.get("type")).collect();
            let k: Vec<String> = chunk.iter().map(|x| x.get("state_key")).collect();
            let e: Vec<String> = chunk.iter().map(|x| x.get("event_id")).collect();
            let m: Vec<Option<String>> = chunk.iter().map(|x| x.get("membership")).collect();
            n += sqlx::query("INSERT INTO current_state_events (room_id, event_type, state_key, event_id, membership) SELECT * FROM UNNEST($1::text[], $2::text[], $3::text[], $4::text[], $5::text[]) ON CONFLICT DO NOTHING")
                .bind(&r).bind(&t).bind(&k).bind(&e).bind(&m).execute(&self.dst).await?.rows_affected() as usize;
        }
        // Membership rows from the current member events, with Synapse's forgotten flag.
        let nm = sqlx::query(
            "INSERT INTO room_memberships (room_id, user_id, membership, event_id, sender, display_name, avatar_url, stream_ordering, forgotten)
             SELECT c.room_id, c.state_key, c.membership, c.event_id, e.sender,
                    CASE WHEN jsonb_typeof((e.json::jsonb)->'content'->'displayname') = 'string' THEN (e.json::jsonb)->'content'->>'displayname' END,
                    CASE WHEN jsonb_typeof((e.json::jsonb)->'content'->'avatar_url') = 'string' THEN (e.json::jsonb)->'content'->>'avatar_url' END,
                    e.stream_ordering, FALSE
               FROM current_state_events c JOIN events e ON e.event_id = c.event_id
              WHERE c.event_type = 'm.room.member' AND c.membership IS NOT NULL
             ON CONFLICT (room_id, user_id) DO NOTHING",
        )
        .execute(&self.dst).await?.rows_affected();
        let forgotten = sqlx::query("SELECT DISTINCT room_id, user_id FROM room_memberships WHERE forgotten = 1 AND user_id = ANY($1) AND room_id = ANY($2)")
            .bind(&self.active_users).bind(&self.rooms).fetch_all(&self.src).await?;
        for r in &forgotten {
            sqlx::query(
                "UPDATE room_memberships SET forgotten = TRUE WHERE room_id = $1 AND user_id = $2",
            )
            .bind(r.get::<String, _>("room_id"))
            .bind(r.get::<String, _>("user_id"))
            .execute(&self.dst)
            .await?;
        }
        let ext = sqlx::query(
            "SELECT room_id, event_id FROM event_forward_extremities WHERE room_id = ANY($1)",
        )
        .bind(&self.rooms)
        .fetch_all(&self.src)
        .await?;
        for r in &ext {
            sqlx::query("INSERT INTO forward_extremities (room_id, event_id) VALUES ($1, $2) ON CONFLICT DO NOTHING")
                .bind(r.get::<String, _>("room_id")).bind(r.get::<String, _>("event_id")).execute(&self.dst).await?;
        }
        // Gaps: prev events referenced but not held.
        let missing = sqlx::query(
            "INSERT INTO missing_events (room_id, event_id, referenced_by)
             SELECT e.room_id, g.prev_event_id, g.event_id FROM event_edges g JOIN events e ON e.event_id = g.event_id
              WHERE NOT EXISTS (SELECT 1 FROM events p WHERE p.event_id = g.prev_event_id)
             ON CONFLICT DO NOTHING",
        )
        .execute(&self.dst).await?.rows_affected();
        println!(
            "current state: {n}, memberships: {nm}, forgotten: {}, extremities: {}, missing prevs: {missing}",
            forgotten.len(),
            ext.len()
        );
        Ok(())
    }

    async fn receipts(&self) -> anyhow::Result<()> {
        let rows = sqlx::query("SELECT room_id, receipt_type, user_id, event_id, data, thread_id FROM receipts_linearized WHERE room_id = ANY($1) ORDER BY stream_id")
            .bind(&self.rooms).fetch_all(&self.src).await?;
        let mut n = 0usize;
        for r in &rows {
            n += sqlx::query(
                "INSERT INTO receipts (room_id, user_id, receipt_type, thread_id, event_id, data, stream_id) VALUES ($1, $2, $3, $4, $5, $6, nextval('receipts_seq'))
                 ON CONFLICT (room_id, user_id, receipt_type, thread_id) DO UPDATE SET event_id = EXCLUDED.event_id, data = EXCLUDED.data, stream_id = EXCLUDED.stream_id",
            )
            .bind(r.get::<String, _>("room_id")).bind(r.get::<String, _>("user_id")).bind(r.get::<String, _>("receipt_type"))
            .bind(r.get::<Option<String>, _>("thread_id").unwrap_or_default()).bind(r.get::<String, _>("event_id"))
            .bind(to_jsonb(&r.get::<Option<String>, _>("data").unwrap_or_else(|| "{}".into())))
            .execute(&self.dst).await?.rows_affected() as usize;
        }
        println!("receipts: {n}");
        Ok(())
    }

    async fn media(&self, src: Option<&Path>) -> anyhow::Result<()> {
        let rows = sqlx::query(
            "SELECT media_id, media_type, media_length, created_ts, upload_name, user_id, quarantined_by, sha256
               FROM local_media_repository WHERE user_id = ANY($1)",
        )
        .bind(&self.active_users).fetch_all(&self.src).await?;
        let (mut n, mut linked, mut missing) = (0usize, 0usize, 0usize);
        let dst_dir = self.media_dir.join(safe_component(&self.server_name));
        if src.is_some() {
            std::fs::create_dir_all(&dst_dir)?;
        }
        for r in &rows {
            let media_id: String = r.get("media_id");
            n += sqlx::query(
                "INSERT INTO media (server_name, media_id, user_id, content_type, size, upload_name, sha256, created_ts, uploaded_ts, quarantined)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8, $9) ON CONFLICT DO NOTHING",
            )
            .bind(&self.server_name).bind(&media_id).bind(r.get::<Option<String>, _>("user_id")).bind(r.get::<Option<String>, _>("media_type"))
            .bind(r.get::<Option<i32>, _>("media_length").map(i64::from)).bind(r.get::<Option<String>, _>("upload_name")).bind(r.get::<Option<String>, _>("sha256"))
            .bind(r.get::<Option<i64>, _>("created_ts").unwrap_or(self.now)).bind(r.get::<Option<String>, _>("quarantined_by").is_some())
            .execute(&self.dst).await?.rows_affected() as usize;
            if let Some(src) = src
                && media_id.len() > 4
            {
                let from = src
                    .join("local_content")
                    .join(&media_id[..2])
                    .join(&media_id[2..4])
                    .join(&media_id[4..]);
                let to = dst_dir.join(safe_component(&media_id));
                if to.exists() {
                    linked += 1;
                } else if from.exists() {
                    if std::fs::hard_link(&from, &to).is_err() {
                        std::fs::copy(&from, &to)
                            .with_context(|| format!("copying {}", from.display()))?;
                    }
                    linked += 1;
                } else {
                    missing += 1;
                }
            }
        }
        println!("media: {n} rows, {linked} files linked, {missing} files missing");
        Ok(())
    }

    async fn signing_key(&self, path: &Path) -> anyhow::Result<()> {
        let text =
            std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        let mut n = 0usize;
        for line in text.lines() {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() != 3 || parts[0] != "ed25519" {
                continue;
            }
            // Synapse writes the seed as unpadded base64, as tessera does.
            let key_id = format!("ed25519:{}", parts[1]);
            n += sqlx::query("INSERT INTO server_signing_keys (key_id, seed_b64, created_ts, expired_ts) VALUES ($1, $2, $3, NULL) ON CONFLICT (key_id) DO UPDATE SET seed_b64 = EXCLUDED.seed_b64, expired_ts = NULL")
                .bind(&key_id).bind(parts[2]).bind(self.now).execute(&self.dst).await?.rows_affected() as usize;
        }
        if n == 0 {
            anyhow::bail!("no ed25519 key found in {}", path.display());
        }
        println!("signing keys: {n}");
        Ok(())
    }

    async fn sequences(&self) -> anyhow::Result<()> {
        let row = sqlx::query("SELECT COALESCE(max(stream_ordering), 0), COALESCE(min(stream_ordering), 0) FROM events").fetch_one(&self.dst).await?;
        let (max, min): (i64, i64) = (row.get(0), row.get(1));
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "ALTER SEQUENCE events_stream_seq RESTART WITH {}",
            max.max(0) + 1
        )))
        .execute(&self.dst)
        .await?;
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "ALTER SEQUENCE events_backfill_seq RESTART WITH {}",
            min.min(0) - 1
        )))
        .execute(&self.dst)
        .await?;
        println!(
            "sequences: live events continue at {}, backfill at {}",
            max + 1,
            min - 1
        );
        Ok(())
    }
}

fn safe_component(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_') {
                c
            } else {
                '_'
            }
        })
        .collect()
}

#[allow(dead_code)]
fn _room_id_check(s: &str) -> Option<OwnedRoomId> {
    OwnedRoomId::try_from(s).ok()
}
