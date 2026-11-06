use async_trait::async_trait;
use ruma::{EventId, OwnedEventId, OwnedServerName, RoomId, ServerName, UserId};
use sqlx::Row;

use super::{PostgresStore, strings};
use crate::{
    DestinationState, DiscoveryEntry, Error, FederationStore, OutboxItem, OutboxKind, Result, Ts,
    now_ts,
};

#[async_trait]
impl FederationStore for PostgresStore {
    async fn enqueue_pdu(
        &self,
        destinations: &[OwnedServerName],
        event_id: &EventId,
    ) -> Result<()> {
        if destinations.is_empty() {
            return Ok(());
        }
        sqlx::query(
            "INSERT INTO federation_outbox (destination, kind, event_id, created_ts)
             SELECT d, 'pdu', $2, $3 FROM UNNEST($1::text[]) AS d",
        )
        .bind(strings(destinations))
        .bind(event_id.as_str())
        .bind(now_ts())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn enqueue_edu(
        &self,
        destinations: &[OwnedServerName],
        edu: &serde_json::Value,
    ) -> Result<()> {
        if destinations.is_empty() {
            return Ok(());
        }
        // Presence and typing describe a current state: a newer update for the same user (or
        // room) makes a queued older one worthless, so replace it instead of queueing behind it.
        // Without this a user whose presence changed quickly could pile up an unbounded number
        // of updates per destination.
        let edu_type = edu.get("edu_type").and_then(|t| t.as_str());
        let superseded_by = match edu_type {
            Some("m.presence") => edu
                .pointer("/content/push/0/user_id")
                .and_then(|u| u.as_str())
                .map(|u| ("/content/push/0/user_id", u)),
            Some("m.typing") => edu
                .pointer("/content/room_id")
                .and_then(|r| r.as_str())
                .map(|r| ("/content/room_id", r)),
            _ => None,
        };
        if let (Some(edu_type), Some((path, value))) = (edu_type, superseded_by) {
            sqlx::query(
                "DELETE FROM federation_outbox
                 WHERE destination = ANY($1) AND kind = 'edu'
                   AND edu_json->>'edu_type' = $2 AND edu_json #>> $3 = $4",
            )
            .bind(strings(destinations))
            .bind(edu_type)
            .bind(json_path(path))
            .bind(value)
            .execute(&self.pool)
            .await?;
        }
        sqlx::query(
            "INSERT INTO federation_outbox (destination, kind, edu_json, created_ts)
             SELECT d, 'edu', $2, $3 FROM UNNEST($1::text[]) AS d",
        )
        .bind(strings(destinations))
        .bind(edu)
        .bind(now_ts())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn outbox_for(
        &self,
        destination: &ServerName,
        max_pdus: usize,
        max_edus: usize,
        first_edu_types: &[&str],
    ) -> Result<Vec<OutboxItem>> {
        let limit = |n: usize| i64::try_from(n).unwrap_or(i64::MAX);
        let mut rows = sqlx::query(
            "SELECT id, kind, event_id, edu_json, created_ts FROM federation_outbox
             WHERE destination = $1 AND kind = 'pdu' ORDER BY id LIMIT $2",
        )
        .bind(destination.as_str())
        .bind(limit(max_pdus))
        .fetch_all(&self.pool)
        .await?;
        rows.extend(
            sqlx::query(
                "SELECT id, kind, event_id, edu_json, created_ts FROM federation_outbox
                 WHERE destination = $1 AND kind = 'edu'
                 ORDER BY (edu_json->>'edu_type' = ANY($3)) IS NOT TRUE, id LIMIT $2",
            )
            .bind(destination.as_str())
            .bind(limit(max_edus))
            .bind(first_edu_types)
            .fetch_all(&self.pool)
            .await?,
        );
        rows.into_iter()
            .map(|r| {
                let kind: String = r.get("kind");
                let kind = if kind == "pdu" {
                    OutboxKind::Pdu(
                        OwnedEventId::try_from(r.get::<String, _>("event_id"))
                            .map_err(|e| Error::Corrupt(e.to_string()))?,
                    )
                } else {
                    OutboxKind::Edu(r.get::<serde_json::Value, _>("edu_json"))
                };
                Ok(OutboxItem {
                    id: r.get("id"),
                    destination: destination.to_owned(),
                    kind,
                    created_ts: r.get("created_ts"),
                })
            })
            .collect()
    }

    async fn delete_outbox_items(&self, ids: &[i64]) -> Result<()> {
        if ids.is_empty() {
            return Ok(());
        }
        sqlx::query("DELETE FROM federation_outbox WHERE id = ANY($1)")
            .bind(ids)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn outbox_destinations(&self) -> Result<Vec<OwnedServerName>> {
        let rows = sqlx::query("SELECT DISTINCT destination FROM federation_outbox")
            .fetch_all(&self.pool)
            .await?;
        Ok(crate::parse_ids(
            rows.into_iter().map(|r| r.get::<String, _>(0)).collect(),
        ))
    }

    async fn prune_dead_outbox(&self, failing_since: Ts) -> Result<u64> {
        let done = sqlx::query(
            "WITH dead AS (
                 SELECT destination FROM federation_destinations
                 WHERE failure_ts IS NOT NULL AND failure_ts < $1
             ), keep AS (
                 SELECT max(o.id) AS id
                 FROM federation_outbox o
                 JOIN dead ON dead.destination = o.destination
                 JOIN events e ON e.event_id = o.event_id
                 WHERE o.kind = 'pdu'
                 GROUP BY o.destination, e.room_id
             )
             DELETE FROM federation_outbox o USING dead
             WHERE o.destination = dead.destination
               AND NOT EXISTS (SELECT 1 FROM keep WHERE keep.id = o.id)",
        )
        .bind(failing_since)
        .execute(&self.pool)
        .await?;
        Ok(done.rows_affected())
    }

    async fn destination_state(&self, destination: &ServerName) -> Result<DestinationState> {
        let row = sqlx::query(
            "SELECT failure_ts, retry_last_ts, retry_interval_ms, last_successful_ts FROM federation_destinations WHERE destination = $1",
        )
        .bind(destination.as_str())
        .fetch_optional(&self.pool)
        .await?;
        Ok(row
            .map(|r| DestinationState {
                failure_ts: r.get("failure_ts"),
                retry_last_ts: r.get("retry_last_ts"),
                retry_interval_ms: r.get("retry_interval_ms"),
                last_successful_ts: r.get("last_successful_ts"),
            })
            .unwrap_or_default())
    }

    async fn set_destination_state(
        &self,
        destination: &ServerName,
        state: &DestinationState,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO federation_destinations (destination, failure_ts, retry_last_ts, retry_interval_ms, last_successful_ts)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (destination) DO UPDATE SET failure_ts = EXCLUDED.failure_ts, retry_last_ts = EXCLUDED.retry_last_ts,
                retry_interval_ms = EXCLUDED.retry_interval_ms, last_successful_ts = EXCLUDED.last_successful_ts",
        )
        .bind(destination.as_str())
        .bind(state.failure_ts)
        .bind(state.retry_last_ts)
        .bind(state.retry_interval_ms)
        .bind(state.last_successful_ts)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn received_transaction(
        &self,
        origin: &ServerName,
        txn_id: &str,
    ) -> Result<Option<serde_json::Value>> {
        let row = sqlx::query("SELECT response FROM federation_received_transactions WHERE origin = $1 AND transaction_id = $2")
            .bind(origin.as_str())
            .bind(txn_id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(|r| r.get::<serde_json::Value, _>(0)))
    }

    async fn store_received_transaction(
        &self,
        origin: &ServerName,
        txn_id: &str,
        response: &serde_json::Value,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO federation_received_transactions (origin, transaction_id, response, received_ts) VALUES ($1, $2, $3, $4)
             ON CONFLICT (origin, transaction_id) DO UPDATE SET response = EXCLUDED.response",
        )
        .bind(origin.as_str())
        .bind(txn_id)
        .bind(response)
        .bind(now_ts())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn prune_received_transactions(&self, before: Ts) -> Result<u64> {
        let done =
            sqlx::query("DELETE FROM federation_received_transactions WHERE received_ts < $1")
                .bind(before)
                .execute(&self.pool)
                .await?;
        Ok(done.rows_affected())
    }

    async fn get_discovery(&self, server_name: &ServerName) -> Result<Option<DiscoveryEntry>> {
        let row = sqlx::query(
            "SELECT host, port, host_header, tls_name, expires_ts FROM federation_server_discovery WHERE server_name = $1",
        )
        .bind(server_name.as_str())
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| DiscoveryEntry {
            server_name: server_name.to_owned(),
            host: r.get("host"),
            port: u16::try_from(r.get::<i32, _>("port")).unwrap_or(8448),
            host_header: r.get("host_header"),
            tls_name: r.get("tls_name"),
            expires_ts: r.get("expires_ts"),
        }))
    }

    async fn set_discovery(&self, e: &DiscoveryEntry) -> Result<()> {
        sqlx::query(
            "INSERT INTO federation_server_discovery (server_name, host, port, host_header, tls_name, expires_ts)
             VALUES ($1, $2, $3, $4, $5, $6)
             ON CONFLICT (server_name) DO UPDATE SET host = EXCLUDED.host, port = EXCLUDED.port,
                host_header = EXCLUDED.host_header, tls_name = EXCLUDED.tls_name, expires_ts = EXCLUDED.expires_ts",
        )
        .bind(e.server_name.as_str())
        .bind(&e.host)
        .bind(i32::from(e.port))
        .bind(&e.host_header)
        .bind(&e.tls_name)
        .bind(e.expires_ts)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn set_invite_state(
        &self,
        room_id: &RoomId,
        user_id: &UserId,
        state: &[serde_json::Value],
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO invite_state (room_id, user_id, state) VALUES ($1, $2, $3)
             ON CONFLICT (room_id, user_id) DO UPDATE SET state = EXCLUDED.state",
        )
        .bind(room_id.as_str())
        .bind(user_id.as_str())
        .bind(serde_json::Value::Array(state.to_vec()))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn get_invite_state(
        &self,
        room_id: &RoomId,
        user_id: &UserId,
    ) -> Result<Option<Vec<serde_json::Value>>> {
        let row = sqlx::query("SELECT state FROM invite_state WHERE room_id = $1 AND user_id = $2")
            .bind(room_id.as_str())
            .bind(user_id.as_str())
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.and_then(|r| r.get::<serde_json::Value, _>(0).as_array().cloned()))
    }

    async fn delete_invite_state(&self, room_id: &RoomId, user_id: &UserId) -> Result<()> {
        sqlx::query("DELETE FROM invite_state WHERE room_id = $1 AND user_id = $2")
            .bind(room_id.as_str())
            .bind(user_id.as_str())
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}

/// A JSON pointer (`/content/push/0/user_id`) as the `text[]` path PostgreSQL's `#>>` takes.
fn json_path(pointer: &str) -> Vec<String> {
    pointer
        .split('/')
        .filter(|p| !p.is_empty())
        .map(str::to_owned)
        .collect()
}
