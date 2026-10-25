use async_trait::async_trait;
use ruma::{EventId, OwnedEventId, RoomId, events::StateEventType};
use sqlx::{PgConnection, Row};

use super::{EVENT_COLUMNS, EventRow, PostgresStore};
use crate::{
    CurrentStateDelta, CurrentStateEntry, Error, Result, StateDelta, StateGroupId, StateStore,
    StoredEvent,
};

/// Per-row of state as `(type, state_key, event_id)`.
fn entry_from_row(r: &sqlx::postgres::PgRow) -> Result<CurrentStateEntry> {
    Ok(CurrentStateEntry {
        event_type: StateEventType::from(r.get::<String, _>("event_type")),
        state_key: r.get("state_key"),
        event_id: OwnedEventId::try_from(r.get::<String, _>("event_id"))
            .map_err(|e| Error::Corrupt(e.to_string()))?,
    })
}

/// Recursive CTE walking a state group chain; nearest group wins per `(type, state_key)`.
const STATE_AT_GROUP: &str = "
    WITH RECURSIVE chain AS (
        SELECT id, prev_group, 0 AS dist FROM state_groups WHERE id = $1
        UNION ALL
        SELECT sg.id, sg.prev_group, c.dist + 1 FROM state_groups sg JOIN chain c ON sg.id = c.prev_group
    )
    SELECT DISTINCT ON (s.event_type, s.state_key) s.event_type, s.state_key, s.event_id
    FROM chain JOIN state_groups_state s ON s.state_group = chain.id
    ORDER BY s.event_type, s.state_key, chain.dist ASC";

pub(crate) async fn create_state_group(
    conn: &mut PgConnection,
    room_id: &RoomId,
    prev: Option<StateGroupId>,
    delta: &StateDelta,
) -> Result<StateGroupId> {
    let id: i64 =
        sqlx::query("INSERT INTO state_groups (room_id, prev_group) VALUES ($1, $2) RETURNING id")
            .bind(room_id.as_str())
            .bind(prev)
            .fetch_one(&mut *conn)
            .await?
            .get(0);
    if !delta.is_empty() {
        let types: Vec<String> = delta.iter().map(|((t, _), _)| t.to_string()).collect();
        let keys: Vec<String> = delta.iter().map(|((_, k), _)| k.clone()).collect();
        let ids: Vec<String> = delta.iter().map(|(_, id)| id.to_string()).collect();
        sqlx::query(
            "INSERT INTO state_groups_state (state_group, event_type, state_key, event_id)
             SELECT $1, * FROM UNNEST($2::text[], $3::text[], $4::text[])
             ON CONFLICT (state_group, event_type, state_key) DO UPDATE SET event_id = EXCLUDED.event_id",
        )
        .bind(id)
        .bind(&types)
        .bind(&keys)
        .bind(&ids)
        .execute(&mut *conn)
        .await?;
    }
    Ok(id)
}

/// Membership details pulled from an `m.room.member` event's JSON.
struct MemberContent {
    membership: String,
    display_name: Option<String>,
    avatar_url: Option<String>,
}

fn member_content(json: &str) -> Option<MemberContent> {
    let v: serde_json::Value = serde_json::from_str(json).ok()?;
    let c = v.get("content")?;
    Some(MemberContent {
        membership: c.get("membership")?.as_str()?.to_owned(),
        display_name: c
            .get("displayname")
            .and_then(|d| d.as_str())
            .map(str::to_owned),
        avatar_url: c
            .get("avatar_url")
            .and_then(|d| d.as_str())
            .map(str::to_owned),
    })
}

pub(crate) async fn apply_current_state_delta(
    conn: &mut PgConnection,
    room_id: &RoomId,
    delta: &CurrentStateDelta,
) -> Result<()> {
    for ((event_type, state_key), event_id) in &delta.upsert {
        let mut membership: Option<String> = None;
        if *event_type == StateEventType::RoomMember {
            let row =
                sqlx::query("SELECT json, sender, stream_ordering FROM events WHERE event_id = $1")
                    .bind(event_id.as_str())
                    .fetch_optional(&mut *conn)
                    .await?;
            if let Some(row) = row
                && let Some(mc) = member_content(&row.get::<String, _>("json"))
            {
                membership = Some(mc.membership.clone());
                sqlx::query(
                        "INSERT INTO room_memberships (room_id, user_id, membership, event_id, sender, display_name, avatar_url, stream_ordering)
                         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                         ON CONFLICT (room_id, user_id) DO UPDATE SET membership = EXCLUDED.membership,
                            event_id = EXCLUDED.event_id, sender = EXCLUDED.sender,
                            display_name = EXCLUDED.display_name, avatar_url = EXCLUDED.avatar_url,
                            stream_ordering = EXCLUDED.stream_ordering, forgotten = FALSE",
                    )
                    .bind(room_id.as_str())
                    .bind(state_key)
                    .bind(&mc.membership)
                    .bind(event_id.as_str())
                    .bind(row.get::<String, _>("sender"))
                    .bind(&mc.display_name)
                    .bind(&mc.avatar_url)
                    .bind(row.get::<i64, _>("stream_ordering"))
                    .execute(&mut *conn)
                    .await?;
            }
        }
        sqlx::query(
            "INSERT INTO current_state_events (room_id, event_type, state_key, event_id, membership)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (room_id, event_type, state_key) DO UPDATE SET event_id = EXCLUDED.event_id, membership = EXCLUDED.membership",
        )
        .bind(room_id.as_str())
        .bind(event_type.to_string())
        .bind(state_key)
        .bind(event_id.as_str())
        .bind(&membership)
        .execute(&mut *conn)
        .await?;
    }
    for (event_type, state_key) in &delta.remove {
        sqlx::query("DELETE FROM current_state_events WHERE room_id = $1 AND event_type = $2 AND state_key = $3")
            .bind(room_id.as_str())
            .bind(event_type.to_string())
            .bind(state_key)
            .execute(&mut *conn)
            .await?;
        if *event_type == StateEventType::RoomMember {
            sqlx::query("DELETE FROM room_memberships WHERE room_id = $1 AND user_id = $2")
                .bind(room_id.as_str())
                .bind(state_key)
                .execute(&mut *conn)
                .await?;
        }
    }
    Ok(())
}

#[async_trait]
impl StateStore for PostgresStore {
    async fn state_at_group(&self, group: StateGroupId) -> Result<Vec<CurrentStateEntry>> {
        let rows = sqlx::query(STATE_AT_GROUP)
            .bind(group)
            .fetch_all(&self.pool)
            .await?;
        rows.iter().map(entry_from_row).collect()
    }

    async fn state_at_group_filtered(
        &self,
        group: StateGroupId,
        keys: &[(StateEventType, String)],
    ) -> Result<Vec<CurrentStateEntry>> {
        let types: Vec<String> = keys.iter().map(|(t, _)| t.to_string()).collect();
        let state_keys: Vec<String> = keys.iter().map(|(_, k)| k.clone()).collect();
        let sql = STATE_AT_GROUP.replace(
                "FROM chain JOIN state_groups_state s ON s.state_group = chain.id",
                "FROM chain JOIN state_groups_state s ON s.state_group = chain.id
                 JOIN UNNEST($2::text[], $3::text[]) AS want(t, k) ON want.t = s.event_type AND want.k = s.state_key"
            );
        let rows = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(group)
            .bind(&types)
            .bind(&state_keys)
            .fetch_all(&self.pool)
            .await?;
        rows.iter().map(entry_from_row).collect()
    }

    async fn state_group_before(&self, event_id: &EventId) -> Result<Option<StateGroupId>> {
        let row = sqlx::query(
            "SELECT e.state_key, e.state_group, sg.prev_group FROM events e
             LEFT JOIN state_groups sg ON sg.id = e.state_group WHERE e.event_id = $1",
        )
        .bind(event_id.as_str())
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.and_then(|r| {
            let group: Option<i64> = r.get("state_group");
            let is_state = r.get::<Option<String>, _>("state_key").is_some();
            if is_state {
                r.get::<Option<i64>, _>("prev_group")
            } else {
                group
            }
        }))
    }

    async fn create_state_group(
        &self,
        room_id: &RoomId,
        prev: Option<StateGroupId>,
        delta: &StateDelta,
    ) -> Result<StateGroupId> {
        let mut tx = self.pool.begin().await?;
        let id = create_state_group(&mut tx, room_id, prev, delta).await?;
        tx.commit().await?;
        Ok(id)
    }

    async fn current_state(&self, room_id: &RoomId) -> Result<Vec<CurrentStateEntry>> {
        let rows = sqlx::query(
            "SELECT event_type, state_key, event_id FROM current_state_events WHERE room_id = $1",
        )
        .bind(room_id.as_str())
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(entry_from_row).collect()
    }

    async fn current_state_event(
        &self,
        room_id: &RoomId,
        event_type: &StateEventType,
        state_key: &str,
    ) -> Result<Option<OwnedEventId>> {
        let row = sqlx::query(
            "SELECT event_id FROM current_state_events WHERE room_id = $1 AND event_type = $2 AND state_key = $3",
        )
        .bind(room_id.as_str())
        .bind(event_type.to_string())
        .bind(state_key)
        .fetch_optional(&self.pool)
        .await?;
        row.map(|r| {
            OwnedEventId::try_from(r.get::<String, _>(0)).map_err(|e| Error::Corrupt(e.to_string()))
        })
        .transpose()
    }

    async fn current_state_pdus(&self, room_id: &RoomId) -> Result<Vec<StoredEvent>> {
        let rows: Vec<EventRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT {EVENT_COLUMNS} FROM current_state_events c
             JOIN events e ON e.event_id = c.event_id JOIN rooms r ON r.room_id = e.room_id
             WHERE c.room_id = $1"
        )))
        .bind(room_id.as_str())
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(StoredEvent::try_from).collect()
    }

    async fn apply_current_state_delta(
        &self,
        room_id: &RoomId,
        delta: &CurrentStateDelta,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        apply_current_state_delta(&mut tx, room_id, delta).await?;
        tx.commit().await?;
        Ok(())
    }
}
