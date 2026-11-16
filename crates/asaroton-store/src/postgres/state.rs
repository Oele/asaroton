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
    // One row per state key (a later entry wins).
    let delta: Vec<_> = delta
        .iter()
        .cloned()
        .collect::<std::collections::BTreeMap<_, _>>()
        .into_iter()
        .collect();
    let types: Vec<String> = delta.iter().map(|((t, _), _)| t.to_string()).collect();
    let keys: Vec<String> = delta.iter().map(|((_, k), _)| k.clone()).collect();
    let ids: Vec<String> = delta.iter().map(|(_, id)| id.to_string()).collect();
    let id: i64 = sqlx::query(
        "WITH g AS (INSERT INTO state_groups (room_id, prev_group) VALUES ($1, $2) RETURNING id),
              s AS (
                INSERT INTO state_groups_state (state_group, event_type, state_key, event_id)
                SELECT g.id, t, k, e FROM g, UNNEST($3::text[], $4::text[], $5::text[]) AS u(t, k, e)
              )
         SELECT id FROM g",
    )
    .bind(room_id.as_str())
    .bind(prev)
    .bind(&types)
    .bind(&keys)
    .bind(&ids)
    .fetch_one(&mut *conn)
    .await?
    .get(0);
    Ok(id)
}

pub(crate) async fn apply_current_state_delta(
    conn: &mut PgConnection,
    room_id: &RoomId,
    delta: &CurrentStateDelta,
) -> Result<()> {
    // One row per key, last entry wins; a key both set and removed ends up removed.
    let upserts: std::collections::BTreeMap<_, _> = delta
        .upsert
        .iter()
        .filter(|(key, _)| !delta.remove.contains(key))
        .cloned()
        .collect();
    let up_types: Vec<String> = upserts.keys().map(|(t, _)| t.to_string()).collect();
    let up_keys: Vec<String> = upserts.keys().map(|(_, k)| k.clone()).collect();
    let up_ids: Vec<String> = upserts.values().map(ToString::to_string).collect();
    let rm_types: Vec<String> = delta.remove.iter().map(|(t, _)| t.to_string()).collect();
    let rm_keys: Vec<String> = delta.remove.iter().map(|(_, k)| k.clone()).collect();
    // Membership rows are denormalised from the member events, read here and not by casting
    // their JSON to jsonb in SQL: jsonb cannot hold a NUL character, and one anywhere in a
    // state event (a display name, a topic) made the whole statement fail and the room stall
    // (tracker issue 67). The text columns get the strings without NUL.
    let member_ids: Vec<&str> = upserts
        .iter()
        .filter(|((t, _), _)| t.to_string() == "m.room.member")
        .map(|(_, id)| id.as_str())
        .collect();
    let rows = if member_ids.is_empty() {
        Vec::new()
    } else {
        sqlx::query(
            "SELECT event_id, json, sender, stream_ordering FROM events WHERE event_id = ANY($1)",
        )
        .bind(&member_ids)
        .fetch_all(&mut *conn)
        .await?
    };
    let no_nul = |s: &str| s.replace('\0', "");
    let (mut m_ids, mut m_memberships, mut m_senders) = (Vec::new(), Vec::new(), Vec::new());
    let (mut m_names, mut m_avatars, mut m_orderings) = (Vec::new(), Vec::new(), Vec::new());
    for row in &rows {
        let json: String = row.get("json");
        let Ok(event) = serde_json::from_str::<serde_json::Value>(&json) else {
            continue;
        };
        let content = event.get("content");
        let text = |key: &str| {
            content
                .and_then(|c| c.get(key))
                .and_then(serde_json::Value::as_str)
                .map(no_nul)
        };
        // As before: a member event whose membership is not a string has no membership row.
        let Some(membership) = text("membership") else {
            continue;
        };
        m_ids.push(row.get::<String, _>("event_id"));
        m_memberships.push(membership);
        m_senders.push(row.get::<String, _>("sender"));
        m_names.push(text("displayname"));
        m_avatars.push(text("avatar_url"));
        m_orderings.push(row.get::<i64, _>("stream_ordering"));
    }
    sqlx::query(
        "WITH up AS (
            SELECT * FROM UNNEST($2::text[], $3::text[], $4::text[]) AS u(event_type, state_key, event_id)
         ),
         m AS (
            SELECT * FROM UNNEST($7::text[], $8::text[], $9::text[], $10::text[], $11::text[], $12::bigint[])
                AS m(event_id, membership, sender, display_name, avatar_url, stream_ordering)
         ),
         mem AS (
            INSERT INTO room_memberships (room_id, user_id, membership, event_id, sender, display_name, avatar_url, stream_ordering)
            SELECT $1, up.state_key, m.membership, m.event_id, m.sender, m.display_name, m.avatar_url, m.stream_ordering
            FROM up JOIN m ON m.event_id = up.event_id
            WHERE up.event_type = 'm.room.member'
            ON CONFLICT (room_id, user_id) DO UPDATE SET membership = EXCLUDED.membership,
                event_id = EXCLUDED.event_id, sender = EXCLUDED.sender,
                display_name = EXCLUDED.display_name, avatar_url = EXCLUDED.avatar_url,
                stream_ordering = EXCLUDED.stream_ordering, forgotten = FALSE
         ),
         cur AS (
            INSERT INTO current_state_events (room_id, event_type, state_key, event_id, membership)
            SELECT $1, up.event_type, up.state_key, up.event_id,
                   CASE WHEN up.event_type = 'm.room.member' THEN
                        (SELECT m.membership FROM m WHERE m.event_id = up.event_id)
                   END
            FROM up
            ON CONFLICT (room_id, event_type, state_key) DO UPDATE SET event_id = EXCLUDED.event_id,
                membership = EXCLUDED.membership
         ),
         gone AS (
            DELETE FROM current_state_events
            WHERE room_id = $1 AND (event_type, state_key) IN (SELECT t, k FROM UNNEST($5::text[], $6::text[]) AS r(t, k))
         )
         DELETE FROM room_memberships
         WHERE room_id = $1 AND user_id IN (SELECT k FROM UNNEST($5::text[], $6::text[]) AS r(t, k) WHERE t = 'm.room.member')",
    )
    .bind(room_id.as_str())
    .bind(&up_types)
    .bind(&up_keys)
    .bind(&up_ids)
    .bind(&rm_types)
    .bind(&rm_keys)
    .bind(&m_ids)
    .bind(&m_memberships)
    .bind(&m_senders)
    .bind(&m_names)
    .bind(&m_avatars)
    .bind(&m_orderings)
    .execute(&mut *conn)
    .await?;
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
        let mut tx = self.begin().await?;
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

    async fn current_state_pdus_for(
        &self,
        room_id: &RoomId,
        keys: &[(StateEventType, String)],
    ) -> Result<Vec<StoredEvent>> {
        let types: Vec<String> = keys.iter().map(|(t, _)| t.to_string()).collect();
        let state_keys: Vec<String> = keys.iter().map(|(_, k)| k.clone()).collect();
        let rows: Vec<EventRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT {EVENT_COLUMNS} FROM current_state_events c
             JOIN events e ON e.event_id = c.event_id JOIN rooms r ON r.room_id = e.room_id
             WHERE c.room_id = $1
               AND (c.event_type, c.state_key) IN (SELECT t, k FROM UNNEST($2::text[], $3::text[]) AS u(t, k))"
        )))
        .bind(room_id.as_str())
        .bind(&types)
        .bind(&state_keys)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(StoredEvent::try_from).collect()
    }

    async fn apply_current_state_delta(
        &self,
        room_id: &RoomId,
        delta: &CurrentStateDelta,
    ) -> Result<()> {
        let mut tx = self.begin().await?;
        apply_current_state_delta(&mut tx, room_id, delta).await?;
        tx.commit().await?;
        Ok(())
    }
}
