use async_trait::async_trait;
use ruma::{EventId, OwnedEventId, OwnedRoomId, RoomId, UserId};
use sqlx::{FromRow, PgConnection, Row};
use tessera_core::Pdu;

use super::{EVENT_COLUMNS, EventRow, PostgresStore, strings};
use crate::{
    Direction, Error, EventState, EventStore, PersistEvent, PersistedEvent, RelationRow, Result,
    SearchHit, StoredEvent, StreamOrdering, ThreadSummary, now_ts, parse_ids, uint_to_i64,
};

async fn persist_one(conn: &mut PgConnection, ev: PersistEvent) -> Result<PersistedEvent> {
    let pdu = &ev.pdu;
    let room_id = pdu.room_id();

    // Idempotency: an event we already have is not written twice.
    if let Some(row) =
        sqlx::query("SELECT stream_ordering, state_group, outlier FROM events WHERE event_id = $1")
            .bind(pdu.event_id().as_str())
            .fetch_optional(&mut *conn)
            .await?
    {
        let existing = PersistedEvent {
            event_id: pdu.event_id().to_owned(),
            stream_ordering: row.get("stream_ordering"),
            state_group: row.get("state_group"),
        };
        // An outlier being persisted again with state is "de-outliered".
        if row.get::<bool, _>("outlier") && !matches!(ev.state, EventState::Outlier) {
            let group = resolve_state_group(&mut *conn, room_id, &ev.state).await?;
            sqlx::query("UPDATE events SET outlier = FALSE, state_group = $2, soft_failed = $3 WHERE event_id = $1")
                .bind(pdu.event_id().as_str())
                .bind(group)
                .bind(ev.soft_failed)
                .execute(&mut *conn)
                .await?;
            if let Some(delta) = &ev.current_state_delta {
                super::state::apply_current_state_delta(&mut *conn, room_id, delta).await?;
            }
            if let Some(ext) = &ev.forward_extremities {
                set_forward_extremities(&mut *conn, room_id, ext).await?;
            }
            return Ok(PersistedEvent {
                state_group: group,
                ..existing
            });
        }
        return Ok(existing);
    }

    let state_group = resolve_state_group(&mut *conn, room_id, &ev.state).await?;
    let outlier = matches!(ev.state, EventState::Outlier);
    let seq = if ev.backfilled {
        "events_backfill_seq"
    } else {
        "events_stream_seq"
    };
    let stream_ordering: i64 = sqlx::query(sqlx::AssertSqlSafe(format!("SELECT nextval('{seq}')")))
        .fetch_one(&mut *conn)
        .await?
        .get(0);

    let mut json_obj = pdu.raw().clone();
    json_obj.remove("unsigned");
    let json = tessera_core::canonical::to_string(&json_obj);

    sqlx::query(
        "INSERT INTO events (stream_ordering, event_id, room_id, event_type, state_key, sender,
            origin_server_ts, depth, json, state_group, rejected, soft_failed, outlier, received_ts)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)",
    )
    .bind(stream_ordering)
    .bind(pdu.event_id().as_str())
    .bind(room_id.as_str())
    .bind(pdu.event_type().to_string())
    .bind(pdu.state_key())
    .bind(pdu.sender().as_str())
    .bind(uint_to_i64(pdu.origin_server_ts().0))
    .bind(uint_to_i64(pdu.depth()))
    .bind(&json)
    .bind(state_group)
    .bind(pdu.rejected())
    .bind(ev.soft_failed)
    .bind(outlier)
    .bind(now_ts())
    .execute(&mut *conn)
    .await?;

    let prev = strings(pdu.prev_events());
    if !prev.is_empty() {
        sqlx::query("INSERT INTO event_edges (event_id, prev_event_id) SELECT $1, * FROM UNNEST($2::text[]) ON CONFLICT DO NOTHING")
            .bind(pdu.event_id().as_str())
            .bind(&prev)
            .execute(&mut *conn)
            .await?;
        // Track prev events we do not have.
        sqlx::query(
            "INSERT INTO missing_events (room_id, event_id, referenced_by)
             SELECT $1, p, $2 FROM UNNEST($3::text[]) AS p WHERE NOT EXISTS (SELECT 1 FROM events WHERE event_id = p)
             ON CONFLICT DO NOTHING",
        )
        .bind(room_id.as_str())
        .bind(pdu.event_id().as_str())
        .bind(&prev)
        .execute(&mut *conn)
        .await?;
    }
    if !pdu.rejected() {
        index_relation_and_search(&mut *conn, pdu, stream_ordering).await?;
    }
    let auth = strings(pdu.auth_events());
    if !auth.is_empty() {
        sqlx::query("INSERT INTO event_auth (event_id, auth_event_id) SELECT $1, * FROM UNNEST($2::text[]) ON CONFLICT DO NOTHING")
            .bind(pdu.event_id().as_str())
            .bind(&auth)
            .execute(&mut *conn)
            .await?;
    }
    // This event is no longer missing.
    sqlx::query("DELETE FROM missing_events WHERE event_id = $1")
        .bind(pdu.event_id().as_str())
        .execute(&mut *conn)
        .await?;

    if let Some(delta) = &ev.current_state_delta {
        super::state::apply_current_state_delta(&mut *conn, room_id, delta).await?;
    }
    if let Some(ext) = &ev.forward_extremities {
        set_forward_extremities(&mut *conn, room_id, ext).await?;
    }

    Ok(PersistedEvent {
        event_id: pdu.event_id().to_owned(),
        stream_ordering,
        state_group,
    })
}

async fn resolve_state_group(
    conn: &mut PgConnection,
    room_id: &RoomId,
    state: &EventState,
) -> Result<Option<i64>> {
    Ok(match state {
        EventState::Outlier => None,
        EventState::Existing(g) => Some(*g),
        EventState::New { prev, delta } => {
            Some(super::state::create_state_group(&mut *conn, room_id, *prev, delta).await?)
        }
    })
}

async fn set_forward_extremities(
    conn: &mut PgConnection,
    room_id: &RoomId,
    event_ids: &[OwnedEventId],
) -> Result<()> {
    sqlx::query("DELETE FROM forward_extremities WHERE room_id = $1")
        .bind(room_id.as_str())
        .execute(&mut *conn)
        .await?;
    if !event_ids.is_empty() {
        sqlx::query("INSERT INTO forward_extremities (room_id, event_id) SELECT $1, * FROM UNNEST($2::text[])")
            .bind(room_id.as_str())
            .bind(strings(event_ids))
            .execute(&mut *conn)
            .await?;
    }
    Ok(())
}

fn ids_from_rows(rows: Vec<sqlx::postgres::PgRow>) -> Vec<OwnedEventId> {
    parse_ids(rows.into_iter().map(|r| r.get::<String, _>(0)).collect())
}

#[async_trait]
impl EventStore for PostgresStore {
    async fn persist_event(&self, event: PersistEvent) -> Result<PersistedEvent> {
        let mut tx = self.pool.begin().await?;
        let out = persist_one(&mut tx, event).await?;
        tx.commit().await?;
        Ok(out)
    }

    async fn persist_events(&self, events: Vec<PersistEvent>) -> Result<Vec<PersistedEvent>> {
        let mut tx = self.pool.begin().await?;
        let mut out = Vec::with_capacity(events.len());
        for ev in events {
            out.push(persist_one(&mut tx, ev).await?);
        }
        tx.commit().await?;
        Ok(out)
    }

    async fn get_event(&self, event_id: &EventId) -> Result<Option<StoredEvent>> {
        let row: Option<EventRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT {EVENT_COLUMNS} FROM events e JOIN rooms r ON r.room_id = e.room_id WHERE e.event_id = $1"
        )))
        .bind(event_id.as_str())
        .fetch_optional(&self.pool)
        .await?;
        row.map(StoredEvent::try_from).transpose()
    }

    async fn get_events(&self, event_ids: &[OwnedEventId]) -> Result<Vec<StoredEvent>> {
        let rows: Vec<EventRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT {EVENT_COLUMNS} FROM events e JOIN rooms r ON r.room_id = e.room_id WHERE e.event_id = ANY($1)"
        )))
        .bind(strings(event_ids))
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(StoredEvent::try_from).collect()
    }

    async fn have_events(&self, event_ids: &[OwnedEventId]) -> Result<Vec<OwnedEventId>> {
        let rows = sqlx::query("SELECT event_id FROM events WHERE event_id = ANY($1)")
            .bind(strings(event_ids))
            .fetch_all(&self.pool)
            .await?;
        Ok(ids_from_rows(rows))
    }

    async fn redact_event(&self, event_id: &EventId, redacted: &Pdu) -> Result<()> {
        // The redacted form carries `unsigned.redacted_because`, which clients need; keep it.
        let json = tessera_core::canonical::to_string(redacted.raw());
        sqlx::query(
            "UPDATE events SET original_json = COALESCE(original_json, json), json = $2 WHERE event_id = $1",
        )
        .bind(event_id.as_str())
        .bind(&json)
        .execute(&self.pool)
        .await?;
        sqlx::query("DELETE FROM event_relations WHERE event_id = $1")
            .bind(event_id.as_str())
            .execute(&self.pool)
            .await?;
        sqlx::query("DELETE FROM event_search WHERE event_id = $1")
            .bind(event_id.as_str())
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn relations(
        &self,
        room_id: &RoomId,
        event_id: &EventId,
        rel_type: Option<&str>,
        event_type: Option<&str>,
        recurse: bool,
        from: Option<StreamOrdering>,
        to: Option<StreamOrdering>,
        direction: Direction,
        limit: usize,
    ) -> Result<Vec<StoredEvent>> {
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let (cmp_from, cmp_to, order) = match direction {
            Direction::Forwards => (">", "<=", "ASC"),
            Direction::Backwards => ("<", ">=", "DESC"),
        };
        let source = if recurse {
            "WITH RECURSIVE rel AS (
                SELECT event_id, rel_type, event_type, stream_ordering, 1 AS depth
                  FROM event_relations WHERE relates_to = $2
                UNION ALL
                SELECT r.event_id, r.rel_type, r.event_type, r.stream_ordering, rel.depth + 1
                  FROM event_relations r JOIN rel ON r.relates_to = rel.event_id
                 WHERE rel.depth < 3
            ) SELECT DISTINCT ON (event_id) * FROM rel"
        } else {
            "SELECT event_id, rel_type, event_type, stream_ordering FROM event_relations WHERE relates_to = $2"
        };
        let sql = format!(
            "SELECT {EVENT_COLUMNS} FROM ({source}) x
               JOIN events e ON e.event_id = x.event_id JOIN rooms r ON r.room_id = e.room_id
              WHERE e.room_id = $1 AND NOT e.rejected
                AND ($3::text IS NULL OR x.rel_type = $3)
                AND ($4::text IS NULL OR x.event_type = $4)
                AND ($5::bigint IS NULL OR x.stream_ordering {cmp_from} $5)
                AND ($6::bigint IS NULL OR x.stream_ordering {cmp_to} $6)
              ORDER BY x.stream_ordering {order} LIMIT $7"
        );
        let rows: Vec<EventRow> = sqlx::query_as(sqlx::AssertSqlSafe(sql))
            .bind(room_id.as_str())
            .bind(event_id.as_str())
            .bind(rel_type)
            .bind(event_type)
            .bind(from)
            .bind(to)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter().map(StoredEvent::try_from).collect()
    }

    async fn relation_summary(&self, event_id: &EventId) -> Result<Vec<RelationRow>> {
        let rows = sqlx::query(
            "SELECT r.event_id, r.rel_type, r.event_type, r.sender, r.origin_server_ts, r.stream_ordering
               FROM event_relations r JOIN events e ON e.event_id = r.event_id
              WHERE r.relates_to = $1 AND NOT e.rejected
              ORDER BY r.stream_ordering ASC",
        )
        .bind(event_id.as_str())
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|r| {
                Ok(RelationRow {
                    event_id: OwnedEventId::try_from(r.get::<String, _>(0))
                        .map_err(|e| Error::Corrupt(format!("event id: {e}")))?,
                    rel_type: r.get(1),
                    event_type: r.get(2),
                    sender: ruma::OwnedUserId::try_from(r.get::<String, _>(3))
                        .map_err(|e| Error::Corrupt(format!("user id: {e}")))?,
                    origin_server_ts: r.get(4),
                    stream_ordering: r.get(5),
                })
            })
            .collect()
    }

    async fn threads(
        &self,
        room_id: &RoomId,
        participant: Option<&UserId>,
        before: Option<StreamOrdering>,
        limit: usize,
    ) -> Result<Vec<ThreadSummary>> {
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let rows = sqlx::query(
            "SELECT r.relates_to, MAX(r.stream_ordering) AS latest
               FROM event_relations r JOIN events root ON root.event_id = r.relates_to
              WHERE r.room_id = $1 AND r.rel_type = 'm.thread' AND NOT root.rejected
              GROUP BY r.relates_to
             HAVING ($2::bigint IS NULL OR MAX(r.stream_ordering) < $2)
                AND ($3::text IS NULL OR MAX(root.sender) = $3
                     OR bool_or(r.sender = $3))
              ORDER BY latest DESC LIMIT $4",
        )
        .bind(room_id.as_str())
        .bind(before)
        .bind(participant.map(UserId::as_str))
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            let root_id = OwnedEventId::try_from(r.get::<String, _>(0))
                .map_err(|e| Error::Corrupt(format!("event id: {e}")))?;
            if let Some(root) = self.get_event(&root_id).await? {
                out.push(ThreadSummary {
                    root,
                    latest_stream_ordering: r.get(1),
                });
            }
        }
        Ok(out)
    }

    async fn search_events(
        &self,
        room_ids: &[OwnedRoomId],
        term: &str,
        order_recent: bool,
        offset: usize,
        limit: usize,
    ) -> Result<(Vec<SearchHit>, usize)> {
        if room_ids.is_empty() {
            return Ok((Vec::new(), 0));
        }
        let rooms = strings(room_ids);
        let count: i64 = sqlx::query(
            "SELECT COUNT(*) FROM event_search s
              WHERE s.room_id = ANY($1) AND s.vector @@ websearch_to_tsquery('english', $2)",
        )
        .bind(&rooms)
        .bind(term)
        .fetch_one(&self.pool)
        .await?
        .get(0);
        let order = if order_recent {
            "s.origin_server_ts DESC, s.stream_ordering DESC"
        } else {
            "rank DESC, s.stream_ordering DESC"
        };
        let sql = format!(
            "SELECT {EVENT_COLUMNS}, ts_rank(s.vector, websearch_to_tsquery('english', $2)) AS rank
               FROM event_search s JOIN events e ON e.event_id = s.event_id
               JOIN rooms r ON r.room_id = e.room_id
              WHERE s.room_id = ANY($1) AND s.vector @@ websearch_to_tsquery('english', $2)
                AND NOT e.rejected
              ORDER BY {order} OFFSET $3 LIMIT $4"
        );
        let rows = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(&rooms)
            .bind(term)
            .bind(i64::try_from(offset).unwrap_or(i64::MAX))
            .bind(i64::try_from(limit).unwrap_or(i64::MAX))
            .fetch_all(&self.pool)
            .await?;
        let mut hits = Vec::with_capacity(rows.len());
        for row in rows {
            let rank: f32 = row.get("rank");
            let ev = StoredEvent::try_from(EventRow::from_row(&row)?)?;
            hits.push(SearchHit {
                event: ev,
                rank: f64::from(rank),
            });
        }
        Ok((hits, usize::try_from(count).unwrap_or(0)))
    }

    async fn de_outlier(&self, event_id: &EventId, state_group: i64) -> Result<()> {
        sqlx::query("UPDATE events SET outlier = FALSE, state_group = $2 WHERE event_id = $1")
            .bind(event_id.as_str())
            .bind(state_group)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn forward_extremities(&self, room_id: &RoomId) -> Result<Vec<OwnedEventId>> {
        let rows = sqlx::query("SELECT event_id FROM forward_extremities WHERE room_id = $1")
            .bind(room_id.as_str())
            .fetch_all(&self.pool)
            .await?;
        Ok(ids_from_rows(rows))
    }

    async fn set_forward_extremities(
        &self,
        room_id: &RoomId,
        event_ids: &[OwnedEventId],
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        set_forward_extremities(&mut tx, room_id, event_ids).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn missing_prev_events(&self, room_id: &RoomId) -> Result<Vec<OwnedEventId>> {
        let rows = sqlx::query("SELECT DISTINCT event_id FROM missing_events WHERE room_id = $1")
            .bind(room_id.as_str())
            .fetch_all(&self.pool)
            .await?;
        Ok(ids_from_rows(rows))
    }

    async fn backward_extremities(&self, room_id: &RoomId) -> Result<Vec<OwnedEventId>> {
        let rows = sqlx::query(
            "SELECT DISTINCT m.referenced_by FROM missing_events m JOIN events e ON e.event_id = m.referenced_by
             WHERE m.room_id = $1 AND NOT e.outlier",
        )
        .bind(room_id.as_str())
        .fetch_all(&self.pool)
        .await?;
        Ok(ids_from_rows(rows))
    }

    async fn auth_chain(&self, event_ids: &[OwnedEventId]) -> Result<Vec<OwnedEventId>> {
        let rows = sqlx::query(
            "WITH RECURSIVE chain AS (
                SELECT auth_event_id FROM event_auth WHERE event_id = ANY($1)
                UNION
                SELECT ea.auth_event_id FROM event_auth ea JOIN chain c ON ea.event_id = c.auth_event_id
             ) SELECT auth_event_id FROM chain",
        )
        .bind(strings(event_ids))
        .fetch_all(&self.pool)
        .await?;
        Ok(ids_from_rows(rows))
    }

    async fn backfill_from(
        &self,
        room_id: &RoomId,
        latest: &[OwnedEventId],
        limit: usize,
    ) -> Result<Vec<StoredEvent>> {
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let rows: Vec<EventRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "WITH RECURSIVE walk AS (
                SELECT event_id, 0 AS dist FROM events WHERE event_id = ANY($1) AND room_id = $2
                UNION
                SELECT ee.prev_event_id, w.dist + 1 FROM event_edges ee JOIN walk w ON ee.event_id = w.event_id
                WHERE w.dist < $3
             )
             SELECT {EVENT_COLUMNS} FROM (SELECT DISTINCT event_id FROM walk) w
             JOIN events e ON e.event_id = w.event_id JOIN rooms r ON r.room_id = e.room_id
             WHERE NOT e.outlier
             ORDER BY e.depth DESC, e.stream_ordering DESC LIMIT $3"
        )))
        .bind(strings(latest))
        .bind(room_id.as_str())
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(StoredEvent::try_from).collect()
    }

    async fn missing_events_between(
        &self,
        room_id: &RoomId,
        earliest: &[OwnedEventId],
        latest: &[OwnedEventId],
        limit: usize,
        min_depth: i64,
    ) -> Result<Vec<StoredEvent>> {
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        // Walk back from `latest` (exclusive) without crossing `earliest`.
        let rows: Vec<EventRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "WITH RECURSIVE walk AS (
                SELECT ee.prev_event_id AS event_id, 1 AS dist FROM event_edges ee WHERE ee.event_id = ANY($1)
                UNION
                SELECT ee.prev_event_id, w.dist + 1 FROM event_edges ee JOIN walk w ON ee.event_id = w.event_id
                WHERE w.dist < $4 AND NOT (w.event_id = ANY($2))
             )
             SELECT {EVENT_COLUMNS} FROM (SELECT DISTINCT event_id FROM walk) w
             JOIN events e ON e.event_id = w.event_id JOIN rooms r ON r.room_id = e.room_id
             WHERE e.room_id = $3 AND NOT (e.event_id = ANY($2)) AND e.depth >= $5 AND NOT e.outlier
             ORDER BY e.depth ASC, e.stream_ordering ASC LIMIT $4"
        )))
        .bind(strings(latest))
        .bind(strings(earliest))
        .bind(room_id.as_str())
        .bind(limit)
        .bind(min_depth)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(StoredEvent::try_from).collect()
    }

    async fn room_events_in_range(
        &self,
        room_id: &RoomId,
        from: StreamOrdering,
        to: Option<StreamOrdering>,
        direction: Direction,
        limit: usize,
    ) -> Result<Vec<StoredEvent>> {
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let sql = match direction {
            Direction::Forwards => format!(
                "SELECT {EVENT_COLUMNS} FROM events e JOIN rooms r ON r.room_id = e.room_id
                 WHERE e.room_id = $1 AND e.stream_ordering > $2 AND ($3::bigint IS NULL OR e.stream_ordering <= $3)
                   AND NOT e.rejected AND NOT e.outlier
                 ORDER BY e.stream_ordering ASC LIMIT $4"
            ),
            Direction::Backwards => format!(
                "SELECT {EVENT_COLUMNS} FROM events e JOIN rooms r ON r.room_id = e.room_id
                 WHERE e.room_id = $1 AND e.stream_ordering < $2 AND ($3::bigint IS NULL OR e.stream_ordering >= $3)
                   AND NOT e.rejected AND NOT e.outlier
                 ORDER BY e.stream_ordering DESC LIMIT $4"
            ),
        };
        let rows: Vec<EventRow> = sqlx::query_as(sqlx::AssertSqlSafe(sql))
            .bind(room_id.as_str())
            .bind(from)
            .bind(to)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter().map(StoredEvent::try_from).collect()
    }

    async fn max_stream_ordering(&self) -> Result<StreamOrdering> {
        let row = sqlx::query("SELECT COALESCE(MAX(stream_ordering), 0) FROM events")
            .fetch_one(&self.pool)
            .await?;
        Ok(row.get::<i64, _>(0))
    }

    async fn rooms_with_events_in_range(
        &self,
        room_ids: &[OwnedRoomId],
        from: StreamOrdering,
        to: StreamOrdering,
    ) -> Result<Vec<OwnedRoomId>> {
        let rows = sqlx::query(
            "SELECT DISTINCT room_id FROM events WHERE room_id = ANY($1) AND stream_ordering > $2 AND stream_ordering <= $3",
        )
        .bind(strings(room_ids))
        .bind(from)
        .bind(to)
        .fetch_all(&self.pool)
        .await?;
        Ok(parse_ids(
            rows.into_iter().map(|r| r.get::<String, _>(0)).collect(),
        ))
    }

    async fn stream_to_topological(
        &self,
        event_id: &EventId,
    ) -> Result<Option<(i64, StreamOrdering)>> {
        let row = sqlx::query("SELECT depth, stream_ordering FROM events WHERE event_id = $1")
            .bind(event_id.as_str())
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(|r| (r.get::<i64, _>(0), r.get::<i64, _>(1))))
    }
}

impl From<ruma::IdParseError> for Error {
    fn from(e: ruma::IdParseError) -> Self {
        Error::Corrupt(e.to_string())
    }
}

/// Record `m.relates_to` and index searchable text for a freshly persisted event.
async fn index_relation_and_search(
    conn: &mut PgConnection,
    pdu: &Pdu,
    stream_ordering: i64,
) -> Result<()> {
    let content: serde_json::Value = serde_json::from_str(pdu.content().get()).unwrap_or_default();
    if let Some(rel) = content.get("m.relates_to")
        && let (Some(rel_type), Some(target)) = (
            rel.get("rel_type").and_then(|v| v.as_str()),
            rel.get("event_id").and_then(|v| v.as_str()),
        )
        && ruma::EventId::parse(target).is_ok()
    {
        sqlx::query(
            "INSERT INTO event_relations (event_id, room_id, relates_to, rel_type, event_type, sender, origin_server_ts, stream_ordering)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT DO NOTHING",
        )
        .bind(pdu.event_id().as_str())
        .bind(pdu.room_id().as_str())
        .bind(target)
        .bind(rel_type)
        .bind(pdu.event_type().to_string())
        .bind(pdu.sender().as_str())
        .bind(uint_to_i64(pdu.origin_server_ts().0))
        .bind(stream_ordering)
        .execute(&mut *conn)
        .await?;
    }
    let text = match pdu.event_type().to_string().as_str() {
        "m.room.message" => content.get("body").and_then(|v| v.as_str()),
        "m.room.name" => content.get("name").and_then(|v| v.as_str()),
        "m.room.topic" => content.get("topic").and_then(|v| v.as_str()),
        _ => None,
    };
    if let Some(text) = text {
        sqlx::query(
            "INSERT INTO event_search (event_id, room_id, stream_ordering, origin_server_ts, vector)
             VALUES ($1, $2, $3, $4, to_tsvector('english', $5)) ON CONFLICT DO NOTHING",
        )
        .bind(pdu.event_id().as_str())
        .bind(pdu.room_id().as_str())
        .bind(stream_ordering)
        .bind(uint_to_i64(pdu.origin_server_ts().0))
        .bind(text)
        .execute(&mut *conn)
        .await?;
    }
    Ok(())
}
