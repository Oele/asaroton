use asaroton_core::Pdu;
use async_trait::async_trait;
use ruma::{EventId, OwnedEventId, OwnedRoomId, OwnedServerName, RoomId, ServerName, UserId};
use sqlx::{FromRow, PgConnection, Row};

use super::{EVENT_COLUMNS, EventRow, PostgresStore, strings};
use crate::{
    AwaitingAuth, Direction, Error, EventState, EventStore, PersistEvent, PersistedEvent,
    RelationRow, Result, SearchHit, SearchMode, SearchQuery, StoredEvent, StreamOrdering,
    ThreadSummary, now_ts, parse_ids, uint_to_i64,
};

async fn persist_one(
    conn: &mut PgConnection,
    ev: PersistEvent,
    keep_plaintext: bool,
) -> Result<PersistedEvent> {
    let pdu = &ev.pdu;
    let room_id = pdu.room_id();

    // Idempotency: an event we already have is not written twice.
    if let Some(row) = sqlx::query(
        "SELECT stream_ordering, state_group, outlier, rejected FROM events WHERE event_id = $1",
    )
    .bind(pdu.event_id().as_str())
    .fetch_optional(&mut *conn)
    .await?
    {
        let existing = PersistedEvent {
            event_id: pdu.event_id().to_owned(),
            stream_ordering: row.get("stream_ordering"),
            state_group: row.get("state_group"),
        };
        let placed = !matches!(ev.state, EventState::Outlier);
        let (was_outlier, was_rejected): (bool, bool) = (row.get("outlier"), row.get("rejected"));
        // Stored as rejected, and passing its checks on this later look: what it depends on
        // could not be obtained or verified the first time.
        let accepted_after_all = was_rejected && !pdu.rejected();
        // The other way round: an outlier that was accepted on the strength of its auth
        // events alone, and now fails against the room's state. It is not given a place in
        // the timeline, where clients would be shown an event that failed its checks. Its row
        // is not turned into a rejection either: this later check may be working from less
        // (the state at a gap, as another server reports it), and other events rest on the
        // outlier as an auth event.
        if placed && was_outlier && !was_rejected && pdu.rejected() {
            return Ok(existing);
        }
        // An outlier being persisted again with state is "de-outliered"; it takes its place in
        // the timeline at the given (or next backfill) position. So does an event whose
        // rejection is taken back.
        if placed && (was_outlier || accepted_after_all) {
            let group = resolve_state_group(&mut *conn, room_id, &ev.state).await?;
            let new_ordering: i64 = match ev.stream_ordering {
                Some(so) => so,
                None if ev.backfilled => sqlx::query("SELECT nextval('events_backfill_seq')")
                    .fetch_one(&mut *conn)
                    .await?
                    .get(0),
                // A fresh position: sync tokens handed out meanwhile are already past the
                // one the event was given, so keeping it would hide the event from them.
                None => sqlx::query("SELECT nextval('events_stream_seq')")
                    .fetch_one(&mut *conn)
                    .await?
                    .get(0),
            };
            sqlx::query(
                "UPDATE events SET outlier = FALSE, state_group = $2, soft_failed = $3, stream_ordering = $4,
                        rejected = $5
                 WHERE event_id = $1",
            )
            .bind(pdu.event_id().as_str())
            .bind(group)
            .bind(ev.soft_failed)
            .bind(new_ordering)
            .bind(pdu.rejected())
            .execute(&mut *conn)
            .await?;
            if accepted_after_all {
                index_event(&mut *conn, pdu, new_ordering, keep_plaintext).await?;
            }
            let existing = PersistedEvent {
                stream_ordering: new_ordering,
                ..existing
            };
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

    let mut json_obj = pdu.raw().clone();
    json_obj.remove("unsigned");
    let json = asaroton_core::canonical::to_string(&json_obj);
    let (relates_to, rel_type) = relation_of(pdu).unzip();
    let search_text = search_text_of(pdu);

    // One round trip writes the event with its edges, auth links, missing-prev bookkeeping and
    // the relation and search indexes.
    let sql = format!(
        "WITH ev AS (
            INSERT INTO events (stream_ordering, event_id, room_id, event_type, state_key, sender,
                origin_server_ts, depth, json, state_group, rejected, soft_failed, outlier, received_ts)
            VALUES (COALESCE($1::bigint, nextval('{seq}')), $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
            RETURNING stream_ordering
         ),
         edges AS (
            INSERT INTO event_edges (event_id, prev_event_id)
            SELECT $2, p FROM UNNEST($15::text[]) AS p ON CONFLICT DO NOTHING
         ),
         missing AS (
            INSERT INTO missing_events (room_id, event_id, referenced_by)
            SELECT $3, p, $2 FROM UNNEST($15::text[]) AS p
            WHERE NOT EXISTS (SELECT 1 FROM events WHERE event_id = p)
            ON CONFLICT DO NOTHING
         ),
         auth AS (
            INSERT INTO event_auth (event_id, auth_event_id)
            SELECT $2, a FROM UNNEST($16::text[]) AS a ON CONFLICT DO NOTHING
         ),
         found AS (DELETE FROM missing_events WHERE event_id = $2),
         rel AS (
            INSERT INTO event_relations (event_id, room_id, relates_to, rel_type, event_type, sender, origin_server_ts, stream_ordering)
            SELECT $2, $3, $17, $18, $4, $6, $7, ev.stream_ordering FROM ev
            WHERE $17::text IS NOT NULL AND NOT $11 ON CONFLICT DO NOTHING
         ),
         search AS (
            INSERT INTO event_search (event_id, room_id, stream_ordering, origin_server_ts, vector, value)
            SELECT $2, $3, ev.stream_ordering, $7, to_tsvector('english', $19), $20 FROM ev
            WHERE $19::text IS NOT NULL AND NOT $11 ON CONFLICT DO NOTHING
         )
         SELECT stream_ordering FROM ev"
    );
    let stream_ordering: i64 = sqlx::query(sqlx::AssertSqlSafe(sql))
        .bind(ev.stream_ordering)
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
        .bind(strings(pdu.prev_events()))
        .bind(strings(pdu.auth_events()))
        .bind(relates_to)
        .bind(rel_type)
        .bind(&search_text)
        .bind(search_text.as_deref().filter(|_| keep_plaintext))
        .fetch_one(&mut *conn)
        .await?
        .get(0);

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

/// Write the relation and search index rows of an event. They are left out while an event is
/// rejected, so an event whose rejection is taken back gets them here.
async fn index_event(
    conn: &mut PgConnection,
    pdu: &Pdu,
    stream_ordering: i64,
    keep_plaintext: bool,
) -> Result<()> {
    if let Some((target, rel_type)) = relation_of(pdu) {
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
    if let Some(text) = search_text_of(pdu) {
        sqlx::query(
            "INSERT INTO event_search (event_id, room_id, stream_ordering, origin_server_ts, vector, value)
             VALUES ($1, $2, $3, $4, to_tsvector('english', $5), $6) ON CONFLICT DO NOTHING",
        )
        .bind(pdu.event_id().as_str())
        .bind(pdu.room_id().as_str())
        .bind(stream_ordering)
        .bind(uint_to_i64(pdu.origin_server_ts().0))
        .bind(&text)
        .bind(keep_plaintext.then_some(text.as_str()))
        .execute(&mut *conn)
        .await?;
    }
    Ok(())
}

/// `(target, rel_type)` of the event's `m.relates_to`, when it points at a valid event ID.
fn relation_of(pdu: &Pdu) -> Option<(String, String)> {
    let content: serde_json::Value = serde_json::from_str(pdu.content().get()).ok()?;
    let rel = content.get("m.relates_to")?;
    let rel_type = rel.get("rel_type")?.as_str()?;
    let target = rel.get("event_id")?.as_str()?;
    ruma::EventId::parse(target).ok()?;
    Some((target.to_owned(), rel_type.to_owned()))
}

/// The text the search index gets for this event, if any.
fn search_text_of(pdu: &Pdu) -> Option<String> {
    let content: serde_json::Value = serde_json::from_str(pdu.content().get()).ok()?;
    search_text_for(&pdu.event_type().to_string(), &content)
}

/// Same, from an event's stored JSON.
fn search_text_of_json(json: &str) -> Option<String> {
    let v: serde_json::Value = serde_json::from_str(json).ok()?;
    let ty = v.get("type")?.as_str()?.to_owned();
    search_text_for(&ty, v.get("content")?)
}

fn search_text_for(event_type: &str, content: &serde_json::Value) -> Option<String> {
    let key = match event_type {
        "m.room.message" => "body",
        "m.room.name" => "name",
        "m.room.topic" => "topic",
        _ => return None,
    };
    let text = content.get(key)?.as_str()?;
    // PostgreSQL text cannot hold NUL.
    Some(text.replace('\0', ""))
}

/// Whitespace-separated terms, with double-quoted phrases kept whole and terms shorter than
/// `min_length` characters dropped (a trigram index needs three characters to be useful).
fn substring_terms(query: &str, min_length: usize) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();
    let mut quoted = false;
    for c in query.chars() {
        match c {
            '"' => {
                quoted = !quoted;
                if !quoted && !current.is_empty() {
                    out.push(std::mem::take(&mut current));
                }
            }
            c if c.is_whitespace() && !quoted => {
                if !current.is_empty() {
                    out.push(std::mem::take(&mut current));
                }
            }
            c => current.push(c),
        }
    }
    if !current.is_empty() {
        out.push(current);
    }
    out.into_iter()
        .filter(|t| t.chars().count() >= min_length.max(1))
        .collect()
}

/// Escapes `LIKE` metacharacters (the pattern uses `ESCAPE '\\'`).
fn like_escape(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
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
    // Rows staying in the set are left alone, so one statement replaces the set.
    sqlx::query(
        "WITH gone AS (
            DELETE FROM forward_extremities WHERE room_id = $1 AND event_id <> ALL($2::text[])
         )
         INSERT INTO forward_extremities (room_id, event_id)
         SELECT $1, e FROM UNNEST($2::text[]) AS e ON CONFLICT DO NOTHING",
    )
    .bind(room_id.as_str())
    .bind(strings(event_ids))
    .execute(&mut *conn)
    .await?;
    Ok(())
}

fn ids_from_rows(rows: Vec<sqlx::postgres::PgRow>) -> Vec<OwnedEventId> {
    parse_ids(rows.into_iter().map(|r| r.get::<String, _>(0)).collect())
}

#[async_trait]
impl EventStore for PostgresStore {
    async fn stream_positions(&self) -> Result<crate::StreamPositions> {
        let row = sqlx::query(
            "SELECT (SELECT COALESCE(MAX(stream_ordering), 0) FROM events) AS events,
                    GREATEST((SELECT COALESCE(MAX(stream_id), 0) FROM account_data),
                             (SELECT COALESCE(MAX(stream_id), 0) FROM push_rules)) AS account_data,
                    (SELECT COALESCE(MAX(stream_id), 0) FROM receipts) AS receipts,
                    (SELECT COALESCE(MAX(stream_id), 0) FROM to_device_messages) AS to_device,
                    (SELECT COALESCE(MAX(stream_id), 0) FROM device_list_changes) AS device_lists,
                    (SELECT COALESCE(MAX(stream_id), 0) FROM thread_subscriptions) AS thread_subscriptions,
                    (SELECT COALESCE(MAX(stream_id), 0) FROM profile_updates) AS profile_updates",
        )
        .fetch_one(&self.pool)
        .await?;
        Ok(crate::StreamPositions {
            events: row.get("events"),
            account_data: row.get("account_data"),
            receipts: row.get("receipts"),
            to_device: row.get("to_device"),
            device_lists: row.get("device_lists"),
            thread_subscriptions: row.get("thread_subscriptions"),
            profile_updates: row.get("profile_updates"),
        })
    }

    async fn persist_event(&self, event: PersistEvent) -> Result<PersistedEvent> {
        let mut tx = self.pool.begin().await?;
        let keep = self
            .search_plaintext
            .load(std::sync::atomic::Ordering::Relaxed);
        let out = persist_one(&mut tx, event, keep).await?;
        tx.commit().await?;
        Ok(out)
    }

    async fn persist_events(&self, events: Vec<PersistEvent>) -> Result<Vec<PersistedEvent>> {
        let mut tx = self.pool.begin().await?;
        let keep = self
            .search_plaintext
            .load(std::sync::atomic::Ordering::Relaxed);
        let mut out = Vec::with_capacity(events.len());
        for ev in events {
            out.push(persist_one(&mut tx, ev, keep).await?);
        }
        tx.commit().await?;
        Ok(out)
    }

    async fn event_at_timestamp(
        &self,
        room_id: &RoomId,
        ts: i64,
        direction: Direction,
    ) -> Result<Option<StoredEvent>> {
        let sql = match direction {
            Direction::Forwards => format!(
                "SELECT {EVENT_COLUMNS} FROM events e JOIN rooms r ON r.room_id = e.room_id
                  WHERE e.room_id = $1 AND e.origin_server_ts >= $2 AND NOT e.rejected AND NOT e.outlier AND NOT e.soft_failed
                  ORDER BY e.origin_server_ts ASC, e.stream_ordering ASC LIMIT 1"
            ),
            Direction::Backwards => format!(
                "SELECT {EVENT_COLUMNS} FROM events e JOIN rooms r ON r.room_id = e.room_id
                  WHERE e.room_id = $1 AND e.origin_server_ts <= $2 AND NOT e.rejected AND NOT e.outlier AND NOT e.soft_failed
                  ORDER BY e.origin_server_ts DESC, e.stream_ordering DESC LIMIT 1"
            ),
        };
        let row: Option<EventRow> = sqlx::query_as(sqlx::AssertSqlSafe(sql))
            .bind(room_id.as_str())
            .bind(ts)
            .fetch_optional(&self.pool)
            .await?;
        row.map(StoredEvent::try_from).transpose()
    }

    async fn events_after(&self, after: StreamOrdering, limit: usize) -> Result<Vec<StoredEvent>> {
        let sql = format!(
            "SELECT {EVENT_COLUMNS} FROM events e JOIN rooms r ON r.room_id = e.room_id
              WHERE e.stream_ordering > $1 AND NOT e.outlier AND NOT e.rejected AND NOT e.soft_failed
              ORDER BY e.stream_ordering ASC LIMIT $2"
        );
        let rows: Vec<EventRow> = sqlx::query_as(sqlx::AssertSqlSafe(sql))
            .bind(after.max(0))
            .bind(i64::try_from(limit).unwrap_or(i64::MAX))
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter().map(StoredEvent::try_from).collect()
    }

    async fn latest_stream_orderings(
        &self,
        room_ids: &[OwnedRoomId],
        event_types: Option<&[String]>,
    ) -> Result<std::collections::HashMap<OwnedRoomId, StreamOrdering>> {
        if room_ids.is_empty() {
            return Ok(std::collections::HashMap::new());
        }
        let ids: Vec<String> = room_ids.iter().map(ToString::to_string).collect();
        let rows = match event_types {
            Some(types) => {
                sqlx::query(
                    "SELECT room_id, MAX(stream_ordering) FROM events
                     WHERE room_id = ANY($1) AND event_type = ANY($2) AND NOT outlier AND NOT rejected AND NOT soft_failed
                     GROUP BY room_id",
                )
                .bind(&ids)
                .bind(types)
                .fetch_all(&self.pool)
                .await?
            }
            None => {
                sqlx::query(
                    "SELECT room_id, MAX(stream_ordering) FROM events
                     WHERE room_id = ANY($1) AND NOT outlier AND NOT rejected AND NOT soft_failed
                     GROUP BY room_id",
                )
                .bind(&ids)
                .fetch_all(&self.pool)
                .await?
            }
        };
        let mut out = std::collections::HashMap::with_capacity(rows.len());
        for r in rows {
            let id: String = r.get(0);
            if let Ok(id) = OwnedRoomId::try_from(id) {
                out.insert(id, r.get::<i64, _>(1));
            }
        }
        Ok(out)
    }

    async fn appservice_position(&self, appservice_id: &str) -> Result<StreamOrdering> {
        let row = sqlx::query(
            "SELECT stream_ordering FROM appservice_positions WHERE appservice_id = $1",
        )
        .bind(appservice_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map_or(0, |r| r.get::<i64, _>(0)))
    }

    async fn set_appservice_position(
        &self,
        appservice_id: &str,
        position: StreamOrdering,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO appservice_positions (appservice_id, stream_ordering) VALUES ($1, $2)
             ON CONFLICT (appservice_id) DO UPDATE SET stream_ordering = EXCLUDED.stream_ordering",
        )
        .bind(appservice_id)
        .bind(position)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn reserve_backfill_orderings(&self, n: usize) -> Result<Vec<StreamOrdering>> {
        let rows = sqlx::query("SELECT nextval('events_backfill_seq') FROM generate_series(1, $1)")
            .bind(i64::try_from(n).unwrap_or(0))
            .fetch_all(&self.pool)
            .await?;
        Ok(rows.into_iter().map(|r| r.get::<i64, _>(0)).collect())
    }

    async fn room_event_count_in_range(&self, room_id: &RoomId, from: i64, to: i64) -> Result<i64> {
        let row = sqlx::query(
            "SELECT count(*) FROM events WHERE room_id = $1 AND stream_ordering BETWEEN $2 AND $3
             AND NOT rejected AND NOT outlier AND NOT soft_failed",
        )
        .bind(room_id.as_str())
        .bind(from)
        .bind(to)
        .fetch_one(&self.pool)
        .await?;
        Ok(row.get::<i64, _>(0))
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

    async fn clear_rejected(&self, event_ids: &[OwnedEventId]) -> Result<u64> {
        if event_ids.is_empty() {
            return Ok(0);
        }
        let rows = sqlx::query(
            "UPDATE events SET rejected = FALSE WHERE event_id = ANY($1) AND rejected RETURNING event_id",
        )
        .bind(strings(event_ids))
        .fetch_all(&self.pool)
        .await?;
        // What was left out of the indexes while the events were rejected.
        let restored = ids_from_rows(rows);
        let keep = self
            .search_plaintext
            .load(std::sync::atomic::Ordering::Relaxed);
        let mut conn = self.pool.acquire().await?;
        for ev in self.get_events(&restored).await? {
            index_event(&mut conn, &ev.pdu, ev.stream_ordering, keep).await?;
        }
        Ok(restored.len() as u64)
    }

    async fn rejected_timeline_events(
        &self,
        room_id: &RoomId,
        received_since: i64,
    ) -> Result<Vec<StoredEvent>> {
        let rows: Vec<EventRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT {EVENT_COLUMNS} FROM events e JOIN rooms r ON r.room_id = e.room_id
             WHERE e.room_id = $1 AND e.rejected AND NOT e.outlier AND e.received_ts >= $2
             ORDER BY e.stream_ordering LIMIT 10000"
        )))
        .bind(room_id.as_str())
        .bind(received_since)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(StoredEvent::try_from).collect()
    }

    async fn await_auth(
        &self,
        event_id: &EventId,
        room_id: &RoomId,
        origin: &ServerName,
        now: i64,
        next_try_ts: i64,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO events_awaiting_auth (event_id, room_id, origin, first_ts, next_try_ts)
             VALUES ($1, $2, $3, $4, $5) ON CONFLICT (event_id) DO NOTHING",
        )
        .bind(event_id.as_str())
        .bind(room_id.as_str())
        .bind(origin.as_str())
        .bind(now)
        .bind(next_try_ts)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn events_awaiting_auth_due(&self, now: i64, limit: i64) -> Result<Vec<AwaitingAuth>> {
        let rows = sqlx::query(
            "SELECT event_id, room_id, origin, first_ts, tries FROM events_awaiting_auth
             WHERE next_try_ts <= $1 ORDER BY first_ts LIMIT $2",
        )
        .bind(now)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                let corrupt = |what: &str| Error::Corrupt(format!("events_awaiting_auth.{what}"));
                Ok(AwaitingAuth {
                    event_id: OwnedEventId::try_from(row.get::<String, _>("event_id"))
                        .map_err(|_| corrupt("event_id"))?,
                    room_id: OwnedRoomId::try_from(row.get::<String, _>("room_id"))
                        .map_err(|_| corrupt("room_id"))?,
                    origin: OwnedServerName::try_from(row.get::<String, _>("origin"))
                        .map_err(|_| corrupt("origin"))?,
                    first_ts: row.get("first_ts"),
                    tries: row.get("tries"),
                })
            })
            .collect()
    }

    async fn reschedule_awaiting_auth(&self, event_id: &EventId, next_try_ts: i64) -> Result<()> {
        sqlx::query(
            "UPDATE events_awaiting_auth SET tries = tries + 1, next_try_ts = $2 WHERE event_id = $1",
        )
        .bind(event_id.as_str())
        .bind(next_try_ts)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn events_set_aside(&self, room_id: &RoomId) -> Result<Vec<StoredEvent>> {
        let rows: Vec<EventRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT {EVENT_COLUMNS} FROM events_awaiting_auth a
             JOIN events e ON e.event_id = a.event_id JOIN rooms r ON r.room_id = e.room_id
             WHERE a.room_id = $1 ORDER BY a.first_ts, e.depth LIMIT 10000"
        )))
        .bind(room_id.as_str())
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(StoredEvent::try_from).collect()
    }

    async fn hasten_awaiting_auth(&self, room_id: &RoomId, now: i64) -> Result<()> {
        sqlx::query(
            "UPDATE events_awaiting_auth SET next_try_ts = $2 WHERE room_id = $1 AND next_try_ts > $2",
        )
        .bind(room_id.as_str())
        .bind(now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn forget_awaiting_auth(&self, event_id: &EventId) -> Result<()> {
        sqlx::query("DELETE FROM events_awaiting_auth WHERE event_id = $1")
            .bind(event_id.as_str())
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn have_events(&self, event_ids: &[OwnedEventId]) -> Result<Vec<OwnedEventId>> {
        let rows = sqlx::query("SELECT event_id FROM events WHERE event_id = ANY($1)")
            .bind(strings(event_ids))
            .fetch_all(&self.pool)
            .await?;
        Ok(ids_from_rows(rows))
    }

    async fn has_accepted_child(&self, event_id: &EventId) -> Result<bool> {
        let found: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                SELECT 1 FROM event_edges ee JOIN events c ON c.event_id = ee.event_id
                 WHERE ee.prev_event_id = $1 AND NOT c.outlier AND NOT c.rejected)",
        )
        .bind(event_id.as_str())
        .fetch_one(&self.pool)
        .await?;
        Ok(found)
    }

    async fn redact_event(&self, event_id: &EventId, redacted: &Pdu) -> Result<()> {
        // The redacted form carries `unsigned.redacted_because`, which clients need; keep it.
        let json = asaroton_core::canonical::to_string(redacted.raw());
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
              WHERE e.room_id = $1 AND NOT e.rejected AND NOT e.soft_failed
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

    async fn relation_summaries(
        &self,
        event_ids: &[OwnedEventId],
    ) -> Result<std::collections::HashMap<OwnedEventId, Vec<RelationRow>>> {
        let mut out: std::collections::HashMap<OwnedEventId, Vec<RelationRow>> =
            std::collections::HashMap::new();
        if event_ids.is_empty() {
            return Ok(out);
        }
        let ids: Vec<String> = event_ids.iter().map(ToString::to_string).collect();
        let rows = sqlx::query(
            "SELECT r.relates_to, r.event_id, r.rel_type, r.event_type, r.sender, r.origin_server_ts, r.stream_ordering
               FROM event_relations r JOIN events e ON e.event_id = r.event_id
              WHERE r.relates_to = ANY($1) AND NOT e.rejected AND NOT e.soft_failed
              ORDER BY r.stream_ordering ASC",
        )
        .bind(&ids)
        .fetch_all(&self.pool)
        .await?;
        for r in rows {
            let parent = OwnedEventId::try_from(r.get::<String, _>(0))
                .map_err(|e| Error::Corrupt(format!("event id: {e}")))?;
            out.entry(parent).or_default().push(RelationRow {
                event_id: OwnedEventId::try_from(r.get::<String, _>(1))
                    .map_err(|e| Error::Corrupt(format!("event id: {e}")))?,
                rel_type: r.get(2),
                event_type: r.get(3),
                sender: ruma::OwnedUserId::try_from(r.get::<String, _>(4))
                    .map_err(|e| Error::Corrupt(format!("user id: {e}")))?,
                origin_server_ts: r.get(5),
                stream_ordering: r.get(6),
            });
        }
        Ok(out)
    }

    async fn relation_summary(&self, event_id: &EventId) -> Result<Vec<RelationRow>> {
        let rows = sqlx::query(
            "SELECT r.event_id, r.rel_type, r.event_type, r.sender, r.origin_server_ts, r.stream_ordering
               FROM event_relations r JOIN events e ON e.event_id = r.event_id
              WHERE r.relates_to = $1 AND NOT e.rejected AND NOT e.soft_failed
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
        query: &SearchQuery<'_>,
    ) -> Result<(Vec<SearchHit>, usize)> {
        if room_ids.is_empty() {
            return Ok((Vec::new(), 0));
        }
        let (term, mode, min_length, order_recent, offset, limit) = (
            query.term,
            query.mode,
            query.min_length,
            query.order_recent,
            query.offset,
            query.limit,
        );
        let rooms = strings(room_ids);
        // Substring terms: whitespace-separated, quoted phrases kept whole, short ones dropped.
        let patterns: Vec<String> = match mode {
            SearchMode::Word => Vec::new(),
            SearchMode::Substring | SearchMode::Both => substring_terms(term, min_length)
                .into_iter()
                .take(8)
                .map(|t| format!("%{}%", like_escape(&t)))
                .collect(),
        };
        let word = match mode {
            SearchMode::Substring => false,
            SearchMode::Word | SearchMode::Both => true,
        };
        if !word && patterns.is_empty() {
            return Ok((Vec::new(), 0));
        }
        // $1 rooms, $2 term, $3 offset, $4 limit, $5.. substring patterns.
        let mut conds: Vec<String> = Vec::new();
        if word {
            conds.push("s.vector @@ websearch_to_tsquery('english', $2)".to_owned());
        }
        if !patterns.is_empty() {
            let subs: Vec<String> = (0..patterns.len())
                .map(|i| format!("s.value ILIKE ${} ESCAPE '\\'", i + 5))
                .collect();
            conds.push(format!("({})", subs.join(" AND ")));
        }
        let matching = conds.join(" OR ");
        let count_sql = format!(
            "SELECT COUNT(*) FROM event_search s WHERE s.room_id = ANY($1) AND ({matching})"
        );
        let mut q = sqlx::query(sqlx::AssertSqlSafe(count_sql))
            .bind(&rooms)
            .bind(term)
            .bind(0i64)
            .bind(0i64);
        for p in &patterns {
            q = q.bind(p);
        }
        let count: i64 = q.fetch_one(&self.pool).await?.get(0);
        let order = if order_recent {
            "s.origin_server_ts DESC, s.stream_ordering DESC"
        } else {
            "rank DESC, s.stream_ordering DESC"
        };
        let rank = if word {
            "ts_rank(s.vector, websearch_to_tsquery('english', $2))"
        } else {
            "0::real"
        };
        let sql = format!(
            "SELECT {EVENT_COLUMNS}, {rank} AS rank
               FROM event_search s JOIN events e ON e.event_id = s.event_id
               JOIN rooms r ON r.room_id = e.room_id
              WHERE s.room_id = ANY($1) AND ({matching}) AND NOT e.rejected AND NOT e.soft_failed
              ORDER BY {order} OFFSET $3 LIMIT $4"
        );
        let mut q = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(&rooms)
            .bind(term)
            .bind(i64::try_from(offset).unwrap_or(i64::MAX))
            .bind(i64::try_from(limit).unwrap_or(i64::MAX));
        for p in &patterns {
            q = q.bind(p);
        }
        let rows = q.fetch_all(&self.pool).await?;
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

    fn set_search_plaintext(&self, enabled: bool) {
        self.search_plaintext
            .store(enabled, std::sync::atomic::Ordering::Relaxed);
    }

    async fn search_backfill_batch(
        &self,
        after: i64,
        limit: usize,
    ) -> Result<(usize, Option<i64>)> {
        let rows = sqlx::query(
            "SELECT e.event_id, e.room_id, e.stream_ordering, e.origin_server_ts, e.json
               FROM events e LEFT JOIN event_search s ON s.event_id = e.event_id
              WHERE e.stream_ordering > $1 AND NOT e.rejected
                AND e.event_type IN ('m.room.message', 'm.room.name', 'm.room.topic')
                AND (s.event_id IS NULL OR s.value IS NULL)
              ORDER BY e.stream_ordering LIMIT $2",
        )
        .bind(after)
        .bind(i64::try_from(limit).unwrap_or(i64::MAX))
        .fetch_all(&self.pool)
        .await?;
        let Some(last) = rows.last() else {
            return Ok((0, None));
        };
        let last_so: i64 = last.get("stream_ordering");
        let (mut ids, mut room_ids, mut sos, mut tss, mut texts) =
            (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new());
        for r in &rows {
            let json: String = r.get("json");
            let Some(text) = search_text_of_json(&json) else {
                continue;
            };
            ids.push(r.get::<String, _>("event_id"));
            room_ids.push(r.get::<String, _>("room_id"));
            sos.push(r.get::<i64, _>("stream_ordering"));
            tss.push(r.get::<i64, _>("origin_server_ts"));
            texts.push(text);
        }
        if ids.is_empty() {
            return Ok((0, Some(last_so)));
        }
        // NUL cannot be stored in text; the vector and the text both skip it.
        let written = sqlx::query(
            "INSERT INTO event_search (event_id, room_id, stream_ordering, origin_server_ts, vector, value)
             SELECT id, room, so, ts, to_tsvector('english', txt), txt
               FROM UNNEST($1::text[], $2::text[], $3::bigint[], $4::bigint[], $5::text[]) AS u(id, room, so, ts, txt)
             ON CONFLICT (event_id) DO UPDATE SET value = EXCLUDED.value",
        )
        .bind(&ids)
        .bind(&room_ids)
        .bind(&sos)
        .bind(&tss)
        .bind(&texts)
        .execute(&self.pool)
        .await?
        .rows_affected();
        Ok((usize::try_from(written).unwrap_or(0), Some(last_so)))
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

    async fn gap_position_since(
        &self,
        room_id: &RoomId,
        since: StreamOrdering,
    ) -> Result<Option<StreamOrdering>> {
        let row = sqlx::query(
            "SELECT MIN(e.stream_ordering) FROM event_edges g
               JOIN events e ON e.event_id = g.event_id
               LEFT JOIN events p ON p.event_id = g.prev_event_id
              WHERE e.room_id = $1 AND e.stream_ordering > $2 AND NOT e.outlier AND NOT e.rejected AND NOT e.soft_failed
                AND (p.event_id IS NULL OR p.outlier)",
        )
        .bind(room_id.as_str())
        .bind(since)
        .fetch_one(&self.pool)
        .await?;
        Ok(row.get::<Option<i64>, _>(0))
    }

    async fn backfill_points(&self, room_id: &RoomId, limit: usize) -> Result<Vec<OwnedEventId>> {
        let rows = sqlx::query(
            "SELECT m.event_id FROM missing_events m JOIN events e ON e.event_id = m.referenced_by
             WHERE m.room_id = $1 AND NOT e.outlier AND NOT e.rejected
             GROUP BY m.event_id ORDER BY MIN(e.stream_ordering) LIMIT $2",
        )
        .bind(room_id.as_str())
        .bind(i64::try_from(limit).unwrap_or(i64::MAX))
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
                   AND NOT e.rejected AND NOT e.outlier AND NOT e.soft_failed
                 ORDER BY e.stream_ordering ASC LIMIT $4"
            ),
            Direction::Backwards => format!(
                "SELECT {EVENT_COLUMNS} FROM events e JOIN rooms r ON r.room_id = e.room_id
                 WHERE e.room_id = $1 AND e.stream_ordering < $2 AND ($3::bigint IS NULL OR e.stream_ordering >= $3)
                   AND NOT e.rejected AND NOT e.outlier AND NOT e.soft_failed
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

    async fn room_events_topological(
        &self,
        room_id: &RoomId,
        upto: StreamOrdering,
        limit: usize,
    ) -> Result<Vec<StoredEvent>> {
        let sql = format!(
            "SELECT {EVENT_COLUMNS} FROM events e JOIN rooms r ON r.room_id = e.room_id
             WHERE e.room_id = $1 AND e.stream_ordering <= $2 AND NOT e.rejected AND NOT e.outlier AND NOT e.soft_failed
             ORDER BY e.depth DESC, e.stream_ordering DESC LIMIT $3"
        );
        let rows: Vec<EventRow> = sqlx::query_as(sqlx::AssertSqlSafe(sql))
            .bind(room_id.as_str())
            .bind(upto)
            .bind(i64::try_from(limit).unwrap_or(i64::MAX))
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

#[cfg(test)]
mod search_tests {
    use super::*;

    #[test]
    fn terms_and_escaping() {
        assert_eq!(
            substring_terms("foo \"bar baz\" qu x", 3),
            vec!["foo".to_owned(), "bar baz".to_owned()]
        );
        assert_eq!(substring_terms("  ", 3), Vec::<String>::new());
        assert_eq!(like_escape("50%_a\\b"), "50\\%\\_a\\\\b");
    }
}
