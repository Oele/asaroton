//! Event relations: `/relations`, `/threads` and bundled aggregations.

use ruma::{EventId, OwnedEventId, RoomId, UserId};
use serde_json::{Value, json};
use tessera_store::{Direction, RelationRow, StoredEvent, ThreadSummary};

use super::timeline::StreamToken;
use crate::{Error, Result, Services};

/// A page of related events.
#[derive(Debug)]
pub struct RelationsPage {
    pub events: Vec<StoredEvent>,
    pub next_batch: Option<StreamToken>,
    pub prev_batch: Option<StreamToken>,
}

impl Services {
    /// Events relating to `event_id`, paged by stream token.
    #[allow(clippy::too_many_arguments)]
    pub async fn relations(
        &self,
        room_id: &RoomId,
        user: &UserId,
        event_id: &EventId,
        rel_type: Option<&str>,
        event_type: Option<&str>,
        recurse: bool,
        from: Option<StreamToken>,
        to: Option<StreamToken>,
        direction: Direction,
        limit: usize,
    ) -> Result<RelationsPage> {
        // The target must exist in this room and be visible.
        self.get_room_event(room_id, user, event_id).await?;
        let limit = limit.clamp(1, 1000);
        // Tokens are positions after an event (see `Services::messages`).
        let (from_pos, to_pos) = match direction {
            Direction::Backwards => (from.map(|t| t.0 + 1), to.map(|t| t.0 + 1)),
            Direction::Forwards => (from.map(|t| t.0), to.map(|t| t.0)),
        };
        let events = self
            .store
            .relations(
                room_id, event_id, rel_type, event_type, recurse, from_pos, to_pos, direction,
                limit,
            )
            .await?;
        let next_batch = if events.len() < limit {
            None
        } else {
            events.last().map(|e| match direction {
                Direction::Backwards => StreamToken(e.stream_ordering - 1),
                Direction::Forwards => StreamToken(e.stream_ordering),
            })
        };
        let prev_batch = from;
        Ok(RelationsPage {
            events,
            next_batch,
            prev_batch,
        })
    }

    /// Thread roots in a room, most recently active first.
    pub async fn threads(
        &self,
        room_id: &RoomId,
        user: &UserId,
        participated_only: bool,
        from: Option<StreamToken>,
        limit: usize,
    ) -> Result<(Vec<ThreadSummary>, Option<StreamToken>)> {
        if !self.can_read_room(room_id, user).await? {
            return Err(Error::forbidden("You are not allowed to read this room."));
        }
        let limit = limit.clamp(1, 1000);
        let threads = self
            .store
            .threads(
                room_id,
                participated_only.then_some(user),
                from.map(|t| t.0),
                limit,
            )
            .await?;
        let next = if threads.len() < limit {
            None
        } else {
            threads
                .last()
                .map(|t| StreamToken(t.latest_stream_ordering))
        };
        Ok((threads, next))
    }

    /// The `unsigned.m.relations` bundle for an event, if it has any relations.
    pub async fn bundled_aggregations(
        &self,
        ev: &StoredEvent,
        user: Option<&UserId>,
    ) -> Result<Option<Value>> {
        let rows = self.store.relation_summary(ev.event_id()).await?;
        self.aggregations_from_rows(ev, rows, user).await
    }

    /// `bundled_aggregations` from relation rows already fetched.
    pub async fn aggregations_from_rows(
        &self,
        ev: &StoredEvent,
        rows: Vec<RelationRow>,
        user: Option<&UserId>,
    ) -> Result<Option<Value>> {
        if rows.is_empty() {
            return Ok(None);
        }
        let mut out = serde_json::Map::new();

        // m.replace: the latest edit by the original sender, of the same type.
        let mut edits: Vec<&RelationRow> = rows
            .iter()
            .filter(|r| {
                r.rel_type == "m.replace"
                    && r.sender == ev.pdu.sender()
                    && r.event_type == ev.pdu.event_type().to_string()
            })
            .collect();
        edits.sort_by(|a, b| {
            a.origin_server_ts
                .cmp(&b.origin_server_ts)
                .then_with(|| a.event_id.cmp(&b.event_id))
        });
        if let Some(latest) = edits.last()
            && let Some(edit) = self.store.get_event(&latest.event_id).await?
        {
            out.insert("m.replace".into(), self.client_event_plain(&edit));
        }

        // m.thread: summary of the thread rooted here.
        let thread: Vec<&RelationRow> = rows.iter().filter(|r| r.rel_type == "m.thread").collect();
        if let Some(latest) = thread.iter().max_by_key(|r| r.stream_ordering)
            && let Some(latest_ev) = self.store.get_event(&latest.event_id).await?
        {
            let participated =
                user.is_some_and(|u| u == ev.pdu.sender() || thread.iter().any(|r| r.sender == u));
            out.insert(
                "m.thread".into(),
                json!({
                    "latest_event": self.client_event_plain(&latest_ev),
                    "count": thread.len(),
                    "current_user_participated": participated,
                }),
            );
        }

        // m.reference: the IDs of referencing events.
        let refs: Vec<&OwnedEventId> = rows
            .iter()
            .filter(|r| r.rel_type == "m.reference")
            .map(|r| &r.event_id)
            .collect();
        if !refs.is_empty() {
            let chunk: Vec<Value> = refs.iter().map(|id| json!({ "event_id": id })).collect();
            out.insert("m.reference".into(), json!({ "chunk": chunk }));
        }

        Ok(if out.is_empty() {
            None
        } else {
            Some(Value::Object(out))
        })
    }

    /// Client-format JSON with `unsigned.age`, without aggregations.
    pub fn client_event_plain(&self, ev: &StoredEvent) -> Value {
        let mut v = ev.pdu.to_client_json();
        let now = tessera_store::now_ts();
        let age =
            now.saturating_sub(i64::try_from(u64::from(ev.pdu.origin_server_ts().0)).unwrap_or(0));
        if let Some(u) = v.as_object_mut().and_then(|o| {
            o.entry("unsigned")
                .or_insert_with(|| json!({}))
                .as_object_mut()
        }) {
            u.insert("age".into(), json!(age));
        }
        v
    }

    /// Client-format JSON with `unsigned.age` and bundled aggregations.
    pub async fn client_event(&self, ev: &StoredEvent, user: Option<&UserId>) -> Result<Value> {
        let mut v = self.client_event_plain(ev);
        if let Some(agg) = self.bundled_aggregations(ev, user).await?
            && let Some(u) = v
                .as_object_mut()
                .and_then(|o| o.get_mut("unsigned"))
                .and_then(Value::as_object_mut)
        {
            u.insert("m.relations".into(), agg);
        }
        Ok(v)
    }

    /// `client_event` for a batch.
    pub async fn client_events(
        &self,
        events: &[StoredEvent],
        user: Option<&UserId>,
    ) -> Result<Vec<Value>> {
        let mut out = Vec::with_capacity(events.len());
        // One relations query for the whole batch.
        let ids: Vec<ruma::OwnedEventId> = events.iter().map(|e| e.event_id().to_owned()).collect();
        let mut summaries = self.store.relation_summaries(&ids).await?;
        for e in events {
            let mut v = self.client_event_plain(e);
            let rows = summaries.remove(e.event_id()).unwrap_or_default();
            if let Some(agg) = self.aggregations_from_rows(e, rows, user).await?
                && let Some(u) = v
                    .as_object_mut()
                    .and_then(|o| o.get_mut("unsigned"))
                    .and_then(Value::as_object_mut)
            {
                u.insert("m.relations".into(), agg);
            }
            out.push(v);
        }
        for (v, e) in out.iter_mut().zip(events) {
            if let Some((ty, key)) = e.pdu.state_tuple()
                && let Some((prev, sender)) = self.previous_state_content(e, &ty, key).await?
                && let Some(u) = v
                    .as_object_mut()
                    .and_then(|o| o.get_mut("unsigned"))
                    .and_then(Value::as_object_mut)
            {
                u.insert("prev_content".into(), prev);
                u.insert("prev_sender".into(), json!(sender));
            }
        }
        if let Some(user) = user {
            for (v, m) in out
                .iter_mut()
                .zip(self.membership_at_events(user, events).await?)
            {
                if let Some(u) = v
                    .as_object_mut()
                    .and_then(|o| o.get_mut("unsigned"))
                    .and_then(Value::as_object_mut)
                {
                    u.insert("membership".into(), json!(m));
                }
            }
        }
        Ok(out)
    }
}

impl Services {
    /// Content of the state event `ev` replaced, if any (`unsigned.prev_content`).
    async fn previous_state_content(
        &self,
        ev: &StoredEvent,
        ty: &ruma::events::StateEventType,
        key: &str,
    ) -> Result<Option<(Value, ruma::OwnedUserId)>> {
        let Some(group) = self.store.state_group_before(ev.event_id()).await? else {
            return Ok(None);
        };
        let prev_id = self
            .store
            .state_at_group(group)
            .await?
            .into_iter()
            .find(|e| e.event_type == *ty && e.state_key == key)
            .map(|e| e.event_id);
        let Some(prev_id) = prev_id else {
            return Ok(None);
        };
        Ok(self.store.get_event(&prev_id).await?.and_then(|p| {
            serde_json::from_str(p.pdu.content().get())
                .ok()
                .map(|c| (c, p.pdu.sender().to_owned()))
        }))
    }
}
