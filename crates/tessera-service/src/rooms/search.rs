//! `/search`: full-text search over room events.

use std::collections::BTreeSet;

use ruma::{OwnedRoomId, OwnedUserId, UserId};
use tessera_store::{Direction, StoredEvent};

use super::timeline::StreamToken;
use crate::{Result, Services};

/// What to search for.
#[derive(Debug)]
pub struct SearchRequest {
    pub term: String,
    pub rooms: Option<Vec<OwnedRoomId>>,
    pub not_rooms: Vec<OwnedRoomId>,
    pub senders: Option<Vec<OwnedUserId>>,
    pub not_senders: Vec<OwnedUserId>,
    pub types: Option<Vec<String>>,
    pub not_types: Vec<String>,
    pub order_recent: bool,
    pub before_limit: usize,
    pub after_limit: usize,
    pub include_profile: bool,
    pub offset: usize,
    pub limit: usize,
}

/// One hit with its context.
#[derive(Debug)]
pub struct SearchResult {
    pub event: StoredEvent,
    pub rank: f64,
    pub before: Vec<StoredEvent>,
    pub after: Vec<StoredEvent>,
    pub start: StreamToken,
    pub end: StreamToken,
    /// `(user, displayname, avatar_url)` for senders in the result and context.
    pub profiles: Vec<(OwnedUserId, Option<String>, Option<String>)>,
}

/// A page of results.
#[derive(Debug)]
pub struct SearchPage {
    pub results: Vec<SearchResult>,
    pub count: usize,
    pub next_offset: Option<usize>,
}

impl Services {
    pub async fn search(&self, user: &UserId, req: SearchRequest) -> Result<SearchPage> {
        let joined = self.store.rooms_for_user(user, "join").await?;
        let rooms: Vec<OwnedRoomId> = joined
            .into_iter()
            .filter(|r| req.rooms.as_ref().is_none_or(|allow| allow.contains(r)))
            .filter(|r| !req.not_rooms.contains(r))
            .collect();
        let limit = req.limit.clamp(1, 100);
        let (hits, count) = self
            .store
            .search_events(&rooms, &req.term, req.order_recent, req.offset, limit)
            .await?;
        let mut results = Vec::with_capacity(hits.len());
        for hit in hits {
            let ev = hit.event;
            let ty = ev.pdu.event_type().to_string();
            if req.types.as_ref().is_some_and(|t| !t.contains(&ty))
                || req.not_types.contains(&ty)
                || req
                    .senders
                    .as_ref()
                    .is_some_and(|s| !s.iter().any(|u| u == ev.pdu.sender()))
                || req.not_senders.iter().any(|u| u == ev.pdu.sender())
            {
                continue;
            }
            let room_id = ev.pdu.room_id().to_owned();
            let before = if req.before_limit > 0 {
                self.store
                    .room_events_in_range(
                        &room_id,
                        ev.stream_ordering,
                        None,
                        Direction::Backwards,
                        req.before_limit,
                    )
                    .await?
            } else {
                Vec::new()
            };
            let after = if req.after_limit > 0 {
                self.store
                    .room_events_in_range(
                        &room_id,
                        ev.stream_ordering,
                        None,
                        Direction::Forwards,
                        req.after_limit,
                    )
                    .await?
            } else {
                Vec::new()
            };
            let start = StreamToken(
                before
                    .last()
                    .map_or(ev.stream_ordering, |e| e.stream_ordering),
            );
            let end = StreamToken(
                after
                    .last()
                    .map_or(ev.stream_ordering, |e| e.stream_ordering),
            );
            let mut profiles = Vec::new();
            if req.include_profile {
                let senders: BTreeSet<OwnedUserId> = std::iter::once(&ev)
                    .chain(before.iter())
                    .chain(after.iter())
                    .map(|e| e.pdu.sender().to_owned())
                    .collect();
                for s in senders {
                    let (name, avatar) = self.member_profile(&room_id, &s).await?;
                    profiles.push((s, name, avatar));
                }
            }
            results.push(SearchResult {
                event: ev,
                rank: hit.rank,
                before,
                after,
                start,
                end,
                profiles,
            });
        }
        let next_offset = (req.offset + limit < count).then_some(req.offset + limit);
        Ok(SearchPage {
            results,
            count,
            next_offset,
        })
    }

    /// Display name and avatar from a user's current membership in a room.
    pub async fn member_profile(
        &self,
        room_id: &ruma::RoomId,
        user: &UserId,
    ) -> Result<(Option<String>, Option<String>)> {
        let Some(id) = self
            .store
            .current_state_event(
                room_id,
                &ruma::events::StateEventType::RoomMember,
                user.as_str(),
            )
            .await?
        else {
            return Ok((None, None));
        };
        let Some(ev) = self.store.get_event(&id).await? else {
            return Ok((None, None));
        };
        let content: serde_json::Value =
            serde_json::from_str(ev.pdu.content().get()).unwrap_or_default();
        let get = |k: &str| content.get(k).and_then(|v| v.as_str()).map(str::to_owned);
        Ok((get("displayname"), get("avatar_url")))
    }
}
