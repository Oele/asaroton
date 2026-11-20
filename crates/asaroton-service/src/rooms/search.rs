//! `/search`: full-text search over room events.

use std::collections::BTreeSet;

use asaroton_store::{Direction, SearchFilter, StoredEvent};
use ruma::{OwnedRoomId, OwnedUserId, UserId};

use super::timeline::StreamToken;
use crate::{Result, Services};

/// What to search for.
#[derive(Debug)]
pub struct SearchRequest {
    pub term: String,
    pub rooms: Option<Vec<OwnedRoomId>>,
    pub not_rooms: Vec<OwnedRoomId>,
    /// Senders, types, `contains_url` and the `net.oele.search_filters` extension; the
    /// searcher's ignored users are added here.
    pub filter: SearchFilter,
    pub order_recent: bool,
    /// The events around each result, when the request asked for them.
    pub context: Option<SearchContext>,
    pub offset: usize,
    pub limit: usize,
}

/// `event_context` of a search request.
#[derive(Debug)]
pub struct SearchContext {
    pub before_limit: usize,
    pub after_limit: usize,
    pub include_profile: bool,
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
        let mut filter = req.filter;
        filter.ignored = self
            .ignored_users(user)
            .await?
            .iter()
            .map(ToString::to_string)
            .collect();
        let (hits, count) = self
            .store
            .search_events(
                &rooms,
                &asaroton_store::SearchQuery {
                    term: &req.term,
                    mode: self.config.search.mode,
                    min_length: self.config.search.min_length,
                    order_recent: req.order_recent,
                    offset: req.offset,
                    limit,
                    filter: &filter,
                },
            )
            .await?;
        let mut results = Vec::with_capacity(hits.len());
        let (before_limit, after_limit, include_profile) =
            req.context.as_ref().map_or((0, 0, false), |c| {
                (c.before_limit, c.after_limit, c.include_profile)
            });
        for hit in hits {
            let ev = hit.event;
            let room_id = ev.pdu.room_id().to_owned();
            let before = if before_limit > 0 {
                self.store
                    .room_events_in_range(
                        &room_id,
                        ev.stream_ordering,
                        None,
                        Direction::Backwards,
                        before_limit,
                    )
                    .await?
            } else {
                Vec::new()
            };
            let after = if after_limit > 0 {
                self.store
                    .room_events_in_range(
                        &room_id,
                        ev.stream_ordering,
                        None,
                        Direction::Forwards,
                        after_limit,
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
            if include_profile {
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
        // A full page has a next batch, as Synapse does and Complement expects (the next page may
        // then be empty); with the filters in the query every page but the last is full.
        let next_offset = (results.len() >= limit).then_some(req.offset + limit);
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
