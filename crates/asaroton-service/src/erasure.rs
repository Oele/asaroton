//! Account erasure: `erase: true` on `/account/deactivate` (Matrix v1.10).
//!
//! The spec: "any users (or servers) which join the room after the erasure request are served
//! redacted copies of the events sent by this account. Users which had visibility on those
//! events prior to the erasure are still able to see unredacted copies. No redactions are sent
//! and the erasure request is not shared over federation". The account's other data (account
//! data, push rules, filters, pushers, profile, email addresses) goes for good.
//!
//! Who had visibility before the erasure: a viewer whose first join or invite in the room is
//! older than the erasure, by the position in the events stream (for a server, the first of
//! any of its users). Which events they may see at all is still history visibility's business
//! and decided as before; this only picks the original or the redacted copy of what they get.
//!
//! A state event that the room's current state still holds goes out as it is. The current
//! state is served whole by `/state`, sync's `state` and the like, and a client takes the state
//! events of a timeline as state too: a redacted create event (before room version 11) would
//! lose the room version, a redacted name the room's name.
//!
//! Where it applies: everything that renders events for a client (`client_events` and
//! `client_event`: sync and sliding sync timelines, `/messages`, `/context`, `/event`,
//! `/relations`, `/threads`, `/search`, with the bundled aggregations and `prev_content`),
//! the sticky events sections, `/search`'s matching, and the events other servers fetch
//! (`/event`, `/backfill`, `/get_missing_events`). The erased accounts' sticky events are not
//! pushed to a server that joins.

use std::collections::HashMap;
use std::sync::{Arc, PoisonError};

use asaroton_store::{StoredEvent, StreamOrdering};
use ruma::{
    OwnedRoomId, OwnedUserId, RoomId, ServerName, UserId, events::StateEventType,
    room_version_rules::RoomVersionRules,
};
use serde_json::Value;

use crate::{Error, Result, Services};

/// What a redacted copy shows clients as its `unsigned.redacted_because`. Clients tell a
/// redacted event by that field (ruma, so matrix-rust-sdk, and matrix-js-sdk alike); without
/// it a message with empty content fails to parse instead of showing as deleted. No such event
/// exists, is stored or is sent: it says that the sender removed the event, with the event's
/// own time (not the time of the erasure) and an event ID derived from the target's, so that
/// every copy of one event shows the same.
fn erasure_redaction(pdu: &asaroton_core::Pdu) -> Value {
    use base64::Engine as _;
    use sha2::Digest as _;
    let target = pdu.event_id().as_str();
    let hash = sha2::Sha256::digest(format!("asaroton erasure {target}").as_bytes());
    let id = format!(
        "${}",
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(hash)
    );
    serde_json::json!({
        "type": "m.room.redaction",
        "event_id": id,
        "sender": pdu.sender(),
        "room_id": pdu.room_id(),
        "origin_server_ts": pdu.origin_server_ts(),
        "redacts": target,
        "content": { "redacts": target },
        "unsigned": {},
    })
}

/// The erased accounts, each with the events stream position of its erasure.
pub type ErasedUsers = HashMap<OwnedUserId, StreamOrdering>;

/// The erased accounts as last read from the store. They are few and looked at for every
/// timeline that is served, so they are kept in memory; an erasure drops the copy.
#[derive(Debug, Default)]
pub struct ErasedCache {
    users: std::sync::RwLock<Option<Arc<ErasedUsers>>>,
    /// Held while the store is read for the copy or written by an erasure, so that a copy
    /// read before an erasure cannot be put in place after it.
    turn: tokio::sync::Mutex<()>,
}

impl ErasedCache {
    fn get(&self) -> Option<Arc<ErasedUsers>> {
        self.users
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    fn set(&self, users: Option<Arc<ErasedUsers>>) {
        *self.users.write().unwrap_or_else(PoisonError::into_inner) = users;
    }
}

/// Who events are served to.
#[derive(Clone, Copy, Debug)]
pub enum Viewer<'a> {
    User(&'a UserId),
    Server(&'a ServerName),
}

/// Whether a viewer whose first join or invite in a room is at `first` (`None`: there is none)
/// gets redacted copies of the events of an account erased at `erased_at`: they did not have
/// visibility on those events before the erasure.
#[must_use]
pub fn sees_redacted(first: Option<StreamOrdering>, erased_at: StreamOrdering) -> bool {
    first.is_none_or(|f| f > erased_at)
}

/// The erasure rule for one viewer over one response. The common case, nobody erased, costs a
/// set lookup per event; otherwise each room asked about costs one query, once.
#[derive(Debug)]
pub struct ErasureCheck<'a> {
    erased: Arc<ErasedUsers>,
    viewer: Option<Viewer<'a>>,
    /// Per room, the viewer's first join or invite.
    first: HashMap<OwnedRoomId, Option<StreamOrdering>>,
    rules: HashMap<OwnedRoomId, RoomVersionRules>,
}

impl ErasureCheck<'_> {
    /// A check that keeps everything as it is (there is no viewer to judge for).
    #[must_use]
    pub fn none() -> Self {
        Self {
            erased: Arc::default(),
            viewer: None,
            first: HashMap::new(),
            rules: HashMap::new(),
        }
    }

    /// Whether no event can be affected: no viewer, or nobody erased.
    #[must_use]
    pub fn is_idle(&self) -> bool {
        self.viewer.is_none() || self.erased.is_empty()
    }

    /// Whether any of `events` was sent by an erased account (and there is a viewer), so that
    /// [`Self::copies`] may change something.
    #[must_use]
    pub fn concerns(&self, events: &[StoredEvent]) -> bool {
        !self.is_idle()
            && events
                .iter()
                .any(|e| self.erased.contains_key(e.pdu.sender()))
    }

    /// Whether the viewer gets redacted copies of `sender`'s events in `room_id`.
    pub async fn hides(
        &mut self,
        services: &Services,
        room_id: &RoomId,
        sender: &UserId,
    ) -> Result<bool> {
        let (Some(viewer), Some(&erased_at)) = (self.viewer, self.erased.get(sender)) else {
            return Ok(false);
        };
        if matches!(viewer, Viewer::User(u) if u == sender) {
            return Ok(false);
        }
        let first = if let Some(f) = self.first.get(room_id) {
            *f
        } else {
            let f = match viewer {
                Viewer::User(u) => services
                    .store
                    .first_member_positions(u, &[room_id.to_owned()])
                    .await?
                    .remove(room_id),
                Viewer::Server(s) => {
                    services
                        .store
                        .first_server_member_position(room_id, s)
                        .await?
                }
            };
            self.first.insert(room_id.to_owned(), f);
            f
        };
        Ok(sees_redacted(first, erased_at))
    }

    async fn rules(&mut self, services: &Services, room_id: &RoomId) -> Result<&RoomVersionRules> {
        if !self.rules.contains_key(room_id) {
            let room = services
                .store
                .get_room(room_id)
                .await?
                .ok_or_else(|| Error::internal(format!("no room version for {room_id}")))?;
            let rules = asaroton_core::room_version::rules(&room.room_version)?;
            self.rules.insert(room_id.to_owned(), rules);
        }
        self.rules
            .get(room_id)
            .ok_or_else(|| Error::internal("room version rules"))
    }

    /// The copy of `ev` the viewer is to get, when it is not `ev` itself: the redacted form of
    /// an event of an erased account that the viewer had no visibility on before the erasure.
    /// An event that is redacted already, and a state event the room's current state still
    /// holds, go out as they are.
    pub async fn copy_of(
        &mut self,
        services: &Services,
        ev: &StoredEvent,
    ) -> Result<Option<StoredEvent>> {
        let room_id = ev.pdu.room_id();
        if ev.redacted || !self.hides(services, room_id, ev.pdu.sender()).await? {
            return Ok(None);
        }
        if let Some((ty, key)) = ev.pdu.state_tuple()
            && services
                .store
                .current_state_event(room_id, &ty, key)
                .await?
                .as_deref()
                == Some(ev.event_id())
        {
            return Ok(None);
        }
        // Clients get the redaction to tell the copy by; servers redact by the algorithm.
        let because = match self.viewer {
            Some(Viewer::User(_)) => Some(asaroton_core::canonical::from_json_value(
                erasure_redaction(&ev.pdu),
            )?),
            _ => None,
        };
        let rules = self.rules(services, room_id).await?;
        let pdu = asaroton_core::redaction::redact_pdu_because(&ev.pdu, rules, because)?;
        Ok(Some(StoredEvent {
            pdu,
            redacted: true,
            ..ev.clone()
        }))
    }

    /// `events` as the viewer is to get them.
    pub async fn copies(
        &mut self,
        services: &Services,
        events: Vec<StoredEvent>,
    ) -> Result<Vec<StoredEvent>> {
        if !self.concerns(&events) {
            return Ok(events);
        }
        let mut out = Vec::with_capacity(events.len());
        for ev in events {
            out.push(match self.copy_of(services, &ev).await? {
                Some(copy) => copy,
                None => ev,
            });
        }
        Ok(out)
    }

    /// The content of an earlier `event_type` state event by `sender`, as the viewer is to get
    /// it in `unsigned.prev_content`: redacted like the event itself would be.
    pub async fn prev_content(
        &mut self,
        services: &Services,
        room_id: &RoomId,
        event_type: &StateEventType,
        sender: &UserId,
        content: Value,
    ) -> Result<Value> {
        if !self.hides(services, room_id, sender).await? {
            return Ok(content);
        }
        let rules = self.rules(services, room_id).await?;
        // What cannot be read as canonical JSON is left out rather than shown.
        let Ok(mut object) = asaroton_core::canonical::from_json_value(content) else {
            return Ok(Value::Object(serde_json::Map::new()));
        };
        ruma::canonical_json::redact_content_in_place(
            &mut object,
            &rules.redaction,
            event_type.to_string(),
        );
        Ok(asaroton_core::canonical::to_json_value(object))
    }
}

impl Services {
    /// The erased accounts, read from the store at first use.
    pub async fn erased_users(&self) -> Result<Arc<ErasedUsers>> {
        if let Some(users) = self.erased.get() {
            return Ok(users);
        }
        let _turn = self.erased.turn.lock().await;
        if let Some(users) = self.erased.get() {
            return Ok(users);
        }
        let users: Arc<ErasedUsers> =
            Arc::new(self.store.erased_users().await?.into_iter().collect());
        self.erased.set(Some(Arc::clone(&users)));
        Ok(users)
    }

    /// The erasure rule for `viewer` (none: everything stays as it is).
    pub async fn erasure_check<'a>(&self, viewer: Option<Viewer<'a>>) -> Result<ErasureCheck<'a>> {
        let Some(viewer) = viewer else {
            return Ok(ErasureCheck::none());
        };
        Ok(ErasureCheck {
            erased: self.erased_users().await?,
            viewer: Some(viewer),
            first: HashMap::new(),
            rules: HashMap::new(),
        })
    }

    /// `events` as `viewer` is to get them (see [`ErasureCheck::copy_of`]).
    pub async fn erasure_copies(
        &self,
        viewer: Viewer<'_>,
        events: Vec<StoredEvent>,
    ) -> Result<Vec<StoredEvent>> {
        self.erasure_check(Some(viewer))
            .await?
            .copies(self, events)
            .await
    }

    /// `events` without those `user` would get as redacted copies of an erased account's: for
    /// sticky events, whose point is their content.
    pub async fn without_erased_for(
        &self,
        user: &UserId,
        events: Vec<StoredEvent>,
    ) -> Result<Vec<StoredEvent>> {
        let mut check = self.erasure_check(Some(Viewer::User(user))).await?;
        if check.is_idle() {
            return Ok(events);
        }
        let mut out = Vec::with_capacity(events.len());
        for ev in events {
            if !check.hides(self, ev.pdu.room_id(), ev.pdu.sender()).await? {
                out.push(ev);
            }
        }
        Ok(out)
    }

    /// For `/search` by `viewer` in `rooms`: the erased accounts whose events it is not to
    /// find, per room. A hit would give away what a redacted copy hides.
    pub(crate) async fn erased_out_of_search(
        &self,
        viewer: &UserId,
        rooms: &[OwnedRoomId],
    ) -> Result<Vec<(OwnedRoomId, OwnedUserId)>> {
        let erased = self.erased_users().await?;
        if erased.is_empty() {
            return Ok(Vec::new());
        }
        // The rooms each erased account was ever in, among those searched.
        let mut candidates: Vec<(OwnedRoomId, &OwnedUserId, StreamOrdering)> = Vec::new();
        for (user, &erased_at) in erased.iter() {
            if user == viewer {
                continue;
            }
            for m in self.store.memberships_for_user(user).await? {
                if rooms.contains(&m.room_id) {
                    candidates.push((m.room_id, user, erased_at));
                }
            }
        }
        if candidates.is_empty() {
            return Ok(Vec::new());
        }
        let mut in_rooms: Vec<OwnedRoomId> = candidates.iter().map(|c| c.0.clone()).collect();
        in_rooms.sort_unstable();
        in_rooms.dedup();
        let first = self.store.first_member_positions(viewer, &in_rooms).await?;
        Ok(candidates
            .into_iter()
            .filter(|(room, _, erased_at)| sees_redacted(first.get(room).copied(), *erased_at))
            .map(|(room, user, _)| (room, user.clone()))
            .collect())
    }

    /// Erase an account that has just been deactivated: mark it, so that its events are served
    /// redacted to whoever joins a room from now on, and remove its other data (account data
    /// with push rules, filters, pushers, profile). Its email addresses went with the
    /// deactivation. Nothing is sent: no redactions, and other servers are not told.
    pub async fn erase_account(&self, user_id: &UserId) -> Result<()> {
        let erased_at = {
            let _turn = self.erased.turn.lock().await;
            let at = self
                .store
                .mark_erased(user_id, asaroton_store::now_ts())
                .await?;
            self.erased.set(None);
            at
        };
        tracing::info!(%user_id, erased_at, "account erased");
        self.store.erase_account_data(user_id).await?;
        self.push_rules_cache.remove(user_id);
        self.store
            .delete_pushers_except_token(user_id, None)
            .await?;
        self.pushers_generation
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.clear_profile(user_id).await
    }
}

#[cfg(test)]
mod tests {
    use super::sees_redacted;

    #[test]
    fn who_was_there_before_the_erasure_keeps_the_originals() {
        // Erased at position 100.
        assert!(!sees_redacted(Some(40), 100), "joined long before");
        assert!(
            !sees_redacted(Some(100), 100),
            "joined at the last event before"
        );
        assert!(
            !sees_redacted(Some(-7), 100),
            "joined in backfilled history"
        );
        assert!(sees_redacted(Some(101), 100), "joined right after");
        assert!(
            sees_redacted(None, 100),
            "never joined or invited (a peeker)"
        );
    }
}
