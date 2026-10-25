//! The incoming event pipeline: everything a PDU goes through between arriving from another
//! server and being part of the room.
//!
//! Steps (spec: "Checks performed on receipt of a PDU"):
//! 1. parse for the room version, check format and size;
//! 2. verify hashes and signatures (redact on hash mismatch);
//! 3. make sure the auth events and prev events are known, fetching what is missing;
//! 4. compute the state before the event (state resolution across prev events);
//! 5. authorise against the auth events, then against that state; soft-fail against the
//!    current state;
//! 6. persist with its state group, update forward extremities and the current state.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

use ruma::{
    CanonicalJsonObject, OwnedEventId, OwnedRoomId, OwnedServerName, OwnedUserId, RoomId,
    RoomVersionId, ServerName, UInt,
    api::federation::{
        event::{get_event, get_missing_events, get_room_state_ids},
        transactions::{edu::Edu, send_transaction_message},
    },
    events::StateEventType,
    room_version_rules::RoomVersionRules,
    signatures::Verified,
    state_res::{StateMap, utils::event_id_set::EventIdSet},
};
use serde_json::Value;
use tessera_core::{Pdu, auth, redaction, room_version, signing, state};
use tessera_service::{Notification, Services};
use tessera_store::{CurrentStateDelta, EventState, PersistEvent, StateGroupId, StoredEvent};

use crate::{Error, Federation, Result};

/// How deep we chase missing prev events through `get_missing_events` before falling back
/// to fetching the state at the gap.
const MAX_MISSING_EVENTS: u32 = 10;
/// Bound on recursive auth-event fetching.
const MAX_AUTH_FETCH_DEPTH: u32 = 50;

/// Outcome of processing one PDU.
#[derive(Debug)]
pub enum Outcome {
    Accepted,
    Rejected(String),
    SoftFailed,
    AlreadyKnown,
}

impl Federation {
    // ------------------------------------------------------------------ transactions

    /// `PUT /send/{txnId}`.
    pub async fn handle_transaction(
        &self,
        origin: &ServerName,
        req: send_transaction_message::v1::Request,
    ) -> Result<send_transaction_message::v1::Response> {
        if req.origin != origin {
            return Err(Error::Unauthorized(
                "transaction origin does not match signature".into(),
            ));
        }
        let txn_id = req.transaction_id.to_string();
        if let Some(prev) = self
            .services
            .store
            .received_transaction(origin, &txn_id)
            .await?
        {
            let pdus = serde_json::from_value(prev).unwrap_or_default();
            return Ok(send_transaction_message::v1::Response::new(pdus));
        }

        let mut results: BTreeMap<OwnedEventId, std::result::Result<(), String>> = BTreeMap::new();
        // Group PDUs by room, keeping order, so each room is processed under its lock.
        let mut by_room: BTreeMap<OwnedRoomId, Vec<CanonicalJsonObject>> = BTreeMap::new();
        let mut unparseable = 0usize;
        for raw in &req.pdus {
            let Ok(obj) = tessera_core::canonical::from_str(raw.get()) else {
                unparseable += 1;
                continue;
            };
            let Some(room_id) = obj
                .get("room_id")
                .and_then(|r| r.as_str())
                .and_then(|r| OwnedRoomId::try_from(r).ok())
            else {
                unparseable += 1;
                continue;
            };
            by_room.entry(room_id).or_default().push(obj);
        }
        if unparseable > 0 {
            tracing::warn!(%origin, unparseable, "transaction contained unparseable PDUs");
        }
        for (room_id, pdus) in by_room {
            for obj in pdus {
                let event_id_hint = obj
                    .get("event_id")
                    .and_then(|e| e.as_str())
                    .map(str::to_owned);
                match self.handle_incoming_pdu(origin, &room_id, obj, false).await {
                    Ok((event_id, Outcome::Rejected(reason))) => {
                        results.insert(event_id, Err(reason));
                    }
                    Ok((event_id, _)) => {
                        results.insert(event_id, Ok(()));
                    }
                    Err(e) => {
                        tracing::warn!(%origin, %room_id, error = %e, "failed to process PDU");
                        if let Some(id) = event_id_hint.and_then(|s| OwnedEventId::try_from(s).ok())
                        {
                            results.insert(id, Err(e.to_string()));
                        }
                    }
                }
            }
        }

        for edu in &req.edus {
            match edu.deserialize() {
                Ok(edu) => {
                    if let Err(e) = self.handle_edu(origin, edu).await {
                        tracing::warn!(%origin, error = %e, "failed to process EDU");
                    }
                }
                Err(e) => tracing::debug!(%origin, error = %e, "ignoring unknown EDU"),
            }
        }

        let resp = send_transaction_message::v1::Response::new(results);
        let _ = self
            .services
            .store
            .store_received_transaction(
                origin,
                &txn_id,
                &serde_json::to_value(&resp.pdus).unwrap_or(Value::Null),
            )
            .await;
        Ok(resp)
    }

    // ------------------------------------------------------------------ PDUs

    /// Process one PDU for a room we are in. Returns the event ID and what happened to it.
    pub async fn handle_incoming_pdu(
        &self,
        origin: &ServerName,
        room_id: &RoomId,
        raw: CanonicalJsonObject,
        backfilled: bool,
    ) -> Result<(OwnedEventId, Outcome)> {
        let services = &self.services;
        let Some(room) = services.store.get_room(room_id).await? else {
            return Err(Error::Rejected("unknown room".into()));
        };
        let rules = room_version::rules(&room.room_version)?;
        let pdu = self.parse_and_verify(origin, raw, &rules).await?;
        if pdu.room_id() != room_id {
            return Err(Error::Rejected("event room_id does not match".into()));
        }
        // Only servers that are in the room may send us events for it; the sender's server must
        // be the origin unless the event is a join authorised by someone on our side.
        if pdu.sender().server_name() != origin && !is_join_via(&pdu, origin) {
            return Err(Error::Rejected(format!(
                "sender {} is not on origin {origin}",
                pdu.sender()
            )));
        }
        if !self.we_are_in_room(room_id).await? {
            return Err(Error::Rejected("this server is not in the room".into()));
        }

        if let Some(existing) = services.store.get_event(pdu.event_id()).await?
            && !existing.outlier
        {
            return Ok((pdu.event_id().to_owned(), Outcome::AlreadyKnown));
        }

        let _guard = services.lock_room(room_id).await;
        let outcome = self
            .process_pdu(origin, pdu.clone(), &rules, backfilled)
            .await?;
        Ok((pdu.event_id().to_owned(), outcome))
    }

    /// Parse a PDU, check its format and verify hashes and signatures. A content-hash mismatch
    /// yields the redacted event, per spec.
    pub async fn parse_and_verify(
        &self,
        origin: &ServerName,
        raw: CanonicalJsonObject,
        rules: &RoomVersionRules,
    ) -> Result<Pdu> {
        let pdu = Pdu::from_canonical(raw, rules)
            .map_err(|e| Error::Rejected(format!("malformed event: {e}")))?;
        auth::check_format(&pdu, rules)
            .map_err(|e| Error::Rejected(format!("invalid event: {e}")))?;
        let servers = signing::required_signing_servers(pdu.raw(), rules)
            .map_err(|e| Error::Rejected(format!("cannot determine signing servers: {e}")))?;
        let min_valid = i64::try_from(u64::from(pdu.origin_server_ts().0)).unwrap_or(0);
        let mut keys = ruma::signatures::PublicKeyMap::new();
        for server in servers {
            let set = self
                .public_keys_for(&server, &[], min_valid)
                .await
                .map_err(|e| {
                    Error::Rejected(format!(
                        "no keys to verify signature of {server} (via {origin}): {e}"
                    ))
                })?;
            keys.insert(server.to_string(), set);
        }
        match signing::verify_event(&keys, pdu.raw(), rules) {
            Ok(Verified::All) => Ok(pdu),
            Ok(Verified::Signatures) => {
                tracing::warn!(event_id = %pdu.event_id(), "content hash mismatch; redacting");
                Ok(redaction::redact_pdu(&pdu, rules, None)?)
            }
            Err(e) => Err(Error::Rejected(format!(
                "signature verification failed: {e}"
            ))),
        }
    }

    async fn we_are_in_room(&self, room_id: &RoomId) -> Result<bool> {
        let members = self.services.store.members(room_id, Some("join")).await?;
        Ok(members.iter().any(|m| self.services.is_local(&m.user_id)))
    }

    /// The main pipeline for a verified PDU in a known room. Caller holds the room lock.
    async fn process_pdu(
        &self,
        origin: &ServerName,
        pdu: Pdu,
        rules: &RoomVersionRules,
        backfilled: bool,
    ) -> Result<Outcome> {
        let services = &self.services;
        let room_id = pdu.room_id().to_owned();

        // 3a. auth events
        self.ensure_auth_events(origin, &room_id, &pdu, rules, 0)
            .await?;

        // 3b. prev events
        if !backfilled {
            self.fetch_missing_prev_events(origin, &room_id, &pdu, rules)
                .await?;
        }

        // 4. state before the event
        let (state_before, before_group) = self
            .state_before_event(origin, &room_id, &pdu, rules)
            .await?;

        // 5. authorisation
        let fetch = |id: &ruma::EventId| self.cached_event(id);
        let mut rejected_reason: Option<String> = None;
        if let Err(e) = auth::check_state_independent(&pdu, rules, fetch) {
            rejected_reason = Some(e.to_string());
        } else if let Err(e) =
            auth::check_against_auth_events(&pdu, rules, |id| self.cached_event(id))
        {
            rejected_reason = Some(e.to_string());
        } else if let Err(e) = auth::check_against_state(&pdu, rules, &state_before) {
            rejected_reason = Some(e.to_string());
        }

        if let Some(reason) = rejected_reason {
            tracing::info!(event_id = %pdu.event_id(), %reason, "rejecting event");
            services
                .store
                .persist_event(PersistEvent {
                    pdu: pdu.with_rejected(true),
                    state: EventState::Outlier,
                    soft_failed: false,
                    backfilled,
                    current_state_delta: None,
                    forward_extremities: None,
                })
                .await?;
            return Ok(Outcome::Rejected(reason));
        }

        // Soft fail: passes its own history but not the current state.
        let current = self.current_state_pdus(&room_id).await?;
        let soft_failed = auth::check_against_state(&pdu, rules, &current).is_err();

        // 6. persist with state group
        let state = match pdu.state_tuple() {
            Some((ty, key)) => EventState::New {
                prev: before_group,
                delta: match before_group {
                    Some(_) => vec![((ty, key.to_owned()), pdu.event_id().to_owned())],
                    None => {
                        let mut full: Vec<_> = state::state_map_from_pdus(state_before.values())
                            .into_iter()
                            .collect();
                        full.push(((ty, key.to_owned()), pdu.event_id().to_owned()));
                        full
                    }
                },
            },
            None => match before_group {
                Some(g) => EventState::Existing(g),
                None => EventState::New {
                    prev: None,
                    delta: state::state_map_from_pdus(state_before.values())
                        .into_iter()
                        .collect(),
                },
            },
        };

        let (delta, extremities) = if soft_failed || backfilled {
            (None, None)
        } else {
            self.new_current_state(&room_id, &pdu, rules, &state_before)
                .await?
        };

        let persisted = services
            .store
            .persist_event(PersistEvent {
                pdu: pdu.clone(),
                state,
                soft_failed,
                backfilled,
                current_state_delta: delta,
                forward_extremities: extremities,
            })
            .await?;
        self.event_cache
            .insert(pdu.event_id().to_owned(), pdu.clone());

        if pdu.event_type() == &ruma::events::TimelineEventType::RoomRedaction && !soft_failed {
            self.apply_remote_redaction(&room_id, &pdu, rules, &current)
                .await?;
        }
        if !soft_failed {
            services.notifier.notify(Notification::RoomEvent {
                room_id: room_id.clone(),
                stream_ordering: persisted.stream_ordering,
            });
        }
        Ok(if soft_failed {
            Outcome::SoftFailed
        } else {
            Outcome::Accepted
        })
    }

    /// Look up an event in the in-memory cache or the store (blocking-free: the cache is
    /// populated by the fetch steps before auth checks run).
    fn cached_event(&self, id: &ruma::EventId) -> Option<Pdu> {
        self.event_cache.get(id).map(|p| p.clone())
    }

    /// Load events into the cache from the store.
    async fn warm_cache(&self, ids: &[OwnedEventId]) -> Result<()> {
        let missing: Vec<OwnedEventId> = ids
            .iter()
            .filter(|id| !self.event_cache.contains_key(*id))
            .cloned()
            .collect();
        if missing.is_empty() {
            return Ok(());
        }
        for ev in self.services.store.get_events(&missing).await? {
            self.event_cache
                .insert(ev.pdu.event_id().to_owned(), ev.pdu);
        }
        Ok(())
    }

    /// Make sure every auth event (transitively) is stored, fetching from `origin` if needed.
    async fn ensure_auth_events(
        &self,
        origin: &ServerName,
        room_id: &RoomId,
        pdu: &Pdu,
        rules: &RoomVersionRules,
        depth: u32,
    ) -> Result<()> {
        if depth > MAX_AUTH_FETCH_DEPTH {
            return Err(Error::Rejected("auth chain too deep".into()));
        }
        let mut needed: Vec<OwnedEventId> = pdu.auth_events().to_vec();
        if !rules.event_format.allow_room_create_in_auth_events
            && !pdu.is_create()
            && let Ok(create_id) = tessera_core::pdu::create_event_id_for_room(room_id)
        {
            needed.push(create_id);
        }
        self.warm_cache(&needed).await?;
        let have: HashSet<OwnedEventId> = self
            .services
            .store
            .have_events(&needed)
            .await?
            .into_iter()
            .collect();
        for id in needed {
            if have.contains(&id) {
                continue;
            }
            let fetched = self.fetch_event(origin, &id, rules).await?;
            // Recurse first so its own auth events exist, then persist as an outlier.
            Box::pin(self.ensure_auth_events(origin, room_id, &fetched, rules, depth + 1)).await?;
            let rejected =
                auth::check_against_auth_events(&fetched, rules, |i| self.cached_event(i)).is_err();
            self.services
                .store
                .persist_event(PersistEvent {
                    pdu: fetched.with_rejected(rejected),
                    state: EventState::Outlier,
                    soft_failed: false,
                    backfilled: false,
                    current_state_delta: None,
                    forward_extremities: None,
                })
                .await?;
            self.event_cache.insert(
                fetched.event_id().to_owned(),
                fetched.with_rejected(rejected),
            );
        }
        Ok(())
    }

    /// `GET /event/{id}` from a server, verified.
    pub async fn fetch_event(
        &self,
        origin: &ServerName,
        event_id: &ruma::EventId,
        rules: &RoomVersionRules,
    ) -> Result<Pdu> {
        let resp = self
            .client
            .send(origin, get_event::v1::Request::new(event_id.to_owned()))
            .await?;
        let obj = tessera_core::canonical::from_str(resp.pdu.get())?;
        let pdu = self.parse_and_verify(origin, obj, rules).await?;
        if pdu.event_id() != event_id {
            return Err(Error::Protocol(format!(
                "{origin} returned {} for {event_id}",
                pdu.event_id()
            )));
        }
        Ok(pdu)
    }

    /// Fetch and process prev events we do not have, newest-first via `get_missing_events`.
    async fn fetch_missing_prev_events(
        &self,
        origin: &ServerName,
        room_id: &RoomId,
        pdu: &Pdu,
        rules: &RoomVersionRules,
    ) -> Result<()> {
        let have: HashSet<OwnedEventId> = self
            .services
            .store
            .have_events(pdu.prev_events())
            .await?
            .into_iter()
            .collect();
        let missing: Vec<OwnedEventId> = pdu
            .prev_events()
            .iter()
            .filter(|p| !have.contains(*p))
            .cloned()
            .collect();
        if missing.is_empty() {
            return Ok(());
        }
        let earliest = self.services.store.forward_extremities(room_id).await?;
        let min_depth = UInt::from(0u32);
        let mut req =
            get_missing_events::v1::Request::new(room_id.to_owned(), earliest, missing.clone());
        req.limit = UInt::from(MAX_MISSING_EVENTS);
        req.min_depth = min_depth;
        let resp = match self.client.send_long(origin, req).await {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(%origin, error = %e, "get_missing_events failed");
                return Ok(());
            }
        };
        // Oldest first, so each event's prev events are processed before it.
        let mut fetched: Vec<Pdu> = Vec::new();
        for raw in resp.events {
            if let Ok(obj) = tessera_core::canonical::from_str(raw.get())
                && let Ok(p) = self.parse_and_verify(origin, obj, rules).await
                && p.room_id() == room_id
            {
                fetched.push(p);
            }
        }
        fetched.sort_by_key(|p| u64::from(p.depth()));
        let mut queue: VecDeque<Pdu> = fetched.into();
        while let Some(p) = queue.pop_front() {
            if self
                .services
                .store
                .get_event(p.event_id())
                .await?
                .is_some_and(|e| !e.outlier)
            {
                continue;
            }
            // Each fetched event goes through the full pipeline (without further prev-chasing
            // beyond what its own processing does).
            if let Err(e) = Box::pin(self.process_pdu(origin, p.clone(), rules, false)).await {
                tracing::warn!(event_id = %p.event_id(), error = %e, "processing fetched event failed");
            }
        }
        Ok(())
    }

    /// The room state immediately before `pdu`, resolved across its prev events, and the state
    /// group holding it if a single existing group does.
    async fn state_before_event(
        &self,
        origin: &ServerName,
        room_id: &RoomId,
        pdu: &Pdu,
        rules: &RoomVersionRules,
    ) -> Result<(HashMap<(StateEventType, String), Pdu>, Option<StateGroupId>)> {
        if pdu.is_create() {
            return Ok((HashMap::new(), None));
        }
        let mut state_maps: Vec<StateMap<OwnedEventId>> = Vec::new();
        let mut groups: Vec<Option<StateGroupId>> = Vec::new();
        for prev_id in pdu.prev_events() {
            match self.services.store.get_event(prev_id).await? {
                Some(prev) if prev.state_group.is_some() => {
                    let g = prev.state_group;
                    let entries = self
                        .services
                        .store
                        .state_at_group(g.unwrap_or_default())
                        .await?;
                    state_maps.push(
                        entries
                            .into_iter()
                            .map(|e| ((e.event_type, e.state_key), e.event_id))
                            .collect(),
                    );
                    groups.push(g);
                }
                _ => {
                    // Unknown prev or outlier: ask the origin for the state at that point.
                    let (map, _) = self.fetch_state_at(origin, room_id, prev_id, rules).await?;
                    state_maps.push(map);
                    groups.push(None);
                }
            }
        }
        if state_maps.is_empty() {
            return Err(Error::Rejected("event has no prev events".into()));
        }
        let resolved: StateMap<OwnedEventId> = if state_maps.len() == 1 {
            state_maps.remove(0)
        } else {
            self.resolve_states(room_id, rules, &state_maps).await?
        };
        let group = if groups.len() == 1 { groups[0] } else { None };
        let ids: Vec<OwnedEventId> = resolved.values().cloned().collect();
        self.warm_cache(&ids).await?;
        let mut out = HashMap::new();
        for ((ty, key), id) in resolved {
            if let Some(p) = self.cached_event(&id) {
                out.insert((ty, key), p);
            }
        }
        Ok((out, group))
    }

    /// Fetch `/state_ids` at an event from the origin, make sure we have all those events
    /// (persisting them as outliers), and return the state map.
    async fn fetch_state_at(
        &self,
        origin: &ServerName,
        room_id: &RoomId,
        event_id: &ruma::EventId,
        rules: &RoomVersionRules,
    ) -> Result<(StateMap<OwnedEventId>, Vec<OwnedEventId>)> {
        let req = get_room_state_ids::v1::Request::new(event_id.to_owned(), room_id.to_owned());
        let resp = self.client.send_long(origin, req).await?;
        let mut all: Vec<OwnedEventId> = resp.auth_chain_ids.clone();
        all.extend(resp.pdu_ids.iter().cloned());
        let have: HashSet<OwnedEventId> = self
            .services
            .store
            .have_events(&all)
            .await?
            .into_iter()
            .collect();
        let mut fetched: Vec<Pdu> = Vec::new();
        for id in all.iter().filter(|i| !have.contains(*i)) {
            match self.fetch_event(origin, id, rules).await {
                Ok(p) => fetched.push(p),
                Err(e) => tracing::warn!(%id, error = %e, "could not fetch state event"),
            }
        }
        // Persist in dependency order (auth events first) as outliers.
        fetched.sort_by_key(|p| u64::from(p.depth()));
        for p in &fetched {
            self.event_cache.insert(p.event_id().to_owned(), p.clone());
        }
        for p in fetched {
            let rejected =
                auth::check_against_auth_events(&p, rules, |i| self.cached_event(i)).is_err();
            self.services
                .store
                .persist_event(PersistEvent {
                    pdu: p.with_rejected(rejected),
                    state: EventState::Outlier,
                    soft_failed: false,
                    backfilled: false,
                    current_state_delta: None,
                    forward_extremities: None,
                })
                .await?;
        }
        self.warm_cache(&resp.pdu_ids).await?;
        let mut map = StateMap::new();
        for id in &resp.pdu_ids {
            if let Some(p) = self.cached_event(id)
                && let Some((ty, key)) = p.state_tuple()
            {
                map.insert((ty, key.to_owned()), id.clone());
            }
        }
        Ok((map, resp.auth_chain_ids))
    }

    /// State resolution over several state maps of one room.
    pub async fn resolve_states(
        &self,
        room_id: &RoomId,
        rules: &RoomVersionRules,
        maps: &[StateMap<OwnedEventId>],
    ) -> Result<StateMap<OwnedEventId>> {
        if state::needs_state_res_v1(rules) {
            // Room versions 1: ruma does not implement v1 resolution. Take the most recent
            // state map as an approximation; this only affects very old rooms.
            return Ok(maps.last().cloned().unwrap_or_default());
        }
        let mut chains: Vec<EventIdSet<OwnedEventId>> = Vec::new();
        let mut all_ids: HashSet<OwnedEventId> = HashSet::new();
        for m in maps {
            let ids: Vec<OwnedEventId> = m.values().cloned().collect();
            let chain = self.services.store.auth_chain(&ids).await?;
            let mut set = EventIdSet::new();
            for id in &chain {
                set.insert(id.clone());
                all_ids.insert(id.clone());
            }
            for id in &ids {
                all_ids.insert(id.clone());
            }
            chains.push(set);
        }
        let all: Vec<OwnedEventId> = all_ids.into_iter().collect();
        self.warm_cache(&all).await?;
        let room_id = room_id.to_owned();
        let resolved = state::resolve(
            rules,
            maps,
            chains,
            |id| self.cached_event(id),
            |conflicted| Some(self.conflicted_state_subgraph(&room_id, conflicted)),
        )?;
        Ok(resolved)
    }

    /// Room v12: events reachable from the conflicted state set via `auth_events` /
    /// `prev_events` that are themselves part of the conflicted set, plus the set itself.
    fn conflicted_state_subgraph(
        &self,
        _room_id: &RoomId,
        conflicted: &StateMap<Vec<OwnedEventId>>,
    ) -> EventIdSet<OwnedEventId> {
        let mut set: EventIdSet<OwnedEventId> = EventIdSet::new();
        let members: HashSet<OwnedEventId> = conflicted.values().flatten().cloned().collect();
        let mut stack: Vec<OwnedEventId> = members.iter().cloned().collect();
        while let Some(id) = stack.pop() {
            if !set.insert(id.clone()) {
                continue;
            }
            if let Some(p) = self.cached_event(&id) {
                for next in p.auth_events().iter().chain(p.prev_events()) {
                    if members.contains(next) && !set.contains(next) {
                        stack.push(next.clone());
                    }
                }
            }
        }
        set
    }

    async fn current_state_pdus(
        &self,
        room_id: &RoomId,
    ) -> Result<HashMap<(StateEventType, String), Pdu>> {
        let mut out = HashMap::new();
        for se in self.services.store.current_state_pdus(room_id).await? {
            if let Some((ty, key)) = se.pdu.state_tuple() {
                self.event_cache
                    .insert(se.pdu.event_id().to_owned(), se.pdu.clone());
                out.insert((ty, key.to_owned()), se.pdu);
            }
        }
        Ok(out)
    }

    /// New forward extremities and current-state delta after accepting `pdu`.
    async fn new_current_state(
        &self,
        room_id: &RoomId,
        pdu: &Pdu,
        rules: &RoomVersionRules,
        state_before: &HashMap<(StateEventType, String), Pdu>,
    ) -> Result<(Option<CurrentStateDelta>, Option<Vec<OwnedEventId>>)> {
        let old_ext = self.services.store.forward_extremities(room_id).await?;
        let mut new_ext: Vec<OwnedEventId> = old_ext
            .iter()
            .filter(|e| !pdu.prev_events().contains(e))
            .cloned()
            .collect();
        new_ext.push(pdu.event_id().to_owned());

        // State after this event along its own branch.
        let mut after: StateMap<OwnedEventId> = state::state_map_from_pdus(state_before.values());
        if let Some((ty, key)) = pdu.state_tuple() {
            after.insert((ty, key.to_owned()), pdu.event_id().to_owned());
        }

        let new_state: StateMap<OwnedEventId> = if new_ext.len() == 1 {
            after
        } else {
            // Resolve across all extremities' states.
            let mut maps = vec![after];
            for ext in &new_ext {
                if ext == pdu.event_id() {
                    continue;
                }
                if let Some(ev) = self.services.store.get_event(ext).await?
                    && let Some(g) = ev.state_group
                {
                    let entries = self.services.store.state_at_group(g).await?;
                    maps.push(
                        entries
                            .into_iter()
                            .map(|e| ((e.event_type, e.state_key), e.event_id))
                            .collect(),
                    );
                }
            }
            self.resolve_states(room_id, rules, &maps).await?
        };

        // Diff against the stored current state.
        let current: BTreeMap<(StateEventType, String), OwnedEventId> = self
            .services
            .store
            .current_state(room_id)
            .await?
            .into_iter()
            .map(|e| ((e.event_type, e.state_key), e.event_id))
            .collect();
        let mut delta = CurrentStateDelta::default();
        for (k, v) in &new_state {
            if current.get(k) != Some(v) {
                delta.upsert.push((k.clone(), v.clone()));
            }
        }
        for k in current.keys() {
            if !new_state.contains_key(k) {
                delta.remove.push(k.clone());
            }
        }
        Ok((Some(delta), Some(new_ext)))
    }

    /// Apply a remote redaction if the sender may redact the target.
    async fn apply_remote_redaction(
        &self,
        room_id: &RoomId,
        redaction: &Pdu,
        rules: &RoomVersionRules,
        current: &HashMap<(StateEventType, String), Pdu>,
    ) -> Result<()> {
        let Some(target_id) = redaction.redacts() else {
            return Ok(());
        };
        let Some(target) = self.services.store.get_event(target_id).await? else {
            return Ok(());
        };
        if target.pdu.room_id() != room_id || target.redacted {
            return Ok(());
        }
        let allowed = target.pdu.sender() == redaction.sender()
            || tessera_service::rooms::power::can(current, redaction.sender(), "redact", rules);
        if allowed {
            let redacted = redaction::redact_pdu(&target.pdu, rules, Some(redaction))?;
            self.services
                .store
                .redact_event(target_id, &redacted)
                .await?;
        }
        Ok(())
    }

    // ------------------------------------------------------------------ EDUs

    async fn handle_edu(&self, origin: &ServerName, edu: Edu) -> Result<()> {
        let services: &Services = &self.services;
        match edu {
            Edu::Typing(t) => {
                if t.user_id.server_name() != origin {
                    return Ok(());
                }
                if services.is_joined(&t.room_id, &t.user_id).await? {
                    services.set_typing(
                        &t.room_id,
                        &t.user_id,
                        t.typing,
                        std::time::Duration::from_secs(30),
                    );
                }
            }
            Edu::Receipt(r) => {
                for (room_id, map) in r.receipts {
                    for (user_id, data) in map.read {
                        if user_id.server_name() != origin {
                            continue;
                        }
                        for event_id in data.event_ids {
                            let ts = data.data.ts.map_or(0, |t| u64::from(t.0));
                            let _ = services
                                .store
                                .set_receipt(tessera_store::Receipt {
                                    room_id: room_id.clone(),
                                    user_id: user_id.clone(),
                                    receipt_type: "m.read".into(),
                                    thread_id: match &data.data.thread {
                                        ruma::events::receipt::ReceiptThread::Unthreaded => None,
                                        ruma::events::receipt::ReceiptThread::Main => {
                                            Some("main".into())
                                        }
                                        ruma::events::receipt::ReceiptThread::Thread(id) => {
                                            Some(id.to_string())
                                        }
                                        _ => None,
                                    },
                                    event_id,
                                    data: serde_json::json!({ "ts": ts }),
                                    stream_id: 0,
                                })
                                .await;
                        }
                    }
                    services
                        .notifier
                        .notify(Notification::RoomEphemeral { room_id });
                }
            }
            Edu::Presence(p) => {
                for update in p.push {
                    if update.user_id.server_name() != origin {
                        continue;
                    }
                    services.set_presence(
                        &update.user_id,
                        update.presence,
                        Some(update.status_msg),
                    );
                }
            }
            Edu::DeviceListUpdate(d) => {
                if d.user_id.server_name() != origin {
                    return Ok(());
                }
                // Record the change; clients refetch keys over federation on demand.
                if let Some(keys) = d.keys
                    && let Ok(v) = serde_json::to_value(&keys)
                {
                    services
                        .store
                        .set_device_keys(&d.user_id, &d.device_id, &v)
                        .await?;
                }
                if d.deleted == Some(true) {
                    services
                        .store
                        .delete_device_keys(&d.user_id, &d.device_id)
                        .await?;
                }
                services
                    .notify_device_list_change(&d.user_id, Some(&d.device_id))
                    .await?;
            }
            Edu::SigningKeyUpdate(s) => {
                if s.user_id.server_name() != origin {
                    return Ok(());
                }
                if let Some(k) = s.master_key {
                    services
                        .store
                        .set_cross_signing_key(&s.user_id, "master", &serde_json::to_value(&k)?)
                        .await?;
                }
                if let Some(k) = s.self_signing_key {
                    services
                        .store
                        .set_cross_signing_key(
                            &s.user_id,
                            "self_signing",
                            &serde_json::to_value(&k)?,
                        )
                        .await?;
                }
                services.notify_device_list_change(&s.user_id, None).await?;
            }
            Edu::DirectToDevice(d) => {
                if d.sender.server_name() != origin {
                    return Ok(());
                }
                let mut messages: BTreeMap<OwnedUserId, BTreeMap<String, Value>> = BTreeMap::new();
                for (user, per_device) in d.messages {
                    if !services.is_local(&user) {
                        continue;
                    }
                    let mut m = BTreeMap::new();
                    for (dev, content) in per_device {
                        let key = match dev {
                            ruma::to_device::DeviceIdOrAllDevices::DeviceId(id) => id.to_string(),
                            ruma::to_device::DeviceIdOrAllDevices::AllDevices => "*".to_owned(),
                        };
                        if let Ok(v) = serde_json::from_str(content.json().get()) {
                            m.insert(key, v);
                        }
                    }
                    messages.insert(user, m);
                }
                services
                    .send_to_device(&d.sender, &d.ev_type.to_string(), &messages)
                    .await?;
            }
            _ => {}
        }
        Ok(())
    }
}

/// A join event may arrive from the resident server that authorised it rather than from the
/// joining user's server (restricted rooms).
fn is_join_via(pdu: &Pdu, origin: &ServerName) -> bool {
    if pdu.event_type() != &ruma::events::TimelineEventType::RoomMember {
        return false;
    }
    let Ok(content) = serde_json::from_str::<Value>(pdu.content().get()) else {
        return false;
    };
    content
        .get("join_authorised_via_users_server")
        .and_then(|v| v.as_str())
        .and_then(|u| OwnedUserId::try_from(u).ok())
        .is_some_and(|u| u.server_name() == origin)
}

/// Helper used by callers that need a `StoredEvent` for a freshly processed PDU.
pub fn stored_from(
    pdu: Pdu,
    stream_ordering: i64,
    state_group: Option<StateGroupId>,
) -> StoredEvent {
    StoredEvent {
        pdu,
        stream_ordering,
        state_group,
        soft_failed: false,
        outlier: false,
        redacted: false,
    }
}

/// Servers in a room, excluding ours.
pub async fn remote_servers_in_room(
    services: &Services,
    room_id: &RoomId,
) -> Result<Vec<OwnedServerName>> {
    Ok(services
        .store
        .servers_in_room(room_id)
        .await?
        .into_iter()
        .filter(|s| *s != services.config.server_name)
        .collect())
}

/// The room version a create event declares, for rooms we do not have yet.
pub fn room_version_of_create(create: &CanonicalJsonObject) -> Option<RoomVersionId> {
    match create.get("content")?.as_object()?.get("room_version") {
        None => Some(RoomVersionId::V1),
        Some(v) => RoomVersionId::try_from(v.as_str()?).ok(),
    }
}
