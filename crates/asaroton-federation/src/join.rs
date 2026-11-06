//! Joining, leaving and inviting across servers, plus serving those requests to peers.

use std::collections::{HashMap, HashSet};

use asaroton_core::{Pdu, auth, builder::PduBuilder, room_version, state};
use asaroton_store::{CurrentStateDelta, EventState, PersistEvent, RoomInfo, now_ts};
use ruma::{
    CanonicalJsonObject, CanonicalJsonValue, OwnedEventId, OwnedServerName, OwnedUserId, RoomId,
    RoomVersionId, ServerName, UInt, UserId,
    api::federation::membership::{
        create_invite, create_join_event, create_leave_event, prepare_join_event,
        prepare_leave_event,
    },
    events::{StateEventType, TimelineEventType, room::member::MembershipState},
    room_version_rules::RoomVersionRules,
};
use serde_json::{Value, json};

use crate::{Error, Federation, Result};

impl Federation {
    // ------------------------------------------------------------------ outbound join

    /// Join a remote room on behalf of a local user: `make_join` then `send_join`.
    pub async fn join_remote_room(
        &self,
        user_id: &UserId,
        room_id: &RoomId,
        via: &[OwnedServerName],
        reason: Option<&str>,
    ) -> Result<()> {
        let services = &self.services;
        if self
            .joins_in_progress
            .insert(room_id.to_owned(), ())
            .is_some()
        {
            return Err(Error::Rejected(
                "a join to this room is already in progress".into(),
            ));
        }
        let result = self
            .join_remote_room_inner(user_id, room_id, via, reason)
            .await;
        self.joins_in_progress.remove(room_id);
        let _ = services;
        result
    }

    async fn join_remote_room_inner(
        &self,
        user_id: &UserId,
        room_id: &RoomId,
        via: &[OwnedServerName],
        reason: Option<&str>,
    ) -> Result<()> {
        let services = &self.services;
        let mut candidates: Vec<OwnedServerName> = via
            .iter()
            .filter(|s| **s != services.config.server_name)
            .cloned()
            .collect();
        if candidates.is_empty()
            && let Some(server) = room_id.server_name()
            && server != services.config.server_name
        {
            candidates.push(server.to_owned());
        }
        if candidates.is_empty() {
            return Err(Error::Rejected("no servers to join through".into()));
        }

        let mut last_err = Error::Rejected("join failed".into());
        for server in &candidates {
            match self.try_join_via(server, user_id, room_id, reason).await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    tracing::warn!(%server, %room_id, error = %e, "join attempt failed");
                    last_err = e;
                }
            }
        }
        Err(last_err)
    }

    async fn try_join_via(
        &self,
        server: &ServerName,
        user_id: &UserId,
        room_id: &RoomId,
        reason: Option<&str>,
    ) -> Result<()> {
        let services = &self.services;

        // make_join
        let mut mj = prepare_join_event::v1::Request::new(room_id.to_owned(), user_id.to_owned());
        mj.ver = asaroton_core::SUPPORTED_ROOM_VERSIONS.to_vec();
        let mj_resp = self.client.send(server, mj).await?;
        let version = mj_resp.room_version.unwrap_or(RoomVersionId::V1);
        let rules = room_version::rules(&version)?;
        let template: Value = serde_json::from_str(mj_resp.event.get())?;

        // Build our join event from the template, with our profile and timestamp, signed by us.
        let mut content = services.member_content_for(user_id, "join", reason).await?;
        if let Some(c) = template.get("content").and_then(|c| c.as_object())
            && let Some(v) = c.get("join_authorised_via_users_server")
        {
            content["join_authorised_via_users_server"] = v.clone();
        }
        let prev: Vec<OwnedEventId> = ids_from(template.get("prev_events"));
        let auth_ids: Vec<OwnedEventId> = ids_from(template.get("auth_events"));
        let depth = template
            .get("depth")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        let mut builder = PduBuilder::new(user_id.to_owned(), TimelineEventType::RoomMember)
            .room_id(room_id.to_owned())
            .state_key(user_id.as_str())
            .content(&content)?
            .depth(UInt::try_from(depth).unwrap_or(UInt::MAX))
            .prev_events(prev)
            .auth_events(auth_ids);
        if let Some(ts) = template
            .get("origin_server_ts")
            .and_then(serde_json::Value::as_u64)
        {
            builder = builder.origin_server_ts(ruma::MilliSecondsSinceUnixEpoch(
                UInt::try_from(ts).unwrap_or(UInt::MAX),
            ));
        }
        let join = builder.build(&rules, &services.signing_key, &services.config.server_name)?;

        // send_join (v2)
        let pdu_json = asaroton_core::canonical::to_json_value(join.to_federation_json());
        let mut sj = create_join_event::v2::Request::new(
            room_id.to_owned(),
            join.event_id().to_owned(),
            serde_json::value::to_raw_value(&pdu_json)?,
        );
        sj.omit_members = false;
        let sj_resp = self.client.send_long(server, sj).await?;
        let room_state = sj_resp.room_state;

        // The resident server may have added its signature (restricted joins); prefer their copy.
        let join = match &room_state.event {
            Some(raw) => {
                let obj = asaroton_core::canonical::from_str(raw.get())?;
                self.parse_and_verify(server, obj, &rules).await?
            }
            None => join,
        };

        // Parse and verify everything we were given.
        let mut auth_chain: Vec<Pdu> = Vec::new();
        for raw in &room_state.auth_chain {
            if let Some(p) = self
                .join_response_event(server, room_id, raw.get(), &rules)
                .await
            {
                auth_chain.push(p);
            }
        }
        let mut state_events: Vec<Pdu> = Vec::new();
        for raw in &room_state.state {
            if let Some(p) = self
                .join_response_event(server, room_id, raw.get(), &rules)
                .await
            {
                state_events.push(p);
            }
        }
        let create = state_events
            .iter()
            .find(|p| p.is_create())
            .cloned()
            .ok_or_else(|| Error::Protocol("send_join response has no m.room.create".into()))?;
        if create.declared_room_version().as_ref() != Some(&version) {
            return Err(Error::Protocol(
                "room version mismatch in send_join response".into(),
            ));
        }
        if create.room_id() != room_id {
            return Err(Error::Protocol(
                "send_join state is for another room".into(),
            ));
        }

        let _guard = services.lock_room(room_id).await;
        let join_event_id = join.event_id().to_owned();
        self.persist_joined_room(
            room_id,
            &version,
            &rules,
            auth_chain,
            state_events,
            join,
            server,
        )
        .await?;
        // We tell the other servers about our join ourselves (the resident server does not
        // forward events from other servers).
        let servers: Vec<OwnedServerName> = services
            .remote_servers_in_room(room_id)
            .await?
            .into_iter()
            .filter(|s| s != server)
            .collect();
        if !servers.is_empty() {
            self.self_arc().queue_pdu(servers, &join_event_id).await;
        }
        services.after_user_joined(user_id, room_id).await;
        Ok(())
    }

    /// One event of a `send_join` response, parsed and verified. An event that does not check
    /// out is left out, and that is said in the log: whatever depends on it cannot be checked
    /// either, and a silent gap here is very hard to find afterwards.
    async fn join_response_event(
        &self,
        server: &ServerName,
        room_id: &RoomId,
        raw: &str,
        rules: &RoomVersionRules,
    ) -> Option<Pdu> {
        let obj = match asaroton_core::canonical::from_str(raw) {
            Ok(obj) => obj,
            Err(e) => {
                tracing::warn!(%room_id, %server, error = %e, "leaving out a malformed event of the join response");
                return None;
            }
        };
        let described = Pdu::from_canonical(obj.clone(), rules)
            .ok()
            .map(|p| format!("{} ({} from {})", p.event_id(), p.event_type(), p.sender()));
        match self.parse_and_verify(server, obj, rules).await {
            Ok(pdu) => Some(pdu),
            Err(e) => {
                tracing::warn!(
                    %room_id, %server,
                    event = described.as_deref().unwrap_or("unparseable"),
                    error = %e,
                    "leaving out an event of the join response that could not be verified"
                );
                None
            }
        }
    }

    /// Store the auth chain and state from a join (or an invite's resolved state), compute the
    /// state group, check every event against its auth events, and make the join current.
    ///
    /// An event is rejected only when it was checked and failed. One whose auth events cannot
    /// all be had (not in the response, not stored, not obtainable from `via`, or left out
    /// above because its signature could not be verified) is not stored at all: that we
    /// cannot check it today does not make it invalid, and a stored rejection is passed on to
    /// everything that cites the event later.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    async fn persist_joined_room(
        &self,
        room_id: &RoomId,
        version: &RoomVersionId,
        rules: &RoomVersionRules,
        auth_chain: Vec<Pdu>,
        state_events: Vec<Pdu>,
        join: Pdu,
        via: &ServerName,
    ) -> Result<()> {
        let services = &self.services;
        services
            .store
            .create_room(RoomInfo {
                room_id: room_id.to_owned(),
                room_version: version.clone(),
                creator: state_events
                    .iter()
                    .find(|p| p.is_create())
                    .map(|p| p.sender().to_owned()),
                is_public: false,
                created_ts: now_ts(),
            })
            .await?;

        // What is to be checked and stored: the response's events...
        let mut all: HashMap<OwnedEventId, Pdu> = HashMap::new();
        for p in auth_chain.iter().chain(state_events.iter()) {
            all.insert(p.event_id().to_owned(), p.clone());
        }
        let mut to_check: HashSet<OwnedEventId> = all.keys().cloned().collect();
        // ...plus the auth events they cite that the response does not carry. Those may be
        // stored already (then their stored verdict stands), or the server we join through
        // can hand them over one by one.
        let missing_from = |all: &HashMap<OwnedEventId, Pdu>| -> Vec<OwnedEventId> {
            let mut ids: Vec<OwnedEventId> = all
                .values()
                .flat_map(|p| p.auth_events().iter().cloned())
                .filter(|id| !all.contains_key(id))
                .collect();
            ids.sort_unstable();
            ids.dedup();
            ids
        };
        let mut unobtainable: HashSet<OwnedEventId> = HashSet::new();
        for _ in 0..8 {
            let mut wanted = missing_from(&all);
            wanted.retain(|id| !unobtainable.contains(id));
            if wanted.is_empty() {
                break;
            }
            for stored in services.store.get_events(&wanted).await? {
                if stored.pdu.room_id() == room_id {
                    all.insert(stored.pdu.event_id().to_owned(), stored.pdu);
                }
            }
            for id in wanted {
                if all.contains_key(&id) {
                    continue;
                }
                match self.fetch_event(via, &id, rules).await {
                    Ok(p) if p.room_id() == room_id => {
                        to_check.insert(id.clone());
                        all.insert(id, p);
                    }
                    Ok(_) => {
                        unobtainable.insert(id);
                    }
                    Err(e) => {
                        tracing::warn!(%room_id, event_id = %id, %via, error = %e, "an auth event that the join response lacks could not be obtained");
                        unobtainable.insert(id);
                    }
                }
            }
        }

        // Everything in dependency order.
        let mut ordered: Vec<Pdu> = to_check
            .iter()
            .filter_map(|id| all.get(id).cloned())
            .collect();
        ordered.sort_by_key(|p| u64::from(p.depth()));

        let mut rejected: HashSet<OwnedEventId> = HashSet::new();
        let mut unchecked: HashSet<OwnedEventId> = HashSet::new();
        let mut accepted: Vec<OwnedEventId> = Vec::new();
        let mut batch: Vec<PersistEvent> = Vec::new();
        for p in &ordered {
            if let Some(lacking) = p
                .auth_events()
                .iter()
                .find(|a| !all.contains_key(*a) || unchecked.contains(*a))
            {
                tracing::warn!(
                    %room_id, event_id = %p.event_id(), sender = %p.sender(), auth_event = %lacking,
                    "not storing an event of the join response: an auth event it cites is unavailable, so it cannot be checked"
                );
                unchecked.insert(p.event_id().to_owned());
                continue;
            }
            let ok = auth::check_against_auth_events(p, rules, |id| all.get(id).cloned()).is_ok()
                && p.auth_events().iter().all(|a| !rejected.contains(a));
            if ok {
                accepted.push(p.event_id().to_owned());
            } else {
                rejected.insert(p.event_id().to_owned());
            }
            self.event_cache
                .insert(p.event_id().to_owned(), p.with_rejected(!ok));
            batch.push(PersistEvent {
                pdu: p.with_rejected(!ok),
                state: EventState::Outlier,
                soft_failed: false,
                backfilled: false,
                stream_ordering: None,
                current_state_delta: None,
                forward_extremities: None,
            });
        }
        services.store.persist_events(batch).await?;
        // An event stored earlier keeps its row. If it had been rejected then for want of
        // something that is available now, the fresh verdict replaces the old one.
        let restored = services.store.clear_rejected(&accepted).await?;
        if restored > 0 {
            tracing::info!(%room_id, restored, "events rejected on an earlier join pass their checks now");
        }
        rejected.extend(unchecked);

        // The state before the join: the state events we were given, minus rejected ones.
        let state_pdus: Vec<&Pdu> = state_events
            .iter()
            .filter(|p| !rejected.contains(p.event_id()))
            .collect();
        let state_map = state::state_map_from_pdus(state_pdus.iter().copied());
        let state_hash: HashMap<(StateEventType, String), Pdu> = state_pdus
            .iter()
            .filter_map(|p| {
                p.state_tuple()
                    .map(|(t, k)| ((t, k.to_owned()), (*p).clone()))
            })
            .collect();
        auth::check_against_state(&join, rules, &state_hash)
            .map_err(|e| Error::Rejected(format!("join event not authorised: {e}")))?;

        let before_group = services
            .store
            .create_state_group(
                room_id,
                None,
                &state_map.clone().into_iter().collect::<Vec<_>>(),
            )
            .await?;
        let mut after: Vec<((StateEventType, String), OwnedEventId)> = Vec::new();
        if let Some((ty, key)) = join.state_tuple() {
            after.push(((ty, key.to_owned()), join.event_id().to_owned()));
        }
        // The state and auth events stay outliers: they are not part of our timeline until
        // backfill fetches the real history around them.
        let mut delta = CurrentStateDelta {
            upsert: state_map.into_iter().collect(),
            remove: vec![],
        };
        delta.upsert.extend(after.iter().cloned());
        let persisted = services
            .store
            .persist_event(PersistEvent {
                pdu: join.clone(),
                state: EventState::New {
                    prev: Some(before_group),
                    delta: after,
                },
                soft_failed: false,
                backfilled: false,
                stream_ordering: None,
                current_state_delta: Some(delta),
                forward_extremities: Some(vec![join.event_id().to_owned()]),
            })
            .await?;
        self.event_cache
            .insert(join.event_id().to_owned(), join.clone());
        services.notify_room_event(&join, persisted.stream_ordering);
        if let Err(e) = self
            .reauthorise_rejected(room_id, rules, persisted.state_group)
            .await
        {
            tracing::warn!(%room_id, error = %e, "could not look again at rejected events");
        }
        // Events of this room that are waiting for auth events they cite: the join may have
        // brought those in, so they are due now.
        if let Err(e) = services.store.hasten_awaiting_auth(room_id, now_ts()).await {
            tracing::warn!(%room_id, error = %e, "could not bring forward the events that were set aside");
        }
        Ok(())
    }

    /// Look again at the events of a room that are stored as rejected: the ones in its
    /// timeline, and the ones set aside because an auth event they cite could not be obtained
    /// (see `recheck`). Where a rejection came from an auth event that was itself wrongly
    /// rejected, or missing, it no longer holds once the join has brought that event in.
    ///
    /// An event is restored when it passes against the auth events it cites and against the
    /// room's current state. The state before the event, which the normal checks use, is no
    /// measure here: it was put together before this join, from the same incomplete picture
    /// that made the event fail. Only events without a state key are restored: taking back
    /// the rejection of a state event would mean recomputing the state of everything after
    /// it, and state is repaired by the join itself.
    ///
    /// A timeline event keeps its place. An event that had been set aside has none yet; it
    /// is placed at the join (`joined_at`, the state group after it) and reaches the clients
    /// as an event that arrived late.
    async fn reauthorise_rejected(
        &self,
        room_id: &RoomId,
        rules: &RoomVersionRules,
        joined_at: Option<asaroton_store::StateGroupId>,
    ) -> Result<()> {
        let services = &self.services;
        let in_timeline = services.store.rejected_timeline_events(room_id, 0).await?;
        let set_aside = services.store.events_set_aside(room_id).await?;
        if in_timeline.is_empty() && set_aside.is_empty() {
            return Ok(());
        }
        let current: HashMap<(StateEventType, String), Pdu> = services
            .store
            .current_state_pdus(room_id)
            .await?
            .into_iter()
            .filter_map(|e| {
                let key = e.pdu.state_tuple().map(|(t, k)| (t, k.to_owned()))?;
                Some((key, e.pdu))
            })
            .collect();
        let mut restored: Vec<OwnedEventId> = Vec::new();
        let mut placed = 0usize;
        let mut kept = 0usize;
        for (ev, aside) in in_timeline
            .into_iter()
            .map(|e| (e, false))
            .chain(set_aside.into_iter().map(|e| (e, true)))
        {
            let p = &ev.pdu;
            if p.is_state() || !p.rejected() || (aside && !ev.outlier) {
                kept += 1;
                continue;
            }
            let cited: HashMap<OwnedEventId, Pdu> = services
                .store
                .get_events(p.auth_events())
                .await?
                .into_iter()
                .map(|e| (e.pdu.event_id().to_owned(), e.pdu))
                .collect();
            let lookup = |id: &ruma::EventId| {
                cited
                    .get(id)
                    .cloned()
                    .or_else(|| current.values().find(|c| c.event_id() == id).cloned())
            };
            // An event that was set aside never got as far as the checks of its form.
            let well_formed = !aside
                || (auth::check_size_limits(p).is_ok()
                    && auth::check_state_independent(p, rules, lookup).is_ok());
            let passes = well_formed
                && p.auth_events()
                    .iter()
                    .all(|id| lookup(id).is_some_and(|cited| !cited.rejected()))
                && auth::check_against_auth_events(p, rules, lookup).is_ok()
                && auth::check_against_state(p, rules, &current).is_ok();
            if !passes {
                kept += 1;
                continue;
            }
            if !aside {
                restored.push(p.event_id().to_owned());
                continue;
            }
            let Some(group) = joined_at else {
                kept += 1;
                continue;
            };
            let accepted = p.with_rejected(false);
            let persisted = services
                .store
                .persist_event(PersistEvent {
                    pdu: accepted.clone(),
                    state: EventState::Existing(group),
                    soft_failed: false,
                    backfilled: false,
                    stream_ordering: None,
                    current_state_delta: None,
                    forward_extremities: None,
                })
                .await?;
            services.store.forget_awaiting_auth(p.event_id()).await?;
            self.event_cache
                .insert(p.event_id().to_owned(), accepted.clone());
            services.notify_room_event(&accepted, persisted.stream_ordering);
            placed += 1;
        }
        let cleared = services.store.clear_rejected(&restored).await?;
        for id in &restored {
            self.event_cache.remove(id);
        }
        tracing::info!(%room_id, restored = cleared, placed, still_rejected = kept, "looked again at the room's rejected events");
        Ok(())
    }

    // ------------------------------------------------------------------ outbound leave

    /// Reject an invite (or leave) in a room where we have no other members: `make_leave` /
    /// `send_leave` through one of the given servers.
    pub async fn leave_remote_room(
        &self,
        user_id: &UserId,
        room_id: &RoomId,
        via: &[OwnedServerName],
    ) -> Result<()> {
        let services = &self.services;
        let mut last_err = Error::Rejected("leave failed".into());
        for server in via.iter().filter(|s| **s != services.config.server_name) {
            let ml = prepare_leave_event::v1::Request::new(room_id.to_owned(), user_id.to_owned());
            let ml_resp = match self.client.send(server, ml).await {
                Ok(r) => r,
                Err(e) => {
                    last_err = e;
                    continue;
                }
            };
            let version = ml_resp.room_version.unwrap_or(RoomVersionId::V1);
            let rules = room_version::rules(&version)?;
            let template: Value = serde_json::from_str(ml_resp.event.get())?;
            let depth = template
                .get("depth")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0);
            let leave = PduBuilder::new(user_id.to_owned(), TimelineEventType::RoomMember)
                .room_id(room_id.to_owned())
                .state_key(user_id.as_str())
                .content(&json!({ "membership": "leave" }))?
                .depth(UInt::try_from(depth).unwrap_or(UInt::MAX))
                .prev_events(ids_from(template.get("prev_events")))
                .auth_events(ids_from(template.get("auth_events")))
                .build(&rules, &services.signing_key, &services.config.server_name)?;
            let pdu_json = asaroton_core::canonical::to_json_value(leave.to_federation_json());
            let sl = create_leave_event::v2::Request::new(
                room_id.to_owned(),
                leave.event_id().to_owned(),
                serde_json::value::to_raw_value(&pdu_json)?,
            );
            match self.client.send(server, sl).await {
                Ok(_) => {
                    // Record the leave locally as an outlier that updates membership.
                    let delta = leave.state_tuple().map(|(t, k)| CurrentStateDelta {
                        upsert: vec![((t, k.to_owned()), leave.event_id().to_owned())],
                        remove: vec![],
                    });
                    // The wake-up waits until a sync can be told about the leave's position
                    // (see `wake_when_reportable`); without a stored event there is none.
                    let mut at = 0;
                    if services.store.room_exists(room_id).await? {
                        at = services
                            .store
                            .persist_event(PersistEvent {
                                pdu: leave.clone(),
                                state: EventState::Outlier,
                                soft_failed: false,
                                backfilled: false,
                                stream_ordering: None,
                                current_state_delta: delta,
                                forward_extremities: None,
                            })
                            .await?
                            .stream_ordering;
                    }
                    services.wake_when_reportable(
                        asaroton_store::SyncStream::Events,
                        at,
                        vec![asaroton_service::Notification::User {
                            user_id: user_id.to_owned(),
                        }],
                    );
                    self.fan_out_own_event(room_id, leave.event_id(), server)
                        .await;
                    return Ok(());
                }
                Err(e) => last_err = e,
            }
        }
        Err(last_err)
    }

    /// After a membership event of ours was accepted by a resident server, tell the other
    /// servers in the room about it (resident servers do not forward it for us).
    async fn fan_out_own_event(
        &self,
        room_id: &RoomId,
        event_id: &ruma::EventId,
        resident: &ServerName,
    ) {
        let Ok(servers) = self.services.remote_servers_in_room(room_id).await else {
            return;
        };
        let servers: Vec<OwnedServerName> = servers.into_iter().filter(|s| s != resident).collect();
        if !servers.is_empty() {
            self.self_arc().queue_pdu(servers, event_id).await;
        }
    }

    // ------------------------------------------------------------------ knocking

    /// Knock on a remote room through one of `via`: `make_knock`, sign, `send_knock`, then
    /// record the knock and the stripped state locally.
    pub async fn knock_remote_room(
        &self,
        user_id: &UserId,
        room_id: &RoomId,
        via: &[OwnedServerName],
        reason: Option<&str>,
    ) -> Result<()> {
        use ruma::api::federation::membership::{create_knock_event, prepare_knock_event};
        let services = &self.services;
        let mut last_err = Error::Rejected("knock failed".into());
        for server in via.iter().filter(|s| **s != services.config.server_name) {
            let mut mk =
                prepare_knock_event::v1::Request::new(room_id.to_owned(), user_id.to_owned());
            mk.ver = asaroton_core::SUPPORTED_ROOM_VERSIONS.to_vec();
            let mk_resp = match self.client.send(server, mk).await {
                Ok(r) => r,
                Err(e) => {
                    last_err = e;
                    continue;
                }
            };
            let version = mk_resp.room_version;
            let rules = room_version::rules(&version)?;
            let template: Value = serde_json::from_str(mk_resp.event.get())?;
            let depth = template
                .get("depth")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0);
            let mut content = services
                .member_content_for(user_id, "knock", reason)
                .await?;
            content["membership"] = json!("knock");
            let knock = PduBuilder::new(user_id.to_owned(), TimelineEventType::RoomMember)
                .room_id(room_id.to_owned())
                .state_key(user_id.as_str())
                .content(&content)?
                .depth(UInt::try_from(depth).unwrap_or(UInt::MAX))
                .prev_events(ids_from(template.get("prev_events")))
                .auth_events(ids_from(template.get("auth_events")))
                .build(&rules, &services.signing_key, &services.config.server_name)?;
            let pdu_json = asaroton_core::canonical::to_json_value(knock.to_federation_json());
            let sk = create_knock_event::v1::Request::new(
                room_id.to_owned(),
                knock.event_id().to_owned(),
                serde_json::value::to_raw_value(&pdu_json)?,
            );
            match self.client.send(server, sk).await {
                Ok(resp) => {
                    if services.store.get_room(room_id).await?.is_none() {
                        services
                            .store
                            .create_room(RoomInfo {
                                room_id: room_id.to_owned(),
                                room_version: version.clone(),
                                creator: None,
                                is_public: false,
                                created_ts: now_ts(),
                            })
                            .await?;
                    }
                    let delta = knock.state_tuple().map(|(t, k)| CurrentStateDelta {
                        upsert: vec![((t, k.to_owned()), knock.event_id().to_owned())],
                        remove: vec![],
                    });
                    let participating = self.we_are_in_room_pub(room_id).await?;
                    let at = services
                        .store
                        .persist_event(PersistEvent {
                            pdu: knock.clone(),
                            state: EventState::Outlier,
                            soft_failed: false,
                            backfilled: false,
                            stream_ordering: None,
                            current_state_delta: if participating { None } else { delta },
                            forward_extremities: None,
                        })
                        .await?
                        .stream_ordering;
                    let stripped: Vec<Value> = resp
                        .knock_room_state
                        .iter()
                        .filter_map(|s| serde_json::to_value(s).ok())
                        .collect();
                    services
                        .store
                        .set_invite_state(room_id, user_id, &stripped)
                        .await?;
                    services.wake_when_reportable(
                        asaroton_store::SyncStream::Events,
                        at,
                        vec![asaroton_service::Notification::User {
                            user_id: user_id.to_owned(),
                        }],
                    );
                    self.fan_out_own_event(room_id, knock.event_id(), server)
                        .await;
                    return Ok(());
                }
                Err(e) => last_err = e,
            }
        }
        Err(last_err)
    }

    /// `make_knock`: a knock template for a remote user.
    pub async fn make_knock(
        &self,
        origin: &ServerName,
        room_id: &RoomId,
        user_id: &UserId,
        versions: &[RoomVersionId],
    ) -> Result<(RoomVersionId, Value)> {
        let services = &self.services;
        if user_id.server_name() != origin {
            return Err(Error::Rejected(
                "user is not on the requesting server".into(),
            ));
        }
        let room = services
            .store
            .get_room(room_id)
            .await?
            .ok_or_else(|| Error::Rejected("unknown room".into()))?;
        if !versions.is_empty() && !versions.contains(&room.room_version) {
            return Err(Error::RemoteError {
                server: origin.to_owned(),
                status: http::StatusCode::BAD_REQUEST,
                body: json!({ "errcode": "M_INCOMPATIBLE_ROOM_VERSION", "error": "Your homeserver does not support this room version", "room_version": room.room_version }).to_string(),
            });
        }
        if !self.we_are_in_room_pub(room_id).await? {
            return Err(Error::Rejected("this server is not in the room".into()));
        }
        let ctx = services.room_context(room_id).await?;
        if !ctx.rules.authorization.knocking {
            return Err(Error::Rejected(
                "this room version does not support knocking".into(),
            ));
        }
        let join_rule = ctx
            .get(StateEventType::RoomJoinRules, "")
            .and_then(|p| serde_json::from_str::<Value>(p.content().get()).ok())
            .and_then(|v| {
                v.get("join_rule")
                    .and_then(|j| j.as_str())
                    .map(str::to_owned)
            })
            .unwrap_or_else(|| "invite".to_owned());
        if !matches!(join_rule.as_str(), "knock" | "knock_restricted") {
            return Err(Error::Rejected("this room does not allow knocking".into()));
        }
        match ctx.membership(user_id) {
            Some(MembershipState::Ban) => return Err(Error::Rejected("user is banned".into())),
            Some(MembershipState::Join) => {
                return Err(Error::Rejected("user is already in the room".into()));
            }
            _ => {}
        }
        let template = self
            .event_template(
                &ctx,
                user_id,
                TimelineEventType::RoomMember,
                Some(user_id.as_str()),
                json!({ "membership": "knock" }),
            )
            .await?;
        Ok((room.room_version, template))
    }

    /// `send_knock`: accept a signed knock and return the stripped room state.
    pub async fn send_knock(
        &self,
        origin: &ServerName,
        room_id: &RoomId,
        raw: CanonicalJsonObject,
    ) -> Result<Vec<Value>> {
        let services = &self.services;
        let room = services
            .store
            .get_room(room_id)
            .await?
            .ok_or_else(|| Error::Rejected("unknown room".into()))?;
        let rules = room_version::rules(&room.room_version)?;
        let pdu = self.parse_and_verify(origin, raw, &rules).await?;
        let membership = serde_json::from_str::<Value>(pdu.content().get())
            .ok()
            .and_then(|c| {
                c.get("membership")
                    .and_then(|m| m.as_str())
                    .map(str::to_owned)
            });
        if pdu.event_type() != &TimelineEventType::RoomMember
            || membership.as_deref() != Some("knock")
            || pdu.state_key() != Some(pdu.sender().as_str())
            || pdu.sender().server_name() != origin
        {
            return Err(Error::Rejected("not a knock event".into()));
        }
        if let (_, crate::receive::Outcome::Rejected(reason)) = self
            .handle_incoming_pdu(
                origin,
                room_id,
                pdu.to_federation_json_with_id(&rules),
                false,
            )
            .await?
        {
            return Err(Error::Rejected(reason));
        }
        // Like the reference implementation, the resident server sends the accepted event to
        // the other servers on the origin's behalf (the origin itself already has it).
        self.fan_out_own_event(room_id, pdu.event_id(), origin)
            .await;
        services
            .stripped_state_full(room_id)
            .await
            .map_err(Into::into)
    }

    // ------------------------------------------------------------------ outbound invite

    /// Send an invite event to the invitee's server; returns the event with their signature.
    pub async fn send_invite(
        &self,
        pdu: &Pdu,
        room_version: &RoomVersionId,
        stripped_state: Vec<Value>,
    ) -> Result<Pdu> {
        let rules = room_version::rules(room_version)?;
        let invitee = pdu
            .state_key()
            .and_then(|k| OwnedUserId::try_from(k).ok())
            .ok_or_else(|| Error::Protocol("invite without state_key".into()))?;
        let pdu_json = asaroton_core::canonical::to_json_value(pdu.to_federation_json());
        let req = create_invite::v2::Request::new(
            pdu.room_id().to_owned(),
            pdu.event_id().to_owned(),
            room_version.clone(),
            serde_json::value::to_raw_value(&pdu_json)?,
            stripped_state
                .iter()
                .filter_map(|v| serde_json::value::to_raw_value(v).ok())
                .map(|raw| {
                    ruma::serde::Raw::<ruma::events::AnyStrippedStateEvent>::from_json(raw).into()
                })
                .collect(),
        );
        let resp = self.client.send(invitee.server_name(), req).await?;
        let obj = asaroton_core::canonical::from_str(resp.event.get())?;
        let signed = self
            .parse_and_verify(invitee.server_name(), obj, &rules)
            .await?;
        if signed.event_id() != pdu.event_id() {
            return Err(Error::Protocol(
                "invitee server returned a different event".into(),
            ));
        }
        Ok(signed)
    }

    // ------------------------------------------------------------------ serving peers

    /// `GET /make_join`: a join template for a remote user.
    pub async fn make_join(
        &self,
        origin: &ServerName,
        room_id: &RoomId,
        user_id: &UserId,
        versions: &[RoomVersionId],
    ) -> Result<(RoomVersionId, Value)> {
        let services = &self.services;
        if user_id.server_name() != origin {
            return Err(Error::Rejected(
                "user is not on the requesting server".into(),
            ));
        }
        let room = services
            .store
            .get_room(room_id)
            .await?
            .ok_or_else(|| Error::Rejected("unknown room".into()))?;
        if !versions.is_empty() && !versions.contains(&room.room_version) {
            return Err(Error::RemoteError {
                server: origin.to_owned(),
                status: http::StatusCode::BAD_REQUEST,
                body: json!({ "errcode": "M_INCOMPATIBLE_ROOM_VERSION", "error": "Your homeserver does not support this room version", "room_version": room.room_version }).to_string(),
            });
        }
        if !self.we_are_in_room_pub(room_id).await? {
            return Err(Error::Rejected("this server is not in the room".into()));
        }
        let ctx = services.room_context(room_id).await?;
        let mut content = json!({ "membership": "join" });
        match ctx.membership(user_id) {
            Some(MembershipState::Ban) => return Err(Error::Rejected("user is banned".into())),
            Some(MembershipState::Join | MembershipState::Invite) => {}
            _ => {
                let jr = ctx
                    .get(StateEventType::RoomJoinRules, "")
                    .and_then(|p| serde_json::from_str::<Value>(p.content().get()).ok())
                    .and_then(|v| {
                        v.get("join_rule")
                            .and_then(|j| j.as_str())
                            .map(str::to_owned)
                    })
                    .unwrap_or_else(|| "invite".into());
                match jr.as_str() {
                    "public" => {}
                    "restricted" | "knock_restricted" => {
                        // Vouch with a local member that can invite, if the user qualifies.
                        if !services.satisfies_restricted_allow(&ctx, user_id).await? {
                            return Err(Error::Rejected(
                                "user does not satisfy the restricted join rule".into(),
                            ));
                        }
                        let authoriser =
                            services.find_join_authoriser(&ctx).await?.ok_or_else(|| {
                                Error::Rejected("no local user can authorise this join".into())
                            })?;
                        content["join_authorised_via_users_server"] = json!(authoriser);
                    }
                    _ => return Err(Error::Rejected("you are not invited to this room".into())),
                }
            }
        }
        let template = self
            .event_template(
                &ctx,
                user_id,
                TimelineEventType::RoomMember,
                Some(user_id.as_str()),
                content,
            )
            .await?;
        Ok((room.room_version, template))
    }

    /// `PUT /send_join`: accept a remote user's join.
    pub async fn send_join(
        &self,
        origin: &ServerName,
        room_id: &RoomId,
        raw: CanonicalJsonObject,
    ) -> Result<create_join_event::v2::RoomState> {
        let services = &self.services;
        let room = services
            .store
            .get_room(room_id)
            .await?
            .ok_or_else(|| Error::Rejected("unknown room".into()))?;
        let rules = room_version::rules(&room.room_version)?;
        // Restricted joins: a join we authorised must carry our signature too, so add it
        // before the signature check that requires it.
        let mut raw_signed = raw;
        let authorised_by_us = raw_signed
            .get("content")
            .and_then(|c| c.as_object())
            .and_then(|c| c.get("join_authorised_via_users_server"))
            .and_then(|v| v.as_str())
            .and_then(|u| OwnedUserId::try_from(u).ok())
            .is_some_and(|u| u.server_name() == services.config.server_name);
        if authorised_by_us {
            services.signing_key.sign_event(
                &services.config.server_name,
                &mut raw_signed,
                &rules,
            )?;
        }
        let pdu = self
            .parse_and_verify(origin, raw_signed.clone(), &rules)
            .await?;
        if pdu.event_type() != &TimelineEventType::RoomMember
            || pdu.state_key() != Some(pdu.sender().as_str())
            || pdu.sender().server_name() != origin
            || pdu.room_id() != room_id
        {
            return Err(Error::Rejected("not a valid join event".into()));
        }

        if let (_, crate::receive::Outcome::Rejected(reason)) = self
            .handle_incoming_pdu(
                origin,
                room_id,
                pdu.to_federation_json_with_id(&rules),
                false,
            )
            .await?
        {
            return Err(Error::Rejected(reason));
        }
        self.fan_out_own_event(room_id, pdu.event_id(), origin)
            .await;

        // Respond with the state and auth chain *before* the join (its state group's parent).
        let state_before = match services.store.state_group_before(pdu.event_id()).await? {
            Some(group) => {
                let ids: Vec<OwnedEventId> = services
                    .store
                    .state_at_group(group)
                    .await?
                    .into_iter()
                    .map(|e| e.event_id)
                    .collect();
                services.store.get_events(&ids).await?
            }
            None => {
                let mut s = services.store.current_state_pdus(room_id).await?;
                s.retain(|e| e.pdu.event_id() != pdu.event_id());
                s
            }
        };
        let state_ids: Vec<OwnedEventId> = state_before
            .iter()
            .map(|e| e.pdu.event_id().to_owned())
            .collect();
        let mut chain_ids = services.store.auth_chain(&state_ids).await?;
        chain_ids.retain(|id| !state_ids.contains(id));
        let chain = services.store.get_events(&chain_ids).await?;
        crate::warn_if_chain_incomplete(room_id, "send_join", &chain_ids, &chain);
        let to_raw = |p: &Pdu| {
            serde_json::value::to_raw_value(&asaroton_core::canonical::to_json_value(
                p.to_federation_json_with_id(&rules),
            ))
        };
        let mut out = create_join_event::v2::RoomState::new();
        out.state = state_before
            .iter()
            .filter_map(|e| to_raw(&e.pdu).ok())
            .collect();
        out.auth_chain = chain.iter().filter_map(|e| to_raw(&e.pdu).ok()).collect();
        out.event = to_raw(&pdu).ok();
        out.members_omitted = false;
        Ok(out)
    }

    /// `GET /make_leave`.
    pub async fn make_leave(
        &self,
        origin: &ServerName,
        room_id: &RoomId,
        user_id: &UserId,
    ) -> Result<(RoomVersionId, Value)> {
        let services = &self.services;
        if user_id.server_name() != origin {
            return Err(Error::Rejected(
                "user is not on the requesting server".into(),
            ));
        }
        let room = services
            .store
            .get_room(room_id)
            .await?
            .ok_or_else(|| Error::Rejected("unknown room".into()))?;
        let ctx = services.room_context(room_id).await?;
        let template = self
            .event_template(
                &ctx,
                user_id,
                TimelineEventType::RoomMember,
                Some(user_id.as_str()),
                json!({ "membership": "leave" }),
            )
            .await?;
        Ok((room.room_version, template))
    }

    /// `PUT /send_leave`.
    pub async fn send_leave(
        &self,
        origin: &ServerName,
        room_id: &RoomId,
        raw: CanonicalJsonObject,
    ) -> Result<()> {
        let room = self
            .services
            .store
            .get_room(room_id)
            .await?
            .ok_or_else(|| Error::Rejected("unknown room".into()))?;
        let rules = room_version::rules(&room.room_version)?;
        let pdu = self.parse_and_verify(origin, raw, &rules).await?;
        if pdu.event_type() != &TimelineEventType::RoomMember
            || pdu.sender().server_name() != origin
        {
            return Err(Error::Rejected("not a valid leave event".into()));
        }
        if let (_, crate::receive::Outcome::Rejected(reason)) = self
            .handle_incoming_pdu(
                origin,
                room_id,
                pdu.to_federation_json_with_id(&rules),
                false,
            )
            .await?
        {
            return Err(Error::Rejected(reason));
        }
        // Like the reference implementation, the resident server sends the accepted event to
        // the other servers on the origin's behalf (the origin itself already has it).
        self.fan_out_own_event(room_id, pdu.event_id(), origin)
            .await;
        Ok(())
    }

    /// `PUT /invite/v2`: a remote server invites one of our users.
    pub async fn receive_invite(
        &self,
        origin: &ServerName,
        room_id: &RoomId,
        room_version: &RoomVersionId,
        raw: CanonicalJsonObject,
        stripped_state: Option<Vec<Value>>,
    ) -> Result<CanonicalJsonObject> {
        let services = &self.services;
        let rules = room_version::rules(room_version).map_err(|_| Error::RemoteError {
            server: origin.to_owned(),
            status: http::StatusCode::BAD_REQUEST,
            body: json!({ "errcode": "M_UNSUPPORTED_ROOM_VERSION", "error": "unsupported room version" }).to_string(),
        })?;
        let pdu = self.parse_and_verify(origin, raw.clone(), &rules).await?;
        let invitee = pdu
            .state_key()
            .and_then(|k| OwnedUserId::try_from(k).ok())
            .ok_or_else(|| Error::Rejected("invite without state_key".into()))?;
        if pdu.event_type() != &TimelineEventType::RoomMember
            || !services.is_local(&invitee)
            || pdu.sender().server_name() != origin
            || pdu.room_id() != room_id
        {
            return Err(Error::Rejected(
                "not a valid invite for a local user".into(),
            ));
        }
        let content: Value = serde_json::from_str(pdu.content().get())?;
        if content.get("membership").and_then(|m| m.as_str()) != Some("invite") {
            return Err(Error::Rejected("not an invite".into()));
        }
        // Rooms identified by their create event: the invite must prove which room it is.
        if rules.room_id_format == ruma::room_version_rules::RoomIdFormatVersion::V2
            && let Some(stripped) = &stripped_state
        {
            let create_ok = stripped.iter().any(|e| {
                e.get("type").and_then(Value::as_str) == Some("m.room.create")
                    && e.get("event_id")
                        .and_then(Value::as_str)
                        .is_none_or(|id| id.strip_prefix('$') == room_id.as_str().strip_prefix('!'))
            });
            if !create_ok {
                return Err(Error::RemoteError {
                    server: origin.to_owned(),
                    status: http::StatusCode::BAD_REQUEST,
                    body: json!({ "errcode": "M_MISSING_PARAM", "error": "invite_room_state must include the room's m.room.create event" }).to_string(),
                });
            }
        }
        if services
            .store
            .get_user(&invitee)
            .await?
            .is_none_or(|u| u.is_deactivated)
        {
            return Err(Error::Rejected("unknown user".into()));
        }

        // Sign and store as an outlier that sets the invitee's membership.
        let mut signed = raw;
        services
            .signing_key
            .sign_event(&services.config.server_name, &mut signed, &rules)?;
        let pdu = Pdu::from_canonical(signed.clone(), &rules)?;
        let decision = services.invite_decision(&invitee, pdu.sender()).await?;
        if let Some(errcode) = decision.errcode() {
            return Err(Error::RemoteError {
                server: origin.to_owned(),
                status: http::StatusCode::FORBIDDEN,
                body: json!({ "errcode": errcode, "error": "The invitee does not accept invites from you." }).to_string(),
            });
        }
        // The inviter sees success; the invitee never sees the invite.
        if decision == asaroton_service::invite_filter::InviteDecision::Ignore {
            return Ok(signed);
        }
        if services.store.get_room(room_id).await?.is_none() {
            services
                .store
                .create_room(RoomInfo {
                    room_id: room_id.to_owned(),
                    room_version: room_version.clone(),
                    creator: None,
                    is_public: false,
                    created_ts: now_ts(),
                })
                .await?;
        }
        let delta = pdu.state_tuple().map(|(t, k)| CurrentStateDelta {
            upsert: vec![((t, k.to_owned()), pdu.event_id().to_owned())],
            remove: vec![],
        });
        let _guard = services.lock_room(room_id).await;
        // Only let the invite drive current state if we are not already participating.
        let participating = self.we_are_in_room_pub(room_id).await?;
        tracing::info!(%room_id, invitee = %invitee, participating, "federation invite received");
        let at = services
            .store
            .persist_event(PersistEvent {
                pdu: pdu.clone(),
                state: EventState::Outlier,
                soft_failed: false,
                backfilled: false,
                stream_ordering: None,
                current_state_delta: if participating { None } else { delta },
                forward_extremities: None,
            })
            .await?
            .stream_ordering;
        services
            .store
            .set_invite_state(room_id, &invitee, &stripped_state.unwrap_or_default())
            .await?;
        // The invitation's number is what the invited user's sync goes by: the wake-up waits
        // until a sync can be told about it.
        services.wake_when_reportable(
            asaroton_store::SyncStream::Events,
            at,
            vec![asaroton_service::Notification::User {
                user_id: invitee.clone(),
            }],
        );
        Ok(signed)
    }

    pub(crate) async fn we_are_in_room_pub(&self, room_id: &RoomId) -> Result<bool> {
        let members = self.services.store.members(room_id, Some("join")).await?;
        Ok(members.iter().any(|m| self.services.is_local(&m.user_id)))
    }

    /// An unsigned event template (as `make_join`/`make_leave` return) built on the current
    /// state: prev events, auth events, depth and content filled in.
    async fn event_template(
        &self,
        ctx: &asaroton_service::rooms::pipeline::RoomContext,
        sender: &UserId,
        event_type: TimelineEventType,
        state_key: Option<&str>,
        content: Value,
    ) -> Result<Value> {
        let services = &self.services;
        let prev_pdus = services.store.get_events(&ctx.extremities).await?;
        let depth = prev_pdus
            .iter()
            .map(|e| u64::from(e.pdu.depth()))
            .max()
            .unwrap_or(0)
            + 1;
        let content_obj = asaroton_core::canonical::from_json_value(content.clone())?;
        let content_raw = serde_json::value::RawValue::from_string(
            asaroton_core::canonical::to_string(&content_obj),
        )?;
        let auth_ids = auth::select_auth_events(
            &event_type,
            sender,
            state_key,
            &content_raw,
            &ctx.state_map(),
            &ctx.rules,
        )?;
        let mut t = json!({
            "type": event_type.to_string(),
            "sender": sender,
            "room_id": ctx.room_id,
            "content": content,
            "origin": services.config.server_name,
            "origin_server_ts": now_ts(),
            "depth": depth,
            "prev_events": ctx.extremities,
            "auth_events": auth_ids,
        });
        if let Some(k) = state_key {
            t["state_key"] = json!(k);
        }
        Ok(t)
    }
}

fn ids_from(v: Option<&Value>) -> Vec<OwnedEventId> {
    v.and_then(|v| v.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|e| match e {
                    Value::String(s) => OwnedEventId::try_from(s.as_str()).ok(),
                    Value::Array(pair) => pair
                        .first()
                        .and_then(|s| s.as_str())
                        .and_then(|s| OwnedEventId::try_from(s).ok()),
                    _ => None,
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Extension used above: the federation JSON with `event_id` included for room versions 1-2.
pub trait PduFederationJson {
    fn to_federation_json_with_id(&self, rules: &RoomVersionRules) -> CanonicalJsonObject;
}

impl PduFederationJson for Pdu {
    fn to_federation_json_with_id(&self, rules: &RoomVersionRules) -> CanonicalJsonObject {
        let mut obj = self.to_federation_json();
        if rules.event_id_format == ruma::room_version_rules::EventIdFormatVersion::V1 {
            obj.insert(
                "event_id".into(),
                CanonicalJsonValue::String(self.event_id().to_string()),
            );
        }
        obj
    }
}
