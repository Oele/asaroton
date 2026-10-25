//! The local event pipeline: turn a request from a local user into a signed, authorised,
//! persisted event.
//!
//! Federated events arrive through a different entry point (the federation crate) because
//! they need state resolution across forward extremities; local sends always build on the
//! room's current state, which keeps this path simple and fast.

use std::{collections::HashMap, sync::Arc};

use ruma::{
    CanonicalJsonObject, MilliSecondsSinceUnixEpoch, OwnedEventId, OwnedRoomId, OwnedUserId,
    RoomId, UInt,
    events::{StateEventType, TimelineEventType},
    room_version_rules::RoomVersionRules,
};
use tessera_core::{Pdu, auth, builder::PduBuilder, room_version};
use tessera_store::{CurrentStateDelta, EventState, PersistEvent, StoredEvent};
use tokio::sync::{Mutex, OwnedMutexGuard};

use crate::{Error, Notification, Result, Services, rooms::power::StatePdus};

/// A request to create an event on behalf of a local user.
#[derive(Clone, Debug)]
pub struct LocalEvent {
    pub room_id: OwnedRoomId,
    pub sender: OwnedUserId,
    pub event_type: TimelineEventType,
    pub state_key: Option<String>,
    pub content: CanonicalJsonObject,
    pub redacts: Option<OwnedEventId>,
    /// Appservices may set the timestamp (`ts` massaging).
    pub origin_server_ts: Option<MilliSecondsSinceUnixEpoch>,
}

impl LocalEvent {
    pub fn new(
        room_id: OwnedRoomId,
        sender: OwnedUserId,
        event_type: impl Into<TimelineEventType>,
    ) -> Self {
        Self {
            room_id,
            sender,
            event_type: event_type.into(),
            state_key: None,
            content: CanonicalJsonObject::new(),
            redacts: None,
            origin_server_ts: None,
        }
    }

    #[must_use]
    pub fn state_key(mut self, key: impl Into<String>) -> Self {
        self.state_key = Some(key.into());
        self
    }

    pub fn content<T: serde::Serialize>(mut self, content: &T) -> Result<Self> {
        self.content = tessera_core::canonical::from_serialize(content)?;
        Ok(self)
    }

    #[must_use]
    pub fn content_object(mut self, content: CanonicalJsonObject) -> Self {
        self.content = content;
        self
    }
}

/// The room's current state loaded as PDUs, plus the room version rules.
#[derive(Debug)]
pub struct RoomContext {
    pub room_id: OwnedRoomId,
    pub version: ruma::RoomVersionId,
    pub rules: RoomVersionRules,
    pub state: StatePdus,
    pub state_group: Option<i64>,
    pub extremities: Vec<OwnedEventId>,
}

impl RoomContext {
    pub fn state_map(&self) -> tessera_core::state::StateMap {
        tessera_core::state::state_map_from_pdus(self.state.values())
    }

    pub fn get(&self, ty: StateEventType, key: &str) -> Option<&Pdu> {
        self.state.get(&(ty, key.to_owned()))
    }

    pub fn membership(
        &self,
        user: &ruma::UserId,
    ) -> Option<ruma::events::room::member::MembershipState> {
        let pdu = self.get(StateEventType::RoomMember, user.as_str())?;
        let v: serde_json::Value = serde_json::from_str(pdu.content().get()).ok()?;
        v.get("membership")
            .and_then(|m| m.as_str())
            .map(|m| m.to_owned().into())
    }
}

impl Services {
    /// Per-room lock serialising local sends, so forward extremities and state groups advance
    /// linearly.
    pub async fn lock_room(&self, room_id: &RoomId) -> OwnedMutexGuard<()> {
        let lock = self
            .room_locks
            .entry(room_id.to_owned())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();
        lock.lock_owned().await
    }

    /// Load the room's version, rules and current state as PDUs.
    pub async fn room_context(&self, room_id: &RoomId) -> Result<RoomContext> {
        let info = self
            .store
            .get_room(room_id)
            .await?
            .ok_or_else(|| Error::not_found("Room not found."))?;
        let rules = room_version::rules(&info.room_version)?;
        let pdus = self.store.current_state_pdus(room_id).await?;
        let mut state: StatePdus = HashMap::new();
        for se in pdus {
            if let Some((ty, key)) = se.pdu.state_tuple() {
                state.insert((ty, key.to_owned()), se.pdu.clone());
            }
        }
        let extremities = self.store.forward_extremities(room_id).await?;
        // State group of the single extremity, if there is exactly one; otherwise a fresh
        // full group gets created when the next event is persisted.
        let state_group = if extremities.len() == 1 {
            self.store
                .get_event(&extremities[0])
                .await?
                .and_then(|e| e.state_group)
        } else {
            None
        };
        Ok(RoomContext {
            room_id: room_id.to_owned(),
            version: info.room_version,
            rules,
            state,
            state_group,
            extremities,
        })
    }

    /// Build, authorise and persist a local event. Takes the room lock.
    pub async fn send_local_event(&self, ev: LocalEvent) -> Result<StoredEvent> {
        let _guard = self.lock_room(&ev.room_id).await;
        let mut ctx = self.room_context(&ev.room_id).await?;
        self.send_local_event_locked(&mut ctx, ev).await
    }

    /// Same as [`Services::send_local_event`], for callers that already hold the room lock
    /// and a context (room creation sends many events in a row). Updates `ctx` in place.
    pub async fn send_local_event_locked(
        &self,
        ctx: &mut RoomContext,
        ev: LocalEvent,
    ) -> Result<StoredEvent> {
        let rules = ctx.rules.clone();

        // prev events and depth
        let prev_pdus = self.store.get_events(&ctx.extremities).await?;
        let depth = prev_pdus
            .iter()
            .map(|e| u64::from(e.pdu.depth()))
            .max()
            .unwrap_or(0)
            + 1;
        let prev_refs: Vec<tessera_core::builder::EventRef> = prev_pdus
            .iter()
            .map(|e| tessera_core::builder::EventRef {
                event_id: e.pdu.event_id().to_owned(),
                sha256: content_hash(e.pdu.raw()),
            })
            .collect();

        // auth events from current state
        let content_raw = serde_json::value::RawValue::from_string(
            tessera_core::canonical::to_string(&ev.content),
        )?;
        let auth_ids = auth::select_auth_events(
            &ev.event_type,
            &ev.sender,
            ev.state_key.as_deref(),
            &content_raw,
            &ctx.state_map(),
            &rules,
        )?;
        let auth_refs: Vec<tessera_core::builder::EventRef> = auth_ids
            .iter()
            .map(|id| tessera_core::builder::EventRef {
                event_id: id.clone(),
                sha256: ctx
                    .state
                    .values()
                    .find(|p| p.event_id() == id)
                    .and_then(|p| content_hash(p.raw())),
            })
            .collect();

        let mut builder = PduBuilder::new(ev.sender.clone(), ev.event_type.clone())
            .content_object(ev.content)
            .depth(UInt::try_from(depth).unwrap_or(UInt::MAX))
            .prev_events(prev_refs)
            .auth_events(auth_refs);
        if let Some(key) = &ev.state_key {
            builder = builder.state_key(key.clone());
        }
        if let Some(r) = &ev.redacts {
            builder = builder.redacts(r.clone());
        }
        if let Some(ts) = ev.origin_server_ts {
            builder = builder.origin_server_ts(ts);
        }
        let is_create = ev.event_type == TimelineEventType::RoomCreate;
        if !(is_create && rules.room_id_format == ruma::room_version_rules::RoomIdFormatVersion::V2)
        {
            builder = builder.room_id(ev.room_id.clone());
        }
        let pdu = builder.build(&rules, &self.signing_key, &self.config.server_name)?;

        // checks
        auth::check_format(&pdu, &rules).map_err(|e| {
            Error::matrix(
                ruma::api::error::ErrorKind::TooLarge,
                http::StatusCode::BAD_REQUEST,
                e.to_string(),
            )
        })?;
        auth::check_against_state(&pdu, &rules, &ctx.state).map_err(|e| match e {
            tessera_core::Error::Auth(msg) => Error::forbidden(msg),
            other => other.into(),
        })?;

        // state bookkeeping
        let (state, delta) = match pdu.state_tuple() {
            Some((ty, key)) => {
                let d = vec![((ty, key.to_owned()), pdu.event_id().to_owned())];
                let prev = ctx.state_group;
                let base_delta = if prev.is_none() {
                    // No reusable group: new full group from the current state plus this event.
                    let mut full: Vec<_> = ctx.state_map().into_iter().collect();
                    full.extend(d.clone());
                    full
                } else {
                    d.clone()
                };
                (
                    EventState::New {
                        prev,
                        delta: base_delta,
                    },
                    Some(CurrentStateDelta {
                        upsert: d,
                        remove: vec![],
                    }),
                )
            }
            None => match ctx.state_group {
                Some(g) => (EventState::Existing(g), None),
                None => (
                    EventState::New {
                        prev: None,
                        delta: ctx.state_map().into_iter().collect(),
                    },
                    None,
                ),
            },
        };

        let persisted = self
            .store
            .persist_event(PersistEvent {
                pdu: pdu.clone(),
                state,
                soft_failed: false,
                backfilled: false,
                current_state_delta: delta,
                forward_extremities: Some(vec![pdu.event_id().to_owned()]),
            })
            .await?;

        // update the in-memory context for the next event in this batch
        if let Some((ty, key)) = pdu.state_tuple() {
            ctx.state.insert((ty, key.to_owned()), pdu.clone());
        }
        ctx.state_group = persisted.state_group;
        ctx.extremities = vec![pdu.event_id().to_owned()];

        // side effects
        if pdu.event_type() == &TimelineEventType::RoomRedaction {
            self.apply_redaction(ctx, &pdu).await?;
        }
        self.notifier.notify(Notification::RoomEvent {
            room_id: ctx.room_id.clone(),
            stream_ordering: persisted.stream_ordering,
        });

        Ok(StoredEvent {
            pdu,
            stream_ordering: persisted.stream_ordering,
            state_group: persisted.state_group,
            soft_failed: false,
            outlier: false,
            redacted: false,
        })
    }

    /// Apply an accepted redaction event to its target if the sender is allowed to.
    async fn apply_redaction(&self, ctx: &RoomContext, redaction: &Pdu) -> Result<()> {
        let Some(target_id) = redaction.redacts() else {
            return Ok(());
        };
        let Some(target) = self.store.get_event(target_id).await? else {
            return Ok(());
        };
        if target.pdu.room_id() != ctx.room_id || target.redacted {
            return Ok(());
        }
        let allowed = target.pdu.sender() == redaction.sender()
            || super::power::can(&ctx.state, redaction.sender(), "redact", &ctx.rules);
        if !allowed {
            return Ok(());
        }
        let redacted =
            tessera_core::redaction::redact_pdu(&target.pdu, &ctx.rules, Some(redaction))?;
        self.store.redact_event(target_id, &redacted).await?;
        Ok(())
    }
}

/// The `sha256` content hash of a stored event, for room versions 1-2 event references.
fn content_hash(raw: &CanonicalJsonObject) -> Option<String> {
    raw.get("hashes")?
        .as_object()?
        .get("sha256")?
        .as_str()
        .map(str::to_owned)
}
