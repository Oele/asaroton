//! `POST /createRoom`.

use asaroton_core::{builder::random_room_id, room_version};
use asaroton_store::{RoomAlias, RoomInfo, now_ts};
use ruma::{
    OwnedRoomAliasId, OwnedRoomId, OwnedUserId, RoomVersionId, UserId,
    api::client::room::{Visibility, create_room::v3::RoomPreset},
    events::{StateEventType, TimelineEventType},
    room_version_rules::RoomIdFormatVersion,
    serde::Raw,
};
use serde_json::{Value, json};

use crate::{
    Error, Result, Services,
    rooms::{
        making::InTheMaking,
        pipeline::{LocalEvent, RoomContext},
    },
};

/// Everything the client may ask for when creating a room.
#[derive(Debug)]
pub struct CreateRoomRequest {
    pub creator: OwnedUserId,
    pub room_version: Option<RoomVersionId>,
    pub visibility: Option<Visibility>,
    pub preset: Option<RoomPreset>,
    pub name: Option<String>,
    pub topic: Option<String>,
    pub room_alias_name: Option<String>,
    pub invite: Vec<OwnedUserId>,
    pub is_direct: bool,
    pub creation_content: Option<Value>,
    pub power_level_content_override: Option<Value>,
    /// `(event_type, state_key, content)` triples in client order.
    pub initial_state: Vec<(String, String, Value)>,
}

impl Services {
    /// Create a room, sending the whole initial event sequence under the room lock.
    pub async fn create_room(&self, req: CreateRoomRequest) -> Result<OwnedRoomId> {
        let version = req
            .room_version
            .clone()
            .unwrap_or_else(|| self.config.default_room_version.clone());
        let rules = room_version::rules(&version).map_err(|_| Error::unsupported_room_version())?;
        let creator = req.creator.clone();

        // --- alias is checked before anything is persisted ------------------------------
        let alias = match &req.room_alias_name {
            Some(local) => {
                let alias =
                    OwnedRoomAliasId::try_from(format!("#{local}:{}", self.config.server_name))
                        .map_err(|_| Error::invalid_param("Invalid room alias name."))?;
                if self.store.get_alias(&alias).await?.is_some() {
                    return Err(Error::matrix(
                        ruma::api::error::ErrorKind::RoomInUse,
                        http::StatusCode::BAD_REQUEST,
                        "Room alias already taken.",
                    ));
                }
                Some(alias)
            }
            None => None,
        };

        // A room has one create event, and it is written here. One among `initial_state`
        // would not be refused where state events are otherwise checked: in the room
        // versions whose room ID is the hash of the create event it has no room ID of its
        // own, and would be stored as the first event of yet another room.
        if req
            .initial_state
            .iter()
            .any(|(ty, _, _)| ty == "m.room.create")
        {
            return Err(Error::bad_json(
                "initial_state cannot contain m.room.create.",
            ));
        }

        let preset = req.preset.clone().unwrap_or(match req.visibility {
            Some(Visibility::Public) => RoomPreset::PublicChat,
            _ => RoomPreset::PrivateChat,
        });

        // --- m.room.create --------------------------------------------------------------
        let mut create_content = match req.creation_content {
            Some(Value::Object(m)) => Value::Object(m),
            Some(_) => return Err(Error::bad_json("creation_content must be an object.")),
            None => json!({}),
        };
        create_content["room_version"] = json!(version.as_str());
        if !rules.authorization.use_room_create_sender {
            create_content["creator"] = json!(creator);
        }
        if rules.authorization.explicitly_privilege_room_creators {
            // Additional creators: whatever the client listed, plus everyone invited at
            // creation (they are trusted like the creator).
            let mut extra: Vec<OwnedUserId> = match create_content.get("additional_creators") {
                None => Vec::new(),
                Some(Value::Array(items)) => {
                    let mut out = Vec::new();
                    for i in items {
                        let id = i
                            .as_str()
                            .and_then(|s| OwnedUserId::try_from(s).ok())
                            .ok_or_else(|| {
                                Error::bad_json("additional_creators must be valid user IDs.")
                            })?;
                        out.push(id);
                    }
                    out
                }
                Some(_) => return Err(Error::bad_json("additional_creators must be an array.")),
            };
            // A trusted private chat treats the people invited at creation as peers of the
            // creator, which in these room versions means being creators themselves.
            if preset == RoomPreset::TrustedPrivateChat {
                for u in &req.invite {
                    if *u != creator && !extra.contains(u) {
                        extra.push(u.clone());
                    }
                }
            }
            if extra.is_empty() {
                create_content
                    .as_object_mut()
                    .map(|o| o.remove("additional_creators"));
            } else {
                create_content["additional_creators"] = json!(extra);
            }
        } else if create_content.get("additional_creators").is_some() {
            return Err(Error::bad_json(
                "additional_creators is only valid in room versions with privileged creators.",
            ));
        }
        if version != RoomVersionId::V1 && create_content.get("room_version").is_none() {
            create_content["room_version"] = json!(version.as_str());
        }

        let provisional_room_id = if rules.room_id_format == RoomIdFormatVersion::V1 {
            random_room_id(&self.config.server_name)
        } else {
            // v12: the real ID comes from the create event; use a placeholder for the request.
            OwnedRoomId::try_from("!pending").map_err(|e| Error::internal(e.to_string()))?
        };

        let mut ctx = RoomContext {
            room_id: provisional_room_id.clone(),
            version: version.clone(),
            rules: rules.clone(),
            state: std::collections::HashMap::default(),
            state_group: None,
            extremities: Vec::new(),
            unreferenced: Vec::new(),
            prev_refs: Vec::new(),
            next_depth: 1,
        };

        // The create event needs the room row to exist for the store's version lookup. For v12
        // we only know the ID after building, so build first (no persistence), then register.
        let create_ev = LocalEvent::new(
            provisional_room_id.clone(),
            creator.clone(),
            TimelineEventType::RoomCreate,
        )
        .state_key("")
        .content(&create_content)?;
        // From its first event until the last of its own, the room is kept out of the
        // creator's syncs (see `rooms::making`).
        let (room_id, making) = self
            .create_room_row(&mut ctx, create_ev, &version, &creator)
            .await?;
        let _guard = self.lock_room(&room_id).await;

        // The room's own events. When one of them cannot be written (the creator gave away
        // the power to send the next one, an alias that somebody took in the meantime, the
        // database), what there is of the room is taken away again, unless somebody else is
        // in it by then: a room exists whole or not at all, and the creator's syncs have not
        // been shown any of it.
        let written: Result<()> = async {
            // --- creator join ---------------------------------------------------------------
            let member_content = self.member_content_for(&creator, "join", None).await?;
            self.send_local_event_locked(
                &mut ctx,
                LocalEvent::new(
                    room_id.clone(),
                    creator.clone(),
                    TimelineEventType::RoomMember,
                )
                .state_key(creator.as_str())
                .content(&member_content)?,
            )
            .await?;

            // --- power levels ---------------------------------------------------------------
            let mut pl = default_power_levels(&creator, &rules);
            // Like Synapse: private rooms let every member invite.
            if matches!(
                preset,
                RoomPreset::PrivateChat | RoomPreset::TrustedPrivateChat
            ) {
                pl["invite"] = json!(0);
            }
            if preset == RoomPreset::TrustedPrivateChat
                && !rules.authorization.explicitly_privilege_room_creators
            {
                // (With privileged creators the invitees are creators instead, see above.)
                for u in &req.invite {
                    pl["users"][u.as_str()] = json!(100);
                }
            }
            if let Some(Value::Object(over)) = &req.power_level_content_override {
                // Each key given replaces the default wholesale, maps included (as the reference
                // implementation does), so `"events": {}` really means no per-type levels.
                for (k, v) in over {
                    pl[k] = v.clone();
                }
            }
            self.send_local_event_locked(
                &mut ctx,
                LocalEvent::new(
                    room_id.clone(),
                    creator.clone(),
                    TimelineEventType::RoomPowerLevels,
                )
                .state_key("")
                .content(&pl)?,
            )
            .await
            .map_err(|e| match e {
                Error::Matrix {
                    kind: ruma::api::error::ErrorKind::Forbidden,
                    ..
                } => Error::invalid_param("Invalid power_level_content_override."),
                other => other,
            })?;

            // --- canonical alias ------------------------------------------------------------
            if let Some(alias) = &alias {
                self.store
                    .create_alias(RoomAlias {
                        alias: alias.clone(),
                        room_id: room_id.clone(),
                        creator: Some(creator.clone()),
                    })
                    .await
                    .map_err(|e| match e {
                        asaroton_store::Error::AlreadyExists(_) => Error::matrix(
                            ruma::api::error::ErrorKind::RoomInUse,
                            http::StatusCode::BAD_REQUEST,
                            "Room alias already taken.",
                        ),
                        other => other.into(),
                    })?;
            }

            // --- preset state ---------------------------------------------------------------
            let (join_rule, history, guest) = match preset {
                RoomPreset::PublicChat => ("public", "shared", "forbidden"),
                RoomPreset::PrivateChat | RoomPreset::TrustedPrivateChat => {
                    ("invite", "shared", "can_join")
                }
                _ => ("invite", "shared", "can_join"),
            };
            let initial_has = |ty: &str| {
                req.initial_state
                    .iter()
                    .any(|(t, k, _)| t == ty && k.is_empty())
            };
            if !initial_has("m.room.join_rules") {
                self.send_state_locked(
                    &mut ctx,
                    &room_id,
                    &creator,
                    "m.room.join_rules",
                    "",
                    json!({ "join_rule": join_rule }),
                )
                .await?;
            }
            if !initial_has("m.room.history_visibility") {
                self.send_state_locked(
                    &mut ctx,
                    &room_id,
                    &creator,
                    "m.room.history_visibility",
                    "",
                    json!({ "history_visibility": history }),
                )
                .await?;
            }
            if guest == "can_join" && !initial_has("m.room.guest_access") {
                self.send_state_locked(
                    &mut ctx,
                    &room_id,
                    &creator,
                    "m.room.guest_access",
                    "",
                    json!({ "guest_access": guest }),
                )
                .await?;
            }

            // --- initial_state, name, topic -------------------------------------------------
            for (ty, key, content) in &req.initial_state {
                self.send_state_locked(&mut ctx, &room_id, &creator, ty, key, content.clone())
                    .await?;
            }
            if let Some(name) = &req.name {
                self.send_state_locked(
                    &mut ctx,
                    &room_id,
                    &creator,
                    "m.room.name",
                    "",
                    json!({ "name": name }),
                )
                .await?;
            }
            if let Some(topic) = &req.topic {
                // Plain `topic` plus the extensible `m.topic` representation (spec v1.15+).
                let content = json!({
                    "topic": topic,
                    "m.topic": { "m.text": [{ "body": topic }] },
                });
                self.send_state_locked(&mut ctx, &room_id, &creator, "m.room.topic", "", content)
                    .await?;
            }
            if let Some(alias) = &alias {
                self.send_state_locked(
                    &mut ctx,
                    &room_id,
                    &creator,
                    "m.room.canonical_alias",
                    "",
                    json!({ "alias": alias }),
                )
                .await?;
            }
            Ok(())
        }
        .await;
        if let Err(e) = written {
            self.discard_unfinished_room(&room_id, &creator).await;
            drop(making);
            return Err(e);
        }

        // --- invites --------------------------------------------------------------------
        // The room is whole now. The invitations are events of a room that exists.
        drop(making);
        for invitee in &req.invite {
            if invitee == &creator {
                continue;
            }
            match self
                .invite_locked(&mut ctx, &creator, invitee, None, req.is_direct)
                .await
            {
                Ok(_) | Err(Error::InviteIgnored) => {}
                Err(e) => return Err(e),
            }
        }

        if req.visibility == Some(Visibility::Public) {
            self.store.set_room_public(&room_id, true).await?;
        }
        // A manually upgraded room (predecessor in creation content) carries the creator's
        // room-specific push rules over, like /upgrade does.
        self.after_user_joined(&creator, &room_id).await;
        Ok(room_id)
    }

    /// Build the create event, derive the room ID, create the room row, persist the event.
    /// Also returns the mark that keeps the room out of the creator's syncs while it is
    /// written.
    async fn create_room_row(
        &self,
        ctx: &mut RoomContext,
        create_ev: LocalEvent,
        version: &RoomVersionId,
        creator: &UserId,
    ) -> Result<(OwnedRoomId, InTheMaking<'_>)> {
        if ctx.rules.room_id_format == RoomIdFormatVersion::V1 {
            let room_id = ctx.room_id.clone();
            self.store
                .create_room(RoomInfo {
                    room_id: room_id.clone(),
                    room_version: version.clone(),
                    creator: Some(creator.to_owned()),
                    is_public: false,
                    created_ts: now_ts(),
                })
                .await?;
            let making = self
                .rooms_in_the_making
                .begin(&self.notifier, creator, &room_id);
            let stored = match self.send_local_event_locked(ctx, create_ev).await {
                Ok(stored) => stored,
                Err(e) => {
                    self.discard_unfinished_room(&room_id, creator).await;
                    return Err(e);
                }
            };
            making.first_event_at(stored.stream_ordering);
            return Ok((room_id, making));
        }
        // v12: build without a room_id to learn the ID, register the room, then persist.
        //
        // The ID is the hash of the create event, and two create events of one user with the
        // same content and the same millisecond are one and the same event. The timestamp is
        // the one thing that may differ, so it moves on until the ID is one no room has yet.
        // Whoever registers the room first has it; the store says who that was.
        let mut ts = ruma::MilliSecondsSinceUnixEpoch::now();
        let mut tries = 0;
        let pdu = loop {
            let pdu = asaroton_core::builder::PduBuilder::new(
                creator.to_owned(),
                TimelineEventType::RoomCreate,
            )
            .state_key("")
            .content_object(create_ev.content.clone())
            .depth(ruma::UInt::from(1u32))
            .origin_server_ts(ts)
            .build(&ctx.rules, &self.signing_key, &self.config.server_name)?;
            let is_new = self
                .store
                .create_room(RoomInfo {
                    room_id: pdu.room_id().to_owned(),
                    room_version: version.clone(),
                    creator: Some(creator.to_owned()),
                    is_public: false,
                    created_ts: now_ts(),
                })
                .await?;
            if is_new {
                break pdu;
            }
            tries += 1;
            ts =
                ts.0.checked_add(ruma::UInt::from(1u32))
                    .map(ruma::MilliSecondsSinceUnixEpoch)
                    .filter(|_| tries < 1000)
                    .ok_or_else(|| Error::internal("no free room ID for a new room"))?;
        };
        let room_id = pdu.room_id().to_owned();
        ctx.room_id = room_id.clone();
        let mut ev = create_ev;
        ev.room_id = room_id.clone();
        ev.origin_server_ts = Some(pdu.origin_server_ts());
        let making = self
            .rooms_in_the_making
            .begin(&self.notifier, creator, &room_id);
        let stored = match self.send_local_event_locked(ctx, ev).await {
            Ok(stored) => stored,
            Err(e) => {
                self.discard_unfinished_room(&room_id, creator).await;
                return Err(e);
            }
        };
        debug_assert_eq!(stored.pdu.event_id(), pdu.event_id());
        making.first_event_at(stored.stream_ordering);
        Ok((room_id, making))
    }

    /// Takes away what was written of a room that could not be created in full, as long as
    /// its creator is the only one the room exists for. The creator has not been shown any
    /// of it: it is kept out of their syncs until it is whole. But `initial_state` may hold a
    /// membership event for somebody else, and that person has then seen an invitation. A
    /// room that vanished from under it would leave them with an invitation they can neither
    /// take nor turn down, so such a room stays, unfinished as it is. (An application
    /// service that follows the creator may have been sent the room's first events either
    /// way.)
    async fn discard_unfinished_room(&self, room_id: &ruma::RoomId, creator: &UserId) {
        match self.store.members(room_id, None).await {
            Ok(members) if members.iter().all(|m| m.user_id == *creator) => {}
            Ok(_) => {
                tracing::debug!(%room_id, "a room whose creation failed is kept: it has other members");
                return;
            }
            Err(e) => {
                tracing::warn!(%room_id, error = %e, "could not look at a room whose creation failed");
                return;
            }
        }
        if let Err(e) = self.store.purge_room(room_id).await {
            tracing::warn!(%room_id, error = %e, "could not remove a room whose creation failed");
        }
        self.forget_room_context(room_id);
    }

    pub(crate) async fn send_state_locked(
        &self,
        ctx: &mut RoomContext,
        room_id: &ruma::RoomId,
        sender: &UserId,
        event_type: &str,
        state_key: &str,
        content: Value,
    ) -> Result<()> {
        self.send_local_event_locked(
            ctx,
            LocalEvent::new(room_id.to_owned(), sender.to_owned(), event_type.to_owned())
                .state_key(state_key)
                .content(&content)?,
        )
        .await?;
        Ok(())
    }

    /// Membership event content for a local user, with profile fields filled in.
    pub async fn member_content_for(
        &self,
        user: &UserId,
        membership: &str,
        reason: Option<&str>,
    ) -> Result<Value> {
        let mut c = json!({ "membership": membership });
        if (membership == "join" || membership == "invite")
            && let Some(u) = self.store.get_user(user).await?
        {
            if let Some(name) = u.display_name {
                c["displayname"] = json!(name);
            }
            if let Some(url) = u.avatar_url {
                c["avatar_url"] = json!(url);
            }
        }
        if let Some(r) = reason {
            c["reason"] = json!(r);
        }
        Ok(c)
    }
}

/// Default power levels for a new room (matches what Synapse and the spec examples use).
fn default_power_levels(
    creator: &UserId,
    rules: &ruma::room_version_rules::RoomVersionRules,
) -> Value {
    let mut pl = json!({
        "users_default": 0,
        "events": {
            "m.room.name": 50,
            "m.room.power_levels": 100,
            "m.room.history_visibility": 100,
            "m.room.canonical_alias": 50,
            "m.room.avatar": 50,
            "m.room.tombstone": 100,
            "m.room.server_acl": 100,
            "m.room.encryption": 100,
        },
        "events_default": 0,
        "state_default": 50,
        "ban": 50,
        "kick": 50,
        "redact": 50,
        "invite": 50,
        "historical": 100,
        "users": {},
    });
    if rules.authorization.explicitly_privilege_room_creators {
        // Creators are privileged by the room version; upgrades need more than an admin.
        pl["events"]["m.room.tombstone"] = json!(150);
    } else {
        pl["users"][creator.as_str()] = json!(100);
    }
    pl
}

/// Parse `initial_state` entries from the client API representation.
pub fn initial_state_from_raw(
    raw: &[Raw<ruma::events::AnyInitialStateEvent>],
) -> Result<Vec<(String, String, Value)>> {
    let mut out = Vec::with_capacity(raw.len());
    for r in raw {
        let v: Value = serde_json::from_str(r.json().get())?;
        let ty = v
            .get("type")
            .and_then(|t| t.as_str())
            .ok_or_else(|| Error::bad_json("initial_state entry missing type"))?;
        let key = v.get("state_key").and_then(|t| t.as_str()).unwrap_or("");
        let content = v.get("content").cloned().unwrap_or_else(|| json!({}));
        out.push((ty.to_owned(), key.to_owned(), content));
    }
    Ok(out)
}

/// `(type, state_key)` helper used by callers building state lookups.
pub fn state_key(ty: &str, key: &str) -> (StateEventType, String) {
    (StateEventType::from(ty), key.to_owned())
}
