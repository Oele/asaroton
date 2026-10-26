//! Room upgrades: tombstone the old room and create a successor carrying its settings.

use ruma::room_version_rules::RoomIdFormatVersion;
use ruma::{OwnedRoomId, OwnedUserId, RoomId, RoomVersionId, UserId, events::StateEventType};
use serde_json::{Value, json};
use tessera_core::room_version;

use super::{LocalEvent, create::CreateRoomRequest};
use crate::{Error, Result, Services};

/// State event types copied into the new room, in the order they are sent.
const COPIED_STATE: &[(&str, StateEventType)] = &[
    ("m.room.join_rules", StateEventType::RoomJoinRules),
    (
        "m.room.history_visibility",
        StateEventType::RoomHistoryVisibility,
    ),
    ("m.room.guest_access", StateEventType::RoomGuestAccess),
    ("m.room.name", StateEventType::RoomName),
    ("m.room.topic", StateEventType::RoomTopic),
    ("m.room.avatar", StateEventType::RoomAvatar),
    ("m.room.encryption", StateEventType::RoomEncryption),
    ("m.room.server_acl", StateEventType::RoomServerAcl),
];

impl Services {
    /// Upgrade `old_room` to `new_version`, returning the replacement room ID.
    pub async fn upgrade_room(
        &self,
        user: &UserId,
        old_room: &RoomId,
        new_version: RoomVersionId,
        additional_creators: Vec<OwnedUserId>,
    ) -> Result<OwnedRoomId> {
        self.require_joined(old_room, user).await?;
        let guard = self.lock_room(old_room).await;
        let mut ctx = self.room_context(old_room).await?;

        // Build (but do not yet persist) the tombstone so the successor can point at it; this
        // also checks the upgrader's power to send it.
        let tombstone = self
            .build_local_event_locked(
                &ctx,
                LocalEvent::new(
                    old_room.to_owned(),
                    user.to_owned(),
                    ruma::events::TimelineEventType::RoomTombstone,
                )
                .state_key("")
                .content(&json!({
                    "body": "This room has been replaced",
                    "replacement_room": "!pending",
                }))?,
            )
            .await?;
        let tombstone_id = tombstone.event_id().to_owned();

        // --- the successor --------------------------------------------------------------
        let new_rules =
            room_version::rules(&new_version).map_err(|_| Error::unsupported_room_version())?;
        let mut creation_content = if new_rules.room_id_format == RoomIdFormatVersion::V1 {
            json!({ "predecessor": { "room_id": old_room, "event_id": tombstone_id } })
        } else {
            json!({ "predecessor": { "room_id": old_room } })
        };
        if let Some(old_create) = self
            .state_content(old_room, &StateEventType::RoomCreate, "")
            .await?
        {
            if let Some(t) = old_create.get("type") {
                creation_content["type"] = t.clone();
            }
            if let Some(f) = old_create.get("m.federate") {
                creation_content["m.federate"] = f.clone();
            }
        }
        if !additional_creators.is_empty() {
            creation_content["additional_creators"] = json!(additional_creators);
        }
        let mut initial_state = Vec::new();
        for (ty, st) in COPIED_STATE {
            if let Some(c) = self.state_content(old_room, st, "").await? {
                initial_state.push(((*ty).to_owned(), String::new(), c));
            }
        }
        let old_pl = self
            .state_content(old_room, &StateEventType::RoomPowerLevels, "")
            .await?;
        // Rooms with privileged creators must not list them in `users`.
        let mut new_pl = old_pl.clone();
        if new_rules.authorization.explicitly_privilege_room_creators
            && let Some(pl) = new_pl.as_mut()
            && let Some(Value::Object(users)) = pl.get_mut("users")
        {
            users.remove(user.as_str());
            for c in &additional_creators {
                users.remove(c.as_str());
            }
        }
        // The other way round: a creator whose power was implicit needs an explicit level in a
        // version without privileged creators, or they could not even restore the state.
        if !new_rules.authorization.explicitly_privilege_room_creators
            && let Some(Value::Object(pl)) = new_pl.as_mut()
            && let Value::Object(users) = pl.entry("users").or_insert_with(|| json!({}))
        {
            let current = users
                .get(user.as_str())
                .and_then(Value::as_i64)
                .unwrap_or(0);
            if current < 100 {
                users.insert(user.to_string(), json!(100));
            }
        }
        let new_room = self
            .create_room(CreateRoomRequest {
                creator: user.to_owned(),
                room_version: Some(new_version),
                visibility: None,
                preset: None,
                name: None,
                topic: None,
                room_alias_name: None,
                invite: Vec::new(),
                is_direct: false,
                creation_content: Some(creation_content),
                power_level_content_override: new_pl,
                initial_state,
            })
            .await?;
        if self
            .store
            .get_room(old_room)
            .await?
            .is_some_and(|r| r.is_public)
        {
            self.store.set_room_public(&new_room, true).await?;
        }

        // --- tombstone and lock-down of the old room ------------------------------------
        let tombstone = self
            .build_local_event_locked(
                &ctx,
                LocalEvent::new(
                    old_room.to_owned(),
                    user.to_owned(),
                    ruma::events::TimelineEventType::RoomTombstone,
                )
                .state_key("")
                .content(&json!({
                    "body": "This room has been replaced",
                    "replacement_room": new_room,
                }))?,
            )
            .await?;
        self.persist_built_event_locked(&mut ctx, tombstone).await?;
        if let Some(mut pl) = old_pl {
            let bump = |pl: &mut Value, key: &str| {
                let cur = pl.get(key).and_then(Value::as_i64).unwrap_or(0);
                if cur < 50 {
                    pl[key] = json!(50);
                }
            };
            bump(&mut pl, "events_default");
            bump(&mut pl, "invite");
            // Best effort: the upgrader may lack the power to change power levels.
            let _ = self
                .send_state_locked(&mut ctx, old_room, user, "m.room.power_levels", "", pl)
                .await;
        }

        // --- aliases, push rules --------------------------------------------------------
        for alias in self.store.aliases_for_room(old_room).await? {
            if alias.alias.server_name() == self.config.server_name {
                let _ = self.store.delete_alias(&alias.alias).await;
                let _ = self
                    .store
                    .create_alias(tessera_store::RoomAlias {
                        alias: alias.alias,
                        room_id: new_room.clone(),
                        creator: alias.creator,
                    })
                    .await;
            }
        }
        if let Some(canonical) = self
            .state_content(old_room, &StateEventType::RoomCanonicalAlias, "")
            .await?
            && canonical.get("alias").is_some_and(|a| !a.is_null())
        {
            let _ = self
                .send_state_locked(
                    &mut ctx,
                    old_room,
                    user,
                    "m.room.canonical_alias",
                    "",
                    json!({}),
                )
                .await;
            let _ = self
                .send_state(
                    &new_room,
                    user,
                    "m.room.canonical_alias",
                    "",
                    canonical,
                    None,
                )
                .await;
        }
        drop(guard);
        self.copy_room_push_rules(user, old_room, &new_room).await?;
        Ok(new_room)
    }

    /// Content of a current state event, if present.
    pub async fn state_content(
        &self,
        room_id: &RoomId,
        ty: &StateEventType,
        key: &str,
    ) -> Result<Option<Value>> {
        let Some(id) = self.store.current_state_event(room_id, ty, key).await? else {
            return Ok(None);
        };
        let Some(ev) = self.store.get_event(&id).await? else {
            return Ok(None);
        };
        Ok(serde_json::from_str(ev.pdu.content().get()).ok())
    }

    /// Room-specific push rules (keyed by room ID) follow the user into the new room.
    pub(crate) async fn copy_room_push_rules(
        &self,
        user: &UserId,
        old_room: &RoomId,
        new_room: &RoomId,
    ) -> Result<()> {
        let Some((mut rules, _)) = self.store.get_push_rules(user).await? else {
            return Ok(());
        };
        let mut changed = false;
        if let Some(Value::Array(room_rules)) = rules.get_mut("room")
            && let Some(old) = room_rules
                .iter()
                .find(|r| r.get("rule_id").and_then(Value::as_str) == Some(old_room.as_str()))
                .cloned()
        {
            let mut new = old;
            new["rule_id"] = json!(new_room);
            room_rules
                .retain(|r| r.get("rule_id").and_then(Value::as_str) != Some(new_room.as_str()));
            room_rules.insert(0, new);
            changed = true;
        }
        for kind in ["override", "underride"] {
            if let Some(Value::Array(list)) = rules.get_mut(kind) {
                let mut extra = Vec::new();
                for r in list.iter() {
                    let targets_old =
                        r.get("conditions")
                            .and_then(Value::as_array)
                            .is_some_and(|conds| {
                                conds.iter().any(|c| {
                                    c.get("kind").and_then(Value::as_str) == Some("event_match")
                                        && c.get("key").and_then(Value::as_str) == Some("room_id")
                                        && c.get("pattern").and_then(Value::as_str)
                                            == Some(old_room.as_str())
                                })
                            });
                    if targets_old {
                        let mut n = r.clone();
                        n["rule_id"] = json!(format!(
                            "{}_{}",
                            r.get("rule_id").and_then(Value::as_str).unwrap_or(""),
                            new_room
                        ));
                        if let Some(Value::Array(conds)) = n.get_mut("conditions") {
                            for c in conds.iter_mut() {
                                if c.get("key").and_then(Value::as_str) == Some("room_id") {
                                    c["pattern"] = json!(new_room);
                                }
                            }
                        }
                        extra.push(n);
                    }
                }
                if !extra.is_empty() {
                    let mut merged = extra;
                    merged.append(list);
                    *list = merged;
                    changed = true;
                }
            }
        }
        if changed {
            self.store.set_push_rules(user, &rules).await?;
            self.push_rules_cache.remove(user);
            self.notifier.notify(crate::notifier::Notification::User {
                user_id: user.to_owned(),
            });
        }
        Ok(())
    }
}

impl Error {
    /// Convenience for handlers: the error for an unknown room.
    pub fn room_not_found() -> Self {
        Self::not_found("Room not found.")
    }
}
