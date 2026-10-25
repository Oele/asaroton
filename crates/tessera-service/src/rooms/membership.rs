//! Membership changes initiated by local users: join, leave, invite, kick, ban, unban, forget.
//! Remote rooms and remote users are handled by the federation crate in a later phase.

use ruma::{
    OwnedRoomId, OwnedUserId, RoomId, UserId,
    events::{StateEventType, TimelineEventType, room::member::MembershipState},
};
use serde_json::{Value, json};
use tessera_store::StoredEvent;

use crate::{
    Error, Result, Services,
    rooms::{
        pipeline::{LocalEvent, RoomContext},
        power,
    },
};

impl Services {
    /// Join a room this server already participates in.
    pub async fn join_local_room(
        &self,
        room_id: &RoomId,
        user: &UserId,
        reason: Option<&str>,
    ) -> Result<StoredEvent> {
        self.join_local_room_with(room_id, user, reason, None).await
    }

    /// Join a room this server is already in. `extra` holds additional client-supplied
    /// membership content (anything but the protected fields).
    pub async fn join_local_room_with(
        &self,
        room_id: &RoomId,
        user: &UserId,
        reason: Option<&str>,
        extra: Option<&serde_json::Map<String, Value>>,
    ) -> Result<StoredEvent> {
        let _guard = self.lock_room(room_id).await;
        let mut ctx = self.room_context(room_id).await?;

        let current = ctx.membership(user);
        if current == Some(MembershipState::Ban) {
            return Err(Error::forbidden("You are banned from this room."));
        }
        if current == Some(MembershipState::Join) {
            // Idempotent: return the existing join event.
            let id = ctx
                .get(StateEventType::RoomMember, user.as_str())
                .map(|p| p.event_id().to_owned());
            if let Some(id) = id
                && let Some(ev) = self.store.get_event(&id).await?
            {
                return Ok(ev);
            }
        }

        let mut content = self.member_content_for(user, "join", reason).await?;
        if let Some(extra) = extra {
            for (k, v) in extra {
                if !matches!(
                    k.as_str(),
                    "membership" | "join_authorised_via_users_server" | "third_party_invite"
                ) && content.get(k).is_none()
                {
                    content[k] = v.clone();
                }
            }
        }
        let join_rule = join_rule(&ctx);
        let already_allowed = matches!(
            current,
            Some(MembershipState::Invite | MembershipState::Join)
        );
        match join_rule.as_str() {
            "public" => {}
            "invite" | "knock" if already_allowed => {}
            "invite" | "knock" => {
                return Err(Error::forbidden("You are not invited to this room."));
            }
            "restricted" | "knock_restricted" if already_allowed => {}
            "restricted" | "knock_restricted" => {
                if !self.satisfies_restricted_allow(&ctx, user).await? {
                    return Err(Error::forbidden("You are not allowed to join this room."));
                }
                // Resident server vouches for the join with a local user who can invite.
                let authoriser = self.find_join_authoriser(&ctx).await?.ok_or_else(|| {
                    Error::matrix(
                        ruma::api::error::ErrorKind::UnableToGrantJoin,
                        http::StatusCode::FORBIDDEN,
                        "No local user can authorise this join.",
                    )
                })?;
                content["join_authorised_via_users_server"] = json!(authoriser);
            }
            _ => return Err(Error::forbidden("Unknown join rule.")),
        }

        let ev = self
            .send_local_event_locked(
                &mut ctx,
                LocalEvent::new(
                    room_id.to_owned(),
                    user.to_owned(),
                    TimelineEventType::RoomMember,
                )
                .state_key(user.as_str())
                .content(&content)?,
            )
            .await?;
        self.after_user_joined(user, room_id).await;
        Ok(ev)
    }

    /// Housekeeping once a local user has joined a room: push rules for an upgraded room's
    /// predecessor carry over.
    pub async fn after_user_joined(&self, user: &UserId, room_id: &RoomId) {
        if let Ok(Some(create)) = self
            .state_content(room_id, &StateEventType::RoomCreate, "")
            .await
            && let Some(pred) = create
                .get("predecessor")
                .and_then(|p| p.get("room_id"))
                .and_then(Value::as_str)
            && let Ok(old) = ruma::OwnedRoomId::try_from(pred)
            && let Err(e) = self.copy_room_push_rules(user, &old, room_id).await
        {
            tracing::debug!(error = %e, "copying push rules from predecessor failed");
        }
    }

    /// Knock on a room this server is in (join rule `knock` or `knock_restricted`).
    pub async fn knock_room(
        &self,
        room_id: &RoomId,
        user: &UserId,
        reason: Option<&str>,
    ) -> Result<StoredEvent> {
        let _guard = self.lock_room(room_id).await;
        let mut ctx = self.room_context(room_id).await?;
        if !ctx.rules.authorization.knocking {
            return Err(Error::forbidden(
                "This room version does not support knocking.",
            ));
        }
        match ctx.membership(user) {
            Some(MembershipState::Join) => {
                return Err(Error::forbidden("You are already in this room."));
            }
            Some(MembershipState::Invite) => {
                return Err(Error::forbidden("You are already invited to this room."));
            }
            Some(MembershipState::Ban) => {
                return Err(Error::forbidden("You are banned from this room."));
            }
            _ => {}
        }
        if !matches!(join_rule(&ctx).as_str(), "knock" | "knock_restricted") {
            return Err(Error::forbidden("This room does not allow knocking."));
        }
        let content = self.member_content_for(user, "knock", reason).await?;
        self.send_local_event_locked(
            &mut ctx,
            LocalEvent::new(
                room_id.to_owned(),
                user.to_owned(),
                TimelineEventType::RoomMember,
            )
            .state_key(user.as_str())
            .content(&content)?,
        )
        .await
    }

    /// Whether `user` is joined to one of the rooms named by the restricted join rule.
    pub async fn satisfies_restricted_allow(
        &self,
        ctx: &RoomContext,
        user: &UserId,
    ) -> Result<bool> {
        let Some(jr) = ctx.get(StateEventType::RoomJoinRules, "") else {
            return Ok(false);
        };
        let v: Value = serde_json::from_str(jr.content().get())?;
        let Some(allow) = v.get("allow").and_then(|a| a.as_array()) else {
            return Ok(false);
        };
        for rule in allow {
            if rule.get("type").and_then(|t| t.as_str()) != Some("m.room_membership") {
                continue;
            }
            let Some(other) = rule.get("room_id").and_then(|r| r.as_str()) else {
                continue;
            };
            let Ok(other) = OwnedRoomId::try_from(other) else {
                continue;
            };
            if let Some(m) = self.store.get_membership(&other, user).await?
                && m.membership == MembershipState::Join
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// A joined local user with enough power to invite, to put in
    /// `join_authorised_via_users_server`.
    pub async fn find_join_authoriser(&self, ctx: &RoomContext) -> Result<Option<OwnedUserId>> {
        let members = self.store.members(&ctx.room_id, Some("join")).await?;
        Ok(members
            .into_iter()
            .map(|m| m.user_id)
            .filter(|u| self.is_local(u))
            .find(|u| power::can(&ctx.state, u, "invite", &ctx.rules)))
    }

    /// Leave a room (or reject an invite).
    pub async fn leave_room(
        &self,
        room_id: &RoomId,
        user: &UserId,
        reason: Option<&str>,
    ) -> Result<Option<StoredEvent>> {
        let _guard = self.lock_room(room_id).await;
        let mut ctx = self.room_context(room_id).await?;
        match ctx.membership(user) {
            Some(MembershipState::Join | MembershipState::Invite | MembershipState::Knock) => {}
            Some(MembershipState::Leave) => return Ok(None),
            Some(MembershipState::Ban) => {
                return Err(Error::forbidden("You are banned from this room."));
            }
            _ => return Err(Error::forbidden("You are not in this room.")),
        }
        let content = self.member_content_for(user, "leave", reason).await?;
        let ev = self
            .send_local_event_locked(
                &mut ctx,
                LocalEvent::new(
                    room_id.to_owned(),
                    user.to_owned(),
                    TimelineEventType::RoomMember,
                )
                .state_key(user.as_str())
                .content(&content)?,
            )
            .await?;
        Ok(Some(ev))
    }

    /// Invite a user. Remote invitees need federation (later phase).
    pub async fn invite_user(
        &self,
        room_id: &RoomId,
        inviter: &UserId,
        invitee: &UserId,
        reason: Option<&str>,
        is_direct: bool,
    ) -> Result<StoredEvent> {
        let _guard = self.lock_room(room_id).await;
        let mut ctx = self.room_context(room_id).await?;
        self.invite_locked(&mut ctx, inviter, invitee, reason, is_direct)
            .await
    }

    pub(crate) async fn invite_locked(
        &self,
        ctx: &mut RoomContext,
        inviter: &UserId,
        invitee: &UserId,
        reason: Option<&str>,
        is_direct: bool,
    ) -> Result<StoredEvent> {
        if self.is_local(invitee) && self.store.get_user(invitee).await?.is_none() {
            return Err(Error::not_found("Invitee does not exist."));
        }
        match ctx.membership(invitee) {
            Some(MembershipState::Join) => {
                return Err(Error::forbidden("User is already in the room."));
            }
            Some(MembershipState::Ban) => {
                return Err(Error::forbidden("User is banned from the room."));
            }
            _ => {}
        }
        if ctx.membership(inviter) != Some(MembershipState::Join) {
            return Err(Error::forbidden("You are not in the room."));
        }
        let mut content = self.member_content_for(invitee, "invite", reason).await?;
        if is_direct {
            content["is_direct"] = json!(true);
        }
        let ev = LocalEvent::new(
            ctx.room_id.clone(),
            inviter.to_owned(),
            TimelineEventType::RoomMember,
        )
        .state_key(invitee.as_str())
        .content(&content)?;
        if self.is_local(invitee) {
            return self.send_local_event_locked(ctx, ev).await;
        }
        // Remote invitee: build the event, have their server sign it, then persist that copy.
        let built = self.build_local_event_locked(ctx, ev).await?;
        let stripped = Self::stripped_state_for_invite(ctx);
        let signed = self
            .federation()
            .remote_invite(&built, &ctx.version, stripped)
            .await?;
        self.persist_built_event_locked(ctx, signed).await
    }

    /// Kick, ban or unban (`membership` is `leave` or `ban`; unban is `leave` from `ban`).
    pub async fn set_membership_of_other(
        &self,
        room_id: &RoomId,
        sender: &UserId,
        target: &UserId,
        membership: &str,
        reason: Option<&str>,
    ) -> Result<StoredEvent> {
        let _guard = self.lock_room(room_id).await;
        let mut ctx = self.room_context(room_id).await?;
        if ctx.membership(sender) != Some(MembershipState::Join) {
            return Err(Error::forbidden("You are not in the room."));
        }
        let current = ctx.membership(target);
        if membership == "leave" && current == Some(MembershipState::Ban) {
            // unban
            if !power::can(&ctx.state, sender, "ban", &ctx.rules) {
                return Err(Error::forbidden(
                    "You do not have permission to unban users.",
                ));
            }
        } else if membership == "leave"
            && !matches!(
                current,
                Some(MembershipState::Join | MembershipState::Invite | MembershipState::Knock)
            )
        {
            return Err(Error::forbidden("The target user is not in the room."));
        }
        let mut content = json!({ "membership": membership });
        if let Some(r) = reason {
            content["reason"] = json!(r);
        }
        self.send_local_event_locked(
            &mut ctx,
            LocalEvent::new(
                room_id.to_owned(),
                sender.to_owned(),
                TimelineEventType::RoomMember,
            )
            .state_key(target.as_str())
            .content(&content)?,
        )
        .await
        .map_err(|e| match e {
            Error::Matrix {
                kind: ruma::api::error::ErrorKind::Forbidden,
                ..
            } => Error::forbidden("You do not have permission to do that."),
            other => other,
        })
    }

    /// Forget a room the user has left.
    pub async fn forget_room(&self, room_id: &RoomId, user: &UserId) -> Result<()> {
        match self.store.get_membership(room_id, user).await? {
            Some(m) if m.membership == MembershipState::Join => Err(Error::matrix(
                ruma::api::error::ErrorKind::Unknown,
                http::StatusCode::BAD_REQUEST,
                "You must leave the room before forgetting it.",
            )),
            Some(_) => {
                self.store.set_forgotten(room_id, user, true).await?;
                Ok(())
            }
            None => Err(Error::not_found("You have never been in this room.")),
        }
    }

    /// Rooms the user is currently joined to.
    pub async fn joined_rooms(&self, user: &UserId) -> Result<Vec<OwnedRoomId>> {
        Ok(self.store.rooms_for_user(user, "join").await?)
    }

    /// Whether the user is currently joined.
    pub async fn is_joined(&self, room_id: &RoomId, user: &UserId) -> Result<bool> {
        Ok(self
            .store
            .get_membership(room_id, user)
            .await?
            .is_some_and(|m| m.membership == MembershipState::Join))
    }
}

fn join_rule(ctx: &RoomContext) -> String {
    ctx.get(StateEventType::RoomJoinRules, "")
        .and_then(|p| serde_json::from_str::<Value>(p.content().get()).ok())
        .and_then(|v| {
            v.get("join_rule")
                .and_then(|j| j.as_str())
                .map(str::to_owned)
        })
        .unwrap_or_else(|| "invite".to_owned())
}

impl Services {
    /// Stripped state for a remote invite, from the in-memory room context.
    fn stripped_state_for_invite(ctx: &RoomContext) -> Vec<Value> {
        let mut out = Vec::new();
        for pdu in ctx.state.values() {
            let keep = matches!(
                pdu.state_tuple(),
                Some((
                    StateEventType::RoomCreate
                        | StateEventType::RoomJoinRules
                        | StateEventType::RoomName
                        | StateEventType::RoomAvatar
                        | StateEventType::RoomCanonicalAlias
                        | StateEventType::RoomTopic
                        | StateEventType::RoomEncryption,
                    _
                ))
            );
            if keep {
                let mut v = pdu.to_client_json();
                if let Value::Object(o) = &mut v {
                    o.retain(|k, _| {
                        matches!(k.as_str(), "type" | "state_key" | "content" | "sender")
                    });
                }
                out.push(v);
            }
        }
        out
    }
}
