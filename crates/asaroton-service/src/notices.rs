//! Server notices: messages from the server to a user, delivered in a dedicated room the
//! user cannot decline (Synapse-compatible `send_server_notice`).

use asaroton_store::NewUser;
use ruma::{OwnedRoomId, OwnedUserId, RoomId, UserId, events::room::member::MembershipState};
use serde_json::{Value, json};

use crate::{Error, Result, Services, rooms::create::CreateRoomRequest};

/// The room tag marking a user's server notices room.
const TAG: &str = "m.server_notice";

impl Services {
    /// The server notices user, if notices are enabled.
    pub fn server_notices_user(&self) -> Option<OwnedUserId> {
        let cfg = self.config.server_notices.as_ref()?;
        OwnedUserId::try_from(format!(
            "@{}:{}",
            cfg.user_localpart, self.config.server_name
        ))
        .ok()
    }

    async fn ensure_notices_user(&self) -> Result<OwnedUserId> {
        let user = self
            .server_notices_user()
            .ok_or_else(|| Error::forbidden("Server notices are not enabled."))?;
        if self.store.get_user(&user).await?.is_none() {
            let name = self
                .config
                .server_notices
                .as_ref()
                .map(|c| c.display_name.clone());
            self.store
                .create_user(NewUser {
                    user_id: user.clone(),
                    password_hash: None,
                    display_name: name,
                    is_admin: false,
                    is_guest: false,
                    appservice_id: None,
                })
                .await?;
        }
        Ok(user)
    }

    /// The user's server notices room, found through the `m.server_notice` tag.
    pub async fn server_notice_room_for(&self, user: &UserId) -> Result<Option<OwnedRoomId>> {
        for ad in self.store.account_data_since(user, 0).await? {
            if ad.data_type == "m.tag"
                && let Some(room) = &ad.room_id
                && ad
                    .content
                    .get("tags")
                    .and_then(Value::as_object)
                    .is_some_and(|t| t.contains_key(TAG))
            {
                return Ok(Some(room.clone()));
            }
        }
        Ok(None)
    }

    /// Whether `room_id` is `user`'s server notices room.
    pub async fn is_server_notice_room(&self, user: &UserId, room_id: &RoomId) -> Result<bool> {
        Ok(self.server_notice_room_for(user).await?.as_deref() == Some(room_id))
    }

    async fn tag_notice_room(&self, user: &UserId, room_id: &RoomId) -> Result<()> {
        let mut content = self
            .store
            .get_account_data(user, Some(room_id), "m.tag")
            .await?
            .map_or_else(|| json!({ "tags": {} }), |a| a.content);
        if !content.get("tags").is_some_and(Value::is_object) {
            content["tags"] = json!({});
        }
        content["tags"][TAG] = json!({});
        self.store
            .set_account_data(user, Some(room_id), "m.tag", &content)
            .await?;
        Ok(())
    }

    /// Send a notice to `target`, creating or re-inviting to their notices room as needed.
    pub async fn send_server_notice(
        &self,
        target: &UserId,
        event_type: &str,
        content: Value,
        txn_id: Option<&str>,
    ) -> Result<ruma::OwnedEventId> {
        let notices_user = self.ensure_notices_user().await?;
        if !self.is_local(target) || self.store.get_user(target).await?.is_none() {
            return Err(Error::not_found("Unknown local user."));
        }
        let room_id = match self.server_notice_room_for(target).await? {
            Some(room) => {
                let membership = self
                    .store
                    .get_membership(&room, target)
                    .await?
                    .map(|m| m.membership);
                if !matches!(
                    membership,
                    Some(MembershipState::Join | MembershipState::Invite)
                ) {
                    self.invite_user(&room, &notices_user, target, None, false)
                        .await?;
                }
                room
            }
            None => {
                let name = self
                    .config
                    .server_notices
                    .as_ref()
                    .map(|c| c.display_name.clone());
                let room = self
                    .create_room(CreateRoomRequest {
                        creator: notices_user.clone(),
                        room_version: None,
                        visibility: None,
                        preset: Some(
                            ruma::api::client::room::create_room::v3::RoomPreset::PrivateChat,
                        ),
                        name,
                        topic: None,
                        room_alias_name: None,
                        invite: vec![target.to_owned()],
                        is_direct: false,
                        creation_content: None,
                        // Only the server talks in this room.
                        power_level_content_override: Some(json!({ "users_default": -10 })),
                        initial_state: Vec::new(),
                    })
                    .await?;
                self.tag_notice_room(target, &room).await?;
                room
            }
        };
        let txn = txn_id.map_or_else(
            || format!("notice-{}", asaroton_store::now_ts()),
            |t| format!("notice-{target}-{t}"),
        );
        self.send_message(
            &room_id,
            &notices_user,
            None,
            &txn,
            event_type,
            content,
            None,
        )
        .await
    }
}
