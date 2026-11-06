//! Profiles (display name, avatar), propagated to the user's membership events.

use ruma::UserId;

use crate::{Result, Services};

impl Services {
    pub async fn set_display_name(&self, user_id: &UserId, name: Option<&str>) -> Result<()> {
        self.store.set_profile(user_id, Some(name), None).await?;
        self.profile_changed(user_id, "displayname").await;
        self.propagate_profile(user_id).await;
        Ok(())
    }

    pub async fn set_avatar_url(&self, user_id: &UserId, url: Option<&str>) -> Result<()> {
        self.store.set_profile(user_id, None, Some(url)).await?;
        self.profile_changed(user_id, "avatar_url").await;
        self.propagate_profile(user_id).await;
        Ok(())
    }

    /// Set (`Some`) or clear (`None`) a custom profile field.
    pub async fn set_profile_field(
        &self,
        user_id: &UserId,
        key: &str,
        value: Option<&serde_json::Value>,
    ) -> Result<()> {
        self.store.set_profile_field(user_id, key, value).await?;
        self.profile_changed(user_id, key).await;
        Ok(())
    }

    /// Record a change to a profile field and wake the syncs that follow profile updates
    /// (MSC4429). A failure to record it must not fail the change itself.
    async fn profile_changed(&self, user_id: &UserId, field: &str) {
        if let Err(e) = self.store.record_profile_update(user_id, field).await {
            tracing::warn!(%user_id, field, error = %e, "could not record a profile update");
        }
        self.notifier.notify(crate::Notification::Profile {
            user_id: user_id.to_owned(),
        });
    }

    /// Send an updated join event in every joined room. Failures in one room (for example a
    /// room where the user may no longer send events) do not block the others.
    async fn propagate_profile(&self, user_id: &UserId) {
        let Ok(rooms) = self.store.rooms_for_user(user_id, "join").await else {
            return;
        };
        for room_id in rooms {
            // Sent directly: the membership logic treats a join of an already joined user as
            // idempotent and would return the existing event.
            let result = async {
                let content = self.member_content_for(user_id, "join", None).await?;
                let _guard = self.lock_room(&room_id).await;
                let mut ctx = self.room_context(&room_id).await?;
                let ev = crate::rooms::LocalEvent::new(
                    room_id.clone(),
                    user_id.to_owned(),
                    ruma::events::TimelineEventType::RoomMember,
                )
                .state_key(user_id.as_str())
                .content(&content)?;
                self.send_local_event_locked(&mut ctx, ev).await
            }
            .await;
            if let Err(e) = result {
                tracing::warn!(%room_id, error = %e, "profile propagation failed");
            }
        }
    }
}
