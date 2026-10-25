//! Profiles (display name, avatar), propagated to the user's membership events.

use ruma::UserId;

use crate::{Result, Services};

impl Services {
    pub async fn set_display_name(&self, user_id: &UserId, name: Option<&str>) -> Result<()> {
        self.store.set_profile(user_id, Some(name), None).await?;
        self.propagate_profile(user_id).await;
        Ok(())
    }

    pub async fn set_avatar_url(&self, user_id: &UserId, url: Option<&str>) -> Result<()> {
        self.store.set_profile(user_id, None, Some(url)).await?;
        self.propagate_profile(user_id).await;
        Ok(())
    }

    /// Send an updated join event in every joined room. Failures in one room (for example a
    /// room where the user may no longer send events) do not block the others.
    async fn propagate_profile(&self, user_id: &UserId) {
        let Ok(rooms) = self.store.rooms_for_user(user_id, "join").await else {
            return;
        };
        for room_id in rooms {
            let Ok(content) = self.member_content_for(user_id, "join", None).await else {
                continue;
            };
            if let Err(e) = self
                .send_state(
                    &room_id,
                    user_id,
                    "m.room.member",
                    user_id.as_str(),
                    content,
                )
                .await
            {
                tracing::debug!(%room_id, error = %e, "profile propagation failed");
            }
        }
    }
}
