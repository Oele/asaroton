//! Profiles (display name, avatar). Membership-event propagation comes with rooms.

use ruma::UserId;

use crate::{Result, Services};

impl Services {
    pub async fn set_display_name(&self, user_id: &UserId, name: Option<&str>) -> Result<()> {
        self.store.set_profile(user_id, Some(name), None).await?;
        Ok(())
    }

    pub async fn set_avatar_url(&self, user_id: &UserId, url: Option<&str>) -> Result<()> {
        self.store.set_profile(user_id, None, Some(url)).await?;
        Ok(())
    }
}
