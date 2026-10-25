//! Push rules and pushers. Rule evaluation and push gateway delivery come with notifications
//! in a later phase; this stores and serves what clients manage.

use ruma::{UserId, push::Ruleset};
use serde_json::Value;
use tessera_store::Pusher;

use crate::{Error, Notification, Result, Services};

impl Services {
    /// The user's push ruleset, creating the server defaults on first access.
    pub async fn push_rules(&self, user_id: &UserId) -> Result<Ruleset> {
        if let Some((json, _)) = self.store.get_push_rules(user_id).await? {
            return serde_json::from_value(json)
                .map_err(|e| Error::internal(format!("stored push rules: {e}")));
        }
        Ok(Ruleset::server_default(user_id))
    }

    /// The ruleset as the `m.push_rules` account data content.
    pub async fn push_rules_account_data(&self, user_id: &UserId) -> Result<Value> {
        let rules = self.push_rules(user_id).await?;
        Ok(serde_json::json!({ "global": rules }))
    }

    /// Stream id of the last push-rules change (0 if the defaults were never modified).
    pub async fn push_rules_stream_id(&self, user_id: &UserId) -> Result<i64> {
        Ok(self
            .store
            .get_push_rules(user_id)
            .await?
            .map_or(0, |(_, s)| s))
    }

    /// Persist a modified ruleset and wake the user's syncs.
    pub async fn save_push_rules(&self, user_id: &UserId, rules: &Ruleset) -> Result<()> {
        let json = serde_json::to_value(rules)?;
        self.store.set_push_rules(user_id, &json).await?;
        self.notifier.notify(Notification::User {
            user_id: user_id.to_owned(),
        });
        Ok(())
    }

    pub async fn pushers(&self, user_id: &UserId) -> Result<Vec<Pusher>> {
        Ok(self.store.pushers(user_id).await?)
    }

    pub async fn set_pusher(&self, pusher: Pusher) -> Result<()> {
        Ok(self.store.set_pusher(pusher).await?)
    }

    pub async fn delete_pusher(&self, user_id: &UserId, app_id: &str, pushkey: &str) -> Result<()> {
        Ok(self.store.delete_pusher(user_id, app_id, pushkey).await?)
    }
}
