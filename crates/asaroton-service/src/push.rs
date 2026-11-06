//! Push rules and pushers. Rule evaluation and push gateway delivery come with notifications
//! in a later phase; this stores and serves what clients manage.

use std::sync::Arc;

use asaroton_store::Pusher;
use ruma::{
    UserId,
    push::{EventMatchConditionData, PushCondition, Ruleset, ScalarJsonValue},
};
use serde_json::Value;

use crate::{Error, Notification, Result, Services};

/// The server default ruleset. The poll rules (MSC3930) match the event type with
/// `event_match`, as the MSC and Synapse do, rather than ruma's `event_property_is`.
pub fn default_ruleset(user_id: &UserId) -> Ruleset {
    let mut rules = Ruleset::server_default(user_id);
    // The thread-subscription rules (MSC4306) would silence every thread the user has not
    // subscribed to, which clients without subscription support cannot act on; they stay out
    // until the MSC is stable.
    rules.postcontent.clear();
    let poll = |rule_id: &str| rule_id.contains(".rule.poll_");
    for rule in rules
        .override_
        .iter()
        .chain(rules.underride.iter())
        .filter(|r| poll(&r.rule_id))
        .cloned()
        .collect::<Vec<_>>()
    {
        let mut fixed = rule.clone();
        for cond in &mut fixed.conditions {
            if let PushCondition::EventPropertyIs(data) = cond
                && let ScalarJsonValue::String(value) = &data.value
            {
                *cond = PushCondition::EventMatch(EventMatchConditionData::new(
                    data.key.clone(),
                    value.clone(),
                ));
            }
        }
        if rules.override_.contains(&rule) {
            rules.override_.replace(fixed);
        } else {
            rules.underride.replace(fixed);
        }
    }
    rules
}

impl Services {
    /// The user's push ruleset, creating the server defaults on first access.
    pub async fn push_rules(&self, user_id: &UserId) -> Result<Ruleset> {
        Ok(self.cached_push_rules(user_id).await?.0.clone())
    }

    /// The ruleset and the stream id of its last change (0 for untouched defaults).
    async fn cached_push_rules(&self, user_id: &UserId) -> Result<Arc<(Ruleset, i64)>> {
        if let Some(c) = self.push_rules_cache.get(user_id) {
            return Ok(Arc::clone(&c));
        }
        let entry = match self.store.get_push_rules(user_id).await? {
            Some((json, stream)) => Arc::new((
                serde_json::from_value(json)
                    .map_err(|e| Error::internal(format!("stored push rules: {e}")))?,
                stream,
            )),
            None => Arc::new((default_ruleset(user_id), 0)),
        };
        if self.push_rules_cache.len() > 50_000 {
            self.push_rules_cache.clear();
        }
        self.push_rules_cache
            .insert(user_id.to_owned(), Arc::clone(&entry));
        Ok(entry)
    }

    /// The ruleset as the `m.push_rules` account data content.
    pub async fn push_rules_account_data(&self, user_id: &UserId) -> Result<Value> {
        let rules = self.push_rules(user_id).await?;
        Ok(serde_json::json!({ "global": rules }))
    }

    /// Stream id of the last push-rules change (0 if the defaults were never modified).
    pub async fn push_rules_stream_id(&self, user_id: &UserId) -> Result<i64> {
        Ok(self.cached_push_rules(user_id).await?.1)
    }

    /// Persist a modified ruleset and wake the user's syncs.
    pub async fn save_push_rules(&self, user_id: &UserId, rules: &Ruleset) -> Result<()> {
        let json = serde_json::to_value(rules)?;
        self.store.set_push_rules(user_id, &json).await?;
        self.push_rules_cache.remove(user_id);
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
