//! Push rules and pushers. Rule evaluation and push gateway delivery come with notifications
//! in a later phase; this stores and serves what clients manage.

use std::sync::Arc;

use asaroton_store::Pusher;
use ruma::{
    UserId,
    push::{EventMatchConditionData, PushCondition, Ruleset, ScalarJsonValue},
};
use serde_json::Value;

use crate::{Error, Notification, Result, Services, txn::TxnTurn};

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
        let entry = Arc::new(self.stored_push_rules(user_id).await?);
        Ok(self.remember_push_rules(user_id, entry))
    }

    /// The ruleset as stored, with the stream id of its last change.
    async fn stored_push_rules(&self, user_id: &UserId) -> Result<(Ruleset, i64)> {
        Ok(match self.store.get_push_rules(user_id).await? {
            Some((json, stream)) => (
                serde_json::from_value(json)
                    .map_err(|e| Error::internal(format!("stored push rules: {e}")))?,
                stream,
            ),
            None => (default_ruleset(user_id), 0),
        })
    }

    /// Puts a ruleset in the cache unless the cache has a later one, and returns what the
    /// cache has. A request that read the store just before a change must not put back what
    /// the change replaced: later changes would start from it and undo this one.
    pub(crate) fn remember_push_rules(
        &self,
        user_id: &UserId,
        entry: Arc<(Ruleset, i64)>,
    ) -> Arc<(Ruleset, i64)> {
        if self.push_rules_cache.len() > 50_000 {
            self.push_rules_cache.clear();
        }
        let mut cached = self
            .push_rules_cache
            .entry(user_id.to_owned())
            .or_insert_with(|| Arc::clone(&entry));
        if cached.1 < entry.1 {
            *cached = entry;
        }
        Arc::clone(&cached)
    }

    /// The user's ruleset for a change, with the user's turn: changes of one user's push rules
    /// are made one at a time, each on what the one before it saved, as each saves the whole
    /// ruleset. Hold the turn until the changed ruleset is saved.
    pub async fn push_rules_to_change(&self, user_id: &UserId) -> Result<(TxnTurn<'_>, Ruleset)> {
        let turn = self.push_rules_turn(user_id).await;
        let (rules, _) = self.stored_push_rules(user_id).await?;
        Ok((turn, rules))
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

    /// Persist a ruleset from [`Services::push_rules_to_change`] and wake the user's syncs.
    pub async fn save_push_rules(&self, user_id: &UserId, rules: &Ruleset) -> Result<()> {
        let json = serde_json::to_value(rules)?;
        let at = self.store.set_push_rules(user_id, &json).await?;
        self.remember_push_rules(user_id, Arc::new((rules.clone(), at)));
        self.wake_when_reportable(
            asaroton_store::SyncStream::AccountData,
            at,
            vec![Notification::User {
                user_id: user_id.to_owned(),
            }],
        );
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
