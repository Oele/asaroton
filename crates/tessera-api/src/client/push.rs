//! Push rules and pushers.

use ruma::{
    api::client::push::{
        delete_pushrule, get_pushers, get_pushrule, get_pushrule_actions, get_pushrule_enabled,
        get_pushrules_all, get_pushrules_global_scope, set_pusher, set_pushrule,
        set_pushrule_actions, set_pushrule_enabled,
    },
    push::{InsertPushRuleError, RemovePushRuleError, RuleKind},
};
use serde_json::Value;
use tessera_store::Pusher;

use crate::{AppState, Error, Result, Ruma};

fn rule_not_found() -> Error {
    Error::not_found("Push rule not found.")
}

pub async fn rules_all(
    services: AppState,
    req: Ruma<get_pushrules_all::v3::Request>,
) -> Result<get_pushrules_all::v3::Response> {
    let rules = services.push_rules(&req.sender()?.user_id).await?;
    Ok(get_pushrules_all::v3::Response::new(rules))
}

pub async fn rules_global(
    services: AppState,
    req: Ruma<get_pushrules_global_scope::v3::Request>,
) -> Result<get_pushrules_global_scope::v3::Response> {
    let rules = services.push_rules(&req.sender()?.user_id).await?;
    Ok(get_pushrules_global_scope::v3::Response::new(rules))
}

pub async fn get_rule(
    services: AppState,
    req: Ruma<get_pushrule::v3::Request>,
) -> Result<get_pushrule::v3::Response> {
    let rules = services.push_rules(&req.sender()?.user_id).await?;
    let rule = rules
        .get(req.body.kind.clone(), &req.body.rule_id)
        .ok_or_else(rule_not_found)?;
    Ok(get_pushrule::v3::Response::new(rule.to_owned().into()))
}

pub async fn set_rule(
    services: AppState,
    req: Ruma<set_pushrule::v3::Request>,
) -> Result<set_pushrule::v3::Response> {
    let user_id = &req.sender()?.user_id;
    let mut rules = services.push_rules(user_id).await?;
    rules
        .insert(
            req.body.rule.clone(),
            req.body.after.as_deref(),
            req.body.before.as_deref(),
        )
        .map_err(|e| match e {
            InsertPushRuleError::ServerDefaultRuleId => {
                Error::invalid_param("Rule IDs starting with a dot are reserved.")
            }
            InsertPushRuleError::InvalidRuleId => Error::invalid_param("Invalid rule ID."),
            InsertPushRuleError::RelativeToServerDefaultRule => {
                Error::invalid_param("Cannot position a rule relative to a server default rule.")
            }
            InsertPushRuleError::UnknownRuleId => {
                Error::not_found("The before/after rule does not exist.")
            }
            InsertPushRuleError::BeforeHigherThanAfter => {
                Error::invalid_param("before must come before after.")
            }
            _ => Error::invalid_param("Invalid push rule."),
        })?;
    services.save_push_rules(user_id, &rules).await?;
    Ok(set_pushrule::v3::Response::new())
}

pub async fn delete_rule(
    services: AppState,
    req: Ruma<delete_pushrule::v3::Request>,
) -> Result<delete_pushrule::v3::Response> {
    let user_id = &req.sender()?.user_id;
    let mut rules = services.push_rules(user_id).await?;
    rules
        .remove(req.body.kind.clone(), &req.body.rule_id)
        .map_err(|e| match e {
            RemovePushRuleError::ServerDefault => {
                Error::invalid_param("Cannot delete a server default rule.")
            }
            _ => rule_not_found(),
        })?;
    services.save_push_rules(user_id, &rules).await?;
    Ok(delete_pushrule::v3::Response::new())
}

pub async fn get_enabled(
    services: AppState,
    req: Ruma<get_pushrule_enabled::v3::Request>,
) -> Result<get_pushrule_enabled::v3::Response> {
    let rules = services.push_rules(&req.sender()?.user_id).await?;
    let rule = rules
        .get(req.body.kind.clone(), &req.body.rule_id)
        .ok_or_else(rule_not_found)?;
    Ok(get_pushrule_enabled::v3::Response::new(rule.enabled()))
}

pub async fn set_enabled(
    services: AppState,
    req: Ruma<set_pushrule_enabled::v3::Request>,
) -> Result<set_pushrule_enabled::v3::Response> {
    let user_id = &req.sender()?.user_id;
    let mut rules = services.push_rules(user_id).await?;
    rules
        .set_enabled(req.body.kind.clone(), &req.body.rule_id, req.body.enabled)
        .map_err(|_| rule_not_found())?;
    services.save_push_rules(user_id, &rules).await?;
    Ok(set_pushrule_enabled::v3::Response::new())
}

pub async fn get_actions(
    services: AppState,
    req: Ruma<get_pushrule_actions::v3::Request>,
) -> Result<get_pushrule_actions::v3::Response> {
    let rules = services.push_rules(&req.sender()?.user_id).await?;
    let rule = rules
        .get(req.body.kind.clone(), &req.body.rule_id)
        .ok_or_else(rule_not_found)?;
    Ok(get_pushrule_actions::v3::Response::new(
        rule.actions().to_vec(),
    ))
}

pub async fn set_actions(
    services: AppState,
    req: Ruma<set_pushrule_actions::v3::Request>,
) -> Result<set_pushrule_actions::v3::Response> {
    let user_id = &req.sender()?.user_id;
    let mut rules = services.push_rules(user_id).await?;
    rules
        .set_actions(
            req.body.kind.clone(),
            &req.body.rule_id,
            req.body.actions.clone(),
        )
        .map_err(|_| rule_not_found())?;
    services.save_push_rules(user_id, &rules).await?;
    Ok(set_pushrule_actions::v3::Response::new())
}

pub async fn get_pushers(
    services: AppState,
    req: Ruma<get_pushers::v3::Request>,
) -> Result<get_pushers::v3::Response> {
    let pushers = services.pushers(&req.sender()?.user_id).await?;
    let mut out = Vec::new();
    for p in pushers {
        let v = serde_json::json!({
            "app_id": p.app_id, "pushkey": p.pushkey, "kind": p.kind, "app_display_name": p.app_display_name,
            "device_display_name": p.device_display_name, "profile_tag": p.profile_tag, "lang": p.lang, "data": p.data,
        });
        if let Ok(pusher) = serde_json::from_value::<ruma::api::client::push::Pusher>(v) {
            out.push(pusher);
        }
    }
    Ok(get_pushers::v3::Response::new(out))
}

pub async fn set_pusher(
    services: AppState,
    req: Ruma<set_pusher::v3::Request>,
) -> Result<set_pusher::v3::Response> {
    let user_id = &req.sender()?.user_id;
    match &req.body.action {
        set_pusher::v3::PusherAction::Post(post) => {
            let v: Value =
                serde_json::to_value(&post.pusher).map_err(|e| Error::bad_json(e.to_string()))?;
            let get = |k: &str| v.get(k).and_then(|x| x.as_str()).map(str::to_owned);
            services
                .set_pusher(Pusher {
                    user_id: user_id.clone(),
                    app_id: get("app_id").unwrap_or_default(),
                    pushkey: get("pushkey").unwrap_or_default(),
                    kind: get("kind").unwrap_or_default(),
                    app_display_name: get("app_display_name").unwrap_or_default(),
                    device_display_name: get("device_display_name").unwrap_or_default(),
                    profile_tag: get("profile_tag"),
                    lang: get("lang").unwrap_or_default(),
                    data: v
                        .get("data")
                        .cloned()
                        .unwrap_or(Value::Object(serde_json::Map::new())),
                })
                .await?;
        }
        set_pusher::v3::PusherAction::Delete(ids) => {
            services
                .delete_pusher(user_id, &ids.app_id, &ids.pushkey)
                .await?;
        }
        _ => return Err(Error::bad_json("Unknown pusher action.")),
    }
    Ok(set_pusher::v3::Response::new())
}

/// Keep `RuleKind` referenced for readers of this module.
#[allow(dead_code)]
fn _kinds(_: RuleKind) {}
