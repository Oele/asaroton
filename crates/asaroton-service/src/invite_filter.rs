//! Invite filtering.
//!
//! Invite blocking (MSC4380, in the specification as `m.invite_permission_config`): one
//! switch with which a user refuses every invite. It is looked at first.
//!
//! Finer rules (MSC4155): a user's `org.matrix.msc4155.invite_permission_config` account data
//! decides whether an invite from a given user is allowed, blocked or silently dropped. They
//! stay in force when the switch above is absent or does not block.

use ruma::UserId;
use serde::Deserialize;

use crate::{Result, Services, glob_match};

/// The account data type holding the MSC4155 configuration.
pub const INVITE_PERMISSION_CONFIG: &str = "org.matrix.msc4155.invite_permission_config";
/// The account data types of the block-everything switch: its name in the specification and
/// the one the proposal used before.
pub const INVITE_BLOCKING_CONFIG: [&str; 2] = [
    "m.invite_permission_config",
    "org.matrix.msc4380.invite_permission_config",
];

/// What to do with an invite.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InviteDecision {
    Allow,
    /// The user refuses all invites: reject with `M_INVITE_BLOCKED`.
    BlockAll,
    /// Reject with `ORG.MATRIX.MSC4155.M_INVITE_BLOCKED`.
    Block,
    /// Report success to the inviter but never show the invite to the invitee.
    Ignore,
}

impl InviteDecision {
    /// The error code to refuse the invite with, when it is to be refused.
    pub fn errcode(self) -> Option<&'static str> {
        match self {
            Self::BlockAll => Some("M_INVITE_BLOCKED"),
            Self::Block => Some("ORG.MATRIX.MSC4155.M_INVITE_BLOCKED"),
            Self::Allow | Self::Ignore => None,
        }
    }
}

/// Whether the content of an invite blocking configuration refuses all invites. Anything but
/// `default_action: "block"` (an empty object, another value) leaves invites as they are.
fn blocks_all(content: &serde_json::Value) -> bool {
    content.get("default_action").and_then(|v| v.as_str()) == Some("block")
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct Config {
    allowed_users: Option<Vec<String>>,
    ignored_users: Option<Vec<String>>,
    blocked_users: Option<Vec<String>>,
    allowed_servers: Option<Vec<String>>,
    ignored_servers: Option<Vec<String>>,
    blocked_servers: Option<Vec<String>>,
}

impl Config {
    /// User lists take precedence over server lists; within each, allow beats block beats
    /// ignore.
    fn decide(&self, inviter: &UserId) -> InviteDecision {
        let hit = |list: &Option<Vec<String>>, value: &str| {
            list.as_deref()
                .unwrap_or_default()
                .iter()
                .any(|g| glob_match(g, value))
        };
        let user = inviter.as_str();
        let server = inviter.server_name().as_str();
        if hit(&self.allowed_users, user) {
            InviteDecision::Allow
        } else if hit(&self.blocked_users, user) {
            InviteDecision::Block
        } else if hit(&self.ignored_users, user) {
            InviteDecision::Ignore
        } else if hit(&self.allowed_servers, server) {
            InviteDecision::Allow
        } else if hit(&self.blocked_servers, server) {
            InviteDecision::Block
        } else if hit(&self.ignored_servers, server) {
            InviteDecision::Ignore
        } else {
            InviteDecision::Allow
        }
    }
}

impl Services {
    /// Whether `invitee` (a local user) accepts an invite from `inviter`.
    pub async fn invite_decision(
        &self,
        invitee: &UserId,
        inviter: &UserId,
    ) -> Result<InviteDecision> {
        if self.invites_blocked(invitee).await? {
            return Ok(InviteDecision::BlockAll);
        }
        let Some(ad) = self
            .store
            .get_account_data(invitee, None, INVITE_PERMISSION_CONFIG)
            .await?
        else {
            return Ok(InviteDecision::Allow);
        };
        let cfg: Config = serde_json::from_value(ad.content).unwrap_or_default();
        Ok(cfg.decide(inviter))
    }

    /// Whether the user has switched all invites off. While that is so, invites they still
    /// hold are not shown to them either.
    pub async fn invites_blocked(&self, user: &UserId) -> Result<bool> {
        for ty in INVITE_BLOCKING_CONFIG {
            if let Some(ad) = self.store.get_account_data(user, None, ty).await?
                && blocks_all(&ad.content)
            {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(json: serde_json::Value) -> Config {
        serde_json::from_value(json).unwrap()
    }

    #[test]
    fn precedence() {
        let bob = ruma::user_id!("@bob:hs2");
        let evil = ruma::user_id!("@user-evil:hs2");
        let c = cfg(serde_json::json!({ "allowed_users": [bob], "blocked_servers": ["hs*"] }));
        assert_eq!(c.decide(bob), InviteDecision::Allow);
        assert_eq!(c.decide(evil), InviteDecision::Block);
        let c = cfg(
            serde_json::json!({ "blocked_users": ["@user-?*"], "allowed_servers": ["hs2"], "ignored_users": null }),
        );
        assert_eq!(c.decide(evil), InviteDecision::Block);
        assert_eq!(c.decide(bob), InviteDecision::Allow);
        let c = cfg(serde_json::json!({ "ignored_servers": ["hs2"] }));
        assert_eq!(c.decide(bob), InviteDecision::Ignore);
        assert_eq!(
            cfg(serde_json::json!({})).decide(bob),
            InviteDecision::Allow
        );
    }

    #[test]
    fn only_an_explicit_block_switches_invites_off() {
        assert!(blocks_all(
            &serde_json::json!({ "default_action": "block" })
        ));
        for other in [
            serde_json::json!({}),
            serde_json::json!({ "default_action": "allow" }),
            serde_json::json!({ "default_action": null }),
            serde_json::json!({ "default_action": true }),
            serde_json::json!({ "blocked_users": ["*"] }),
        ] {
            assert!(!blocks_all(&other), "{other}");
        }
        assert_eq!(InviteDecision::BlockAll.errcode(), Some("M_INVITE_BLOCKED"));
        assert_eq!(
            InviteDecision::Block.errcode(),
            Some("ORG.MATRIX.MSC4155.M_INVITE_BLOCKED")
        );
        assert_eq!(InviteDecision::Ignore.errcode(), None);
    }
}
