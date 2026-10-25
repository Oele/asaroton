//! Power-level helpers over a room's current state.

use std::collections::HashMap;

use ruma::{
    Int, UserId,
    events::{StateEventType, TimelineEventType},
};
use tessera_core::Pdu;

/// A snapshot of the room state needed for permission decisions.
pub type StatePdus = HashMap<(StateEventType, String), Pdu>;

fn pl_content(state: &StatePdus) -> Option<serde_json::Value> {
    let pdu = state.get(&(StateEventType::RoomPowerLevels, String::new()))?;
    serde_json::from_str(pdu.content().get()).ok()
}

fn as_i64(v: Option<&serde_json::Value>, default: i64) -> i64 {
    match v {
        Some(serde_json::Value::Number(n)) => n.as_i64().unwrap_or(default),
        Some(serde_json::Value::String(s)) => s.parse().unwrap_or(default),
        _ => default,
    }
}

/// Creators of the room (room versions 12+ give them infinite power).
pub fn creators(
    state: &StatePdus,
    rules: &ruma::room_version_rules::RoomVersionRules,
) -> Vec<ruma::OwnedUserId> {
    let Some(create) = state.get(&(StateEventType::RoomCreate, String::new())) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    if rules.authorization.use_room_create_sender {
        out.push(create.sender().to_owned());
    } else if let Ok(c) = serde_json::from_str::<serde_json::Value>(create.content().get())
        && let Some(creator) = c.get("creator").and_then(|v| v.as_str())
        && let Ok(uid) = ruma::OwnedUserId::try_from(creator)
    {
        out.push(uid);
    }
    if rules.authorization.additional_room_creators
        && let Ok(c) = serde_json::from_str::<serde_json::Value>(create.content().get())
        && let Some(extra) = c.get("additional_creators").and_then(|v| v.as_array())
    {
        for v in extra {
            if let Some(s) = v.as_str()
                && let Ok(uid) = ruma::OwnedUserId::try_from(s)
            {
                out.push(uid);
            }
        }
    }
    out
}

/// The power level of a user. Creators in v12+ rooms are reported as `i64::MAX`.
pub fn user_power_level(
    state: &StatePdus,
    user: &UserId,
    rules: &ruma::room_version_rules::RoomVersionRules,
) -> i64 {
    if rules.authorization.explicitly_privilege_room_creators
        && creators(state, rules).iter().any(|c| c == user)
    {
        return i64::MAX;
    }
    match pl_content(state) {
        Some(pl) => {
            let users = pl.get("users");
            let explicit = users.and_then(|u| u.get(user.as_str()));
            if explicit.is_some() {
                as_i64(explicit, 0)
            } else {
                as_i64(pl.get("users_default"), 0)
            }
        }
        None => {
            // No power levels event: the creator has 100, everyone else 0.
            if creators(state, rules).iter().any(|c| c == user) {
                100
            } else {
                0
            }
        }
    }
}

/// The level required for a named action (`invite`, `kick`, `ban`, `redact`).
pub fn action_level(state: &StatePdus, action: &str) -> i64 {
    let default = match action {
        "invite" => 0,
        _ => 50,
    };
    pl_content(state).map_or(default, |pl| as_i64(pl.get(action), default))
}

/// The level required to send an event type.
pub fn event_level(state: &StatePdus, event_type: &TimelineEventType, is_state: bool) -> i64 {
    let Some(pl) = pl_content(state) else {
        return if is_state { 50 } else { 0 };
    };
    if let Some(v) = pl.get("events").and_then(|e| e.get(event_type.to_string())) {
        return as_i64(Some(v), 0);
    }
    if is_state {
        as_i64(pl.get("state_default"), 50)
    } else {
        as_i64(pl.get("events_default"), 0)
    }
}

/// Whether `user` may perform `action` (invite/kick/ban/redact).
pub fn can(
    state: &StatePdus,
    user: &UserId,
    action: &str,
    rules: &ruma::room_version_rules::RoomVersionRules,
) -> bool {
    user_power_level(state, user, rules) >= action_level(state, action)
}

/// `Int` helper for building power level content.
pub fn int(v: i64) -> Int {
    Int::new(v).unwrap_or_default()
}
