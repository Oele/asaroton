//! Presence, kept in memory. Federation of presence comes later.

use std::sync::atomic::{AtomicI64, Ordering};

use dashmap::DashMap;
use ruma::{OwnedUserId, UserId, presence::PresenceState};
use serde_json::{Value, json};
use tessera_store::now_ts;

use crate::{Notification, Services};

/// A user's presence.
#[derive(Clone, Debug)]
pub struct PresenceEntry {
    pub state: PresenceState,
    pub status_msg: Option<String>,
    pub last_active_ts: i64,
    /// Position of the last change, for incremental sync.
    pub stream: i64,
}

/// All known presence.
#[derive(Debug)]
pub struct PresenceMap {
    users: DashMap<OwnedUserId, PresenceEntry>,
    position: AtomicI64,
}

impl Default for PresenceMap {
    /// Positions start at the current time in milliseconds, so after a restart they are above
    /// anything a client's sync token still carries from the previous process (which bumped by
    /// one per change, far slower than the clock): the client then sees presence again.
    fn default() -> Self {
        Self {
            users: DashMap::new(),
            position: AtomicI64::new(now_ts()),
        }
    }
}

impl PresenceMap {
    pub fn position(&self) -> i64 {
        self.position.load(Ordering::SeqCst)
    }

    fn bump(&self) -> i64 {
        self.position.fetch_add(1, Ordering::SeqCst) + 1
    }

    pub fn get(&self, user: &UserId) -> Option<PresenceEntry> {
        self.users.get(user).map(|e| e.clone())
    }

    /// Set state (and optionally status message). Returns true if anything changed.
    pub fn set(
        &self,
        user: &UserId,
        state: PresenceState,
        status_msg: Option<Option<String>>,
    ) -> bool {
        let mut entry = self
            .users
            .entry(user.to_owned())
            .or_insert_with(|| PresenceEntry {
                state: PresenceState::Offline,
                status_msg: None,
                last_active_ts: now_ts(),
                stream: 0,
            });
        let new_msg = status_msg.unwrap_or_else(|| entry.status_msg.clone());
        let changed = entry.state != state || entry.status_msg != new_msg || entry.stream == 0;
        entry.state = state;
        entry.status_msg = new_msg;
        if changed {
            entry.last_active_ts = now_ts();
            entry.stream = self.bump();
        }
        changed
    }

    /// Mark activity (a sync) without necessarily changing state.
    /// Users whose presence changed after `since`.
    pub fn changed_since(&self, since: i64) -> Vec<(OwnedUserId, PresenceEntry)> {
        self.users
            .iter()
            .filter(|e| e.stream > since)
            .map(|e| (e.key().clone(), e.value().clone()))
            .collect()
    }

    pub fn touch(&self, user: &UserId) {
        if let Some(mut e) = self.users.get_mut(user) {
            e.last_active_ts = now_ts();
        }
    }
}

/// The `m.presence` event for a user.
pub fn presence_event(user: &UserId, e: &PresenceEntry) -> Value {
    let now = now_ts();
    let mut content = json!({
        "presence": e.state,
        "last_active_ago": (now - e.last_active_ts).max(0),
        "currently_active": e.state == PresenceState::Online,
    });
    if let Some(m) = &e.status_msg {
        content["status_msg"] = json!(m);
    }
    json!({ "type": "m.presence", "sender": user, "content": content })
}

impl Services {
    /// Client-initiated presence change.
    pub async fn set_presence(
        &self,
        user: &UserId,
        state: PresenceState,
        status_msg: Option<Option<String>>,
    ) {
        if self.presence.set(user, state.clone(), status_msg) {
            self.notifier.notify(Notification::Presence {
                user_id: user.to_owned(),
            });
            if self.is_local(user)
                && let Some(entry) = self.presence.get(user)
                && let Ok(servers) = self.remote_servers_sharing_rooms_with(user).await
            {
                let now = now_ts();
                let mut update = json!({
                    "user_id": user,
                    "presence": entry.state,
                    "last_active_ago": (now - entry.last_active_ts).max(0),
                    "currently_active": entry.state == PresenceState::Online,
                });
                if let Some(m) = &entry.status_msg {
                    update["status_msg"] = json!(m);
                }
                self.send_edu_to(
                    servers,
                    json!({ "edu_type": "m.presence", "content": { "push": [update] } }),
                );
            }
        }
    }
}
