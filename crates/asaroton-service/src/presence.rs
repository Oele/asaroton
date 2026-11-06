//! Presence, kept in memory.
//!
//! A user's presence is the strongest state any of their recently seen devices asked for
//! (online over unavailable over offline), so two clients that disagree (a phone syncing as
//! online, a browser that went idle and syncs as unavailable) do not flip the user back and
//! forth. Before this aggregation every sync could flip the state, each flip woke the other
//! client's long poll, which flipped it back, and every flip was pushed to every server sharing
//! a room with the user: a feedback loop that flooded the federation. Devices that stop syncing
//! drop out after [`DEVICE_TTL_MS`], and pushes to other servers are limited per user to a
//! few at once and then one per [`FEDERATION_MIN_INTERVAL_MS`], the latest state winning.
#![allow(clippy::option_option)] // `Some(None)` clears the status message, `None` keeps it

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
    time::Duration,
};

use asaroton_store::now_ts;
use dashmap::DashMap;
use ruma::{DeviceId, OwnedUserId, UserId, presence::PresenceState};
use serde_json::{Value, json};

use crate::{Notification, Services};

/// A device that has not synced for this long no longer counts towards its user's presence.
pub const DEVICE_TTL_MS: i64 = 5 * 60 * 1000;
/// Long-run spacing between presence pushes to other servers for one user.
pub const FEDERATION_MIN_INTERVAL_MS: i64 = 15_000;
/// Pushes a user may make at once before the spacing applies. Ordinary use changes state a
/// couple of times in a row (coming online, then going idle); only something that keeps
/// flipping runs into the limit, and that is then held to the long-run rate.
pub const FEDERATION_BURST: i64 = 3;

/// A user's allowance of presence pushes to other servers.
#[derive(Clone, Copy, Debug, Default)]
struct PushSlot {
    /// When the next push is due on the steady schedule (milliseconds).
    next: i64,
    /// A change is waiting for the allowance.
    pending: bool,
}

impl PushSlot {
    fn allowed(&self, now: i64) -> bool {
        self.next - now <= FEDERATION_MIN_INTERVAL_MS * (FEDERATION_BURST - 1)
    }

    fn take(&mut self, now: i64) {
        self.next = self.next.max(now) + FEDERATION_MIN_INTERVAL_MS;
        self.pending = false;
    }
}

/// A user's presence as clients and other servers see it.
#[derive(Clone, Debug)]
pub struct PresenceEntry {
    pub state: PresenceState,
    pub status_msg: Option<String>,
    pub last_active_ts: i64,
    /// Position of the last change, for incremental sync.
    pub stream: i64,
}

/// What one device of a local user last asked for.
#[derive(Clone, Debug)]
struct DeviceState {
    state: PresenceState,
    last_seen: i64,
}

/// All known presence.
#[derive(Debug)]
pub struct PresenceMap {
    users: DashMap<OwnedUserId, PresenceEntry>,
    /// Per local user, the state each device asked for; the key is the device ID, or an empty
    /// string for a request without one.
    devices: DashMap<OwnedUserId, HashMap<String, DeviceState>>,
    /// Each local user's allowance of pushes to other servers, and whether a change is
    /// waiting for it.
    federation: DashMap<OwnedUserId, PushSlot>,
    position: AtomicI64,
}

impl Default for PresenceMap {
    /// Positions start at the current time in milliseconds, so after a restart they are above
    /// anything a client's sync token still carries from the previous process (which bumped by
    /// one per change, far slower than the clock): the client then sees presence again.
    fn default() -> Self {
        Self {
            users: DashMap::new(),
            devices: DashMap::new(),
            federation: DashMap::new(),
            position: AtomicI64::new(now_ts()),
        }
    }
}

fn rank(state: &PresenceState) -> u8 {
    match state {
        PresenceState::Online => 3,
        PresenceState::Unavailable => 2,
        PresenceState::Offline => 0,
        _ => 1,
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

    /// Records what one device of a local user asked for and applies the resulting user state.
    /// Returns true if the user's presence changed.
    pub fn set_device(
        &self,
        user: &UserId,
        device: Option<&DeviceId>,
        state: PresenceState,
        status_msg: Option<Option<String>>,
    ) -> bool {
        let now = now_ts();
        let aggregate = {
            let mut devices = self.devices.entry(user.to_owned()).or_default();
            devices.insert(
                device.map(ToString::to_string).unwrap_or_default(),
                DeviceState {
                    state,
                    last_seen: now,
                },
            );
            devices.retain(|_, d| now - d.last_seen <= DEVICE_TTL_MS);
            strongest(&devices)
        };
        self.apply(user, aggregate, status_msg)
    }

    /// Presence of a user of another server, as that server reported it.
    pub fn set_remote(
        &self,
        user: &UserId,
        state: PresenceState,
        status_msg: Option<String>,
    ) -> bool {
        self.apply(user, state, Some(status_msg))
    }

    /// Sets the user's visible state; `status_msg` `None` keeps the current message. Returns
    /// true if anything changed.
    fn apply(
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

    /// Drops devices that stopped syncing and returns the users whose presence changed as a
    /// result (typically to offline).
    pub fn expire_devices(&self) -> Vec<OwnedUserId> {
        let now = now_ts();
        let mut changed = Vec::new();
        for mut devices in self.devices.iter_mut() {
            let before = devices.len();
            devices.retain(|_, d| now - d.last_seen <= DEVICE_TTL_MS);
            if devices.len() == before {
                continue;
            }
            let user = devices.key().clone();
            let state = strongest(&devices);
            drop(devices);
            if self.apply(&user, state, None) {
                changed.push(user);
            }
        }
        changed
    }

    /// Users whose presence changed after `since`.
    pub fn changed_since(&self, since: i64) -> Vec<(OwnedUserId, PresenceEntry)> {
        self.users
            .iter()
            .filter(|e| e.stream > since)
            .map(|e| (e.key().clone(), e.value().clone()))
            .collect()
    }

    /// Marks activity (a sync) without changing state.
    pub fn touch(&self, user: &UserId) {
        if let Some(mut e) = self.users.get_mut(user) {
            e.last_active_ts = now_ts();
        }
    }

    /// Whether a push to other servers may go out now (recorded as done if so); otherwise the
    /// push is marked pending for [`PresenceMap::due_pushes`].
    fn federation_slot(&self, user: &UserId) -> bool {
        self.federation_slot_at(user, now_ts())
    }

    fn federation_slot_at(&self, user: &UserId, now: i64) -> bool {
        let mut slot = self.federation.entry(user.to_owned()).or_default();
        if slot.allowed(now) {
            slot.take(now);
            true
        } else {
            slot.pending = true;
            false
        }
    }

    /// Users with a pending push that their allowance now covers; recorded as pushed now.
    fn due_pushes(&self) -> Vec<OwnedUserId> {
        self.due_pushes_at(now_ts())
    }

    fn due_pushes_at(&self, now: i64) -> Vec<OwnedUserId> {
        let mut due = Vec::new();
        for mut slot in self.federation.iter_mut() {
            if slot.pending && slot.allowed(now) {
                slot.take(now);
                due.push(slot.key().clone());
            }
        }
        due
    }
}

fn strongest(devices: &HashMap<String, DeviceState>) -> PresenceState {
    devices
        .values()
        .max_by_key(|d| rank(&d.state))
        .map_or(PresenceState::Offline, |d| d.state.clone())
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
    /// A local user's client asks for a presence state (explicitly, or through `set_presence`
    /// on a sync).
    pub async fn set_presence(
        &self,
        user: &UserId,
        device: Option<&DeviceId>,
        state: PresenceState,
        status_msg: Option<Option<String>>,
    ) {
        if self.presence.set_device(user, device, state, status_msg) {
            self.presence_changed(user).await;
        }
    }

    /// Another server reports one of its users' presence.
    pub fn set_remote_presence(
        &self,
        user: &UserId,
        state: PresenceState,
        status_msg: Option<String>,
    ) {
        if self.presence.set_remote(user, state, status_msg) {
            self.notifier.notify(Notification::Presence {
                user_id: user.to_owned(),
            });
        }
    }

    async fn presence_changed(&self, user: &UserId) {
        self.notifier.notify(Notification::Presence {
            user_id: user.to_owned(),
        });
        if !self.is_local(user) {
            return;
        }
        if self.presence.federation_slot(user) {
            self.push_presence(user).await;
        }
    }

    /// Sends the user's current presence to every server sharing a room with them.
    async fn push_presence(&self, user: &UserId) {
        let Some(entry) = self.presence.get(user) else {
            return;
        };
        let Ok(servers) = self.remote_servers_sharing_rooms_with(user).await else {
            return;
        };
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

/// Starts the worker that sends the presence pushes held back by the spacing interval and
/// takes users offline once none of their devices sync any more.
pub fn start_worker(services: Arc<Services>) {
    tokio::spawn(async move {
        let mut tick: u32 = 0;
        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;
            for user in services.presence.due_pushes() {
                services.push_presence(&user).await;
            }
            tick += 1;
            if tick.is_multiple_of(6) {
                for user in services.presence.expire_devices() {
                    services.presence_changed(&user).await;
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strongest_device_wins_and_disagreement_does_not_flip() {
        let map = PresenceMap::default();
        let user: OwnedUserId = "@a:example.org".try_into().unwrap();
        let phone: &DeviceId = "PHONE".into();
        let web: &DeviceId = "WEB".into();
        assert!(map.set_device(&user, Some(phone), PresenceState::Online, None));
        assert!(!map.set_device(&user, Some(web), PresenceState::Unavailable, None));
        assert!(!map.set_device(&user, Some(phone), PresenceState::Online, None));
        assert_eq!(map.get(&user).unwrap().state, PresenceState::Online);
        assert!(map.set_device(&user, Some(phone), PresenceState::Unavailable, None));
        assert_eq!(map.get(&user).unwrap().state, PresenceState::Unavailable);
        assert!(!map.set_device(&user, Some(web), PresenceState::Offline, None));
        assert!(map.set_device(&user, Some(phone), PresenceState::Offline, None));
        assert_eq!(map.get(&user).unwrap().state, PresenceState::Offline);
    }

    #[test]
    fn federation_pushes_come_in_a_small_burst_and_then_spaced() {
        let map = PresenceMap::default();
        let user: OwnedUserId = "@a:example.org".try_into().unwrap();
        let start = 1_793_000_000_000;
        // Coming online and going idle right after both go out at once.
        for _ in 0..FEDERATION_BURST {
            assert!(map.federation_slot_at(&user, start));
        }
        // The next change waits, and is delivered once, when the allowance covers it.
        assert!(!map.federation_slot_at(&user, start + 10));
        assert_eq!(map.due_pushes_at(start + 20), Vec::<OwnedUserId>::new());
        assert_eq!(
            map.due_pushes_at(start + FEDERATION_MIN_INTERVAL_MS),
            vec![user.clone()]
        );
        assert_eq!(
            map.due_pushes_at(start + FEDERATION_MIN_INTERVAL_MS + 1),
            Vec::<OwnedUserId>::new()
        );

        // Something that flips the state every 100 ms for ten minutes gets the burst and
        // then one push per interval, however fast it flips.
        let map = PresenceMap::default();
        let mut pushes = 0;
        let minutes = 10;
        for tick in 0..minutes * 600 {
            let now = start + tick * 100;
            if map.federation_slot_at(&user, now) {
                pushes += 1;
            }
            pushes += i64::try_from(map.due_pushes_at(now).len()).unwrap_or(0);
        }
        let steady = minutes * 60_000 / FEDERATION_MIN_INTERVAL_MS;
        assert!(
            pushes >= steady && pushes <= steady + FEDERATION_BURST,
            "{pushes} pushes"
        );

        // After a quiet spell the burst is available again, and no more than the burst.
        let later = start + 3_600_000;
        let map = PresenceMap::default();
        assert!(map.federation_slot_at(&user, start));
        for _ in 0..FEDERATION_BURST {
            assert!(map.federation_slot_at(&user, later));
        }
        assert!(!map.federation_slot_at(&user, later));
    }
}
