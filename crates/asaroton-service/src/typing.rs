//! Typing notifications, kept in memory (they are ephemeral by definition).

use std::{
    collections::HashMap,
    sync::atomic::{AtomicI64, Ordering},
    time::{Duration, Instant},
};

use dashmap::DashMap;
use ruma::{OwnedRoomId, OwnedUserId, RoomId, UserId};

use crate::{Notification, Services};

/// Typing state of every room with at least one typing user.
#[derive(Debug)]
pub struct TypingState {
    rooms: DashMap<OwnedRoomId, RoomTyping>,
    /// Monotonic counter bumped on every change; sync tokens carry it.
    position: AtomicI64,
}

impl Default for TypingState {
    /// Positions start at the current time in milliseconds, so after a restart they are above
    /// anything a client's sync token still carries from the previous process.
    fn default() -> Self {
        Self {
            rooms: DashMap::new(),
            position: AtomicI64::new(asaroton_store::now_ts()),
        }
    }
}

#[derive(Debug, Default)]
struct RoomTyping {
    users: HashMap<OwnedUserId, Instant>,
    /// Position of the last change in this room.
    last_change: i64,
}

impl TypingState {
    pub fn position(&self) -> i64 {
        self.position.load(Ordering::SeqCst)
    }

    fn bump(&self) -> i64 {
        self.position.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// Set or clear typing for a user. Returns whether anything changed.
    pub fn set(&self, room_id: &RoomId, user_id: &UserId, typing: bool, timeout: Duration) -> bool {
        let mut room = self.rooms.entry(room_id.to_owned()).or_default();
        let changed = if typing {
            let expiry = Instant::now() + timeout;
            room.users.insert(user_id.to_owned(), expiry);
            true
        } else {
            room.users.remove(user_id).is_some()
        };
        if changed {
            room.last_change = self.bump();
        }
        changed
    }

    /// Drop every typing entry whose time is up at `now` and return who stopped where. The
    /// worker calls this, so that a client that never says it stopped (the app went to the
    /// background, the connection dropped) still stops typing for everyone else.
    pub fn expire(&self, now: Instant) -> Vec<(OwnedRoomId, OwnedUserId)> {
        let mut stopped = Vec::new();
        for mut room in self.rooms.iter_mut() {
            let before = stopped.len();
            let room_id = room.key().clone();
            room.users.retain(|user, expiry| {
                let typing = *expiry > now;
                if !typing {
                    stopped.push((room_id.clone(), user.clone()));
                }
                typing
            });
            if stopped.len() != before {
                room.last_change = self.bump();
            }
        }
        stopped
    }

    /// Users currently typing in a room (expired entries pruned).
    pub fn typing_users(&self, room_id: &RoomId) -> Vec<OwnedUserId> {
        let Some(mut room) = self.rooms.get_mut(room_id) else {
            return Vec::new();
        };
        let now = Instant::now();
        let before = room.users.len();
        room.users.retain(|_, exp| *exp > now);
        if room.users.len() != before {
            room.last_change = self.bump();
        }
        room.users.keys().cloned().collect()
    }

    /// Whether the room's typing set changed after `since`.
    /// Rooms whose typing set changed after `since`.
    pub fn rooms_changed_since(&self, since: i64) -> Vec<OwnedRoomId> {
        self.rooms
            .iter()
            .filter(|r| r.last_change > since)
            .map(|r| r.key().clone())
            .collect()
    }

    pub fn changed_since(&self, room_id: &RoomId, since: i64) -> bool {
        self.rooms
            .get(room_id)
            .is_some_and(|r| r.last_change > since)
    }
}

impl Services {
    /// Client request to start/stop typing.
    pub async fn set_typing(
        &self,
        room_id: &RoomId,
        user_id: &UserId,
        typing: bool,
        timeout: Duration,
    ) {
        if self.typing.set(
            room_id,
            user_id,
            typing,
            timeout.min(Duration::from_secs(120)),
        ) {
            self.notifier.notify(Notification::RoomEphemeral {
                room_id: room_id.to_owned(),
            });
            if self.is_local(user_id) {
                self.send_typing(room_id, user_id, typing).await;
            }
        }
    }

    /// Tell the other servers in the room. Queued before returning, not in a task of its
    /// own: "typing" and "stopped" often follow each other within milliseconds and must
    /// reach the queue in that order.
    async fn send_typing(&self, room_id: &RoomId, user_id: &UserId, typing: bool) {
        if let Ok(servers) = self.remote_servers_in_room(room_id).await
            && !servers.is_empty()
        {
            self.federation()
                .send_edu(
                    servers,
                    serde_json::json!({
                        "edu_type": "m.typing",
                        "content": { "room_id": room_id, "user_id": user_id, "typing": typing },
                    }),
                )
                .await;
        }
    }
}

/// Starts the worker that ends typing notices whose time is up. Local clients see the user
/// stop, and for our own users the other servers are told: over federation a typing notice
/// carries no timeout, so a server that is never told keeps showing the user as typing for as
/// long as it likes.
pub fn start_worker(services: std::sync::Arc<Services>) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let stopped = services.typing.expire(Instant::now());
            let mut rooms: Vec<&OwnedRoomId> = stopped.iter().map(|(room, _)| room).collect();
            rooms.sort_unstable();
            rooms.dedup();
            for room_id in rooms {
                services.notifier.notify(Notification::RoomEphemeral {
                    room_id: room_id.clone(),
                });
            }
            for (room_id, user_id) in &stopped {
                if services.is_local(user_id) {
                    services.send_typing(room_id, user_id, false).await;
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typing_ends_when_its_time_is_up() {
        let state = TypingState::default();
        let room: OwnedRoomId = "!a:example.org"
            .try_into()
            .unwrap_or_else(|e| panic!("{e}"));
        let other: OwnedRoomId = "!b:example.org"
            .try_into()
            .unwrap_or_else(|e| panic!("{e}"));
        let alice: OwnedUserId = "@alice:example.org"
            .try_into()
            .unwrap_or_else(|e| panic!("{e}"));
        let bob: OwnedUserId = "@bob:example.org"
            .try_into()
            .unwrap_or_else(|e| panic!("{e}"));
        let start = Instant::now();
        state.set(&room, &alice, true, Duration::from_secs(5));
        state.set(&room, &bob, true, Duration::from_secs(30));
        state.set(&other, &alice, true, Duration::from_secs(30));
        let position = state.position();

        // Nothing is due yet, and nothing is reported as changed.
        assert_eq!(state.expire(start + Duration::from_secs(1)), vec![]);
        assert_eq!(state.position(), position);

        // Alice's five seconds in the first room are over; the rest stays.
        let stopped = state.expire(start + Duration::from_secs(10));
        assert_eq!(stopped, vec![(room.clone(), alice.clone())]);
        assert!(state.changed_since(&room, position));
        assert!(!state.changed_since(&other, position));
        assert_eq!(state.typing_users(&room), vec![bob.clone()]);
        assert_eq!(state.typing_users(&other), vec![alice.clone()]);

        // Reported once, and a client that stops by itself is not reported at all.
        assert_eq!(state.expire(start + Duration::from_secs(11)), vec![]);
        assert!(state.set(&room, &bob, false, Duration::ZERO));
        let mut rest = state.expire(start + Duration::from_secs(60));
        rest.sort();
        assert_eq!(rest, vec![(other, alice)]);
    }
}
