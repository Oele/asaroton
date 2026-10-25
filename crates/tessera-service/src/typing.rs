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
#[derive(Debug, Default)]
pub struct TypingState {
    rooms: DashMap<OwnedRoomId, RoomTyping>,
    /// Monotonic counter bumped on every change; sync tokens carry it.
    position: AtomicI64,
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
    pub fn changed_since(&self, room_id: &RoomId, since: i64) -> bool {
        self.rooms
            .get(room_id)
            .is_some_and(|r| r.last_change > since)
    }
}

impl Services {
    /// Client request to start/stop typing.
    pub fn set_typing(&self, room_id: &RoomId, user_id: &UserId, typing: bool, timeout: Duration) {
        if self.typing.set(
            room_id,
            user_id,
            typing,
            timeout.min(Duration::from_secs(120)),
        ) {
            self.notifier.notify(Notification::RoomEphemeral {
                room_id: room_id.to_owned(),
            });
        }
    }
}
