//! Wake-ups for long-polling `/sync` and other listeners.
//!
//! A single broadcast channel carries small notifications; waiters filter for the rooms and
//! users they care about. This is simple and correct; sharding per user can come later.

use std::sync::Arc;

use ruma::{OwnedRoomId, OwnedUserId};
use tokio::sync::broadcast;

/// Something happened that a sync loop may care about.
#[derive(Clone, Debug)]
pub enum Notification {
    /// A new timeline/state event in a room (stream ordering attached).
    RoomEvent {
        room_id: OwnedRoomId,
        stream_ordering: i64,
    },
    /// Ephemeral or per-user data changed for a user (account data, to-device, presence...).
    User { user_id: OwnedUserId },
    /// Ephemeral room data changed (typing, receipts).
    RoomEphemeral { room_id: OwnedRoomId },
    /// A user's device list changed; everyone sharing a room may care.
    DeviceLists { user_id: OwnedUserId },
    /// A user's presence changed.
    Presence { user_id: OwnedUserId },
    /// A field of a user's profile changed.
    Profile { user_id: OwnedUserId },
}

/// Broadcast hub.
#[derive(Clone, Debug)]
pub struct Notifier {
    tx: broadcast::Sender<Arc<Notification>>,
}

impl Default for Notifier {
    fn default() -> Self {
        Self::new()
    }
}

impl Notifier {
    pub fn new() -> Self {
        let (tx, _rx) = broadcast::channel(4096);
        Self { tx }
    }

    /// Publish; dropped silently when nobody listens.
    pub fn notify(&self, n: Notification) {
        let _ = self.tx.send(Arc::new(n));
    }

    /// Subscribe. Must be called *before* checking the store for changes so no wake-up is
    /// missed between the check and the wait.
    pub fn subscribe(&self) -> broadcast::Receiver<Arc<Notification>> {
        self.tx.subscribe()
    }
}

/// Wait until `interested` matches a notification or `timeout` elapses. Lagged receivers are
/// treated as "something happened".
pub async fn wait_for(
    rx: &mut broadcast::Receiver<Arc<Notification>>,
    timeout: std::time::Duration,
    mut interested: impl FnMut(&Notification) -> bool,
) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        match tokio::time::timeout_at(deadline, rx.recv()).await {
            Ok(Ok(n)) => {
                if interested(&n) {
                    return true;
                }
            }
            Ok(Err(broadcast::error::RecvError::Lagged(_))) => return true,
            Ok(Err(broadcast::error::RecvError::Closed)) | Err(_) => return false,
        }
    }
}
