//! The text a room directory search matches on: name, topic and canonical alias of every public
//! room, kept in memory. Each search first brings it up to date from what changed since the
//! previous one (rooms with new state events or redactions, rooms that became public or stopped
//! being public), so a search among thousands of public rooms reads only the changes.

use std::collections::{HashMap, HashSet};

use asaroton_store::PublicRoomText;
use ruma::OwnedRoomId;
use tokio::sync::Mutex;

use crate::{Result, Services};

/// The cached text, behind a lock that searches take in turn: each one is short.
#[derive(Debug, Default)]
pub struct DirectoryText {
    snapshot: Mutex<Option<Snapshot>>,
}

#[derive(Debug)]
struct Snapshot {
    /// The event stream position the text is up to date with.
    upto: i64,
    /// Every public room, in directory order.
    rooms: Vec<PublicRoomText>,
}

impl Services {
    /// The public rooms whose name, topic or canonical alias contains `term` (in lower case),
    /// in directory order.
    pub async fn public_rooms_matching(&self, term: &str) -> Result<Vec<OwnedRoomId>> {
        let mut guard = self.directory_text.snapshot.lock().await;
        let now = self.store.stream_positions().await?.events;
        let snapshot = match guard.take() {
            None => Snapshot {
                upto: now,
                rooms: self.store.public_rooms_text(None).await?,
            },
            Some(old) => self.refresh_directory_text(old, now).await?,
        };
        let matching = snapshot
            .rooms
            .iter()
            .filter(|t| text_matches(t, term))
            .map(|t| t.room_id.clone())
            .collect();
        *guard = Some(snapshot);
        Ok(matching)
    }

    async fn refresh_directory_text(&self, old: Snapshot, now: i64) -> Result<Snapshot> {
        let changed: HashSet<OwnedRoomId> = if now > old.upto {
            self.store
                .rooms_with_state_changes(old.upto, now)
                .await?
                .into_iter()
                .collect()
        } else {
            HashSet::new()
        };
        // Visibility is no event: the list of public rooms is asked every time.
        let public = self.store.list_public_rooms(i64::MAX, 0).await?;
        let mut cached: HashMap<OwnedRoomId, PublicRoomText> = old
            .rooms
            .into_iter()
            .map(|t| (t.room_id.clone(), t))
            .collect();
        let reload: Vec<OwnedRoomId> = public
            .iter()
            .map(|r| &r.room_id)
            .filter(|id| changed.contains(*id) || !cached.contains_key(*id))
            .cloned()
            .collect();
        if !reload.is_empty() {
            for text in self.store.public_rooms_text(Some(&reload)).await? {
                cached.insert(text.room_id.clone(), text);
            }
        }
        let rooms = public
            .into_iter()
            .filter_map(|r| cached.remove(&r.room_id))
            .collect();
        Ok(Snapshot { upto: now, rooms })
    }
}

/// Whether a public room's name, topic or canonical alias contains `term` (lower case).
fn text_matches(t: &PublicRoomText, term: &str) -> bool {
    let has = |s: Option<&str>| s.is_some_and(|s| s.to_lowercase().contains(term));
    has(t.name.as_deref()) || has(t.topic.as_deref()) || has(t.alias.as_deref())
}
