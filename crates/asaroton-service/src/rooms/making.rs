//! Rooms that `createRoom` is still writing.
//!
//! `createRoom` writes a room as a row of events: the create event, the creator's membership,
//! power levels, what the preset says, `initial_state`, name and topic. A sync of the creator
//! that is answered between two of them would show a room without, say, its encryption event.
//! Clients that take their picture of a room from the first answer that mentions it
//! (matrix-rust-sdk does) then treat the room as unencrypted until a later answer says
//! otherwise, and a message sent in between leaves the client in plain text.
//!
//! So a sync shows a new room whole or not yet: it leaves out the rooms its user is still
//! writing, and is woken when one is done. Until then the creator is the only one the room
//! exists for, so nobody else's sync is concerned. The invitations `createRoom` sends come
//! after, as events of a room that exists.
//!
//! A sync has to ask for the rooms under way *after* it has read its user's memberships. A
//! room it then finds the user joined to was begun before that read; if it is not under way
//! any more, all of it has been written, and whatever the sync reads of the room's state from
//! here on is whole.

use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::{DashMap, mapref::entry::Entry};
use ruma::{OwnedRoomId, OwnedUserId, RoomId, UserId};

use crate::notifier::{Notification, Notifier};

/// A room that is being written.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Unfinished {
    pub room_id: OwnedRoomId,
    /// Where the room's first event sits in the event stream, once that event is stored.
    /// Known by the time the creator's membership can be seen, since that is the second.
    pub first_event: Option<i64>,
}

#[derive(Debug)]
struct Making {
    /// Tells apart two attempts at the same room.
    id: u64,
    room: Unfinished,
}

/// The rooms being created right now, per creator.
#[derive(Debug, Default)]
pub struct RoomsInTheMaking {
    creators: DashMap<OwnedUserId, Vec<Making>>,
    next_id: AtomicU64,
}

/// Marks a room as being written until it is dropped, and then wakes its creator's syncs.
#[derive(Debug)]
pub(crate) struct InTheMaking<'a> {
    rooms: &'a RoomsInTheMaking,
    notifier: &'a Notifier,
    creator: OwnedUserId,
    id: u64,
}

impl InTheMaking<'_> {
    /// Notes where the room's first event went in the event stream.
    pub(crate) fn first_event_at(&self, stream_ordering: i64) {
        if let Some(mut makings) = self.rooms.creators.get_mut(&self.creator) {
            for m in makings.iter_mut().filter(|m| m.id == self.id) {
                m.room.first_event = Some(stream_ordering);
            }
        }
    }
}

impl Drop for InTheMaking<'_> {
    fn drop(&mut self) {
        if let Entry::Occupied(mut entry) = self.rooms.creators.entry(self.creator.clone()) {
            entry.get_mut().retain(|m| m.id != self.id);
            if entry.get().is_empty() {
                entry.remove();
            }
        }
        // The creator's waiting syncs were woken by the membership event and left the room
        // out. Nothing else would tell them that it is there now.
        self.notifier.notify(Notification::User {
            user_id: self.creator.clone(),
        });
    }
}

impl RoomsInTheMaking {
    /// Marks `room_id` as being written by `creator` until the returned value is dropped.
    /// To be called before the room's first event is stored.
    pub(crate) fn begin<'a>(
        &'a self,
        notifier: &'a Notifier,
        creator: &UserId,
        room_id: &RoomId,
    ) -> InTheMaking<'a> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.creators
            .entry(creator.to_owned())
            .or_default()
            .push(Making {
                id,
                room: Unfinished {
                    room_id: room_id.to_owned(),
                    first_event: None,
                },
            });
        InTheMaking {
            rooms: self,
            notifier,
            creator: creator.to_owned(),
            id,
        }
    }

    /// The rooms `creator` is writing right now. See the module documentation for when a
    /// sync has to ask.
    pub(crate) fn under_way(&self, creator: &UserId) -> Vec<Unfinished> {
        self.creators.get(creator).map_or_else(Vec::new, |makings| {
            makings.iter().map(|m| m.room.clone()).collect()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::notifier::wait_for;

    fn ids() -> (OwnedUserId, OwnedRoomId, OwnedRoomId) {
        (
            OwnedUserId::try_from("@a:x.test").unwrap(),
            OwnedRoomId::try_from("!one:x.test").unwrap(),
            OwnedRoomId::try_from("!two:x.test").unwrap(),
        )
    }

    fn under_way(rooms: &RoomsInTheMaking, user: &UserId) -> Vec<(OwnedRoomId, Option<i64>)> {
        rooms
            .under_way(user)
            .into_iter()
            .map(|u| (u.room_id, u.first_event))
            .collect()
    }

    #[test]
    fn a_room_is_under_way_until_its_mark_is_dropped() {
        let (user, one, two) = ids();
        let other = OwnedUserId::try_from("@b:x.test").unwrap();
        let (rooms, notifier) = (RoomsInTheMaking::default(), Notifier::new());
        assert_eq!(under_way(&rooms, &user), vec![]);

        let first = rooms.begin(&notifier, &user, &one);
        assert_eq!(under_way(&rooms, &user), vec![(one.clone(), None)]);
        first.first_event_at(41);
        let second = rooms.begin(&notifier, &user, &two);
        assert_eq!(
            under_way(&rooms, &user),
            vec![(one.clone(), Some(41)), (two.clone(), None)]
        );
        assert_eq!(under_way(&rooms, &other), vec![]);

        drop(first);
        assert_eq!(under_way(&rooms, &user), vec![(two, None)]);
        drop(second);
        assert_eq!(under_way(&rooms, &user), vec![]);
        assert!(
            rooms.creators.is_empty(),
            "nothing is kept for a creator who is done"
        );
    }

    #[test]
    fn a_second_attempt_at_the_same_room_does_not_end_the_first() {
        let (user, one, _) = ids();
        let (rooms, notifier) = (RoomsInTheMaking::default(), Notifier::new());
        let first = rooms.begin(&notifier, &user, &one);
        first.first_event_at(7);
        let second = rooms.begin(&notifier, &user, &one);
        drop(second);
        assert_eq!(under_way(&rooms, &user), vec![(one, Some(7))]);
        drop(first);
        assert_eq!(under_way(&rooms, &user), vec![]);
    }

    #[tokio::test]
    async fn the_creator_is_woken_when_a_room_is_done() {
        let (user, one, _) = ids();
        let (rooms, notifier) = (RoomsInTheMaking::default(), Notifier::new());
        let mut rx = notifier.subscribe();
        let making = rooms.begin(&notifier, &user, &one);
        let for_user =
            |n: &Notification| matches!(n, Notification::User { user_id } if *user_id == user);
        let short = std::time::Duration::from_millis(50);
        assert!(
            !wait_for(&mut rx, short, for_user).await,
            "not while it is written"
        );
        drop(making);
        assert!(wait_for(&mut rx, short, for_user).await, "when it is done");
    }
}
