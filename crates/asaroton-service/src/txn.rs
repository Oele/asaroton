//! One request at a time for each transaction ID.
//!
//! A client that got no answer to a send tries again with the same transaction ID, and must
//! get the event of its first attempt instead of a second one. The first attempt notes its
//! answer when it is done, and it can still be running when the second comes in: a request
//! that writes is seen through to the end whatever becomes of its connection. Both would
//! then find the ID unanswered, and the message would be sent twice.
//!
//! So a request holds the turn of its transaction ID from before it looks whether the ID was
//! answered until after it has noted its own answer. A second request with the same ID
//! waits, and then finds the answer.
//!
//! The same goes for what is recognised as a repetition without a transaction ID: a state
//! event with the content the room already has, a server notice for a user who has no room
//! for notices yet.

use std::sync::Arc;

use ruma::{DeviceId, ServerName, UserId};
use tokio::sync::{Mutex, OwnedMutexGuard};

use crate::Services;

/// The transaction IDs that requests are answering right now.
#[derive(Default)]
pub(crate) struct Turns(dashmap::DashMap<String, Arc<Mutex<()>>>);

/// The turn of one transaction ID, for as long as this lives.
pub struct TxnTurn<'a> {
    turns: &'a Turns,
    key: String,
    held: Option<OwnedMutexGuard<()>>,
}

// A key can hold a refresh token: neither of these shows its keys.
impl std::fmt::Debug for Turns {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Turns")
            .field("held", &self.0.len())
            .finish()
    }
}

impl std::fmt::Debug for TxnTurn<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TxnTurn").finish_non_exhaustive()
    }
}

impl Turns {
    async fn take(&self, key: String) -> TxnTurn<'_> {
        let lock = Arc::clone(&*self.0.entry(key.clone()).or_default());
        // Made before the wait, so that a request dropped while it waits tidies up as well.
        let mut turn = TxnTurn {
            turns: self,
            key,
            held: None,
        };
        turn.held = Some(lock.lock_owned().await);
        turn
    }
}

impl Drop for TxnTurn<'_> {
    fn drop(&mut self) {
        // Let go first, then forget the ID, unless someone else has it or is waiting for it.
        self.held = None;
        self.turns
            .0
            .remove_if(&self.key, |_, lock| Arc::strong_count(lock) == 1);
    }
}

impl Services {
    /// Waits for the turn of a transaction ID of this user's device. `txn_key` is the ID with
    /// the endpoint it was used on, as it is stored with its answer.
    pub async fn txn_turn(
        &self,
        user_id: &UserId,
        device_id: Option<&DeviceId>,
        txn_key: &str,
    ) -> TxnTurn<'_> {
        self.txn_turns
            .take(format!(
                "{user_id}\n{}\n{txn_key}",
                device_id.map_or("", DeviceId::as_str)
            ))
            .await
    }

    /// Waits for the turn of a transaction that another server sends (`PUT /send/{txnId}`).
    /// A server that got no answer sends the transaction again under the same ID, and what
    /// is in it (a to-device message, say) must not be taken in twice.
    pub async fn federation_txn_turn(&self, origin: &ServerName, txn_id: &str) -> TxnTurn<'_> {
        // No user ID begins with a line break: these cannot meet the keys above.
        self.txn_turns
            .take(format!("\nfederation\n{origin}\n{txn_id}"))
            .await
    }

    /// Waits for the turn of a refresh token: a client that asks again with the same one
    /// waits for the first request, and gets the same answer.
    pub(crate) async fn refresh_turn(&self, refresh_token: &str) -> TxnTurn<'_> {
        self.txn_turns
            .take(format!("\nrefresh\n{refresh_token}"))
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::Turns;

    #[tokio::test]
    async fn a_second_request_with_the_same_id_waits_for_the_first() {
        let turns = Turns::default();
        let first = turns.take("a".into()).await;
        let mut second = Box::pin(turns.take("a".into()));
        assert!(futures::poll!(second.as_mut()).is_pending());
        // Another ID has nothing to wait for.
        drop(turns.take("b".into()).await);

        drop(first);
        let second = second.await;
        assert_eq!(turns.0.len(), 1);
        drop(second);
        assert_eq!(
            turns.0.len(),
            0,
            "an ID that nobody is answering is forgotten"
        );
    }

    #[tokio::test]
    async fn a_request_dropped_while_it_waits_leaves_nothing_behind() {
        let turns = Turns::default();
        // Dropped while the first still has the turn.
        let first = turns.take("a".into()).await;
        {
            let mut second = Box::pin(turns.take("a".into()));
            assert!(futures::poll!(second.as_mut()).is_pending());
        }
        assert_eq!(turns.0.len(), 1, "the first still has it");
        drop(first);
        assert_eq!(turns.0.len(), 0);

        // Dropped after the first let go, before it was its turn.
        let first = turns.take("a".into()).await;
        let mut second = Box::pin(turns.take("a".into()));
        assert!(futures::poll!(second.as_mut()).is_pending());
        drop(first);
        drop(second);
        assert_eq!(turns.0.len(), 0);
        // And the ID is free again.
        drop(turns.take("a".into()).await);
    }
}
