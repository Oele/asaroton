//! Events that could not be checked when they arrived are looked at again.
//!
//! An event is checked against the auth events it cites. When one of those cannot be obtained
//! (the server that should hand it over is down, or it is signed with a key nobody can
//! produce right now) the event cannot be checked. That says nothing about the event. It is
//! stored as rejected so that the room can go on (`Federation::set_aside_unchecked`) and
//! listed in `events_awaiting_auth`. The worker here goes through that list. Once the auth
//! events can be had, the event goes through the normal pipeline and gets its real verdict,
//! and so do the events that were rejected meanwhile only because they cite it.
//!
//! An event that is accepted this way gets a fresh place in the stream, so clients receive it
//! as a message that arrived late, which is what it is.

use std::{collections::HashSet, sync::Arc, time::Duration};

use asaroton_core::{Pdu, auth, room_version};
use asaroton_store::{AwaitingAuth, now_ts};
use ruma::{EventId, OwnedEventId, OwnedServerName, RoomId, room_version_rules::RoomVersionRules};

use crate::{
    Federation, Result,
    receive::{AUTH_UNAVAILABLE, Outcome, auth_event_ids},
};

/// How many listed events one round looks at.
const PER_ROUND: i64 = 20;
/// The wait before the first new try when the configuration does not set one.
const DEFAULT_FIRST_DELAY: Duration = Duration::from_mins(2);
/// The waits double up to this.
const MAX_DELAY: Duration = Duration::from_hours(6);
/// After this long an event is taken off the list and stays rejected.
const GIVE_UP_AFTER: Duration = Duration::from_hours(7 * 24);
/// How far a chain of rejected auth events is followed when judging one again.
const MAX_CHAIN: u32 = 8;
/// How many rejected events of a room are looked at again after one acceptance.
const MAX_CASCADE: usize = 500;

fn millis(d: Duration) -> i64 {
    i64::try_from(d.as_millis()).unwrap_or(i64::MAX)
}

/// The wait before try number `tries + 1`: the first delay, doubling, up to [`MAX_DELAY`].
fn delay_after(first: Duration, tries: i32) -> Duration {
    let doublings = u32::try_from(tries).unwrap_or(0).min(20);
    first.saturating_mul(1 << doublings).min(MAX_DELAY)
}

/// Start the worker.
pub(crate) fn start(fed: &Arc<Federation>) {
    let fed = Arc::clone(fed);
    // Often enough to honour a short first delay, and no more than once a minute.
    let round =
        (fed.first_recheck_delay() / 2).clamp(Duration::from_secs(1), Duration::from_mins(1));
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(round).await;
            if let Err(e) = fed.recheck_due().await {
                tracing::error!(error = %e, "looking again at the events that were set aside failed");
            }
        }
    });
}

impl Federation {
    fn first_recheck_delay(&self) -> Duration {
        match self.config.unchecked_event_retry_secs {
            0 => DEFAULT_FIRST_DELAY,
            secs => Duration::from_secs(u64::from(secs)),
        }
    }

    /// When an event that has been tried `tries` times is due again, counted from now.
    pub(crate) fn next_recheck_ts(&self, tries: i32) -> i64 {
        now_ts() + millis(delay_after(self.first_recheck_delay(), tries))
    }

    async fn recheck_due(&self) -> Result<()> {
        let due = self
            .services
            .store
            .events_awaiting_auth_due(now_ts(), PER_ROUND)
            .await?;
        for row in due {
            if let Err(e) = self.recheck_one(&row).await {
                tracing::warn!(event_id = %row.event_id, room_id = %row.room_id, error = %e, "looking again at an event that was set aside failed");
                self.wait_or_give_up(&row).await?;
            }
        }
        Ok(())
    }

    /// Put a listed event back for a later try, or take it off the list when its week is
    /// over. Returns whether it was given up.
    async fn wait_or_give_up(&self, row: &AwaitingAuth) -> Result<bool> {
        let store = &self.services.store;
        if now_ts() - row.first_ts > millis(GIVE_UP_AFTER) {
            store.forget_awaiting_auth(&row.event_id).await?;
            return Ok(true);
        }
        store
            .reschedule_awaiting_auth(&row.event_id, self.next_recheck_ts(row.tries + 1))
            .await?;
        Ok(false)
    }

    async fn recheck_one(&self, row: &AwaitingAuth) -> Result<()> {
        let services = &self.services;
        let store = &services.store;
        let room_id = &row.room_id;
        let (Some(stored), Some(room)) = (
            store.get_event(&row.event_id).await?,
            store.get_room(room_id).await?,
        ) else {
            // The room or the event has been purged.
            return Ok(store.forget_awaiting_auth(&row.event_id).await?);
        };
        if !self.we_are_in_room(room_id).await? {
            // Left, perhaps to join again in a moment (which is how a room is repaired by
            // hand). The event keeps its place on the list until its week is over.
            self.wait_or_give_up(row).await?;
            return Ok(());
        }
        let rules = room_version::rules(&room.room_version)?;
        if !stored.outlier {
            // It has been placed meanwhile: it came in once more, with a later event.
            store.forget_awaiting_auth(&row.event_id).await?;
            if !stored.pdu.rejected() {
                let _guard = services.lock_room(room_id).await;
                self.recheck_rejected(room_id, &rules, row.first_ts).await?;
            }
            return Ok(());
        }

        // Can the auth events be had now? The server that delivered the event is asked, then
        // the sender's own. This is done before taking the room's lock: a server that does
        // not answer must not hold up the room.
        let pdu = stored.pdu.with_rejected(false);
        let mut servers: Vec<OwnedServerName> = vec![row.origin.clone()];
        if !servers.iter().any(|s| s == pdu.sender().server_name()) {
            servers.push(pdu.sender().server_name().to_owned());
        }
        servers.retain(|s| s != self.server_name());
        // A new try is the point of this worker, so it does ask a server again that failed
        // to hand over an event some minutes ago. Only the listed events of one round share
        // a failure: they often wait for the same event.
        let miss_ttl = (self.first_recheck_delay() / 2).min(Duration::from_secs(30));
        let mut via: Option<&OwnedServerName> = None;
        let mut last_error = String::new();
        for server in &servers {
            match self
                .ensure_auth_events(server, room_id, &pdu, &rules, 0, miss_ttl)
                .await
            {
                Ok(()) => {
                    via = Some(server);
                    break;
                }
                Err(e) => last_error = e.to_string(),
            }
        }
        let Some(via) = via else {
            if self.wait_or_give_up(row).await? {
                tracing::warn!(
                    event_id = %row.event_id, %room_id, sender = %pdu.sender(), error = %last_error,
                    "giving up on an event that was set aside a week ago: its auth events can still not be obtained. It stays rejected"
                );
            } else {
                tracing::debug!(event_id = %row.event_id, %room_id, tries = row.tries + 1, error = %last_error, "an event that was set aside can still not be checked");
            }
            return Ok(());
        };

        let _guard = services.lock_room(room_id).await;
        if store
            .get_event(&row.event_id)
            .await?
            .is_some_and(|e| !e.outlier)
        {
            // Placed while we were asking around; the next round deals with what cites it.
            return Ok(());
        }
        let outcome = self
            .process_pdu_inner(via, pdu.clone(), &rules, false, false, None)
            .await?;
        match outcome {
            Outcome::Rejected(reason) if reason.starts_with(AUTH_UNAVAILABLE) => {
                self.wait_or_give_up(row).await?;
            }
            Outcome::Rejected(reason) => {
                store.forget_awaiting_auth(&row.event_id).await?;
                tracing::info!(event_id = %row.event_id, %room_id, sender = %pdu.sender(), %reason, "an event that had been set aside could be checked now, and is rejected");
            }
            Outcome::Accepted | Outcome::SoftFailed | Outcome::AlreadyKnown => {
                store.forget_awaiting_auth(&row.event_id).await?;
                tracing::info!(event_id = %row.event_id, %room_id, sender = %pdu.sender(), %via, "an event that had been set aside could be checked now, and is accepted");
                self.recheck_rejected(room_id, &rules, row.first_ts).await?;
            }
        }
        Ok(())
    }

    /// Send the events of a room that were rejected since `since` through the pipeline once
    /// more, oldest first: the ones rejected only for citing an event that could not be
    /// checked then pass now. An event that is rejected for a reason of its own is rejected
    /// again. The caller holds the room's lock. Returns how many events are accepted now.
    async fn recheck_rejected(
        &self,
        room_id: &RoomId,
        rules: &RoomVersionRules,
        since: i64,
    ) -> Result<usize> {
        let store = &self.services.store;
        let candidates = store.rejected_timeline_events(room_id, since).await?;
        // Events that are still set aside get the whole pipeline in their own turn; nothing
        // here judges them on less.
        let listed: HashSet<OwnedEventId> = store
            .events_set_aside(room_id)
            .await?
            .into_iter()
            .map(|e| e.pdu.event_id().to_owned())
            .collect();
        let mut restored = 0usize;
        // Bounded: each candidate is a run of the pipeline with the room's lock held.
        for ev in candidates
            .into_iter()
            // History that was fetched for a client keeps its place; it is not turned into
            // an event that arrives now.
            .filter(|ev| ev.stream_ordering > 0)
            .take(MAX_CASCADE)
        {
            let origin = ev.pdu.sender().server_name().to_owned();
            if origin == *self.server_name() {
                continue;
            }
            let mut stands = true;
            for id in ev.pdu.auth_events() {
                if !self.auth_event_stands(id, rules, &listed, 0).await? {
                    stands = false;
                    break;
                }
            }
            if !stands {
                continue;
            }
            let pdu = ev.pdu.with_rejected(false);
            match Box::pin(self.process_pdu_inner(&origin, pdu, rules, false, false, None)).await {
                Ok(Outcome::Accepted | Outcome::SoftFailed) => restored += 1,
                Ok(_) => {}
                Err(e) => {
                    tracing::debug!(event_id = %ev.pdu.event_id(), error = %e, "looking again at a rejected event failed");
                }
            }
        }
        if restored > 0 {
            tracing::info!(%room_id, restored, "events that were rejected for citing an event that could not be checked are accepted now");
        }
        Ok(restored)
    }

    /// Whether an event can serve as an auth event now. One that is stored as a rejected
    /// outlier is judged again: an outlier's verdict rests only on the event itself and the
    /// auth events it cites, and one of those may have been rejected then and stand now.
    async fn auth_event_stands(
        &self,
        id: &EventId,
        rules: &RoomVersionRules,
        listed: &HashSet<OwnedEventId>,
        depth: u32,
    ) -> Result<bool> {
        let Some(ev) = self.services.store.get_event(id).await? else {
            return Ok(false);
        };
        if !ev.pdu.rejected() {
            return Ok(true);
        }
        // A rejected event in the timeline has had, or gets, its own turn in the pipeline,
        // and so does one that is still set aside.
        if !ev.outlier || listed.contains(id) || depth >= MAX_CHAIN {
            return Ok(false);
        }
        for cited in ev.pdu.auth_events() {
            if !Box::pin(self.auth_event_stands(cited, rules, listed, depth + 1)).await? {
                return Ok(false);
            }
        }
        // With the create event, which newer room versions do not list among the auth events.
        let cited = self
            .events_by_id(&auth_event_ids(&ev.pdu, ev.pdu.room_id(), rules))
            .await?;
        let lookup = |i: &EventId| cited.get(i).cloned();
        if !passes_alone(&ev.pdu, rules, lookup) {
            return Ok(false);
        }
        self.services.store.clear_rejected(&[id.to_owned()]).await?;
        self.event_cache
            .insert(id.to_owned(), ev.pdu.with_rejected(false));
        tracing::info!(event_id = %id, "an auth event that had been rejected passes its checks now");
        Ok(true)
    }
}

/// The checks an event gets without the room's state: its size, its form, and the auth events
/// it cites (which the caller has found to stand).
fn passes_alone(
    pdu: &Pdu,
    rules: &RoomVersionRules,
    lookup: impl Fn(&EventId) -> Option<Pdu> + Copy,
) -> bool {
    auth::check_size_limits(pdu).is_ok()
        && auth::check_state_independent(pdu, rules, lookup).is_ok()
        && pdu.auth_events().iter().all(|id| lookup(id).is_some())
        && auth::check_against_auth_events(pdu, rules, lookup).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_wait_doubles_up_to_the_ceiling() {
        let first = Duration::from_mins(2);
        assert_eq!(delay_after(first, 0), Duration::from_mins(2));
        assert_eq!(delay_after(first, 1), Duration::from_mins(4));
        assert_eq!(delay_after(first, 3), Duration::from_mins(16));
        assert_eq!(delay_after(first, 7), Duration::from_mins(256));
        assert_eq!(delay_after(first, 8), MAX_DELAY);
        assert_eq!(delay_after(first, 1000), MAX_DELAY);
        assert_eq!(delay_after(first, -1), Duration::from_mins(2));
        // A short first delay, as the tests configure, grows the same way.
        assert_eq!(
            delay_after(Duration::from_secs(2), 2),
            Duration::from_secs(8)
        );
    }
}
