//! Outbound backfill: fetch older history from other servers when clients paginate past
//! what we hold.

use asaroton_core::Pdu;
use ruma::{OwnedEventId, RoomId, UInt, api::federation::backfill::get_backfill};

use crate::{Federation, Result};

impl Federation {
    /// Fetch up to `limit` older events for the room from any other server in it, store them
    /// as backfilled history, and return how many new events were stored.
    pub async fn backfill_room(&self, room_id: &RoomId, limit: usize) -> Result<usize> {
        let services = &self.services;
        // Continue from the gaps in the timeline itself; the prev events of outlier state
        // events (a join's auth chain reaches back to the room's creation) are not history.
        let mut from: Vec<OwnedEventId> = services.store.backfill_points(room_id, 5).await?;
        if from.is_empty() {
            from = services.store.backward_extremities(room_id).await?;
        }
        if from.is_empty() {
            return Ok(0);
        }
        from.truncate(5);
        let servers = services.remote_servers_in_room(room_id).await?;
        let room = services
            .store
            .get_room(room_id)
            .await?
            .ok_or_else(|| crate::Error::Protocol("unknown room".into()))?;
        let rules = asaroton_core::room_version::rules(&room.room_version)?;
        let limit = limit.clamp(1, 100);
        for server in servers {
            let req = get_backfill::v1::Request::new(
                room_id.to_owned(),
                from.clone(),
                UInt::try_from(limit).unwrap_or(UInt::MAX),
            );
            let resp = match self.client.send_long(&server, req).await {
                Ok(r) => r,
                Err(e) => {
                    tracing::debug!(%server, error = %e, "backfill request failed");
                    continue;
                }
            };
            let mut pdus: Vec<Pdu> = Vec::new();
            for raw in resp.pdus {
                if let Ok(obj) = asaroton_core::canonical::from_str(raw.get())
                    && let Ok(p) = self.parse_and_verify(&server, obj, &rules).await
                    && p.room_id() == room_id
                {
                    pdus.push(p);
                }
            }
            let ids: Vec<OwnedEventId> = pdus.iter().map(|p| p.event_id().to_owned()).collect();
            let have: std::collections::HashSet<OwnedEventId> = services
                .store
                .have_events(&ids)
                .await?
                .into_iter()
                .collect();
            // Outliers we already hold (from a join's auth chain) still need to become history.
            let mut new: Vec<Pdu> = Vec::new();
            for p in pdus {
                let stored = services.store.get_event(p.event_id()).await?;
                if !have.contains(p.event_id()) || stored.is_some_and(|e| e.outlier) {
                    new.push(p);
                }
            }
            if new.is_empty() {
                continue;
            }
            // Oldest first for processing; the reserved orderings run the other way so the
            // oldest event ends up earliest in the stream.
            new.sort_by_key(|p| u64::from(p.depth()));
            let mut orderings = services.store.reserve_backfill_orderings(new.len()).await?;
            orderings.reverse();
            let mut stored = 0usize;
            for (p, so) in new.into_iter().zip(orderings) {
                match self
                    .process_pdu_inner(&server, p.clone(), &rules, true, false, Some(so))
                    .await
                {
                    Ok(crate::receive::Outcome::Rejected(reason)) => {
                        tracing::debug!(event_id = %p.event_id(), %reason, "backfilled event rejected");
                    }
                    Ok(_) => stored += 1,
                    Err(e) => {
                        tracing::warn!(event_id = %p.event_id(), error = %e, "backfill processing failed");
                    }
                }
            }
            return Ok(stored);
        }
        Ok(0)
    }
}
