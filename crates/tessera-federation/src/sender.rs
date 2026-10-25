//! The outbound transaction sender: one worker per destination, fed by the persistent outbox.
//!
//! Items are queued in the database (`federation_outbox`), so nothing is lost on restart. A
//! worker drains its destination's queue into transactions of at most 50 PDUs and 100 EDUs,
//! retrying with exponential backoff on failure.

use std::{sync::Arc, time::Duration};

use dashmap::DashMap;
use ruma::{
    MilliSecondsSinceUnixEpoch, OwnedServerName, OwnedTransactionId, ServerName,
    api::federation::transactions::send_transaction_message,
};
use tessera_store::{DestinationState, OutboxKind, now_ts};
use tokio::sync::Notify;

use crate::{Error, Federation};

const MAX_PDUS: usize = 50;
const MAX_EDUS: usize = 100;
const MIN_RETRY: Duration = Duration::from_secs(2);
const MAX_RETRY: Duration = Duration::from_hours(1);

/// Wake-up handles per destination.
#[derive(Debug, Default)]
pub struct SenderHandle {
    workers: DashMap<OwnedServerName, Arc<Notify>>,
}

impl SenderHandle {
    fn notify(&self, fed: &Arc<Federation>, destination: &ServerName) {
        let notify = self
            .workers
            .entry(destination.to_owned())
            .or_insert_with(|| {
                let n = Arc::new(Notify::new());
                tokio::spawn(worker(
                    Arc::clone(fed),
                    destination.to_owned(),
                    Arc::clone(&n),
                ));
                n
            })
            .clone();
        notify.notify_one();
    }
}

/// Start workers for every destination that already has queued items.
pub fn start(fed: Arc<Federation>) {
    tokio::spawn(async move {
        match fed.services.store.outbox_destinations().await {
            Ok(dests) => {
                for d in dests {
                    fed.sender.notify(&fed, &d);
                }
            }
            Err(e) => tracing::error!(error = %e, "could not read federation outbox"),
        }
    });
}

impl Federation {
    /// Queue a PDU for the given destinations and wake their workers.
    pub async fn queue_pdu(
        self: &Arc<Self>,
        destinations: Vec<OwnedServerName>,
        event_id: &ruma::EventId,
    ) {
        let destinations: Vec<OwnedServerName> = destinations
            .into_iter()
            .filter(|d| *d != self.services.config.server_name)
            .collect();
        if let Err(e) = self
            .services
            .store
            .enqueue_pdu(&destinations, event_id)
            .await
        {
            tracing::error!(error = %e, "could not enqueue PDU");
            return;
        }
        for d in &destinations {
            self.sender.notify(self, d);
        }
    }

    /// Queue an EDU for the given destinations.
    pub async fn queue_edu(
        self: &Arc<Self>,
        destinations: Vec<OwnedServerName>,
        edu: serde_json::Value,
    ) {
        let destinations: Vec<OwnedServerName> = destinations
            .into_iter()
            .filter(|d| *d != self.services.config.server_name)
            .collect();
        if let Err(e) = self.services.store.enqueue_edu(&destinations, &edu).await {
            tracing::error!(error = %e, "could not enqueue EDU");
            return;
        }
        for d in &destinations {
            self.sender.notify(self, d);
        }
    }
}

async fn worker(fed: Arc<Federation>, destination: OwnedServerName, notify: Arc<Notify>) {
    loop {
        // Respect backoff.
        let state = fed
            .services
            .store
            .destination_state(&destination)
            .await
            .unwrap_or_default();
        if let (Some(last), interval) = (state.retry_last_ts, state.retry_interval_ms)
            && interval > 0
        {
            let due = last + interval;
            let now = now_ts();
            if due > now {
                let wait = Duration::from_millis(u64::try_from(due - now).unwrap_or(0));
                tokio::select! {
                    () = tokio::time::sleep(wait) => {}
                    () = notify.notified() => {}
                }
                continue;
            }
        }

        let items = match fed
            .services
            .store
            .outbox_for(&destination, MAX_PDUS + MAX_EDUS)
            .await
        {
            Ok(items) => items,
            Err(e) => {
                tracing::error!(error = %e, %destination, "outbox read failed");
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };
        if items.is_empty() {
            notify.notified().await;
            continue;
        }

        let mut pdus = Vec::new();
        let mut edus = Vec::new();
        let mut ids = Vec::new();
        for item in items {
            match item.kind {
                OutboxKind::Pdu(event_id) => {
                    if pdus.len() >= MAX_PDUS {
                        break;
                    }
                    match fed.services.store.get_event(&event_id).await {
                        Ok(Some(ev)) => {
                            let json =
                                tessera_core::canonical::to_json_value(ev.pdu.to_federation_json());
                            if let Ok(raw) = serde_json::value::to_raw_value(&json) {
                                pdus.push(raw);
                            }
                        }
                        Ok(None) => tracing::warn!(%event_id, "queued event vanished"),
                        Err(e) => tracing::error!(error = %e, "loading queued event"),
                    }
                }
                OutboxKind::Edu(json) => {
                    if edus.len() >= MAX_EDUS {
                        break;
                    }
                    if let Ok(raw) = serde_json::value::to_raw_value(&json) {
                        edus.push(ruma::serde::Raw::from_json(raw));
                    }
                }
            }
            ids.push(item.id);
        }

        let txn_id = OwnedTransactionId::from(format!("{}", now_ts()));
        let mut req = send_transaction_message::v1::Request::new(
            txn_id,
            fed.services.config.server_name.clone(),
            MilliSecondsSinceUnixEpoch::now(),
        );
        req.pdus = pdus;
        req.edus = edus;
        let pdu_count = req.pdus.len();
        let edu_count = req.edus.len();

        match fed.client.send(&destination, req).await {
            Ok(resp) => {
                for (event_id, result) in &resp.pdus {
                    if let Err(reason) = result {
                        tracing::warn!(%destination, %event_id, %reason, "remote rejected PDU");
                    }
                }
                tracing::debug!(%destination, pdu_count, edu_count, "transaction sent");
                let _ = fed.services.store.delete_outbox_items(&ids).await;
                let _ = fed
                    .services
                    .store
                    .set_destination_state(
                        &destination,
                        &DestinationState {
                            failure_ts: None,
                            retry_last_ts: None,
                            retry_interval_ms: 0,
                            last_successful_ts: Some(now_ts()),
                        },
                    )
                    .await;
            }
            Err(e) => {
                // Permanent remote errors (4xx) drop the batch; anything else backs off.
                let permanent =
                    matches!(&e, Error::RemoteError { status, .. } if status.is_client_error());
                let now = now_ts();
                let next = if state.retry_interval_ms == 0 {
                    MIN_RETRY
                } else {
                    Duration::from_millis(u64::try_from(state.retry_interval_ms).unwrap_or(0))
                        .saturating_mul(2)
                        .min(MAX_RETRY)
                };
                tracing::warn!(%destination, error = %e, retry_in = ?next, permanent, "transaction failed");
                if permanent {
                    let _ = fed.services.store.delete_outbox_items(&ids).await;
                }
                let _ = fed
                    .services
                    .store
                    .set_destination_state(
                        &destination,
                        &DestinationState {
                            failure_ts: state.failure_ts.or(Some(now)),
                            retry_last_ts: Some(now),
                            retry_interval_ms: i64::try_from(next.as_millis()).unwrap_or(i64::MAX),
                            last_successful_ts: state.last_successful_ts,
                        },
                    )
                    .await;
            }
        }
    }
}

/// Group destinations for an EDU by server, dropping ourselves.
pub fn servers_except_self(
    servers: impl IntoIterator<Item = OwnedServerName>,
    me: &ServerName,
) -> Vec<OwnedServerName> {
    let mut seen: std::collections::HashSet<OwnedServerName> = std::collections::HashSet::new();
    servers
        .into_iter()
        .filter(|s| *s != me)
        .filter(|s| seen.insert(s.clone()))
        .collect()
}
