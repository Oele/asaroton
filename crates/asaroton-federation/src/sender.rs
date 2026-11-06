//! The outbound transaction sender: one worker per destination, fed by the persistent outbox.
//!
//! Items are queued in the database (`federation_outbox`), so nothing is lost on restart. A
//! worker drains its destination's queue into transactions of at most 50 PDUs and 100 EDUs,
//! each kind in the order it was queued.
//!
//! A transaction keeps its ID and its contents until the destination gives a definite answer.
//! A destination that is still working on the first attempt, or that answered while we had
//! stopped listening, then recognises the retry instead of seeing a second transaction. Only
//! an answer that cannot change (the batch is malformed, or the destination refuses us)
//! discards a batch; rate limits, timeouts and server errors keep it and back off.
//!
//! PDUs and to-device messages go out as soon as they are queued. Transactions that carry
//! nothing else (presence, typing, receipts, device lists) are paced: they wait a moment so
//! that a burst shares one transaction, each destination gets a limited number of them, and
//! all destinations together stay under a ceiling. That keeps a feedback loop in any EDU
//! source from flooding the federation, which has happened.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use asaroton_store::{DestinationState, OutboxItem, OutboxKind, Ts, now_ts};
use dashmap::DashMap;
use parking_lot::Mutex;
use ruma::{
    MilliSecondsSinceUnixEpoch, OwnedServerName, OwnedTransactionId, ServerName,
    api::federation::transactions::{edu::Edu, send_transaction_message},
    serde::Raw,
};
use serde_json::value::RawValue;
use sha2::{Digest, Sha256};
use tokio::sync::Notify;

use crate::{Federation, client::TransactionReply};

const MAX_PDUS: usize = 50;
const MAX_EDUS: usize = 100;
const MAX_ITEMS: usize = MAX_PDUS + MAX_EDUS;
const MIN_RETRY: Duration = Duration::from_secs(2);
const MAX_RETRY: Duration = Duration::from_hours(1);
/// A batch that could not be delivered is retried unchanged for this long. After that the
/// destination can no longer be busy with it, and the queue may hold fresher state.
const PENDING_MAX_AGE: Duration = Duration::from_hours(1);

/// EDU-only work waits this long after it is first seen, so a burst shares a transaction.
const COALESCE: Duration = Duration::from_millis(200);
/// Minimum time between a transaction and a following EDU-only one to the same destination.
const EDU_SPACING: Duration = Duration::from_secs(1);
/// EDU-only transactions per destination: this many at once, then one per interval.
const DESTINATION_BURST: u32 = 30;
const DESTINATION_INTERVAL: Duration = Duration::from_secs(5);
/// Default ceiling on EDU-only transactions per minute over all destinations. A server with
/// a few active users peaks around 200 a minute.
pub const DEFAULT_EDU_TRANSACTIONS_PER_MINUTE: u32 = 600;

/// Wake-up signals for one destination's worker.
#[derive(Debug, Default)]
struct Wake {
    /// Something was queued.
    any: Notify,
    /// Something that must not wait was queued: a PDU or a to-device message.
    urgent: Notify,
}

/// The sender's shared state: the workers and the limits that span destinations.
#[derive(Debug)]
pub struct SenderHandle {
    workers: DashMap<OwnedServerName, Arc<Wake>>,
    /// Budget for EDU-only transactions over all destinations.
    budget: Mutex<Rate>,
    /// Transactions of any kind in the current minute, for the alarm.
    minute: Mutex<(Instant, u32)>,
    per_minute: u32,
}

impl SenderHandle {
    /// `per_minute` is the ceiling on EDU-only transactions over all destinations; zero
    /// selects the default.
    pub fn new(per_minute: u32) -> Self {
        let per_minute = if per_minute == 0 {
            DEFAULT_EDU_TRANSACTIONS_PER_MINUTE
        } else {
            per_minute
        };
        let now = Instant::now();
        Self {
            workers: DashMap::new(),
            budget: Mutex::new(Rate::new(
                (per_minute / 2).max(1),
                Duration::from_mins(1) / per_minute,
                now,
            )),
            minute: Mutex::new((now, 0)),
            per_minute,
        }
    }

    fn notify(&self, fed: &Arc<Federation>, destination: &ServerName, urgent: bool) {
        let wake = self
            .workers
            .entry(destination.to_owned())
            .or_insert_with(|| {
                let wake = Arc::new(Wake::default());
                tokio::spawn(worker(
                    Arc::clone(fed),
                    destination.to_owned(),
                    Arc::clone(&wake),
                ));
                wake
            })
            .clone();
        wake.any.notify_one();
        if urgent {
            wake.urgent.notify_one();
        }
    }

    /// Take one EDU-only transaction from the shared budget, or learn how long to wait.
    fn take_budget(&self, now: Instant) -> Result<(), Duration> {
        let mut budget = self.budget.lock();
        let wait = budget.wait(now);
        if wait.is_zero() {
            budget.take(now);
            Ok(())
        } else {
            Err(wait)
        }
    }

    /// Count a transaction, and raise the alarm once in a minute that sees too many. The
    /// ceiling holds back EDU-only transactions, so this can only be passed with the help of
    /// PDUs or to-device messages; either way somebody should look.
    fn count(&self, now: Instant) {
        let mut minute = self.minute.lock();
        if now.saturating_duration_since(minute.0) >= Duration::from_mins(1) {
            *minute = (now, 0);
        }
        minute.1 += 1;
        if minute.1 == self.per_minute {
            tracing::error!(
                transactions = minute.1,
                "outbound federation is unusually busy: this many transactions were sent within \
                 a minute; check for a loop in presence, typing, receipts or device lists"
            );
        }
    }
}

/// Start workers for every destination that already has queued items.
pub fn start(fed: Arc<Federation>) {
    tokio::spawn(async move {
        match fed.services.store.outbox_destinations().await {
            Ok(dests) => {
                for d in dests {
                    fed.sender.notify(&fed, &d, false);
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
            self.sender.notify(self, d, true);
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
        let urgent = edu
            .get("edu_type")
            .and_then(|t| t.as_str())
            .is_some_and(is_urgent_edu);
        for d in &destinations {
            self.sender.notify(self, d, urgent);
        }
    }
}

/// EDUs that must not be held back: to-device messages carry call signalling and room keys.
const URGENT_EDU_TYPES: &[&str] = &["m.direct_to_device"];

fn is_urgent_edu(edu_type: &str) -> bool {
    URGENT_EDU_TYPES.contains(&edu_type)
}

/// A rate limit: `burst` at once, then one per `interval`.
#[derive(Debug)]
struct Rate {
    interval: Duration,
    /// How far ahead of the steady schedule a burst may run.
    tolerance: Duration,
    /// When the next one is due on the steady schedule.
    next: Instant,
}

impl Rate {
    fn new(burst: u32, interval: Duration, now: Instant) -> Self {
        Self {
            interval,
            tolerance: interval.saturating_mul(burst.saturating_sub(1)),
            next: now,
        }
    }

    /// How long until one may go; zero when one may go now.
    fn wait(&self, now: Instant) -> Duration {
        self.next
            .saturating_duration_since(now)
            .saturating_sub(self.tolerance)
    }

    fn take(&mut self, now: Instant) {
        self.next = self.next.max(now) + self.interval;
    }
}

/// Holds back EDU-only transactions to one destination.
#[derive(Debug)]
struct Pace {
    /// When the EDU-only work now waiting was first seen.
    waiting_since: Option<Instant>,
    /// When the previous transaction went out.
    last_sent: Option<Instant>,
    rate: Rate,
}

impl Pace {
    fn new(now: Instant) -> Self {
        Self {
            waiting_since: None,
            last_sent: None,
            rate: Rate::new(DESTINATION_BURST, DESTINATION_INTERVAL, now),
        }
    }

    /// How long EDU-only work still has to wait, if at all.
    fn wait(&mut self, now: Instant) -> Option<Duration> {
        let since = *self.waiting_since.get_or_insert(now);
        let mut wait = (since + COALESCE).saturating_duration_since(now);
        if let Some(last) = self.last_sent {
            wait = wait.max((last + EDU_SPACING).saturating_duration_since(now));
        }
        wait = wait.max(self.rate.wait(now));
        (!wait.is_zero()).then_some(wait)
    }

    /// An EDU-only transaction is about to go.
    fn spend(&mut self, now: Instant) {
        self.rate.take(now);
    }

    /// A transaction of any kind went out.
    fn sent(&mut self, now: Instant) {
        self.last_sent = Some(now);
        self.waiting_since = None;
    }

    /// The queue is empty.
    fn idle(&mut self) {
        self.waiting_since = None;
    }
}

/// A transaction as it goes out, unchanged from one attempt to the next.
#[derive(Debug)]
struct Batch {
    txn_id: OwnedTransactionId,
    origin_server_ts: MilliSecondsSinceUnixEpoch,
    /// The outbox rows this transaction settles.
    ids: Vec<i64>,
    pdus: Vec<Box<RawValue>>,
    edus: Vec<Raw<Edu>>,
    edu_types: Vec<String>,
    /// Carries something that must not wait.
    urgent: bool,
    built: Instant,
}

impl Batch {
    fn is_empty(&self) -> bool {
        self.pdus.is_empty() && self.edus.is_empty()
    }

    fn request(&self, origin: &ServerName) -> send_transaction_message::v1::Request {
        let mut req = send_transaction_message::v1::Request::new(
            self.txn_id.clone(),
            origin.to_owned(),
            self.origin_server_ts,
        );
        req.pdus.clone_from(&self.pdus);
        req.edus.clone_from(&self.edus);
        req
    }
}

/// The transaction ID for a set of outbox rows. It is derived from the rows, not drawn at
/// random, so that the same rows get the same ID after a restart as well. The queue times
/// keep IDs from repeating should the row numbering ever start over, which would make
/// destinations take new transactions for ones they have seen.
fn transaction_id(rows: &[(i64, Ts)]) -> OwnedTransactionId {
    let mut hash = Sha256::new();
    for (id, queued) in rows {
        hash.update(id.to_be_bytes());
        hash.update(queued.to_be_bytes());
    }
    let digest = hash.finalize();
    OwnedTransactionId::from(hex::encode(&digest[..16]))
}

/// Read the next transaction's worth of a destination's queue: at most `cap` items, events
/// before EDUs.
async fn next_items(
    fed: &Federation,
    destination: &ServerName,
    cap: usize,
) -> asaroton_store::Result<Vec<OutboxItem>> {
    let mut items = fed
        .services
        .store
        .outbox_for(
            destination,
            MAX_PDUS.min(cap),
            MAX_EDUS.min(cap),
            URGENT_EDU_TYPES,
        )
        .await?;
    items.truncate(cap);
    Ok(items)
}

/// Turn queued items into a transaction.
async fn build_batch(fed: &Federation, items: Vec<OutboxItem>) -> Batch {
    let mut pdus = Vec::new();
    let mut edus = Vec::new();
    let mut edu_types: Vec<String> = Vec::new();
    let mut rows = Vec::new();
    let mut urgent = false;
    for item in items {
        match item.kind {
            OutboxKind::Pdu(event_id) => {
                urgent = true;
                match fed.services.store.get_event(&event_id).await {
                    Ok(Some(ev)) => {
                        let json =
                            asaroton_core::canonical::to_json_value(ev.pdu.to_federation_json());
                        if let Ok(raw) = serde_json::value::to_raw_value(&json) {
                            pdus.push(raw);
                        }
                    }
                    Ok(None) => tracing::warn!(%event_id, "queued event vanished"),
                    Err(e) => tracing::error!(error = %e, "loading queued event"),
                }
            }
            OutboxKind::Edu(json) => {
                if let Some(t) = json.get("edu_type").and_then(|t| t.as_str()) {
                    urgent |= is_urgent_edu(t);
                    edu_types.push(t.to_owned());
                }
                if let Ok(raw) = serde_json::value::to_raw_value(&json) {
                    edus.push(Raw::from_json(raw));
                }
            }
        }
        rows.push((item.id, item.created_ts));
    }
    Batch {
        txn_id: transaction_id(&rows),
        origin_server_ts: MilliSecondsSinceUnixEpoch::now(),
        ids: rows.into_iter().map(|(id, _)| id).collect(),
        pdus,
        edus,
        edu_types,
        urgent,
        built: Instant::now(),
    }
}

/// What to do with a batch once the destination has answered.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Verdict {
    /// Accepted: the rows are settled.
    Delivered,
    /// Refused in a way that sending it again cannot change.
    Discard,
    /// Too large for the destination: send fewer items at a time.
    Split,
    /// A condition that passes: keep the batch and try again later.
    Retry,
}

fn verdict(status: http::StatusCode) -> Verdict {
    match status.as_u16() {
        200..=299 => Verdict::Delivered,
        // The batch itself is at fault (400), or the destination refuses to federate with
        // us (403).
        400 | 403 => Verdict::Discard,
        413 => Verdict::Split,
        // Everything else passes, or may: a rate limit (429), a destination that could not
        // fetch our key just now (401), a proxy without its backend (404, 5xx). A queue for
        // a destination that never recovers is thinned out by the housekeeping task.
        _ => Verdict::Retry,
    }
}

/// How long the destination asked us to stay away: `Retry-After`, or `retry_after_ms` in
/// the body.
fn requested_delay(reply: &TransactionReply) -> Option<Duration> {
    reply.retry_after.or_else(|| {
        serde_json::from_slice::<serde_json::Value>(&reply.body)
            .ok()?
            .get("retry_after_ms")?
            .as_u64()
            .map(Duration::from_millis)
    })
}

/// The wait before the next attempt: double the previous one, or what the destination
/// asked for when that is longer.
fn next_retry(previous_ms: i64, requested: Option<Duration>) -> Duration {
    let doubled = match u64::try_from(previous_ms) {
        Ok(ms) if ms > 0 => Duration::from_millis(ms).saturating_mul(2),
        _ => MIN_RETRY,
    };
    doubled.max(requested.unwrap_or_default()).min(MAX_RETRY)
}

/// The time left before a failing destination may be tried again.
fn backoff_left(state: &DestinationState, now: Ts) -> Option<Duration> {
    let last = state.retry_last_ts?;
    if state.retry_interval_ms <= 0 {
        return None;
    }
    let left = u64::try_from(last.saturating_add(state.retry_interval_ms) - now).ok()?;
    (left > 0).then(|| Duration::from_millis(left))
}

/// PDUs the destination accepted the transaction for but rejected individually.
fn rejected_pdus(body: &[u8]) -> Vec<(String, String)> {
    let Ok(serde_json::Value::Object(mut reply)) = serde_json::from_slice(body) else {
        return Vec::new();
    };
    // Some servers answer `{}`; a transaction without the `pdus` key was accepted all the same.
    let Some(serde_json::Value::Object(pdus)) = reply.remove("pdus") else {
        return Vec::new();
    };
    pdus.into_iter()
        .filter_map(|(event_id, result)| {
            let reason = result.get("error")?;
            let reason = reason
                .as_str()
                .map_or_else(|| reason.to_string(), str::to_owned);
            Some((event_id, reason))
        })
        .collect()
}

/// A response body for the log: on one line and not too long.
fn one_line(body: &[u8]) -> String {
    const LIMIT: usize = 300;
    let text = String::from_utf8_lossy(body);
    let mut line = String::new();
    for word in text.split_whitespace() {
        if !line.is_empty() {
            line.push(' ');
        }
        line.push_str(word);
        if line.len() > LIMIT {
            let mut end = LIMIT;
            while !line.is_char_boundary(end) {
                end -= 1;
            }
            line.truncate(end);
            line.push('…');
            break;
        }
    }
    line
}

#[allow(clippy::too_many_lines)]
async fn worker(fed: Arc<Federation>, destination: OwnedServerName, wake: Arc<Wake>) {
    let store = &fed.services.store;
    // The batch that still awaits a definite answer.
    let mut pending: Option<Batch> = None;
    // Items per transaction; smaller after the destination called one too large.
    let mut cap = MAX_ITEMS;
    let mut pace = Pace::new(Instant::now());
    loop {
        let state = store
            .destination_state(&destination)
            .await
            .unwrap_or_default();
        if let Some(wait) = backoff_left(&state, now_ts()) {
            tokio::select! {
                () = tokio::time::sleep(wait) => {}
                () = wake.any.notified() => {}
            }
            continue;
        }

        let batch = match pending
            .take()
            .filter(|b| b.built.elapsed() < PENDING_MAX_AGE)
        {
            Some(batch) => batch,
            None => {
                let items = match next_items(&fed, &destination, cap).await {
                    Ok(items) => items,
                    Err(e) => {
                        tracing::error!(error = %e, %destination, "outbox read failed");
                        tokio::time::sleep(Duration::from_secs(5)).await;
                        continue;
                    }
                };
                if items.is_empty() {
                    pace.idle();
                    wake.any.notified().await;
                    continue;
                }
                let batch = build_batch(&fed, items).await;
                if batch.is_empty() {
                    // Nothing sendable in these rows (their events are gone).
                    let _ = store.delete_outbox_items(&batch.ids).await;
                    continue;
                }
                if !batch.urgent {
                    let now = Instant::now();
                    let wait = match pace.wait(now) {
                        Some(wait) => Some(wait),
                        None => fed.sender.take_budget(now).err(),
                    };
                    if let Some(wait) = wait {
                        // Read the queue again afterwards: more will have arrived to share
                        // the transaction, and something urgent ends the wait at once.
                        tokio::select! {
                            () = tokio::time::sleep(wait) => {}
                            () = wake.urgent.notified() => {}
                        }
                        continue;
                    }
                    pace.spend(now);
                }
                batch
            }
        };

        let started = Instant::now();
        pace.sent(started);
        fed.sender.count(started);
        let reply = fed
            .client
            .send_transaction(&destination, batch.request(fed.server_name()))
            .await;
        let (verdict, requested, problem) = match &reply {
            Ok(reply) => {
                let verdict = verdict(reply.status);
                let problem = (verdict != Verdict::Delivered).then(|| {
                    format!(
                        "{destination} answered {}: {}",
                        reply.status,
                        one_line(&reply.body)
                    )
                });
                (verdict, requested_delay(reply), problem)
            }
            Err(e) => (Verdict::Retry, None, Some(e.to_string())),
        };
        let pdu_count = batch.pdus.len();
        let edu_count = batch.edus.len();
        let txn_id = &batch.txn_id;

        match verdict {
            Verdict::Delivered => {
                if let Ok(reply) = &reply {
                    for (event_id, reason) in rejected_pdus(&reply.body) {
                        tracing::warn!(%destination, %event_id, %reason, "remote rejected PDU");
                    }
                }
                tracing::debug!(%destination, pdu_count, edu_count, edu_types = %batch.edu_types.join(","), %txn_id, "transaction sent");
                let _ = store.delete_outbox_items(&batch.ids).await;
                let _ = store
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
                cap = cap.saturating_mul(2).min(MAX_ITEMS);
            }
            Verdict::Split if batch.ids.len() > 1 => {
                // Fewer items make a different transaction, so there is nothing to wait for.
                cap = batch.ids.len() / 2;
                tracing::warn!(%destination, pdu_count, edu_count, %txn_id, retry_with = cap, "transaction too large for the destination; sending fewer items at a time");
            }
            Verdict::Discard | Verdict::Split | Verdict::Retry => {
                let discard = verdict != Verdict::Retry;
                let next = next_retry(state.retry_interval_ms, requested);
                let action = if discard { "discard" } else { "retry" };
                tracing::warn!(%destination, error = problem.as_deref().unwrap_or_default(), retry_in = ?next, action, pdu_count, edu_count, %txn_id, "transaction failed");
                if discard {
                    let _ = store.delete_outbox_items(&batch.ids).await;
                } else {
                    pending = Some(batch);
                }
                let now = now_ts();
                let _ = store
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

#[cfg(test)]
mod tests {
    use super::*;

    fn reply(status: u16, retry_after: Option<u64>, body: &str) -> TransactionReply {
        TransactionReply {
            status: http::StatusCode::from_u16(status).unwrap_or_default(),
            retry_after: retry_after.map(Duration::from_secs),
            body: bytes::Bytes::copy_from_slice(body.as_bytes()),
        }
    }

    #[test]
    fn only_a_refusal_that_cannot_change_discards_a_batch() {
        for status in [200, 201, 204] {
            assert_eq!(verdict(reply(status, None, "").status), Verdict::Delivered);
        }
        for status in [400, 403] {
            assert_eq!(verdict(reply(status, None, "").status), Verdict::Discard);
        }
        assert_eq!(verdict(reply(413, None, "").status), Verdict::Split);
        // Rate limits, a failed key fetch, a missing backend, overload, redirects.
        for status in [301, 401, 404, 405, 408, 429, 500, 502, 503, 504, 520] {
            assert_eq!(
                verdict(reply(status, None, "").status),
                Verdict::Retry,
                "{status}"
            );
        }
    }

    #[test]
    fn the_destination_can_ask_for_a_longer_wait() {
        let header = reply(429, Some(30), r#"{"retry_after_ms": 5000}"#);
        assert_eq!(requested_delay(&header), Some(Duration::from_secs(30)));
        let body = reply(
            429,
            None,
            r#"{"errcode":"M_LIMIT_EXCEEDED","retry_after_ms":5000}"#,
        );
        assert_eq!(requested_delay(&body), Some(Duration::from_secs(5)));
        assert_eq!(
            requested_delay(&reply(503, None, "<html>busy</html>")),
            None
        );

        // Doubling from the minimum, never beyond the maximum.
        assert_eq!(next_retry(0, None), MIN_RETRY);
        assert_eq!(next_retry(2_000, None), Duration::from_secs(4));
        assert_eq!(next_retry(i64::MAX, None), MAX_RETRY);
        // The longer of the two wins, and the maximum bounds what a destination can ask.
        assert_eq!(
            next_retry(2_000, Some(Duration::from_secs(30))),
            Duration::from_secs(30)
        );
        assert_eq!(
            next_retry(60_000, Some(Duration::from_secs(30))),
            Duration::from_mins(2)
        );
        assert_eq!(next_retry(0, Some(Duration::from_hours(48))), MAX_RETRY);
    }

    #[test]
    fn backoff_runs_from_the_last_attempt() {
        let failing = DestinationState {
            failure_ts: Some(1_000),
            retry_last_ts: Some(10_000),
            retry_interval_ms: 4_000,
            last_successful_ts: None,
        };
        assert_eq!(backoff_left(&failing, 11_000), Some(Duration::from_secs(3)));
        assert_eq!(backoff_left(&failing, 14_000), None);
        assert_eq!(backoff_left(&failing, 99_000), None);
        assert_eq!(backoff_left(&DestinationState::default(), 11_000), None);
    }

    #[test]
    fn the_same_rows_give_the_same_transaction_id() {
        let rows = [(7, 1_793_000_000_000), (8, 1_793_000_000_100)];
        let id = transaction_id(&rows);
        assert_eq!(id, transaction_id(&rows));
        assert_eq!(id.as_str().len(), 32);
        // One more row, one row fewer, or the same row numbers queued at another time
        // (a database that started over) are different transactions.
        assert_ne!(
            id,
            transaction_id(&[rows[0], rows[1], (9, 1_793_000_000_200)])
        );
        assert_ne!(id, transaction_id(&rows[..1]));
        assert_ne!(id, transaction_id(&[(7, 1_793_000_000_001), rows[1]]));
    }

    #[test]
    fn a_rate_allows_its_burst_and_then_one_per_interval() {
        let start = Instant::now();
        let second = Duration::from_secs(1);
        let mut rate = Rate::new(3, second, start);
        for _ in 0..3 {
            assert_eq!(rate.wait(start), Duration::ZERO);
            rate.take(start);
        }
        assert_eq!(rate.wait(start), second);
        assert_eq!(rate.wait(start + second / 2), second / 2);
        assert_eq!(rate.wait(start + second), Duration::ZERO);
        rate.take(start + second);
        assert_eq!(rate.wait(start + second), second);
        // Idle time restores the burst, and no more than the burst.
        let later = start + Duration::from_hours(1);
        for _ in 0..3 {
            assert_eq!(rate.wait(later), Duration::ZERO);
            rate.take(later);
        }
        assert_eq!(rate.wait(later), second);
    }

    #[test]
    fn edu_only_work_is_coalesced_spaced_and_limited() {
        let start = Instant::now();
        let mut pace = Pace::new(start);
        // First sight: wait for the rest of the burst to arrive.
        assert_eq!(pace.wait(start), Some(COALESCE));
        // Looking again does not restart the clock.
        assert_eq!(pace.wait(start + COALESCE / 2), Some(COALESCE / 2));
        assert_eq!(pace.wait(start + COALESCE), None);
        pace.spend(start + COALESCE);
        pace.sent(start + COALESCE);
        // The next EDU-only transaction keeps its distance from the previous one.
        let again = start + COALESCE + Duration::from_millis(300);
        assert_eq!(
            pace.wait(again),
            Some(EDU_SPACING.saturating_sub(Duration::from_millis(300)))
        );
        // After a transaction with events in it, too.
        let mut pace = Pace::new(start);
        pace.sent(start);
        assert_eq!(pace.wait(start), Some(EDU_SPACING));

        // A destination that keeps getting EDU-only transactions runs out of its burst and
        // is then held to the steady rate.
        let mut pace = Pace::new(start);
        let mut now = start;
        let mut sent = 0;
        while now < start + Duration::from_mins(10) {
            match pace.wait(now) {
                Some(wait) => now += wait,
                None => {
                    pace.spend(now);
                    pace.sent(now);
                    sent += 1;
                }
            }
        }
        let steady =
            u32::try_from(Duration::from_mins(10).as_secs() / DESTINATION_INTERVAL.as_secs())
                .unwrap_or(u32::MAX);
        assert!(
            sent >= steady && sent <= steady + DESTINATION_BURST,
            "{sent}"
        );
    }

    #[test]
    fn the_shared_budget_bounds_a_flood() {
        let sender = SenderHandle::new(0);
        let start = Instant::now();
        // Far more demand than budget for two minutes, asked for every millisecond.
        let mut granted = 0;
        for ms in 0..120_000 {
            if sender
                .take_budget(start + Duration::from_millis(ms))
                .is_ok()
            {
                granted += 1;
            }
        }
        let ceiling = 2 * DEFAULT_EDU_TRANSACTIONS_PER_MINUTE;
        assert!(
            granted > ceiling && granted <= ceiling + DEFAULT_EDU_TRANSACTIONS_PER_MINUTE / 2,
            "{granted}"
        );
    }

    #[test]
    fn an_accepted_transaction_may_still_reject_single_events() {
        let none: Vec<(String, String)> = Vec::new();
        for accepted in ["{}", "", "OK", r#"{"pdus":{"$a":{}}}"#] {
            assert_eq!(rejected_pdus(accepted.as_bytes()), none, "{accepted}");
        }
        assert_eq!(
            rejected_pdus(br#"{"pdus":{"$a":{},"$b":{"error":"not allowed"}}}"#),
            vec![("$b".to_owned(), "not allowed".to_owned())]
        );
    }

    #[test]
    fn response_bodies_are_logged_on_one_line() {
        assert_eq!(
            one_line(b"{\n  \"errcode\": \"M_UNKNOWN\",\n\t\"error\": \"x\"\n}"),
            r#"{ "errcode": "M_UNKNOWN", "error": "x" }"#
        );
        let long = one_line("é ".repeat(400).as_bytes());
        assert!(long.chars().count() <= 301 && long.ends_with('…'), "{long}");
    }

    #[test]
    fn only_to_device_messages_are_urgent_among_edus() {
        assert!(is_urgent_edu("m.direct_to_device"));
        for t in [
            "m.presence",
            "m.typing",
            "m.receipt",
            "m.device_list_update",
            "m.signing_key_update",
        ] {
            assert!(!is_urgent_edu(t));
        }
    }
}
