//! Periodic cleanup of the federation tables, which otherwise only grow.

use std::{sync::Arc, time::Duration};

use asaroton_service::Services;
use asaroton_store::now_ts;

/// How long the stored answer to an inbound transaction is kept. It only serves to answer a
/// retransmission, and those come within minutes.
const RECEIVED_TRANSACTION_AGE: Duration = Duration::from_hours(24);
/// A destination failing for this long has its queue thinned out.
const DEAD_AFTER: Duration = Duration::from_hours(7 * 24);
const FIRST_RUN: Duration = Duration::from_mins(5);
const INTERVAL: Duration = Duration::from_hours(1);

/// Start the hourly cleanup.
pub(crate) fn start(services: Arc<Services>) {
    tokio::spawn(async move {
        tokio::time::sleep(FIRST_RUN).await;
        loop {
            run(&services).await;
            tokio::time::sleep(INTERVAL).await;
        }
    });
}

async fn run(services: &Services) {
    let now = now_ts();
    match services
        .store
        .prune_received_transactions(now - millis(RECEIVED_TRANSACTION_AGE))
        .await
    {
        Ok(0) => {}
        Ok(rows) => tracing::info!(rows, "forgot old inbound transactions"),
        Err(e) => tracing::error!(error = %e, "pruning inbound transactions failed"),
    }
    match services
        .store
        .prune_dead_outbox(now - millis(DEAD_AFTER))
        .await
    {
        Ok(0) => {}
        Ok(rows) => {
            tracing::info!(
                rows,
                "thinned out the queues of destinations that have been failing for a week"
            );
        }
        Err(e) => tracing::error!(error = %e, "pruning the federation outbox failed"),
    }
}

fn millis(d: Duration) -> i64 {
    i64::try_from(d.as_millis()).unwrap_or(i64::MAX)
}
