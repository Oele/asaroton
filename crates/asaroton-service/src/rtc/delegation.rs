//! The worker behind delegated delayed leaves: watches the SFU for the participants whose
//! leave event this server was handed, restarts the event while they stay connected and
//! sends it once they are gone. Webhooks report disconnects immediately; polling covers
//! webhooks that never arrive.

use std::{collections::HashMap, sync::Arc, time::Duration};

use asaroton_store::now_ts;

use crate::Services;

/// How often the SFU is asked who is connected.
const POLL_INTERVAL: Duration = Duration::from_secs(15);

/// A participant that never showed up stops being waited for after this long (or the
/// delay itself when that is shorter): the delayed event then fires on its own.
const CONNECT_WAIT_MS: i64 = 60 * 60 * 1000;

/// Expired `OpenID` tokens and stale participant records are purged this often.
const CLEANUP_INTERVAL: Duration = Duration::from_mins(10);

/// Participant records older than this are dropped.
const PARTICIPANT_MAX_AGE_MS: i64 = 7 * 24 * 60 * 60 * 1000;

/// Starts the worker; does nothing when no SFU is configured.
pub fn start_worker(services: Arc<Services>) {
    tokio::spawn(async move {
        let mut last_cleanup = std::time::Instant::now();
        loop {
            if let Err(e) = cleanup(&services, &mut last_cleanup).await {
                tracing::warn!(error = %e, "RTC cleanup failed");
            }
            if services.rtc.api.is_some()
                && let Err(e) = poll(&services).await
            {
                tracing::warn!(error = %e, "delegated leave poll failed");
            }
            tokio::select! {
                () = tokio::time::sleep(POLL_INTERVAL) => {}
                () = services.rtc.wake.notified() => {}
            }
        }
    });
}

async fn cleanup(services: &Services, last: &mut std::time::Instant) -> crate::Result<()> {
    if last.elapsed() < CLEANUP_INTERVAL {
        return Ok(());
    }
    *last = std::time::Instant::now();
    let now = now_ts();
    services.store.purge_expired_openid_tokens(now).await?;
    services
        .store
        .purge_rtc_participants_before(now - PARTICIPANT_MAX_AGE_MS)
        .await?;
    Ok(())
}

async fn poll(services: &Services) -> crate::Result<()> {
    let leaves = services.store.delegated_leaves().await?;
    if leaves.is_empty() {
        return Ok(());
    }
    let Some(api) = services.rtc.api.as_ref() else {
        return Ok(());
    };
    let mut by_room: HashMap<&str, Vec<&asaroton_store::DelegatedLeave>> = HashMap::new();
    for leave in &leaves {
        by_room.entry(&leave.lk_room).or_default().push(leave);
    }
    let now = now_ts();
    for (lk_room, leaves) in by_room {
        let connected: Vec<String> = match api.list_participants(lk_room).await {
            Ok(list) => list
                .into_iter()
                .filter(super::livekit::Participant::is_connected)
                .map(|p| p.identity)
                .collect(),
            Err(e) => {
                tracing::warn!(lk_room, error = %e, "listing SFU participants failed");
                continue;
            }
        };
        for leave in leaves {
            let Some(ev) = services.store.get_delayed_event(&leave.delay_id).await? else {
                // Sent or cancelled by the client; the delegation went with it.
                services
                    .store
                    .remove_delegated_leave(&leave.delay_id)
                    .await?;
                continue;
            };
            if connected.iter().any(|id| id == &leave.lk_identity) {
                services
                    .store
                    .mark_delegated_leave_connected(&leave.delay_id, now)
                    .await?;
                // Restart with headroom: the event never gets closer than half its delay.
                if now - ev.running_since > ev.delay_ms / 2 {
                    services
                        .store
                        .restart_delayed_event(&leave.delay_id, now)
                        .await?;
                    services.delayed.wake.notify_one();
                }
            } else if leave.connected_ts.is_some() {
                services.send_delegated_leave(leave).await;
            } else if now - leave.created_ts > CONNECT_WAIT_MS.min(ev.delay_ms) {
                tracing::info!(delay_id = %leave.delay_id, "participant never connected: dropping delegation");
                services
                    .store
                    .remove_delegated_leave(&leave.delay_id)
                    .await?;
            }
        }
    }
    Ok(())
}
