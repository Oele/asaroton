//! The SFU in asaroton's own process (`[rtc.livekit] in_process = true`): musivum, a port of
//! `LiveKit`'s media server, started from the configuration `asaroton livekit-config` writes
//! for `livekit-server`.
//!
//! It takes the three places where asaroton otherwise reaches an external SFU:
//!
//! - the room calls (create a room, list and remove participants) go to it directly
//!   ([`SfuRooms`]) instead of to `LiveKit`'s API over HTTP;
//! - its signalling routes are served under asaroton's signalling path, instead of the relay
//!   to an external SFU, and it names the real TURN port itself, which the relay corrects
//!   for `livekit-server`;
//! - its events (who joined and left, rooms that closed) are handed to
//!   `Services::rtc_sfu_event` in the process; it sends no webhooks.
//!
//! musivum runs on asaroton's runtime and starts some threads of its own: see the
//! documentation of `musivum_server::embed`.
//!
//! It is not shut down when the server stops: that would tell every client that the call is
//! over (a leave with the action `DISCONNECT`), and they would not come back. Ending with the
//! process cuts their connections instead; they try to resume them on the server that comes up,
//! which tells them to reconnect, and they do (seconds). Their leaves wait meanwhile (see
//! `SfuRooms::started_at`).

use std::{path::Path, sync::Arc};

use anyhow::Context;
use asaroton_service::{
    Services,
    rtc::livekit::{ApiError, Participant, SfuRooms},
};
use musivum::protocol::livekit;
use musivum_server::embed::{EmbedOptions, Embedded, twirp_code};
use tokio::sync::broadcast;

use crate::config::Config;

/// The SFU in the process.
pub struct InProcessSfu {
    embedded: Embedded,
    started_at: i64,
}

impl std::fmt::Debug for InProcessSfu {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InProcessSfu").finish_non_exhaustive()
    }
}

/// What starting it gives: the SFU, the routes to serve under the signalling path, and its
/// events (taken before anything can happen, so that none is missed).
pub struct Started {
    pub sfu: Arc<InProcessSfu>,
    pub router: axum::Router,
    pub events: broadcast::Receiver<livekit::WebhookEvent>,
}

impl InProcessSfu {
    /// Starts the SFU with the media and TURN listeners of `[rtc.livekit]`.
    pub async fn start(config: &Config) -> anyhow::Result<Started> {
        let lk = config
            .rtc
            .livekit
            .as_ref()
            .context("[rtc.livekit] is not configured")?;
        anyhow::ensure!(
            lk.url.is_none(),
            "[rtc.livekit] in_process serves the signalling under asaroton's own address: leave url out"
        );
        let yaml = crate::livekit_config::render_in_process(config, Path::new("[rtc.livekit]"))?;
        // Reading the configuration may ask STUN servers for the public address
        // (`sfu.external_ip`), which blocks.
        let conf = tokio::task::spawn_blocking(move || musivum::config::Config::new(&yaml, true))
            .await
            .context("reading the SFU's configuration")?
            .context("the SFU's configuration")?;
        let options = EmbedOptions {
            // asaroton names the port TURN listens on: there is no load balancer on 443.
            advertised_turn_tls_port: lk.turn.as_ref().filter(|t| t.enabled).map(|t| t.tls_port),
            metrics: false,
            ..EmbedOptions::default()
        };
        let embedded = Embedded::start(conf, options)
            .await
            .context("starting the SFU in the process")?;
        let router = embedded.router();
        let events = embedded.events();
        tracing::info!(
            udp = ?lk.sfu.udp_ports,
            tcp = lk.sfu.tcp_port,
            turn_tls = lk.turn.as_ref().filter(|t| t.enabled).map(|t| t.tls_port),
            "SFU started in the process"
        );
        Ok(Started {
            sfu: Arc::new(Self {
                embedded,
                started_at: asaroton_store::now_ts(),
            }),
            router,
            events,
        })
    }
}

/// Hands the SFU's events to the homeserver until the SFU stops.
pub fn forward_events(
    mut events: broadcast::Receiver<livekit::WebhookEvent>,
    services: Arc<Services>,
) {
    tokio::spawn(async move {
        loop {
            match events.recv().await {
                Ok(event) => {
                    let room = event.room.as_ref().map_or("", |r| r.name.as_str());
                    let identity = event
                        .participant
                        .as_ref()
                        .map_or("", |p| p.identity.as_str());
                    tracing::debug!(kind = %event.event, lk_room = room, identity, "SFU event");
                    if let Err(e) = services.rtc_sfu_event(&event.event, room, identity).await {
                        tracing::warn!(error = %e, kind = %event.event, "handling an SFU event failed");
                    }
                }
                // The delegation worker's poll catches what was lost.
                Err(broadcast::error::RecvError::Lagged(lost)) => {
                    tracing::warn!(lost, "SFU events were lost: handling them fell behind");
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    });
}

fn api_error(err: &musivum::service::Error) -> ApiError {
    ApiError::Remote {
        status: 0,
        code: twirp_code(err),
        message: err.to_string(),
    }
}

#[async_trait::async_trait]
impl SfuRooms for InProcessSfu {
    async fn create_room(&self, name: &str) -> Result<(), ApiError> {
        self.embedded
            .create_room(livekit::CreateRoomRequest {
                name: name.to_owned(),
                empty_timeout: 300,
                departure_timeout: 20,
                max_participants: 0,
                ..Default::default()
            })
            .await
            .map(|_| ())
            .map_err(|e| api_error(&e))
    }

    async fn list_participants(&self, room: &str) -> Result<Vec<Participant>, ApiError> {
        match self
            .embedded
            .list_participants(livekit::ListParticipantsRequest {
                room: room.to_owned(),
            })
            .await
        {
            Ok(response) => Ok(response
                .participants
                .into_iter()
                .map(|p| Participant {
                    state: livekit::participant_info::State::try_from(p.state)
                        .map_or("ACTIVE", |s| s.as_str_name())
                        .to_owned(),
                    identity: p.identity,
                })
                .collect()),
            Err(e) => {
                let err = api_error(&e);
                if err.is_not_found() {
                    Ok(Vec::new())
                } else {
                    Err(err)
                }
            }
        }
    }

    async fn remove_participant(&self, room: &str, identity: &str) -> Result<(), ApiError> {
        match self
            .embedded
            .remove_participant(livekit::RoomParticipantIdentity {
                room: room.to_owned(),
                identity: identity.to_owned(),
                ..Default::default()
            })
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => {
                let err = api_error(&e);
                if err.is_not_found() { Ok(()) } else { Err(err) }
            }
        }
    }

    fn started_at(&self) -> Option<i64> {
        Some(self.started_at)
    }
}
