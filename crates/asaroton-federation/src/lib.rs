//! Federation for the asaroton homeserver.
//!
//! * [`discovery`]: resolving server names to hosts (well-known, SRV).
//! * [`client`]: signed outbound requests.
//! * [`keys`]: server signing keys, ours and cached remote ones.
//! * [`inbound`]: verifying `X-Matrix` authentication on incoming requests.
//! * [`receive`]: the incoming PDU/EDU pipeline (transactions, joins, invites).
//! * [`join`]: joining and leaving remote rooms.
//! * [`sender`]: the per-destination outbound queue.

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

pub mod appservice;
pub mod backfill;
pub mod client;
pub mod discovery;
mod error;
pub mod hooks;
mod housekeeping;
pub mod inbound;
pub mod join;
pub mod keys;
pub mod outbound;
pub mod push;
pub mod receive;
mod recheck;
pub mod sender;
pub mod url_preview;

use std::sync::Arc;

use asaroton_service::Services;

pub use error::{Error, Result};

/// Federation configuration beyond what the services already know.
#[derive(Clone, Debug, Default)]
pub struct FederationConfig {
    /// Extra CA certificates (PEM) to trust for outbound TLS, e.g. a test CA.
    pub extra_ca_certs_pem: Vec<Vec<u8>>,
    /// Servers reached over plain HTTP (tests only).
    pub insecure_http_servers: Vec<ruma::OwnedServerName>,
    /// Skip TLS verification entirely (tests only).
    pub danger_accept_invalid_certs: bool,
    /// Ceiling on transactions per minute that carry only EDUs, over all destinations. Zero
    /// selects the default.
    pub max_edu_transactions_per_minute: u32,
    /// Address ranges (CIDR) that URL previews, push gateways and media redirects may reach
    /// although they are not on the public internet.
    pub allowed_internal_ranges: Vec<String>,
    /// Seconds before an event that could not be checked for want of its auth events is tried
    /// again for the first time (the waits double from there). Zero selects the default.
    pub unchecked_event_retry_secs: u32,
}

/// Say so when an auth chain we are about to hand to another server is not complete: events
/// that the chain names and this server does not hold are simply absent from the answer, and
/// the other side then cannot check what cites them.
pub fn warn_if_chain_incomplete(
    room_id: &ruma::RoomId,
    request: &str,
    wanted: &[ruma::OwnedEventId],
    found: &[asaroton_store::StoredEvent],
) {
    if found.len() >= wanted.len() {
        return;
    }
    let example = wanted
        .iter()
        .find(|id| !found.iter().any(|e| e.pdu.event_id() == *id));
    tracing::warn!(
        %room_id, request, missing = wanted.len() - found.len(), of = wanted.len(), example = ?example,
        "the auth chain handed to another server is incomplete: this server does not hold all of its events"
    );
}

/// The federation subsystem handle.
pub struct Federation {
    pub services: Arc<Services>,
    pub client: client::FederationClient,
    pub discovery: discovery::Discovery,
    /// The client for URLs that somebody else chose (previews, push gateways, media redirects).
    pub outbound: outbound::Outbound,
    pub config: FederationConfig,
    /// Rooms whose join we are currently performing, to avoid duplicate remote joins.
    joins_in_progress: dashmap::DashMap<ruma::OwnedRoomId, ()>,
    sender: sender::SenderHandle,
    /// Recently seen events, so auth checks and state resolution can look events up
    /// synchronously. Bounded by periodic clearing.
    event_cache: dashmap::DashMap<ruma::OwnedEventId, asaroton_core::Pdu>,
    /// `/state_ids` answers fetched while processing events, keyed by event: the state at a
    /// gap is needed more than once while the events around it are processed.
    state_at_cache: dashmap::DashMap<
        ruma::OwnedEventId,
        (asaroton_core::state::StateMap, Vec<ruma::OwnedEventId>),
    >,
    /// Auth events a server could not give us, and when that was last tried.
    auth_misses: dashmap::DashMap<(ruma::OwnedServerName, ruma::OwnedEventId), std::time::Instant>,
    /// Events refused since the last hourly report because they were for a room this server
    /// is not in: how many per room, and a server that sent them.
    foreign_room_events: dashmap::DashMap<ruma::OwnedRoomId, (u64, ruma::OwnedServerName)>,
    /// Signing keys that could not be obtained, and when that was last tried.
    key_misses: dashmap::DashMap<(ruma::OwnedServerName, String), std::time::Instant>,
    /// Weak handle to ourselves, for code paths that need an `Arc<Federation>`.
    weak_self: std::sync::OnceLock<std::sync::Weak<Federation>>,
}

impl std::fmt::Debug for Federation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Federation")
    }
}

impl Federation {
    /// Build the federation subsystem and start the outbound sender.
    pub fn new(services: Arc<Services>, config: FederationConfig) -> Result<Arc<Self>> {
        let discovery = discovery::Discovery::new(Arc::clone(&services), &config)?;
        let client =
            client::FederationClient::new(Arc::clone(&services), discovery.clone(), &config)?;
        let sender = sender::SenderHandle::new(config.max_edu_transactions_per_minute);
        let outbound = outbound::Outbound::new(&config.allowed_internal_ranges)?;
        let fed = Arc::new(Self {
            services,
            client,
            discovery,
            outbound,
            config,
            joins_in_progress: dashmap::DashMap::new(),
            sender,
            event_cache: dashmap::DashMap::new(),
            state_at_cache: dashmap::DashMap::new(),
            auth_misses: dashmap::DashMap::new(),
            foreign_room_events: dashmap::DashMap::new(),
            key_misses: dashmap::DashMap::new(),
            weak_self: std::sync::OnceLock::new(),
        });
        let _ = fed.weak_self.set(Arc::downgrade(&fed));
        sender::start(Arc::clone(&fed));
        housekeeping::start(Arc::clone(&fed.services));
        recheck::start(&fed);
        appservice::start(&fed);
        push::start(&fed);
        // Keep the event cache bounded.
        let cache_owner = Arc::clone(&fed);
        tokio::spawn(async move {
            let mut rounds = 0u32;
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(300)).await;
                rounds += 1;
                if rounds.is_multiple_of(12) {
                    cache_owner.report_events_for_foreign_rooms();
                }
                if cache_owner.event_cache.len() > 50_000 {
                    cache_owner.event_cache.clear();
                }
                cache_owner.state_at_cache.clear();
                cache_owner
                    .auth_misses
                    .retain(|_, at| at.elapsed() < receive::AUTH_MISS_TTL);
            }
        });
        Ok(fed)
    }

    /// A strong handle to this federation instance.
    pub fn self_arc(&self) -> Arc<Self> {
        self.weak_self
            .get()
            .and_then(std::sync::Weak::upgrade)
            .unwrap_or_else(|| unreachable!("federation dropped while in use"))
    }

    /// Our own server name.
    pub fn server_name(&self) -> &ruma::ServerName {
        &self.services.config.server_name
    }
}
