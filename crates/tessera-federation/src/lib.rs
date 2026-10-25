//! Federation for the tessera homeserver.
//!
//! * [`discovery`]: resolving server names to hosts (well-known, SRV).
//! * [`client`]: signed outbound requests.
//! * [`keys`]: server signing keys, ours and cached remote ones.
//! * [`inbound`]: verifying `X-Matrix` authentication on incoming requests.
//! * [`receive`]: the incoming PDU/EDU pipeline (transactions, joins, invites).
//! * [`join`]: joining and leaving remote rooms.
//! * [`sender`]: the per-destination outbound queue.

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

pub mod client;
pub mod discovery;
mod error;
pub mod hooks;
pub mod inbound;
pub mod join;
pub mod keys;
pub mod receive;
pub mod sender;

use std::sync::Arc;

use tessera_service::Services;

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
}

/// The federation subsystem handle.
pub struct Federation {
    pub services: Arc<Services>,
    pub client: client::FederationClient,
    pub discovery: discovery::Discovery,
    pub config: FederationConfig,
    /// Rooms whose join we are currently performing, to avoid duplicate remote joins.
    joins_in_progress: dashmap::DashMap<ruma::OwnedRoomId, ()>,
    sender: sender::SenderHandle,
    /// Recently seen events, so auth checks and state resolution can look events up
    /// synchronously. Bounded by periodic clearing.
    event_cache: dashmap::DashMap<ruma::OwnedEventId, tessera_core::Pdu>,
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
        let fed = Arc::new(Self {
            services,
            client,
            discovery,
            config,
            joins_in_progress: dashmap::DashMap::new(),
            sender: sender::SenderHandle::default(),
            event_cache: dashmap::DashMap::new(),
            weak_self: std::sync::OnceLock::new(),
        });
        let _ = fed.weak_self.set(Arc::downgrade(&fed));
        sender::start(Arc::clone(&fed));
        // Keep the event cache bounded.
        let cache_owner = Arc::clone(&fed);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(300)).await;
                if cache_owner.event_cache.len() > 50_000 {
                    cache_owner.event_cache.clear();
                }
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
