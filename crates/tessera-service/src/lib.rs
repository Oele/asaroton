//! Business logic for the tessera homeserver.
//!
//! [`Services`] is the single shared handle API handlers receive: configuration, the store,
//! the signing key, the authenticator and the notifier, plus the per-area services built on
//! top of them.

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

pub mod account;
pub mod config;
mod error;
pub mod notifier;
pub mod profile;
pub mod signing;

use std::sync::Arc;

use tessera_auth::{Authenticator, UiaSessions};
use tessera_core::SigningKey;
use tessera_store::DynStore;

pub use config::Config;
pub use error::{Error, Result};
pub use notifier::{Notification, Notifier};

/// Everything the server's handlers need, behind one `Arc`.
pub struct Services {
    pub config: Arc<Config>,
    pub store: DynStore,
    pub signing_key: Arc<SigningKey>,
    pub auth: Authenticator,
    pub uia: UiaSessions,
    pub notifier: Notifier,
    /// Process start, for uptime and `age` calculations.
    pub started_at: std::time::Instant,
}

impl std::fmt::Debug for Services {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Services")
            .field("server_name", &self.config.server_name)
            .finish_non_exhaustive()
    }
}

impl Services {
    /// Wire up the services. The store must already be migrated.
    pub async fn new(config: Config, store: DynStore) -> Result<Arc<Self>> {
        let signing_key = signing::load_or_create_signing_key(&*store).await?;
        let config = Arc::new(config);
        Ok(Arc::new(Self {
            auth: Authenticator::new(Arc::clone(&store)),
            uia: UiaSessions::default(),
            notifier: Notifier::new(),
            signing_key: Arc::new(signing_key),
            config,
            store,
            started_at: std::time::Instant::now(),
        }))
    }

    /// This server's name.
    pub fn server_name(&self) -> &ruma::ServerName {
        &self.config.server_name
    }

    /// Whether a user ID belongs to this server.
    pub fn is_local(&self, user_id: &ruma::UserId) -> bool {
        user_id.server_name() == self.config.server_name
    }
}
