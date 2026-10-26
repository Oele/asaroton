//! Business logic for the tessera homeserver.
//!
//! [`Services`] is the single shared handle API handlers receive: configuration, the store,
//! the signing key, the authenticator and the notifier, plus the per-area services built on
//! top of them.

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

pub mod account;
pub mod admin;
pub mod appservice;
pub mod config;
pub mod delayed_events;
pub mod e2ee;
mod error;
pub mod federation_hooks;
pub mod invite_filter;
pub mod media;
pub mod notices;
pub mod notifications;
pub mod notifier;
pub mod presence;
pub mod profile;
pub mod push;
pub mod rooms;
pub mod signing;
pub mod sliding_sync;
pub mod spaces;
pub mod sync;
pub mod thirdparty;
pub mod thread_subscriptions;
pub mod typing;

use std::sync::Arc;

use tessera_auth::{Authenticator, UiaSessions};
use tessera_core::SigningKey;
use tessera_store::DynStore;

pub use config::Config;
pub use error::{Error, Result};
pub use notifier::{Notification, Notifier};

/// Shell-style glob matching (`*` any run, `?` one character), as used by filters and MSC4155.
#[must_use]
pub fn glob_match(pattern: &str, value: &str) -> bool {
    fn rec(p: &[char], v: &[char]) -> bool {
        match (p.first(), v.first()) {
            (None, None) => true,
            (Some('*'), _) => rec(&p[1..], v) || (!v.is_empty() && rec(p, &v[1..])),
            (Some('?'), Some(_)) => rec(&p[1..], &v[1..]),
            (Some(a), Some(b)) if a == b => rec(&p[1..], &v[1..]),
            _ => false,
        }
    }
    let p: Vec<char> = pattern.chars().collect();
    let v: Vec<char> = value.chars().collect();
    rec(&p, &v)
}

/// Everything the server's handlers need, behind one `Arc`.
pub struct Services {
    pub config: Arc<Config>,
    pub store: DynStore,
    pub signing_key: Arc<SigningKey>,
    pub auth: Authenticator,
    pub uia: UiaSessions,
    pub notifier: Notifier,
    pub typing: typing::TypingState,
    pub presence: presence::PresenceMap,
    pub appservices: appservice::AppserviceRegistry,
    /// The authentication service, when authentication is delegated to it.
    pub mas: Option<Arc<tessera_auth::mas::MasClient>>,
    /// Users the authentication service allowed to replace their cross-signing keys without
    /// further interactive auth, and when.
    pub cross_signing_reset_allowed: dashmap::DashMap<ruma::OwnedUserId, std::time::Instant>,
    /// Unread counts computed so far per (room, user).
    pub unread_cache:
        dashmap::DashMap<(ruma::OwnedRoomId, ruma::OwnedUserId), notifications::UnreadCacheEntry>,
    /// Sliding sync connections and what each has been sent.
    pub sliding_sync: sliding_sync::SlidingSyncConnections,
    /// Delayed event scheduling state (MSC4140).
    pub delayed: delayed_events::DelayedEvents,
    /// Parsed push rulesets with their stream id, dropped when the user changes them.
    pub push_rules_cache: dashmap::DashMap<ruma::OwnedUserId, Arc<(ruma::push::Ruleset, i64)>>,
    /// Nonces handed out by the shared-secret registration endpoint.
    admin_nonces: dashmap::DashMap<String, std::time::Instant>,
    federation_hooks: federation_hooks::HooksSlot,
    /// Process start, for uptime and `age` calculations.
    pub started_at: std::time::Instant,
    /// Per-room locks for local sends (see `rooms::pipeline`).
    pub(crate) room_locks: dashmap::DashMap<ruma::OwnedRoomId, Arc<tokio::sync::Mutex<()>>>,
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
        let appservices =
            appservice::AppserviceRegistry::new(&config.appservices, &config.server_name)?;
        let mas = config.mas.as_ref().map(|m| {
            Arc::new(tessera_auth::mas::MasClient::new(
                &m.endpoint,
                m.secret.clone(),
                config.server_name.clone(),
            ))
        });
        let auth = Authenticator::new(Arc::clone(&store))
            .with_appservices(
                appservices
                    .all()
                    .iter()
                    .map(|a| tessera_auth::AppserviceToken {
                        token: a.as_token.clone(),
                        id: a.id.clone(),
                        user_id: a.sender.clone(),
                    })
                    .collect(),
            )
            .with_mas(mas.clone());
        Ok(Arc::new(Self {
            auth,
            mas,
            cross_signing_reset_allowed: dashmap::DashMap::new(),
            unread_cache: dashmap::DashMap::new(),
            appservices,
            uia: UiaSessions::default(),
            notifier: Notifier::new(),
            typing: typing::TypingState::default(),
            presence: presence::PresenceMap::default(),
            sliding_sync: sliding_sync::SlidingSyncConnections::default(),
            delayed: delayed_events::DelayedEvents::default(),
            push_rules_cache: dashmap::DashMap::new(),
            admin_nonces: dashmap::DashMap::new(),
            federation_hooks: federation_hooks::HooksSlot::default(),
            signing_key: Arc::new(signing_key),
            config,
            store,
            started_at: std::time::Instant::now(),
            room_locks: dashmap::DashMap::new(),
        }))
    }

    /// This server's name.
    pub fn server_name(&self) -> &ruma::ServerName {
        &self.config.server_name
    }

    /// Issue a nonce for shared-secret registration (valid for one minute).
    pub fn new_admin_nonce(&self) -> String {
        let now = std::time::Instant::now();
        self.admin_nonces
            .retain(|_, t| now.duration_since(*t) < std::time::Duration::from_secs(60));
        let nonce = tessera_auth::token::generate_session_id();
        self.admin_nonces.insert(nonce.clone(), now);
        nonce
    }

    /// Consume a nonce; false if unknown or expired.
    pub fn take_admin_nonce(&self, nonce: &str) -> bool {
        self.admin_nonces
            .remove(nonce)
            .is_some_and(|(_, t)| t.elapsed() < std::time::Duration::from_secs(60))
    }

    /// Whether a user ID belongs to this server.
    pub fn is_local(&self, user_id: &ruma::UserId) -> bool {
        user_id.server_name() == self.config.server_name
    }
}
