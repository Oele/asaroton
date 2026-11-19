//! HTTP API surface for the asaroton homeserver.
//!
//! Handlers are written against `ruma` request/response types. [`Ruma<T>`] is an axum
//! extractor that parses the incoming request into `T` and performs the authentication the
//! endpoint declares; [`route`] registers a handler under every path `ruma` knows for it.

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

pub mod client;
mod error;
mod extract;
pub mod federation;
pub mod ratelimit;
mod router;

use std::sync::Arc;

use asaroton_federation::Federation;
use asaroton_service::Services;

pub use error::{Error, Result};
pub use extract::{Ruma, RumaResponse};
pub use router::{build_router, route, route_with_extra_paths};

/// Everything handlers can reach. Derefs to [`Services`] so client handlers use it directly.
pub struct App {
    pub services: Arc<Services>,
    /// `None` when federation is disabled.
    pub federation: Option<Arc<Federation>>,
    /// Request rate limiting state.
    pub limiter: ratelimit::Limiter,
    /// The signalling and room API of an SFU that runs in this process. When set it is served
    /// under the signalling path itself, in place of the relay to an external SFU.
    pub sfu_router: Option<axum::Router>,
    /// The rendezvous of sign-in with a QR code, from the authorization server in this process
    /// (`[oauth]`): full `/_matrix/client/...` paths, merged beside the client API so that its
    /// layers (CORS among them) cover them.
    pub oauth_client_router: Option<axum::Router>,
}

impl std::fmt::Debug for App {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("App")
            .field("federation", &self.federation.is_some())
            .finish_non_exhaustive()
    }
}

impl std::ops::Deref for App {
    type Target = Services;
    fn deref(&self) -> &Services {
        &self.services
    }
}

impl App {
    /// The federation handle, or a Matrix error when federation is disabled.
    pub fn fed(&self) -> Result<&Arc<Federation>> {
        self.federation
            .as_ref()
            .ok_or_else(|| Error::forbidden("Federation is disabled on this server."))
    }
}

/// Shared application state for all handlers.
pub type AppState = Arc<App>;
