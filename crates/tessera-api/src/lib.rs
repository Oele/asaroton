//! HTTP API surface for the tessera homeserver.
//!
//! Handlers are written against `ruma` request/response types. [`Ruma<T>`] is an axum
//! extractor that parses the incoming request into `T` and performs the authentication the
//! endpoint declares; [`route`] registers a handler under every path `ruma` knows for it.

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

pub mod client;
mod error;
mod extract;
mod router;

use std::sync::Arc;

use tessera_service::Services;

pub use error::{Error, Result};
pub use extract::{Ruma, RumaResponse};
pub use router::{build_router, route};

/// Shared application state for all handlers.
pub type AppState = Arc<Services>;
