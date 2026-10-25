//! Matrix protocol core for the tessera homeserver: event model, room versions, canonical JSON,
//! hashing and signing, authorization rules and state resolution. No I/O lives here.
//!
//! Most of the heavy lifting is delegated to the `ruma` crates; this crate provides tessera's
//! own [`Pdu`] type, a uniform error type and small, well-tested wrappers.

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

pub mod auth;
pub mod builder;
pub mod canonical;
mod error;
pub mod pdu;
pub mod redaction;
pub mod room_version;
pub mod signing;
pub mod state;

pub use error::{Error, Result};
pub use pdu::Pdu;
pub use room_version::{DEFAULT_ROOM_VERSION, SUPPORTED_ROOM_VERSIONS};
pub use ruma;
pub use signing::SigningKey;
