//! Error type for protocol-level operations.

use ruma::{CanonicalJsonError, IdParseError, RoomVersionId};

/// Errors produced by `tessera-core`.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The room version is not one this server implements.
    #[error("unsupported room version {0}")]
    UnsupportedRoomVersion(RoomVersionId),

    /// The PDU is malformed for its room version.
    #[error("invalid PDU: {0}")]
    InvalidPdu(String),

    /// JSON that is not valid canonical JSON (floats, oversized integers, ...).
    #[error("canonical JSON error: {0}")]
    CanonicalJson(#[from] CanonicalJsonError),

    /// A canonical JSON object was missing a field or had a field of the wrong type.
    #[error("canonical JSON field error: {0}")]
    CanonicalJsonField(#[from] ruma::canonical_json::CanonicalJsonFieldError),

    /// JSON (de)serialization failure.
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// Hashing or signing failed.
    #[error("signing error: {0}")]
    Signing(#[from] ruma::signatures::JsonError),

    /// Signature verification failed.
    #[error("signature verification failed: {0}")]
    Verification(#[from] ruma::signatures::VerificationError),

    /// A signing key could not be parsed.
    #[error("invalid signing key: {0}")]
    KeyParse(#[from] ruma::signatures::Ed25519KeyPairParseError),

    /// The event failed the authorization rules.
    #[error("event failed authorization: {0}")]
    Auth(String),

    /// State resolution failed (usually a missing event).
    #[error("state resolution failed: {0}")]
    StateRes(#[from] ruma::state_res::Error),

    /// An identifier (user, room, event, server) was malformed.
    #[error("invalid identifier: {0}")]
    IdParse(#[from] IdParseError),
}

/// Result alias for `tessera-core`.
pub type Result<T, E = Error> = std::result::Result<T, E>;
