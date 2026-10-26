//! Service errors, mapped to Matrix error codes by the API layer.

use ruma::api::error::ErrorKind;

/// A failure while handling a request.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A Matrix-level error the client should see: kind, HTTP status and message.
    #[error("{message}")]
    Matrix {
        kind: ErrorKind,
        status: http::StatusCode,
        message: String,
    },
    #[error(transparent)]
    Store(#[from] tessera_store::Error),
    #[error(transparent)]
    Core(#[from] tessera_core::Error),
    #[error(transparent)]
    Auth(#[from] tessera_auth::AuthError),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("internal error: {0}")]
    Internal(String),
    /// A Matrix error whose errcode is outside ruma's set (unstable MSCs).
    #[error("{message}")]
    Custom {
        errcode: String,
        status: http::StatusCode,
        message: String,
    },
    /// An invite the invitee's filter drops silently (MSC4155); callers report success.
    #[error("invite ignored by the invitee")]
    InviteIgnored,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl Error {
    pub fn matrix(kind: ErrorKind, status: http::StatusCode, message: impl Into<String>) -> Self {
        Self::Matrix {
            kind,
            status,
            message: message.into(),
        }
    }
    pub fn forbidden(message: impl Into<String>) -> Self {
        Self::matrix(ErrorKind::Forbidden, http::StatusCode::FORBIDDEN, message)
    }
    pub fn custom(
        errcode: impl Into<String>,
        status: http::StatusCode,
        message: impl Into<String>,
    ) -> Self {
        Self::Custom {
            errcode: errcode.into(),
            status,
            message: message.into(),
        }
    }
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::matrix(ErrorKind::NotFound, http::StatusCode::NOT_FOUND, message)
    }
    pub fn bad_json(message: impl Into<String>) -> Self {
        Self::matrix(ErrorKind::BadJson, http::StatusCode::BAD_REQUEST, message)
    }
    pub fn invalid_param(message: impl Into<String>) -> Self {
        Self::matrix(
            ErrorKind::InvalidParam,
            http::StatusCode::BAD_REQUEST,
            message,
        )
    }
    pub fn unknown(message: impl Into<String>) -> Self {
        Self::matrix(ErrorKind::Unknown, http::StatusCode::BAD_REQUEST, message)
    }
    pub fn unrecognized(message: impl Into<String>) -> Self {
        Self::matrix(
            ErrorKind::Unrecognized,
            http::StatusCode::NOT_FOUND,
            message,
        )
    }
    pub fn user_in_use() -> Self {
        Self::matrix(
            ErrorKind::UserInUse,
            http::StatusCode::BAD_REQUEST,
            "User ID already taken.",
        )
    }
    pub fn invalid_username(message: impl Into<String>) -> Self {
        Self::matrix(
            ErrorKind::InvalidUsername,
            http::StatusCode::BAD_REQUEST,
            message,
        )
    }
    pub fn unsupported_room_version() -> Self {
        Self::matrix(
            ErrorKind::UnsupportedRoomVersion,
            http::StatusCode::BAD_REQUEST,
            "Unsupported room version.",
        )
    }
    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal(message.into())
    }
}
