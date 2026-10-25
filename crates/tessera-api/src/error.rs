//! API error type: everything a handler can fail with, rendered as a Matrix error response.

use axum::response::{IntoResponse, Response};
use http::StatusCode;
use ruma::api::{
    OutgoingResponseExt,
    client::uiaa::{UiaaInfo, UiaaResponse},
    error::{Error as MatrixError, ErrorBody, ErrorKind, StandardErrorBody},
};

/// A request failure.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A Matrix error with a code and status.
    #[error("{1}")]
    Matrix(ErrorKind, String, StatusCode),
    /// User-Interactive Authentication is required (401 with flows).
    #[error("user-interactive authentication required")]
    Uiaa(Box<UiaaInfo>),
    #[error(transparent)]
    Service(#[from] tessera_service::Error),
    #[error("internal error: {0}")]
    Internal(String),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl Error {
    pub fn matrix(kind: ErrorKind, status: StatusCode, message: impl Into<String>) -> Self {
        Self::Matrix(kind, message.into(), status)
    }
    pub fn forbidden(message: impl Into<String>) -> Self {
        Self::matrix(ErrorKind::Forbidden, StatusCode::FORBIDDEN, message)
    }
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::matrix(ErrorKind::NotFound, StatusCode::NOT_FOUND, message)
    }
    pub fn unrecognized(message: impl Into<String>) -> Self {
        Self::matrix(ErrorKind::Unrecognized, StatusCode::NOT_FOUND, message)
    }
    pub fn bad_json(message: impl Into<String>) -> Self {
        Self::matrix(ErrorKind::BadJson, StatusCode::BAD_REQUEST, message)
    }
    pub fn not_json(message: impl Into<String>) -> Self {
        Self::matrix(ErrorKind::NotJson, StatusCode::BAD_REQUEST, message)
    }
    pub fn invalid_param(message: impl Into<String>) -> Self {
        Self::matrix(ErrorKind::InvalidParam, StatusCode::BAD_REQUEST, message)
    }
    pub fn missing_param(message: impl Into<String>) -> Self {
        Self::matrix(ErrorKind::MissingParam, StatusCode::BAD_REQUEST, message)
    }
    pub fn missing_token() -> Self {
        Self::matrix(
            ErrorKind::MissingToken,
            StatusCode::UNAUTHORIZED,
            "Missing access token.",
        )
    }
    pub fn unknown_token(soft_logout: bool) -> Self {
        Self::matrix(
            ErrorKind::UnknownToken(unknown_token_data(soft_logout)),
            StatusCode::UNAUTHORIZED,
            "Unrecognised access token.",
        )
    }
    pub fn unauthorized(message: impl Into<String>) -> Self {
        Self::matrix(ErrorKind::Unauthorized, StatusCode::UNAUTHORIZED, message)
    }
    pub fn unknown(message: impl Into<String>) -> Self {
        Self::matrix(ErrorKind::Unknown, StatusCode::BAD_REQUEST, message)
    }
    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal(message.into())
    }

    /// The HTTP status this error renders with.
    pub fn status(&self) -> StatusCode {
        match self {
            Self::Matrix(_, _, s) => *s,
            Self::Uiaa(_) => StatusCode::UNAUTHORIZED,
            Self::Service(e) => service_status(e),
            Self::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

fn service_status(e: &tessera_service::Error) -> StatusCode {
    use tessera_service::Error as S;
    match e {
        S::Matrix { status, .. } => *status,
        S::Auth(
            tessera_auth::AuthError::UnknownToken { .. } | tessera_auth::AuthError::Expired,
        ) => StatusCode::UNAUTHORIZED,
        S::Auth(tessera_auth::AuthError::Deactivated) => StatusCode::FORBIDDEN,
        S::Store(tessera_store::Error::NotFound(_)) => StatusCode::NOT_FOUND,
        S::Core(
            tessera_core::Error::UnsupportedRoomVersion(_)
            | tessera_core::Error::CanonicalJson(_)
            | tessera_core::Error::CanonicalJsonField(_),
        ) => StatusCode::BAD_REQUEST,
        S::Json(_) => StatusCode::BAD_REQUEST,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

impl From<tessera_store::Error> for Error {
    fn from(e: tessera_store::Error) -> Self {
        Self::Service(e.into())
    }
}

impl From<tessera_core::Error> for Error {
    fn from(e: tessera_core::Error) -> Self {
        Self::Service(e.into())
    }
}

impl From<tessera_auth::AuthError> for Error {
    fn from(e: tessera_auth::AuthError) -> Self {
        Self::Service(e.into())
    }
}

impl From<UiaaInfo> for Error {
    fn from(info: UiaaInfo) -> Self {
        Self::Uiaa(Box::new(info))
    }
}

impl Error {
    fn into_matrix(self) -> std::result::Result<MatrixError, UiaaInfo> {
        use tessera_service::Error as S;
        let (kind, message, status) = match self {
            Self::Uiaa(info) => return Err(*info),
            Self::Matrix(kind, message, status) => (kind, message, status),
            Self::Service(S::Matrix {
                kind,
                status,
                message,
            }) => (kind, message, status),
            Self::Service(S::Auth(tessera_auth::AuthError::UnknownToken { soft_logout })) => (
                ErrorKind::UnknownToken(unknown_token_data(soft_logout)),
                "Unrecognised access token.".to_owned(),
                StatusCode::UNAUTHORIZED,
            ),
            Self::Service(S::Auth(tessera_auth::AuthError::Expired)) => (
                ErrorKind::UnknownToken(unknown_token_data(true)),
                "Access token expired.".to_owned(),
                StatusCode::UNAUTHORIZED,
            ),
            Self::Service(S::Auth(tessera_auth::AuthError::Deactivated)) => (
                ErrorKind::UserDeactivated,
                "This account has been deactivated.".to_owned(),
                StatusCode::FORBIDDEN,
            ),
            Self::Service(S::Store(tessera_store::Error::NotFound(what))) => (
                ErrorKind::NotFound,
                format!("{what} not found"),
                StatusCode::NOT_FOUND,
            ),
            Self::Service(S::Core(tessera_core::Error::UnsupportedRoomVersion(_))) => (
                ErrorKind::UnsupportedRoomVersion,
                "Unsupported room version.".to_owned(),
                StatusCode::BAD_REQUEST,
            ),
            Self::Service(S::Json(e)) => {
                (ErrorKind::BadJson, e.to_string(), StatusCode::BAD_REQUEST)
            }
            Self::Service(S::Core(
                e @ (tessera_core::Error::CanonicalJson(_)
                | tessera_core::Error::CanonicalJsonField(_)),
            )) => (ErrorKind::BadJson, e.to_string(), StatusCode::BAD_REQUEST),
            Self::Service(e) => {
                tracing::error!(error = %e, "internal error");
                (
                    ErrorKind::Unknown,
                    "Internal server error".to_owned(),
                    StatusCode::INTERNAL_SERVER_ERROR,
                )
            }
            Self::Internal(msg) => {
                tracing::error!(error = %msg, "internal error");
                (
                    ErrorKind::Unknown,
                    "Internal server error".to_owned(),
                    StatusCode::INTERNAL_SERVER_ERROR,
                )
            }
        };
        Ok(MatrixError::new(
            status,
            ErrorBody::Standard(StandardErrorBody::new(kind, message)),
        ))
    }
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let response = match self.into_matrix() {
            Ok(err) => UiaaResponse::MatrixError(err),
            Err(info) => UiaaResponse::AuthResponse(info),
        };
        match response.try_into_http_response::<Vec<u8>>() {
            Ok(resp) => resp.map(axum::body::Body::from).into_response(),
            Err(e) => {
                tracing::error!(error = %e, "failed to serialise error response");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    r#"{"errcode":"M_UNKNOWN","error":"Internal server error"}"#,
                )
                    .into_response()
            }
        }
    }
}

fn unknown_token_data(soft_logout: bool) -> ruma::api::error::UnknownTokenErrorData {
    let mut data = ruma::api::error::UnknownTokenErrorData::new();
    data.soft_logout = soft_logout;
    data
}
