//! Federation errors.

use ruma::OwnedServerName;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("could not resolve server {0}: {1}")]
    Discovery(OwnedServerName, String),
    #[error("request to {0} failed: {1}")]
    Transport(OwnedServerName, String),
    #[error("{server} answered {status}: {body}")]
    RemoteError {
        server: OwnedServerName,
        status: http::StatusCode,
        body: String,
    },
    #[error("no signing keys available for {0}")]
    NoKeys(OwnedServerName),
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error("authentication failed: {0}")]
    Unauthorized(String),
    #[error("event rejected: {0}")]
    Rejected(String),
    #[error(transparent)]
    Service(#[from] tessera_service::Error),
    #[error(transparent)]
    Core(#[from] tessera_core::Error),
    #[error(transparent)]
    Store(#[from] tessera_store::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// An errcode outside the `M_` namespace in a remote error body, kept verbatim for the client.
fn custom_errcode(body: &str) -> Option<String> {
    let code = serde_json::from_str::<serde_json::Value>(body)
        .ok()?
        .get("errcode")?
        .as_str()?
        .to_owned();
    (!code.starts_with("M_")).then_some(code)
}

impl From<Error> for tessera_service::Error {
    fn from(e: Error) -> Self {
        use ruma::api::error::ErrorKind;
        match e {
            Error::Service(s) => s,
            Error::Core(c) => c.into(),
            Error::Store(s) => s.into(),
            Error::Json(j) => j.into(),
            Error::RemoteError {
                server,
                status,
                body,
            } => {
                // Pass through Matrix errors from the remote where sensible.
                let (kind, message) = serde_json::from_str::<serde_json::Value>(&body)
                    .ok()
                    .and_then(|v| {
                        let code = v.get("errcode")?.as_str()?.to_owned();
                        let msg = v
                            .get("error")
                            .and_then(|m| m.as_str())
                            .unwrap_or("")
                            .to_owned();
                        Some((code, msg))
                    })
                    .map_or(
                        (ErrorKind::Unknown, format!("{server} answered {status}")),
                        |(code, msg)| {
                            let kind = match code.as_str() {
                                "M_FORBIDDEN" => ErrorKind::Forbidden,
                                "M_NOT_FOUND" => ErrorKind::NotFound,
                                "M_UNSUPPORTED_ROOM_VERSION" | "M_INCOMPATIBLE_ROOM_VERSION" => {
                                    ErrorKind::UnsupportedRoomVersion
                                }
                                "M_MISSING_PARAM" => ErrorKind::MissingParam,
                                "M_INVALID_PARAM" => ErrorKind::InvalidParam,
                                "M_BAD_JSON" => ErrorKind::BadJson,
                                "M_UNABLE_TO_GRANT_JOIN" => ErrorKind::UnableToGrantJoin,
                                "M_UNABLE_TO_AUTHORISE_JOIN" => ErrorKind::UnableToAuthorizeJoin,
                                _ => ErrorKind::Unknown,
                            };
                            (kind, format!("{server}: {msg}"))
                        },
                    );
                let status = if status.is_client_error() {
                    status
                } else {
                    http::StatusCode::BAD_GATEWAY
                };
                if kind == ErrorKind::Unknown
                    && let Some(code) = custom_errcode(&body)
                {
                    return tessera_service::Error::custom(code, status, message);
                }
                tessera_service::Error::matrix(kind, status, message)
            }
            Error::Discovery(s, m) | Error::Transport(s, m) => tessera_service::Error::matrix(
                ErrorKind::Unknown,
                http::StatusCode::BAD_GATEWAY,
                format!("{s}: {m}"),
            ),
            Error::NoKeys(s) => tessera_service::Error::matrix(
                ErrorKind::Unknown,
                http::StatusCode::BAD_GATEWAY,
                format!("no keys for {s}"),
            ),
            Error::Unauthorized(m) => tessera_service::Error::matrix(
                ErrorKind::Unauthorized,
                http::StatusCode::UNAUTHORIZED,
                m,
            ),
            Error::Rejected(m) => {
                tessera_service::Error::matrix(ErrorKind::Forbidden, http::StatusCode::FORBIDDEN, m)
            }
            Error::Protocol(m) => {
                tessera_service::Error::matrix(ErrorKind::Unknown, http::StatusCode::BAD_GATEWAY, m)
            }
        }
    }
}
