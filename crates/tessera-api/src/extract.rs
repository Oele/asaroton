//! Bridging axum and ruma: the [`Ruma<T>`] extractor and the [`RumaResponse<T>`] responder.

use std::any::TypeId;

use axum::{
    body::Bytes,
    extract::{FromRequest, FromRequestParts, RawPathParams, Request},
    response::{IntoResponse, Response},
};
use http::{StatusCode, header};
use ruma::{
    OwnedServerName,
    api::{
        IncomingRequest, IncomingRequestExt, OutgoingResponse, OutgoingResponseExt,
        auth_scheme::{AccessToken, AccessTokenOptional, AppserviceToken, AppserviceTokenOptional},
        error::{DeserializationError, ErrorKind, FromHttpRequestError},
    },
};
use tessera_auth::{AuthError, Sender};

use crate::{AppState, Error};

/// Maximum request body we accept on the client and federation APIs.
pub(crate) const MAX_BODY_BYTES: usize = 64 * 1024 * 1024;

/// A parsed, authenticated request.
#[derive(Debug)]
pub struct Ruma<T> {
    /// The ruma request.
    pub body: T,
    /// The authenticated local user, when the endpoint requires or accepts an access token.
    pub sender: Option<Sender>,
    /// The authenticated remote server, for federation endpoints (filled in by the federation
    /// layer in a later phase).
    pub origin: Option<OwnedServerName>,
    /// The raw JSON body, kept for endpoints that need to re-serialise or sign it.
    pub json: Option<serde_json::Value>,
}

impl<T> Ruma<T> {
    /// The sender, or a `M_MISSING_TOKEN` error for endpoints where it is optional but the
    /// handler needs it after all.
    pub fn sender(&self) -> Result<&Sender, Error> {
        self.sender.as_ref().ok_or_else(Error::missing_token)
    }
}

/// What kind of authentication an endpoint declared.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Auth {
    None,
    Required,
    Optional,
    Appservice,
    AppserviceOptional,
}

fn auth_kind<T: IncomingRequest>() -> Auth
where
    T::Authentication: 'static,
{
    let id = TypeId::of::<T::Authentication>();
    if id == TypeId::of::<AccessToken>() {
        Auth::Required
    } else if id == TypeId::of::<AccessTokenOptional>() {
        Auth::Optional
    } else if id == TypeId::of::<AppserviceToken>() {
        Auth::Appservice
    } else if id == TypeId::of::<AppserviceTokenOptional>() {
        Auth::AppserviceOptional
    } else {
        Auth::None
    }
}

/// Pull the access token out of `Authorization: Bearer` or the legacy `access_token` query
/// parameter.
fn extract_token(req: &Request) -> Option<String> {
    if let Some(v) = req.headers().get(header::AUTHORIZATION)
        && let Ok(s) = v.to_str()
        && let Some(tok) = s.strip_prefix("Bearer ")
    {
        return Some(tok.trim().to_owned());
    }
    let query = req.uri().query()?;
    for pair in query.split('&') {
        if let Some(v) = pair.strip_prefix("access_token=") {
            return percent_decode(v);
        }
    }
    None
}

fn percent_decode(s: &str) -> Option<String> {
    let mut out = Vec::with_capacity(s.len());
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => {
                let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).ok()?;
                out.push(u8::from_str_radix(hex, 16).ok()?);
                i += 3;
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8(out).ok()
}

impl<T> FromRequest<AppState> for Ruma<T>
where
    T: IncomingRequest + Send + 'static,
    T::Authentication: 'static,
{
    type Rejection = Error;

    async fn from_request(req: Request, services: &AppState) -> Result<Self, Self::Rejection> {
        // --- authentication -------------------------------------------------------------
        let auth = auth_kind::<T>();
        let token = extract_token(&req);
        let sender = match (auth, token) {
            (Auth::None, _) => None,
            (Auth::Required | Auth::Appservice, None) => return Err(Error::missing_token()),
            (Auth::Optional | Auth::AppserviceOptional, None) => None,
            (_, Some(token)) => match services.auth.authenticate(&token).await {
                Ok(sender) => Some(sender),
                Err(AuthError::UnknownToken { soft_logout }) => {
                    return Err(Error::unknown_token(soft_logout));
                }
                Err(e) => return Err(e.into()),
            },
        };
        if auth == Auth::Appservice && sender.as_ref().is_none_or(|s| s.appservice_id.is_none()) {
            return Err(Error::forbidden(
                "This endpoint is for application services only.",
            ));
        }

        // --- path args ------------------------------------------------------------------
        let (mut parts, body) = req.into_parts();
        let path_args: Vec<String> =
            match RawPathParams::from_request_parts(&mut parts, services).await {
                Ok(params) => params.iter().map(|(_, v)| v.to_owned()).collect(),
                Err(_) => Vec::new(),
            };
        let path_args: Vec<&str> = path_args.iter().map(String::as_str).collect();

        // --- body -----------------------------------------------------------------------
        let bytes = axum::body::to_bytes(body, MAX_BODY_BYTES)
            .await
            .map_err(|e| {
                Error::matrix(
                    ErrorKind::TooLarge,
                    StatusCode::PAYLOAD_TOO_LARGE,
                    e.to_string(),
                )
            })?;
        // Clients routinely send POST/PUT with no body where the spec body is all-optional.
        let bytes =
            if bytes.is_empty() && matches!(parts.method, http::Method::POST | http::Method::PUT) {
                Bytes::from_static(b"{}")
            } else {
                bytes
            };
        let json = if bytes.is_empty() {
            None
        } else {
            serde_json::from_slice(&bytes).ok()
        };

        let http_req = http::Request::from_parts(parts, bytes.as_ref());
        let body = T::try_from_http_request(http_req, &path_args).map_err(map_request_error)?;

        Ok(Self {
            body,
            sender,
            origin: None,
            json,
        })
    }
}

fn map_request_error(e: FromHttpRequestError) -> Error {
    match e {
        FromHttpRequestError::Deserialization(DeserializationError::Json(err)) => {
            if err.is_syntax() || err.is_eof() {
                Error::not_json(format!("Invalid JSON: {err}"))
            } else {
                Error::bad_json(format!("{err}"))
            }
        }
        FromHttpRequestError::Deserialization(DeserializationError::Ident(err)) => {
            Error::invalid_param(format!("Invalid identifier: {err}"))
        }
        FromHttpRequestError::Deserialization(DeserializationError::Query(err)) => {
            Error::invalid_param(format!("Invalid query: {err}"))
        }
        FromHttpRequestError::Deserialization(err) => Error::bad_json(err.to_string()),
        FromHttpRequestError::MethodMismatch { .. } => Error::matrix(
            ErrorKind::Unrecognized,
            StatusCode::METHOD_NOT_ALLOWED,
            "Unrecognized request",
        ),
        other => Error::bad_json(other.to_string()),
    }
}

/// Wraps a ruma response so it can be returned from an axum handler.
#[derive(Debug)]
pub struct RumaResponse<T>(pub T);

impl<T: OutgoingResponse> IntoResponse for RumaResponse<T> {
    fn into_response(self) -> Response {
        match self.0.try_into_http_response::<Vec<u8>>() {
            Ok(resp) => {
                let (parts, body) = resp.into_parts();
                let mut response =
                    http::Response::from_parts(parts, axum::body::Body::from(Bytes::from(body)));
                add_cors(response.headers_mut());
                response
            }
            Err(e) => Error::internal(format!("failed to serialise response: {e}")).into_response(),
        }
    }
}

/// CORS headers the spec requires on every client API response.
pub(crate) fn add_cors(headers: &mut http::HeaderMap) {
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_ORIGIN,
        http::HeaderValue::from_static("*"),
    );
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_METHODS,
        http::HeaderValue::from_static("GET, POST, PUT, DELETE, OPTIONS"),
    );
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_HEADERS,
        http::HeaderValue::from_static("X-Requested-With, Content-Type, Authorization"),
    );
}
