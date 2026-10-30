//! `MatrixRTC` transports (MSC4143) and the `LiveKit` transport (MSC4195), plus `OpenID` tokens.
//!
//! The `/rtc/livekit/*` prefix doubles as the `livekit_service_url` for clients that still
//! use the standalone-service flow: `get_token` and `delegate_delayed_leave` accept either
//! the homeserver-authenticated body or the legacy one carrying an `OpenID` token, and
//! `sfu/get` is the oldest form.

use axum::{
    body::Bytes,
    extract::{Request, State},
    http::HeaderMap,
    response::{IntoResponse, Response},
};
use ruma::api::client::account::request_openid_token;
use serde_json::{Value, json};
use tessera_service::{
    openid::OPENID_TOKEN_LIFETIME,
    rtc::{
        DelegateRequest, FederationGetTokenRequest, GetTokenRequest, LegacyDelegateRequest,
        LegacyGetTokenRequest, LegacySfuRequest,
    },
};

use crate::{
    AppState, Error, Result, Ruma, client::account::authenticate_headers, extract::add_cors,
};

/// The client-side prefix the `LiveKit` endpoints live under.
pub const PREFIX: &str = "/_matrix/client/unstable/io.element.msc4195/rtc/livekit";

fn json_response(v: Value) -> Response {
    let mut resp = axum::Json(v).into_response();
    add_cors(resp.headers_mut());
    resp
}

fn parse<T: serde::de::DeserializeOwned>(body: &[u8]) -> Result<T> {
    let value: Value =
        serde_json::from_slice(body).map_err(|e| Error::not_json(format!("Invalid JSON: {e}")))?;
    serde_json::from_value(value).map_err(|e| Error::bad_json(e.to_string()))
}

fn is_legacy(body: &[u8]) -> bool {
    serde_json::from_slice::<Value>(body).is_ok_and(|v| v.get("openid_token").is_some())
}

/// `GET /_matrix/client/{v1,unstable/org.matrix.msc4143}/rtc/transports`
pub async fn transports(State(services): State<AppState>, headers: HeaderMap) -> Response {
    let result = async {
        authenticate_headers(&services, &headers).await?;
        if !services.has_rtc() {
            return Err(Error::unrecognized("This server offers no RTC transports."));
        }
        let transports = services.rtc_transports();
        Ok::<_, Error>(json!({ "rtc_transports": transports, "transports": transports }))
    }
    .await;
    match result {
        Ok(v) => json_response(v),
        Err(e) => e.into_response(),
    }
}

/// `POST {PREFIX}/get_token`, homeserver-authenticated or legacy.
pub async fn get_token(
    State(services): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let result = async {
        if is_legacy(&body) {
            let req: LegacyGetTokenRequest = parse(&body)?;
            let token = services.rtc_legacy_get_token(req).await?;
            return Ok(json!({ "url": token.url, "jwt": token.jwt }));
        }
        let sender = authenticate_headers(&services, &headers).await?;
        let req: GetTokenRequest = parse(&body)?;
        let jwt = services.rtc_get_token(&sender.user_id, req).await?;
        Ok::<_, Error>(json!({ "jwt": jwt }))
    }
    .await;
    match result {
        Ok(v) => json_response(v),
        Err(e) => e.into_response(),
    }
}

/// `POST {PREFIX}/sfu/get`, the oldest legacy form.
pub async fn sfu_get(State(services): State<AppState>, body: Bytes) -> Response {
    let result = async {
        let req: LegacySfuRequest = parse(&body)?;
        let token = services.rtc_legacy_sfu_get(req).await?;
        Ok::<_, Error>(json!({ "url": token.url, "jwt": token.jwt }))
    }
    .await;
    match result {
        Ok(v) => json_response(v),
        Err(e) => e.into_response(),
    }
}

/// `POST {PREFIX}/delegate_delayed_leave`, homeserver-authenticated or legacy.
pub async fn delegate_delayed_leave(
    State(services): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let result = async {
        if is_legacy(&body) {
            let req: LegacyDelegateRequest = parse(&body)?;
            services.rtc_legacy_delegate(req).await?;
            return Ok(json!({}));
        }
        let sender = authenticate_headers(&services, &headers).await?;
        let req: DelegateRequest = parse(&body)?;
        services
            .rtc_delegate_delayed_leave(&sender.user_id, req)
            .await?;
        Ok::<_, Error>(json!({}))
    }
    .await;
    match result {
        Ok(v) => json_response(v),
        Err(e) => e.into_response(),
    }
}

/// `POST {PREFIX}/sfu_webhook`: `LiveKit`'s webhook receiver.
pub async fn sfu_webhook(
    State(services): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let auth = headers
        .get(http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok());
    match services.rtc_webhook(auth, &body).await {
        Ok(()) => json_response(json!({})),
        Err(e) => Error::from(e).into_response(),
    }
}

/// `POST /_matrix/client/v3/user/{userId}/openid/request_token`
pub async fn request_openid_token(
    services: AppState,
    req: Ruma<request_openid_token::v3::Request>,
) -> Result<request_openid_token::v3::Response> {
    let sender = req.sender()?;
    if sender.user_id != req.body.user_id {
        return Err(Error::forbidden("Cannot request a token for another user."));
    }
    let token = services.create_openid_token(&sender.user_id).await?;
    Ok(request_openid_token::v3::Response::new(
        token,
        ruma::authentication::TokenType::Bearer,
        services.config.server_name.clone(),
        OPENID_TOKEN_LIFETIME,
    ))
}

/// `POST /_matrix/federation/{v1,unstable/io.element.msc4195}/rtc/livekit/get_token`
pub async fn federation_get_token(State(app): State<AppState>, request: Request) -> Response {
    let result = async {
        let (parts, body) = request.into_parts();
        let bytes = axum::body::to_bytes(body, crate::extract::MAX_BODY_BYTES)
            .await
            .map_err(|e| Error::bad_json(e.to_string()))?;
        let http_req = http::Request::from_parts(parts, bytes.as_ref());
        let origin = app
            .fed()?
            .authenticate_request(&http_req)
            .await
            .map_err(|e| Error::unauthorized(e.to_string()))?;
        let req: FederationGetTokenRequest = parse(&bytes)?;
        let jwt = app.rtc_federation_get_token(&origin, req).await?;
        Ok::<_, Error>(json!({ "jwt": jwt }))
    }
    .await;
    match result {
        Ok(v) => json_response(v),
        Err(e) => e.into_response(),
    }
}
