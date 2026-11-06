//! Route registration driven by ruma endpoint metadata.

use std::{collections::BTreeSet, future::Future};

use axum::{
    Router,
    extract::State,
    response::{IntoResponse, Response},
    routing::{MethodFilter, on},
};
use http::StatusCode;
use ruma::api::{IncomingRequest, OutgoingResponse, path_builder::PathBuilder};

use crate::{AppState, Error, Ruma, RumaResponse, extract::add_cors};

/// Register `handler` under every path (stable and unstable) ruma knows for endpoint `T`.
pub fn route<T, H, Fut>(router: Router<AppState>, handler: H) -> Router<AppState>
where
    T: IncomingRequest + Send + 'static,
    T::Authentication: 'static,
    T::OutgoingResponse: OutgoingResponse + Send,
    H: Fn(AppState, Ruma<T>) -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = Result<T::OutgoingResponse, Error>> + Send + 'static,
{
    route_with_extra_paths::<T, H, Fut>(router, &[], handler)
}

/// Like [`route`], plus additional path templates ruma does not list (for example the
/// state-key-less forms of the `/state/{event_type}` endpoints).
pub fn route_with_extra_paths<T, H, Fut>(
    router: Router<AppState>,
    extra: &[&'static str],
    handler: H,
) -> Router<AppState>
where
    T: IncomingRequest + Send + 'static,
    T::Authentication: 'static,
    T::OutgoingResponse: OutgoingResponse + Send,
    H: Fn(AppState, Ruma<T>) -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = Result<T::OutgoingResponse, Error>> + Send + 'static,
{
    let method = T::METHOD;
    let filter = MethodFilter::try_from(method.clone()).unwrap_or(MethodFilter::GET);
    let mut paths: BTreeSet<&'static str> = T::PATH_BUILDER.all_paths().collect();
    paths.extend(extra.iter().copied());
    let mut router = router;
    for path in paths {
        let handler = handler.clone();
        let h = move |State(state): State<AppState>, req: Ruma<T>| {
            let handler = handler.clone();
            async move { handler(state, req).await.map(RumaResponse) }
        };
        router = router.route(path, on(filter, h));
    }
    router
}

/// Like [`route_with_extra_paths`] for handlers that build their own response.
pub(crate) fn route_raw<T, H, Fut>(
    router: Router<AppState>,
    extra: &[&'static str],
    handler: H,
) -> Router<AppState>
where
    T: IncomingRequest + Send + 'static,
    T::Authentication: 'static,
    H: Fn(AppState, Ruma<T>) -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = Result<Response, Error>> + Send + 'static,
{
    let method = T::METHOD;
    let filter = MethodFilter::try_from(method.clone()).unwrap_or(MethodFilter::GET);
    let mut paths: BTreeSet<&'static str> = T::PATH_BUILDER.all_paths().collect();
    paths.extend(extra.iter().copied());
    let mut router = router;
    for path in paths {
        let handler = handler.clone();
        let h = move |State(state): State<AppState>, req: Ruma<T>| {
            let handler = handler.clone();
            async move {
                match handler(state, req).await {
                    Ok(resp) => resp,
                    Err(e) => e.into_response(),
                }
            }
        };
        router = router.route(path, on(filter, h));
    }
    router
}

/// Assemble the full router: client API, well-known, fallbacks and CORS preflight.
pub fn build_router(state: AppState) -> Router {
    let router = Router::new();
    let router = crate::client::routes(router);
    let router = crate::federation::routes(router);
    router
        .route("/_matrix/client/{*rest}", axum::routing::any(unrecognized))
        .route("/_matrix/{*rest}", axum::routing::any(unrecognized))
        .method_not_allowed_fallback(method_not_allowed)
        .fallback(not_found)
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            crate::ratelimit::middleware,
        ))
        .layer(axum::middleware::from_fn(cors_and_options))
        .with_state(state)
}

/// Logs a request for an endpoint this server does not implement. The watch job counts these
/// lines to see which features real clients and servers ask for; the query string is left out
/// because it can carry tokens.
fn log_unrecognized(method: &axum::http::Method, uri: &axum::http::Uri) {
    tracing::info!(%method, path = uri.path(), "unrecognized endpoint");
}

async fn unrecognized(method: axum::http::Method, uri: axum::http::Uri) -> Response {
    log_unrecognized(&method, &uri);
    Error::unrecognized("Unrecognized request").into_response()
}

async fn method_not_allowed(method: axum::http::Method, uri: axum::http::Uri) -> Response {
    log_unrecognized(&method, &uri);
    Error::matrix(
        ruma::api::error::ErrorKind::Unrecognized,
        StatusCode::METHOD_NOT_ALLOWED,
        "Unrecognized request",
    )
    .into_response()
}

async fn not_found() -> Response {
    (StatusCode::NOT_FOUND, "Not found").into_response()
}

/// Answer CORS preflight requests and stamp CORS headers on every response.
async fn cors_and_options(req: axum::extract::Request, next: axum::middleware::Next) -> Response {
    if req.method() == http::Method::OPTIONS {
        let mut resp = StatusCode::NO_CONTENT.into_response();
        add_cors(resp.headers_mut());
        return resp;
    }
    let mut resp = next.run(req).await;
    add_cors(resp.headers_mut());
    resp
}
