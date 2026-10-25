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
    let method = T::METHOD;
    let filter = MethodFilter::try_from(method.clone()).unwrap_or(MethodFilter::GET);
    let paths: BTreeSet<&'static str> = T::PATH_BUILDER.all_paths().collect();
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

/// Assemble the full router: client API, well-known, fallbacks and CORS preflight.
pub fn build_router(state: AppState) -> Router {
    let router = Router::new();
    let router = crate::client::routes(router);
    router
        .route("/_matrix/client/{*rest}", axum::routing::any(unrecognized))
        .route("/_matrix/{*rest}", axum::routing::any(unrecognized))
        .method_not_allowed_fallback(method_not_allowed)
        .fallback(not_found)
        .layer(axum::middleware::from_fn(cors_and_options))
        .with_state(state)
}

async fn unrecognized() -> Response {
    Error::unrecognized("Unrecognized request").into_response()
}

async fn method_not_allowed() -> Response {
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
