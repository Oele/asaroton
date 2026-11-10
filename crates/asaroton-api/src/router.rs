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
            state.closing.clone(),
            see_writes_through,
        ))
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

/// A request that writes is seen through to the end, also when the client hangs up halfway.
///
/// The server stops working on a request the moment its connection is gone. For a request
/// that reads, that is right: nobody is waiting for the answer. A request that writes is a
/// row of steps that belong together. An event is stored, the room moves on to it, those who
/// wait for it are woken, it goes out to other servers, and the transaction ID it answers
/// is noted. Stopped after the first of these, the event was there and none of the rest: the
/// client that tried again, as a client does when its connection is lost, got its message a
/// second time, and the next sender built past the event.
///
/// So every request that is not a GET runs in a task of its own, which the connection waits
/// for and does not take down with it. A client that tries again while the first attempt is
/// still running waits its turn where transaction IDs are compared (`Services::txn_turn`),
/// and so does another server with a transaction it sends again.
///
/// The exception is a request that waits for news (see [`waits_for_news`]): it has to end
/// with its connection, or it would go on waiting for a client that has already asked again.
/// That, and every GET, can still be dropped anywhere; what they write has to take that, and
/// where it is more than one statement (history fetched from other servers) it gets a task
/// of its own there.
///
/// Nothing ends such a task early: a request that is seen through takes as long as it takes,
/// which is bounded by the time limits on what it asks of other servers. A server that is
/// told to stop waits for these tasks, for a while (`Closing::writes_done`), which is why
/// each is counted. Behind a reverse proxy that does not pass a client's hanging up on
/// (Apache does not; it only gives up on a request after its own time limit), requests have
/// always been seen through like this.
async fn see_writes_through(
    State(closing): State<asaroton_service::closing::Closing>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    if matches!(*req.method(), http::Method::GET | http::Method::HEAD)
        || waits_for_news(req.uri().path())
    {
        return next.run(req).await;
    }
    let write = closing.write();
    let seen_through = tokio::spawn(async move {
        let _write = write;
        next.run(req).await
    });
    match seen_through.await {
        Ok(response) => response,
        Err(stopped) => match stopped.try_into_panic() {
            // A panic stays what it was: it takes the connection down.
            Ok(panic) => std::panic::resume_unwind(panic),
            Err(stopped) => {
                Error::internal(format!("the request did not finish: {stopped}")).into_response()
            }
        },
    }
}

/// Whether a request that is not a GET waits for news instead of bringing some: sliding sync,
/// which is a POST that stays open until there is something to tell.
fn waits_for_news(path: &str) -> bool {
    path == crate::client::sliding_sync::PATH
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

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use axum::{
        Router,
        body::Body,
        routing::{get, post},
    };
    use tower::Service;

    const WRITE: &str = "/_matrix/client/v3/rooms/!r:example.org/send/m.room.message/1";
    const READ: &str = "/_matrix/client/v3/sync";

    /// Sends a request to a handler that takes a tenth of a second and hangs up long before
    /// that. Returns how many times the handler got to its end.
    async fn hang_up(method: http::Method, path: &'static str, query: &str) -> usize {
        let finished = Arc::new(AtomicUsize::new(0));
        let handler = {
            let finished = Arc::clone(&finished);
            move || {
                let finished = Arc::clone(&finished);
                async move {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    finished.fetch_add(1, Ordering::SeqCst);
                }
            }
        };
        let mut app: Router = Router::new()
            .route(
                path,
                get(handler.clone())
                    .post(handler.clone())
                    .put(handler.clone())
                    .delete(handler),
            )
            .layer(axum::middleware::from_fn_with_state(
                asaroton_service::closing::Closing::new(),
                super::see_writes_through,
            ));
        let request = http::Request::builder()
            .method(method)
            .uri(format!("{path}{query}"))
            .body(Body::empty())
            .unwrap();
        let hung_up = tokio::time::timeout(Duration::from_millis(20), app.call(request)).await;
        assert!(hung_up.is_err(), "the client was gone before the answer");
        tokio::time::sleep(Duration::from_millis(300)).await;
        finished.load(Ordering::SeqCst)
    }

    #[tokio::test]
    async fn a_request_that_writes_is_seen_through_when_its_client_hangs_up() {
        assert_eq!(hang_up(http::Method::PUT, WRITE, "").await, 1);
        assert_eq!(hang_up(http::Method::POST, WRITE, "?ts=1").await, 1);
        assert_eq!(hang_up(http::Method::DELETE, WRITE, "").await, 1);
    }

    #[tokio::test]
    async fn a_request_that_reads_or_waits_for_news_ends_with_its_client() {
        assert_eq!(hang_up(http::Method::GET, READ, "?timeout=30000").await, 0);
        let sliding_sync = crate::client::sliding_sync::PATH;
        assert_eq!(hang_up(http::Method::POST, sliding_sync, "").await, 0);
        assert_eq!(
            hang_up(http::Method::POST, sliding_sync, "?timeout=30000&pos=5").await,
            0
        );
    }

    #[tokio::test]
    async fn a_request_that_is_seen_through_is_answered_as_before() {
        let mut app: Router = Router::new().route(WRITE, post(|| async { "done" })).layer(
            axum::middleware::from_fn_with_state(
                asaroton_service::closing::Closing::new(),
                super::see_writes_through,
            ),
        );
        let request = http::Request::builder()
            .method(http::Method::POST)
            .uri(WRITE)
            .body(Body::empty())
            .unwrap();
        let response = app.call(request).await.unwrap();
        assert_eq!(response.status(), http::StatusCode::OK);
    }
}
