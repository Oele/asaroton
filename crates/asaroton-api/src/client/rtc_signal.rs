//! `LiveKit` signalling through asaroton: `GET {SIGNAL_PATH}/rtc` (WebSocket) and
//! `GET {SIGNAL_PATH}/rtc/validate` are forwarded to the SFU's own listener, so the SFU can
//! stay on the loopback interface and asaroton can correct the TURN port in its answers
//! (see `asaroton_service::rtc::signal`).
//!
//! The same goes for `{SIGNAL_PATH}/rtc/v1` and `{SIGNAL_PATH}/rtc/v1/validate`, which
//! clients try first since the SFU got a second way to open the connection (the join request
//! travels in one parameter). The SFU answers both with the same messages, so the correction
//! of the TURN port applies to both. An SFU from before `/rtc/v1` answers 404 itself, and the
//! client falls back to `/rtc`.

use asaroton_service::rtc::signal::{SIGNAL_PATH, rewrite_turn_port};
use axum::{
    extract::{
        RawQuery, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::{HeaderMap, HeaderValue, StatusCode, Uri, header::AUTHORIZATION},
    response::{IntoResponse, Response},
};
use futures::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::{self, client::IntoClientRequest};

use crate::AppState;

fn upstream(app: &AppState) -> Option<(String, Option<u16>)> {
    let settings = app.services.config.rtc.as_ref()?;
    Some((settings.api_url.clone(), settings.advertised_turn_port))
}

/// The SFU's path for a request to the proxy: what follows the proxy's own prefix. Only the
/// paths registered for these handlers get here.
fn sfu_path(uri: &Uri) -> &str {
    uri.path().strip_prefix(SIGNAL_PATH).unwrap_or("/rtc")
}

/// The SFU takes a client's token from the `access_token` parameter or from the
/// `Authorization` header. Browsers cannot set a header on a WebSocket and use the parameter;
/// `LiveKit`'s native SDKs (Rust, and Android and iOS on top of it) send the header. The
/// parameter travels with the query; this is the header, to be passed on as it came.
fn token_header(headers: &HeaderMap) -> Option<HeaderValue> {
    headers.get(AUTHORIZATION).cloned()
}

/// The request that opens the signalling connection to the SFU.
fn sfu_request(
    url: &str,
    authorization: Option<HeaderValue>,
) -> tungstenite::Result<tungstenite::handshake::client::Request> {
    let mut request = url.into_client_request()?;
    if let Some(authorization) = authorization {
        request.headers_mut().insert(AUTHORIZATION, authorization);
    }
    Ok(request)
}

/// `GET .../signal/rtc/validate` and `.../signal/rtc/v1/validate`: the SFU's token check,
/// which `LiveKit` clients call to get a readable error when the WebSocket connection is
/// refused.
pub async fn validate(
    State(app): State<AppState>,
    uri: Uri,
    RawQuery(query): RawQuery,
    headers: HeaderMap,
) -> Response {
    let Some((api_url, _)) = upstream(&app) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let url = format!(
        "{api_url}{}{}",
        sfu_path(&uri),
        query.map_or(String::new(), |q| format!("?{q}"))
    );
    let mut request = reqwest::Client::new().get(&url);
    if let Some(authorization) = token_header(&headers) {
        request = request.header(AUTHORIZATION, authorization);
    }
    match request.send().await {
        Ok(resp) => {
            let status =
                StatusCode::from_u16(resp.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
            let body = resp.text().await.unwrap_or_default();
            (status, body).into_response()
        }
        Err(e) => {
            tracing::warn!(error = %e, "SFU validate request failed");
            StatusCode::BAD_GATEWAY.into_response()
        }
    }
}

/// `GET .../signal/rtc` and `.../signal/rtc/v1`: the signalling WebSocket, relayed to the SFU.
pub async fn websocket(
    State(app): State<AppState>,
    uri: Uri,
    RawQuery(query): RawQuery,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    let Some((api_url, turn_port)) = upstream(&app) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let ws_base = api_url
        .strip_prefix("https://")
        .map(|rest| format!("wss://{rest}"))
        .or_else(|| {
            api_url
                .strip_prefix("http://")
                .map(|rest| format!("ws://{rest}"))
        })
        .unwrap_or(api_url);
    let url = format!(
        "{ws_base}{}{}",
        sfu_path(&uri),
        query.map_or(String::new(), |q| format!("?{q}"))
    );
    let closing = app.closing.clone();
    let authorization = token_header(&headers);
    ws.on_upgrade(move |socket| relay(socket, url, authorization, turn_port, closing))
}

async fn relay(
    client: WebSocket,
    upstream_url: String,
    authorization: Option<HeaderValue>,
    turn_port: Option<u16>,
    closing: asaroton_service::closing::Closing,
) {
    let connecting = async {
        tokio_tungstenite::connect_async(sfu_request(&upstream_url, authorization)?).await
    };
    let (sfu, _) = match connecting.await {
        Ok(conn) => conn,
        Err(e) => {
            tracing::warn!(error = %e, "could not open the SFU signalling connection");
            let mut client = client;
            let _ = client.close().await;
            return;
        }
    };
    let (mut to_client, mut from_client) = client.split();
    let (mut to_sfu, mut from_sfu) = sfu.split();

    let client_to_sfu = async {
        while let Some(Ok(msg)) = from_client.next().await {
            let out = match msg {
                Message::Binary(b) => tungstenite::Message::Binary(b),
                Message::Text(t) => tungstenite::Message::Text(t.as_str().into()),
                Message::Ping(p) => tungstenite::Message::Ping(p),
                Message::Pong(p) => tungstenite::Message::Pong(p),
                Message::Close(_) => break,
            };
            if to_sfu.send(out).await.is_err() {
                break;
            }
        }
        let _ = to_sfu.close().await;
    };
    let sfu_to_client = async {
        while let Some(Ok(msg)) = from_sfu.next().await {
            let out = match msg {
                tungstenite::Message::Binary(b) => Message::Binary(match turn_port {
                    Some(port) => rewrite_turn_port(&b, port).map_or(b, Into::into),
                    None => b,
                }),
                tungstenite::Message::Text(t) => Message::Text(t.as_str().into()),
                tungstenite::Message::Ping(p) => Message::Ping(p),
                tungstenite::Message::Pong(p) => Message::Pong(p),
                tungstenite::Message::Close(_) => break,
                tungstenite::Message::Frame(_) => continue,
            };
            if to_client.send(out).await.is_err() {
                break;
            }
        }
        let _ = to_client.close().await;
    };
    tokio::select! {
        () = client_to_sfu => {}
        () = sfu_to_client => {}
        // A server that stops lets go of the connection, so that the client opens its next
        // one to the server that follows; the call itself runs between client and SFU.
        () = closing.begun() => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_token_in_the_authorization_header_goes_on_to_the_sfu() {
        let mut headers = HeaderMap::new();
        assert!(token_header(&headers).is_none());
        let none = sfu_request("ws://127.0.0.1:7880/rtc?access_token=x", None).unwrap();
        assert!(none.headers().get(AUTHORIZATION).is_none());
        assert_eq!(none.uri().query(), Some("access_token=x"));

        headers.insert(AUTHORIZATION, HeaderValue::from_static("Bearer a.b.c"));
        headers.insert("cookie", HeaderValue::from_static("not for the SFU"));
        let with = sfu_request("ws://127.0.0.1:7880/rtc/v1", token_header(&headers)).unwrap();
        assert_eq!(with.headers().get(AUTHORIZATION).unwrap(), "Bearer a.b.c");
        assert!(with.headers().get("cookie").is_none());
    }

    /// The relay, with a WebSocket server in the SFU's place that says what it was sent.
    #[tokio::test]
    #[allow(clippy::result_large_err)] // the shape of the callback is the library's
    async fn the_sfu_sees_the_header_on_the_signalling_connection() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let sfu = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut seen = None;
            let callback =
                |request: &tungstenite::handshake::server::Request,
                 response: tungstenite::handshake::server::Response| {
                    seen = request.headers().get(AUTHORIZATION).cloned();
                    Ok(response)
                };
            let _socket = tokio_tungstenite::accept_hdr_async(stream, callback)
                .await
                .unwrap();
            seen
        });
        let request = sfu_request(
            &format!("ws://{address}/rtc"),
            Some(HeaderValue::from_static("Bearer a.b.c")),
        )
        .unwrap();
        let _connection = tokio_tungstenite::connect_async(request).await.unwrap();
        assert_eq!(sfu.await.unwrap().unwrap(), "Bearer a.b.c");
    }

    #[test]
    fn the_path_for_the_sfu_is_what_follows_the_prefix() {
        for path in ["/rtc", "/rtc/validate", "/rtc/v1", "/rtc/v1/validate"] {
            let uri: Uri = format!("{SIGNAL_PATH}{path}?access_token=x")
                .parse()
                .unwrap();
            assert_eq!(sfu_path(&uri), path);
        }
    }
}
