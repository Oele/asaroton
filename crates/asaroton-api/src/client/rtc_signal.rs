//! `LiveKit` signalling through asaroton: `GET {SIGNAL_PATH}/rtc` (WebSocket) and
//! `GET {SIGNAL_PATH}/rtc/validate` are forwarded to the SFU's own listener, so the SFU can
//! stay on the loopback interface and asaroton can correct the TURN port in its answers
//! (see `asaroton_service::rtc::signal`).

use asaroton_service::rtc::signal::rewrite_turn_port;
use axum::{
    extract::{
        RawQuery, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::StatusCode,
    response::{IntoResponse, Response},
};
use futures::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite;

use crate::AppState;

fn upstream(app: &AppState) -> Option<(String, Option<u16>)> {
    let settings = app.services.config.rtc.as_ref()?;
    Some((settings.api_url.clone(), settings.advertised_turn_port))
}

/// `GET .../signal/rtc/validate`: the SFU's token check, which `LiveKit` clients call to get a
/// readable error when the WebSocket connection is refused.
pub async fn validate(State(app): State<AppState>, RawQuery(query): RawQuery) -> Response {
    let Some((api_url, _)) = upstream(&app) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let url = format!(
        "{api_url}/rtc/validate{}",
        query.map_or(String::new(), |q| format!("?{q}"))
    );
    match reqwest::Client::new().get(&url).send().await {
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

/// `GET .../signal/rtc`: the signalling WebSocket, relayed to the SFU.
pub async fn websocket(
    State(app): State<AppState>,
    RawQuery(query): RawQuery,
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
        "{ws_base}/rtc{}",
        query.map_or(String::new(), |q| format!("?{q}"))
    );
    ws.on_upgrade(move |socket| relay(socket, url, turn_port))
}

async fn relay(client: WebSocket, upstream_url: String, turn_port: Option<u16>) {
    let (sfu, _) = match tokio_tungstenite::connect_async(&upstream_url).await {
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
    }
}
