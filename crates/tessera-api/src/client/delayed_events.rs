//! Delayed events (MSC4140).

use axum::response::{IntoResponse, Response};
use ruma::api::client::{
    delayed_events::get_all_delayed_events, message::send_message_event, state::send_state_event,
};
use serde_json::{Value, json};
use tessera_service::delayed_events::UpdateAction;

use crate::{AppState, Error, Result, Ruma, RumaResponse, client::rooms, extract::add_cors};

/// The query parameter that turns a normal send into a delayed one.
const DELAY_PARAM: &str = "org.matrix.msc4140.delay";

fn json_response(v: Value) -> Response {
    let mut resp = axum::Json(v).into_response();
    add_cors(resp.headers_mut());
    resp
}

/// The delay requested through `?org.matrix.msc4140.delay=`, if present.
fn delay_param<T>(req: &Ruma<T>) -> Result<Option<i64>> {
    let Some(raw) = req.query.as_deref().and_then(|q| {
        q.split('&')
            .find_map(|kv| kv.strip_prefix(DELAY_PARAM)?.strip_prefix('='))
    }) else {
        return Ok(None);
    };
    raw.parse::<i64>().map(Some).map_err(|_| {
        Error::invalid_param(format!(
            "{DELAY_PARAM} must be an integer number of milliseconds."
        ))
    })
}

/// `PUT /rooms/{roomId}/send/{eventType}/{txnId}`, delayed when the query asks for it.
pub async fn send_message(
    services: AppState,
    req: Ruma<send_message_event::v3::Request>,
) -> Result<Response> {
    let Some(delay_ms) = delay_param(&req)? else {
        return Ok(RumaResponse(rooms::send_message(services, req).await?).into_response());
    };
    let sender = req.sender()?;
    let content: Value = serde_json::from_str(req.body.body.json().get())
        .map_err(|e| Error::bad_json(e.to_string()))?;
    let delay_id = services
        .schedule_delayed_event(
            &sender.user_id,
            sender.device_id.as_deref(),
            &req.body.room_id,
            &req.body.event_type.to_string(),
            None,
            content,
            delay_ms,
            Some(req.body.txn_id.as_str()),
        )
        .await?;
    Ok(json_response(json!({ "delay_id": delay_id })))
}

/// `PUT /rooms/{roomId}/state/{eventType}/{stateKey}`, delayed when the query asks for it.
pub async fn send_state(
    services: AppState,
    req: Ruma<send_state_event::v3::Request>,
) -> Result<Response> {
    let Some(delay_ms) = delay_param(&req)? else {
        return Ok(RumaResponse(rooms::send_state(services, req).await?).into_response());
    };
    let sender = req.sender()?;
    let content: Value = serde_json::from_str(req.body.body.json().get())
        .map_err(|e| Error::bad_json(e.to_string()))?;
    let delay_id = services
        .schedule_delayed_event(
            &sender.user_id,
            sender.device_id.as_deref(),
            &req.body.room_id,
            &req.body.event_type.to_string(),
            Some(req.body.state_key.clone()),
            content,
            delay_ms,
            None,
        )
        .await?;
    Ok(json_response(json!({ "delay_id": delay_id })))
}

/// `PUT /_matrix/client/unstable/org.matrix.msc4140/rooms/{roomId}/delayed_event/{type}/{txnId}`
///
/// The body carries `delay_ms` (as Complement sends it) or `delay`, optional `state_key`,
/// and `content`.
pub async fn schedule(
    axum::extract::State(services): axum::extract::State<AppState>,
    headers: axum::http::HeaderMap,
    axum::extract::Path((room_id, event_type, txn_id)): axum::extract::Path<(
        String,
        String,
        String,
    )>,
    body: axum::body::Bytes,
) -> Response {
    match schedule_inner(&services, &headers, &room_id, &event_type, &txn_id, &body).await {
        Ok(delay_id) => json_response(json!({ "delay_id": delay_id })),
        Err(e) => e.into_response(),
    }
}

async fn schedule_inner(
    services: &AppState,
    headers: &axum::http::HeaderMap,
    room_id: &str,
    event_type: &str,
    txn_id: &str,
    body: &[u8],
) -> Result<String> {
    let sender = super::account::authenticate_headers(services, headers).await?;
    let room_id = ruma::OwnedRoomId::try_from(room_id)
        .map_err(|_| Error::invalid_param("Invalid room ID."))?;
    let body: Value = serde_json::from_slice(body).map_err(|e| Error::bad_json(e.to_string()))?;
    let delay = body
        .get("delay_ms")
        .or_else(|| body.get("delay"))
        .ok_or_else(|| Error::bad_json("Missing delay_ms."))?;
    let delay_ms = delay
        .as_i64()
        .ok_or_else(|| Error::bad_json("delay_ms must be an integer number of milliseconds."))?;
    let state_key = match body.get("state_key") {
        None | Some(Value::Null) => None,
        Some(Value::String(s)) => Some(s.clone()),
        Some(_) => return Err(Error::bad_json("state_key must be a string.")),
    };
    let content = body
        .get("content")
        .cloned()
        .ok_or_else(|| Error::bad_json("Missing content."))?;
    Ok(services
        .schedule_delayed_event(
            &sender.user_id,
            sender.device_id.as_deref(),
            &room_id,
            event_type,
            state_key,
            content,
            delay_ms,
            Some(txn_id),
        )
        .await?)
}

/// `GET /_matrix/client/unstable/org.matrix.msc4140/delayed_events`
pub async fn list(
    services: AppState,
    req: Ruma<get_all_delayed_events::unstable::Request>,
) -> Result<Response> {
    let sender = req.sender()?;
    let delayed_events = services.delayed_events_for(&sender.user_id).await?;
    Ok(json_response(json!({ "delayed_events": delayed_events })))
}

/// `POST /_matrix/client/unstable/org.matrix.msc4140/delayed_events/{delayId}` with the
/// action in the body.
pub async fn update_with_body(
    axum::extract::State(services): axum::extract::State<AppState>,
    headers: axum::http::HeaderMap,
    axum::extract::Path(delay_id): axum::extract::Path<String>,
    body: axum::body::Bytes,
) -> Response {
    let action = serde_json::from_slice::<Value>(&body)
        .ok()
        .and_then(|v| v.get("action")?.as_str()?.parse::<UpdateAction>().ok());
    match action {
        Some(action) => update(&services, &headers, &delay_id, action).await,
        None => Error::invalid_param("Missing or unknown action.").into_response(),
    }
}

/// `POST /_matrix/client/unstable/org.matrix.msc4140/delayed_events/{delayId}/{action}`
pub async fn update_with_path(
    axum::extract::State(services): axum::extract::State<AppState>,
    headers: axum::http::HeaderMap,
    axum::extract::Path((delay_id, action)): axum::extract::Path<(String, String)>,
) -> Response {
    match action.parse::<UpdateAction>() {
        Ok(action) => update(&services, &headers, &delay_id, action).await,
        Err(()) => Error::unrecognized("Unknown delayed event action.").into_response(),
    }
}

async fn update(
    services: &AppState,
    headers: &axum::http::HeaderMap,
    delay_id: &str,
    action: UpdateAction,
) -> Response {
    let result = async {
        let sender = super::account::authenticate_headers(services, headers).await?;
        services
            .update_delayed_event(&sender.user_id, delay_id, action)
            .await?;
        Ok::<_, Error>(())
    }
    .await;
    match result {
        Ok(()) => json_response(json!({})),
        Err(e) => e.into_response(),
    }
}
