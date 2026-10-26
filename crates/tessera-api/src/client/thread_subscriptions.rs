//! Thread subscriptions (MSC4306) and their change stream (MSC4308).

use axum::response::{IntoResponse, Response};
use ruma::api::client::threads::{
    get_thread_subscription, get_thread_subscriptions_changes, subscribe_thread, unsubscribe_thread,
};
use serde_json::{Map, Value, json};
use tessera_service::thread_subscriptions::ThreadSubscriptionChanges;

use crate::{AppState, Error, Result, Ruma, extract::add_cors};

pub async fn subscribe(
    services: AppState,
    req: Ruma<subscribe_thread::unstable::Request>,
) -> Result<subscribe_thread::unstable::Response> {
    let sender = req.sender()?;
    services
        .subscribe_thread(
            &sender.user_id,
            &req.body.room_id,
            &req.body.thread_root,
            req.body.automatic.as_deref(),
        )
        .await?;
    Ok(subscribe_thread::unstable::Response::new())
}

pub async fn unsubscribe(
    services: AppState,
    req: Ruma<unsubscribe_thread::unstable::Request>,
) -> Result<unsubscribe_thread::unstable::Response> {
    let sender = req.sender()?;
    services
        .unsubscribe_thread(&sender.user_id, &req.body.room_id, &req.body.thread_root)
        .await?;
    Ok(unsubscribe_thread::unstable::Response::new())
}

pub async fn get(
    services: AppState,
    req: Ruma<get_thread_subscription::unstable::Request>,
) -> Result<get_thread_subscription::unstable::Response> {
    let sender = req.sender()?;
    let automatic = services
        .thread_subscription(&sender.user_id, &req.body.room_id, &req.body.thread_root)
        .await?
        .ok_or_else(|| Error::not_found("Not subscribed to this thread."))?;
    Ok(get_thread_subscription::unstable::Response::new(automatic))
}

/// `GET /_matrix/client/unstable/io.element.msc4308/thread_subscriptions`: pages backwards
/// from `from` (exclusive) down to `to` (exclusive), newest first.
pub async fn changes(
    services: AppState,
    req: Ruma<get_thread_subscriptions_changes::unstable::Request>,
) -> Result<Response> {
    let sender = req.sender()?;
    let parse = |v: &Option<String>| -> Result<Option<i64>> {
        v.as_deref()
            .map(|s| {
                s.parse::<i64>()
                    .map_err(|_| Error::invalid_param("Invalid pagination token."))
            })
            .transpose()
    };
    let below = parse(&req.body.from)?;
    let above = parse(&req.body.to)?;
    let limit = req
        .body
        .limit
        .map_or(100, |l| usize::try_from(u64::from(l)).unwrap_or(100));
    let changes = services
        .thread_subscription_changes(&sender.user_id, above, below, limit, false)
        .await?;
    let mut body = changes_json(&changes);
    if let Some(end) = body.as_object_mut().and_then(|o| o.remove("prev_batch")) {
        body["end"] = end;
    }
    let mut resp = axum::Json(body).into_response();
    add_cors(resp.headers_mut());
    Ok(resp)
}

/// The MSC4308 wire form: `subscribed`, `unsubscribed` (each only when non-empty) and
/// `prev_batch`.
pub(crate) fn changes_json(changes: &ThreadSubscriptionChanges) -> Value {
    let mut out = Map::new();
    if !changes.subscribed.is_empty() {
        let subscribed: Map<String, Value> = changes
            .subscribed
            .iter()
            .map(|(room, threads)| {
                let threads: Map<String, Value> = threads
                    .iter()
                    .map(|(root, (automatic, bump))| {
                        (
                            root.to_string(),
                            json!({ "automatic": automatic, "bump_stamp": bump }),
                        )
                    })
                    .collect();
                (room.to_string(), Value::Object(threads))
            })
            .collect();
        out.insert("subscribed".into(), Value::Object(subscribed));
    }
    if !changes.unsubscribed.is_empty() {
        let unsubscribed: Map<String, Value> = changes
            .unsubscribed
            .iter()
            .map(|(room, threads)| {
                let threads: Map<String, Value> = threads
                    .iter()
                    .map(|(root, bump)| (root.to_string(), json!({ "bump_stamp": bump })))
                    .collect();
                (room.to_string(), Value::Object(threads))
            })
            .collect();
        out.insert("unsubscribed".into(), Value::Object(unsubscribed));
    }
    if let Some(pb) = &changes.prev_batch {
        out.insert("prev_batch".into(), json!(pb));
    }
    Value::Object(out)
}
