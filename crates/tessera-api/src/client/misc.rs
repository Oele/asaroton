//! Account data, filters, typing, receipts, read markers, presence.

use std::time::Duration;

use ruma::{
    MilliSecondsSinceUnixEpoch, UInt,
    api::client as c,
    api::client::{
        config::{
            get_global_account_data, get_room_account_data, set_global_account_data,
            set_room_account_data,
        },
        filter::{create_filter, get_filter},
        presence::{get_presence, set_presence},
        read_marker::set_read_marker,
        receipt::create_receipt,
        typing::create_typing_event,
        user_directory::search_users,
        voip::get_turn_server_info,
    },
    events::receipt::ReceiptThread,
};
use serde_json::{Value, json};
use tessera_service::Notification;
use tessera_store::Receipt;

use crate::{AppState, Error, Result, Ruma, client::rooms::raw};

pub async fn set_global_account_data(
    services: AppState,
    req: Ruma<set_global_account_data::v3::Request>,
) -> Result<set_global_account_data::v3::Response> {
    let sender = req.sender()?;
    if sender.user_id != req.body.user_id {
        return Err(Error::forbidden("Cannot set another user's account data."));
    }
    let ty = req.body.event_type.to_string();
    if ty == "m.fully_read" || ty == "m.push_rules" {
        return Err(Error::bad_json(format!(
            "Cannot set {ty} through this API."
        )));
    }
    let content: Value = serde_json::from_str(req.body.data.json().get())
        .map_err(|e| Error::bad_json(e.to_string()))?;
    services
        .store
        .set_account_data(&sender.user_id, None, &ty, &content)
        .await?;
    services.notifier.notify(Notification::User {
        user_id: sender.user_id.clone(),
    });
    Ok(set_global_account_data::v3::Response::new())
}

pub async fn set_room_account_data(
    services: AppState,
    req: Ruma<set_room_account_data::v3::Request>,
) -> Result<set_room_account_data::v3::Response> {
    let sender = req.sender()?;
    if sender.user_id != req.body.user_id {
        return Err(Error::forbidden("Cannot set another user's account data."));
    }
    let ty = req.body.event_type.to_string();
    if ty == "m.fully_read" {
        return Err(Error::bad_json("Cannot set m.fully_read through this API."));
    }
    let content: Value = serde_json::from_str(req.body.data.json().get())
        .map_err(|e| Error::bad_json(e.to_string()))?;
    services
        .store
        .set_account_data(&sender.user_id, Some(&req.body.room_id), &ty, &content)
        .await?;
    services.notifier.notify(Notification::User {
        user_id: sender.user_id.clone(),
    });
    Ok(set_room_account_data::v3::Response::new())
}

pub async fn get_global_account_data(
    services: AppState,
    req: Ruma<get_global_account_data::v3::Request>,
) -> Result<get_global_account_data::v3::Response> {
    let sender = req.sender()?;
    if sender.user_id != req.body.user_id {
        return Err(Error::forbidden("Cannot read another user's account data."));
    }
    let ad = services
        .store
        .get_account_data(&sender.user_id, None, &req.body.event_type.to_string())
        .await?
        .ok_or_else(|| Error::not_found("Account data not found."))?;
    Ok(get_global_account_data::v3::Response::new(
        raw(&ad.content)?,
    ))
}

pub async fn get_room_account_data(
    services: AppState,
    req: Ruma<get_room_account_data::v3::Request>,
) -> Result<get_room_account_data::v3::Response> {
    let sender = req.sender()?;
    if sender.user_id != req.body.user_id {
        return Err(Error::forbidden("Cannot read another user's account data."));
    }
    let ad = services
        .store
        .get_account_data(
            &sender.user_id,
            Some(&req.body.room_id),
            &req.body.event_type.to_string(),
        )
        .await?
        .ok_or_else(|| Error::not_found("Account data not found."))?;
    Ok(get_room_account_data::v3::Response::new(raw(&ad.content)?))
}

pub async fn create_filter(
    services: AppState,
    req: Ruma<create_filter::v3::Request>,
) -> Result<create_filter::v3::Response> {
    let sender = req.sender()?;
    if sender.user_id != req.body.user_id {
        return Err(Error::forbidden("Cannot create filters for another user."));
    }
    let json =
        serde_json::to_value(&req.body.filter).map_err(|e| Error::bad_json(e.to_string()))?;
    let id = services.store.create_filter(&sender.user_id, &json).await?;
    Ok(create_filter::v3::Response::new(id.to_string()))
}

pub async fn get_filter(
    services: AppState,
    req: Ruma<get_filter::v3::Request>,
) -> Result<get_filter::v3::Response> {
    let sender = req.sender()?;
    if sender.user_id != req.body.user_id {
        return Err(Error::forbidden("Cannot read another user's filters."));
    }
    let id: i64 = req
        .body
        .filter_id
        .parse()
        .map_err(|_| Error::not_found("Filter not found."))?;
    let json = services
        .store
        .get_filter(&sender.user_id, id)
        .await?
        .ok_or_else(|| Error::not_found("Filter not found."))?;
    let filter = serde_json::from_value(json).map_err(|e| Error::internal(e.to_string()))?;
    Ok(get_filter::v3::Response::new(filter))
}

pub async fn typing(
    services: AppState,
    req: Ruma<create_typing_event::v3::Request>,
) -> Result<create_typing_event::v3::Response> {
    let sender = req.sender()?;
    if sender.user_id != req.body.user_id {
        return Err(Error::forbidden("Cannot set typing for another user."));
    }
    services
        .require_joined(&req.body.room_id, &sender.user_id)
        .await?;
    match &req.body.state {
        create_typing_event::v3::Typing::Yes(info) => {
            services
                .set_typing(&req.body.room_id, &sender.user_id, true, info.timeout)
                .await;
        }
        create_typing_event::v3::Typing::No => {
            services
                .set_typing(&req.body.room_id, &sender.user_id, false, Duration::ZERO)
                .await;
        }
    }
    Ok(create_typing_event::v3::Response::new())
}

async fn store_receipt(
    services: &AppState,
    user_id: &ruma::UserId,
    room_id: &ruma::RoomId,
    receipt_type: &str,
    thread: Option<String>,
    event_id: &ruma::EventId,
) -> Result<()> {
    services
        .store
        .set_receipt(Receipt {
            room_id: room_id.to_owned(),
            user_id: user_id.to_owned(),
            receipt_type: receipt_type.to_owned(),
            thread_id: thread,
            event_id: event_id.to_owned(),
            data: json!({ "ts": tessera_store::now_ts() }),
            stream_id: 0,
        })
        .await?;
    services.notifier.notify(Notification::RoomEphemeral {
        room_id: room_id.to_owned(),
    });
    Ok(())
}

pub async fn receipt(
    services: AppState,
    req: Ruma<create_receipt::v3::Request>,
) -> Result<create_receipt::v3::Response> {
    let sender = req.sender()?;
    services
        .require_joined(&req.body.room_id, &sender.user_id)
        .await?;
    let ty = req.body.receipt_type.to_string();
    let thread = match &req.body.thread {
        ReceiptThread::Unthreaded => None,
        ReceiptThread::Main => Some("main".to_owned()),
        ReceiptThread::Thread(id) => Some(id.to_string()),
        _ => None,
    };
    if ty == "m.fully_read" {
        services
            .store
            .set_account_data(
                &sender.user_id,
                Some(&req.body.room_id),
                "m.fully_read",
                &json!({ "event_id": req.body.event_id }),
            )
            .await?;
        services.notifier.notify(Notification::User {
            user_id: sender.user_id.clone(),
        });
    } else {
        store_receipt(
            &services,
            &sender.user_id,
            &req.body.room_id,
            &ty,
            thread,
            &req.body.event_id,
        )
        .await?;
    }
    Ok(create_receipt::v3::Response::new())
}

pub async fn read_markers(
    services: AppState,
    req: Ruma<set_read_marker::v3::Request>,
) -> Result<set_read_marker::v3::Response> {
    let sender = req.sender()?;
    services
        .require_joined(&req.body.room_id, &sender.user_id)
        .await?;
    if let Some(ev) = &req.body.fully_read {
        services
            .store
            .set_account_data(
                &sender.user_id,
                Some(&req.body.room_id),
                "m.fully_read",
                &json!({ "event_id": ev }),
            )
            .await?;
        services.notifier.notify(Notification::User {
            user_id: sender.user_id.clone(),
        });
    }
    if let Some(ev) = &req.body.read_receipt {
        store_receipt(
            &services,
            &sender.user_id,
            &req.body.room_id,
            "m.read",
            None,
            ev,
        )
        .await?;
    }
    if let Some(ev) = &req.body.private_read_receipt {
        store_receipt(
            &services,
            &sender.user_id,
            &req.body.room_id,
            "m.read.private",
            None,
            ev,
        )
        .await?;
    }
    Ok(set_read_marker::v3::Response::new())
}

pub async fn set_presence(
    services: AppState,
    req: Ruma<set_presence::v3::Request>,
) -> Result<set_presence::v3::Response> {
    let sender = req.sender()?;
    if sender.user_id != req.body.user_id {
        return Err(Error::forbidden("Cannot set presence for another user."));
    }
    services
        .set_presence(
            &sender.user_id,
            req.body.presence.clone(),
            Some(req.body.status_msg.clone()),
        )
        .await;
    Ok(set_presence::v3::Response::new())
}

pub async fn get_presence(
    services: AppState,
    req: Ruma<get_presence::v3::Request>,
) -> Result<get_presence::v3::Response> {
    let sender = req.sender()?;
    let target = &req.body.user_id;
    if *target != sender.user_id {
        // Only users sharing a room may see each other's presence.
        let mine = services
            .store
            .rooms_for_user(&sender.user_id, "join")
            .await?;
        let theirs = services.store.rooms_for_user(target, "join").await?;
        if !mine.iter().any(|r| theirs.contains(r)) {
            return Err(Error::forbidden(
                "You are not allowed to see their presence.",
            ));
        }
    }
    let Some(entry) = services.presence.get(target) else {
        let mut resp = get_presence::v3::Response::new(ruma::presence::PresenceState::Offline);
        resp.currently_active = Some(false);
        return Ok(resp);
    };
    let mut resp = get_presence::v3::Response::new(entry.state.clone());
    resp.status_msg = entry.status_msg;
    resp.currently_active = Some(entry.state == ruma::presence::PresenceState::Online);
    let ago = (tessera_store::now_ts() - entry.last_active_ts).max(0);
    resp.last_active_ago = Some(Duration::from_millis(ago.unsigned_abs()));
    let _ = (MilliSecondsSinceUnixEpoch::now(), UInt::from(0u32));
    Ok(resp)
}

pub async fn search_users(
    services: AppState,
    req: Ruma<search_users::v3::Request>,
) -> Result<search_users::v3::Response> {
    let sender = req.sender()?;
    let term = req.body.search_term.to_lowercase();
    let limit = usize::try_from(u64::from(req.body.limit))
        .unwrap_or(10)
        .clamp(1, 100);
    // Users who share a room with the searcher, plus everyone in public rooms.
    let mut seen = std::collections::BTreeSet::new();
    let mut results = Vec::new();
    let mut rooms = services
        .store
        .rooms_for_user(&sender.user_id, "join")
        .await?;
    for r in services.store.list_public_rooms(200, 0).await? {
        if !rooms.contains(&r.room_id) {
            rooms.push(r.room_id);
        }
    }
    'outer: for room in rooms {
        for m in services.store.members(&room, Some("join")).await? {
            if m.user_id == sender.user_id || !seen.insert(m.user_id.clone()) {
                continue;
            }
            let name = m.display_name.clone().unwrap_or_default().to_lowercase();
            if m.user_id.as_str().to_lowercase().contains(&term) || name.contains(&term) {
                let mut u = search_users::v3::User::new(m.user_id.clone());
                u.display_name = m.display_name;
                u.avatar_url = m.avatar_url.map(ruma::OwnedMxcUri::from);
                results.push(u);
                if results.len() > limit {
                    break 'outer;
                }
            }
        }
    }
    let limited = results.len() > limit;
    results.truncate(limit);
    Ok(search_users::v3::Response::new(results, limited))
}

pub async fn turn_server(
    _services: AppState,
    req: Ruma<get_turn_server_info::v3::Request>,
) -> Result<get_turn_server_info::v3::Response> {
    let _ = req.sender()?;
    // No TURN server configured: the spec says to return an empty uris list.
    Ok(get_turn_server_info::v3::Response::new(
        String::new(),
        String::new(),
        Vec::new(),
        Duration::from_hours(24),
    ))
}

/// `POST /_matrix/client/v1/appservice/{appserviceId}/ping`.
pub async fn appservice_ping(
    services: AppState,
    req: Ruma<ruma::api::client::appservice::request_ping::v1::Request>,
) -> Result<ruma::api::client::appservice::request_ping::v1::Response> {
    let sender = req.sender()?;
    if sender.appservice_id.as_deref() != Some(req.body.appservice_id.as_str()) {
        return Err(Error::forbidden(
            "Only the appservice itself may ping its URL.",
        ));
    }
    let txn = req
        .body
        .transaction_id
        .as_ref()
        .map_or_else(String::new, ToString::to_string);
    let ms = services
        .federation()
        .appservice_ping(&req.body.appservice_id, &txn)
        .await?;
    Ok(
        ruma::api::client::appservice::request_ping::v1::Response::new(
            std::time::Duration::from_millis(ms),
        ),
    )
}

/// `GET /_matrix/client/v3/thirdparty/protocols`.
pub async fn thirdparty_protocols(
    services: AppState,
    req: Ruma<c::thirdparty::get_protocols::v3::Request>,
) -> Result<axum::response::Response> {
    use axum::response::IntoResponse;
    req.sender()?;
    Ok(axum::Json(services.thirdparty_protocols().await?).into_response())
}

/// `GET /_matrix/client/v3/thirdparty/protocol/{protocol}`.
pub async fn thirdparty_protocol(
    services: AppState,
    req: Ruma<c::thirdparty::get_protocol::v3::Request>,
) -> Result<axum::response::Response> {
    use axum::response::IntoResponse;
    req.sender()?;
    Ok(axum::Json(services.thirdparty_protocol(&req.body.protocol).await?).into_response())
}

fn fields_query(fields: &std::collections::BTreeMap<String, String>) -> Vec<(String, String)> {
    fields.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
}

/// `GET /_matrix/client/v3/thirdparty/location/{protocol}`.
pub async fn thirdparty_location(
    services: AppState,
    req: Ruma<c::thirdparty::get_location_for_protocol::v3::Request>,
) -> Result<axum::response::Response> {
    use axum::response::IntoResponse;
    req.sender()?;
    let items = services
        .thirdparty_lookup(
            "location",
            Some(&req.body.protocol),
            &fields_query(&req.body.fields),
        )
        .await?;
    Ok(axum::Json(Value::Array(items)).into_response())
}

/// `GET /_matrix/client/v3/thirdparty/user/{protocol}`.
pub async fn thirdparty_user(
    services: AppState,
    req: Ruma<c::thirdparty::get_user_for_protocol::v3::Request>,
) -> Result<axum::response::Response> {
    use axum::response::IntoResponse;
    req.sender()?;
    let items = services
        .thirdparty_lookup(
            "user",
            Some(&req.body.protocol),
            &fields_query(&req.body.fields),
        )
        .await?;
    Ok(axum::Json(Value::Array(items)).into_response())
}

/// `GET /_matrix/client/v3/thirdparty/location?alias=`.
pub async fn thirdparty_location_by_alias(
    services: AppState,
    req: Ruma<c::thirdparty::get_location_for_room_alias::v3::Request>,
) -> Result<axum::response::Response> {
    use axum::response::IntoResponse;
    req.sender()?;
    let q = vec![("alias".to_owned(), req.body.alias.to_string())];
    let items = services.thirdparty_lookup("location", None, &q).await?;
    Ok(axum::Json(Value::Array(items)).into_response())
}

/// `GET /_matrix/client/v3/thirdparty/user?userid=`.
pub async fn thirdparty_user_by_id(
    services: AppState,
    req: Ruma<c::thirdparty::get_user_for_user_id::v3::Request>,
) -> Result<axum::response::Response> {
    use axum::response::IntoResponse;
    req.sender()?;
    let q = vec![("userid".to_owned(), req.body.userid.to_string())];
    let items = services.thirdparty_lookup("user", None, &q).await?;
    Ok(axum::Json(Value::Array(items)).into_response())
}
