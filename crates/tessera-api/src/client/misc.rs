//! Account data, filters, typing, receipts, read markers, presence.

use std::time::Duration;

use ruma::{
    MilliSecondsSinceUnixEpoch, UInt,
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
            services.set_typing(&req.body.room_id, &sender.user_id, true, info.timeout);
        }
        create_typing_event::v3::Typing::No => {
            services.set_typing(&req.body.room_id, &sender.user_id, false, Duration::ZERO);
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
    _services: AppState,
    req: Ruma<set_presence::v3::Request>,
) -> Result<set_presence::v3::Response> {
    let sender = req.sender()?;
    if sender.user_id != req.body.user_id {
        return Err(Error::forbidden("Cannot set presence for another user."));
    }
    // Presence is accepted and ignored until the presence stream lands.
    Ok(set_presence::v3::Response::new())
}

pub async fn get_presence(
    _services: AppState,
    req: Ruma<get_presence::v3::Request>,
) -> Result<get_presence::v3::Response> {
    let _ = req.sender()?;
    let mut resp = get_presence::v3::Response::new(ruma::presence::PresenceState::Offline);
    resp.currently_active = Some(false);
    let _ = (MilliSecondsSinceUnixEpoch::now(), UInt::from(0u32));
    Ok(resp)
}
