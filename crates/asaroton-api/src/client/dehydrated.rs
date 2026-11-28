//! Dehydrated devices (MSC3814, under its unstable prefix): see
//! `asaroton_service::dehydrated`.

use std::collections::BTreeMap;

use ruma::api::client::dehydrated_device::{
    delete_dehydrated_device, get_dehydrated_device, get_events, put_dehydrated_device,
};
use serde_json::Value;

use crate::{AppState, Error, Result, Ruma, client::keys::to_json};

fn from_json<T>(value: &Value) -> Result<ruma::serde::Raw<T>> {
    serde_json::value::to_raw_value(value)
        .map(ruma::serde::Raw::from_json)
        .map_err(|e| Error::internal(e.to_string()))
}

pub async fn put(
    services: AppState,
    req: Ruma<put_dehydrated_device::msc3814::Request>,
) -> Result<put_dehydrated_device::msc3814::Response> {
    let sender = req.sender()?;
    let keys = |keys: &BTreeMap<
        ruma::OwnedOneTimeKeyId,
        ruma::serde::Raw<ruma::encryption::OneTimeKey>,
    >| {
        keys.iter()
            .map(|(id, key)| Ok((id.to_string(), to_json(key)?)))
            .collect::<Result<BTreeMap<String, Value>>>()
    };
    services
        .put_dehydrated_device(
            &sender.user_id,
            &req.body.device_id,
            req.body.initial_device_display_name.as_deref(),
            &to_json(&req.body.device_data)?,
            to_json(&req.body.device_keys)?,
            &keys(&req.body.one_time_keys)?,
            &keys(&req.body.fallback_keys)?,
        )
        .await?;
    Ok(put_dehydrated_device::msc3814::Response::new(
        req.body.device_id.clone(),
    ))
}

pub async fn get(
    services: AppState,
    req: Ruma<get_dehydrated_device::msc3814::Request>,
) -> Result<get_dehydrated_device::msc3814::Response> {
    let (device_id, data) = services
        .get_dehydrated_device(&req.sender()?.user_id)
        .await?;
    Ok(get_dehydrated_device::msc3814::Response::new(
        device_id,
        from_json(&data)?,
    ))
}

pub async fn delete(
    services: AppState,
    req: Ruma<delete_dehydrated_device::msc3814::Request>,
) -> Result<delete_dehydrated_device::msc3814::Response> {
    let device_id = services
        .delete_dehydrated_device(&req.sender()?.user_id)
        .await?;
    Ok(delete_dehydrated_device::msc3814::Response::new(device_id))
}

pub async fn events(
    services: AppState,
    req: Ruma<get_events::msc3814::Request>,
) -> Result<get_events::msc3814::Response> {
    let (events, next_batch) = services
        .dehydrated_device_events(
            &req.sender()?.user_id,
            &req.body.device_id,
            req.body.next_batch.as_deref(),
        )
        .await?;
    let mut response = get_events::msc3814::Response::new(
        events.iter().map(from_json).collect::<Result<Vec<_>>>()?,
    );
    response.next_batch = Some(next_batch);
    Ok(response)
}
