//! E2EE key endpoints and to-device messaging.

use std::collections::BTreeMap;

use ruma::{
    OwnedDeviceId, OwnedUserId, UInt,
    api::client::{
        keys::{
            claim_keys, get_key_changes, get_keys, upload_keys, upload_signatures,
            upload_signing_keys,
        },
        to_device::send_event_to_device,
        uiaa::{AuthFlow, AuthType},
    },
    serde::Raw,
    to_device::DeviceIdOrAllDevices,
};
use serde_json::Value;
use tessera_auth::UiaOutcome;
use tessera_service::sync::SyncToken;

use crate::{AppState, Error, Result, Ruma, client::rooms::raw};

fn to_json<T>(r: &Raw<T>) -> Result<Value> {
    serde_json::from_str(r.json().get()).map_err(|e| Error::bad_json(e.to_string()))
}

fn counts_to_api(counts: BTreeMap<String, u64>) -> BTreeMap<ruma::OneTimeKeyAlgorithm, UInt> {
    counts
        .into_iter()
        .map(|(alg, n)| {
            (
                ruma::OneTimeKeyAlgorithm::from(alg),
                UInt::try_from(n).unwrap_or(UInt::MAX),
            )
        })
        .collect()
}

pub async fn upload(
    services: AppState,
    req: Ruma<upload_keys::v3::Request>,
) -> Result<upload_keys::v3::Response> {
    let sender = req.sender()?;
    let device_id = sender
        .device_id
        .as_ref()
        .ok_or_else(|| Error::forbidden("Key upload requires a device."))?;
    let device_keys = req.body.device_keys.as_ref().map(to_json).transpose()?;
    let otks: BTreeMap<String, Value> = req
        .body
        .one_time_keys
        .iter()
        .map(|(id, k)| Ok((id.to_string(), to_json(k)?)))
        .collect::<Result<_>>()?;
    let fallback: BTreeMap<String, Value> = req
        .body
        .fallback_keys
        .iter()
        .map(|(id, k)| Ok((id.to_string(), to_json(k)?)))
        .collect::<Result<_>>()?;
    let counts = services
        .upload_keys(&sender.user_id, device_id, device_keys, &otks, &fallback)
        .await?;
    Ok(upload_keys::v3::Response::new(counts_to_api(counts)))
}

pub async fn query(
    services: AppState,
    req: Ruma<get_keys::v3::Request>,
) -> Result<get_keys::v3::Response> {
    let sender = req.sender()?;
    let result = services
        .query_keys(&sender.user_id, &req.body.device_keys)
        .await?;
    let mut resp = get_keys::v3::Response::new();
    for (user, devices) in result.device_keys {
        let mut per: BTreeMap<OwnedDeviceId, Raw<ruma::encryption::DeviceKeys>> = BTreeMap::new();
        for (d, v) in devices {
            per.insert(d, raw(&v)?);
        }
        resp.device_keys.insert(user, per);
    }
    for (user, k) in result.master_keys {
        resp.master_keys.insert(user, raw(&k)?);
    }
    for (user, k) in result.self_signing_keys {
        resp.self_signing_keys.insert(user, raw(&k)?);
    }
    for (user, k) in result.user_signing_keys {
        resp.user_signing_keys.insert(user, raw(&k)?);
    }
    Ok(resp)
}

pub async fn claim(
    services: AppState,
    req: Ruma<claim_keys::v3::Request>,
) -> Result<claim_keys::v3::Response> {
    let _ = req.sender()?;
    let wanted: BTreeMap<OwnedUserId, BTreeMap<OwnedDeviceId, String>> = req
        .body
        .one_time_keys
        .iter()
        .map(|(u, devs)| {
            (
                u.clone(),
                devs.iter()
                    .map(|(d, alg)| (d.clone(), alg.to_string()))
                    .collect(),
            )
        })
        .collect();
    let claimed = services.claim_keys(&wanted).await?;
    let mut one_time_keys = BTreeMap::new();
    for (user, devices) in claimed {
        let mut per_user = BTreeMap::new();
        for (device, keys) in devices {
            let mut per_device = BTreeMap::new();
            for (id, key) in keys {
                let id: ruma::OwnedOneTimeKeyId = id
                    .parse()
                    .map_err(|_| Error::internal("bad stored key id"))?;
                per_device.insert(id, raw(&key)?);
            }
            per_user.insert(device, per_device);
        }
        one_time_keys.insert(user, per_user);
    }
    Ok(claim_keys::v3::Response::new(one_time_keys))
}

pub async fn changes(
    services: AppState,
    req: Ruma<get_key_changes::v3::Request>,
) -> Result<get_key_changes::v3::Response> {
    let sender = req.sender()?;
    let from = SyncToken::parse(&req.body.from)
        .ok_or_else(|| Error::invalid_param("Invalid from token."))?;
    let to =
        SyncToken::parse(&req.body.to).ok_or_else(|| Error::invalid_param("Invalid to token."))?;
    let (changed, left) = services
        .device_list_changes_for(&sender.user_id, from.device_lists, to.device_lists)
        .await?;
    Ok(get_key_changes::v3::Response::new(changed, left))
}

pub async fn upload_signing(
    services: AppState,
    req: Ruma<upload_signing_keys::v3::Request>,
) -> Result<upload_signing_keys::v3::Response> {
    let sender = req.sender()?;
    // Replacing existing cross-signing keys requires UIA; the first upload does not.
    let has_existing = services
        .store
        .get_cross_signing_key(&sender.user_id, "master")
        .await?
        .is_some();
    if has_existing {
        let flows = vec![AuthFlow::new(vec![AuthType::Password])];
        let outcome = services
            .uia
            .step(
                flows,
                req.body.auth.as_ref(),
                Some(sender.user_id.clone()),
                None,
                async |stage, data, user| {
                    super::account::check_stage(&services, stage, data, user).await
                },
            )
            .await;
        match outcome {
            UiaOutcome::Complete { .. } => {}
            UiaOutcome::Incomplete(info) => return Err(Error::Uiaa(info)),
            UiaOutcome::Rejected { kind, message } => {
                return Err(Error::matrix(kind, http::StatusCode::FORBIDDEN, message));
            }
        }
    }
    services
        .upload_cross_signing_keys(
            &sender.user_id,
            req.body.master_key.as_ref().map(to_json).transpose()?,
            req.body
                .self_signing_key
                .as_ref()
                .map(to_json)
                .transpose()?,
            req.body
                .user_signing_key
                .as_ref()
                .map(to_json)
                .transpose()?,
        )
        .await?;
    Ok(upload_signing_keys::v3::Response::new())
}

pub async fn upload_sigs(
    services: AppState,
    req: Ruma<upload_signatures::v3::Request>,
) -> Result<upload_signatures::v3::Response> {
    let sender = req.sender()?;
    let mut signed: BTreeMap<OwnedUserId, BTreeMap<String, Value>> = BTreeMap::new();
    for (user, keys) in &req.body.signed_keys {
        let v: Value = serde_json::to_value(keys).map_err(|e| Error::bad_json(e.to_string()))?;
        let Value::Object(map) = v else { continue };
        signed.insert(user.clone(), map.into_iter().collect());
    }
    let failures = services.upload_signatures(&sender.user_id, &signed).await?;
    let mut resp = upload_signatures::v3::Response::new();
    for (user, per_key) in failures {
        let mut m = BTreeMap::new();
        for (k, v) in per_key {
            if let Ok(f) = serde_json::from_value::<upload_signatures::v3::Failure>(v) {
                m.insert(k, f);
            }
        }
        resp.failures.insert(user, m);
    }
    Ok(resp)
}

pub async fn send_to_device(
    services: AppState,
    req: Ruma<send_event_to_device::v3::Request>,
) -> Result<send_event_to_device::v3::Response> {
    let sender = req.sender()?;
    // Transaction-ID de-duplication for to-device sends.
    let txn_key = format!("to_device:{}", req.body.txn_id);
    if services
        .store
        .get_txn_event(&sender.user_id, sender.device_id.as_deref(), &txn_key)
        .await?
        .is_some()
    {
        return Ok(send_event_to_device::v3::Response::new());
    }
    let mut messages: BTreeMap<OwnedUserId, BTreeMap<String, Value>> = BTreeMap::new();
    for (user, per_device) in &req.body.messages {
        let mut m = BTreeMap::new();
        for (dev, content) in per_device {
            let key = match dev {
                DeviceIdOrAllDevices::DeviceId(d) => d.to_string(),
                DeviceIdOrAllDevices::AllDevices => "*".to_owned(),
            };
            m.insert(key, to_json(content)?);
        }
        messages.insert(user.clone(), m);
    }
    services
        .send_to_device(&sender.user_id, &req.body.event_type.to_string(), &messages)
        .await?;
    let marker: ruma::OwnedEventId = "$to_device".try_into().unwrap_or_else(|_| unreachable!());
    services
        .store
        .set_txn_event(
            &sender.user_id,
            sender.device_id.as_deref(),
            &txn_key,
            &marker,
        )
        .await?;
    Ok(send_event_to_device::v3::Response::new())
}
