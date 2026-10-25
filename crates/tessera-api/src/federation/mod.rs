//! Server-Server API handlers.

use axum::{Json, Router, extract::State, response::IntoResponse};
use ruma::{
    OwnedEventId, RoomVersionId, UInt,
    api::federation::{
        authorization::get_event_authorization,
        backfill::get_backfill,
        device::get_devices,
        directory::get_public_rooms,
        discovery::{get_remote_server_keys, get_remote_server_keys_batch, get_server_version},
        event::{get_event, get_missing_events, get_room_state, get_room_state_ids},
        keys::{claim_keys, get_keys},
        membership::{
            create_invite, create_join_event, create_leave_event, prepare_join_event,
            prepare_leave_event,
        },
        query::{get_profile_information, get_room_information},
        transactions::send_transaction_message,
    },
    room_version_rules::RoomVersionRules,
};
use serde_json::{Value, json};
use tessera_core::Pdu;
use tessera_federation::join::PduFederationJson;

use crate::{AppState, Error, Result, Ruma, route};

/// Register every server-server endpoint.
pub fn routes(router: Router<AppState>) -> Router<AppState> {
    let r = router;
    let r = r
        .route("/_matrix/key/v2/server", axum::routing::get(server_keys))
        .route("/_matrix/key/v2/server/", axum::routing::get(server_keys))
        .route(
            "/_matrix/key/v2/server/{key_id}",
            axum::routing::get(server_keys),
        );
    let r = route::<get_server_version::v1::Request, _, _>(r, version);
    let r = route::<get_remote_server_keys::v2::Request, _, _>(r, notary_query_single);
    let r = route::<get_remote_server_keys_batch::v2::Request, _, _>(r, notary_query_batch);
    let r = route::<send_transaction_message::v1::Request, _, _>(r, send_transaction);
    let r = route::<prepare_join_event::v1::Request, _, _>(r, make_join);
    let r = route::<create_join_event::v2::Request, _, _>(r, send_join_v2);
    let r = route::<prepare_leave_event::v1::Request, _, _>(r, make_leave);
    let r = route::<create_leave_event::v2::Request, _, _>(r, send_leave_v2);
    let r = route::<create_invite::v2::Request, _, _>(r, invite_v2);
    let r = route::<get_event::v1::Request, _, _>(r, event);
    let r = route::<get_room_state::v1::Request, _, _>(r, state);
    let r = route::<get_room_state_ids::v1::Request, _, _>(r, state_ids);
    let r = route::<get_backfill::v1::Request, _, _>(r, backfill);
    let r = route::<get_missing_events::v1::Request, _, _>(r, missing_events);
    let r = route::<get_event_authorization::v1::Request, _, _>(r, event_auth);
    let r = route::<get_profile_information::v1::Request, _, _>(r, query_profile);
    let r = route::<get_room_information::v1::Request, _, _>(r, query_directory);
    let r = route::<get_devices::v1::Request, _, _>(r, user_devices);
    let r = route::<get_keys::v1::Request, _, _>(r, user_keys_query);
    let r = route::<claim_keys::v1::Request, _, _>(r, user_keys_claim);
    route::<get_public_rooms::v1::Request, _, _>(r, public_rooms)
}

fn fed_err(e: tessera_federation::Error) -> Error {
    Error::Service(e.into())
}

fn raw_pdu(pdu: &Pdu, rules: &RoomVersionRules) -> Option<Box<serde_json::value::RawValue>> {
    serde_json::value::to_raw_value(&tessera_core::canonical::to_json_value(
        pdu.to_federation_json_with_id(rules),
    ))
    .ok()
}

async fn rules_for(app: &AppState, room_id: &ruma::RoomId) -> Result<RoomVersionRules> {
    let room = app
        .store
        .get_room(room_id)
        .await?
        .ok_or_else(|| Error::not_found("Unknown room."))?;
    Ok(tessera_core::room_version::rules(&room.room_version)?)
}

/// Deny unless `origin` currently has a member in the room.
async fn require_in_room(
    app: &AppState,
    origin: &ruma::ServerName,
    room_id: &ruma::RoomId,
) -> Result<()> {
    let servers = app.store.servers_in_room(room_id).await?;
    if servers.iter().any(|s| s == origin) {
        Ok(())
    } else {
        Err(Error::forbidden("Your server is not in this room."))
    }
}

// ---------------------------------------------------------------- discovery

pub async fn server_keys(State(app): State<AppState>) -> axum::response::Response {
    let Ok(fed) = app.fed() else {
        return Error::not_found("Federation is disabled.").into_response();
    };
    match fed.our_server_keys().await {
        Ok(keys) => Json(tessera_core::canonical::to_json_value(keys)).into_response(),
        Err(e) => fed_err(e).into_response(),
    }
}

pub async fn version(
    _app: AppState,
    _req: Ruma<get_server_version::v1::Request>,
) -> Result<get_server_version::v1::Response> {
    let mut resp = get_server_version::v1::Response::new();
    let mut server = get_server_version::v1::Server::new();
    server.name = Some("tessera".to_owned());
    server.version = Some(env!("CARGO_PKG_VERSION").to_owned());
    resp.server = Some(server);
    Ok(resp)
}

pub async fn notary_query_single(
    app: AppState,
    req: Ruma<get_remote_server_keys::v2::Request>,
) -> Result<get_remote_server_keys::v2::Response> {
    let fed = app.fed()?;
    let min = i64::try_from(u64::from(req.body.minimum_valid_until_ts.0)).unwrap_or(0);
    let keys = fed
        .notary_keys_for(&req.body.server_name, min)
        .await
        .map_err(fed_err)?;
    Ok(get_remote_server_keys::v2::Response::new(
        keys.into_iter()
            .filter_map(|k| {
                serde_json::value::to_raw_value(&tessera_core::canonical::to_json_value(k)).ok()
            })
            .map(ruma::serde::Raw::from_json)
            .collect(),
    ))
}

pub async fn notary_query_batch(
    app: AppState,
    req: Ruma<get_remote_server_keys_batch::v2::Request>,
) -> Result<get_remote_server_keys_batch::v2::Response> {
    let fed = app.fed()?;
    let mut out = Vec::new();
    for (server, criteria) in &req.body.server_keys {
        let min = criteria
            .values()
            .filter_map(|c| c.minimum_valid_until_ts)
            .map(|t| i64::try_from(u64::from(t.0)).unwrap_or(0))
            .max()
            .unwrap_or(0);
        if let Ok(keys) = fed.notary_keys_for(server, min).await {
            for k in keys {
                if let Ok(raw) =
                    serde_json::value::to_raw_value(&tessera_core::canonical::to_json_value(k))
                {
                    out.push(ruma::serde::Raw::from_json(raw));
                }
            }
        }
    }
    Ok(get_remote_server_keys_batch::v2::Response::new(out))
}

// ---------------------------------------------------------------- transactions

pub async fn send_transaction(
    app: AppState,
    req: Ruma<send_transaction_message::v1::Request>,
) -> Result<send_transaction_message::v1::Response> {
    let origin = req.origin()?.clone();
    let fed = app.fed()?;
    fed.handle_transaction(&origin, req.body)
        .await
        .map_err(fed_err)
}

// ---------------------------------------------------------------- membership

pub async fn make_join(
    app: AppState,
    req: Ruma<prepare_join_event::v1::Request>,
) -> Result<prepare_join_event::v1::Response> {
    let origin = req.origin()?.clone();
    let fed = app.fed()?;
    let (version, template) = fed
        .make_join(&origin, &req.body.room_id, &req.body.user_id, &req.body.ver)
        .await
        .map_err(fed_err)?;
    let mut resp = prepare_join_event::v1::Response::new(
        serde_json::value::to_raw_value(&template).map_err(|e| Error::internal(e.to_string()))?,
    );
    resp.room_version = Some(version);
    Ok(resp)
}

pub async fn send_join_v2(
    app: AppState,
    req: Ruma<create_join_event::v2::Request>,
) -> Result<create_join_event::v2::Response> {
    let origin = req.origin()?.clone();
    let fed = app.fed()?;
    let raw = tessera_core::canonical::from_str(req.body.pdu.get())?;
    let room_state = fed
        .send_join(&origin, &req.body.room_id, raw)
        .await
        .map_err(fed_err)?;
    Ok(create_join_event::v2::Response::new(room_state))
}

pub async fn make_leave(
    app: AppState,
    req: Ruma<prepare_leave_event::v1::Request>,
) -> Result<prepare_leave_event::v1::Response> {
    let origin = req.origin()?.clone();
    let fed = app.fed()?;
    let (version, template) = fed
        .make_leave(&origin, &req.body.room_id, &req.body.user_id)
        .await
        .map_err(fed_err)?;
    Ok(prepare_leave_event::v1::Response::new(
        Some(version),
        serde_json::value::to_raw_value(&template).map_err(|e| Error::internal(e.to_string()))?,
    ))
}

pub async fn send_leave_v2(
    app: AppState,
    req: Ruma<create_leave_event::v2::Request>,
) -> Result<create_leave_event::v2::Response> {
    let origin = req.origin()?.clone();
    let fed = app.fed()?;
    let raw = tessera_core::canonical::from_str(req.body.pdu.get())?;
    fed.send_leave(&origin, &req.body.room_id, raw)
        .await
        .map_err(fed_err)?;
    Ok(create_leave_event::v2::Response::new())
}

pub async fn invite_v2(
    app: AppState,
    req: Ruma<create_invite::v2::Request>,
) -> Result<create_invite::v2::Response> {
    let origin = req.origin()?.clone();
    let fed = app.fed()?;
    let raw = tessera_core::canonical::from_str(req.body.event.get())?;
    let stripped: Vec<Value> = req
        .body
        .invite_room_state
        .iter()
        .filter_map(|s| serde_json::to_value(s).ok())
        .collect();
    let signed = fed
        .receive_invite(
            &origin,
            &req.body.room_id,
            &req.body.room_version,
            raw,
            stripped,
        )
        .await
        .map_err(fed_err)?;
    Ok(create_invite::v2::Response::new(
        serde_json::value::to_raw_value(&tessera_core::canonical::to_json_value(signed))
            .map_err(|e| Error::internal(e.to_string()))?,
    ))
}

// ---------------------------------------------------------------- events & state

pub async fn event(
    app: AppState,
    req: Ruma<get_event::v1::Request>,
) -> Result<get_event::v1::Response> {
    let origin = req.origin()?.clone();
    let ev = app
        .store
        .get_event(&req.body.event_id)
        .await?
        .ok_or_else(|| Error::not_found("Event not found."))?;
    require_in_room(&app, &origin, ev.pdu.room_id()).await?;
    let rules = rules_for(&app, ev.pdu.room_id()).await?;
    Ok(get_event::v1::Response::new(
        app.config.server_name.clone(),
        ruma::MilliSecondsSinceUnixEpoch::now(),
        raw_pdu(&ev.pdu, &rules).ok_or_else(|| Error::internal("serialising event"))?,
    ))
}

async fn state_and_auth_at(
    app: &AppState,
    room_id: &ruma::RoomId,
    event_id: &ruma::EventId,
) -> Result<(Vec<OwnedEventId>, Vec<OwnedEventId>)> {
    let ev = app
        .store
        .get_event(event_id)
        .await?
        .ok_or_else(|| Error::not_found("Event not found."))?;
    if ev.pdu.room_id() != room_id {
        return Err(Error::not_found("Event not in room."));
    }
    let Some(group) = ev.state_group else {
        return Err(Error::not_found("State at this event is not known."));
    };
    // State *before* the event: for state events, their group includes themselves.
    let before = if ev.pdu.is_state() {
        app.store.state_group_before(event_id).await?
    } else {
        Some(group)
    };
    let entries = match before {
        Some(g) => app.store.state_at_group(g).await?,
        None => Vec::new(),
    };
    let state_ids: Vec<OwnedEventId> = entries.into_iter().map(|e| e.event_id).collect();
    let mut chain = app.store.auth_chain(&state_ids).await?;
    chain.retain(|id| !state_ids.contains(id));
    Ok((state_ids, chain))
}

pub async fn state(
    app: AppState,
    req: Ruma<get_room_state::v1::Request>,
) -> Result<get_room_state::v1::Response> {
    let origin = req.origin()?.clone();
    require_in_room(&app, &origin, &req.body.room_id).await?;
    let rules = rules_for(&app, &req.body.room_id).await?;
    let (state_ids, chain_ids) =
        state_and_auth_at(&app, &req.body.room_id, &req.body.event_id).await?;
    let pdus = app.store.get_events(&state_ids).await?;
    let chain = app.store.get_events(&chain_ids).await?;
    Ok(get_room_state::v1::Response::new(
        chain
            .iter()
            .filter_map(|e| raw_pdu(&e.pdu, &rules))
            .collect(),
        pdus.iter()
            .filter_map(|e| raw_pdu(&e.pdu, &rules))
            .collect(),
    ))
}

pub async fn state_ids(
    app: AppState,
    req: Ruma<get_room_state_ids::v1::Request>,
) -> Result<get_room_state_ids::v1::Response> {
    let origin = req.origin()?.clone();
    require_in_room(&app, &origin, &req.body.room_id).await?;
    let (state_ids, chain_ids) =
        state_and_auth_at(&app, &req.body.room_id, &req.body.event_id).await?;
    Ok(get_room_state_ids::v1::Response::new(chain_ids, state_ids))
}

pub async fn backfill(
    app: AppState,
    req: Ruma<get_backfill::v1::Request>,
) -> Result<get_backfill::v1::Response> {
    let origin = req.origin()?.clone();
    require_in_room(&app, &origin, &req.body.room_id).await?;
    let rules = rules_for(&app, &req.body.room_id).await?;
    let limit = usize::try_from(u64::from(req.body.limit))
        .unwrap_or(100)
        .clamp(1, 100);
    let events = app
        .store
        .backfill_from(&req.body.room_id, &req.body.v, limit)
        .await?;
    Ok(get_backfill::v1::Response::new(
        app.config.server_name.clone(),
        ruma::MilliSecondsSinceUnixEpoch::now(),
        events
            .iter()
            .filter_map(|e| raw_pdu(&e.pdu, &rules))
            .collect(),
    ))
}

pub async fn missing_events(
    app: AppState,
    req: Ruma<get_missing_events::v1::Request>,
) -> Result<get_missing_events::v1::Response> {
    let origin = req.origin()?.clone();
    require_in_room(&app, &origin, &req.body.room_id).await?;
    let rules = rules_for(&app, &req.body.room_id).await?;
    let limit = usize::try_from(u64::from(req.body.limit))
        .unwrap_or(10)
        .clamp(1, 20);
    let min_depth = i64::try_from(u64::from(req.body.min_depth)).unwrap_or(0);
    let events = app
        .store
        .missing_events_between(
            &req.body.room_id,
            &req.body.earliest_events,
            &req.body.latest_events,
            limit,
            min_depth,
        )
        .await?;
    Ok(get_missing_events::v1::Response::new(
        events
            .iter()
            .filter_map(|e| raw_pdu(&e.pdu, &rules))
            .collect(),
    ))
}

pub async fn event_auth(
    app: AppState,
    req: Ruma<get_event_authorization::v1::Request>,
) -> Result<get_event_authorization::v1::Response> {
    let origin = req.origin()?.clone();
    require_in_room(&app, &origin, &req.body.room_id).await?;
    let rules = rules_for(&app, &req.body.room_id).await?;
    let chain_ids = app
        .store
        .auth_chain(std::slice::from_ref(&req.body.event_id))
        .await?;
    let chain = app.store.get_events(&chain_ids).await?;
    Ok(get_event_authorization::v1::Response::new(
        chain
            .iter()
            .filter_map(|e| raw_pdu(&e.pdu, &rules))
            .collect(),
    ))
}

// ---------------------------------------------------------------- queries

pub async fn query_profile(
    app: AppState,
    req: Ruma<get_profile_information::v1::Request>,
) -> Result<get_profile_information::v1::Response> {
    let _ = req.origin()?;
    if !app.is_local(&req.body.user_id) {
        return Err(Error::not_found("User is not on this server."));
    }
    let user = app
        .store
        .get_user(&req.body.user_id)
        .await?
        .ok_or_else(|| Error::not_found("User not found."))?;
    let mut data = ruma::profile::UserProfile::new();
    if let Some(name) = user.display_name {
        data.set("displayname".to_owned(), json!(name));
    }
    if let Some(url) = user.avatar_url {
        data.set("avatar_url".to_owned(), json!(url));
    }
    let mut resp = get_profile_information::v1::Response::new();
    resp.data = data;
    Ok(resp)
}

pub async fn query_directory(
    app: AppState,
    req: Ruma<get_room_information::v1::Request>,
) -> Result<get_room_information::v1::Response> {
    let _ = req.origin()?;
    if req.body.room_alias.server_name() != app.server_name() {
        return Err(Error::not_found("Alias is not on this server."));
    }
    let alias = app
        .store
        .get_alias(&req.body.room_alias)
        .await?
        .ok_or_else(|| Error::not_found("Room alias not found."))?;
    let servers = app.store.servers_in_room(&alias.room_id).await?;
    Ok(get_room_information::v1::Response::new(
        alias.room_id,
        servers,
    ))
}

pub async fn user_devices(
    app: AppState,
    req: Ruma<get_devices::v1::Request>,
) -> Result<get_devices::v1::Response> {
    let _ = req.origin()?;
    if !app.is_local(&req.body.user_id) {
        return Err(Error::not_found("User is not on this server."));
    }
    let stream = app.store.max_device_list_stream_id().await?;
    let mut resp = get_devices::v1::Response::new(
        req.body.user_id.clone(),
        UInt::try_from(stream.max(0)).unwrap_or(UInt::MAX),
    );
    for (device_id, keys) in app.store.all_device_keys(&req.body.user_id).await? {
        let raw: ruma::serde::Raw<ruma::encryption::DeviceKeys> = crate::client::rooms::raw(&keys)?;
        let mut d = get_devices::v1::UserDevice::new(device_id.clone(), raw);
        d.device_display_name = app
            .store
            .get_device(&req.body.user_id, &device_id)
            .await?
            .and_then(|d| d.display_name);
        resp.devices.push(d);
    }
    if let Some(k) = app
        .store
        .get_cross_signing_key(&req.body.user_id, "master")
        .await?
    {
        resp.master_key = Some(crate::client::rooms::raw(&k)?);
    }
    if let Some(k) = app
        .store
        .get_cross_signing_key(&req.body.user_id, "self_signing")
        .await?
    {
        resp.self_signing_key = Some(crate::client::rooms::raw(&k)?);
    }
    Ok(resp)
}

pub async fn user_keys_query(
    app: AppState,
    req: Ruma<get_keys::v1::Request>,
) -> Result<get_keys::v1::Response> {
    let origin = req.origin()?.clone();
    let result = app
        .query_keys(
            &ruma::UserId::parse(format!("@_:{origin}"))
                .map_err(|e| Error::internal(e.to_string()))?,
            &req.body.device_keys,
        )
        .await?;
    let mut device_keys = std::collections::BTreeMap::new();
    for (user, devices) in result.device_keys {
        let mut per = std::collections::BTreeMap::new();
        for (d, v) in devices {
            per.insert(d, crate::client::rooms::raw(&v)?);
        }
        device_keys.insert(user, per);
    }
    let mut resp = get_keys::v1::Response::new(device_keys);
    for (user, k) in result.master_keys {
        resp.master_keys
            .insert(user, crate::client::rooms::raw(&k)?);
    }
    for (user, k) in result.self_signing_keys {
        resp.self_signing_keys
            .insert(user, crate::client::rooms::raw(&k)?);
    }
    Ok(resp)
}

pub async fn user_keys_claim(
    app: AppState,
    req: Ruma<claim_keys::v1::Request>,
) -> Result<claim_keys::v1::Response> {
    let _ = req.origin()?;
    let wanted: std::collections::BTreeMap<
        ruma::OwnedUserId,
        std::collections::BTreeMap<ruma::OwnedDeviceId, String>,
    > = req
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
    let claimed = app.claim_keys(&wanted).await?;
    let mut out = std::collections::BTreeMap::new();
    for (user, devices) in claimed {
        let mut per_user = std::collections::BTreeMap::new();
        for (device, keys) in devices {
            let mut per_device = std::collections::BTreeMap::new();
            for (id, key) in keys {
                let id: ruma::OwnedOneTimeKeyId = id
                    .parse()
                    .map_err(|_| Error::internal("bad stored key id"))?;
                per_device.insert(id, crate::client::rooms::raw(&key)?);
            }
            per_user.insert(device, per_device);
        }
        out.insert(user, per_user);
    }
    Ok(claim_keys::v1::Response::new(out))
}

pub async fn public_rooms(
    app: AppState,
    req: Ruma<get_public_rooms::v1::Request>,
) -> Result<get_public_rooms::v1::Response> {
    let _ = req.origin()?;
    let limit = req
        .body
        .limit
        .map_or(100, |l| i64::try_from(u64::from(l)).unwrap_or(100))
        .clamp(1, 500);
    let offset: i64 = req
        .body
        .since
        .as_deref()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let chunk = crate::client::rooms::public_rooms_chunk(&app, limit, offset).await?;
    let mut resp = get_public_rooms::v1::Response::new();
    resp.chunk = chunk;
    resp.total_room_count_estimate = Some(
        UInt::try_from(app.store.list_public_rooms(i64::MAX, 0).await?.len()).unwrap_or(UInt::MAX),
    );
    Ok(resp)
}

/// Keep `RoomVersionId` referenced for readers of this module.
#[allow(dead_code)]
fn _versions(_: RoomVersionId) {}
