//! Server-side key backups (`/room_keys/*`).

use std::collections::BTreeMap;

use ruma::{
    OwnedRoomId, UInt,
    api::{
        client::backup::{
            self, RoomKeyBackup, add_backup_keys, add_backup_keys_for_room,
            add_backup_keys_for_session, create_backup_version, delete_backup_keys,
            delete_backup_keys_for_room, delete_backup_keys_for_session, delete_backup_version,
            get_backup_info, get_backup_keys, get_backup_keys_for_room,
            get_backup_keys_for_session, get_latest_backup_info, update_backup_version,
        },
        error::{ErrorKind, WrongRoomKeysVersionErrorData},
    },
    serde::Raw,
};
use serde_json::Value;
use tessera_store::{BackupKey, BackupVersion};

use crate::{AppState, Error, Result, Ruma};

fn raw<T>(v: &Value) -> Result<Raw<T>> {
    Ok(Raw::from_json(
        serde_json::value::to_raw_value(v).map_err(|e| Error::internal(e.to_string()))?,
    ))
}

fn parse_raw<T>(r: &Raw<T>) -> Result<Value> {
    serde_json::from_str(r.json().get()).map_err(|e| Error::bad_json(e.to_string()))
}

fn count(n: i64) -> UInt {
    UInt::try_from(n.max(0)).unwrap_or(UInt::MAX)
}

/// The version keys are being written to must be the current one.
async fn require_current_version(
    app: &AppState,
    user: &ruma::UserId,
    version: &str,
) -> Result<BackupVersion> {
    let current = app
        .store
        .get_backup_version(user, None)
        .await?
        .ok_or_else(|| Error::not_found("No current backup version."))?;
    if current.version != version {
        return Err(Error::matrix(
            ErrorKind::WrongRoomKeysVersion(WrongRoomKeysVersionErrorData::new(current.version)),
            http::StatusCode::FORBIDDEN,
            "Wrong backup version.",
        ));
    }
    Ok(current)
}

fn backup_key(room_id: &ruma::RoomId, session_id: &str, data: Value) -> Result<BackupKey> {
    let get_u = |k: &str| data.get(k).and_then(Value::as_i64).unwrap_or(0);
    if data.get("session_data").is_none() {
        return Err(Error::bad_json("Missing session_data."));
    }
    Ok(BackupKey {
        room_id: room_id.to_owned(),
        session_id: session_id.to_owned(),
        first_message_index: get_u("first_message_index"),
        forwarded_count: get_u("forwarded_count"),
        is_verified: data
            .get("is_verified")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        data,
    })
}

// ---------------------------------------------------------------- versions

pub async fn create_version(
    app: AppState,
    req: Ruma<create_backup_version::v3::Request>,
) -> Result<create_backup_version::v3::Response> {
    let sender = req.sender()?;
    let algorithm = parse_raw(&req.body.algorithm)?;
    if algorithm.get("algorithm").and_then(Value::as_str).is_none() {
        return Err(Error::bad_json("Missing algorithm."));
    }
    let version = app
        .store
        .create_backup_version(&sender.user_id, &algorithm)
        .await?;
    Ok(create_backup_version::v3::Response::new(version))
}

fn info_response(v: BackupVersion) -> Result<get_backup_info::v3::Response> {
    Ok(get_backup_info::v3::Response::new(
        raw(&v.algorithm)?,
        count(v.count),
        v.etag.to_string(),
        v.version,
    ))
}

pub async fn latest_version(
    app: AppState,
    req: Ruma<get_latest_backup_info::v3::Request>,
) -> Result<get_latest_backup_info::v3::Response> {
    let sender = req.sender()?;
    let v = app
        .store
        .get_backup_version(&sender.user_id, None)
        .await?
        .ok_or_else(|| Error::not_found("No current backup version."))?;
    Ok(get_latest_backup_info::v3::Response::new(
        raw(&v.algorithm)?,
        count(v.count),
        v.etag.to_string(),
        v.version,
    ))
}

pub async fn get_version(
    app: AppState,
    req: Ruma<get_backup_info::v3::Request>,
) -> Result<get_backup_info::v3::Response> {
    let sender = req.sender()?;
    let v = app
        .store
        .get_backup_version(&sender.user_id, Some(&req.body.version))
        .await?
        .ok_or_else(|| Error::not_found("Unknown backup version."))?;
    info_response(v)
}

pub async fn update_version(
    app: AppState,
    req: Ruma<update_backup_version::v3::Request>,
) -> Result<update_backup_version::v3::Response> {
    let sender = req.sender()?;
    let algorithm = parse_raw(&req.body.algorithm)?;
    if let Some(v) = algorithm.get("version").and_then(Value::as_str)
        && v != req.body.version
    {
        return Err(Error::invalid_param("Version in body does not match path."));
    }
    if !app
        .store
        .update_backup_version(&sender.user_id, &req.body.version, &algorithm)
        .await?
    {
        return Err(Error::not_found("Unknown backup version."));
    }
    Ok(update_backup_version::v3::Response::new())
}

pub async fn delete_version(
    app: AppState,
    req: Ruma<delete_backup_version::v3::Request>,
) -> Result<delete_backup_version::v3::Response> {
    let sender = req.sender()?;
    if !app
        .store
        .delete_backup_version(&sender.user_id, &req.body.version)
        .await?
    {
        return Err(Error::not_found("Unknown backup version."));
    }
    Ok(delete_backup_version::v3::Response::new())
}

// ---------------------------------------------------------------- keys: write

pub async fn put_keys(
    app: AppState,
    req: Ruma<add_backup_keys::v3::Request>,
) -> Result<add_backup_keys::v3::Response> {
    let sender = req.sender()?;
    require_current_version(&app, &sender.user_id, &req.body.version).await?;
    let mut keys = Vec::new();
    for (room_id, rk) in &req.body.rooms {
        for (session_id, data) in &rk.sessions {
            keys.push(backup_key(room_id, session_id, parse_raw(data)?)?);
        }
    }
    let (etag, n) = app
        .store
        .put_backup_keys(&sender.user_id, &req.body.version, &keys)
        .await?;
    Ok(add_backup_keys::v3::Response::new(
        etag.to_string(),
        count(n),
    ))
}

pub async fn put_room_keys(
    app: AppState,
    req: Ruma<add_backup_keys_for_room::v3::Request>,
) -> Result<add_backup_keys_for_room::v3::Response> {
    let sender = req.sender()?;
    require_current_version(&app, &sender.user_id, &req.body.version).await?;
    let mut keys = Vec::new();
    for (session_id, data) in &req.body.sessions {
        keys.push(backup_key(&req.body.room_id, session_id, parse_raw(data)?)?);
    }
    let (etag, n) = app
        .store
        .put_backup_keys(&sender.user_id, &req.body.version, &keys)
        .await?;
    Ok(add_backup_keys_for_room::v3::Response::new(
        etag.to_string(),
        count(n),
    ))
}

pub async fn put_session_key(
    app: AppState,
    req: Ruma<add_backup_keys_for_session::v3::Request>,
) -> Result<add_backup_keys_for_session::v3::Response> {
    let sender = req.sender()?;
    require_current_version(&app, &sender.user_id, &req.body.version).await?;
    let key = backup_key(
        &req.body.room_id,
        &req.body.session_id,
        parse_raw(&req.body.session_data)?,
    )?;
    let (etag, n) = app
        .store
        .put_backup_keys(&sender.user_id, &req.body.version, &[key])
        .await?;
    Ok(add_backup_keys_for_session::v3::Response::new(
        etag.to_string(),
        count(n),
    ))
}

// ---------------------------------------------------------------- keys: read

async fn require_version(app: &AppState, user: &ruma::UserId, version: &str) -> Result<()> {
    app.store
        .get_backup_version(user, Some(version))
        .await?
        .map(|_| ())
        .ok_or_else(|| Error::not_found("Unknown backup version."))
}

pub async fn get_keys(
    app: AppState,
    req: Ruma<get_backup_keys::v3::Request>,
) -> Result<get_backup_keys::v3::Response> {
    let sender = req.sender()?;
    require_version(&app, &sender.user_id, &req.body.version).await?;
    let keys = app
        .store
        .get_backup_keys(&sender.user_id, &req.body.version, None, None)
        .await?;
    let mut rooms: BTreeMap<OwnedRoomId, BTreeMap<String, Raw<backup::KeyBackupData>>> =
        BTreeMap::new();
    for k in keys {
        rooms
            .entry(k.room_id.clone())
            .or_default()
            .insert(k.session_id.clone(), raw(&k.data)?);
    }
    Ok(get_backup_keys::v3::Response::new(
        rooms
            .into_iter()
            .map(|(r, s)| (r, RoomKeyBackup::new(s)))
            .collect(),
    ))
}

pub async fn get_room_keys(
    app: AppState,
    req: Ruma<get_backup_keys_for_room::v3::Request>,
) -> Result<get_backup_keys_for_room::v3::Response> {
    let sender = req.sender()?;
    require_version(&app, &sender.user_id, &req.body.version).await?;
    let keys = app
        .store
        .get_backup_keys(
            &sender.user_id,
            &req.body.version,
            Some(&req.body.room_id),
            None,
        )
        .await?;
    let mut sessions = BTreeMap::new();
    for k in keys {
        sessions.insert(k.session_id.clone(), raw(&k.data)?);
    }
    Ok(get_backup_keys_for_room::v3::Response::new(sessions))
}

pub async fn get_session_key(
    app: AppState,
    req: Ruma<get_backup_keys_for_session::v3::Request>,
) -> Result<get_backup_keys_for_session::v3::Response> {
    let sender = req.sender()?;
    require_version(&app, &sender.user_id, &req.body.version).await?;
    let key = app
        .store
        .get_backup_keys(
            &sender.user_id,
            &req.body.version,
            Some(&req.body.room_id),
            Some(&req.body.session_id),
        )
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| Error::not_found("Key not found."))?;
    Ok(get_backup_keys_for_session::v3::Response::new(raw(
        &key.data
    )?))
}

// ---------------------------------------------------------------- keys: delete

pub async fn delete_keys(
    app: AppState,
    req: Ruma<delete_backup_keys::v3::Request>,
) -> Result<delete_backup_keys::v3::Response> {
    let sender = req.sender()?;
    require_version(&app, &sender.user_id, &req.body.version).await?;
    let (etag, n) = app
        .store
        .delete_backup_keys(&sender.user_id, &req.body.version, None, None)
        .await?;
    Ok(delete_backup_keys::v3::Response::new(
        etag.to_string(),
        count(n),
    ))
}

pub async fn delete_room_keys(
    app: AppState,
    req: Ruma<delete_backup_keys_for_room::v3::Request>,
) -> Result<delete_backup_keys_for_room::v3::Response> {
    let sender = req.sender()?;
    require_version(&app, &sender.user_id, &req.body.version).await?;
    let (etag, n) = app
        .store
        .delete_backup_keys(
            &sender.user_id,
            &req.body.version,
            Some(&req.body.room_id),
            None,
        )
        .await?;
    Ok(delete_backup_keys_for_room::v3::Response::new(
        etag.to_string(),
        count(n),
    ))
}

pub async fn delete_session_key(
    app: AppState,
    req: Ruma<delete_backup_keys_for_session::v3::Request>,
) -> Result<delete_backup_keys_for_session::v3::Response> {
    let sender = req.sender()?;
    require_version(&app, &sender.user_id, &req.body.version).await?;
    let (etag, n) = app
        .store
        .delete_backup_keys(
            &sender.user_id,
            &req.body.version,
            Some(&req.body.room_id),
            Some(&req.body.session_id),
        )
        .await?;
    Ok(delete_backup_keys_for_session::v3::Response::new(
        etag.to_string(),
        count(n),
    ))
}
