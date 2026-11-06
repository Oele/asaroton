//! Synapse-compatible administration API (`/_synapse/admin/*`), the subset that the common
//! admin tools use: users, rooms, deletion and history purging. Callers must be server
//! admins (`is_admin`, or an admin scope from the authentication service).

use std::collections::BTreeMap;

use asaroton_auth::Sender;
use asaroton_service::account::Registration;
use asaroton_store::UserListQuery;
use axum::{
    Json,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode, header},
    response::IntoResponse,
};
use ruma::{OwnedRoomId, OwnedUserId, RoomId, events::StateEventType};
use serde_json::{Value, json};

use crate::{AppState, Error, Result};

/// A JSON body regardless of `Content-Type` (admin tooling and curl often omit it); an empty
/// body is an empty object.
pub struct JsonBody(pub Value);

impl std::fmt::Debug for JsonBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("JsonBody")
    }
}

impl axum::extract::FromRequest<AppState> for JsonBody {
    type Rejection = axum::response::Response;

    async fn from_request(
        req: axum::extract::Request,
        _state: &AppState,
    ) -> std::result::Result<Self, Self::Rejection> {
        let bytes = axum::body::to_bytes(req.into_body(), 1024 * 1024)
            .await
            .map_err(|e| Error::bad_json(e.to_string()).into_response())?;
        if bytes.iter().all(u8::is_ascii_whitespace) {
            return Ok(Self(json!({})));
        }
        serde_json::from_slice(&bytes)
            .map(Self)
            .map_err(|e| Error::bad_json(e.to_string()).into_response())
    }
}

type Reply = Result<(StatusCode, Value)>;

fn respond(r: Reply) -> axum::response::Response {
    match r {
        Ok((status, v)) => {
            let mut resp = (status, Json(v)).into_response();
            crate::extract::add_cors(resp.headers_mut());
            resp
        }
        Err(e) => e.into_response(),
    }
}

async fn require_admin(services: &AppState, headers: &HeaderMap) -> Result<Sender> {
    let token = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .map(str::trim)
        .ok_or_else(Error::missing_token)?;
    let sender = services.auth.authenticate(token).await?;
    if !sender.is_admin {
        return Err(Error::forbidden("You are not a server admin."));
    }
    Ok(sender)
}

fn parse_user(services: &AppState, id: &str) -> Result<OwnedUserId> {
    let user = OwnedUserId::try_from(id).map_err(|_| Error::invalid_param("Invalid user ID."))?;
    if user.server_name() != services.config.server_name {
        return Err(Error::invalid_param("Can only look up local users."));
    }
    Ok(user)
}

fn parse_room(id: &str) -> Result<OwnedRoomId> {
    OwnedRoomId::try_from(id).map_err(|_| Error::invalid_param("Invalid room ID."))
}

fn user_json(u: &asaroton_store::User) -> Value {
    json!({
        "name": u.user_id,
        "displayname": u.display_name,
        "avatar_url": u.avatar_url,
        "admin": u.is_admin,
        "deactivated": u.is_deactivated,
        "is_guest": u.is_guest,
        "user_type": if u.appservice_id.is_some() { Some("support") } else { None },
        "creation_ts": u.created_ts,
        "locked": false,
        "shadow_banned": false,
        "erased": false,
    })
}

/// `GET /_synapse/admin/v1/server_version`
pub async fn server_version(State(services): State<AppState>) -> axum::response::Response {
    respond(Ok((
        StatusCode::OK,
        json!({ "server_version": services.config.software_version, "server": "asaroton" }),
    )))
}

/// `GET /_synapse/admin/v2/users`
pub async fn list_users(
    State(services): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<BTreeMap<String, String>>,
) -> axum::response::Response {
    respond(
        async {
            require_admin(&services, &headers).await?;
            let flag =
                |k: &str, default: bool| q.get(k).map_or(default, |v| v == "true" || v == "1");
            let limit: i64 = q.get("limit").and_then(|v| v.parse().ok()).unwrap_or(100);
            let from: i64 = q.get("from").and_then(|v| v.parse().ok()).unwrap_or(0);
            let (users, total) = services
                .store
                .list_users(&UserListQuery {
                    name: q.get("name").cloned().or_else(|| q.get("user_id").cloned()),
                    include_guests: flag("guests", true),
                    include_deactivated: flag("deactivated", false),
                    admins_only: flag("admins", false),
                    limit: limit.clamp(1, 1000),
                    offset: from.max(0),
                })
                .await?;
            let mut body = json!({
                "users": users.iter().map(user_json).collect::<Vec<_>>(),
                "total": total,
            });
            let next = from + i64::try_from(users.len()).unwrap_or(0);
            if next < total {
                body["next_token"] = json!(next.to_string());
            }
            Ok((StatusCode::OK, body))
        }
        .await,
    )
}

/// `GET /_synapse/admin/v2/users/{user_id}`
pub async fn get_user(
    State(services): State<AppState>,
    headers: HeaderMap,
    Path(user_id): Path<String>,
) -> axum::response::Response {
    respond(
        async {
            require_admin(&services, &headers).await?;
            let id = parse_user(&services, &user_id)?;
            let u = services
                .store
                .get_user(&id)
                .await?
                .ok_or_else(|| Error::not_found("User not found"))?;
            let mut v = user_json(&u);
            v["threepids"] = json!([]);
            v["external_ids"] = json!([]);
            v["consent_version"] = Value::Null;
            Ok((StatusCode::OK, v))
        }
        .await,
    )
}

/// `PUT /_synapse/admin/v2/users/{user_id}`: create or update.
pub async fn put_user(
    State(services): State<AppState>,
    headers: HeaderMap,
    Path(user_id): Path<String>,
    JsonBody(body): JsonBody,
) -> axum::response::Response {
    respond(
        async {
            let admin = require_admin(&services, &headers).await?;
            let id = parse_user(&services, &user_id)?;
            let existing = services.store.get_user(&id).await?;
            let created = existing.is_none();
            let user_id = match existing {
                Some(u) => u.user_id,
                None => {
                    let password = body.get("password").and_then(Value::as_str);
                    services
                        .register(Registration {
                            localpart: Some(id.localpart().to_owned()),
                            password: password.map(str::to_owned),
                            display_name: body
                                .get("displayname")
                                .and_then(Value::as_str)
                                .map(str::to_owned),
                            is_guest: false,
                            is_admin: body.get("admin").and_then(Value::as_bool).unwrap_or(false),
                            appservice_id: None,
                        })
                        .await?
                        .user_id
                }
            };
            if !created {
                if let Some(pw) = body.get("password").and_then(Value::as_str) {
                    let logout = body
                        .get("logout_devices")
                        .and_then(Value::as_bool)
                        .unwrap_or(true);
                    services
                        .change_password(&user_id, pw, logout, None, None)
                        .await?;
                }
                if let Some(a) = body.get("admin").and_then(Value::as_bool) {
                    if user_id == admin.user_id && !a {
                        return Err(Error::forbidden("You may not demote yourself."));
                    }
                    services.store.set_admin(&user_id, a).await?;
                    services.auth.forget_user(&user_id);
                }
                if let Some(d) = body.get("deactivated").and_then(Value::as_bool) {
                    if d {
                        services.deactivate(&user_id).await?;
                    } else {
                        services.store.set_deactivated(&user_id, false).await?;
                    }
                }
            }
            match body.get("displayname") {
                Some(Value::String(n)) if !created => {
                    services.set_display_name(&user_id, Some(n)).await?;
                }
                Some(Value::Null) => {
                    services.set_display_name(&user_id, None).await?;
                }
                _ => {}
            }
            match body.get("avatar_url") {
                Some(Value::String(a)) => {
                    services.set_avatar_url(&user_id, Some(a)).await?;
                }
                Some(Value::Null) => {
                    services.set_avatar_url(&user_id, None).await?;
                }
                _ => {}
            }
            let u = services
                .store
                .get_user(&user_id)
                .await?
                .ok_or_else(|| Error::not_found("User not found"))?;
            Ok((
                if created {
                    StatusCode::CREATED
                } else {
                    StatusCode::OK
                },
                user_json(&u),
            ))
        }
        .await,
    )
}

/// `POST /_synapse/admin/v1/deactivate/{user_id}`
pub async fn deactivate(
    State(services): State<AppState>,
    headers: HeaderMap,
    Path(user_id): Path<String>,
    JsonBody(body): JsonBody,
) -> axum::response::Response {
    respond(
        async {
            require_admin(&services, &headers).await?;
            let id = parse_user(&services, &user_id)?;
            if services.store.get_user(&id).await?.is_none() {
                return Err(Error::not_found("User not found"));
            }
            services.deactivate(&id).await?;
            let erase = body.get("erase").and_then(Value::as_bool).unwrap_or(false);
            if erase {
                services.set_display_name(&id, None).await?;
                services.set_avatar_url(&id, None).await?;
            }
            for m in services.store.memberships_for_user(&id).await? {
                if matches!(
                    m.membership,
                    ruma::events::room::member::MembershipState::Join
                        | ruma::events::room::member::MembershipState::Invite
                ) {
                    let _ = services.leave_room(&m.room_id, &id, None).await;
                }
            }
            Ok((
                StatusCode::OK,
                json!({ "id_server_unbind_result": "success" }),
            ))
        }
        .await,
    )
}

/// `POST /_synapse/admin/v1/reset_password/{user_id}`
pub async fn reset_password(
    State(services): State<AppState>,
    headers: HeaderMap,
    Path(user_id): Path<String>,
    JsonBody(body): JsonBody,
) -> axum::response::Response {
    respond(
        async {
            require_admin(&services, &headers).await?;
            let id = parse_user(&services, &user_id)?;
            let pw = body
                .get("new_password")
                .and_then(Value::as_str)
                .ok_or_else(|| Error::bad_json("Missing `new_password`."))?;
            let logout = body
                .get("logout_devices")
                .and_then(Value::as_bool)
                .unwrap_or(true);
            services
                .change_password(&id, pw, logout, None, None)
                .await?;
            Ok((StatusCode::OK, json!({})))
        }
        .await,
    )
}

/// `GET /_synapse/admin/v1/users/{user_id}/admin`
pub async fn get_admin(
    State(services): State<AppState>,
    headers: HeaderMap,
    Path(user_id): Path<String>,
) -> axum::response::Response {
    respond(
        async {
            require_admin(&services, &headers).await?;
            let id = parse_user(&services, &user_id)?;
            let u = services
                .store
                .get_user(&id)
                .await?
                .ok_or_else(|| Error::not_found("User not found"))?;
            Ok((StatusCode::OK, json!({ "admin": u.is_admin })))
        }
        .await,
    )
}

/// `PUT /_synapse/admin/v1/users/{user_id}/admin`
pub async fn set_admin(
    State(services): State<AppState>,
    headers: HeaderMap,
    Path(user_id): Path<String>,
    JsonBody(body): JsonBody,
) -> axum::response::Response {
    respond(
        async {
            let admin = require_admin(&services, &headers).await?;
            let id = parse_user(&services, &user_id)?;
            let flag = body
                .get("admin")
                .and_then(Value::as_bool)
                .ok_or_else(|| Error::bad_json("Missing `admin`."))?;
            if id == admin.user_id && !flag {
                return Err(Error::forbidden("You may not demote yourself."));
            }
            if services.store.get_user(&id).await?.is_none() {
                return Err(Error::not_found("User not found"));
            }
            services.store.set_admin(&id, flag).await?;
            services.auth.forget_user(&id);
            Ok((StatusCode::OK, json!({})))
        }
        .await,
    )
}

/// `GET /_synapse/admin/v2/users/{user_id}/devices`
pub async fn list_devices(
    State(services): State<AppState>,
    headers: HeaderMap,
    Path(user_id): Path<String>,
) -> axum::response::Response {
    respond(
        async {
            require_admin(&services, &headers).await?;
            let id = parse_user(&services, &user_id)?;
            let devices: Vec<Value> = services
                .store
                .list_devices(&id)
                .await?
                .iter()
                .map(|d| {
                    json!({
                        "device_id": d.device_id,
                        "display_name": d.display_name,
                        "last_seen_ip": d.last_seen_ip,
                        "last_seen_ts": d.last_seen_ts,
                        "last_seen_user_agent": d.last_seen_user_agent,
                        "user_id": d.user_id,
                    })
                })
                .collect();
            let total = devices.len();
            Ok((
                StatusCode::OK,
                json!({ "devices": devices, "total": total }),
            ))
        }
        .await,
    )
}

/// `GET /_synapse/admin/v1/users/{user_id}/joined_rooms`
pub async fn joined_rooms(
    State(services): State<AppState>,
    headers: HeaderMap,
    Path(user_id): Path<String>,
) -> axum::response::Response {
    respond(
        async {
            require_admin(&services, &headers).await?;
            let id = parse_user(&services, &user_id)?;
            let rooms = services.store.rooms_for_user(&id, "join").await?;
            Ok((
                StatusCode::OK,
                json!({ "joined_rooms": rooms, "total": rooms.len() }),
            ))
        }
        .await,
    )
}

async fn state_field(services: &AppState, room_id: &RoomId, ty: &str, key: &str) -> Result<Value> {
    let ty = StateEventType::from(ty);
    Ok(services
        .state_content(room_id, &ty, "")
        .await?
        .and_then(|c| c.get(key).cloned())
        .unwrap_or(Value::Null))
}

async fn room_json(services: &AppState, room: &asaroton_store::RoomInfo) -> Result<Value> {
    let room_id = &room.room_id;
    let content = |ty: &'static str, key: &'static str| state_field(services, room_id, ty, key);
    let members = services.store.members(room_id, Some("join")).await?;
    let local = members
        .iter()
        .filter(|m| services.is_local(&m.user_id))
        .count();
    let state_events = services.store.current_state_pdus(room_id).await?.len();
    let aliases = services.store.aliases_for_room(room_id).await?;
    Ok(json!({
        "room_id": room_id,
        "name": content("m.room.name", "name").await?,
        "topic": content("m.room.topic", "topic").await?,
        "avatar": content("m.room.avatar", "url").await?,
        "canonical_alias": content("m.room.canonical_alias", "alias").await?,
        "joined_members": members.len(),
        "joined_local_members": local,
        "version": room.room_version,
        "creator": room.creator,
        "encryption": content("m.room.encryption", "algorithm").await?,
        "federatable": content("m.room.create", "m.federate").await?.as_bool().unwrap_or(true),
        "public": room.is_public,
        "join_rules": content("m.room.join_rules", "join_rule").await?,
        "guest_access": content("m.room.guest_access", "guest_access").await?,
        "history_visibility": content("m.room.history_visibility", "history_visibility").await?,
        "state_events": state_events,
        "room_type": content("m.room.create", "type").await?,
        "aliases": aliases.iter().map(|a| a.alias.to_string()).collect::<Vec<_>>(),
    }))
}

/// `GET /_synapse/admin/v1/rooms`
pub async fn list_rooms(
    State(services): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<BTreeMap<String, String>>,
) -> axum::response::Response {
    respond(
        async {
            require_admin(&services, &headers).await?;
            let limit: i64 = q.get("limit").and_then(|v| v.parse().ok()).unwrap_or(100);
            let from: i64 = q.get("from").and_then(|v| v.parse().ok()).unwrap_or(0);
            let (rooms, total) = services
                .store
                .list_rooms(
                    q.get("search_term").map(String::as_str),
                    limit.clamp(1, 1000),
                    from.max(0),
                )
                .await?;
            let mut out = Vec::with_capacity(rooms.len());
            for r in &rooms {
                out.push(room_json(&services, r).await?);
            }
            let mut body = json!({ "rooms": out, "offset": from, "total_rooms": total });
            let next = from + i64::try_from(rooms.len()).unwrap_or(0);
            if next < total {
                body["next_batch"] = json!(next);
            }
            if from > 0 {
                body["prev_batch"] = json!((from - limit).max(0));
            }
            Ok((StatusCode::OK, body))
        }
        .await,
    )
}

async fn room_info(services: &AppState, room_id: &RoomId) -> Result<asaroton_store::RoomInfo> {
    services
        .store
        .get_room(room_id)
        .await?
        .ok_or_else(|| Error::not_found("Room not found"))
}

/// `GET /_synapse/admin/v1/rooms/{room_id}`
pub async fn get_room(
    State(services): State<AppState>,
    headers: HeaderMap,
    Path(room_id): Path<String>,
) -> axum::response::Response {
    respond(
        async {
            require_admin(&services, &headers).await?;
            let id = parse_room(&room_id)?;
            let room = room_info(&services, &id).await?;
            Ok((StatusCode::OK, room_json(&services, &room).await?))
        }
        .await,
    )
}

/// `GET /_synapse/admin/v1/rooms/{room_id}/members`
pub async fn room_members(
    State(services): State<AppState>,
    headers: HeaderMap,
    Path(room_id): Path<String>,
) -> axum::response::Response {
    respond(
        async {
            require_admin(&services, &headers).await?;
            let id = parse_room(&room_id)?;
            room_info(&services, &id).await?;
            let members: Vec<OwnedUserId> = services
                .store
                .members(&id, Some("join"))
                .await?
                .into_iter()
                .map(|m| m.user_id)
                .collect();
            Ok((
                StatusCode::OK,
                json!({ "members": members, "total": members.len() }),
            ))
        }
        .await,
    )
}

/// `GET /_synapse/admin/v1/rooms/{room_id}/state`
pub async fn room_state(
    State(services): State<AppState>,
    headers: HeaderMap,
    Path(room_id): Path<String>,
) -> axum::response::Response {
    respond(
        async {
            require_admin(&services, &headers).await?;
            let id = parse_room(&room_id)?;
            room_info(&services, &id).await?;
            let state: Vec<Value> = services
                .store
                .current_state_pdus(&id)
                .await?
                .iter()
                .map(super::rooms::client_event)
                .collect();
            Ok((StatusCode::OK, json!({ "state": state })))
        }
        .await,
    )
}

/// `DELETE /_synapse/admin/v1/rooms/{room_id}` (synchronous variant).
pub async fn delete_room(
    State(services): State<AppState>,
    headers: HeaderMap,
    Path(room_id): Path<String>,
    JsonBody(body): JsonBody,
) -> axum::response::Response {
    respond(
        async {
            require_admin(&services, &headers).await?;
            let id = parse_room(&room_id)?;
            room_info(&services, &id).await?;
            let purge = body.get("purge").and_then(Value::as_bool).unwrap_or(true);
            let r = services.delete_room(&id, purge).await?;
            Ok((
                StatusCode::OK,
                json!({
                    "kicked_users": r.kicked_users,
                    "failed_to_kick_users": r.failed_to_kick_users,
                    "local_aliases": r.local_aliases,
                    "new_room_id": Value::Null,
                }),
            ))
        }
        .await,
    )
}

/// `POST /_synapse/admin/v1/purge_history/{room_id}/{event_id}` (and without an event ID:
/// `purge_up_to_ts` in the body). Runs synchronously; the returned purge is complete.
pub async fn purge_history_at(
    state: State<AppState>,
    headers: HeaderMap,
    Path((room_id, event_id)): Path<(String, String)>,
    JsonBody(body): JsonBody,
) -> axum::response::Response {
    purge_history_inner(state, headers, room_id, Some(event_id), body).await
}

pub async fn purge_history(
    state: State<AppState>,
    headers: HeaderMap,
    Path(room_id): Path<String>,
    JsonBody(body): JsonBody,
) -> axum::response::Response {
    purge_history_inner(state, headers, room_id, None, body).await
}

async fn purge_history_inner(
    State(services): State<AppState>,
    headers: HeaderMap,
    room_id: String,
    event_id: Option<String>,
    body: Value,
) -> axum::response::Response {
    respond(async {
        require_admin(&services, &headers).await?;
        let room_id = parse_room(&room_id)?;
        room_info(&services, &room_id).await?;
        let deleted = if let Some(event_id) = event_id {
            let eid = ruma::OwnedEventId::try_from(event_id.as_str())
                .map_err(|_| Error::invalid_param("Invalid event ID."))?;
            let ev = services
                .store
                .get_event(&eid)
                .await?
                .filter(|e| e.pdu.room_id() == room_id)
                .ok_or_else(|| Error::not_found("Event not found"))?;
            services
                .purge_history_before(&room_id, ev.stream_ordering)
                .await?
        } else {
            let ts = body
                .get("purge_up_to_ts")
                .and_then(Value::as_i64)
                .ok_or_else(|| Error::bad_json("Missing `purge_up_to_ts` or event ID."))?;
            services.purge_history_older_than(&room_id, ts).await?
        };
        Ok((
            StatusCode::OK,
            json!({ "purge_id": format!("{}-{deleted}", asaroton_store::now_ts()), "status": "complete", "deleted_events": deleted }),
        ))
    }
    .await)
}

/// `GET /_synapse/admin/v1/purge_history_status/{purge_id}`: purges run synchronously.
pub async fn purge_status(
    State(services): State<AppState>,
    headers: HeaderMap,
    Path(_purge_id): Path<String>,
) -> axum::response::Response {
    respond(
        async {
            require_admin(&services, &headers).await?;
            Ok((StatusCode::OK, json!({ "status": "complete" })))
        }
        .await,
    )
}

/// `POST /_synapse/admin/v1/join/{room_id_or_alias}`: make a local user join a room.
pub async fn force_join(
    State(services): State<AppState>,
    headers: HeaderMap,
    Path(room): Path<String>,
    JsonBody(body): JsonBody,
) -> axum::response::Response {
    respond(
        async {
            require_admin(&services, &headers).await?;
            let user = body
                .get("user_id")
                .and_then(Value::as_str)
                .map(|u| parse_user(&services, u))
                .transpose()?
                .ok_or_else(|| Error::bad_json("Missing `user_id`."))?;
            let room_id = match OwnedRoomId::try_from(room.as_str()) {
                Ok(id) => id,
                Err(_) => {
                    let alias = ruma::OwnedRoomAliasId::try_from(room.as_str())
                        .map_err(|_| Error::invalid_param("Invalid room ID or alias."))?;
                    services
                        .store
                        .get_alias(&alias)
                        .await?
                        .map(|a| a.room_id)
                        .ok_or_else(|| Error::not_found("Alias not found"))?
                }
            };
            let sender = Sender {
                user_id: user,
                device_id: None,
                is_guest: false,
                appservice_id: None,
                token: String::new(),
                is_admin: false,
            };
            super::rooms::join_room(&services, &sender, &room_id, &[], None, None).await?;
            Ok((StatusCode::OK, json!({ "room_id": room_id })))
        }
        .await,
    )
}

/// `POST /_asaroton/admin/v1/remote_leave`: make a local account leave a room in which other
/// servers still list it as a member (or as invited) while this server has no record of
/// that membership. That is the state rooms are in that were left out when the server's data
/// was migrated: their events keep arriving and the accounts stay visible as members. The
/// leave is made through a server that is in the room (`make_leave`, `send_leave`).
///
/// Body: `{"room_id": ..., "user_id": ..., "via": [server, ...]}`. The caller is a server
/// admin (access token), or a tool on the server's host that proves it holds the registration
/// shared secret: `nonce` from `GET /_synapse/admin/v1/register` and `mac`, the HMAC-SHA1 over
/// nonce, `remote_leave`, room ID and user ID, joined by NUL bytes.
pub async fn remote_leave(
    State(services): State<AppState>,
    headers: HeaderMap,
    JsonBody(body): JsonBody,
) -> axum::response::Response {
    respond(
        async {
            let text = |key: &str| body.get(key).and_then(Value::as_str);
            let (Some(room), Some(user)) = (text("room_id"), text("user_id")) else {
                return Err(Error::bad_json("`room_id` and `user_id` are required."));
            };
            match (text("nonce"), text("mac")) {
                (Some(nonce), Some(mac)) => {
                    if !super::synapse_admin::operator_mac_is_valid(
                        &services,
                        nonce,
                        &["remote_leave", room, user],
                        mac,
                    ) {
                        return Err(Error::forbidden("HMAC incorrect"));
                    }
                }
                _ => {
                    require_admin(&services, &headers).await?;
                }
            }
            let room_id = parse_room(room)?;
            let user_id = parse_user(&services, user)?;
            let via: Vec<ruma::OwnedServerName> = body
                .get("via")
                .and_then(Value::as_array)
                .map(|servers| {
                    servers
                        .iter()
                        .filter_map(Value::as_str)
                        .filter_map(|s| ruma::OwnedServerName::try_from(s).ok())
                        .collect()
                })
                .unwrap_or_default();
            if via.is_empty() {
                return Err(Error::bad_json(
                    "`via` must name at least one server that is in the room.",
                ));
            }
            // A membership this server knows about is ended the ordinary way, which also
            // updates what the account's clients see.
            let known = services.store.get_membership(&room_id, &user_id).await?;
            if known.is_some_and(|m| {
                use ruma::events::room::member::MembershipState as M;
                matches!(m.membership, M::Join | M::Invite | M::Knock)
            }) {
                return Err(Error::invalid_param(
                    "This server has a record of that membership; use the ordinary leave.",
                ));
            }
            services
                .federation()
                .remote_leave(&user_id, &room_id, &via)
                .await
                .map_err(|e| {
                    Error::matrix(
                        ruma::api::error::ErrorKind::Unknown,
                        StatusCode::BAD_GATEWAY,
                        format!("No server completed the leave: {e}"),
                    )
                })?;
            tracing::info!(%room_id, %user_id, "an account left a room this server had no record of, at the operator's request");
            Ok((StatusCode::OK, json!({})))
        }
        .await,
    )
}
