//! `POST /_matrix/client/v4/sync`: Simplified Sliding Sync as accepted (MSC4186).
//!
//! The same engine as the unstable endpoint ([`super::sliding_sync`]) serves it, as
//! [`Dialect::V4`]. This module maps ruma's `v4::Request` to the engine's request itself
//! (ruma's own conversion to the unstable request loses `exclude`, the new filters and the
//! meaning of `include: [{}]` with lazy members), checks what MSC4186 says the server must
//! refuse, and renders the answer in v4's shape, by hand like the unstable one. The extensions
//! follow their MSCs: the common scope rules of MSC4508, to-device (MSC4538), e2ee (MSC4543),
//! account data (MSC4548), receipts (MSC4551) and typing (MSC4508). Thread subscriptions,
//! profiles and sticky events keep their unstable keys and rules.

use std::time::Duration;

use asaroton_auth::Sender;
use asaroton_service::sliding_sync::{
    Dialect, ExtensionScope, ExtensionsRequest, KeyMatch, ListFilters, ListRequest,
    RequiredStateSpec, RoomResult, RoomSubscriptionRequest, ScopeRoom, SlidingSyncRequest,
    SlidingSyncResult, StateMatcher, ToDeviceRequest,
};
use axum::{Json, response::IntoResponse};
use ruma::{
    api::client::sync::sync_events::v4::{self, request::FilterMode},
    presence::PresenceState,
};
use serde_json::{Map, Value, json};

use super::sliding_sync::{
    extensions_json, heroes_json, limit, lists_json, profiles_request, room_type_filter,
    sticky_request, timeline_json,
};
use crate::{AppState, Error, Result, Ruma, client::rooms::client_event};

/// Where `/v4/sync` is served. Like the unstable path, the router lets it end with its client
/// (see `router::waits_for_news`).
pub(crate) const PATH: &str = "/_matrix/client/v4/sync";

/// At most this many lists, and as many room subscriptions (MSC4186).
const MAX_LISTS: usize = 100;
const MAX_SUBSCRIPTIONS: usize = 100;

/// A list key in the grammar of opaque identifiers: 1 to 255 of `[0-9A-Za-z._~-]`.
fn is_list_key(key: &str) -> bool {
    (1..=255).contains(&key.len())
        && key
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'~' | b'-'))
}

/// What MSC4186 has the server refuse with `M_INVALID_PARAM`.
fn validate(b: &v4::Request) -> Result<()> {
    if b.lists.len() > MAX_LISTS {
        return Err(Error::invalid_param(format!(
            "At most {MAX_LISTS} lists are allowed."
        )));
    }
    if b.room_subscriptions.len() > MAX_SUBSCRIPTIONS {
        return Err(Error::invalid_param(format!(
            "At most {MAX_SUBSCRIPTIONS} room subscriptions are allowed."
        )));
    }
    if let Some(bad) = b.lists.keys().find(|k| !is_list_key(k)) {
        return Err(Error::invalid_param(format!(
            "The list key {bad:?} is not an opaque identifier ([0-9A-Za-z._~-], 1 to 255 characters)."
        )));
    }
    if !matches!(
        b.set_presence,
        PresenceState::Online | PresenceState::Offline | PresenceState::Unavailable
    ) {
        return Err(Error::invalid_param(format!(
            "Unknown set_presence {:?}.",
            b.set_presence.as_str()
        )));
    }
    for (key, list) in &b.lists {
        // A range that ends before it starts is refused rather than skipped (open question 8
        // of the v4 plan); one that starts past the end of the list is just empty.
        if let Some((start, end)) = list.range
            && end < start
        {
            return Err(Error::invalid_param(format!(
                "The range of list {key:?} ends before it starts."
            )));
        }
    }
    Ok(())
}

fn matcher(e: &v4::request::RequiredStateElement) -> StateMatcher {
    // No wildcards on v4: an omitted field matches everything, and `*` or `$ME` is just a
    // string.
    StateMatcher {
        ty: e.event_type.as_ref().map(ToString::to_string),
        key: e
            .state_key
            .as_ref()
            .map_or(KeyMatch::Any, |k| KeyMatch::Exact(k.clone())),
    }
}

/// A list's or a subscription's `required_state`. With lazy members, an include without a
/// type covers every state but the memberships (`include: [{}]`, MSC4186's own example).
fn required_state(r: &v4::request::RequiredState) -> RequiredStateSpec {
    RequiredStateSpec {
        include: r.include.iter().map(matcher).collect(),
        exclude: r.exclude.iter().map(matcher).collect(),
        lazy_members: r.lazy_members,
        lazy_narrows_wildcards: true,
    }
}

fn filter_mode(m: FilterMode) -> Option<bool> {
    m.into()
}

fn filters(f: &v4::request::ListFilters) -> Option<ListFilters> {
    if f.is_empty() {
        return None;
    }
    Some(ListFilters {
        is_dm: filter_mode(f.is_dm),
        is_encrypted: filter_mode(f.is_encrypted),
        is_invite: filter_mode(f.is_invited),
        room_types: if f.room_types.is_empty() {
            None
        } else {
            Some(f.room_types.iter().map(room_type_filter).collect())
        },
        not_room_types: f.not_room_types.iter().map(room_type_filter).collect(),
        spaces: f.spaces.clone(),
        tags: f.tags.iter().map(ToString::to_string).collect(),
        not_tags: f.not_tags.iter().map(ToString::to_string).collect(),
    })
}

fn list(l: &v4::request::List) -> ListRequest {
    ListRequest {
        // No range: every room that passes the filters.
        ranges: vec![
            l.range
                .map_or((0, u64::MAX), |(a, z)| (u64::from(a), u64::from(z))),
        ],
        required_state: required_state(&l.required_state),
        timeline_limit: limit(l.timeline_limit),
        filters: filters(&l.filters),
    }
}

/// A per-room extension's `lists` and `rooms` (MSC4508). Either left out means all of them,
/// and `[]` none. `rooms` names room subscriptions only, and there are no wildcards: a `*`
/// among the lists names no list (it cannot be a list key), so it is dropped.
fn scope(lists: Option<&Vec<String>>, rooms: Option<&Vec<ruma::OwnedRoomId>>) -> ExtensionScope {
    ExtensionScope {
        lists: lists.map(|l| l.iter().filter(|k| *k != "*").cloned().collect()),
        rooms: rooms.map(|r| r.iter().cloned().map(ScopeRoom::Subscribed).collect()),
    }
}

/// The engine's request for a v4 body. `raw` is the body as JSON, for the extensions ruma's
/// type does not model here: profiles and sticky events are read from it as on the unstable
/// endpoint (open question 10 of the v4 plan).
fn request(sender: &Sender, b: &v4::Request, raw: Option<&Value>) -> Result<SlidingSyncRequest> {
    validate(b)?;
    let x = &b.extensions;
    // MSC4538: the limit must be positive. (A negative one does not get past ruma.)
    if x.to_device.enabled && x.to_device.limit.is_some_and(|l| l == ruma::UInt::MIN) {
        return Err(Error::bad_json(
            "The to-device limit must be a positive integer.",
        ));
    }
    let extensions = ExtensionsRequest {
        to_device: x.to_device.enabled.then(|| ToDeviceRequest {
            since: x.to_device.since.clone(),
            limit: x.to_device.limit.map_or(0, limit),
        }),
        e2ee: x.e2ee.enabled,
        account_data: x
            .account_data
            .enabled
            .then(|| scope(x.account_data.lists.as_ref(), x.account_data.rooms.as_ref())),
        receipts: x
            .receipts
            .enabled
            .then(|| scope(x.receipts.lists.as_ref(), x.receipts.rooms.as_ref())),
        typing: x
            .typing
            .enabled
            .then(|| scope(x.typing.lists.as_ref(), x.typing.rooms.as_ref())),
        thread_subscriptions: x
            .thread_subscriptions
            .enabled
            .then(|| x.thread_subscriptions.limit.map_or(100, limit)),
        profiles: profiles_request(raw),
        sticky: sticky_request(raw).map(|(s, _)| s),
    };
    Ok(SlidingSyncRequest {
        dialect: Dialect::V4,
        user_id: sender.user_id.clone(),
        device_id: sender.device_id.clone(),
        conn_id: b.conn_id.clone(),
        pos: b.pos.clone(),
        timeout: b
            .timeout
            .unwrap_or(Duration::ZERO)
            .min(Duration::from_secs(300)),
        set_presence: b.set_presence.clone(),
        lists: b
            .lists
            .iter()
            .map(|(name, l)| (name.clone(), list(l)))
            .collect(),
        room_subscriptions: b
            .room_subscriptions
            .iter()
            .map(|(id, s)| {
                (
                    id.clone(),
                    RoomSubscriptionRequest {
                        required_state: required_state(&s.required_state),
                        timeline_limit: limit(s.timeline_limit),
                    },
                )
            })
            .collect(),
        extensions,
    })
}

/// One room of a v4 answer, given its state and timeline in client format.
///
/// Every room has the common fields `bump_stamp`, `membership` and `lists` (left out when
/// the room is only there for a subscription). An invite or a pending knock has its stripped
/// state and nothing more. A declined invite or a withdrawn or refused knock is a room the
/// user left, in the full shape with `membership: leave` and their own membership event as
/// state, as on the unstable endpoint, without stripped state (open question 4 of the v4
/// plan). There are no notification counts (open question 9).
pub(crate) fn room_json(r: &RoomResult, required_state: Vec<Value>, timeline: Vec<Value>) -> Value {
    let mut v = Map::new();
    v.insert("bump_stamp".into(), json!(r.bump_stamp));
    v.insert("membership".into(), json!(r.membership));
    if !r.lists.is_empty() {
        v.insert("lists".into(), json!(r.lists));
    }
    if let Some(stripped) = &r.invite_state {
        v.insert("stripped_state".into(), json!(stripped));
        return Value::Object(v);
    }
    if r.initial {
        v.insert("initial".into(), json!(true));
    }
    // An omitted name means "unchanged" (or none, when `initial`); `null` that it is gone.
    match &r.name {
        Some(n) => {
            v.insert("name".into(), json!(n));
        }
        None if r.name_cleared => {
            v.insert("name".into(), Value::Null);
        }
        None => {}
    }
    match r.avatar.as_deref().filter(|a| !a.is_empty()) {
        Some(a) => {
            v.insert("avatar".into(), json!(a));
        }
        None if r.avatar_cleared => {
            v.insert("avatar".into(), Value::Null);
        }
        None => {}
    }
    if let Some(heroes) = heroes_json(r) {
        v.insert("heroes".into(), heroes);
    }
    // Absent in an initial answer means "not a DM"; afterwards it comes whenever it changed.
    if r.is_dm || r.is_dm_changed {
        v.insert("is_dm".into(), json!(r.is_dm));
    }
    if r.expanded_timeline {
        v.insert("expanded_timeline".into(), json!(true));
    }
    let mut state = required_state;
    state.extend(
        r.stubs
            .iter()
            .map(|(ty, key)| json!({ "type": ty, "state_key": key })),
    );
    v.insert("required_state".into(), Value::Array(state));
    v.insert("timeline".into(), Value::Array(timeline));
    if let Some(pb) = &r.prev_batch {
        v.insert("prev_batch".into(), json!(pb));
    }
    v.insert("limited".into(), json!(r.limited));
    v.insert("num_live".into(), json!(r.num_live));
    v.insert("joined_count".into(), json!(r.joined_count));
    v.insert("invited_count".into(), json!(r.invited_count));
    Value::Object(v)
}

/// The whole v4 answer, given its rooms as rendered by [`room_json`]. It has no `txn_id`.
fn body(result: &SlidingSyncResult, rooms: &Map<String, Value>, sticky: Option<&str>) -> Value {
    json!({
        "pos": result.pos,
        "lists": lists_json(result),
        "rooms": rooms,
        "extensions": extensions_json(&result.extensions, sticky, Dialect::V4),
    })
}

pub async fn sliding_sync(
    services: AppState,
    req: Ruma<v4::Request>,
) -> Result<axum::response::Response> {
    let sender = req.sender()?;
    let request = request(sender, &req.body, req.json.as_ref())?;
    let result = services.sliding_sync(request).await?;
    let mut rooms = Map::new();
    for (room_id, r) in &result.rooms {
        let (state, timeline) = if r.invite_state.is_some() {
            (Vec::new(), Vec::new())
        } else {
            (
                r.required_state.iter().map(client_event).collect(),
                timeline_json(&services, &sender.user_id, r).await?,
            )
        };
        rooms.insert(room_id.to_string(), room_json(r, state, timeline));
    }
    let sticky = sticky_request(req.json.as_ref()).map(|(_, name)| name);
    let mut resp = Json(body(&result, &rooms, sticky)).into_response();
    crate::extract::add_cors(resp.headers_mut());
    Ok(resp)
}

#[cfg(test)]
mod tests {
    use asaroton_service::{
        sliding_sync::{AccountDataResult, E2eeResult, ExtensionsResult, Hero},
        thread_subscriptions::ThreadSubscriptionChanges,
    };
    use ruma::{
        api::client::sync::sync_events::v4::response::{RequiredStateEntry, RoomData},
        events::room::member::MembershipState,
        owned_event_id, owned_room_id, owned_user_id,
    };

    use super::*;

    fn sender() -> Sender {
        Sender {
            user_id: owned_user_id!("@me:x.test"),
            device_id: Some("DEV".into()),
            is_guest: false,
            appservice_id: None,
            token: String::new(),
            is_admin: false,
        }
    }

    /// A request body as the endpoint reads it.
    fn parse(body: &Value) -> v4::Request {
        let bytes = serde_json::to_vec(body).unwrap();
        let http = http::Request::builder()
            .method(http::Method::POST)
            .uri(PATH)
            .body(bytes.as_slice())
            .unwrap();
        <v4::Request as ruma::api::IncomingRequestExt>::try_from_http_request(http, &[]).unwrap()
    }

    fn mapped(body: &Value) -> Result<SlidingSyncRequest> {
        request(&sender(), &parse(body), Some(body))
    }

    fn invalid(body: &Value) -> bool {
        matches!(
            mapped(body),
            Err(Error::Matrix(ruma::api::error::ErrorKind::InvalidParam, _, status))
                if status == http::StatusCode::BAD_REQUEST
        )
    }

    fn a_list() -> Value {
        json!({ "timeline_limit": 1, "required_state": {} })
    }

    #[test]
    fn a_v4_request_maps_to_the_engine() {
        let r = mapped(&json!({
            "conn_id": "main",
            "pos": "s1~n~1",
            "timeout": 30000,
            "set_presence": "unavailable",
            "lists": {
                "all": {
                    "range": [0, 19],
                    "timeline_limit": 5,
                    "required_state": {
                        "include": [{}, { "type": "m.room.member", "state_key": "@friend:x.test" }],
                        "exclude": [{ "type": "m.room.create" }],
                        "lazy_members": true,
                    },
                    "filters": {
                        "is_invited": false,
                        "is_dm": true,
                        "spaces": ["!space:x.test"],
                        "tags": ["m.favourite"],
                        "not_tags": ["m.lowpriority"],
                        "not_room_types": ["m.space"],
                    },
                },
                "everything": { "timeline_limit": 0, "required_state": {} },
            },
            "room_subscriptions": {
                "!r:x.test": { "timeline_limit": 20, "required_state": { "include": [{ "state_key": "" }] } },
            },
        }))
        .unwrap();
        assert_eq!(r.dialect, Dialect::V4);
        assert_eq!(r.pos.as_deref(), Some("s1~n~1"));
        assert_eq!(r.timeout, Duration::from_secs(30));
        assert_eq!(r.set_presence, PresenceState::Unavailable);
        let all = &r.lists["all"];
        assert_eq!(all.ranges, [(0, 19)]);
        assert_eq!(all.timeline_limit, 5);
        let spec = &all.required_state;
        assert!(spec.lazy_members && spec.lazy_narrows_wildcards);
        assert!(spec.include.contains(&StateMatcher {
            ty: None,
            key: KeyMatch::Any
        }));
        assert!(spec.include.contains(&StateMatcher {
            ty: Some("m.room.member".into()),
            key: KeyMatch::Exact("@friend:x.test".into())
        }));
        assert!(spec.exclude.contains(&StateMatcher {
            ty: Some("m.room.create".into()),
            key: KeyMatch::Any
        }));
        let f = all.filters.as_ref().unwrap();
        assert_eq!(
            (f.is_invite, f.is_dm, f.is_encrypted),
            (Some(false), Some(true), None)
        );
        assert_eq!(f.spaces, [owned_room_id!("!space:x.test")]);
        assert_eq!(f.tags, ["m.favourite"]);
        assert_eq!(f.not_tags, ["m.lowpriority"]);
        assert_eq!(f.not_room_types, [Some("m.space".to_owned())]);
        assert!(f.room_types.is_none());
        // No range: every room; no filters: none.
        let everything = &r.lists["everything"];
        assert_eq!(everything.ranges, [(0, u64::MAX)]);
        assert!(everything.filters.is_none());
        let sub = &r.room_subscriptions[&owned_room_id!("!r:x.test")];
        assert_eq!(sub.timeline_limit, 20);
        assert!(sub.required_state.include.contains(&StateMatcher {
            ty: None,
            key: KeyMatch::Exact(String::new())
        }));
    }

    #[test]
    fn what_msc4186_refuses_is_refused() {
        let lists = |n: usize| {
            let lists: Map<String, Value> = (0..n).map(|i| (format!("l{i}"), a_list())).collect();
            json!({ "lists": lists })
        };
        assert!(mapped(&lists(100)).is_ok());
        assert!(invalid(&lists(101)), "101 lists");
        let subs = |n: usize| {
            let subs: Map<String, Value> = (0..n)
                .map(|i| (format!("!r{i}:x.test"), a_list()))
                .collect();
            json!({ "room_subscriptions": subs })
        };
        assert!(mapped(&subs(100)).is_ok());
        assert!(invalid(&subs(101)), "101 subscriptions");
        for bad in ["", "with space", "slash/", "ü", &"x".repeat(256)] {
            assert!(
                invalid(&json!({ "lists": { bad: a_list() } })),
                "list key {bad:?}"
            );
        }
        assert!(mapped(&json!({ "lists": { "Az09._~-": a_list() } })).is_ok());
        assert!(mapped(&json!({ "lists": { "x".repeat(255): a_list() } })).is_ok());
        assert!(invalid(&json!({ "set_presence": "busy" })), "set_presence");
        for known in ["online", "offline", "unavailable"] {
            assert!(mapped(&json!({ "set_presence": known })).is_ok(), "{known}");
        }
        assert!(
            invalid(
                &json!({ "lists": { "a": { "range": [5, 2], "timeline_limit": 1, "required_state": {} } } })
            ),
            "a range that ends before it starts"
        );
        assert!(
            mapped(&json!({ "lists": { "a": { "range": [3, 3], "timeline_limit": 1, "required_state": {} } } }))
                .is_ok()
        );
    }

    fn joined_room() -> RoomResult {
        let mut r = RoomResult::new(MembershipState::Join);
        r.lists = vec!["all".into(), "dms".into()];
        r.initial = true;
        r.name = Some("Room".into());
        r.avatar = Some("mxc://x.test/a".into());
        r.is_dm = true;
        r.limited = true;
        r.prev_batch = Some("t1".into());
        r.joined_count = 2;
        r.invited_count = 1;
        r.num_live = 1;
        r.bump_stamp = 7;
        r.notification_count = 3;
        r.highlight_count = 1;
        r.heroes = vec![Hero {
            user_id: owned_user_id!("@you:x.test"),
            name: Some("You".into()),
            avatar: None,
        }];
        r
    }

    fn state(ty: &str, key: &str, content: &Value) -> Value {
        json!({
            "type": ty, "state_key": key, "content": content, "sender": "@me:x.test",
            "event_id": format!("${ty}{key}"), "origin_server_ts": 1, "unsigned": {},
        })
    }

    fn message(body: &str) -> Value {
        json!({
            "type": "m.room.message", "content": { "msgtype": "m.text", "body": body },
            "sender": "@you:x.test", "event_id": "$m", "origin_server_ts": 2, "unsigned": {},
        })
    }

    /// `v4::Response` as a ruma client reads it. (Its `IncomingResponse` comes with ruma's
    /// client feature, which the server does not build; the parts are the same types.)
    #[derive(serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Oracle {
        pos: String,
        #[serde(default)]
        lists: std::collections::BTreeMap<String, v4::response::List>,
        #[serde(default)]
        rooms: std::collections::BTreeMap<ruma::OwnedRoomId, v4::response::Room>,
        #[serde(default)]
        extensions: v4::response::Extensions,
    }

    /// A v4 answer holding these rooms, as a ruma client reads it.
    fn as_ruma(rooms: Vec<(&str, Value)>) -> (Value, Oracle) {
        let result = SlidingSyncResult {
            pos: "s5_0~nonce~2".into(),
            lists: [("all".to_owned(), 4)].into(),
            ..SlidingSyncResult::default()
        };
        let rooms: Map<String, Value> = rooms.into_iter().map(|(id, v)| (id.into(), v)).collect();
        let body = body(&result, &rooms, None);
        let parsed: Oracle = serde_json::from_value(body.clone()).expect("ruma reads the answer");
        (body, parsed)
    }

    fn some_extensions(changed: bool) -> ExtensionsResult {
        let room = owned_room_id!("!r:x.test");
        ExtensionsResult {
            to_device: Some((
                "5".into(),
                vec![json!({
                    "type": "m.room_key_request", "sender": "@you:x.test",
                    "content": { "action": "request_cancellation", "request_id": "1", "requesting_device_id": "D" },
                })],
            )),
            e2ee: Some(E2eeResult {
                changed: if changed {
                    vec![owned_user_id!("@you:x.test")]
                } else {
                    Vec::new()
                },
                left: Vec::new(),
                one_time_key_counts: [("signed_curve25519".to_owned(), 50)].into(),
                unused_fallback_key_types: vec!["signed_curve25519".to_owned()],
            }),
            account_data: Some(AccountDataResult {
                global: vec![json!({ "type": "m.direct", "content": { "@you:x.test": ["!r:x.test"] } })],
                rooms: [(
                    room.clone(),
                    vec![json!({ "type": "m.tag", "content": { "tags": { "m.favourite": { "order": 0.5 } } } })],
                )]
                .into(),
            }),
            receipts: [(
                room.clone(),
                json!({ "$e:x.test": { "m.read": { "@you:x.test": { "ts": 1 } } } }),
            )]
            .into(),
            typing: [(room.clone(), vec![owned_user_id!("@you:x.test")])].into(),
            thread_subscriptions: Some(ThreadSubscriptionChanges {
                subscribed: [(room, [(owned_event_id!("$t:x.test"), (true, 3))].into())].into(),
                unsubscribed: std::collections::BTreeMap::new(),
                prev_batch: None,
            }),
            ..ExtensionsResult::default()
        }
    }

    #[test]
    fn the_extensions_read_as_ruma_has_them() {
        let room = owned_room_id!("!r:x.test");
        let result = SlidingSyncResult {
            pos: "p".into(),
            extensions: some_extensions(true),
            ..SlidingSyncResult::default()
        };
        let answer = body(&result, &Map::new(), None);
        let raw = &answer["extensions"];
        let x = serde_json::from_value::<Oracle>(answer.clone())
            .expect("ruma reads the answer")
            .extensions;
        let to_device = x.to_device.expect("to_device");
        assert_eq!(to_device.next_batch, "5");
        assert_eq!(to_device.messages.len(), 1, "MSC4538 calls them messages");
        assert!(raw["to_device"].get("events").is_none());
        let e2ee = x.e2ee.expect("e2ee");
        assert_eq!(e2ee.device_lists.changed, [owned_user_id!("@you:x.test")]);
        assert_eq!(
            e2ee.device_one_time_keys_count
                .values()
                .map(|n| u64::from(*n))
                .collect::<Vec<_>>(),
            [50]
        );
        assert_eq!(e2ee.device_unused_fallback_key_types.len(), 1);
        assert_eq!(x.account_data.global.len(), 1);
        assert_eq!(x.account_data.rooms[&room].len(), 1);
        let receipts = x.receipts.rooms[&room]
            .deserialize()
            .expect("bare m.receipt content");
        assert!(receipts.contains_key(&owned_event_id!("$e:x.test")));
        assert!(raw["receipts"]["rooms"]["!r:x.test"].get("type").is_none());
        assert_eq!(
            x.typing.rooms[&room].user_ids,
            [owned_user_id!("@you:x.test")]
        );
        assert!(raw["typing"]["rooms"]["!r:x.test"].get("content").is_none());
        assert_eq!(x.thread_subscriptions.subscribed[&room].len(), 1);

        // Nothing changed in the device lists: no `device_lists` (MSC4543).
        let quiet = SlidingSyncResult {
            pos: "p".into(),
            extensions: some_extensions(false),
            ..SlidingSyncResult::default()
        };
        let answer = body(&quiet, &Map::new(), None);
        assert!(answer["extensions"]["e2ee"].get("device_lists").is_none());
        let x = serde_json::from_value::<Oracle>(answer)
            .expect("ruma reads the answer")
            .extensions;
        assert!(x.e2ee.is_some_and(|e| e.device_lists.is_empty()));
    }

    #[test]
    fn the_unstable_endpoint_keeps_its_extension_shapes() {
        let raw = extensions_json(&some_extensions(false), None, Dialect::Unstable);
        assert_eq!(raw["to_device"]["events"].as_array().map(Vec::len), Some(1));
        assert!(raw["to_device"].get("messages").is_none());
        assert_eq!(
            raw["e2ee"]["device_lists"],
            json!({ "changed": [], "left": [] })
        );
        assert_eq!(
            raw["receipts"]["rooms"]["!r:x.test"],
            json!({
                "type": "m.receipt", "room_id": "!r:x.test",
                "content": { "$e:x.test": { "m.read": { "@you:x.test": { "ts": 1 } } } },
            })
        );
        assert_eq!(
            raw["typing"]["rooms"]["!r:x.test"],
            json!({ "type": "m.typing", "content": { "user_ids": ["@you:x.test"] } })
        );
    }

    #[test]
    fn v4_extensions_map_with_the_msc_rules() {
        let r = mapped(&json!({
            "extensions": {
                "to_device": { "enabled": true, "limit": 50, "since": "7" },
                "e2ee": { "enabled": true },
                "receipts": { "enabled": true, "lists": ["all", "*"], "rooms": ["!r:x.test"] },
                "typing": { "enabled": true },
                "account_data": { "enabled": false },
                "org.example.unknown": { "enabled": true },
            },
        }))
        .unwrap();
        let x = &r.extensions;
        let td = x.to_device.as_ref().unwrap();
        assert_eq!((td.limit, td.since.as_deref()), (50, Some("7")));
        assert!(x.e2ee && x.account_data.is_none());
        let receipts = x.receipts.as_ref().unwrap();
        assert_eq!(receipts.lists.as_deref(), Some(&["all".to_owned()][..]));
        assert!(matches!(
            receipts.rooms.as_deref(),
            Some([ScopeRoom::Subscribed(id)]) if id == "!r:x.test"
        ));
        let typing = x.typing.as_ref().unwrap();
        assert!(typing.lists.is_none() && typing.rooms.is_none());
        // A limit of 0 is refused (MSC4538).
        assert!(matches!(
            mapped(&json!({ "extensions": { "to_device": { "enabled": true, "limit": 0 } } })),
            Err(Error::Matrix(ruma::api::error::ErrorKind::BadJson, _, _))
        ));
        assert!(
            mapped(&json!({ "extensions": { "to_device": { "enabled": false, "limit": 0 } } }))
                .is_ok()
        );
    }

    #[test]
    fn a_joined_room_reads_as_ruma_has_it() {
        let r = joined_room();
        let rendered = room_json(
            &r,
            vec![state("m.room.name", "", &json!({ "name": "Room" }))],
            vec![message("hi")],
        );
        let (body, resp) = as_ruma(vec![("!r:x.test", rendered)]);
        assert!(body.get("txn_id").is_none());
        assert_eq!(resp.pos, "s5_0~nonce~2");
        assert_eq!(u64::from(resp.lists["all"].count), 4);
        let room = &resp.rooms[&owned_room_id!("!r:x.test")];
        assert_eq!(room.membership, Some(MembershipState::Join));
        assert_eq!(room.lists, ["all", "dms"]);
        assert_eq!(room.bump_stamp.map(u64::from), Some(7));
        let RoomData::Full(full) = &room.data else {
            panic!("a joined room has the full shape");
        };
        assert!(full.initial && full.limited);
        assert_eq!(full.name.as_ref().into_option(), Some(&"Room".to_owned()));
        assert_eq!(
            full.avatar.as_ref().into_option().map(ToString::to_string),
            Some("mxc://x.test/a".to_owned())
        );
        assert_eq!(full.is_dm, Some(true));
        assert_eq!(full.heroes.as_ref().map(Vec::len), Some(1));
        assert_eq!(full.required_state.len(), 1);
        assert!(matches!(
            full.required_state[0],
            RequiredStateEntry::Event(_)
        ));
        assert_eq!(full.timeline.len(), 1);
        assert_eq!(full.prev_batch.as_deref(), Some("t1"));
        assert_eq!(full.joined_count.map(u64::from), Some(2));
        assert_eq!(full.invited_count.map(u64::from), Some(1));
        assert_eq!(full.num_live.map(u64::from), Some(1));
        // No notification counts in v4.
        let raw = &body["rooms"]["!r:x.test"];
        for gone in [
            "notification_count",
            "highlight_count",
            "unread_notifications",
            "invite_state",
        ] {
            assert!(raw.get(gone).is_none(), "{gone}");
        }
    }

    #[test]
    fn a_removed_name_is_null_and_gone_state_a_stub() {
        let mut r = joined_room();
        r.initial = false;
        r.name = None;
        r.name_cleared = true;
        r.avatar = None;
        r.avatar_cleared = true;
        r.is_dm = false;
        r.is_dm_changed = true;
        r.heroes = Vec::new();
        r.stubs = vec![("m.room.topic".into(), String::new())];
        let rendered = room_json(&r, Vec::new(), Vec::new());
        let raw = rendered.clone();
        let (_, resp) = as_ruma(vec![("!r:x.test", rendered)]);
        let RoomData::Full(full) = &resp.rooms[&owned_room_id!("!r:x.test")].data else {
            panic!("full shape");
        };
        assert!(full.name.is_null(), "name: null");
        assert!(full.avatar.is_null(), "avatar: null");
        assert_eq!(
            full.is_dm,
            Some(false),
            "a DM flag that changed comes as false"
        );
        assert!(!full.initial);
        let [RequiredStateEntry::Deleted(stub)] = full.required_state.as_slice() else {
            panic!("one stub: {:?}", full.required_state);
        };
        assert_eq!(stub.event_type.to_string(), "m.room.topic");
        assert_eq!(stub.state_key, "");
        assert_eq!(raw["name"], Value::Null);

        // Unchanged, they are left out.
        // `new` says nothing: no name, avatar or DM flag, not initial, in no list.
        let quiet = RoomResult::new(MembershipState::Join);
        let rendered = room_json(&quiet, Vec::new(), Vec::new());
        for absent in ["name", "avatar", "is_dm", "initial", "lists"] {
            assert!(rendered.get(absent).is_none(), "{absent}");
        }
        let (_, resp) = as_ruma(vec![("!r:x.test", rendered)]);
        let RoomData::Full(full) = &resp.rooms[&owned_room_id!("!r:x.test")].data else {
            panic!("full shape");
        };
        assert!(full.name.is_undefined() && full.avatar.is_undefined() && full.is_dm.is_none());
    }

    #[test]
    fn an_invite_has_stripped_state_and_the_common_fields_only() {
        let stripped = vec![
            json!({ "type": "m.room.name", "state_key": "", "content": { "name": "Party" }, "sender": "@you:x.test" }),
            json!({ "type": "m.room.member", "state_key": "@me:x.test", "content": { "membership": "invite" }, "sender": "@you:x.test" }),
        ];
        for membership in [MembershipState::Invite, MembershipState::Knock] {
            let mut r = RoomResult::new(membership.clone());
            r.lists = vec!["invites".into()];
            r.initial = true;
            r.name = Some("Party".into());
            r.is_dm = true;
            r.bump_stamp = 3;
            r.invite_state = Some(stripped.clone());
            let rendered = room_json(&r, Vec::new(), Vec::new());
            let mut keys: Vec<&str> = rendered
                .as_object()
                .unwrap()
                .keys()
                .map(String::as_str)
                .collect();
            keys.sort_unstable();
            assert_eq!(
                keys,
                ["bump_stamp", "lists", "membership", "stripped_state"],
                "{membership:?}"
            );
            let (_, resp) = as_ruma(vec![("!r:x.test", rendered)]);
            let room = &resp.rooms[&owned_room_id!("!r:x.test")];
            assert_eq!(room.membership.as_ref(), Some(&membership));
            assert_eq!(room.lists, ["invites"]);
            let RoomData::Stripped(s) = &room.data else {
                panic!("an invite or a knock has the stripped shape");
            };
            assert_eq!(s.stripped_state.len(), 2);
        }
    }

    #[test]
    fn a_left_room_has_the_full_shape() {
        // A declined invite: no stripped state, the own membership as state.
        let r = RoomResult::new(MembershipState::Leave);
        let rendered = room_json(
            &r,
            vec![state(
                "m.room.member",
                "@me:x.test",
                &json!({ "membership": "leave" }),
            )],
            Vec::new(),
        );
        assert!(rendered.get("lists").is_none());
        let (_, resp) = as_ruma(vec![("!r:x.test", rendered)]);
        let room = &resp.rooms[&owned_room_id!("!r:x.test")];
        assert_eq!(room.membership, Some(MembershipState::Leave));
        assert!(matches!(&room.data, RoomData::Full(f) if f.required_state.len() == 1));
    }
}
