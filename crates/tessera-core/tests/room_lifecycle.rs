//! End-to-end exercise of the core crate: build a room from scratch in several room versions,
//! run every receipt-time check over the events, verify signatures, redact, and resolve a
//! state conflict.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::cast_possible_truncation
)]

use std::collections::HashMap;

use serde_json::json;
use tessera_core::{
    Pdu, SigningKey, auth,
    builder::{PduBuilder, random_room_id},
    redaction, room_version,
    ruma::{
        OwnedEventId, OwnedRoomId, RoomVersionId, UInt,
        events::{StateEventType, TimelineEventType},
        room_version_rules::{RoomIdFormatVersion, RoomVersionRules},
        server_name,
        signatures::Verified,
        state_res::utils::event_id_set::EventIdSet,
        user_id,
    },
    signing, state,
};

/// A tiny in-memory room used to drive the checks the way the server will.
struct Room {
    rules: RoomVersionRules,
    key: SigningKey,
    events: HashMap<OwnedEventId, Pdu>,
    state: HashMap<(StateEventType, String), Pdu>,
    room_id: Option<OwnedRoomId>,
    last: Vec<OwnedEventId>,
    depth: u32,
}

impl Room {
    fn new(version: &RoomVersionId) -> Self {
        Self {
            rules: room_version::rules(version).unwrap(),
            key: SigningKey::generate("k1").unwrap(),
            events: HashMap::new(),
            state: HashMap::new(),
            room_id: None,
            last: Vec::new(),
            depth: 0,
        }
    }

    fn state_map(&self) -> state::StateMap {
        state::state_map_from_pdus(self.state.values())
    }

    /// Build, check and accept an event, exactly as the real pipeline will.
    fn send(&mut self, b: PduBuilder, content: serde_json::Value) -> Pdu {
        let server = server_name!("example.org");
        let content = b.content(&content).unwrap();
        let mut b = content;
        if let Some(room_id) = &self.room_id {
            b = b.room_id(room_id.clone());
        }
        self.depth += 1;
        let b = b
            .depth(UInt::from(self.depth))
            .prev_events(self.last.clone());
        // Build once to get the parsed type/sender/content, pick auth events from the current
        // state, then build the real event. The service layer does the selection up front.
        let pdu = b.build(&self.rules, &self.key, server).unwrap();
        let auth_ids = auth::select_auth_events(
            pdu.event_type(),
            pdu.sender(),
            pdu.state_key(),
            pdu.content(),
            &self.state_map(),
            &self.rules,
        )
        .unwrap();
        let mut rebuilt = PduBuilder::new(pdu.sender().to_owned(), pdu.event_type().clone())
            .content_object(tessera_core::canonical::from_str(pdu.content().get()).unwrap())
            .depth(pdu.depth())
            .prev_events(self.last.clone())
            .auth_events(auth_ids);
        if let Some(k) = pdu.state_key() {
            rebuilt = rebuilt.state_key(k);
        }
        if let Some(room_id) = &self.room_id {
            rebuilt = rebuilt.room_id(room_id.clone());
        }
        let pdu = rebuilt.build(&self.rules, &self.key, server).unwrap();

        // --- receipt checks ---
        auth::check_format(&pdu, &self.rules).unwrap();
        let keys = self.key.public_key_map(server);
        assert_eq!(
            signing::verify_event(&keys, pdu.raw(), &self.rules).unwrap(),
            Verified::All
        );
        let events = self.events.clone();
        auth::check_state_independent(&pdu, &self.rules, |id| events.get(id).cloned()).unwrap();
        auth::check_against_auth_events(&pdu, &self.rules, |id| events.get(id).cloned()).unwrap();
        auth::check_against_state(&pdu, &self.rules, &self.state).unwrap();

        if pdu.is_create() {
            self.room_id = Some(pdu.room_id().to_owned());
        }
        if let Some((ty, key)) = pdu.state_tuple() {
            self.state.insert((ty, key.to_owned()), pdu.clone());
        }
        self.events.insert(pdu.event_id().to_owned(), pdu.clone());
        self.last = vec![pdu.event_id().to_owned()];
        pdu
    }
}

fn build_room(version: &RoomVersionId) -> Room {
    let alice = user_id!("@alice:example.org");
    let mut room = Room::new(version);

    let mut create = PduBuilder::new(alice.to_owned(), TimelineEventType::RoomCreate).state_key("");
    let mut create_content = json!({ "room_version": version.as_str() });
    if room.rules.room_id_format == RoomIdFormatVersion::V1 {
        room.room_id = Some(random_room_id(server_name!("example.org")));
    }
    if !room.rules.authorization.use_room_create_sender {
        create_content["creator"] = json!(alice);
    }
    if version == &RoomVersionId::V1 {
        create_content = json!({ "creator": alice });
    }
    if let Some(room_id) = &room.room_id {
        create = create.room_id(room_id.clone());
    }
    let create = room.send(create, create_content);
    assert_eq!(create.declared_room_version().as_ref(), Some(version));
    if room.rules.room_id_format == RoomIdFormatVersion::V2 {
        assert!(!create.has_room_id_field());
        assert_eq!(
            create.room_id().as_str(),
            format!("!{}", create.event_id().localpart())
        );
    }

    room.send(
        PduBuilder::new(alice.to_owned(), TimelineEventType::RoomMember).state_key(alice.as_str()),
        json!({ "membership": "join" }),
    );
    // Since room version 12 creators have infinite power and must not appear in `users`.
    let mut power_levels = json!({ "users_default": 0, "state_default": 50, "events_default": 0 });
    if !room.rules.authorization.explicitly_privilege_room_creators {
        power_levels["users"] = json!({ alice: 100 });
    }
    room.send(
        PduBuilder::new(alice.to_owned(), TimelineEventType::RoomPowerLevels).state_key(""),
        power_levels,
    );
    room.send(
        PduBuilder::new(alice.to_owned(), TimelineEventType::RoomJoinRules).state_key(""),
        json!({ "join_rule": "public" }),
    );
    room.send(
        PduBuilder::new(alice.to_owned(), TimelineEventType::RoomMessage),
        json!({ "msgtype": "m.text", "body": "hello" }),
    );
    room
}

#[test]
fn builds_rooms_in_every_supported_version() {
    for version in &tessera_core::SUPPORTED_ROOM_VERSIONS {
        let room = build_room(version);
        assert_eq!(room.events.len(), 5, "room version {version}");
    }
}

#[test]
fn unauthorised_sender_is_rejected() {
    let mut room = build_room(&RoomVersionId::V12);
    let bob = user_id!("@bob:elsewhere.org");
    // Bob has not joined, so a message from him must fail the state-dependent rules.
    let pdu = PduBuilder::new(bob.to_owned(), TimelineEventType::RoomMessage)
        .room_id(room.room_id.clone().unwrap())
        .content(&json!({ "msgtype": "m.text", "body": "sneaky" }))
        .unwrap()
        .prev_events(room.last.clone())
        .auth_events(
            auth::select_auth_events(
                &TimelineEventType::RoomMessage,
                bob,
                None,
                &serde_json::value::RawValue::from_string("{}".into()).unwrap(),
                &room.state_map(),
                &room.rules,
            )
            .unwrap(),
        )
        .depth(UInt::from(6u32))
        .build(&room.rules, &room.key, server_name!("elsewhere.org"))
        .unwrap();
    let err = auth::check_against_state(&pdu, &room.rules, &room.state).unwrap_err();
    assert!(err.to_string().contains("authorization"), "{err}");
    room.events
        .insert(pdu.event_id().to_owned(), pdu.with_rejected(true));
}

#[test]
fn signature_from_wrong_key_fails() {
    let room = build_room(&RoomVersionId::V10);
    let other = SigningKey::generate("k2").unwrap();
    let keys = other.public_key_map(server_name!("example.org"));
    let pdu = room.events.values().next().unwrap();
    assert!(signing::verify_event(&keys, pdu.raw(), &room.rules).is_err());
}

#[test]
fn redaction_keeps_event_id_and_strips_content() {
    for version in [
        RoomVersionId::V1,
        RoomVersionId::V6,
        RoomVersionId::V11,
        RoomVersionId::V12,
    ] {
        let room = build_room(&version);
        let msg = room
            .events
            .values()
            .find(|p| *p.event_type() == TimelineEventType::RoomMessage)
            .unwrap();
        let redacted = redaction::redact_pdu(msg, &room.rules, None).unwrap();
        assert_eq!(
            redacted.event_id(),
            msg.event_id(),
            "room version {version}"
        );
        assert_eq!(redacted.content().get(), "{}");
        // Signatures are computed over the redacted form, so they survive redaction; the
        // content hash covers the full content, so it no longer matches.
        let keys = room.key.public_key_map(server_name!("example.org"));
        let verified = signing::verify_event(&keys, redacted.raw(), &room.rules).unwrap();
        assert_eq!(verified, Verified::Signatures, "room version {version}");

        // Power levels keep their allowed keys.
        let pl = room
            .state
            .get(&(StateEventType::RoomPowerLevels, String::new()))
            .unwrap();
        let redacted_pl = redaction::redact_pdu(pl, &room.rules, None).unwrap();
        let content: serde_json::Value = redacted_pl.content_as().unwrap();
        assert_eq!(content["users_default"], json!(0));
        assert_eq!(content["state_default"], json!(50));
    }
}

#[test]
fn resolves_conflicting_topics_by_power_then_timestamp() {
    for version in [RoomVersionId::V2, RoomVersionId::V10, RoomVersionId::V12] {
        let mut room = build_room(&version);
        let alice = user_id!("@alice:example.org");
        let server = server_name!("example.org");
        let base_state = room.state_map();
        let fork_point = room.last.clone();

        // Two topic events built from the same fork point: a classic conflict.
        let mk = |body: &str, ts: u64, room: &Room| {
            PduBuilder::new(alice.to_owned(), TimelineEventType::RoomTopic)
                .state_key("")
                .room_id(room.room_id.clone().unwrap())
                .content(&json!({ "topic": body }))
                .unwrap()
                .prev_events(fork_point.clone())
                .auth_events(
                    auth::select_auth_events(
                        &TimelineEventType::RoomTopic,
                        alice,
                        Some(""),
                        &serde_json::value::RawValue::from_string("{}".into()).unwrap(),
                        &base_state,
                        &room.rules,
                    )
                    .unwrap(),
                )
                .depth(UInt::from(6u32))
                .origin_server_ts(tessera_core::ruma::MilliSecondsSinceUnixEpoch(UInt::from(
                    ts as u32,
                )))
                .build(&room.rules, &room.key, server)
                .unwrap()
        };
        let older = mk("older", 1_000, &room);
        let newer = mk("newer", 2_000, &room);
        room.events
            .insert(older.event_id().to_owned(), older.clone());
        room.events
            .insert(newer.event_id().to_owned(), newer.clone());

        let mut s1 = base_state.clone();
        s1.insert(
            (StateEventType::RoomTopic, String::new()),
            older.event_id().to_owned(),
        );
        let mut s2 = base_state.clone();
        s2.insert(
            (StateEventType::RoomTopic, String::new()),
            newer.event_id().to_owned(),
        );

        // Auth chains: everything reachable via auth_events from each state's events.
        let events = room.events.clone();
        let auth_chain = |s: &state::StateMap| {
            let mut set: EventIdSet<OwnedEventId> = EventIdSet::new();
            let mut stack: Vec<OwnedEventId> = s.values().cloned().collect();
            while let Some(id) = stack.pop() {
                if let Some(ev) = events.get(&id) {
                    for a in ev.auth_events() {
                        if set.insert(a.clone()) {
                            stack.push(a.clone());
                        }
                    }
                }
            }
            set
        };
        let chains = vec![auth_chain(&s1), auth_chain(&s2)];

        let resolved = state::resolve(
            &room.rules,
            &[s1, s2],
            chains,
            |id| events.get(id).cloned(),
            |conflicted| {
                // Both topics are in the conflicted set; nothing else is reachable from them that
                // is itself conflicted, so the subgraph is just the conflicted events.
                let mut set = EventIdSet::new();
                for ids in conflicted.values() {
                    for id in ids {
                        set.insert(id.clone());
                    }
                }
                Some(set)
            },
        )
        .unwrap();

        // Same sender and power level, so the mainline tie-break falls to origin_server_ts:
        // the later event wins.
        assert_eq!(
            resolved
                .get(&(StateEventType::RoomTopic, String::new()))
                .unwrap(),
            newer.event_id(),
            "room version {version}"
        );
        // Unconflicted state is carried through untouched.
        assert_eq!(resolved.len(), base_state.len() + 1);
    }
}
