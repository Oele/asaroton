//! Integration tests for the PostgreSQL store. Each test gets a fresh database from
//! `#[sqlx::test]`, created from `DATABASE_URL` (see `.env.example`).

#![allow(clippy::pedantic, clippy::unwrap_used, clippy::expect_used)]

use std::collections::HashMap;

use asaroton_core::{
    Pdu, SigningKey, auth,
    builder::PduBuilder,
    redaction, room_version,
    ruma::{
        OwnedEventId, OwnedRoomId, OwnedUserId, RoomVersionId, UInt, device_id,
        events::{StateEventType, TimelineEventType, room::member::MembershipState},
        room_alias_id, server_name, user_id,
    },
    state,
};
use asaroton_store::{
    AccessToken, AccountDataStore, CurrentStateDelta, Direction, EventState, EventStore, KeyStore,
    LocalSigningKey, NewUser, PersistEvent, PostgresStore, RemoteVerifyKey, RoomAlias, RoomInfo,
    RoomStore, StateStore, Store, UserStore, now_ts,
};
use serde_json::json;
use sqlx::PgPool;

/// Drives a room exactly like the service layer will: build, auth-check, compute state delta,
/// persist. Keeps enough in memory to compute auth events and state groups.
struct RoomDriver {
    store: PostgresStore,
    rules: asaroton_core::ruma::room_version_rules::RoomVersionRules,
    key: SigningKey,
    room_id: Option<OwnedRoomId>,
    state: HashMap<(StateEventType, String), Pdu>,
    state_group: Option<i64>,
    last: Vec<OwnedEventId>,
    depth: u32,
    pub events: Vec<Pdu>,
}

impl RoomDriver {
    fn new(store: PostgresStore, version: &RoomVersionId) -> Self {
        Self {
            store,
            rules: room_version::rules(version).unwrap(),
            key: SigningKey::generate("k1").unwrap(),
            room_id: None,
            state: HashMap::new(),
            state_group: None,
            last: Vec::new(),
            depth: 0,
            events: Vec::new(),
        }
    }

    async fn send(
        &mut self,
        sender: &asaroton_core::ruma::UserId,
        ty: TimelineEventType,
        state_key: Option<&str>,
        content: serde_json::Value,
    ) -> Pdu {
        let server = server_name!("example.org");
        let state_map = state::state_map_from_pdus(self.state.values());
        let content_obj = asaroton_core::canonical::from_json_value(content).unwrap();
        let raw_content = serde_json::value::RawValue::from_string(
            asaroton_core::canonical::to_string(&content_obj),
        )
        .unwrap();
        let auth_ids = auth::select_auth_events(
            &ty,
            sender,
            state_key,
            &raw_content,
            &state_map,
            &self.rules,
        )
        .unwrap();
        self.depth += 1;
        let mut b = PduBuilder::new(sender.to_owned(), ty.clone())
            .content_object(content_obj)
            .depth(UInt::from(self.depth))
            .prev_events(self.last.clone())
            .auth_events(auth_ids);
        if let Some(k) = state_key {
            b = b.state_key(k);
        }
        if let Some(room_id) = &self.room_id {
            b = b.room_id(room_id.clone());
        }
        let pdu = b.build(&self.rules, &self.key, server).unwrap();
        auth::check_format(&pdu, &self.rules).unwrap();
        auth::check_against_state(&pdu, &self.rules, &self.state).unwrap();

        if pdu.is_create() {
            let room_id = pdu.room_id().to_owned();
            self.store
                .create_room(RoomInfo {
                    room_id: room_id.clone(),
                    room_version: pdu.declared_room_version().unwrap(),
                    creator: Some(sender.to_owned()),
                    is_public: false,
                    created_ts: now_ts(),
                })
                .await
                .unwrap();
            self.room_id = Some(room_id);
        }

        let (event_state, delta) = if let Some((sty, sk)) = pdu.state_tuple() {
            let d = vec![((sty, sk.to_owned()), pdu.event_id().to_owned())];
            (
                EventState::New {
                    prev: self.state_group,
                    delta: d.clone(),
                },
                Some(CurrentStateDelta {
                    upsert: d,
                    remove: vec![],
                }),
            )
        } else {
            (EventState::Existing(self.state_group.unwrap()), None)
        };
        let persisted = self
            .store
            .persist_event(PersistEvent {
                pdu: pdu.clone(),
                state: event_state,
                soft_failed: false,
                backfilled: false,
                stream_ordering: None,
                current_state_delta: delta,
                forward_extremities: Some(vec![pdu.event_id().to_owned()]),
            })
            .await
            .unwrap();
        assert!(persisted.stream_ordering > 0);
        self.state_group = persisted.state_group;
        if let Some((sty, sk)) = pdu.state_tuple() {
            self.state.insert((sty, sk.to_owned()), pdu.clone());
        }
        self.last = vec![pdu.event_id().to_owned()];
        self.events.push(pdu.clone());
        pdu
    }
}

async fn build_room(store: &PostgresStore, version: &RoomVersionId) -> RoomDriver {
    let alice = user_id!("@alice:example.org");
    let bob = user_id!("@bob:example.org");
    let mut d = RoomDriver::new(store.clone(), version);
    let mut create = json!({ "room_version": version.as_str() });
    if !d.rules.authorization.use_room_create_sender {
        create["creator"] = json!(alice);
    }
    if d.rules.room_id_format == asaroton_core::ruma::room_version_rules::RoomIdFormatVersion::V1 {
        d.room_id = Some(asaroton_core::builder::random_room_id(server_name!(
            "example.org"
        )));
    }
    d.send(alice, TimelineEventType::RoomCreate, Some(""), create)
        .await;
    d.send(
        alice,
        TimelineEventType::RoomMember,
        Some(alice.as_str()),
        json!({ "membership": "join", "displayname": "Alice" }),
    )
    .await;
    let mut pl =
        json!({ "users_default": 0, "state_default": 50, "events_default": 0, "invite": 0 });
    if !d.rules.authorization.explicitly_privilege_room_creators {
        pl["users"] = json!({ alice: 100 });
    }
    d.send(alice, TimelineEventType::RoomPowerLevels, Some(""), pl)
        .await;
    d.send(
        alice,
        TimelineEventType::RoomJoinRules,
        Some(""),
        json!({ "join_rule": "public" }),
    )
    .await;
    d.send(
        alice,
        TimelineEventType::RoomMessage,
        None,
        json!({ "msgtype": "m.text", "body": "hello" }),
    )
    .await;
    d.send(
        bob,
        TimelineEventType::RoomMember,
        Some(bob.as_str()),
        json!({ "membership": "join" }),
    )
    .await;
    d.send(
        bob,
        TimelineEventType::RoomMessage,
        None,
        json!({ "msgtype": "m.text", "body": "hi alice" }),
    )
    .await;
    d
}

#[sqlx::test(migrations = "./migrations")]
async fn users_devices_tokens(pool: PgPool) {
    let store = PostgresStore::from_pool(pool);
    let alice = user_id!("@alice:example.org");
    let user = store
        .create_user(NewUser {
            user_id: alice.to_owned(),
            password_hash: Some("$argon2$fake".into()),
            display_name: Some("Alice".into()),
            is_admin: false,
            is_guest: false,
            appservice_id: None,
        })
        .await
        .unwrap();
    assert_eq!(user.user_id, alice);
    assert!(matches!(
        store
            .create_user(NewUser {
                user_id: alice.to_owned(),
                password_hash: None,
                display_name: None,
                is_admin: false,
                is_guest: false,
                appservice_id: None
            })
            .await,
        Err(asaroton_store::Error::AlreadyExists("user"))
    ));
    assert_eq!(
        store.get_password_hash(alice).await.unwrap().as_deref(),
        Some("$argon2$fake")
    );
    assert_eq!(store.count_users().await.unwrap(), 1);

    let dev = store
        .create_device(alice, device_id!("DEV1"), Some("Laptop"))
        .await
        .unwrap();
    assert_eq!(dev.display_name.as_deref(), Some("Laptop"));
    store
        .touch_device(alice, device_id!("DEV1"), Some("127.0.0.1"), Some("ua"))
        .await
        .unwrap();
    let dev = store
        .get_device(alice, device_id!("DEV1"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(dev.last_seen_ip.as_deref(), Some("127.0.0.1"));

    let token = AccessToken {
        token: "tok1".into(),
        user_id: alice.to_owned(),
        device_id: Some(device_id!("DEV1").to_owned()),
        created_ts: now_ts(),
        expires_ts: None,
        refresh_token: None,
        puppets_user_id: None,
    };
    store.create_access_token(token.clone()).await.unwrap();
    assert_eq!(
        store.get_access_token("tok1").await.unwrap().unwrap(),
        token
    );
    store
        .delete_device(alice, device_id!("DEV1"))
        .await
        .unwrap();
    assert!(store.get_access_token("tok1").await.unwrap().is_none());
    assert!(store.list_devices(alice).await.unwrap().is_empty());

    store
        .set_profile(alice, Some(Some("Alice B")), Some(None))
        .await
        .unwrap();
    assert_eq!(
        store
            .get_user(alice)
            .await
            .unwrap()
            .unwrap()
            .display_name
            .as_deref(),
        Some("Alice B")
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn persists_rooms_and_state(pool: PgPool) {
    let store = PostgresStore::from_pool(pool);
    for version in [
        RoomVersionId::V1,
        RoomVersionId::V6,
        RoomVersionId::V11,
        RoomVersionId::V12,
    ] {
        let d = build_room(&store, &version).await;
        let room_id = d.room_id.clone().unwrap();
        let alice = user_id!("@alice:example.org");
        let bob = user_id!("@bob:example.org");

        // Round trip: same event ID and identical signed JSON.
        for pdu in &d.events {
            let stored = store.get_event(pdu.event_id()).await.unwrap().unwrap();
            assert_eq!(stored.pdu.event_id(), pdu.event_id(), "{version}");
            assert_eq!(stored.pdu.raw(), pdu.raw(), "{version}");
            assert_eq!(stored.pdu.room_id(), pdu.room_id());
            assert!(!stored.outlier && !stored.redacted);
        }

        // Current state and memberships.
        let cs = store.current_state(&room_id).await.unwrap();
        assert_eq!(
            cs.len(),
            5,
            "{version}: create, 2 members, power levels, join rules"
        );
        let m = store.get_membership(&room_id, bob).await.unwrap().unwrap();
        assert_eq!(m.membership, MembershipState::Join);
        let am = store
            .get_membership(&room_id, alice)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(am.display_name.as_deref(), Some("Alice"));
        assert!(
            store
                .rooms_for_user(bob, "join")
                .await
                .unwrap()
                .contains(&room_id)
        );
        assert_eq!(store.joined_member_count(&room_id).await.unwrap(), 2);
        assert_eq!(
            store.servers_in_room(&room_id).await.unwrap(),
            vec![server_name!("example.org").to_owned()]
        );

        // State groups: the state at the last event's group equals the current state.
        let last = store
            .get_event(d.events.last().unwrap().event_id())
            .await
            .unwrap()
            .unwrap();
        let at_group = store
            .state_at_group(last.state_group.unwrap())
            .await
            .unwrap();
        assert_eq!(at_group.len(), 5);
        // State before bob's join lacks bob.
        let bob_join = &d.events[5];
        let before = store
            .state_group_before(bob_join.event_id())
            .await
            .unwrap()
            .unwrap();
        let before_state = store.state_at_group(before).await.unwrap();
        assert_eq!(before_state.len(), 4);
        assert!(!before_state.iter().any(|e| e.state_key == bob.as_str()));
        let filtered = store
            .state_at_group_filtered(before, &[(StateEventType::RoomJoinRules, String::new())])
            .await
            .unwrap();
        assert_eq!(filtered.len(), 1);

        // Auth chain of the last message: the create, alice's membership chain, power levels, join rules, bob's member.
        let chain = store
            .auth_chain(&[last.event_id().to_owned()])
            .await
            .unwrap();
        assert!(chain.len() >= 3, "{version}: {chain:?}");
        assert!(!chain.contains(&last.event_id().to_owned()));

        // Extremities and graph walks.
        assert_eq!(
            store.forward_extremities(&room_id).await.unwrap(),
            vec![last.event_id().to_owned()]
        );
        assert!(
            store
                .missing_prev_events(&room_id)
                .await
                .unwrap()
                .is_empty()
        );
        let back = store
            .backfill_from(&room_id, &[last.event_id().to_owned()], 3)
            .await
            .unwrap();
        assert_eq!(back.len(), 3);
        assert_eq!(back[0].event_id(), last.event_id());
        assert!(back[0].pdu.depth() > back[2].pdu.depth());

        // Timeline ranges.
        let all = store
            .room_events_in_range(&room_id, 0, None, Direction::Forwards, 100)
            .await
            .unwrap();
        assert_eq!(all.len(), 7);
        let newest_two = store
            .room_events_in_range(&room_id, i64::MAX, None, Direction::Backwards, 2)
            .await
            .unwrap();
        assert_eq!(newest_two[0].event_id(), last.event_id());
        assert_eq!(newest_two.len(), 2);
        let rooms = store
            .rooms_with_events_in_range(std::slice::from_ref(&room_id), 0, i64::MAX)
            .await
            .unwrap();
        assert_eq!(rooms, vec![room_id.clone()]);

        // Redaction in place.
        let msg = &d.events[4];
        let redacted = redaction::redact_pdu(msg, &d.rules, None).unwrap();
        store.redact_event(msg.event_id(), &redacted).await.unwrap();
        let stored = store.get_event(msg.event_id()).await.unwrap().unwrap();
        assert!(stored.redacted);
        assert_eq!(stored.pdu.content().get(), "{}");

        // Persisting again is a no-op.
        let again = store
            .persist_event(PersistEvent {
                pdu: msg.clone(),
                state: EventState::Outlier,
                soft_failed: false,
                backfilled: false,
                stream_ordering: None,
                current_state_delta: None,
                forward_extremities: None,
            })
            .await
            .unwrap();
        assert_eq!(again.stream_ordering, stored.stream_ordering);

        // Aliases.
        let alias = room_alias_id!("#room:example.org").to_owned();
        let _ = store.delete_alias(&alias).await;
        store
            .create_alias(RoomAlias {
                alias: alias.clone(),
                room_id: room_id.clone(),
                creator: Some(alice.to_owned()),
            })
            .await
            .unwrap();
        assert_eq!(
            store.get_alias(&alias).await.unwrap().unwrap().room_id,
            room_id
        );
        assert!(matches!(
            store
                .create_alias(RoomAlias {
                    alias: alias.clone(),
                    room_id: room_id.clone(),
                    creator: None
                })
                .await,
            Err(asaroton_store::Error::AlreadyExists("alias"))
        ));
    }
}

#[sqlx::test(migrations = "./migrations")]
async fn tracks_missing_prev_events_and_outliers(pool: PgPool) {
    let store = PostgresStore::from_pool(pool);
    let d = build_room(&store, &RoomVersionId::V10).await;
    let room_id = d.room_id.clone().unwrap();
    let alice = user_id!("@alice:example.org");
    // An event whose prev event we do not have.
    let ghost: OwnedEventId = "$ghostghostghostghostghostghostghostghostghs"
        .try_into()
        .unwrap();
    let pdu = PduBuilder::new(alice.to_owned(), TimelineEventType::RoomMessage)
        .room_id(room_id.clone())
        .content(&json!({ "msgtype": "m.text", "body": "from the void" }))
        .unwrap()
        .prev_events(vec![ghost.clone()])
        .auth_events(
            d.events
                .iter()
                .take(3)
                .map(|e| e.event_id().to_owned())
                .collect::<Vec<_>>(),
        )
        .depth(UInt::from(99u32))
        .build(&d.rules, &d.key, server_name!("example.org"))
        .unwrap();
    store
        .persist_event(PersistEvent {
            pdu: pdu.clone(),
            state: EventState::Outlier,
            soft_failed: false,
            backfilled: false,
            stream_ordering: None,
            current_state_delta: None,
            forward_extremities: None,
        })
        .await
        .unwrap();
    let stored = store.get_event(pdu.event_id()).await.unwrap().unwrap();
    assert!(stored.outlier && stored.state_group.is_none());
    assert_eq!(
        store.missing_prev_events(&room_id).await.unwrap(),
        vec![ghost.clone()]
    );
    // Outliers are not backward extremities, and not in the timeline.
    assert!(
        store
            .backward_extremities(&room_id)
            .await
            .unwrap()
            .is_empty()
    );
    let all = store
        .room_events_in_range(&room_id, 0, None, Direction::Forwards, 100)
        .await
        .unwrap();
    assert!(all.iter().all(|e| e.event_id() != pdu.event_id()));

    // De-outlier by persisting again with state.
    let again = store
        .persist_event(PersistEvent {
            pdu: pdu.clone(),
            state: EventState::Existing(d.state_group.unwrap()),
            soft_failed: true,
            backfilled: false,
            stream_ordering: None,
            current_state_delta: None,
            forward_extremities: None,
        })
        .await
        .unwrap();
    assert_eq!(again.state_group, d.state_group);
    let stored = store.get_event(pdu.event_id()).await.unwrap().unwrap();
    assert!(!stored.outlier && stored.soft_failed);
    assert_eq!(
        store.backward_extremities(&room_id).await.unwrap(),
        vec![pdu.event_id().to_owned()]
    );

    // Backfilled events get negative stream orderings.
    let old = PduBuilder::new(alice.to_owned(), TimelineEventType::RoomMessage)
        .room_id(room_id.clone())
        .content(&json!({ "msgtype": "m.text", "body": "ancient" }))
        .unwrap()
        .depth(UInt::from(0u32))
        .build(&d.rules, &d.key, server_name!("example.org"))
        .unwrap();
    let p = store
        .persist_event(PersistEvent {
            pdu: old,
            state: EventState::Outlier,
            soft_failed: false,
            backfilled: true,
            stream_ordering: None,
            current_state_delta: None,
            forward_extremities: None,
        })
        .await
        .unwrap();
    assert!(p.stream_ordering < 0);
}

#[sqlx::test(migrations = "./migrations")]
async fn account_data_and_filters(pool: PgPool) {
    let store = PostgresStore::from_pool(pool);
    let alice = user_id!("@alice:example.org");
    let room: OwnedRoomId = "!r:example.org".try_into().unwrap();
    let s1 = store
        .set_account_data(alice, None, "m.push_rules", &json!({ "global": {} }))
        .await
        .unwrap();
    let s2 = store
        .set_account_data(
            alice,
            Some(&room),
            "m.fully_read",
            &json!({ "event_id": "$x" }),
        )
        .await
        .unwrap();
    assert!(s2 > s1);
    let s3 = store
        .set_account_data(
            alice,
            None,
            "m.push_rules",
            &json!({ "global": { "override": [] } }),
        )
        .await
        .unwrap();
    assert!(s3 > s2);
    let got = store
        .get_account_data(alice, None, "m.push_rules")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got.content["global"]["override"], json!([]));
    let since = store.account_data_since(alice, s1).await.unwrap();
    assert_eq!(since.len(), 2);
    assert_eq!(since[0].room_id.as_ref(), Some(&room));
    assert_eq!(store.max_account_data_stream_id().await.unwrap(), s3);

    let f1 = store
        .create_filter(alice, &json!({ "room": {} }))
        .await
        .unwrap();
    let f2 = store
        .create_filter(alice, &json!({ "presence": {} }))
        .await
        .unwrap();
    assert_eq!((f1, f2), (1, 2));
    assert_eq!(
        store.get_filter(alice, 2).await.unwrap().unwrap(),
        json!({ "presence": {} })
    );
    assert!(store.get_filter(alice, 3).await.unwrap().is_none());
}

#[sqlx::test(migrations = "./migrations")]
async fn signing_keys(pool: PgPool) {
    let store = PostgresStore::from_pool(pool);
    let key = LocalSigningKey {
        key_id: "ed25519:a1".try_into().unwrap(),
        seed_b64: "seed".into(),
        created_ts: 1,
        expired_ts: None,
    };
    store.add_local_signing_key(key.clone()).await.unwrap();
    assert_eq!(store.local_signing_keys().await.unwrap(), vec![key.clone()]);
    store
        .expire_local_signing_key("ed25519:a1", 5)
        .await
        .unwrap();
    assert_eq!(
        store.local_signing_keys().await.unwrap()[0].expired_ts,
        Some(5)
    );

    let remote = RemoteVerifyKey {
        server_name: server_name!("matrix.org").to_owned(),
        key_id: "ed25519:auto".try_into().unwrap(),
        verify_key_b64: "abc".into(),
        valid_until_ts: 100,
        expired_ts: None,
        fetched_ts: 1,
        fetched_from: server_name!("matrix.org").to_owned(),
        key_json: "{}".into(),
    };
    store
        .store_remote_verify_keys(vec![remote.clone()])
        .await
        .unwrap();
    let mut newer = remote.clone();
    newer.valid_until_ts = 50; // older validity never shrinks the cached one
    store.store_remote_verify_keys(vec![newer]).await.unwrap();
    let got = store
        .remote_verify_keys(server_name!("matrix.org"))
        .await
        .unwrap();
    assert_eq!(got[0].valid_until_ts, 100);
    let got = store
        .remote_verify_keys_for(server_name!("matrix.org"), &["ed25519:auto".into()])
        .await
        .unwrap();
    assert_eq!(got.len(), 1);
    assert!(
        store
            .remote_verify_keys_for(server_name!("matrix.org"), &["ed25519:other".into()])
            .await
            .unwrap()
            .is_empty()
    );
}

#[sqlx::test]
async fn migrate_is_idempotent(pool: PgPool) {
    let store = PostgresStore::from_pool(pool);
    store.migrate().await.unwrap();
    store.migrate().await.unwrap();
    let _: OwnedUserId = user_id!("@x:example.org").to_owned();
}

#[sqlx::test(migrations = "./migrations")]
async fn queued_presence_and_typing_edus_are_replaced(pool: PgPool) {
    use asaroton_store::FederationStore;
    let store = PostgresStore::from_pool(pool);
    let dest: ruma::OwnedServerName = "remote.example.org".try_into().unwrap();
    let presence = |state: &str| json!({"edu_type": "m.presence", "content": {"push": [{"user_id": "@a:example.org", "presence": state}]}});
    store
        .enqueue_edu(std::slice::from_ref(&dest), &presence("online"))
        .await
        .unwrap();
    store
        .enqueue_edu(std::slice::from_ref(&dest), &presence("unavailable"))
        .await
        .unwrap();
    store
        .enqueue_edu(std::slice::from_ref(&dest), &presence("online"))
        .await
        .unwrap();
    store
        .enqueue_edu(std::slice::from_ref(&dest), &json!({"edu_type": "m.presence", "content": {"push": [{"user_id": "@b:example.org", "presence": "online"}]}}))
        .await
        .unwrap();
    store
        .enqueue_edu(
            std::slice::from_ref(&dest),
            &json!({"edu_type": "m.receipt", "content": {"!r:example.org": {}}}),
        )
        .await
        .unwrap();
    store
        .enqueue_edu(
            std::slice::from_ref(&dest),
            &json!({"edu_type": "m.receipt", "content": {"!r:example.org": {}}}),
        )
        .await
        .unwrap();
    let items = store.outbox_for(&dest, 100, 100, &[]).await.unwrap();
    // one presence row per user (the latest), both receipts kept
    assert_eq!(items.len(), 4, "{items:?}");
    let latest_a = items
        .iter()
        .filter_map(|i| match &i.kind {
            asaroton_store::OutboxKind::Edu(e) => Some(e),
            asaroton_store::OutboxKind::Pdu(_) => None,
        })
        .find(|e| {
            e.pointer("/content/push/0/user_id")
                .and_then(|u| u.as_str())
                == Some("@a:example.org")
        })
        .unwrap();
    assert_eq!(
        latest_a.pointer("/content/push/0/presence").unwrap(),
        "online"
    );
}

/// Housekeeping: old inbound transactions go, and a destination that has been failing for a
/// long time keeps only the newest event per room.
#[sqlx::test(migrations = "./migrations")]
async fn prunes_federation_tables(pool: PgPool) {
    use asaroton_store::{DestinationState, FederationStore, OutboxKind};

    let store = PostgresStore::from_pool(pool.clone());
    let now = now_ts();
    let day = 24 * 3600 * 1000;

    // Inbound transactions: one from two days ago, one from just now.
    let origin = server_name!("remote.example").to_owned();
    for txn in ["old", "new"] {
        store
            .store_received_transaction(&origin, txn, &json!({"pdus": {}}))
            .await
            .unwrap();
    }
    sqlx::query(
        "UPDATE federation_received_transactions SET received_ts = $1 WHERE transaction_id = 'old'",
    )
    .bind(now - 2 * day)
    .execute(&pool)
    .await
    .unwrap();
    assert_eq!(
        store.prune_received_transactions(now - day).await.unwrap(),
        1
    );
    assert!(
        store
            .received_transaction(&origin, "old")
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .received_transaction(&origin, "new")
            .await
            .unwrap()
            .is_some()
    );

    // Two rooms with events queued for three destinations: one failing for ten days, one
    // failing since yesterday, one healthy.
    let room_a = build_room(&store, &RoomVersionId::V11).await;
    let room_b = build_room(&store, &RoomVersionId::V10).await;
    let dead = server_name!("dead.example").to_owned();
    let flaky = server_name!("flaky.example").to_owned();
    let fine = server_name!("fine.example").to_owned();
    let all = [dead.clone(), flaky.clone(), fine.clone()];
    for (dest, failing_days) in [(&dead, 10), (&flaky, 1)] {
        store
            .set_destination_state(
                dest,
                &DestinationState {
                    failure_ts: Some(now - failing_days * day),
                    retry_last_ts: Some(now),
                    retry_interval_ms: 3_600_000,
                    last_successful_ts: None,
                },
            )
            .await
            .unwrap();
    }
    for room in [&room_a, &room_b] {
        for event in &room.events {
            store.enqueue_pdu(&all, event.event_id()).await.unwrap();
        }
    }
    store
        .enqueue_edu(&all, &json!({"edu_type": "m.receipt", "content": {}}))
        .await
        .unwrap();
    // A queued event that no longer exists must not survive either.
    let gone: OwnedEventId = "$gone".try_into().unwrap();
    store.enqueue_pdu(&all, &gone).await.unwrap();
    let queued = room_a.events.len() + room_b.events.len() + 2;
    assert_eq!(
        store
            .outbox_for(&dead, 1000, 1000, &[])
            .await
            .unwrap()
            .len(),
        queued
    );

    let expected = u64::try_from(queued - 2).unwrap();
    assert_eq!(
        store.prune_dead_outbox(now - 7 * day).await.unwrap(),
        expected
    );
    let left: Vec<_> = store
        .outbox_for(&dead, 1000, 1000, &[])
        .await
        .unwrap()
        .into_iter()
        .map(|item| item.kind)
        .collect();
    let newest =
        |room: &RoomDriver| OutboxKind::Pdu(room.events.last().unwrap().event_id().to_owned());
    assert_eq!(left, vec![newest(&room_a), newest(&room_b)]);
    // The others are untouched, and a second run finds nothing more.
    assert_eq!(
        store
            .outbox_for(&flaky, 1000, 1000, &[])
            .await
            .unwrap()
            .len(),
        queued
    );
    assert_eq!(
        store
            .outbox_for(&fine, 1000, 1000, &[])
            .await
            .unwrap()
            .len(),
        queued
    );
    assert_eq!(store.prune_dead_outbox(now - 7 * day).await.unwrap(), 0);
}

/// The sender's view of a queue: events never wait behind EDUs, and of the EDUs the kinds
/// that must not wait come first.
#[sqlx::test(migrations = "./migrations")]
async fn outbox_puts_events_and_urgent_edus_first(pool: PgPool) {
    use asaroton_store::{FederationStore, OutboxKind};

    let store = PostgresStore::from_pool(pool);
    let room = build_room(&store, &RoomVersionId::V11).await;
    let dest = [server_name!("remote.example").to_owned()];
    let edu = |kind: &str, n: u32| json!({"edu_type": kind, "content": {"n": n}});
    // A backlog of receipts, then a to-device message, a malformed EDU and two events.
    for n in 0..5 {
        store
            .enqueue_edu(&dest, &edu("m.receipt", n))
            .await
            .unwrap();
    }
    store
        .enqueue_edu(&dest, &edu("m.direct_to_device", 5))
        .await
        .unwrap();
    store
        .enqueue_edu(&dest, &json!({"content": {"n": 6}}))
        .await
        .unwrap();
    store
        .enqueue_edu(&dest, &edu("m.direct_to_device", 7))
        .await
        .unwrap();
    for event in &room.events[..2] {
        store.enqueue_pdu(&dest, event.event_id()).await.unwrap();
    }

    let order = |items: Vec<asaroton_store::OutboxItem>| -> Vec<String> {
        items
            .into_iter()
            .map(|item| match item.kind {
                OutboxKind::Pdu(_) => "pdu".to_owned(),
                OutboxKind::Edu(e) => e["content"]["n"].to_string(),
            })
            .collect()
    };
    let first = ["m.direct_to_device"];
    let items = store.outbox_for(&dest[0], 50, 100, &first).await.unwrap();
    assert!(items.iter().all(|i| i.created_ts > 0));
    assert_eq!(
        order(items),
        ["pdu", "pdu", "5", "7", "0", "1", "2", "3", "4", "6"]
    );
    // The limits apply to each kind on its own.
    let items = store.outbox_for(&dest[0], 1, 3, &first).await.unwrap();
    assert_eq!(order(items), ["pdu", "5", "7", "0"]);
    // Without urgent kinds, plain queue order.
    let items = store.outbox_for(&dest[0], 0, 3, &[]).await.unwrap();
    assert_eq!(order(items), ["0", "1", "2"]);
}

/// A room row whose creator cannot be read (Synapse's empty string, copied by an early import)
/// still loads; the creator is simply unknown.
#[sqlx::test(migrations = "./migrations")]
async fn a_room_with_an_unreadable_creator_still_loads(pool: PgPool) {
    let store = PostgresStore::from_pool(pool.clone());
    for (room, creator) in [
        ("!empty:example.org", Some("")),
        ("!odd:example.org", Some("not a user id")),
        ("!none:example.org", None),
        ("!fine:example.org", Some("@alice:example.org")),
    ] {
        sqlx::query("INSERT INTO rooms (room_id, room_version, creator, created_ts) VALUES ($1, '11', $2, 1)")
            .bind(room)
            .bind(creator)
            .execute(&pool)
            .await
            .unwrap();
        let room_id: OwnedRoomId = room.try_into().unwrap();
        let info = store.get_room(&room_id).await.unwrap().unwrap();
        let expected =
            (room == "!fine:example.org").then(|| user_id!("@alice:example.org").to_owned());
        assert_eq!(info.creator, expected, "{room}");
    }
}

/// A queued typing notice is replaced only by a newer one about the same user in the same
/// room. Replacing per room dropped one user's "stopped typing" when another user in the room
/// started, and the other servers kept showing the first one as typing.
#[sqlx::test(migrations = "./migrations")]
async fn queued_typing_notices_are_replaced_per_user_and_room(pool: PgPool) {
    use asaroton_store::{FederationStore, OutboxKind};
    let store = PostgresStore::from_pool(pool);
    let dest = [server_name!("remote.example.org").to_owned()];
    let typing = |room: &str, user: &str, typing: bool| json!({"edu_type": "m.typing", "content": {"room_id": room, "user_id": user, "typing": typing}});
    for edu in [
        typing("!r:example.org", "@a:example.org", true),
        typing("!r:example.org", "@a:example.org", false), // replaces the line above
        typing("!r:example.org", "@b:example.org", true),  // another user: both stay
        typing("!other:example.org", "@a:example.org", true), // another room: stays
        typing("!r:example.org", "@b:example.org", false), // replaces b's "typing"
    ] {
        store.enqueue_edu(&dest, &edu).await.unwrap();
    }
    let queued: Vec<(String, String, bool)> = store
        .outbox_for(&dest[0], 100, 100, &[])
        .await
        .unwrap()
        .into_iter()
        .map(|item| match item.kind {
            OutboxKind::Edu(e) => (
                e["content"]["room_id"].as_str().unwrap().to_owned(),
                e["content"]["user_id"].as_str().unwrap().to_owned(),
                e["content"]["typing"].as_bool().unwrap(),
            ),
            OutboxKind::Pdu(_) => panic!("no events were queued"),
        })
        .collect();
    let row = |room: &str, user: &str, typing: bool| (room.to_owned(), user.to_owned(), typing);
    assert_eq!(
        queued,
        vec![
            row("!r:example.org", "@a:example.org", false),
            row("!other:example.org", "@a:example.org", true),
            row("!r:example.org", "@b:example.org", false),
        ]
    );
}

/// An event stored as rejected because it could not be checked takes its place once it passes:
/// a fresh position in the stream, its rows in the relation and search indexes, and the
/// rejection gone. The reverse never happens here: an accepted event stays accepted.
#[sqlx::test(migrations = "./migrations")]
async fn a_rejection_is_taken_back_when_the_event_passes_later(pool: PgPool) {
    let store = PostgresStore::from_pool(pool.clone());
    let d = build_room(&store, &RoomVersionId::V10).await;
    let room_id = d.room_id.clone().unwrap();
    let group = d.state_group.unwrap();
    let bob = user_id!("@bob:example.org");
    let auth_ids: Vec<OwnedEventId> = d
        .events
        .iter()
        .take(3)
        .map(|e| e.event_id().to_owned())
        .collect();
    let message = |body: &str, relates_to: Option<&OwnedEventId>| {
        let mut content = json!({ "msgtype": "m.text", "body": body });
        if let Some(target) = relates_to {
            content["m.relates_to"] = json!({ "rel_type": "m.thread", "event_id": target });
        }
        PduBuilder::new(bob.to_owned(), TimelineEventType::RoomMessage)
            .room_id(room_id.clone())
            .content(&content)
            .unwrap()
            .prev_events(d.last.clone())
            .auth_events(auth_ids.clone())
            .depth(UInt::from(50u32))
            .build(&d.rules, &d.key, server_name!("example.org"))
            .unwrap()
    };
    let persist = |pdu: Pdu, state: EventState| {
        let store = store.clone();
        async move {
            store
                .persist_event(PersistEvent {
                    pdu,
                    state,
                    soft_failed: false,
                    backfilled: false,
                    stream_ordering: None,
                    current_state_delta: None,
                    forward_extremities: None,
                })
                .await
                .unwrap()
        }
    };
    let count = |table: &'static str, id: OwnedEventId| {
        let pool = pool.clone();
        async move {
            sqlx::query_scalar::<_, i64>(sqlx::AssertSqlSafe(format!(
                "SELECT count(*) FROM {table} WHERE event_id = $1"
            )))
            .bind(id.as_str())
            .fetch_one(&pool)
            .await
            .unwrap()
        }
    };

    // Set aside: stored as a rejected outlier, and listed.
    let aside = message("could not be checked at first", None);
    persist(aside.with_rejected(true), EventState::Outlier).await;
    let now = now_ts();
    let origin = server_name!("remote.example");
    store
        .await_auth(aside.event_id(), &room_id, origin, now, now + 120_000)
        .await
        .unwrap();
    // Listing it again changes nothing.
    store
        .await_auth(aside.event_id(), &room_id, origin, now + 5, now + 5)
        .await
        .unwrap();
    assert!(
        store
            .events_awaiting_auth_due(now + 1000, 10)
            .await
            .unwrap()
            .is_empty()
    );
    let due = store
        .events_awaiting_auth_due(now + 120_000, 10)
        .await
        .unwrap();
    assert_eq!(due.len(), 1);
    assert_eq!(
        (
            &due[0].event_id,
            &due[0].room_id,
            due[0].first_ts,
            due[0].tries
        ),
        (&aside.event_id().to_owned(), &room_id, now, 0)
    );
    assert_eq!(due[0].origin, origin);
    store
        .reschedule_awaiting_auth(aside.event_id(), now + 600_000)
        .await
        .unwrap();
    assert!(
        store
            .events_awaiting_auth_due(now + 120_000, 10)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        store
            .events_awaiting_auth_due(now + 600_000, 10)
            .await
            .unwrap()[0]
            .tries,
        1
    );

    // An event that cites it was rejected in the timeline meanwhile.
    let reply = message("a reply in a thread", Some(&aside.event_id().to_owned()));
    let first = persist(reply.with_rejected(true), EventState::Existing(group)).await;
    let listed = store.rejected_timeline_events(&room_id, 0).await.unwrap();
    assert_eq!(
        listed.iter().map(|e| e.event_id()).collect::<Vec<_>>(),
        vec![reply.event_id()],
        "the outlier is not part of the timeline"
    );
    assert!(
        store
            .rejected_timeline_events(&room_id, now_ts() + 60_000)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        count("event_relations", reply.event_id().to_owned()).await,
        0
    );
    assert_eq!(count("event_search", aside.event_id().to_owned()).await, 0);

    // The event that was set aside passes now.
    let placed = persist(aside.clone(), EventState::Existing(group)).await;
    let stored = store.get_event(aside.event_id()).await.unwrap().unwrap();
    assert!(!stored.outlier && !stored.pdu.rejected());
    assert!(placed.stream_ordering > first.stream_ordering);
    assert_eq!(count("event_search", aside.event_id().to_owned()).await, 1);

    // And so does the one that cited it: a new position, since sync has moved past the old.
    let again = persist(reply.clone(), EventState::Existing(group)).await;
    let stored = store.get_event(reply.event_id()).await.unwrap().unwrap();
    assert!(!stored.outlier && !stored.pdu.rejected());
    assert!(again.stream_ordering > placed.stream_ordering);
    assert_eq!(
        count("event_relations", reply.event_id().to_owned()).await,
        1
    );
    assert!(
        store
            .rejected_timeline_events(&room_id, 0)
            .await
            .unwrap()
            .is_empty()
    );
    let timeline = store
        .room_events_in_range(&room_id, 0, None, Direction::Forwards, 100)
        .await
        .unwrap();
    for pdu in [&aside, &reply] {
        assert!(timeline.iter().any(|e| e.event_id() == pdu.event_id()));
    }

    // Persisting an accepted event again as rejected does not reject it, in the timeline...
    let unchanged = persist(reply.with_rejected(true), EventState::Existing(group)).await;
    assert_eq!(unchanged.stream_ordering, again.stream_ordering);
    assert!(
        !store
            .get_event(reply.event_id())
            .await
            .unwrap()
            .unwrap()
            .pdu
            .rejected()
    );
    // ...and an accepted outlier that fails when it is to be placed stays what it was: not
    // in the timeline, and not rejected (other events may rest on it as an auth event).
    let outlier = message("an accepted outlier", None);
    persist(outlier.clone(), EventState::Outlier).await;
    persist(outlier.with_rejected(true), EventState::Existing(group)).await;
    let stored = store.get_event(outlier.event_id()).await.unwrap().unwrap();
    assert!(stored.outlier && !stored.pdu.rejected() && stored.state_group.is_none());

    // Who has an accepted event building on it is known: `reply` names the room's last
    // event as its prev event, and nothing names `reply`.
    assert!(store.has_accepted_child(&d.last[0]).await.unwrap());
    assert!(!store.has_accepted_child(reply.event_id()).await.unwrap());
    // A rejected event that builds on one does not count.
    let rejected_child = PduBuilder::new(bob.to_owned(), TimelineEventType::RoomMessage)
        .room_id(room_id.clone())
        .content(&json!({ "msgtype": "m.text", "body": "builds on the reply" }))
        .unwrap()
        .prev_events(vec![reply.event_id().to_owned()])
        .auth_events(auth_ids.clone())
        .depth(UInt::from(51u32))
        .build(&d.rules, &d.key, server_name!("example.org"))
        .unwrap();
    persist(
        rejected_child.with_rejected(true),
        EventState::Existing(group),
    )
    .await;
    assert!(!store.has_accepted_child(reply.event_id()).await.unwrap());

    // Taking a rejection back by ID writes the index rows too.
    let third = message("another reply", Some(&aside.event_id().to_owned()));
    persist(third.with_rejected(true), EventState::Existing(group)).await;
    assert_eq!(
        count("event_relations", third.event_id().to_owned()).await,
        0
    );
    assert_eq!(
        store
            .clear_rejected(&[third.event_id().to_owned(), reply.event_id().to_owned()])
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        count("event_relations", third.event_id().to_owned()).await,
        1
    );
    assert_eq!(count("event_search", third.event_id().to_owned()).await, 1);

    // The room's listed events can be read with their events, and made due at once.
    let set_aside = store.events_set_aside(&room_id).await.unwrap();
    assert_eq!(
        set_aside.iter().map(|e| e.event_id()).collect::<Vec<_>>(),
        vec![aside.event_id()]
    );
    store.hasten_awaiting_auth(&room_id, now).await.unwrap();
    assert_eq!(
        store.events_awaiting_auth_due(now, 10).await.unwrap().len(),
        1
    );

    store.forget_awaiting_auth(aside.event_id()).await.unwrap();
    assert!(
        store
            .events_awaiting_auth_due(i64::MAX, 10)
            .await
            .unwrap()
            .is_empty()
    );
}

/// A stream position is not reported past a write that is still under way.
///
/// Two rooms. An event in the first has taken its number and is held up before its
/// transaction commits; an event in the second, with the next number, is committed. A sync
/// that were told the higher number now would look for events up to it, not see the first
/// room's event, and continue after it for good.
#[sqlx::test(migrations = "./migrations")]
async fn a_position_is_not_reported_past_a_write_that_is_under_way(pool: PgPool) {
    use asaroton_store::SyncStream;
    let store = PostgresStore::from_pool(pool.clone());
    let one = build_room(&store, &RoomVersionId::V10).await;
    let two = build_room(&store, &RoomVersionId::V10).await;
    let before = store.stream_positions().await.unwrap().events;

    // The first room's write stores its event, which takes a number, and then goes on to
    // replace the room's forward extremities. That table is locked here against writing
    // (reading stays possible), so it waits there.
    let lock = lock_against_writing(&pool, "forward_extremities").await;
    let held_event = message_in(&one, "held up");
    let held = write_in_a_task(
        &store,
        held_event.clone(),
        one.state_group.unwrap(),
        true,
        None,
    );
    until(
        &pool,
        "SELECT last_value > $1 FROM events_stream_seq",
        before,
    )
    .await;

    // The second room's event does not touch that table, and is committed. Its write does
    // not wait for the first one.
    let free_event = message_in(&two, "free to go");
    let free = write_in_a_task(
        &store,
        free_event.clone(),
        two.state_group.unwrap(),
        false,
        None,
    );
    let free = tokio::time::timeout(std::time::Duration::from_secs(5), free)
        .await
        .expect("a write does not wait for the one before it")
        .unwrap();

    let at = store.stream_positions().await.unwrap().events;
    assert_eq!(
        at, before,
        "the position stays below the event that is being written"
    );
    assert!(
        store.events_after(before, 10).await.unwrap().is_empty(),
        "whoever follows the stream is not handed the later event ahead of the earlier one"
    );
    // Whoever wakes syncs for the later event has to wait.
    assert!(!store.stream_position_reportable(SyncStream::Events, free.stream_ordering));
    let woken = {
        let store = store.clone();
        tokio::spawn(async move {
            store
                .stream_position_settles(SyncStream::Events, free.stream_ordering)
                .await;
        })
    };
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(!woken.is_finished());

    lock.rollback().await.unwrap();
    let held = held.await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), woken)
        .await
        .expect("the wake-up for the later event goes out when the earlier one is done")
        .unwrap();
    assert!(held.stream_ordering < free.stream_ordering);
    assert!(store.stream_position_reportable(SyncStream::Events, free.stream_ordering));
    assert_eq!(
        store.stream_positions().await.unwrap().events,
        free.stream_ordering
    );
    let fed: Vec<OwnedEventId> = store
        .events_after(before, 10)
        .await
        .unwrap()
        .into_iter()
        .map(|e| e.pdu.event_id().to_owned())
        .collect();
    assert_eq!(
        fed,
        vec![
            held_event.event_id().to_owned(),
            free_event.event_id().to_owned()
        ]
    );
}

/// An invitation from another server is stored as an outlier, in no timeline, but the user's
/// membership carries its number and sync goes by that. Such a write holds the position too.
#[sqlx::test(migrations = "./migrations")]
async fn a_membership_stored_as_an_outlier_holds_the_position_too(pool: PgPool) {
    let store = PostgresStore::from_pool(pool.clone());
    let one = build_room(&store, &RoomVersionId::V10).await;
    let two = build_room(&store, &RoomVersionId::V10).await;
    let before = store.stream_positions().await.unwrap().events;
    let carol = user_id!("@carol:example.org");
    let invitation = PduBuilder::new(
        user_id!("@alice:example.org").to_owned(),
        TimelineEventType::RoomMember,
    )
    .room_id(one.room_id.clone().unwrap())
    .state_key(carol.as_str())
    .content(&json!({ "membership": "invite" }))
    .unwrap()
    .prev_events(one.last.clone())
    .auth_events(one.events.iter().take(3).map(|e| e.event_id().to_owned()))
    .depth(UInt::from(50u32))
    .build(&one.rules, &one.key, server_name!("example.org"))
    .unwrap();
    let delta = CurrentStateDelta {
        upsert: vec![(
            (StateEventType::RoomMember, carol.to_string()),
            invitation.event_id().to_owned(),
        )],
        remove: vec![],
    };

    // The membership is written after the event has its number; that is where it waits.
    let lock = lock_against_writing(&pool, "room_memberships").await;
    let held = {
        let store = store.clone();
        tokio::spawn(async move {
            store
                .persist_event(PersistEvent {
                    pdu: invitation,
                    state: EventState::Outlier,
                    soft_failed: false,
                    backfilled: false,
                    stream_ordering: None,
                    current_state_delta: Some(delta),
                    forward_extremities: None,
                })
                .await
                .unwrap()
        })
    };
    until(
        &pool,
        "SELECT last_value > $1 FROM events_stream_seq",
        before,
    )
    .await;
    let free = write_in_a_task(
        &store,
        message_in(&two, "free to go"),
        two.state_group.unwrap(),
        false,
        None,
    )
    .await
    .unwrap();
    assert_eq!(
        store.stream_positions().await.unwrap().events,
        before,
        "the position stays below the invitation that is being written"
    );
    lock.rollback().await.unwrap();
    let held = held.await.unwrap();
    assert!(held.stream_ordering < free.stream_ordering);
    let invited = store
        .memberships_for_user(carol)
        .await
        .unwrap()
        .into_iter()
        .find(|m| m.room_id == *one.room_id.as_ref().unwrap())
        .expect("the invitation is stored");
    assert_eq!(invited.stream_ordering, held.stream_ordering);
    assert_eq!(
        store.stream_positions().await.unwrap().events,
        free.stream_ordering
    );
}

/// The same for a stream that is written with single statements: account data and push rules
/// share one.
#[sqlx::test(migrations = "./migrations")]
async fn account_data_is_not_reported_past_a_write_that_is_under_way(pool: PgPool) {
    use asaroton_store::E2eeStore;
    let store = PostgresStore::from_pool(pool.clone());
    let alice = user_id!("@alice:example.org");
    store.set_push_rules(alice, &json!({})).await.unwrap();
    let before = store.stream_positions().await.unwrap().account_data;

    // A change of push rules takes its number and then has to replace the user's row, which
    // is locked here.
    let mut lock = pool.begin().await.unwrap();
    sqlx::query("SELECT 1 FROM push_rules WHERE user_id = $1 FOR UPDATE")
        .bind(alice.as_str())
        .execute(&mut *lock)
        .await
        .unwrap();
    let held = {
        let store = store.clone();
        tokio::spawn(async move {
            store
                .set_push_rules(alice, &json!({ "n": 1 }))
                .await
                .unwrap()
        })
    };
    until(
        &pool,
        "SELECT last_value > $1 FROM account_data_seq",
        before,
    )
    .await;
    // Account data written meanwhile gets the next number and is committed.
    let free = store
        .set_account_data(alice, None, "m.test", &json!({ "n": 1 }))
        .await
        .unwrap();
    assert_eq!(
        store.stream_positions().await.unwrap().account_data,
        before,
        "the position waits for the push rules"
    );

    lock.rollback().await.unwrap();
    let held = held.await.unwrap();
    assert!(held < free);
    assert_eq!(store.stream_positions().await.unwrap().account_data, free);
}

/// A call that is dropped while its transaction begins leaves the connection as it found it.
///
/// With sqlx 0.9.0 it did not: a call dropped between sending BEGIN and reading the answer
/// gave its connection back to the pool inside a transaction, and whatever the next user
/// wrote on it stayed invisible to every other connection. The server's requests are such
/// calls: a client that hangs up takes its request down wherever it is. Seen from outside, a
/// device that `/login` had just handed out was not there for the next request.
///
/// The store has one connection here, so that each write gets the one the dropped call had.
#[sqlx::test(migrations = "./migrations")]
async fn a_call_dropped_while_its_transaction_begins_leaves_the_connection_clean(
    options: sqlx::postgres::PgPoolOptions,
    database: sqlx::postgres::PgConnectOptions,
) {
    let pool = options
        .clone()
        .max_connections(1)
        .connect_with(database.clone())
        .await
        .unwrap();
    let store = PostgresStore::from_pool(pool);
    let elsewhere = PostgresStore::from_pool(
        options
            .max_connections(1)
            .connect_with(database)
            .await
            .unwrap(),
    );
    let alice = user_id!("@alice:example.org");
    store
        .create_user(NewUser {
            user_id: alice.to_owned(),
            password_hash: None,
            display_name: None,
            is_admin: false,
            is_guest: false,
            appservice_id: None,
        })
        .await
        .unwrap();

    // Dropped after one step, after two, and so on, with time in between for the database to
    // answer: one of them is the step that leaves BEGIN sent and not yet answered.
    for steps in 1..=8 {
        {
            let mut call = Box::pin(store.delete_device(alice, device_id!("NOSUCHDEVICE")));
            for _ in 0..steps {
                if futures::poll!(call.as_mut()).is_ready() {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        }
        let device = asaroton_core::ruma::OwnedDeviceId::from(format!("DEVICE{steps}"));
        store.create_device(alice, &device, None).await.unwrap();
        assert!(
            elsewhere
                .get_device(alice, &device)
                .await
                .unwrap()
                .is_some(),
            "a device created after a call that was dropped at step {steps} is there for another connection"
        );
        let inside: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pg_stat_activity
             WHERE datname = current_database() AND state LIKE 'idle in transaction%'",
        )
        .fetch_one(elsewhere.pool())
        .await
        .unwrap();
        assert_eq!(
            inside, 0,
            "no connection is left inside a transaction (dropped at step {steps})"
        );
    }
}

/// A refresh token is kept until what replaced it is used, and then goes together with the
/// access tokens that were handed out with it.
#[sqlx::test(migrations = "./migrations")]
async fn a_refresh_token_goes_when_what_replaced_it_is_used(pool: PgPool) {
    use asaroton_store::RefreshToken;
    let store = PostgresStore::from_pool(pool);
    let alice = user_id!("@alice:example.org");
    let device = device_id!("PHONE");
    store
        .create_user(NewUser {
            user_id: alice.to_owned(),
            password_hash: None,
            display_name: None,
            is_admin: false,
            is_guest: false,
            appservice_id: None,
        })
        .await
        .unwrap();
    store.create_device(alice, device, None).await.unwrap();
    let now = now_ts();
    let refresh = |token: &str, next: Option<&str>| RefreshToken {
        token: token.into(),
        user_id: alice.to_owned(),
        device_id: device.to_owned(),
        created_ts: now,
        expires_ts: None,
        next_token: next.map(Into::into),
    };
    let access = |token: &str, with: &str, created_ts: i64, expires_ts: i64| AccessToken {
        token: token.into(),
        user_id: alice.to_owned(),
        device_id: Some(device.to_owned()),
        created_ts,
        expires_ts: Some(expires_ts),
        refresh_token: Some(with.into()),
        puppets_user_id: None,
    };
    // The old pair, the pair that replaced it, and a token that has nothing to do with them.
    store
        .create_refresh_token(refresh("r1", Some("r2")))
        .await
        .unwrap();
    store
        .create_refresh_token(refresh("r2", None))
        .await
        .unwrap();
    for token in [
        access("a1", "r1", now, now + 60_000),
        access("a2-expired", "r2", now - 2, now - 1),
        access("a2-older", "r2", now - 1, now + 60_000),
        access("a2", "r2", now, now + 60_000),
    ] {
        store.create_access_token(token).await.unwrap();
    }
    store
        .create_access_token(AccessToken {
            token: "plain".into(),
            user_id: alice.to_owned(),
            device_id: Some(device.to_owned()),
            created_ts: now,
            expires_ts: None,
            refresh_token: None,
            puppets_user_id: None,
        })
        .await
        .unwrap();

    let handed_out = store.access_token_for_refresh_token("r2").await.unwrap();
    assert_eq!(
        handed_out.map(|t| t.token).as_deref(),
        Some("a2"),
        "the newest that has not expired"
    );
    assert!(
        store
            .access_token_for_refresh_token("none")
            .await
            .unwrap()
            .is_none()
    );

    // Nothing replaced r1, so using it revokes nothing.
    assert!(
        store
            .revoke_refresh_token_predecessor("r1")
            .await
            .unwrap()
            .is_empty()
    );
    assert!(store.get_refresh_token("r1").await.unwrap().is_some());
    // Using r2, or an access token that came with it, ends r1 and a1.
    assert_eq!(
        store.revoke_refresh_token_predecessor("r2").await.unwrap(),
        vec!["a1".to_owned()]
    );
    assert!(store.get_refresh_token("r1").await.unwrap().is_none());
    assert!(store.get_access_token("a1").await.unwrap().is_none());
    assert!(store.get_refresh_token("r2").await.unwrap().is_some());
    assert!(store.get_access_token("a2").await.unwrap().is_some());
    assert!(store.get_access_token("plain").await.unwrap().is_some());
    assert!(
        store
            .revoke_refresh_token_predecessor("r2")
            .await
            .unwrap()
            .is_empty(),
        "once"
    );
}

/// A table lock that keeps writers out and lets readers in, for as long as the returned
/// transaction is open.
async fn lock_against_writing(
    pool: &PgPool,
    table: &str,
) -> sqlx::Transaction<'static, sqlx::Postgres> {
    let mut lock = pool.begin().await.unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "LOCK TABLE {table} IN SHARE ROW EXCLUSIVE MODE"
    )))
    .execute(&mut *lock)
    .await
    .unwrap();
    lock
}

/// Waits until a query with one number as its parameter answers true.
async fn until(pool: &PgPool, sql: &'static str, n: i64) {
    for _ in 0..500 {
        if sqlx::query_scalar::<_, bool>(sql)
            .bind(n)
            .fetch_one(pool)
            .await
            .unwrap()
        {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("timed out waiting for: {sql}");
}

/// A message of Bob's in the room, building on its newest event.
fn message_in(d: &RoomDriver, body: &str) -> Pdu {
    PduBuilder::new(
        user_id!("@bob:example.org").to_owned(),
        TimelineEventType::RoomMessage,
    )
    .room_id(d.room_id.clone().unwrap())
    .content(&json!({ "msgtype": "m.text", "body": body }))
    .unwrap()
    .prev_events(d.last.clone())
    .auth_events(d.events.iter().take(3).map(|e| e.event_id().to_owned()))
    .depth(UInt::from(50u32))
    .build(&d.rules, &d.key, server_name!("example.org"))
    .unwrap()
}

/// Stores a timeline event from a task of its own, with or without new forward extremities.
fn write_in_a_task(
    store: &PostgresStore,
    pdu: Pdu,
    group: i64,
    with_extremities: bool,
    delta: Option<CurrentStateDelta>,
) -> tokio::task::JoinHandle<asaroton_store::PersistedEvent> {
    let store = store.clone();
    tokio::spawn(async move {
        let extremities = with_extremities.then(|| vec![pdu.event_id().to_owned()]);
        store
            .persist_event(PersistEvent {
                pdu,
                state: EventState::Existing(group),
                soft_failed: false,
                backfilled: false,
                stream_ordering: None,
                current_state_delta: delta,
                forward_extremities: extremities,
            })
            .await
            .unwrap()
    })
}
