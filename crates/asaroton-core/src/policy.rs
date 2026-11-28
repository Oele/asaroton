//! Policy servers (Matrix v1.18, MSC4284): whether a room uses one, which events it signs, and
//! checking its signature. Asking it is the federation crate's job; nothing here does I/O.
//!
//! A room uses a policy server when its current state has a valid `m.room.policy` event with
//! an empty state key (a string `via` and a string `public_keys.ed25519`) and the server
//! named by `via` has a joined member. Every event in such a room, except that
//! `m.room.policy` event itself, should carry the policy server's signature under the key ID
//! `ed25519:policy_server`, made with the key from the state event. That key is not one the
//! server publishes, so the ordinary signature check skips it.

use std::collections::HashMap;

use ruma::{
    CanonicalJsonObject, OwnedServerName, ServerName,
    events::{StateEventType, TimelineEventType, room::policy::RoomPolicyEventContent},
    room_version_rules::RoomVersionRules,
    serde::Base64,
};
use serde_json::value::RawValue;

use crate::Pdu;

/// The key ID of every policy server signature.
pub const KEY_ID: &str = ruma::events::room::policy::POLICY_SERVER_ED25519_SIGNING_KEY_ID;

/// A room's policy server, as its `m.room.policy` state event names it.
#[derive(Clone, Debug)]
pub struct RoomPolicy {
    content: RoomPolicyEventContent,
}

impl RoomPolicy {
    /// Reads the content of an `m.room.policy` event. `None` when it is not valid, which means
    /// the room uses no policy server: `via` and `public_keys.ed25519` must be strings. A `via`
    /// that is no server name counts as invalid too, as no user of it can be joined.
    ///
    /// A key that is a string but not base64 still makes a valid event (the room uses the
    /// policy server); no signature can be valid for it.
    pub fn from_content(content: &RawValue) -> Option<Self> {
        #[derive(serde::Deserialize)]
        struct Content<'a> {
            via: Option<serde_json::Value>,
            #[serde(borrow)]
            public_keys: Option<&'a RawValue>,
        }
        let content: Content<'_> = serde_json::from_str(content.get()).ok()?;
        let via = content.via?;
        let via = OwnedServerName::try_from(via.as_str()?).ok()?;
        let keys: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(content.public_keys?.get()).ok()?;
        let key = keys.get("ed25519")?.as_str()?;
        let key = Base64::parse(key).unwrap_or_else(|_| Base64::new(Vec::new()));
        Some(Self {
            content: RoomPolicyEventContent::new(via, key),
        })
    }

    /// The policy server: the server to ask, and whose name its signature goes under.
    pub fn via(&self) -> &ServerName {
        &self.content.via
    }

    /// Whether `event` carries a valid signature of this policy server, made with the key
    /// the room names. (True for the room's `m.room.policy` state event, which needs none.)
    pub fn signed(&self, event: &CanonicalJsonObject, rules: &RoomVersionRules) -> bool {
        ruma::signatures::verify_policy_server_signature(&self.content, event, rules).is_ok()
    }
}

/// Whether the policy server signs an event of this type and state key: every event except
/// the room's `m.room.policy` state event (the one with an empty state key). Membership and
/// power level changes, other `m.room.policy` state keys and non-state `m.room.policy`
/// events are all signed.
pub fn needs_signature(event_type: &TimelineEventType, state_key: Option<&str>) -> bool {
    !(*event_type == TimelineEventType::RoomPolicy && state_key == Some(""))
}

/// The policy server a room uses, judged by `state`, its current state. For a room without
/// an `m.room.policy` event (nearly all of them) this is a single lookup; only with one are
/// the members of `via` looked for.
pub fn in_use(state: &HashMap<(StateEventType, String), Pdu>) -> Option<RoomPolicy> {
    let event = state.get(&(StateEventType::RoomPolicy, String::new()))?;
    let policy = RoomPolicy::from_content(event.content())?;
    has_joined_member(state, policy.via()).then_some(policy)
}

/// Whether a user of `server` is joined according to `state`.
fn has_joined_member(state: &HashMap<(StateEventType, String), Pdu>, server: &ServerName) -> bool {
    state.iter().any(|((ty, key), pdu)| {
        *ty == StateEventType::RoomMember
            && key
                .split_once(':')
                .is_some_and(|(_, name)| name == server.as_str())
            && is_join(pdu.content())
    })
}

/// Whether a membership content says `join`.
pub fn is_join(content: &RawValue) -> bool {
    #[derive(serde::Deserialize)]
    struct Member<'a> {
        #[serde(borrow)]
        membership: Option<&'a str>,
    }
    serde_json::from_str::<Member<'_>>(content.get()).is_ok_and(|m| m.membership == Some("join"))
}

#[cfg(test)]
mod tests {
    use ruma::{
        OwnedEventId, RoomVersionId, events::TimelineEventType, owned_room_id, server_name, user_id,
    };
    use serde_json::json;

    use super::*;
    use crate::{SigningKey, builder::PduBuilder, room_version};

    fn rules() -> RoomVersionRules {
        room_version::rules(&RoomVersionId::V11).unwrap()
    }

    fn raw(v: &serde_json::Value) -> Box<RawValue> {
        serde_json::value::to_raw_value(v).unwrap()
    }

    /// An event of `sender`'s server, signed with `key` as `origin`.
    fn event(
        key: &SigningKey,
        origin: &ServerName,
        sender: &ruma::UserId,
        ty: &str,
        state_key: Option<&str>,
        content: &serde_json::Value,
    ) -> Pdu {
        let prev: OwnedEventId = "$prev".try_into().unwrap();
        let mut b = PduBuilder::new(sender.to_owned(), TimelineEventType::from(ty))
            .room_id(owned_room_id!("!room:example.org"))
            .content(content)
            .unwrap()
            .depth(5u32.into())
            .prev_events(vec![crate::builder::EventRef {
                event_id: prev,
                sha256: None,
            }]);
        if let Some(k) = state_key {
            b = b.state_key(k);
        }
        b.build(&rules(), key, origin).unwrap()
    }

    fn policy_content(via: &str, key: &SigningKey) -> serde_json::Value {
        json!({ "via": via, "public_keys": { "ed25519": key.public_key_base64() } })
    }

    #[test]
    fn a_valid_policy_event_has_string_via_and_ed25519_key() {
        let key = SigningKey::generate("policy_server").unwrap();
        let ok = RoomPolicy::from_content(&raw(&policy_content("ps.example.org", &key))).unwrap();
        assert_eq!(ok.via(), "ps.example.org");
        // Extra fields and other key types do no harm.
        assert!(
            RoomPolicy::from_content(&raw(&json!({
                "via": "ps.example.org",
                "public_keys": { "ed25519": key.public_key_base64(), "curve25519": "abc" },
                "extra": true,
            })))
            .is_some()
        );
        for invalid in [
            json!({}),
            json!({ "via": "ps.example.org" }),
            json!({ "public_keys": { "ed25519": key.public_key_base64() } }),
            json!({ "via": 5, "public_keys": { "ed25519": key.public_key_base64() } }),
            json!({ "via": "ps.example.org", "public_keys": "nope" }),
            json!({ "via": "ps.example.org", "public_keys": {} }),
            json!({ "via": "ps.example.org", "public_keys": { "ed25519": 5 } }),
            json!({ "via": "not a server name!", "public_keys": { "ed25519": "abc" } }),
        ] {
            assert!(
                RoomPolicy::from_content(&raw(&invalid)).is_none(),
                "{invalid} is not valid"
            );
        }
        // A key that does not decode still counts: the room uses the policy server, and
        // nothing it signs can be valid.
        assert!(
            RoomPolicy::from_content(&raw(&json!({
                "via": "ps.example.org", "public_keys": { "ed25519": "!!!" }
            })))
            .is_some()
        );
    }

    #[test]
    fn only_the_rooms_own_policy_event_goes_unsigned() {
        let policy = TimelineEventType::RoomPolicy;
        assert!(!needs_signature(&policy, Some("")));
        assert!(needs_signature(&policy, Some("other")));
        assert!(needs_signature(&policy, None));
        assert!(needs_signature(
            &TimelineEventType::RoomMember,
            Some("@a:b")
        ));
        assert!(needs_signature(
            &TimelineEventType::RoomPowerLevels,
            Some("")
        ));
        assert!(needs_signature(&TimelineEventType::RoomMessage, None));
    }

    #[test]
    fn a_room_uses_its_policy_server_only_while_a_member_of_it_is_joined() {
        let server_key = SigningKey::generate("k1").unwrap();
        let policy_key = SigningKey::generate("policy_server").unwrap();
        let alice = user_id!("@alice:example.org");
        let ps = server_name!("ps.example.org");
        let mut state: HashMap<(StateEventType, String), Pdu> = HashMap::new();
        let put = |state: &mut HashMap<_, _>, ty: &str, key: &str, content| {
            let pdu = event(
                &server_key,
                server_name!("example.org"),
                alice,
                ty,
                Some(key),
                &content,
            );
            state.insert((StateEventType::from(ty), key.to_owned()), pdu);
        };
        put(
            &mut state,
            "m.room.member",
            alice.as_str(),
            json!({ "membership": "join" }),
        );
        assert!(in_use(&state).is_none(), "no m.room.policy at all");

        put(
            &mut state,
            "m.room.policy",
            "",
            policy_content(ps.as_str(), &policy_key),
        );
        assert!(in_use(&state).is_none(), "nobody of the policy server here");
        // Invited, left or of another server whose name only begins the same: not enough.
        put(
            &mut state,
            "m.room.member",
            "@bot:ps.example.org",
            json!({ "membership": "invite" }),
        );
        put(
            &mut state,
            "m.room.member",
            "@x:ps.example.org.evil",
            json!({ "membership": "join" }),
        );
        assert!(
            in_use(&state).is_none(),
            "invited and look-alike servers do not count"
        );

        put(
            &mut state,
            "m.room.member",
            "@bot:ps.example.org",
            json!({ "membership": "join" }),
        );
        assert_eq!(in_use(&state).unwrap().via(), ps);

        // Replaced by an invalid one, the room no longer uses it.
        put(
            &mut state,
            "m.room.policy",
            "",
            json!({ "via": ps.as_str() }),
        );
        assert!(
            in_use(&state).is_none(),
            "an invalid m.room.policy disables it"
        );
    }

    #[test]
    fn the_signature_must_be_made_with_the_rooms_key() {
        let origin_key = SigningKey::generate("k1").unwrap();
        let policy_key = SigningKey::generate("policy_server").unwrap();
        let ps = server_name!("ps.example.org");
        let policy =
            RoomPolicy::from_content(&raw(&policy_content(ps.as_str(), &policy_key))).unwrap();
        let ev = event(
            &origin_key,
            server_name!("example.org"),
            user_id!("@alice:example.org"),
            "m.room.message",
            None,
            &json!({ "body": "hello", "msgtype": "m.text" }),
        );
        assert!(!policy.signed(ev.raw(), &rules()), "unsigned");

        let mut signed = ev.raw().clone();
        policy_key.sign_event(ps, &mut signed, &rules()).unwrap();
        assert!(policy.signed(&signed, &rules()));

        // The signature added to the event as a policy server answer gives it.
        let sig = signed["signatures"].as_object().unwrap()[ps.as_str()]
            .as_object()
            .unwrap()[KEY_ID]
            .as_str()
            .unwrap()
            .to_owned();
        let with = ev.with_signature(ps.as_str(), KEY_ID, sig);
        assert_eq!(with.event_id(), ev.event_id(), "still the same event");
        assert!(policy.signed(with.raw(), &rules()));
        assert!(
            with.raw()["signatures"]
                .as_object()
                .unwrap()
                .contains_key("example.org"),
            "the origin's signature stays"
        );

        // Another key, or the right key under the wrong server name, does not do.
        let other = SigningKey::generate("policy_server").unwrap();
        let mut wrong = ev.raw().clone();
        other.sign_event(ps, &mut wrong, &rules()).unwrap();
        assert!(!policy.signed(&wrong, &rules()));
        let mut elsewhere = ev.raw().clone();
        policy_key
            .sign_event(server_name!("other.example.org"), &mut elsewhere, &rules())
            .unwrap();
        assert!(!policy.signed(&elsewhere, &rules()));

        // A signature that no longer matches the event (here: the content changed) fails.
        let mut tampered = signed.clone();
        tampered.insert("origin_server_ts".into(), ruma::int!(1).into());
        assert!(!policy.signed(&tampered, &rules()));
    }

    #[test]
    fn a_policy_server_signing_its_own_event_has_two_signatures_under_one_name() {
        // The policy server's user sends an event: its server key and its policy key both
        // sign under the same server name.
        let ps = server_name!("ps.example.org");
        let server_key = SigningKey::generate("k1").unwrap();
        let policy_key = SigningKey::generate("policy_server").unwrap();
        let policy =
            RoomPolicy::from_content(&raw(&policy_content(ps.as_str(), &policy_key))).unwrap();
        let ev = event(
            &server_key,
            ps,
            user_id!("@bot:ps.example.org"),
            "m.room.message",
            None,
            &json!({ "body": "hi" }),
        );
        let mut signed = ev.raw().clone();
        policy_key.sign_event(ps, &mut signed, &rules()).unwrap();
        let by_ps = signed["signatures"].as_object().unwrap()[ps.as_str()]
            .as_object()
            .unwrap();
        assert_eq!(by_ps.len(), 2);
        assert!(policy.signed(&signed, &rules()));
    }

    #[test]
    fn a_key_that_does_not_decode_validates_nothing() {
        let ps = server_name!("ps.example.org");
        let policy_key = SigningKey::generate("policy_server").unwrap();
        let broken = RoomPolicy::from_content(&raw(&json!({
            "via": ps.as_str(), "public_keys": { "ed25519": "!!!" }
        })))
        .unwrap();
        let ev = event(
            &SigningKey::generate("k1").unwrap(),
            server_name!("example.org"),
            user_id!("@alice:example.org"),
            "m.room.message",
            None,
            &json!({ "body": "x" }),
        );
        let mut signed = ev.raw().clone();
        policy_key.sign_event(ps, &mut signed, &rules()).unwrap();
        assert!(!broken.signed(&signed, &rules()));
    }
}
