//! The persistent data unit: a room event as exchanged between servers.
//!
//! [`Pdu`] is asaroton's own event type. It keeps the original canonical JSON (needed for
//! signatures, hashes and storage) alongside parsed copies of the fields the server reasons
//! about, and implements [`ruma::state_res::Event`] so it can be fed straight into the auth
//! rules and state resolution.

use std::{collections::BTreeSet, fmt, sync::Arc};

use ruma::{
    CanonicalJsonObject, CanonicalJsonValue, EventId, MilliSecondsSinceUnixEpoch, OwnedEventId,
    OwnedRoomId, OwnedUserId, RoomId, RoomVersionId, UInt, UserId,
    events::{StateEventType, TimelineEventType},
    room_version_rules::{EventIdFormatVersion, RoomIdFormatVersion, RoomVersionRules},
    signatures::reference_hash,
    state_res::Event,
};
use serde_json::value::RawValue;

use crate::{Error, Result, canonical};

/// A room event in its federation form, with derived fields.
#[derive(Clone)]
pub struct Pdu {
    inner: Arc<PduInner>,
}

struct PduInner {
    event_id: OwnedEventId,
    /// The room this event belongs to. Always known: for a v12 `m.room.create` event, which
    /// carries no `room_id` field, it is derived from the event ID.
    room_id: OwnedRoomId,
    /// Whether the JSON itself carries a `room_id` field (false for v12 create events).
    has_room_id_field: bool,
    sender: OwnedUserId,
    origin_server_ts: MilliSecondsSinceUnixEpoch,
    event_type: TimelineEventType,
    state_key: Option<String>,
    content: Box<RawValue>,
    prev_events: Vec<OwnedEventId>,
    auth_events: Vec<OwnedEventId>,
    depth: UInt,
    redacts: Option<OwnedEventId>,
    rejected: bool,
    /// The event exactly as signed, including `hashes`, `signatures` and `unsigned`.
    /// For room versions 3+, this does *not* contain `event_id`.
    raw: CanonicalJsonObject,
}

impl fmt::Debug for Pdu {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Pdu")
            .field("event_id", &self.inner.event_id)
            .field("room_id", &self.inner.room_id)
            .field("type", &self.inner.event_type)
            .field("state_key", &self.inner.state_key)
            .field("sender", &self.inner.sender)
            .field("rejected", &self.inner.rejected)
            .finish_non_exhaustive()
    }
}

impl Pdu {
    /// Build a `Pdu` from a canonical JSON object received over federation or loaded from
    /// storage, computing the event ID according to the room version.
    ///
    /// This performs *shape* validation only (the fields exist and have the right types). Size
    /// limits and auth rules are checked separately, see [`crate::auth`].
    pub fn from_canonical(mut raw: CanonicalJsonObject, rules: &RoomVersionRules) -> Result<Self> {
        let event_id = if rules.event_id_format == EventIdFormatVersion::V1 {
            let id = required_str(&raw, "event_id")?;
            OwnedEventId::try_from(id)?
        } else {
            // Room versions 3+ do not carry the event ID on the wire; remove any stray copy
            // so it never takes part in hashing or signing.
            raw.remove("event_id");
            let hash = reference_hash(&raw, rules)?;
            OwnedEventId::try_from(format!("${hash}"))?
        };

        Self::parse(raw, rules, event_id)
    }

    fn parse(
        raw: CanonicalJsonObject,
        rules: &RoomVersionRules,
        event_id: OwnedEventId,
    ) -> Result<Self> {
        let event_type: TimelineEventType = required_str(&raw, "type")?.into();
        let sender = OwnedUserId::try_from(required_str(&raw, "sender")?)?;

        let has_room_id_field = raw.contains_key("room_id");
        let room_id = match raw.get("room_id") {
            Some(CanonicalJsonValue::String(s)) => OwnedRoomId::try_from(s.as_str())?,
            Some(_) => return Err(Error::InvalidPdu("`room_id` must be a string".into())),
            None if event_type == TimelineEventType::RoomCreate
                && rules.room_id_format == RoomIdFormatVersion::V2 =>
            {
                // v12: the room ID is the create event's reference hash with a `!` sigil.
                OwnedRoomId::try_from(format!("!{}", event_id.localpart()))?
            }
            None => return Err(Error::InvalidPdu("missing `room_id`".into())),
        };

        let state_key = match raw.get("state_key") {
            None => None,
            Some(CanonicalJsonValue::String(s)) => Some(s.clone()),
            Some(_) => return Err(Error::InvalidPdu("`state_key` must be a string".into())),
        };

        let origin_server_ts = match raw.get("origin_server_ts") {
            Some(CanonicalJsonValue::Integer(i)) => MilliSecondsSinceUnixEpoch(
                UInt::try_from(i64::from(*i))
                    .map_err(|_| Error::InvalidPdu("`origin_server_ts` is negative".into()))?,
            ),
            _ => {
                return Err(Error::InvalidPdu(
                    "missing or invalid `origin_server_ts`".into(),
                ));
            }
        };

        let depth = match raw.get("depth") {
            Some(CanonicalJsonValue::Integer(i)) => UInt::try_from(i64::from(*i))
                .map_err(|_| Error::InvalidPdu("`depth` is negative".into()))?,
            None => UInt::from(0u32),
            Some(_) => return Err(Error::InvalidPdu("`depth` must be an integer".into())),
        };

        let content = match raw.get("content") {
            Some(CanonicalJsonValue::Object(obj)) => {
                RawValue::from_string(canonical::to_string(obj))?
            }
            _ => return Err(Error::InvalidPdu("missing or invalid `content`".into())),
        };

        let prev_events = event_id_list(&raw, "prev_events", rules)?;
        let auth_events = event_id_list(&raw, "auth_events", rules)?;

        let redacts = if event_type == TimelineEventType::RoomRedaction {
            let field = if rules.redaction.content_field_redacts {
                raw.get("content")
                    .and_then(|c| c.as_object())
                    .and_then(|c| c.get("redacts"))
            } else {
                raw.get("redacts")
            };
            match field {
                Some(CanonicalJsonValue::String(s)) => Some(OwnedEventId::try_from(s.as_str())?),
                _ => None,
            }
        } else {
            None
        };

        Ok(Self {
            inner: Arc::new(PduInner {
                event_id,
                room_id,
                has_room_id_field,
                sender,
                origin_server_ts,
                event_type,
                state_key,
                content,
                prev_events,
                auth_events,
                depth,
                redacts,
                rejected: false,
                raw,
            }),
        })
    }

    /// Like [`Pdu::from_canonical`], but trusts `event_id` instead of recomputing the reference
    /// hash. For loading events we persisted ourselves.
    pub fn from_canonical_trusted(
        mut raw: CanonicalJsonObject,
        rules: &RoomVersionRules,
        event_id: OwnedEventId,
    ) -> Result<Self> {
        if rules.event_id_format == EventIdFormatVersion::V1 {
            raw.insert("event_id".into(), event_id.as_str().into());
        } else {
            raw.remove("event_id");
        }
        Self::parse(raw, rules, event_id)
    }

    /// Parse a PDU from JSON bytes.
    pub fn from_slice(bytes: &[u8], rules: &RoomVersionRules) -> Result<Self> {
        Self::from_canonical(canonical::from_slice(bytes)?, rules)
    }

    /// Parse a PDU from a `serde_json::Value`.
    pub fn from_json_value(value: serde_json::Value, rules: &RoomVersionRules) -> Result<Self> {
        Self::from_canonical(canonical::from_json_value(value)?, rules)
    }

    /// Return a copy of this PDU marked as rejected (failed auth on receipt).
    ///
    /// Rejected events are stored so they can be referenced, but never contribute to state.
    #[must_use]
    pub fn with_rejected(&self, rejected: bool) -> Self {
        if self.inner.rejected == rejected {
            return self.clone();
        }
        let mut inner = self.inner.as_ref().clone_fields();
        inner.rejected = rejected;
        Self {
            inner: Arc::new(inner),
        }
    }

    /// The event ID.
    pub fn event_id(&self) -> &EventId {
        &self.inner.event_id
    }

    /// The room ID (always known, even for v12 create events).
    pub fn room_id(&self) -> &RoomId {
        &self.inner.room_id
    }

    /// The sender.
    pub fn sender(&self) -> &UserId {
        &self.inner.sender
    }

    /// The event type.
    pub fn event_type(&self) -> &TimelineEventType {
        &self.inner.event_type
    }

    /// The state key, if this is a state event.
    pub fn state_key(&self) -> Option<&str> {
        self.inner.state_key.as_deref()
    }

    /// Whether this is a state event.
    pub fn is_state(&self) -> bool {
        self.inner.state_key.is_some()
    }

    /// The `(type, state_key)` pair for state events.
    pub fn state_tuple(&self) -> Option<(StateEventType, &str)> {
        self.inner
            .state_key
            .as_deref()
            .map(|k| (StateEventType::from(self.inner.event_type.to_string()), k))
    }

    /// The content as raw JSON.
    pub fn content(&self) -> &RawValue {
        &self.inner.content
    }

    /// Deserialize the content into a typed value.
    pub fn content_as<T: serde::de::DeserializeOwned>(&self) -> Result<T> {
        Ok(serde_json::from_str(self.inner.content.get())?)
    }

    /// Origin server timestamp.
    pub fn origin_server_ts(&self) -> MilliSecondsSinceUnixEpoch {
        self.inner.origin_server_ts
    }

    /// The `depth` field.
    pub fn depth(&self) -> UInt {
        self.inner.depth
    }

    /// Previous events (forward extremities at the time of sending).
    pub fn prev_events(&self) -> &[OwnedEventId] {
        &self.inner.prev_events
    }

    /// Auth events.
    pub fn auth_events(&self) -> &[OwnedEventId] {
        &self.inner.auth_events
    }

    /// For `m.room.redaction` events, the redacted event ID.
    pub fn redacts(&self) -> Option<&EventId> {
        self.inner.redacts.as_deref()
    }

    /// Whether this event was rejected.
    pub fn rejected(&self) -> bool {
        self.inner.rejected
    }

    /// Whether the event is an `m.room.create` event.
    pub fn is_create(&self) -> bool {
        self.inner.event_type == TimelineEventType::RoomCreate
    }

    /// The room version this event declares, if it is an `m.room.create` event.
    ///
    /// A create event without `room_version` is version 1 (spec).
    pub fn declared_room_version(&self) -> Option<RoomVersionId> {
        if !self.is_create() {
            return None;
        }
        let content: serde_json::Value = serde_json::from_str(self.inner.content.get()).ok()?;
        match content.get("room_version") {
            None => Some(RoomVersionId::V1),
            Some(serde_json::Value::String(v)) => RoomVersionId::try_from(v.as_str()).ok(),
            Some(_) => None,
        }
    }

    /// The signed canonical JSON, exactly as it should be sent over federation.
    pub fn raw(&self) -> &CanonicalJsonObject {
        &self.inner.raw
    }

    /// The federation wire format: the raw object, with `event_id` added for room versions 1-2.
    pub fn to_federation_json(&self) -> CanonicalJsonObject {
        self.inner.raw.clone()
    }

    /// The `unsigned` object, if any.
    pub fn unsigned(&self) -> Option<&CanonicalJsonObject> {
        self.inner
            .raw
            .get("unsigned")
            .and_then(CanonicalJsonValue::as_object)
    }

    /// The client-server representation: `event_id` and `room_id` always present, `hashes`,
    /// `signatures`, `depth`, `prev_events` and `auth_events` removed.
    pub fn to_client_json(&self) -> serde_json::Value {
        let mut obj = self.inner.raw.clone();
        for key in [
            "hashes",
            "signatures",
            "depth",
            "prev_events",
            "auth_events",
            "origin",
        ] {
            obj.remove(key);
        }
        obj.insert("event_id".into(), self.inner.event_id.as_str().into());
        if !self.inner.has_room_id_field {
            obj.insert("room_id".into(), self.inner.room_id.as_str().into());
        }
        // A redaction names its target at the top level in room versions up to 10 and inside
        // `content` from 11 on. Clients look in one place or the other (Element only at the
        // top level, clients on the Rust SDK in `content`), so they are given both, as the
        // specification asks of servers. Without it a deletion in a newer room is not
        // applied by the clients that look in the old place.
        if let Some(target) = &self.inner.redacts {
            obj.insert("redacts".into(), target.as_str().into());
            if let Some(CanonicalJsonValue::Object(content)) = obj.get_mut("content") {
                content
                    .entry("redacts".into())
                    .or_insert_with(|| target.as_str().into());
            }
        }
        canonical::to_json_value(obj)
    }

    /// Whether this PDU carries an explicit `room_id` field (false only for v12 create events).
    pub fn has_room_id_field(&self) -> bool {
        self.inner.has_room_id_field
    }
}

impl PduInner {
    fn clone_fields(&self) -> Self {
        Self {
            event_id: self.event_id.clone(),
            room_id: self.room_id.clone(),
            has_room_id_field: self.has_room_id_field,
            sender: self.sender.clone(),
            origin_server_ts: self.origin_server_ts,
            event_type: self.event_type.clone(),
            state_key: self.state_key.clone(),
            content: self.content.clone(),
            prev_events: self.prev_events.clone(),
            auth_events: self.auth_events.clone(),
            depth: self.depth,
            redacts: self.redacts.clone(),
            rejected: self.rejected,
            raw: self.raw.clone(),
        }
    }
}

impl Event for Pdu {
    type Id = OwnedEventId;

    fn event_id(&self) -> &Self::Id {
        &self.inner.event_id
    }

    /// What the JSON says. `None` for a v12 `m.room.create` event, which ruma's auth rules
    /// require; use [`Pdu::room_id`] for the always-known room ID.
    fn room_id(&self) -> Option<&RoomId> {
        self.inner.has_room_id_field.then_some(&self.inner.room_id)
    }

    fn sender(&self) -> &UserId {
        &self.inner.sender
    }

    fn origin_server_ts(&self) -> MilliSecondsSinceUnixEpoch {
        self.inner.origin_server_ts
    }

    fn event_type(&self) -> &TimelineEventType {
        &self.inner.event_type
    }

    fn content(&self) -> &RawValue {
        &self.inner.content
    }

    fn state_key(&self) -> Option<&str> {
        self.inner.state_key.as_deref()
    }

    fn prev_events(&self) -> Box<dyn DoubleEndedIterator<Item = &Self::Id> + '_> {
        Box::new(self.inner.prev_events.iter())
    }

    fn auth_events(&self) -> Box<dyn DoubleEndedIterator<Item = &Self::Id> + '_> {
        Box::new(self.inner.auth_events.iter())
    }

    fn redacts(&self) -> Option<&Self::Id> {
        self.inner.redacts.as_ref()
    }

    fn rejected(&self) -> bool {
        self.inner.rejected
    }
}

impl PartialEq for Pdu {
    fn eq(&self, other: &Self) -> bool {
        self.inner.event_id == other.inner.event_id
    }
}
impl Eq for Pdu {}

fn required_str<'a>(obj: &'a CanonicalJsonObject, key: &str) -> Result<&'a str> {
    match obj.get(key) {
        Some(CanonicalJsonValue::String(s)) => Ok(s),
        Some(_) => Err(Error::InvalidPdu(format!("`{key}` must be a string"))),
        None => Err(Error::InvalidPdu(format!("missing `{key}`"))),
    }
}

/// Parse `prev_events` / `auth_events`, which are plain string arrays in room versions 3+ and
/// `[event_id, {hashes}]` pairs in versions 1-2.
fn event_id_list(
    obj: &CanonicalJsonObject,
    key: &str,
    rules: &RoomVersionRules,
) -> Result<Vec<OwnedEventId>> {
    let Some(CanonicalJsonValue::Array(items)) = obj.get(key) else {
        return Err(Error::InvalidPdu(format!("missing or invalid `{key}`")));
    };
    let mut out = Vec::with_capacity(items.len());
    let mut seen = BTreeSet::new();
    for item in items {
        let id = match (item, &rules.events_reference_format) {
            (CanonicalJsonValue::String(s), _) => s.as_str(),
            (
                CanonicalJsonValue::Array(pair),
                ruma::room_version_rules::EventsReferenceFormatVersion::V1,
            ) => match pair.first() {
                Some(CanonicalJsonValue::String(s)) => s.as_str(),
                _ => return Err(Error::InvalidPdu(format!("invalid entry in `{key}`"))),
            },
            _ => return Err(Error::InvalidPdu(format!("invalid entry in `{key}`"))),
        };
        let id = OwnedEventId::try_from(id)?;
        if seen.insert(id.clone()) {
            out.push(id);
        }
    }
    Ok(out)
}

/// For room versions 12+, the ID of a room's `m.room.create` event is encoded in the room ID
/// itself (`!hash` ↔ `$hash`).
pub fn create_event_id_for_room(room_id: &RoomId) -> Result<OwnedEventId> {
    if room_id.server_name().is_some() {
        return Err(Error::InvalidPdu(format!(
            "room ID {room_id} has a server part and so does not encode its create event"
        )));
    }
    Ok(OwnedEventId::try_from(format!(
        "${}",
        &room_id.as_str()[1..]
    ))?)
}
