//! Constructing new, locally-originated events.

use rand::{Rng, distr::Alphanumeric};
use ruma::{
    CanonicalJsonObject, CanonicalJsonValue, MilliSecondsSinceUnixEpoch, OwnedEventId, OwnedRoomId,
    OwnedUserId, ServerName, UInt,
    events::TimelineEventType,
    room_version_rules::{
        EventIdFormatVersion, EventsReferenceFormatVersion, RoomIdFormatVersion, RoomVersionRules,
    },
};

use crate::{Error, Pdu, Result, SigningKey, canonical};

/// A reference to a prior event: its ID plus, for room versions 1-2, its content hash.
#[derive(Clone, Debug)]
pub struct EventRef {
    /// The event ID.
    pub event_id: OwnedEventId,
    /// The `sha256` content hash, only used on the wire by room versions 1-2.
    pub sha256: Option<String>,
}

impl From<OwnedEventId> for EventRef {
    fn from(event_id: OwnedEventId) -> Self {
        Self {
            event_id,
            sha256: None,
        }
    }
}

/// Builder for a new PDU. Produces a fully hashed and signed [`Pdu`].
#[derive(Debug)]
pub struct PduBuilder {
    room_id: Option<OwnedRoomId>,
    sender: OwnedUserId,
    event_type: TimelineEventType,
    state_key: Option<String>,
    content: CanonicalJsonObject,
    prev_events: Vec<EventRef>,
    auth_events: Vec<EventRef>,
    depth: UInt,
    origin_server_ts: Option<MilliSecondsSinceUnixEpoch>,
    redacts: Option<OwnedEventId>,
    unsigned: Option<CanonicalJsonObject>,
}

impl PduBuilder {
    /// Start building an event of the given type from `sender`.
    pub fn new(sender: OwnedUserId, event_type: impl Into<TimelineEventType>) -> Self {
        Self {
            room_id: None,
            sender,
            event_type: event_type.into(),
            state_key: None,
            content: CanonicalJsonObject::new(),
            prev_events: Vec::new(),
            auth_events: Vec::new(),
            depth: UInt::from(0u32),
            origin_server_ts: None,
            redacts: None,
            unsigned: None,
        }
    }

    /// The room. Required for everything except a v12+ `m.room.create` event.
    #[must_use]
    pub fn room_id(mut self, room_id: OwnedRoomId) -> Self {
        self.room_id = Some(room_id);
        self
    }

    /// Make this a state event.
    #[must_use]
    pub fn state_key(mut self, key: impl Into<String>) -> Self {
        self.state_key = Some(key.into());
        self
    }

    /// Set the content from any serializable value (must serialize to an object).
    pub fn content<T: serde::Serialize>(mut self, content: &T) -> Result<Self> {
        self.content = canonical::from_serialize(content)?;
        Ok(self)
    }

    /// Set the content from a canonical object.
    #[must_use]
    pub fn content_object(mut self, content: CanonicalJsonObject) -> Self {
        self.content = content;
        self
    }

    /// Previous events.
    #[must_use]
    pub fn prev_events(mut self, prev: impl IntoIterator<Item = impl Into<EventRef>>) -> Self {
        self.prev_events = prev.into_iter().map(Into::into).collect();
        self
    }

    /// Auth events.
    #[must_use]
    pub fn auth_events(mut self, auth: impl IntoIterator<Item = impl Into<EventRef>>) -> Self {
        self.auth_events = auth.into_iter().map(Into::into).collect();
        self
    }

    /// Depth (max depth of prev events + 1).
    #[must_use]
    pub fn depth(mut self, depth: UInt) -> Self {
        self.depth = depth;
        self
    }

    /// Override the timestamp (defaults to now).
    #[must_use]
    pub fn origin_server_ts(mut self, ts: MilliSecondsSinceUnixEpoch) -> Self {
        self.origin_server_ts = Some(ts);
        self
    }

    /// For redactions: the event being redacted. Placed in `content` or at top level depending
    /// on the room version.
    #[must_use]
    pub fn redacts(mut self, event_id: OwnedEventId) -> Self {
        self.redacts = Some(event_id);
        self
    }

    /// Unsigned data (not signed, not hashed).
    #[must_use]
    pub fn unsigned(mut self, unsigned: CanonicalJsonObject) -> Self {
        self.unsigned = Some(unsigned);
        self
    }

    /// Assemble, hash and sign the event as `server_name`.
    pub fn build(
        mut self,
        rules: &RoomVersionRules,
        key: &SigningKey,
        server_name: &ServerName,
    ) -> Result<Pdu> {
        let mut obj = CanonicalJsonObject::new();

        let is_create = self.event_type == TimelineEventType::RoomCreate;
        match (&self.room_id, is_create, &rules.room_id_format) {
            (Some(_), true, RoomIdFormatVersion::V2) => {
                return Err(Error::InvalidPdu(
                    "an m.room.create event must not have a room_id in this room version".into(),
                ));
            }
            (Some(room_id), _, _) => {
                obj.insert("room_id".into(), room_id.as_str().into());
            }
            (None, true, RoomIdFormatVersion::V2) => {}
            (None, _, _) => return Err(Error::InvalidPdu("missing room_id".into())),
        }

        obj.insert("type".into(), self.event_type.to_string().into());
        obj.insert("sender".into(), self.sender.as_str().into());
        if let Some(key) = &self.state_key {
            obj.insert("state_key".into(), key.as_str().into());
        }
        if let Some(redacts) = &self.redacts {
            if rules.redaction.content_field_redacts {
                self.content
                    .insert("redacts".into(), redacts.as_str().into());
            } else {
                obj.insert("redacts".into(), redacts.as_str().into());
            }
        }
        obj.insert("content".into(), CanonicalJsonValue::Object(self.content));
        obj.insert(
            "origin_server_ts".into(),
            CanonicalJsonValue::Integer(
                self.origin_server_ts
                    .unwrap_or_else(MilliSecondsSinceUnixEpoch::now)
                    .0
                    .into(),
            ),
        );
        obj.insert(
            "depth".into(),
            CanonicalJsonValue::Integer(self.depth.into()),
        );
        obj.insert("prev_events".into(), event_refs(&self.prev_events, rules));
        obj.insert("auth_events".into(), event_refs(&self.auth_events, rules));
        if let Some(unsigned) = self.unsigned {
            obj.insert("unsigned".into(), CanonicalJsonValue::Object(unsigned));
        }

        if rules.event_id_format == EventIdFormatVersion::V1 {
            obj.insert("event_id".into(), random_v1_event_id(server_name).into());
        }

        key.hash_and_sign_event(server_name, &mut obj, rules)?;
        Pdu::from_canonical(obj, rules)
    }
}

fn event_refs(refs: &[EventRef], rules: &RoomVersionRules) -> CanonicalJsonValue {
    let items = refs
        .iter()
        .map(|r| match rules.events_reference_format {
            EventsReferenceFormatVersion::V1 => {
                let mut hashes = CanonicalJsonObject::new();
                if let Some(h) = &r.sha256 {
                    hashes.insert("sha256".into(), h.as_str().into());
                }
                CanonicalJsonValue::Array(vec![
                    r.event_id.as_str().into(),
                    CanonicalJsonValue::Object(hashes),
                ])
            }
            _ => r.event_id.as_str().into(),
        })
        .collect();
    CanonicalJsonValue::Array(items)
}

fn random_v1_event_id(server_name: &ServerName) -> String {
    let local: String = rand::rng()
        .sample_iter(&Alphanumeric)
        .take(18)
        .map(char::from)
        .collect();
    format!("${local}:{server_name}")
}

/// A random opaque room ID for room versions 1-11 (`!opaque:server`).
pub fn random_room_id(server_name: &ServerName) -> OwnedRoomId {
    let local: String = rand::rng()
        .sample_iter(&Alphanumeric)
        .take(18)
        .map(char::from)
        .collect();
    OwnedRoomId::try_from(format!("!{local}:{server_name}"))
        .unwrap_or_else(|_| unreachable!("alphanumeric localpart is always a valid room id"))
}
