//! Sticky events (Matrix v1.20, MSC4354): message events that every joined user and server is
//! given, history visibility notwithstanding, for a limited time after they were sent.
//!
//! An event is marked by a top-level object holding `duration_ms`. Two dialects are in use: the
//! spec's `sticky` and the unstable `msc4354_sticky` that matrix-js-sdk speaks. An event keeps
//! the marker of the API it was sent with; received over federation, either one makes it
//! sticky, and what clients are told about it follows the marker it carries.

use ruma::{CanonicalJsonObject, CanonicalJsonValue};
use serde_json::Value;

/// Servers keep an event sticky for at most an hour, whatever duration it asks for.
pub const MAX_STICKY_MS: i64 = 3_600_000;

/// Which of the two names an event's stickiness goes by.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StickyMarker {
    /// `sticky` and `unsigned.sticky_duration_remaining_ms` (v1.20).
    Stable,
    /// `msc4354_sticky` and `unsigned.msc4354_sticky_duration_ttl_ms`.
    Unstable,
}

impl StickyMarker {
    /// The top-level key of the event that holds `duration_ms`.
    pub fn key(self) -> &'static str {
        match self {
            Self::Stable => "sticky",
            Self::Unstable => "msc4354_sticky",
        }
    }

    /// The key in `unsigned` that tells clients how much longer the event stays sticky.
    pub fn remaining_key(self) -> &'static str {
        match self {
            Self::Stable => "sticky_duration_remaining_ms",
            Self::Unstable => "msc4354_sticky_duration_ttl_ms",
        }
    }
}

/// The stickiness an event was sent with.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Sticky {
    pub marker: StickyMarker,
    /// The duration the sender asked for, in milliseconds (positive; may exceed
    /// [`MAX_STICKY_MS`], which only bounds how long servers honour it).
    pub duration_ms: i64,
}

impl Sticky {
    /// The marker object as it goes into the event.
    pub fn marker_object(self) -> CanonicalJsonValue {
        let mut obj = CanonicalJsonObject::new();
        obj.insert(
            "duration_ms".into(),
            CanonicalJsonValue::Integer(ruma::Int::new_saturating(self.duration_ms)),
        );
        CanonicalJsonValue::Object(obj)
    }

    /// When the event stops being sticky (milliseconds since the epoch): counted from the
    /// earlier of when it was sent and when this server received it, for at most an hour.
    pub fn expires_at(self, origin_server_ts: i64, received_ts: i64) -> i64 {
        origin_server_ts
            .min(received_ts)
            .saturating_add(self.duration_ms.min(MAX_STICKY_MS))
    }

    /// Reads the stickiness of an event from its JSON (federation or client form). The spec's
    /// marker wins when an event carries both. An event without a positive `duration_ms` is
    /// never sticky.
    pub fn of(event: &serde_json::Map<String, Value>) -> Option<Self> {
        [StickyMarker::Stable, StickyMarker::Unstable]
            .into_iter()
            .find_map(|marker| {
                let duration_ms = event.get(marker.key())?.get("duration_ms")?.as_i64()?;
                (duration_ms > 0).then_some(Self {
                    marker,
                    duration_ms,
                })
            })
    }

    /// [`Sticky::of`] for canonical JSON.
    pub fn of_canonical(event: &CanonicalJsonObject) -> Option<Self> {
        [StickyMarker::Stable, StickyMarker::Unstable]
            .into_iter()
            .find_map(|marker| {
                let CanonicalJsonValue::Object(obj) = event.get(marker.key())? else {
                    return None;
                };
                let CanonicalJsonValue::Integer(d) = obj.get("duration_ms")? else {
                    return None;
                };
                let duration_ms = i64::from(*d);
                (duration_ms > 0).then_some(Self {
                    marker,
                    duration_ms,
                })
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn obj(v: Value) -> serde_json::Map<String, Value> {
        match v {
            Value::Object(m) => m,
            other => panic!("not an object: {other}"),
        }
    }

    #[test]
    fn reads_either_marker_and_prefers_the_spec_one() {
        let s = Sticky::of(&obj(json!({"sticky": {"duration_ms": 600_000}}))).unwrap();
        assert_eq!((s.marker, s.duration_ms), (StickyMarker::Stable, 600_000));
        let u = Sticky::of(&obj(json!({"msc4354_sticky": {"duration_ms": 5}}))).unwrap();
        assert_eq!(u.marker, StickyMarker::Unstable);
        let both = Sticky::of(&obj(json!({
            "sticky": {"duration_ms": 1},
            "msc4354_sticky": {"duration_ms": 2},
        })))
        .unwrap();
        assert_eq!((both.marker, both.duration_ms), (StickyMarker::Stable, 1));
    }

    #[test]
    fn an_event_without_a_positive_duration_is_not_sticky() {
        for v in [
            json!({}),
            json!({"sticky": {}}),
            json!({"sticky": {"duration_ms": 0}}),
            json!({"sticky": {"duration_ms": -5}}),
            json!({"sticky": {"duration_ms": "600000"}}),
            json!({"sticky": 600_000}),
        ] {
            let shown = v.to_string();
            assert_eq!(Sticky::of(&obj(v)), None, "{shown}");
        }
    }

    #[test]
    fn expiry_counts_from_the_earlier_time_for_at_most_an_hour() {
        let s = Sticky {
            marker: StickyMarker::Stable,
            duration_ms: 10_000,
        };
        assert_eq!(s.expires_at(1_000, 5_000), 11_000);
        assert_eq!(s.expires_at(9_000, 2_000), 12_000);
        let long = Sticky {
            marker: StickyMarker::Unstable,
            duration_ms: 10 * MAX_STICKY_MS,
        };
        assert_eq!(long.expires_at(0, 0), MAX_STICKY_MS);
    }

    #[test]
    fn the_canonical_reader_agrees() {
        let canonical: CanonicalJsonObject =
            serde_json::from_value(json!({"msc4354_sticky": {"duration_ms": 42}})).unwrap();
        assert_eq!(
            Sticky::of_canonical(&canonical),
            Some(Sticky {
                marker: StickyMarker::Unstable,
                duration_ms: 42
            })
        );
    }
}
