//! Supported room versions and their rule sets.

use ruma::{RoomVersionId, room_version_rules::RoomVersionRules};

use crate::{Error, Result};

/// The room version used for newly created rooms when the client does not ask for one.
///
/// The spec (v1.19) says servers SHOULD default to room version 12.
pub const DEFAULT_ROOM_VERSION: RoomVersionId = RoomVersionId::V12;

/// All room versions this server can participate in, oldest first.
pub const SUPPORTED_ROOM_VERSIONS: [RoomVersionId; 12] = [
    RoomVersionId::V1,
    RoomVersionId::V2,
    RoomVersionId::V3,
    RoomVersionId::V4,
    RoomVersionId::V5,
    RoomVersionId::V6,
    RoomVersionId::V7,
    RoomVersionId::V8,
    RoomVersionId::V9,
    RoomVersionId::V10,
    RoomVersionId::V11,
    RoomVersionId::V12,
];

/// Whether this server implements the given room version.
pub fn is_supported(version: &RoomVersionId) -> bool {
    SUPPORTED_ROOM_VERSIONS.contains(version)
}

/// The rule set for a room version, or an error if it is unsupported.
pub fn rules(version: &RoomVersionId) -> Result<RoomVersionRules> {
    if !is_supported(version) {
        return Err(Error::UnsupportedRoomVersion(version.clone()));
    }
    version
        .rules()
        .ok_or_else(|| Error::UnsupportedRoomVersion(version.clone()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_supported_version_has_rules() {
        for v in &SUPPORTED_ROOM_VERSIONS {
            assert!(rules(v).is_ok(), "no rules for {v}");
        }
    }

    #[test]
    fn unknown_version_is_rejected() {
        let v: RoomVersionId = "org.example.unknown".try_into().unwrap();
        assert!(!is_supported(&v));
        assert!(matches!(rules(&v), Err(Error::UnsupportedRoomVersion(_))));
    }
}
