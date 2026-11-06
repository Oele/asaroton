//! Redaction algorithm per room version.

use ruma::{
    CanonicalJsonObject, canonical_json::RedactedBecause, room_version_rules::RoomVersionRules,
};

use crate::{Pdu, Result};

/// Redact an event object in place according to the room version's redaction rules.
///
/// If `because` is given, its client-format JSON is placed in `unsigned.redacted_because`.
pub fn redact_in_place(
    event: &mut CanonicalJsonObject,
    rules: &RoomVersionRules,
    because: Option<&Pdu>,
) -> Result<()> {
    let because = match because {
        Some(pdu) => Some(RedactedBecause::from_json(
            crate::canonical::from_json_value(pdu.to_client_json())?,
        )),
        None => None,
    };
    ruma::canonical_json::redact_in_place(event, &rules.redaction, because)?;
    Ok(())
}

/// Redacted copy of a PDU. The event ID is unchanged (reference hashes are computed over the
/// redacted form), so this is safe to store in place of the original.
pub fn redact_pdu(pdu: &Pdu, rules: &RoomVersionRules, because: Option<&Pdu>) -> Result<Pdu> {
    let mut raw = pdu.raw().clone();
    if rules.event_id_format == ruma::room_version_rules::EventIdFormatVersion::V1 {
        raw.insert("event_id".into(), pdu.event_id().as_str().into());
    }
    redact_in_place(&mut raw, rules, because)?;
    let redacted = Pdu::from_canonical(raw, rules)?;
    Ok(redacted.with_rejected(pdu.rejected()))
}
