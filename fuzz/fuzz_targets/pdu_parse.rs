//! Parsing a PDU in any supported room version must never panic, and an accepted PDU must
//! serialise back and re-parse to the same event ID.
#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let Ok(obj) = tessera_core::canonical::from_slice(data) else {
        return;
    };
    for version in &tessera_core::SUPPORTED_ROOM_VERSIONS {
        let Ok(rules) = tessera_core::room_version::rules(version) else {
            continue;
        };
        if let Ok(pdu) = tessera_core::Pdu::from_canonical(obj.clone(), &rules) {
            let json = pdu.to_federation_json();
            let again = tessera_core::Pdu::from_canonical(json, &rules).expect("own output parses");
            assert_eq!(again.event_id(), pdu.event_id());
            let _ = pdu.to_client_json();
        }
    }
});
