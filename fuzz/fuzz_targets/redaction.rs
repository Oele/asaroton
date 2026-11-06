//! Redacting any parsed event in any room version must never panic, and redacting twice
//! must be a no-op.
#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let Ok(obj) = asaroton_core::canonical::from_slice(data) else {
        return;
    };
    for version in &asaroton_core::SUPPORTED_ROOM_VERSIONS {
        let Ok(rules) = asaroton_core::room_version::rules(version) else {
            continue;
        };
        if let Ok(pdu) = asaroton_core::Pdu::from_canonical(obj.clone(), &rules)
            && let Ok(once) = asaroton_core::redaction::redact_pdu(&pdu, &rules, None)
        {
            let twice = asaroton_core::redaction::redact_pdu(&once, &rules, None).expect("re-redaction");
            assert_eq!(
                asaroton_core::canonical::to_string(&once.to_federation_json()),
                asaroton_core::canonical::to_string(&twice.to_federation_json())
            );
        }
    }
});
