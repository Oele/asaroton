//! Canonical JSON must never panic on arbitrary input, and serialising what it accepted must
//! round-trip to the same canonical form.
#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(obj) = asaroton_core::canonical::from_slice(data) {
        let s = asaroton_core::canonical::to_string(&obj);
        let again = asaroton_core::canonical::from_str(&s).expect("canonical output parses");
        assert_eq!(asaroton_core::canonical::to_string(&again), s);
    }
});
