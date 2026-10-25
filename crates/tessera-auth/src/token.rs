//! Random token and ID generation.

use rand::{Rng, distr::Alphanumeric};

fn random_alnum(len: usize) -> String {
    rand::rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}

/// An access or refresh token. The prefix makes tokens recognisable in logs and secret
/// scanners (`tsa_` access, `tsr_` refresh), following the Synapse `syt_` convention.
pub fn generate_token(prefix: &str) -> String {
    format!("{prefix}_{}", random_alnum(40))
}

/// A device ID: 10 upper-case alphanumerics, like most servers generate.
pub fn generate_device_id() -> ruma::OwnedDeviceId {
    let id: String = random_alnum(10).to_ascii_uppercase();
    ruma::OwnedDeviceId::from(id)
}

/// An opaque session/transaction ID.
pub fn generate_session_id() -> String {
    random_alnum(24)
}
