//! Password hashing with Argon2id (PHC string format, compatible with Synapse's bcrypt only
//! via re-hash on login, which is not supported: migrations re-set passwords).

use argon2::{Argon2, PasswordHash, PasswordHasher, PasswordVerifier, password_hash::SaltString};
use rand::RngCore;

/// Hash a password. CPU-heavy: call from `spawn_blocking` on hot paths.
pub fn hash_password(password: &str) -> Result<String, argon2::password_hash::Error> {
    let mut salt_bytes = [0u8; 16];
    rand::rng().fill_bytes(&mut salt_bytes);
    let salt = SaltString::encode_b64(&salt_bytes)?;
    Ok(Argon2::default()
        .hash_password(password.as_bytes(), &salt)?
        .to_string())
}

/// Verify a password against a stored PHC hash. Returns `false` for malformed hashes.
pub fn verify_password(hash: &str, password: &str) -> bool {
    // Hashes imported from Synapse are bcrypt; they are replaced on the next login.
    if is_bcrypt(hash) {
        return bcrypt::verify(password, hash).unwrap_or(false);
    }
    let Ok(parsed) = PasswordHash::new(hash) else {
        return false;
    };
    Argon2::default()
        .verify_password(password.as_bytes(), &parsed)
        .is_ok()
}

/// Whether a stored hash is a bcrypt hash (as Synapse produces) rather than argon2.
#[must_use]
pub fn is_bcrypt(hash: &str) -> bool {
    hash.starts_with("$2a$") || hash.starts_with("$2b$") || hash.starts_with("$2y$")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let h = hash_password("correct horse").unwrap();
        assert!(h.starts_with("$argon2id$"));
        assert!(verify_password(&h, "correct horse"));
        assert!(!verify_password(&h, "wrong"));
        assert!(!verify_password("garbage", "x"));
    }
}
