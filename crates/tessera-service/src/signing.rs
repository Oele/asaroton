//! Server signing key management.

use base64::{Engine, engine::general_purpose::STANDARD_NO_PAD};
use rand::RngCore;
use tessera_core::SigningKey;
use tessera_store::{LocalSigningKey, Store, now_ts};

use crate::Result;

/// Load the active signing key, creating one on first start.
pub async fn load_or_create_signing_key(store: &dyn Store) -> Result<SigningKey> {
    let keys = store.local_signing_keys().await?;
    if let Some(active) = keys.iter().find(|k| k.expired_ts.is_none()) {
        let version = active.key_id.as_str().trim_start_matches("ed25519:");
        return Ok(SigningKey::from_base64_seed(&active.seed_b64, version)?);
    }
    let mut seed = [0u8; 32];
    rand::rng().fill_bytes(&mut seed);
    let version = random_version();
    let key = SigningKey::from_seed(&seed, &version)?;
    store
        .add_local_signing_key(LocalSigningKey {
            key_id: key.key_id().clone(),
            seed_b64: STANDARD_NO_PAD.encode(seed),
            created_ts: now_ts(),
            expired_ts: None,
        })
        .await?;
    tracing::info!(key_id = %key.key_id(), "generated new server signing key");
    Ok(key)
}

fn random_version() -> String {
    use rand::{Rng, distr::Alphanumeric};
    rand::rng()
        .sample_iter(&Alphanumeric)
        .take(6)
        .map(char::from)
        .collect()
}
