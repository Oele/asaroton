//! Server signing keys and event/JSON signing & verification.

use std::collections::BTreeMap;

use base64::{
    Engine, alphabet,
    engine::{
        DecodePaddingMode, GeneralPurpose, GeneralPurposeConfig, general_purpose::STANDARD_NO_PAD,
    },
};
use ruma::{
    CanonicalJsonObject, OwnedServerName, OwnedServerSigningKeyId, ServerName,
    room_version_rules::RoomVersionRules,
    serde::Base64,
    signatures::{Ed25519KeyPair, PublicKeyMap, PublicKeySet, Verified},
};

use crate::{Result, canonical};

/// PKCS#8 v1 DER prefix for an Ed25519 private key (RFC 8410), followed by the 32-byte seed.
const PKCS8_ED25519_PREFIX: [u8; 16] = [
    0x30, 0x2e, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x04, 0x22, 0x04, 0x20,
];

/// Lenient base64 decoder for keys as found in the wild: padding optional, trailing bits
/// ignored (Synapse-generated seeds and the spec's own test vectors need this).
const LENIENT_B64: GeneralPurpose = GeneralPurpose::new(
    &alphabet::STANDARD,
    GeneralPurposeConfig::new()
        .with_decode_allow_trailing_bits(true)
        .with_decode_padding_mode(DecodePaddingMode::Indifferent),
);

/// An Ed25519 signing key with its Matrix key ID (`ed25519:<version>`).
pub struct SigningKey {
    key_pair: Ed25519KeyPair,
    key_id: OwnedServerSigningKeyId,
}

impl std::fmt::Debug for SigningKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SigningKey")
            .field("key_id", &self.key_id)
            .finish_non_exhaustive()
    }
}

impl SigningKey {
    /// Generate a fresh random key with the given version string.
    pub fn generate(version: &str) -> Result<Self> {
        let der = Ed25519KeyPair::generate();
        Self::from_pkcs8_der(&der, version)
    }

    /// Load a key from PKCS#8 DER bytes.
    pub fn from_pkcs8_der(der: &[u8], version: &str) -> Result<Self> {
        let key_pair = Ed25519KeyPair::from_der(der, version.to_owned())?;
        let key_id = OwnedServerSigningKeyId::try_from(format!("ed25519:{version}"))?;
        Ok(Self { key_pair, key_id })
    }

    /// Load a key from a raw 32-byte Ed25519 seed (the format Synapse stores, base64 encoded).
    pub fn from_seed(seed: &[u8; 32], version: &str) -> Result<Self> {
        let mut der = Vec::with_capacity(48);
        der.extend_from_slice(&PKCS8_ED25519_PREFIX);
        der.extend_from_slice(seed);
        Self::from_pkcs8_der(&der, version)
    }

    /// Load a key from an unpadded-base64 seed, as found in Synapse `signing.key` files.
    pub fn from_base64_seed(seed_b64: &str, version: &str) -> Result<Self> {
        let bytes = LENIENT_B64
            .decode(seed_b64.trim())
            .map_err(|e| crate::Error::InvalidPdu(format!("invalid base64 seed: {e}")))?;
        let seed: [u8; 32] = bytes
            .try_into()
            .map_err(|_| crate::Error::InvalidPdu("seed must be 32 bytes".into()))?;
        Self::from_seed(&seed, version)
    }

    /// The key ID, e.g. `ed25519:a_XRhW`.
    pub fn key_id(&self) -> &OwnedServerSigningKeyId {
        &self.key_id
    }

    /// The key version (the part after `ed25519:`).
    pub fn version(&self) -> &str {
        self.key_pair.version()
    }

    /// The public key, unpadded base64 as published in `/_matrix/key/v2/server`.
    pub fn public_key_base64(&self) -> String {
        STANDARD_NO_PAD.encode(self.key_pair.public_key())
    }

    /// The public key as a ruma `Base64` value.
    pub fn public_key(&self) -> Base64 {
        Base64::new(self.key_pair.public_key().to_vec())
    }

    /// Sign an arbitrary JSON object (e.g. a federation request or server key response) in
    /// place, as `server_name`.
    pub fn sign_json(
        &self,
        server_name: &ServerName,
        object: &mut CanonicalJsonObject,
    ) -> Result<()> {
        ruma::signatures::sign_json(server_name.as_str(), &self.key_pair, object)?;
        Ok(())
    }

    /// Add the content hash and this server's signature to an event in place.
    pub fn hash_and_sign_event(
        &self,
        server_name: &ServerName,
        event: &mut CanonicalJsonObject,
        rules: &RoomVersionRules,
    ) -> Result<()> {
        ruma::signatures::hash_and_sign_event(
            server_name.as_str(),
            &self.key_pair,
            event,
            &rules.redaction,
        )?;
        Ok(())
    }

    /// Add this server's signature to an already-hashed event (e.g. when signing a remote
    /// user's join event as the resident server).
    pub fn sign_event(
        &self,
        server_name: &ServerName,
        event: &mut CanonicalJsonObject,
        rules: &RoomVersionRules,
    ) -> Result<()> {
        ruma::signatures::sign_event(
            server_name.as_str(),
            &self.key_pair,
            event,
            &rules.redaction,
        )?;
        Ok(())
    }

    /// A one-entry public key map for this key, convenient for verifying our own output.
    pub fn public_key_map(&self, server_name: &ServerName) -> PublicKeyMap {
        let mut set = PublicKeySet::new();
        set.insert(self.key_id.to_string(), self.public_key());
        let mut map = PublicKeyMap::new();
        map.insert(server_name.to_string(), set);
        map
    }
}

/// Build a public key map from `(server, key_id, base64 key)` triples.
pub fn public_key_map<'a>(
    keys: impl IntoIterator<Item = (&'a ServerName, &'a str, &'a str)>,
) -> Result<PublicKeyMap> {
    let mut map: BTreeMap<String, PublicKeySet> = BTreeMap::new();
    for (server, key_id, key) in keys {
        let key = Base64::parse(key)
            .map_err(|e| crate::Error::InvalidPdu(format!("invalid public key: {e}")))?;
        map.entry(server.to_string())
            .or_default()
            .insert(key_id.to_owned(), key);
    }
    Ok(map)
}

/// Verify the hashes and all required signatures on an event.
///
/// On success tells whether the content hash matched too ([`Verified::All`]) or only the
/// signatures ([`Verified::Signatures`]); in the latter case the event must be redacted before
/// use, per spec.
pub fn verify_event(
    keys: &PublicKeyMap,
    event: &CanonicalJsonObject,
    rules: &RoomVersionRules,
) -> Result<Verified> {
    Ok(ruma::signatures::verify_event(keys, event, rules)?)
}

/// Verify the signatures on a plain JSON object (server keys, federation requests, ...).
pub fn verify_json(keys: &PublicKeyMap, object: &CanonicalJsonObject) -> Result<()> {
    Ok(ruma::signatures::verify_json(keys, object)?)
}

/// The servers whose signatures an event must carry under the given rules.
pub fn required_signing_servers(
    event: &CanonicalJsonObject,
    rules: &RoomVersionRules,
) -> Result<std::collections::BTreeSet<OwnedServerName>> {
    Ok(ruma::signatures::required_server_signatures_to_verify_event(event, &rules.signatures)?)
}

/// Canonical JSON string of an object with `signatures` and `unsigned` stripped: the exact
/// bytes that get signed.
pub fn signable_string(object: &CanonicalJsonObject) -> String {
    let mut copy = object.clone();
    copy.remove("signatures");
    copy.remove("unsigned");
    canonical::to_string(&copy)
}

#[cfg(test)]
mod tests {
    use ruma::{CanonicalJsonValue, server_name};

    use super::*;

    // Test vectors from the spec appendices ("Signing JSON" examples).
    const SEED: &str = "YJDBA9Xnr2sVqXD9Vj7XVUnmFZcZrlw8Md7kMW+3XA1";

    #[test]
    fn spec_vector_empty_object() {
        let key = SigningKey::from_base64_seed(SEED, "1").unwrap();
        let mut obj = CanonicalJsonObject::new();
        key.sign_json(server_name!("domain"), &mut obj).unwrap();
        let sig = obj["signatures"].as_object().unwrap()["domain"]
            .as_object()
            .unwrap()["ed25519:1"]
            .as_str()
            .unwrap();
        assert_eq!(
            sig,
            "K8280/U9SSy9IVtjBuVeLr+HpOB4BQFWbg+UZaADMtTdGYI7Geitb76LTrr5QV/7Xg4ahLwYGYZzuHGZKM5ZAQ"
        );
    }

    #[test]
    fn spec_vector_simple_object() {
        let key = SigningKey::from_base64_seed(SEED, "1").unwrap();
        let mut obj = canonical::from_str(r#"{"one": 1, "two": "Two"}"#).unwrap();
        key.sign_json(server_name!("domain"), &mut obj).unwrap();
        let sig = obj["signatures"].as_object().unwrap()["domain"]
            .as_object()
            .unwrap()["ed25519:1"]
            .as_str()
            .unwrap();
        assert_eq!(
            sig,
            "KqmLSbO39/Bzb0QIYE82zqLwsA+PDzYIpIRA2sRQ4sL53+sN6/fpNSoqE7BP7vBZhG6kYdD13EIMJpvhJI+6Bw"
        );
        // And it verifies with the matching public key.
        verify_json(&key.public_key_map(server_name!("domain")), &obj).unwrap();
    }

    #[test]
    fn tampering_breaks_verification() {
        let key = SigningKey::generate("abc").unwrap();
        let mut obj = canonical::from_str(r#"{"a": 1}"#).unwrap();
        key.sign_json(server_name!("example.org"), &mut obj)
            .unwrap();
        obj.insert("a".into(), CanonicalJsonValue::Integer(ruma::int!(2)));
        assert!(verify_json(&key.public_key_map(server_name!("example.org")), &obj).is_err());
    }

    #[test]
    fn generated_key_roundtrips_through_seed() {
        let key = SigningKey::generate("v1").unwrap();
        assert_eq!(key.key_id().as_str(), "ed25519:v1");
        assert_eq!(key.public_key_base64().len(), 43);
    }
}
