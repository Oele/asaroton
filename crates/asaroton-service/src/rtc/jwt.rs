//! Minimal HS256 JSON Web Tokens, as `LiveKit` issues and expects them.

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use hmac::{Hmac, Mac};
use serde_json::{Value, json};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

/// Clock skew tolerated when checking `exp` and `nbf`.
const LEEWAY: i64 = 60;

/// Signs `claims` with `secret`.
pub fn sign(secret: &[u8], claims: &Value) -> String {
    let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"HS256","typ":"JWT"}"#);
    let payload = URL_SAFE_NO_PAD.encode(claims.to_string());
    let signing_input = format!("{header}.{payload}");
    let mut mac = HmacSha256::new_from_slice(secret).unwrap_or_else(|_| unreachable!());
    mac.update(signing_input.as_bytes());
    let sig = URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
    format!("{signing_input}.{sig}")
}

/// Verifies the signature and validity window of `token` and returns its claims.
pub fn verify(secret: &[u8], token: &str, now_secs: i64) -> Result<Value, String> {
    let mut parts = token.split('.');
    let (Some(header), Some(payload), Some(sig), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err("malformed token".into());
    };
    let header_json: Value = serde_json::from_slice(
        &URL_SAFE_NO_PAD
            .decode(header)
            .map_err(|_| "bad header encoding")?,
    )
    .map_err(|_| "bad header")?;
    if header_json.get("alg").and_then(Value::as_str) != Some("HS256") {
        return Err("unsupported algorithm".into());
    }
    let sig = URL_SAFE_NO_PAD
        .decode(sig)
        .map_err(|_| "bad signature encoding")?;
    let mut mac = HmacSha256::new_from_slice(secret).unwrap_or_else(|_| unreachable!());
    mac.update(format!("{header}.{payload}").as_bytes());
    mac.verify_slice(&sig).map_err(|_| "bad signature")?;
    let claims: Value = serde_json::from_slice(
        &URL_SAFE_NO_PAD
            .decode(payload)
            .map_err(|_| "bad payload encoding")?,
    )
    .map_err(|_| "bad payload")?;
    if let Some(exp) = claims.get("exp").and_then(Value::as_i64)
        && exp + LEEWAY < now_secs
    {
        return Err("token expired".into());
    }
    if let Some(nbf) = claims.get("nbf").and_then(Value::as_i64)
        && nbf - LEEWAY > now_secs
    {
        return Err("token not yet valid".into());
    }
    Ok(claims)
}

/// The claims of a `LiveKit` access token: `sub` is the participant identity and `video`
/// carries the grants.
pub fn livekit_claims(
    api_key: &str,
    identity: Option<&str>,
    video: &Value,
    now_secs: i64,
    ttl_secs: i64,
) -> Value {
    let mut claims = json!({
        "iss": api_key,
        "iat": now_secs,
        "nbf": now_secs,
        "exp": now_secs + ttl_secs,
        "video": video,
    });
    if let Some(identity) = identity {
        claims["sub"] = Value::String(identity.to_owned());
    }
    claims
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let claims = json!({"sub": "alice", "exp": 2_000_000_000_i64, "video": {"room": "r"}});
        let token = sign(b"secret", &claims);
        assert_eq!(token.split('.').count(), 3);
        let back = verify(b"secret", &token, 1_900_000_000).expect("verifies");
        assert_eq!(back["sub"], "alice");
        assert!(verify(b"other", &token, 1_900_000_000).is_err());
        assert!(verify(b"secret", &token, 2_000_000_100).is_err());
    }

    #[test]
    fn rejects_garbage() {
        assert!(verify(b"s", "a.b", 0).is_err());
        assert!(verify(b"s", "a.b.c", 0).is_err());
    }
}
