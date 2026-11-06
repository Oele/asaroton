//! Delegated authentication (MSC3861): access tokens are issued by Matrix Authentication
//! Service and validated here by OAuth 2.0 token introspection.

use std::{
    sync::Mutex,
    time::{Duration, Instant},
};

use dashmap::DashMap;
use ruma::{OwnedDeviceId, OwnedServerName, OwnedUserId, UserId};

use crate::AuthError;

const INTROSPECTION_CACHE_TTL: Duration = Duration::from_secs(120);
const METADATA_CACHE_TTL: Duration = Duration::from_secs(600);
const API_SCOPES: [&str; 2] = [
    "urn:matrix:org.matrix.msc2967.client:api:*",
    "urn:matrix:client:api:*",
];
const DEVICE_SCOPE_PREFIXES: [&str; 2] = [
    "urn:matrix:org.matrix.msc2967.client:device:",
    "urn:matrix:client:device:",
];
const ADMIN_SCOPE: &str = "urn:synapse:admin:*";

/// Who an introspected token belongs to.
#[derive(Clone, Debug)]
pub struct MasIdentity {
    pub user_id: OwnedUserId,
    pub device_id: Option<OwnedDeviceId>,
    pub admin: bool,
}

#[derive(Clone, Debug, serde::Deserialize)]
struct Introspection {
    active: bool,
    #[serde(default)]
    scope: String,
    username: Option<String>,
    device_id: Option<String>,
    expires_in: Option<u64>,
}

/// Client for the authentication service: introspection, metadata, and the shared secret
/// that authenticates its calls to us.
pub struct MasClient {
    endpoint: String,
    secret: String,
    server_name: OwnedServerName,
    http: reqwest::Client,
    introspections: DashMap<String, (Introspection, Instant)>,
    metadata: Mutex<Option<(serde_json::Value, Instant)>>,
}

impl std::fmt::Debug for MasClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MasClient")
            .field("endpoint", &self.endpoint)
            .finish_non_exhaustive()
    }
}

impl MasClient {
    pub fn new(endpoint: &str, secret: String, server_name: OwnedServerName) -> Self {
        Self {
            endpoint: endpoint.trim_end_matches('/').to_owned(),
            secret,
            server_name,
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .unwrap_or_default(),
            introspections: DashMap::new(),
            metadata: Mutex::new(None),
        }
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Whether a bearer token is the shared secret (requests from MAS itself).
    pub fn is_shared_secret(&self, token: &str) -> bool {
        // Constant-time comparison: the lengths leak, the contents do not.
        token.len() == self.secret.len()
            && token
                .bytes()
                .zip(self.secret.bytes())
                .fold(0u8, |acc, (a, b)| acc | (a ^ b))
                == 0
    }

    /// Forget a cached introspection result (a device vanished: the token is dead).
    pub fn forget(&self, token: &str) {
        self.introspections.remove(token);
    }

    /// Resolve an access token.
    pub async fn introspect(&self, token: &str) -> Result<MasIdentity, AuthError> {
        let (intro, retrieved_at) = match self.introspections.get(token) {
            Some(c) if c.1.elapsed() < INTROSPECTION_CACHE_TTL => c.clone(),
            _ => {
                let intro = self.fetch_introspection(token).await?;
                let entry = (intro, Instant::now());
                if self.introspections.len() > 50_000 {
                    self.introspections.clear();
                }
                self.introspections.insert(token.to_owned(), entry.clone());
                entry
            }
        };
        let expired = intro
            .expires_in
            .is_some_and(|secs| retrieved_at.elapsed() >= Duration::from_secs(secs));
        if !intro.active || expired {
            return Err(AuthError::UnknownToken { soft_logout: false });
        }
        let scopes: Vec<&str> = intro.scope.split_whitespace().collect();
        if !scopes.iter().any(|s| API_SCOPES.contains(s)) {
            return Err(AuthError::UnknownToken { soft_logout: false });
        }
        let Some(username) = intro.username.as_deref() else {
            return Err(AuthError::UnknownToken { soft_logout: false });
        };
        let user_id = UserId::parse_with_server_name(username, &self.server_name)
            .map_err(|_| AuthError::UnknownToken { soft_logout: false })?;
        let device_id = match intro.device_id.as_deref() {
            Some(d) => Some(d.to_owned()),
            None => {
                let from_scope: Vec<&str> = scopes
                    .iter()
                    .filter_map(|s| DEVICE_SCOPE_PREFIXES.iter().find_map(|p| s.strip_prefix(p)))
                    .collect();
                match from_scope.as_slice() {
                    [] => None,
                    [one] => Some((*one).to_owned()),
                    _ => return Err(AuthError::UnknownToken { soft_logout: false }),
                }
            }
        };
        let device_id = match device_id {
            Some(d) if !d.is_empty() && d.len() <= 255 => Some(OwnedDeviceId::from(d)),
            Some(_) => return Err(AuthError::UnknownToken { soft_logout: false }),
            None => None,
        };
        Ok(MasIdentity {
            user_id,
            device_id,
            admin: scopes.contains(&ADMIN_SCOPE),
        })
    }

    async fn fetch_introspection(&self, token: &str) -> Result<Introspection, AuthError> {
        let body = format!("token={}&token_type_hint=access_token", urlencode(token));
        let resp = self
            .http
            .post(format!("{}/oauth2/introspect", self.endpoint))
            .header("Authorization", format!("Bearer {}", self.secret))
            .header("Accept", "application/json")
            .header("Content-Type", "application/x-www-form-urlencoded")
            .header("X-MAS-Supports-Device-Id", "1")
            .body(body)
            .send()
            .await
            .map_err(|e| AuthError::Unavailable(e.to_string()))?;
        if !resp.status().is_success() {
            return Err(AuthError::Unavailable(format!(
                "introspection answered {}",
                resp.status()
            )));
        }
        resp.json::<Introspection>()
            .await
            .map_err(|e| AuthError::Unavailable(format!("bad introspection response: {e}")))
    }

    /// The service's OAuth 2.0 server metadata (`/.well-known/openid-configuration`).
    pub async fn metadata(&self) -> Result<serde_json::Value, AuthError> {
        if let Ok(guard) = self.metadata.lock()
            && let Some((v, at)) = guard.as_ref()
            && at.elapsed() < METADATA_CACHE_TTL
        {
            return Ok(v.clone());
        }
        let v: serde_json::Value = self
            .http
            .get(format!(
                "{}/.well-known/openid-configuration",
                self.endpoint
            ))
            .header("Accept", "application/json")
            .send()
            .await
            .map_err(|e| AuthError::Unavailable(e.to_string()))?
            .error_for_status()
            .map_err(|e| AuthError::Unavailable(e.to_string()))?
            .json()
            .await
            .map_err(|e| AuthError::Unavailable(e.to_string()))?;
        if let Ok(mut guard) = self.metadata.lock() {
            *guard = Some((v.clone(), Instant::now()));
        }
        Ok(v)
    }
}

fn urlencode(s: &str) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(b as char);
        } else {
            let _ = write!(out, "%{b:02X}");
        }
    }
    out
}
