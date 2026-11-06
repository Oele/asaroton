//! The `LiveKit` server API (Twirp over HTTP with JSON bodies), for the few calls a
//! homeserver makes: creating rooms, listing participants and removing them.

use std::time::Duration;

use serde_json::{Value, json};

use super::jwt;

/// Access to one `LiveKit` SFU's management API.
pub struct LivekitApi {
    http: reqwest::Client,
    api_url: String,
    api_key: String,
    api_secret: String,
}

impl std::fmt::Debug for LivekitApi {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LivekitApi")
            .field("api_url", &self.api_url)
            .field("api_key", &self.api_key)
            .finish_non_exhaustive()
    }
}

/// A participant as the SFU reports it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Participant {
    pub identity: String,
    /// `JOINING`, `JOINED`, `ACTIVE` or `DISCONNECTED`.
    pub state: String,
}

impl Participant {
    /// Whether the participant counts as connected.
    pub fn is_connected(&self) -> bool {
        self.state != "DISCONNECTED"
    }
}

/// What went wrong talking to the SFU.
#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("LiveKit API unreachable: {0}")]
    Transport(String),
    #[error("LiveKit API error {status}: {code}: {message}")]
    Remote {
        status: u16,
        code: String,
        message: String,
    },
}

impl ApiError {
    /// The SFU does not know the room or participant.
    pub fn is_not_found(&self) -> bool {
        matches!(self, Self::Remote { code, .. } if code == "not_found")
    }
}

impl LivekitApi {
    pub fn new(api_url: &str, api_key: &str, api_secret: &str) -> Self {
        Self {
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .unwrap_or_default(),
            api_url: api_url.trim_end_matches('/').to_owned(),
            api_key: api_key.to_owned(),
            api_secret: api_secret.to_owned(),
        }
    }

    pub fn api_key(&self) -> &str {
        &self.api_key
    }

    pub fn api_secret(&self) -> &[u8] {
        self.api_secret.as_bytes()
    }

    /// A short-lived token for the management API with the given video grants.
    fn admin_token(&self, video: &Value) -> String {
        let now = now_secs();
        jwt::sign(
            self.api_secret(),
            &jwt::livekit_claims(&self.api_key, None, video, now, 60),
        )
    }

    async fn call(&self, method: &str, video: Value, body: Value) -> Result<Value, ApiError> {
        let url = format!("{}/twirp/livekit.RoomService/{method}", self.api_url);
        let resp = self
            .http
            .post(&url)
            .bearer_auth(self.admin_token(&video))
            .header(http::header::CONTENT_TYPE, "application/json")
            .body(body.to_string())
            .send()
            .await
            .map_err(|e| ApiError::Transport(e.to_string()))?;
        let status = resp.status();
        let bytes = resp
            .bytes()
            .await
            .map_err(|e| ApiError::Transport(e.to_string()))?;
        let value: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
        if !status.is_success() {
            return Err(ApiError::Remote {
                status: status.as_u16(),
                code: value
                    .get("code")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown")
                    .to_owned(),
                message: value
                    .get("msg")
                    .and_then(Value::as_str)
                    .unwrap_or_else(|| std::str::from_utf8(&bytes).unwrap_or(""))
                    .to_owned(),
            });
        }
        Ok(value)
    }

    /// Creates the room, or returns the existing one. The room stays open five minutes while
    /// empty and twenty seconds after the last participant leaves, as the reference service
    /// does.
    pub async fn create_room(&self, name: &str) -> Result<(), ApiError> {
        self.call(
            "CreateRoom",
            json!({ "roomCreate": true }),
            json!({
                "name": name,
                "empty_timeout": 300,
                "departure_timeout": 20,
                "max_participants": 0,
            }),
        )
        .await
        .map(|_| ())
    }

    /// The participants of a room. An unknown room is reported as empty.
    pub async fn list_participants(&self, room: &str) -> Result<Vec<Participant>, ApiError> {
        let value = match self
            .call(
                "ListParticipants",
                json!({ "roomAdmin": true, "room": room }),
                json!({ "room": room }),
            )
            .await
        {
            Ok(v) => v,
            Err(e) if e.is_not_found() => return Ok(Vec::new()),
            Err(e) => return Err(e),
        };
        Ok(value
            .get("participants")
            .and_then(Value::as_array)
            .map(|list| {
                list.iter()
                    .filter_map(|p| {
                        Some(Participant {
                            identity: p.get("identity")?.as_str()?.to_owned(),
                            state: p
                                .get("state")
                                .and_then(Value::as_str)
                                .unwrap_or("ACTIVE")
                                .to_owned(),
                        })
                    })
                    .collect()
            })
            .unwrap_or_default())
    }

    /// Removes a participant; an absent participant is not an error.
    pub async fn remove_participant(&self, room: &str, identity: &str) -> Result<(), ApiError> {
        match self
            .call(
                "RemoveParticipant",
                json!({ "roomAdmin": true, "room": room }),
                json!({ "room": room, "identity": identity }),
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(e) if e.is_not_found() => Ok(()),
            Err(e) => Err(e),
        }
    }
}

/// Seconds since the epoch.
pub fn now_secs() -> i64 {
    asaroton_store::now_ts() / 1000
}
