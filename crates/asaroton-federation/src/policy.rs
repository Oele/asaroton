//! Asking a room's policy server for its signature (`POST /_matrix/policy/v1/sign`, Matrix
//! v1.18), for our own events and for those of other servers that arrive without one.
//!
//! The request has a short timeout: the event waits for the answer, and with it the room
//! (the room lock is held). A policy server that could not be reached at all is not asked
//! again for a little while; until then every event that needs it fails at once instead of
//! each waiting out the timeout. Refusals are not remembered: the next event is asked about.
//!
//! The spec has a refusal passed to the client verbatim. Only the answers it defines for
//! `/sign` are (400, 403, 404 and 429, with the errcodes listed there): the policy server is
//! whoever a room's admins name, and errcodes such as `M_UNKNOWN_TOKEN` or
//! `M_CONSENT_NOT_GIVEN` make clients sign out or show a link. Any other answer becomes a
//! plain 403 `M_FORBIDDEN` with the policy server's message.

use std::time::{Duration, Instant};

use asaroton_core::{
    Pdu,
    policy::{KEY_ID, needs_signature},
};
use asaroton_service::rooms::policy::PolicyRefusal;
use ruma::{EventId, RoomId, ServerName, api::federation::policy::sign_event};
use serde_json::json;

use crate::{Error, Federation, Result};

/// How long the policy server gets to answer, all told (finding it included).
pub(crate) const SIGN_TIMEOUT: Duration = Duration::from_secs(5);
/// How long a policy server that could not be reached is taken to be down.
const DOWN_FOR: Duration = Duration::from_secs(30);
/// The most a `/sign` answer may hold: one signature, or an error.
const MAX_ANSWER: usize = 64 * 1024;
/// The longest wait a relayed rate limit may ask of a client.
const MAX_RETRY_AFTER_MS: u64 = 60_000;
/// The longest message of a policy server that is passed on.
const MAX_MESSAGE: usize = 1000;

impl Federation {
    /// Ask `via` to sign `pdu`; its signature, not yet checked.
    pub(crate) async fn ask_policy_server(
        &self,
        via: &ServerName,
        pdu: &Pdu,
    ) -> std::result::Result<String, PolicyRefusal> {
        if let Some(at) = self.policy_down.get(via).map(|at| *at)
            && at.elapsed() < DOWN_FOR
        {
            return Err(PolicyRefusal::Unavailable(format!(
                "not reachable {}s ago",
                at.elapsed().as_secs()
            )));
        }
        let body = serde_json::value::to_raw_value(&asaroton_core::canonical::to_json_value(
            pdu.to_federation_json(),
        ))
        .map_err(|e| PolicyRefusal::Unavailable(format!("cannot encode the event: {e}")))?;
        let request = sign_event::v1::Request::new(body);
        let answer = tokio::time::timeout(
            SIGN_TIMEOUT,
            self.client
                .send_with_timeout(via, request, SIGN_TIMEOUT, Some(MAX_ANSWER)),
        )
        .await;
        let (refusal, unreachable) = match answer {
            Ok(Ok(response)) => {
                self.policy_down.remove(via);
                return response
                    .ed25519_signature(via)
                    .map(str::to_owned)
                    .ok_or(PolicyRefusal::BadSignature);
            }
            Ok(Err(e)) => (
                refusal_for(&e),
                matches!(e, Error::Transport(..) | Error::Discovery(..)),
            ),
            Err(_) => (
                PolicyRefusal::Unavailable(format!("no answer within {}s", SIGN_TIMEOUT.as_secs())),
                true,
            ),
        };
        if unreachable {
            self.policy_down.insert(via.to_owned(), Instant::now());
        } else {
            self.policy_down.remove(via);
        }
        Err(refusal)
    }

    /// Before a resident-server endpoint (`/send_join`, `/send_leave`, `/send_knock`) takes
    /// an event in: the event signed by the room's policy server. The joining (leaving,
    /// knocking) server could not ask, not knowing the room's state. A refusal fails the
    /// request with the policy server's error, so that server knows the event did not get
    /// in; it is not stored. A server the room's ACL denies is not asked about: the event is
    /// refused for that right after.
    pub(crate) async fn policy_sign_for_resident_endpoint(
        &self,
        origin: &ServerName,
        room_id: &RoomId,
        pdu: Pdu,
        rules: &ruma::room_version_rules::RoomVersionRules,
    ) -> Result<Pdu> {
        let Some(policy) = self.services.room_policy(room_id).await? else {
            return Ok(pdu);
        };
        if !self.services.server_acl_allows(room_id, origin).await? {
            return Ok(pdu);
        }
        let event_id = pdu.event_id().to_owned();
        self.services
            .policy_signed(Some(&policy), pdu, rules)
            .await
            .map(|(signed, _)| signed)
            .map_err(|refusal| {
                tracing::info!(%room_id, %event_id, via = %policy.via(), %refusal, "the policy server did not sign an event for a resident endpoint; refused");
                Error::Service(refusal.into_error(policy.via()))
            })
    }

    /// Whether one of our own events, already stored, may go out to the room's other
    /// servers: in a room with a policy server only with its signature, which is asked for
    /// and stored with the event when it lacks one. For our join through another server,
    /// whose state we only know once joined.
    pub(crate) async fn policy_signed_for_fan_out(
        &self,
        room_id: &RoomId,
        event_id: &EventId,
        rules: &ruma::room_version_rules::RoomVersionRules,
    ) -> Result<bool> {
        let Some(policy) = self.services.room_policy(room_id).await? else {
            return Ok(true);
        };
        let Some(stored) = self.services.store.get_event(event_id).await? else {
            return Ok(false);
        };
        let pdu = stored.pdu;
        if !needs_signature(pdu.event_type(), pdu.state_key()) || policy.signed(pdu.raw(), rules) {
            return Ok(true);
        }
        match self.services.policy_signed(Some(&policy), pdu, rules).await {
            Ok((signed, _)) => {
                self.keep_policy_signature(&signed, policy.via()).await?;
                Ok(true)
            }
            Err(refusal) => {
                tracing::info!(%room_id, %event_id, via = %policy.via(), %refusal, "the policy server did not sign our event; it is not sent to the other servers");
                Ok(false)
            }
        }
    }

    /// Store a policy server's signature that `pdu` gained after it was first stored.
    pub(crate) async fn keep_policy_signature(&self, pdu: &Pdu, via: &ServerName) -> Result<()> {
        let signature = pdu
            .raw()
            .get("signatures")
            .and_then(|s| s.as_object())
            .and_then(|s| s.get(via.as_str()))
            .and_then(|s| s.as_object())
            .and_then(|s| s.get(KEY_ID))
            .and_then(|s| s.as_str());
        if let Some(signature) = signature {
            self.services
                .store
                .add_event_signature(pdu.event_id(), via.as_str(), KEY_ID, signature)
                .await?;
            self.event_cache
                .insert(pdu.event_id().to_owned(), pdu.clone());
        }
        Ok(())
    }
}

/// What a failed `/sign` request means. A Matrix error from the policy server is its answer
/// (see the module's note on which are passed on as they came); anything else (no
/// connection, a timeout, a server error, an error page from a proxy, a success that is no
/// signature map) means it is not available.
fn refusal_for(error: &Error) -> PolicyRefusal {
    let Error::RemoteError { status, body, .. } = error else {
        return PolicyRefusal::Unavailable(error.to_string());
    };
    if status.is_server_error() {
        return PolicyRefusal::Unavailable(format!("answered {status}"));
    }
    let Some(body) = matrix_error(body) else {
        return PolicyRefusal::Unavailable(format!("answered {status} without a Matrix error"));
    };
    let errcode = body.get("errcode").and_then(|e| e.as_str()).unwrap_or("");
    let message: String = body
        .get("error")
        .and_then(|e| e.as_str())
        .unwrap_or("")
        .chars()
        .take(MAX_MESSAGE)
        .collect();
    let defined = matches!(
        (status.as_u16(), errcode),
        (400, "M_FORBIDDEN" | "M_BAD_JSON" | "M_NOT_JSON")
            | (403, "M_FORBIDDEN")
            | (404, "M_NOT_FOUND" | "M_UNRECOGNIZED")
            | (429, "M_LIMIT_EXCEEDED")
    );
    if !defined {
        return PolicyRefusal::Answered {
            status: http::StatusCode::FORBIDDEN,
            body: json!({ "errcode": "M_FORBIDDEN", "error": message }),
        };
    }
    let mut relayed = json!({ "errcode": errcode, "error": message });
    if let Some(ms) = body
        .get("retry_after_ms")
        .and_then(serde_json::Value::as_u64)
    {
        relayed["retry_after_ms"] = json!(ms.min(MAX_RETRY_AFTER_MS));
    }
    PolicyRefusal::Answered {
        status: *status,
        body: relayed,
    }
}

/// The body of a Matrix error response: a JSON object with a string `errcode`.
fn matrix_error(body: &str) -> Option<serde_json::Value> {
    let value: serde_json::Value = serde_json::from_str(body).ok()?;
    value.get("errcode")?.as_str()?;
    Some(value)
}

#[cfg(test)]
mod tests {
    use ruma::owned_server_name;
    use serde_json::json;

    use super::*;

    fn remote(status: u16, body: &str) -> Error {
        Error::RemoteError {
            server: owned_server_name!("ps.example.org"),
            status: http::StatusCode::from_u16(status).unwrap(),
            body: body.to_owned(),
        }
    }

    fn answered(error: &Error) -> (u16, serde_json::Value) {
        match refusal_for(error) {
            PolicyRefusal::Answered { status, body } => (status.as_u16(), body),
            other => panic!("{error}: {other:?}"),
        }
    }

    #[test]
    fn the_answers_the_spec_defines_are_passed_on_as_they_came() {
        for (status, errcode) in [
            (400, "M_FORBIDDEN"),
            (400, "M_BAD_JSON"),
            (400, "M_NOT_JSON"),
            (403, "M_FORBIDDEN"),
            (404, "M_NOT_FOUND"),
            (404, "M_UNRECOGNIZED"),
        ] {
            let body = json!({ "errcode": errcode, "error": "The message appears to contain possible spam" });
            assert_eq!(answered(&remote(status, &body.to_string())), (status, body));
        }
        // A rate limit keeps its wait, within reason.
        let limited =
            json!({ "errcode": "M_LIMIT_EXCEEDED", "error": "slow down", "retry_after_ms": 2000 });
        assert_eq!(answered(&remote(429, &limited.to_string())), (429, limited));
        let greedy = json!({ "errcode": "M_LIMIT_EXCEEDED", "error": "wait", "retry_after_ms": 86_400_000u64 });
        assert_eq!(
            answered(&remote(429, &greedy.to_string())).1["retry_after_ms"],
            60_000
        );
    }

    #[test]
    fn other_answers_become_a_plain_refusal() {
        // Errcodes that make clients act: they are no business of a policy server.
        for (status, body) in [
            (
                401,
                json!({ "errcode": "M_UNKNOWN_TOKEN", "error": "gone", "soft_logout": false }),
            ),
            (
                403,
                json!({ "errcode": "M_USER_DEACTIVATED", "error": "gone" }),
            ),
            (
                403,
                json!({ "errcode": "M_CONSENT_NOT_GIVEN", "error": "gone", "consent_uri": "https://evil.example" }),
            ),
            (
                400,
                json!({ "errcode": "M_LIMIT_EXCEEDED", "error": "gone" }),
            ),
            (413, json!({ "errcode": "M_TOO_LARGE", "error": "gone" })),
        ] {
            assert_eq!(
                answered(&remote(status, &body.to_string())),
                (403, json!({ "errcode": "M_FORBIDDEN", "error": "gone" })),
                "{body}"
            );
        }
    }

    #[test]
    fn anything_else_means_the_policy_server_is_not_available() {
        for error in [
            remote(500, "{\"errcode\": \"M_UNKNOWN\", \"error\": \"oops\"}"),
            remote(502, "<html>Bad Gateway</html>"),
            remote(400, "{\"error\": \"no errcode\"}"),
            remote(404, "[]"),
            Error::Transport(owned_server_name!("ps.example.org"), "refused".into()),
            Error::Discovery(owned_server_name!("ps.example.org"), "no such host".into()),
            Error::Protocol("ps.example.org: bad response: invalid type".into()),
            Error::TooLarge(owned_server_name!("ps.example.org"), MAX_ANSWER),
        ] {
            assert!(
                matches!(refusal_for(&error), PolicyRefusal::Unavailable(_)),
                "{error}"
            );
        }
    }
}
