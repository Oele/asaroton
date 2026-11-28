//! Asking a room's policy server for its signature (`POST /_matrix/policy/v1/sign`, Matrix
//! v1.18), for our own events and for those of other servers that arrive without one.
//!
//! The request has a short timeout: the event waits for the answer, and with it the room
//! (the room lock is held). A policy server that could not be reached is not asked again for
//! a little while; until then every event that needs it fails at once instead of each
//! waiting out the timeout. Refusals are not remembered: the next event is asked about.

use std::time::{Duration, Instant};

use asaroton_core::Pdu;
use asaroton_service::rooms::policy::PolicyRefusal;
use ruma::{RoomId, ServerName, api::federation::policy::sign_event};

use crate::{Error, Federation, Result};

/// How long the policy server gets to answer, all told (finding it included).
pub(crate) const SIGN_TIMEOUT: Duration = Duration::from_secs(5);
/// How long a policy server that could not be reached is taken to be down.
const DOWN_FOR: Duration = Duration::from_secs(30);
/// The most a `/sign` answer may hold: one signature, or an error.
const MAX_ANSWER: usize = 64 * 1024;

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
        let refusal = match answer {
            Ok(Ok(response)) => {
                self.policy_down.remove(via);
                return response
                    .ed25519_signature(via)
                    .map(str::to_owned)
                    .ok_or(PolicyRefusal::BadSignature);
            }
            Ok(Err(e)) => refusal_for(&e),
            Err(_) => {
                PolicyRefusal::Unavailable(format!("no answer within {}s", SIGN_TIMEOUT.as_secs()))
            }
        };
        if matches!(refusal, PolicyRefusal::Unavailable(_)) {
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
    /// in; it is not stored.
    pub(crate) async fn policy_sign_for_resident_endpoint(
        &self,
        room_id: &RoomId,
        pdu: Pdu,
        rules: &ruma::room_version_rules::RoomVersionRules,
    ) -> Result<Pdu> {
        let Some(policy) = self.services.room_policy(room_id).await? else {
            return Ok(pdu);
        };
        let event_id = pdu.event_id().to_owned();
        self.services
            .policy_signed(Some(&policy), pdu, rules)
            .await
            .map_err(|refusal| {
                tracing::info!(%room_id, %event_id, via = %policy.via(), %refusal, "the policy server did not sign an event for a resident endpoint; refused");
                Error::Service(refusal.into_error(policy.via()))
            })
    }
}

/// What a failed `/sign` request means. A Matrix error from the policy server is its answer,
/// passed on as it came; anything else (no connection, a timeout, an error page from a proxy,
/// a success that is no signature map) means it is not available.
fn refusal_for(error: &Error) -> PolicyRefusal {
    match error {
        Error::RemoteError { status, body, .. } => match matrix_error(body) {
            Some(body) => PolicyRefusal::Answered {
                status: *status,
                body,
            },
            None => PolicyRefusal::Unavailable(format!("answered {status} without a Matrix error")),
        },
        other => PolicyRefusal::Unavailable(other.to_string()),
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

    #[test]
    fn matrix_errors_are_answers_passed_on_whole() {
        let body = json!({ "errcode": "M_FORBIDDEN", "error": "The message appears to contain possible spam" });
        for status in [400, 403, 404, 429, 500] {
            match refusal_for(&remote(status, &body.to_string())) {
                PolicyRefusal::Answered { status: s, body: b } => {
                    assert_eq!(s.as_u16(), status);
                    assert_eq!(b, body);
                }
                other => panic!("{status}: {other:?}"),
            }
        }
        // Extra fields come along (a rate limit's `retry_after_ms`).
        let limited =
            json!({ "errcode": "M_LIMIT_EXCEEDED", "error": "slow down", "retry_after_ms": 2000 });
        match refusal_for(&remote(429, &limited.to_string())) {
            PolicyRefusal::Answered { body, .. } => assert_eq!(body, limited),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn anything_else_means_the_policy_server_is_not_available() {
        for error in [
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
