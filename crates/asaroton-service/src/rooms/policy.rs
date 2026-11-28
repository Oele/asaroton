//! Policy servers (Matrix v1.18): a room that uses one has it sign every event before the
//! event goes anywhere. See [`asaroton_core::policy`] for when a room uses one.
//!
//! Local events are signed between being built and being persisted
//! ([`Services::policy_sign_local`], called by the pipeline): a refusal fails the client's
//! request with the policy server's own error, and the event is dropped. Events from other
//! servers are checked by the federation crate (step 7 of the checks on receipt of a PDU)
//! with [`Services::policy_signed`], and soft-failed when there is still no valid signature.
//!
//! When the policy server cannot be reached, the event is not sent: no signature means "not
//! recommended for inclusion", and the spec's way out for a room whose policy server is gone
//! is that the `m.room.policy` event itself needs no signature, so the room can drop it.

use asaroton_core::{
    Pdu,
    policy::{KEY_ID, RoomPolicy, needs_signature},
};
use ruma::{RoomId, ServerName, events::StateEventType, room_version_rules::RoomVersionRules};

use crate::{Error, Result, Services, rooms::pipeline::RoomContext};

/// Why a policy server's signature could not be had.
#[derive(Debug, Clone)]
pub enum PolicyRefusal {
    /// The policy server answered with a Matrix error (it refuses, or does not know the room,
    /// or is no policy server). A client gets it as it came.
    Answered {
        status: http::StatusCode,
        body: serde_json::Value,
    },
    /// It answered, but without a signature that is valid for the room's key.
    BadSignature,
    /// No usable answer: unreachable, too slow, or an answer that is no Matrix error.
    Unavailable(String),
}

impl std::fmt::Display for PolicyRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Answered { status, body } => write!(f, "refused ({status}): {body}"),
            Self::BadSignature => f.write_str("no valid signature"),
            Self::Unavailable(why) => write!(f, "unavailable: {why}"),
        }
    }
}

impl PolicyRefusal {
    /// The error a client gets for an event the policy server of the room did not sign.
    pub fn into_error(self, via: &ServerName) -> Error {
        match self {
            Self::Answered { status, body } => Error::Relayed { status, body },
            Self::BadSignature => Error::forbidden(format!(
                "The room's policy server ({via}) did not give this event a valid signature, \
                 so it was not sent."
            )),
            Self::Unavailable(_) => Error::matrix(
                ruma::api::error::ErrorKind::Unknown,
                http::StatusCode::BAD_GATEWAY,
                format!(
                    "The room's policy server ({via}) could not be reached, so the event was \
                     not sent. Try again later."
                ),
            ),
        }
    }

    /// What our own federation API answers to `POST /_matrix/policy/v1/sign`: this server is
    /// no policy server. Given without a request when a room names this server.
    pub fn not_a_policy_server() -> Self {
        Self::Answered {
            status: http::StatusCode::NOT_FOUND,
            body: serde_json::json!({
                "errcode": "M_UNRECOGNIZED",
                "error": "This server is not a policy server.",
            }),
        }
    }
}

impl Services {
    /// `pdu` with a valid signature of `policy`'s server: as it is when it carries one (or
    /// is the room's `m.room.policy` event, which needs none), otherwise with the signature
    /// the policy server gives when asked, once. `policy` is `None` for a room that uses no
    /// policy server, and then nothing happens.
    pub async fn policy_signed(
        &self,
        policy: Option<&RoomPolicy>,
        pdu: Pdu,
        rules: &RoomVersionRules,
    ) -> std::result::Result<Pdu, PolicyRefusal> {
        let Some(policy) = policy else {
            return Ok(pdu);
        };
        if !needs_signature(pdu.event_type(), pdu.state_key()) || policy.signed(pdu.raw(), rules) {
            return Ok(pdu);
        }
        let via = policy.via();
        if via == self.config.server_name {
            return Err(PolicyRefusal::not_a_policy_server());
        }
        let signature = self.federation().policy_sign(via, &pdu).await?;
        let signed = pdu.with_signature(via.as_str(), KEY_ID, signature);
        if policy.signed(signed.raw(), rules) {
            Ok(signed)
        } else {
            Err(PolicyRefusal::BadSignature)
        }
    }

    /// For the local pipeline: `pdu` signed by the room's policy server, judged by the room's
    /// current state in `ctx`. A refusal becomes the error the client gets.
    pub async fn policy_sign_local(&self, ctx: &RoomContext, pdu: Pdu) -> Result<Pdu> {
        let Some(policy) = asaroton_core::policy::in_use(&ctx.state) else {
            return Ok(pdu);
        };
        let event_id = pdu.event_id().to_owned();
        self.policy_signed(Some(&policy), pdu, &ctx.rules)
            .await
            .map_err(|refusal| {
                tracing::info!(room_id = %ctx.room_id, %event_id, via = %policy.via(), %refusal, "the policy server did not sign a local event; it is not sent");
                refusal.into_error(policy.via())
            })
    }

    /// The policy server a room uses, from the store: one lookup for a room without an
    /// `m.room.policy` event; with one, whether a member of the server it names is joined.
    /// For callers that hold no room context.
    pub async fn room_policy(&self, room_id: &RoomId) -> Result<Option<RoomPolicy>> {
        let found = self
            .store
            .current_state_pdus_for(room_id, &[(StateEventType::RoomPolicy, String::new())])
            .await?;
        let Some(policy) = found
            .first()
            .and_then(|e| RoomPolicy::from_content(e.pdu.content()))
        else {
            return Ok(None);
        };
        let joined = self
            .store
            .servers_in_room(room_id)
            .await?
            .iter()
            .any(|s| s == policy.via());
        Ok(joined.then_some(policy))
    }
}
