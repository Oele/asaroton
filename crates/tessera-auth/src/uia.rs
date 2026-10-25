//! User-Interactive Authentication (UIA) sessions.
//!
//! A UIA-protected endpoint declares the flows it accepts. The client first calls without
//! `auth`, receives a 401 with the flows and a session ID, then repeats the call with `auth`
//! for each stage until one flow is complete.

use std::time::{Duration, Instant};

use dashmap::DashMap;
use ruma::{
    OwnedUserId,
    api::{
        client::uiaa::{AuthData, AuthFlow, AuthType, UiaaInfo},
        error::{ErrorKind, StandardErrorBody},
    },
};

const SESSION_TTL: Duration = Duration::from_mins(15);

/// A pending UIA session.
#[derive(Clone, Debug)]
pub struct UiaSession {
    pub flows: Vec<AuthFlow>,
    pub completed: Vec<AuthType>,
    pub created: Instant,
    /// The user the session was started for, when known (e.g. password change).
    pub user_id: Option<OwnedUserId>,
    /// Request-specific data a handler wants to remember across stages.
    pub params: Option<serde_json::Value>,
}

/// In-memory UIA session store.
#[derive(Debug, Default)]
pub struct UiaSessions {
    sessions: DashMap<String, UiaSession>,
}

/// Result of checking a request's `auth` against a session.
#[derive(Debug)]
pub enum UiaOutcome {
    /// Some flow is complete; the handler may proceed. The session has been removed.
    Complete {
        session: String,
        user_id: Option<OwnedUserId>,
        params: Option<serde_json::Value>,
    },
    /// More stages are needed: respond 401 with this info.
    Incomplete(Box<UiaaInfo>),
    /// The request must be refused outright.
    Rejected { kind: ErrorKind, message: String },
}

/// The result of validating one stage: whether it passed, and for password stages, which
/// user it authenticated.
#[derive(Debug)]
pub enum StageResult {
    Passed {
        user_id: Option<OwnedUserId>,
    },
    /// The stage failed; the client may retry within the same session (401).
    Failed {
        kind: ErrorKind,
        message: String,
    },
    /// The request itself is not allowed (e.g. authenticating as a different user): 403.
    Rejected {
        kind: ErrorKind,
        message: String,
    },
}

impl UiaSessions {
    fn evict_expired(&self) {
        let now = Instant::now();
        self.sessions
            .retain(|_, s| now.duration_since(s.created) < SESSION_TTL);
    }

    /// Run one round of UIA.
    ///
    /// * `flows`: the flows this endpoint accepts.
    /// * `auth`: the client's `auth` dict, if any.
    /// * `user_id`: the authenticated user if the endpoint is called with an access token.
    /// * `params`: per-request data to store on a fresh session.
    /// * `check_stage`: validates the stage in `auth` (async, since passwords hit the store).
    pub async fn step<F>(
        &self,
        flows: Vec<AuthFlow>,
        auth: Option<&AuthData>,
        user_id: Option<OwnedUserId>,
        params: Option<serde_json::Value>,
        check_stage: F,
    ) -> UiaOutcome
    where
        F: AsyncFnOnce(AuthType, &AuthData, Option<&OwnedUserId>) -> StageResult,
    {
        self.evict_expired();

        let Some(auth) = auth else {
            return UiaOutcome::Incomplete(Box::new(self.new_session(flows, user_id, params)));
        };

        let stage_type = auth.auth_type();
        let session_id = auth.session().map(str::to_owned);

        // Find or create the session.
        let (session_id, mut session) =
            if let Some(found) = session_id.and_then(|id| self.sessions.remove(&id)) {
                found
            } else {
                let info = self.new_session(flows, user_id.clone(), params);
                let id = info.session.clone().unwrap_or_default();
                match self.sessions.remove(&id) {
                    Some((id, s)) => (id, s),
                    None => return UiaOutcome::Incomplete(Box::new(info)),
                }
            };

        let Some(stage_type) = stage_type else {
            session.completed.clear();
            return UiaOutcome::Incomplete(Box::new(self.reinsert_with_error(
                session_id,
                session,
                ErrorKind::Unrecognized,
                "Unsupported authentication type.",
            )));
        };

        // The stage must appear in some flow.
        if !session.flows.iter().any(|f| f.stages.contains(&stage_type)) {
            return UiaOutcome::Incomplete(Box::new(self.reinsert_with_error(
                session_id,
                session,
                ErrorKind::Forbidden,
                "Authentication type not allowed for this session.",
            )));
        }

        match check_stage(
            stage_type.clone(),
            auth,
            session.user_id.as_ref().or(user_id.as_ref()),
        )
        .await
        {
            StageResult::Passed { user_id: authed } => {
                if session.user_id.is_none() {
                    session.user_id = authed;
                }
                if !session.completed.contains(&stage_type) {
                    session.completed.push(stage_type);
                }
            }
            StageResult::Failed { kind, message } => {
                return UiaOutcome::Incomplete(Box::new(
                    self.reinsert_with_error(session_id, session, kind, message),
                ));
            }
            StageResult::Rejected { kind, message } => {
                self.sessions.insert(session_id, session);
                return UiaOutcome::Rejected { kind, message };
            }
        }

        let done = session
            .flows
            .iter()
            .any(|f| f.stages.iter().all(|s| session.completed.contains(s)));
        if done {
            UiaOutcome::Complete {
                session: session_id,
                user_id: session.user_id,
                params: session.params,
            }
        } else {
            let info = info_for(&session_id, &session, None);
            self.sessions.insert(session_id, session);
            UiaOutcome::Incomplete(Box::new(info))
        }
    }

    fn new_session(
        &self,
        flows: Vec<AuthFlow>,
        user_id: Option<OwnedUserId>,
        params: Option<serde_json::Value>,
    ) -> UiaaInfo {
        let id = crate::token::generate_session_id();
        let session = UiaSession {
            flows,
            completed: Vec::new(),
            created: Instant::now(),
            user_id,
            params,
        };
        let info = info_for(&id, &session, None);
        self.sessions.insert(id, session);
        info
    }

    fn reinsert_with_error(
        &self,
        id: String,
        session: UiaSession,
        kind: ErrorKind,
        message: impl Into<String>,
    ) -> UiaaInfo {
        let info = info_for(
            &id,
            &session,
            Some(StandardErrorBody::new(kind, message.into())),
        );
        self.sessions.insert(id, session);
        info
    }
}

fn info_for(id: &str, session: &UiaSession, error: Option<StandardErrorBody>) -> UiaaInfo {
    let mut info = UiaaInfo::new(session.flows.clone());
    info.completed.clone_from(&session.completed);
    info.session = Some(id.to_owned());
    info.auth_error = error.map(Box::new);
    info
}
