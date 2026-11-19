//! Email addresses on accounts (third-party identifiers), verified by this server itself: no
//! identity server. A client asks for a code to be mailed (`requestToken`), the user gives the
//! code back (or opens the link in the mail), and the client adds the validated address to the
//! account with the user's password. An address belongs to at most one account.

use std::fmt::Write as _;

use asaroton_store::{ThreepidSession, UserThreepid, now_ts};
use ruma::{OwnedUserId, UserId, api::error::ErrorKind};
use sha2::{Digest, Sha256};

use crate::{Error, Result, Services};

/// How long a code works.
const SESSION_LIFETIME_MS: i64 = 60 * 60 * 1000;
/// The least time between two mails to one address.
const RESEND_AFTER_MS: i64 = 60 * 1000;
/// Wrong codes a session takes before it is spent.
const MAX_FAILURES: i32 = 5;
pub const MEDIUM_EMAIL: &str = "email";
/// Where clients send the code back (`submit_url`), below the public base URL.
pub const SUBMIT_PATH: &str = "/_matrix/client/unstable/add_threepid/email/submit_token";

/// What `request_email_token` hands back.
#[derive(Debug)]
pub struct TokenRequested {
    pub sid: String,
    pub submit_url: String,
}

fn digest(token: &str) -> String {
    let mut out = String::new();
    for b in Sha256::digest(token.as_bytes()) {
        let _ = write!(out, "{b:02x}");
    }
    out
}

fn threepid_error(kind: ErrorKind, message: &str) -> Error {
    Error::matrix(kind, http::StatusCode::BAD_REQUEST, message)
}

/// An email address as it is stored: trimmed and in lower case, if it looks like one.
pub fn normalise_email(raw: &str) -> Option<String> {
    let email = raw.trim().to_lowercase();
    let (local, domain) = email.split_once('@')?;
    let ok = !local.is_empty()
        && domain.contains('.')
        && !domain.starts_with('.')
        && !domain.ends_with('.')
        && !domain.contains('@')
        && email.len() <= 254
        && !email
            .chars()
            .any(|c| c.is_whitespace() || c.is_control() || c == '<' || c == '>');
    ok.then_some(email)
}

impl Services {
    /// Mails a code for adding `email` to an account. A repeated request with the same client
    /// secret and a `send_attempt` that is not higher gets the same session without a mail.
    pub async fn request_email_token(
        &self,
        client_secret: &str,
        raw_email: &str,
        send_attempt: u64,
        next_link: Option<&str>,
    ) -> Result<TokenRequested> {
        if self.config.email.is_none() {
            return Err(Error::matrix(
                ErrorKind::ThreepidDenied,
                http::StatusCode::FORBIDDEN,
                "This server does not send mail, so email addresses cannot be added.",
            ));
        }
        let email = normalise_email(raw_email)
            .ok_or_else(|| Error::invalid_param("That is not an email address."))?;
        if self
            .store
            .threepid_owner(MEDIUM_EMAIL, &email)
            .await?
            .is_some()
        {
            return Err(threepid_error(
                ErrorKind::ThreepidInUse,
                "That email address is already on an account.",
            ));
        }
        let submit_url = format!("{}{SUBMIT_PATH}", self.config.public_base_url);
        let send_attempt = i64::try_from(send_attempt).unwrap_or(i64::MAX);
        let now = now_ts();
        let existing = self
            .store
            .threepid_session_for(client_secret, MEDIUM_EMAIL, &email)
            .await?
            .filter(|s| s.expires_ts > now);
        if let Some(s) = &existing {
            if send_attempt <= s.send_attempt {
                return Ok(TokenRequested {
                    sid: s.sid.clone(),
                    submit_url,
                });
            }
            if now - s.last_sent_ts < RESEND_AFTER_MS {
                let mut limit = ruma::api::error::LimitExceededErrorData::new();
                limit.retry_after = Some(ruma::api::error::RetryAfter::Delay(
                    std::time::Duration::from_millis(
                        (RESEND_AFTER_MS - (now - s.last_sent_ts)).unsigned_abs(),
                    ),
                ));
                return Err(Error::matrix(
                    ErrorKind::LimitExceeded(limit),
                    http::StatusCode::TOO_MANY_REQUESTS,
                    "A mail went to that address a moment ago.",
                ));
            }
        }
        let token = format!("{:06}", rand::random_range(0..1_000_000u32));
        let sid = existing
            .as_ref()
            .map_or_else(asaroton_auth::generate_session_id, |s| s.sid.clone());
        let link = format!(
            "{submit_url}?sid={}&client_secret={}&token={token}",
            percent(&sid),
            percent(client_secret)
        );
        let server = &self.config.server_name;
        let body = format!(
            "Someone, hopefully you, asked to add this email address to a Matrix account on {server}.\n\n\
             The code is: {token}\n\n\
             Or open this link to confirm it:\n{link}\n\n\
             The code works for an hour. If you did not ask for this, you can ignore this mail; \
             nothing changes unless the code is used.\n"
        );
        self.send_mail(&email, &format!("Your code for {server}: {token}"), &body)
            .await?;
        self.store
            .put_threepid_session(ThreepidSession {
                sid: sid.clone(),
                client_secret: client_secret.to_owned(),
                medium: MEDIUM_EMAIL.to_owned(),
                address: email,
                token_hash: digest(&token),
                send_attempt,
                next_link: next_link.map(str::to_owned),
                created_ts: existing.as_ref().map_or(now, |s| s.created_ts),
                last_sent_ts: now,
                expires_ts: now + SESSION_LIFETIME_MS,
                validated_at: None,
                failures: 0,
            })
            .await?;
        let _ = self.store.purge_expired_threepid_sessions(now).await;
        Ok(TokenRequested { sid, submit_url })
    }

    /// Checks a mailed code. Returns the session's `next_link` once validated.
    pub async fn submit_email_token(
        &self,
        sid: &str,
        client_secret: &str,
        token: &str,
    ) -> Result<Option<String>> {
        let now = now_ts();
        let session = self
            .store
            .threepid_session(sid)
            .await?
            .filter(|s| s.client_secret == client_secret && s.expires_ts > now)
            .ok_or_else(|| {
                threepid_error(
                    ErrorKind::ThreepidAuthFailed,
                    "Unknown or expired verification. Ask for a new code.",
                )
            })?;
        if session.validated_at.is_some() {
            return Ok(session.next_link);
        }
        if session.failures >= MAX_FAILURES {
            return Err(threepid_error(
                ErrorKind::ThreepidAuthFailed,
                "Too many wrong codes. Ask for a new one.",
            ));
        }
        if digest(token.trim()) != session.token_hash {
            self.store.update_threepid_session(sid, None).await?;
            return Err(threepid_error(
                ErrorKind::ThreepidAuthFailed,
                "That code is not right.",
            ));
        }
        self.store.update_threepid_session(sid, Some(now)).await?;
        Ok(session.next_link)
    }

    /// Adds the address of a validated session to `user`'s account.
    pub async fn add_threepid(&self, user: &UserId, sid: &str, client_secret: &str) -> Result<()> {
        let now = now_ts();
        let session = self
            .store
            .threepid_session(sid)
            .await?
            .filter(|s| s.client_secret == client_secret && s.expires_ts > now)
            .ok_or_else(|| {
                threepid_error(
                    ErrorKind::ThreepidAuthFailed,
                    "Unknown or expired verification. Ask for a new code.",
                )
            })?;
        let Some(validated_at) = session.validated_at else {
            return Err(threepid_error(
                ErrorKind::ThreepidAuthFailed,
                "The address has not been confirmed yet: give the code from the mail first.",
            ));
        };
        let added = self
            .store
            .add_user_threepid(UserThreepid {
                user_id: user.to_owned(),
                medium: session.medium.clone(),
                address: session.address.clone(),
                validated_at,
                added_at: now,
            })
            .await?;
        if !added
            && self
                .store
                .threepid_owner(&session.medium, &session.address)
                .await?
                .as_deref()
                != Some(user)
        {
            return Err(threepid_error(
                ErrorKind::ThreepidInUse,
                "That email address is already on an account.",
            ));
        }
        self.store.delete_threepid_session(sid).await?;
        Ok(())
    }

    pub async fn threepids(&self, user: &UserId) -> Result<Vec<UserThreepid>> {
        Ok(self.store.user_threepids(user).await?)
    }

    /// Removes an address from `user`'s account; whether it was there.
    pub async fn delete_threepid(
        &self,
        user: &UserId,
        medium: &str,
        address: &str,
    ) -> Result<bool> {
        let address = if medium == MEDIUM_EMAIL {
            normalise_email(address).unwrap_or_else(|| address.to_owned())
        } else {
            address.to_owned()
        };
        Ok(self
            .store
            .delete_user_threepid(user, medium, &address)
            .await?)
    }

    /// The account an address belongs to.
    pub async fn threepid_owner(&self, medium: &str, address: &str) -> Result<Option<OwnedUserId>> {
        Ok(self.store.threepid_owner(medium, address).await?)
    }
}

fn percent(s: &str) -> String {
    percent_encoding::utf8_percent_encode(s, percent_encoding::NON_ALPHANUMERIC).to_string()
}

#[cfg(test)]
mod tests {
    use super::normalise_email;

    #[test]
    fn email_addresses_are_normalised_and_checked() {
        assert_eq!(
            normalise_email("  Ann@Example.ORG "),
            Some("ann@example.org".into())
        );
        assert_eq!(normalise_email("ann@example"), None);
        assert_eq!(normalise_email("@example.org"), None);
        assert_eq!(normalise_email("ann example@example.org"), None);
        assert_eq!(normalise_email("ann@@example.org"), None);
        assert_eq!(normalise_email("<ann@example.org>"), None);
    }
}
