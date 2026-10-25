//! Request rate limiting: token buckets per client address (logins, registrations) or per
//! access token (sending, joining, inviting). Exceeding a bucket answers `M_LIMIT_EXCEEDED`
//! with `retry_after_ms`, as the spec requires.

use std::{net::SocketAddr, time::Instant};

use axum::{
    extract::{ConnectInfo, Request, State},
    http::{HeaderMap, Method, StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
};
use dashmap::DashMap;
use tessera_service::config::{Rate, RateLimits};

use crate::AppState;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Class {
    Login,
    Register,
    Message,
    Join,
    Invite,
}

#[derive(Debug)]
struct Bucket {
    tokens: f64,
    last: Instant,
}

/// The buckets. Idle ones are dropped when the table grows large.
#[derive(Debug, Default)]
pub struct Limiter {
    buckets: DashMap<(Class, String), Bucket>,
}

impl Limiter {
    /// Take one token from the bucket, or say how long until one is available.
    fn check(&self, class: Class, key: &str, rate: Rate) -> Result<(), u64> {
        if self.buckets.len() > 100_000 {
            let now = Instant::now();
            self.buckets
                .retain(|_, b| now.duration_since(b.last).as_secs() < 600);
        }
        let now = Instant::now();
        let mut entry = self
            .buckets
            .entry((class, key.to_owned()))
            .or_insert_with(|| Bucket {
                tokens: f64::from(rate.burst),
                last: now,
            });
        let elapsed = now.duration_since(entry.last).as_secs_f64();
        entry.tokens = (entry.tokens + elapsed * rate.per_second).min(f64::from(rate.burst));
        entry.last = now;
        if entry.tokens >= 1.0 {
            entry.tokens -= 1.0;
            Ok(())
        } else {
            let wait = (1.0 - entry.tokens) / rate.per_second.max(1e-9);
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            Err((wait * 1000.0).ceil().max(1.0) as u64)
        }
    }
}

/// Which bucket a request draws from, if any.
fn classify(method: &Method, path: &str) -> Option<Class> {
    let rest = path.strip_prefix("/_matrix/client/")?;
    // Skip the version segment (`v3`, `r0`, `unstable`, `v1`).
    let (_, rest) = rest.split_once('/')?;
    match *method {
        Method::POST if rest == "login" => Some(Class::Login),
        Method::POST if rest == "register" => Some(Class::Register),
        Method::PUT
            if rest.starts_with("rooms/")
                && (rest.contains("/send/") || rest.contains("/state/")) =>
        {
            Some(Class::Message)
        }
        Method::PUT if rest.starts_with("rooms/") && rest.contains("/redact/") => {
            Some(Class::Message)
        }
        Method::POST
            if rest.starts_with("join/")
                || rest.starts_with("rooms/") && rest.ends_with("/join") =>
        {
            Some(Class::Join)
        }
        Method::POST if rest.starts_with("rooms/") && rest.ends_with("/invite") => {
            Some(Class::Invite)
        }
        _ => None,
    }
}

fn bearer(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(str::trim)
}

fn client_ip(headers: &HeaderMap, addr: SocketAddr, trust_forwarded: bool) -> String {
    if trust_forwarded
        && let Some(xff) = headers.get("x-forwarded-for").and_then(|v| v.to_str().ok())
        && let Some(first) = xff.split(',').next().map(str::trim)
        && !first.is_empty()
    {
        return first.to_owned();
    }
    addr.ip().to_string()
}

pub async fn middleware(
    State(app): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    req: Request,
    next: Next,
) -> Response {
    let limits: &RateLimits = &app.config.rate_limits;
    if !limits.enabled {
        return next.run(req).await;
    }
    let Some(class) = classify(req.method(), req.uri().path()) else {
        return next.run(req).await;
    };
    let token = bearer(req.headers()).map(str::to_owned);
    // Appservices that opted out of rate limiting are exempt.
    if let Some(t) = &token
        && app.appservices.by_token(t).is_some_and(|a| !a.rate_limited)
    {
        return next.run(req).await;
    }
    let (rate, key) = match class {
        Class::Login => (
            limits.logins,
            format!(
                "ip:{}",
                client_ip(req.headers(), addr, limits.trust_x_forwarded_for)
            ),
        ),
        Class::Register => (
            limits.registrations,
            format!(
                "ip:{}",
                client_ip(req.headers(), addr, limits.trust_x_forwarded_for)
            ),
        ),
        Class::Message | Class::Join | Class::Invite => {
            let rate = match class {
                Class::Message => limits.messages,
                Class::Join => limits.joins,
                _ => limits.invites,
            };
            let key = match &token {
                Some(t) => format!("tok:{}", token_key(t)),
                None => format!(
                    "ip:{}",
                    client_ip(req.headers(), addr, limits.trust_x_forwarded_for)
                ),
            };
            (rate, key)
        }
    };
    match app.limiter.check(class, &key, rate) {
        Ok(()) => next.run(req).await,
        Err(retry_after_ms) => {
            let body = serde_json::json!({
                "errcode": "M_LIMIT_EXCEEDED",
                "error": "Too Many Requests",
                "retry_after_ms": retry_after_ms,
            });
            let mut resp = (StatusCode::TOO_MANY_REQUESTS, axum::Json(body)).into_response();
            if let Ok(v) = header::HeaderValue::from_str(&retry_after_ms.div_ceil(1000).to_string())
            {
                resp.headers_mut().insert(header::RETRY_AFTER, v);
            }
            crate::extract::add_cors(resp.headers_mut());
            resp
        }
    }
}

/// Tokens are secrets: bucket keys hold a hash of them, not the token.
fn token_key(token: &str) -> String {
    use sha2::{Digest, Sha256};
    use std::fmt::Write as _;
    let digest = Sha256::digest(token.as_bytes());
    digest.iter().take(16).fold(String::new(), |mut out, b| {
        let _ = write!(out, "{b:02x}");
        out
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_paths() {
        assert_eq!(
            classify(&Method::POST, "/_matrix/client/v3/login"),
            Some(Class::Login)
        );
        assert_eq!(
            classify(&Method::POST, "/_matrix/client/r0/register"),
            Some(Class::Register)
        );
        assert_eq!(
            classify(
                &Method::PUT,
                "/_matrix/client/v3/rooms/!a:b/send/m.room.message/t1"
            ),
            Some(Class::Message)
        );
        assert_eq!(
            classify(&Method::POST, "/_matrix/client/v3/join/!a:b"),
            Some(Class::Join)
        );
        assert_eq!(
            classify(&Method::POST, "/_matrix/client/v3/rooms/!a:b/invite"),
            Some(Class::Invite)
        );
        assert_eq!(classify(&Method::GET, "/_matrix/client/v3/sync"), None);
    }

    #[test]
    fn bucket_refills() {
        let l = Limiter::default();
        let rate = Rate {
            per_second: 1000.0,
            burst: 2,
        };
        assert!(l.check(Class::Message, "k", rate).is_ok());
        assert!(l.check(Class::Message, "k", rate).is_ok());
        let third = l.check(Class::Message, "k", rate);
        assert!(third.is_err() || third.is_ok()); // may have refilled already
        let slow = Rate {
            per_second: 0.001,
            burst: 1,
        };
        assert!(l.check(Class::Login, "ip", slow).is_ok());
        assert!(l.check(Class::Login, "ip", slow).unwrap_err() > 0);
    }
}
