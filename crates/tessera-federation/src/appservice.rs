//! Talking to application services: queries, pings and the transaction stream of events.

use std::{collections::HashMap, sync::Arc, time::Duration};

use ruma::{RoomAliasId, UserId};
use serde_json::{Value, json};
use tessera_service::{Error as ServiceError, Result as ServiceResult, Services};

use crate::{Federation, url_preview::plain_http};

/// Events per transaction.
const BATCH: usize = 100;
const MIN_RETRY: Duration = Duration::from_secs(1);
const MAX_RETRY: Duration = Duration::from_secs(300);

fn bad_status(status: reqwest::StatusCode, body: Option<String>) -> ServiceError {
    let mut data = ruma::api::error::BadStatusErrorData::new();
    data.status = http::StatusCode::from_u16(status.as_u16()).ok();
    data.body = body;
    ServiceError::matrix(
        ruma::api::error::ErrorKind::BadStatus(data),
        http::StatusCode::BAD_GATEWAY,
        format!("Appservice responded with {status}"),
    )
}

impl Federation {
    fn appservice_base(
        &self,
        id: &str,
    ) -> ServiceResult<(Arc<tessera_service::appservice::Appservice>, String)> {
        let a = self
            .services
            .appservices
            .by_id(id)
            .ok_or_else(|| ServiceError::not_found("Unknown appservice."))?;
        let url = a.url.clone().ok_or_else(|| {
            ServiceError::matrix(
                ruma::api::error::ErrorKind::UrlNotSet,
                http::StatusCode::BAD_REQUEST,
                "Appservice has no URL.",
            )
        })?;
        Ok((a, url.trim_end_matches('/').to_owned()))
    }

    /// `GET {url}/_matrix/app/v1/users/{userId}`: 200 means the appservice knows (and has
    /// registered) the user.
    pub async fn appservice_query_user(&self, id: &str, user: &UserId) -> ServiceResult<bool> {
        let (a, base) = self.appservice_base(id)?;
        let client = plain_http();
        for path in [
            format!("{base}/_matrix/app/v1/users/{}", urlencode(user.as_str())),
            format!("{base}/users/{}", urlencode(user.as_str())),
        ] {
            let resp = client.get(&path).bearer_auth(&a.hs_token).send().await;
            match resp {
                Ok(r) if r.status().is_success() => return Ok(true),
                Ok(r) if r.status() == reqwest::StatusCode::NOT_FOUND => {}
                Ok(r) => return Err(bad_status(r.status(), None)),
                Err(e) => tracing::debug!(appservice = id, error = %e, "user query failed"),
            }
        }
        Ok(false)
    }

    /// `GET {url}/_matrix/app/v1/rooms/{alias}`.
    pub async fn appservice_query_alias(
        &self,
        id: &str,
        alias: &RoomAliasId,
    ) -> ServiceResult<bool> {
        let (a, base) = self.appservice_base(id)?;
        let client = plain_http();
        for path in [
            format!("{base}/_matrix/app/v1/rooms/{}", urlencode(alias.as_str())),
            format!("{base}/rooms/{}", urlencode(alias.as_str())),
        ] {
            let resp = client.get(&path).bearer_auth(&a.hs_token).send().await;
            match resp {
                Ok(r) if r.status().is_success() => return Ok(true),
                Ok(r) if r.status() == reqwest::StatusCode::NOT_FOUND => {}
                Ok(r) => return Err(bad_status(r.status(), None)),
                Err(e) => tracing::debug!(appservice = id, error = %e, "alias query failed"),
            }
        }
        Ok(false)
    }

    /// `GET {url}/_matrix/app/v1/thirdparty/{path}?{query}`.
    pub async fn appservice_thirdparty(
        &self,
        id: &str,
        path: &str,
        query: &[(String, String)],
    ) -> ServiceResult<Option<Value>> {
        let (a, base) = self.appservice_base(id)?;
        let client = plain_http();
        let qs: Vec<String> = query
            .iter()
            .map(|(k, v)| format!("{}={}", urlencode(k), urlencode(v)))
            .collect();
        let qs = if qs.is_empty() {
            String::new()
        } else {
            format!("?{}", qs.join("&"))
        };
        for url in [
            format!("{base}/_matrix/app/v1/thirdparty/{path}{qs}"),
            format!("{base}/_matrix/app/unstable/thirdparty/{path}{qs}"),
        ] {
            let resp = client.get(&url).bearer_auth(&a.hs_token).send().await;
            match resp {
                Ok(r) if r.status().is_success() => {
                    return Ok(r.json::<Value>().await.ok());
                }
                Ok(r) if r.status() == reqwest::StatusCode::NOT_FOUND => {}
                Ok(r) => return Err(bad_status(r.status(), None)),
                Err(e) => {
                    tracing::debug!(appservice = id, error = %e, "third-party lookup failed");
                }
            }
        }
        Ok(None)
    }

    /// `POST {url}/_matrix/app/v1/ping`.
    pub async fn appservice_ping(&self, id: &str, txn_id: &str) -> ServiceResult<u64> {
        let (a, base) = self.appservice_base(id)?;
        let started = std::time::Instant::now();
        let resp = plain_http()
            .post(format!("{base}/_matrix/app/v1/ping"))
            .bearer_auth(&a.hs_token)
            .json(&json!({ "transaction_id": txn_id }))
            .send()
            .await;
        match resp {
            Ok(r) if r.status().is_success() => {
                Ok(u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX))
            }
            Ok(r) => {
                let status = r.status();
                let body = r.text().await.ok();
                Err(bad_status(status, body))
            }
            Err(e) if e.is_timeout() => Err(ServiceError::matrix(
                ruma::api::error::ErrorKind::ConnectionTimeout,
                http::StatusCode::GATEWAY_TIMEOUT,
                "Appservice did not respond in time.",
            )),
            Err(e) => Err(ServiceError::matrix(
                ruma::api::error::ErrorKind::ConnectionFailed,
                http::StatusCode::BAD_GATEWAY,
                format!("Could not reach the appservice: {e}"),
            )),
        }
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

/// Start one transaction worker per appservice with a URL.
pub fn start(fed: &Arc<Federation>) {
    for a in fed.services.appservices.all() {
        if a.url.is_none() {
            continue;
        }
        let fed = Arc::clone(fed);
        let a = Arc::clone(a);
        tokio::spawn(async move { worker(fed, a).await });
    }
}

async fn worker(fed: Arc<Federation>, a: Arc<tessera_service::appservice::Appservice>) {
    let services: &Services = &fed.services;
    let mut rx = services.notifier.subscribe();
    let mut retry = MIN_RETRY;
    loop {
        match push_pending(services, &a).await {
            Ok(true) => {
                // More may be waiting: go straight round again.
                retry = MIN_RETRY;
            }
            Ok(false) => {
                retry = MIN_RETRY;
                // Wait for a new event or a periodic tick.
                tokio::select! {
                    () = tokio::time::sleep(Duration::from_secs(30)) => {}
                    _ = rx.recv() => {}
                }
            }
            Err(e) => {
                tracing::warn!(appservice = %a.id, error = %e, retry_in = ?retry, "appservice transaction failed");
                tokio::time::sleep(retry).await;
                retry = retry.saturating_mul(2).min(MAX_RETRY);
            }
        }
    }
}

/// Send the next batch of interesting events; `Ok(true)` if there may be more.
async fn push_pending(
    services: &Services,
    a: &tessera_service::appservice::Appservice,
) -> ServiceResult<bool> {
    let position = services.store.appservice_position(&a.id).await?;
    let events = services.store.events_after(position, BATCH).await?;
    let Some(last) = events.last() else {
        return Ok(false);
    };
    let last_pos = last.stream_ordering;
    let mut interest: HashMap<ruma::OwnedRoomId, bool> = HashMap::new();
    let mut payload: Vec<Value> = Vec::new();
    for ev in &events {
        let room = ev.pdu.room_id().to_owned();
        let interested = match interest.get(&room) {
            Some(i) => *i,
            None => {
                let i = services.appservice_interested_in_room(a, &room).await?;
                interest.insert(room, i);
                i
            }
        };
        if interested {
            payload.push(services.client_event_plain(ev));
        }
    }
    if !payload.is_empty() {
        let base = a.url.as_deref().unwrap_or("").trim_end_matches('/');
        let client = plain_http();
        let mut ok = false;
        for path in [
            format!("{base}/_matrix/app/v1/transactions/{last_pos}"),
            format!("{base}/transactions/{last_pos}"),
        ] {
            let resp = client
                .put(&path)
                .bearer_auth(&a.hs_token)
                .json(&json!({ "events": payload }))
                .send()
                .await
                .map_err(|e| ServiceError::internal(format!("appservice request: {e}")))?;
            if resp.status().is_success() {
                ok = true;
                break;
            }
            if resp.status() != reqwest::StatusCode::NOT_FOUND {
                return Err(ServiceError::internal(format!(
                    "appservice answered {}",
                    resp.status()
                )));
            }
        }
        if !ok {
            return Err(ServiceError::internal(
                "appservice does not accept transactions",
            ));
        }
        tracing::debug!(appservice = %a.id, events = payload.len(), txn = last_pos, "appservice transaction sent");
    }
    services
        .store
        .set_appservice_position(&a.id, last_pos)
        .await?;
    Ok(events.len() == BATCH)
}
