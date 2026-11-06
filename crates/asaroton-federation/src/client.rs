//! Signed outbound federation requests.

use std::{borrow::Cow, net::SocketAddr, sync::Arc, time::Duration};

use asaroton_service::Services;
use ruma::{
    OwnedServerName, ServerName,
    api::{
        IncomingResponseExt, MatrixVersion, OutgoingRequest, OutgoingRequestExt, SupportedVersions,
        auth_scheme::NoAuthentication,
        error::FromHttpResponseError,
        federation::{
            authentication::{ServerSignatures, XMatrix, XMatrixSigningInput},
            transactions::send_transaction_message,
        },
        path_builder::{PathBuilder, SinglePath, VersionHistory},
    },
};

use crate::{Error, FederationConfig, Result, discovery::Discovery};

/// Timeout for downloads.
const LONG_REQUEST_TIMEOUT: Duration = Duration::from_secs(120);
/// Timeout for ordinary federation requests. Joins and state fetches use a longer one.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const LONG_TIMEOUT: Duration = Duration::from_secs(180);
/// Timeout for `/send`. A busy server can take minutes over one transaction, and giving up
/// early only means it sees the retry while it is still working on the first attempt.
const TRANSACTION_TIMEOUT: Duration = Duration::from_secs(120);

/// What a destination answered to a transaction, whatever the status.
#[derive(Debug)]
pub struct TransactionReply {
    pub status: http::StatusCode,
    /// The `Retry-After` header, when it gives a number of seconds.
    pub retry_after: Option<Duration>,
    pub body: bytes::Bytes,
}

/// Read a response body, giving up as soon as it is known to be larger than `max`: first by
/// what the sender announces, then by what actually arrives. Nothing beyond the limit is
/// kept in memory.
pub(crate) async fn read_body(
    destination: &ServerName,
    mut resp: reqwest::Response,
    max: Option<usize>,
) -> Result<bytes::Bytes> {
    let transport = |e: reqwest::Error| Error::Transport(destination.to_owned(), error_chain(&e));
    let Some(max) = max else {
        return resp.bytes().await.map_err(transport);
    };
    let too_large = || Error::TooLarge(destination.to_owned(), max);
    if resp
        .content_length()
        .is_some_and(|len| usize::try_from(len).map_or(true, |len| len > max))
    {
        return Err(too_large());
    }
    let mut body = bytes::BytesMut::new();
    while let Some(chunk) = resp.chunk().await.map_err(transport)? {
        if body.len() + chunk.len() > max {
            return Err(too_large());
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body.freeze())
}

/// An error with its sources, as reqwest's top-level message hides the actual cause.
fn error_chain(e: &reqwest::Error) -> String {
    let mut parts = vec![e.to_string()];
    let mut source = std::error::Error::source(e);
    while let Some(s) = source {
        parts.push(s.to_string());
        source = s.source();
    }
    parts.join(": ")
}

/// DNS hook: honour discovery overrides, otherwise system DNS.
struct OverrideResolver {
    overrides: Arc<dashmap::DashMap<String, Vec<SocketAddr>>>,
    fallback: Arc<hickory_resolver::TokioResolver>,
}

impl reqwest::dns::Resolve for OverrideResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let overrides = Arc::clone(&self.overrides);
        let fallback = Arc::clone(&self.fallback);
        Box::pin(async move {
            let host = name.as_str().to_owned();
            if let Some(addrs) = overrides.get(&host) {
                let v = addrs.clone();
                return Ok(Box::new(v.into_iter()) as reqwest::dns::Addrs);
            }
            let ips = fallback
                .lookup_ip(host.as_str())
                .await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
            let addrs: Vec<SocketAddr> = ips.iter().map(|ip| SocketAddr::new(ip, 0)).collect();
            Ok(Box::new(addrs.into_iter()) as reqwest::dns::Addrs)
        })
    }
}

/// HTTP client that signs requests as this server.
#[derive(Clone)]
pub struct FederationClient {
    services: Arc<Services>,
    discovery: Discovery,
    http: reqwest::Client,
}

impl std::fmt::Debug for FederationClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("FederationClient")
    }
}

/// Path builders used by federation endpoints, and how to feed them.
pub trait FedPath: PathBuilder {
    fn input(versions: &SupportedVersions) -> Self::Input<'_>;
}

impl FedPath for SinglePath {
    fn input(_versions: &SupportedVersions) -> Self::Input<'_> {}
}

impl FedPath for VersionHistory {
    fn input(versions: &SupportedVersions) -> Self::Input<'_> {
        Cow::Borrowed(versions)
    }
}

/// Versions we assume peers speak (all paths are stable `v1`/`v2` federation paths anyway).
fn supported_versions() -> SupportedVersions {
    SupportedVersions {
        versions: [MatrixVersion::V1_1, MatrixVersion::V1_11]
            .into_iter()
            .collect(),
        features: std::collections::BTreeSet::default(),
    }
}

impl FederationClient {
    pub fn new(
        services: Arc<Services>,
        discovery: Discovery,
        config: &FederationConfig,
    ) -> Result<Self> {
        let resolver = hickory_resolver::TokioResolver::builder_tokio()
            .map_err(|e| Error::Protocol(format!("dns resolver: {e}")))?
            .build()
            .map_err(|e| Error::Protocol(format!("dns resolver: {e}")))?;
        // HTTP/1.1 only: with HTTP/2 the explicit `Host` header (the delegated name, possibly
        // with a port) next to `:authority` makes some servers (Synapse's own HTTP/2 stack)
        // reset the stream with a protocol error.
        let mut builder = reqwest::Client::builder()
            .user_agent(concat!("asaroton/", env!("CARGO_PKG_VERSION")))
            .http1_only()
            .connect_timeout(Duration::from_secs(10))
            .pool_idle_timeout(Duration::from_secs(90))
            .redirect(reqwest::redirect::Policy::none())
            .dns_resolver(Arc::new(OverrideResolver {
                overrides: Arc::clone(&discovery.overrides),
                fallback: Arc::new(resolver),
            }));
        for pem in &config.extra_ca_certs_pem {
            for cert in reqwest::Certificate::from_pem_bundle(pem)
                .map_err(|e| Error::Protocol(format!("ca cert: {e}")))?
            {
                builder = builder.add_root_certificate(cert);
            }
        }
        if config.danger_accept_invalid_certs {
            builder = builder.danger_accept_invalid_certs(true);
        }
        let http = builder
            .build()
            .map_err(|e| Error::Protocol(format!("http client: {e}")))?;
        Ok(Self {
            services,
            discovery,
            http,
        })
    }

    /// Send a ruma federation request to `destination` and parse the response.
    pub async fn send<R>(&self, destination: &ServerName, request: R) -> Result<R::IncomingResponse>
    where
        R: OutgoingRequest<Authentication = ServerSignatures> + std::fmt::Debug,
        R::PathBuilder: FedPath,
        R::EndpointError: std::fmt::Debug,
    {
        self.send_with_timeout(destination, request, REQUEST_TIMEOUT, None)
            .await
    }

    /// Send an unauthenticated federation request (server key discovery).
    pub async fn send_unauthenticated<R>(
        &self,
        destination: &ServerName,
        request: R,
    ) -> Result<R::IncomingResponse>
    where
        R: OutgoingRequest<Authentication = NoAuthentication> + std::fmt::Debug,
        R::PathBuilder: FedPath,
        R::EndpointError: std::fmt::Debug,
    {
        let resolved = self.discovery.resolve(destination).await?;
        let base = resolved.base_url();
        let versions = supported_versions();
        let http_req: http::Request<Vec<u8>> = request
            .try_into_http_request(&base, (), <R::PathBuilder as FedPath>::input(&versions))
            .map_err(|e| Error::Protocol(format!("building request: {e}")))?;
        self.dispatch::<R>(destination, &resolved, http_req, REQUEST_TIMEOUT, None)
            .await
    }

    /// Like [`Self::send`] with a longer timeout (joins, state fetches, backfill).
    pub async fn send_long<R>(
        &self,
        destination: &ServerName,
        request: R,
    ) -> Result<R::IncomingResponse>
    where
        R: OutgoingRequest<Authentication = ServerSignatures> + std::fmt::Debug,
        R::PathBuilder: FedPath,
        R::EndpointError: std::fmt::Debug,
    {
        self.send_with_timeout(destination, request, LONG_TIMEOUT, None)
            .await
    }

    /// Like [`Self::send_long`], refusing a response body of more than `max_body` bytes
    /// (media downloads: the size of what comes back is the remote server's choice).
    pub async fn send_bounded<R>(
        &self,
        destination: &ServerName,
        request: R,
        max_body: usize,
    ) -> Result<R::IncomingResponse>
    where
        R: OutgoingRequest<Authentication = ServerSignatures> + std::fmt::Debug,
        R::PathBuilder: FedPath,
        R::EndpointError: std::fmt::Debug,
    {
        self.send_with_timeout(destination, request, LONG_TIMEOUT, Some(max_body))
            .await
    }

    /// Send a transaction and hand back the answer as it came, so the sender can tell a
    /// refusal that will pass from one that will not. Only failing to get an answer at all is
    /// an error here.
    pub async fn send_transaction(
        &self,
        destination: &ServerName,
        request: send_transaction_message::v1::Request,
    ) -> Result<TransactionReply> {
        let resolved = self.discovery.resolve(destination).await?;
        let http_req = self.signed(destination, &resolved, request)?;
        let (status, headers, body) = self
            .exchange(destination, &resolved, http_req, TRANSACTION_TIMEOUT, None)
            .await?;
        let retry_after = headers
            .get(http::header::RETRY_AFTER)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.trim().parse::<u64>().ok())
            .map(Duration::from_secs);
        Ok(TransactionReply {
            status,
            retry_after,
            body,
        })
    }

    async fn send_with_timeout<R>(
        &self,
        destination: &ServerName,
        request: R,
        timeout: Duration,
        max_body: Option<usize>,
    ) -> Result<R::IncomingResponse>
    where
        R: OutgoingRequest<Authentication = ServerSignatures> + std::fmt::Debug,
        R::PathBuilder: FedPath,
        R::EndpointError: std::fmt::Debug,
    {
        let resolved = self.discovery.resolve(destination).await?;
        let http_req = self.signed(destination, &resolved, request)?;
        self.dispatch::<R>(destination, &resolved, http_req, timeout, max_body)
            .await
    }

    /// Turn a ruma request into an HTTP request signed as this server.
    fn signed<R>(
        &self,
        destination: &ServerName,
        resolved: &crate::discovery::Resolved,
        request: R,
    ) -> Result<http::Request<Vec<u8>>>
    where
        R: OutgoingRequest<Authentication = ServerSignatures>,
        R::PathBuilder: FedPath,
    {
        let signing = XMatrixSigningInput::new(
            self.services.config.server_name.clone(),
            destination.to_owned(),
            self.services.signing_key.key_pair(),
        );
        let versions = supported_versions();
        request
            .try_into_http_request(
                &resolved.base_url(),
                signing,
                <R::PathBuilder as FedPath>::input(&versions),
            )
            .map_err(|e| Error::Protocol(format!("building request: {e}")))
    }

    async fn dispatch<R>(
        &self,
        destination: &ServerName,
        resolved: &crate::discovery::Resolved,
        http_req: http::Request<Vec<u8>>,
        timeout: Duration,
        max_body: Option<usize>,
    ) -> Result<R::IncomingResponse>
    where
        R: OutgoingRequest,
        R::EndpointError: std::fmt::Debug,
    {
        let (status, headers, bytes) = self
            .exchange(destination, resolved, http_req, timeout, max_body)
            .await?;
        if !status.is_success() {
            return Err(Error::RemoteError {
                server: destination.to_owned(),
                status,
                body: String::from_utf8_lossy(&bytes).into_owned(),
            });
        }
        let mut builder = http::Response::builder().status(status);
        for (k, v) in &headers {
            builder = builder.header(k, v);
        }
        let http_resp = builder
            .body(bytes.as_ref())
            .map_err(|e| Error::Protocol(e.to_string()))?;
        R::IncomingResponse::try_from_http_response(http_resp).map_err(|e| match e {
            FromHttpResponseError::Deserialization(d) => {
                Error::Protocol(format!("{destination}: bad response: {d}"))
            }
            FromHttpResponseError::Server(s) => Error::Protocol(format!("{destination}: {s:?}")),
            other => Error::Protocol(format!("{destination}: {other}")),
        })
    }

    /// Perform the HTTP exchange for a prepared request; any status is an answer.
    async fn exchange(
        &self,
        destination: &ServerName,
        resolved: &crate::discovery::Resolved,
        http_req: http::Request<Vec<u8>>,
        timeout: Duration,
        max_body: Option<usize>,
    ) -> Result<(http::StatusCode, http::HeaderMap, bytes::Bytes)> {
        let (parts, body) = http_req.into_parts();

        let mut req = self
            .http
            .request(parts.method.clone(), parts.uri.to_string())
            .timeout(timeout)
            .body(body);
        for (k, v) in &parts.headers {
            req = req.header(k, v);
        }
        req = req.header(http::header::HOST, &resolved.host_header);
        let resp = match req.send().await {
            Ok(r) => r,
            Err(e) => {
                self.discovery.invalidate(destination);
                return Err(Error::Transport(destination.to_owned(), error_chain(&e)));
            }
        };
        let status = resp.status();
        let headers = resp.headers().clone();
        let bytes = read_body(destination, resp, max_body).await?;
        Ok((status, headers, bytes))
    }

    /// A signed request with a JSON body to an endpoint ruma does not model, returning the
    /// JSON response. Matrix errors from the remote come back as [`Error::RemoteError`].
    pub async fn send_signed_json(
        &self,
        destination: &ServerName,
        method: http::Method,
        path: &str,
        body: Option<&serde_json::Value>,
    ) -> Result<serde_json::Value> {
        let resolved = self.discovery.resolve(destination).await?;
        let url = format!("{}{path}", resolved.base_url());
        let bytes = body
            .map(serde_json::to_vec)
            .transpose()?
            .unwrap_or_default();
        let mut builder = http::Request::builder().method(method).uri(&url);
        if body.is_some() {
            builder = builder.header(http::header::CONTENT_TYPE, "application/json");
        }
        let request = builder
            .body(bytes)
            .map_err(|e| Error::Protocol(format!("building request: {e}")))?;
        let signing = XMatrixSigningInput::new(
            self.services.config.server_name.clone(),
            destination.to_owned(),
            self.services.signing_key.key_pair(),
        );
        let auth = XMatrix::sign_http_request(&request, signing)
            .map_err(|e| Error::Protocol(format!("signing request: {e}")))?;
        let (parts, body) = request.into_parts();
        let mut req = self
            .http
            .request(parts.method, url)
            .timeout(REQUEST_TIMEOUT)
            .header(http::header::AUTHORIZATION, http::HeaderValue::from(&auth))
            .header(http::header::HOST, &resolved.host_header)
            .body(body);
        for (k, v) in &parts.headers {
            req = req.header(k, v);
        }
        let resp = match req.send().await {
            Ok(r) => r,
            Err(e) => {
                self.discovery.invalidate(destination);
                return Err(Error::Transport(destination.to_owned(), error_chain(&e)));
            }
        };
        let status = resp.status();
        let bytes = resp
            .bytes()
            .await
            .map_err(|e| Error::Transport(destination.to_owned(), error_chain(&e)))?;
        if !status.is_success() {
            return Err(Error::RemoteError {
                server: destination.to_owned(),
                status,
                body: String::from_utf8_lossy(&bytes).into_owned(),
            });
        }
        serde_json::from_slice(&bytes)
            .map_err(|e| Error::Protocol(format!("{destination}: bad JSON: {e}")))
    }

    /// Unsigned `GET` returning JSON (server keys).
    pub async fn get_json(
        &self,
        destination: &ServerName,
        path: &str,
    ) -> Result<serde_json::Value> {
        let resolved = self.discovery.resolve(destination).await?;
        let url = format!("{}{path}", resolved.base_url());
        let resp = self
            .http
            .get(&url)
            .header(http::header::HOST, &resolved.host_header)
            .timeout(REQUEST_TIMEOUT)
            .send()
            .await
            .map_err(|e| {
                self.discovery.invalidate(destination);
                Error::Transport(destination.to_owned(), error_chain(&e))
            })?;
        let status = resp.status();
        let bytes = resp
            .bytes()
            .await
            .map_err(|e| Error::Transport(destination.to_owned(), error_chain(&e)))?;
        if !status.is_success() {
            return Err(Error::RemoteError {
                server: destination.to_owned(),
                status,
                body: String::from_utf8_lossy(&bytes).into_owned(),
            });
        }
        serde_json::from_slice(&bytes)
            .map_err(|e| Error::Protocol(format!("{destination}: bad JSON: {e}")))
    }

    /// Unauthenticated GET of a binary resource: bytes, content type and disposition.
    pub async fn get_bytes(
        &self,
        destination: &ServerName,
        path: &str,
        max_body: usize,
    ) -> Result<(Vec<u8>, String, Option<String>)> {
        let resolved = self.discovery.resolve(destination).await?;
        let url = format!("{}{path}", resolved.base_url());
        let resp = self
            .http
            .get(&url)
            .header(http::header::HOST, &resolved.host_header)
            .timeout(LONG_REQUEST_TIMEOUT)
            .send()
            .await
            .map_err(|e| {
                self.discovery.invalidate(destination);
                Error::Transport(destination.to_owned(), error_chain(&e))
            })?;
        let status = resp.status();
        let content_type = resp
            .headers()
            .get(http::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("application/octet-stream")
            .to_owned();
        let disposition = resp
            .headers()
            .get(http::header::CONTENT_DISPOSITION)
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        let bytes = read_body(destination, resp, Some(max_body)).await?;
        if !status.is_success() {
            return Err(Error::RemoteError {
                server: destination.to_owned(),
                status,
                body: String::from_utf8_lossy(&bytes).into_owned(),
            });
        }
        Ok((bytes.to_vec(), content_type, disposition))
    }

    /// Fetch media from the URL a server redirected us to (typically its CDN): a plain `GET`
    /// without our signature, bounded in time and size. Returns the bytes and content type.
    pub async fn get_redirected(
        &self,
        origin: &ServerName,
        url: &str,
        max_body: usize,
    ) -> Result<(Vec<u8>, String)> {
        let parsed = reqwest::Url::parse(url)
            .ok()
            .filter(|u| matches!(u.scheme(), "http" | "https"))
            .ok_or_else(|| Error::Protocol(format!("{origin}: unusable media location")))?;
        let resp = self
            .http
            .get(parsed)
            .timeout(LONG_REQUEST_TIMEOUT)
            .send()
            .await
            .map_err(|e| Error::Transport(origin.to_owned(), error_chain(&e)))?;
        let status = resp.status();
        let content_type = resp
            .headers()
            .get(http::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("application/octet-stream")
            .to_owned();
        let bytes = read_body(origin, resp, Some(max_body)).await?;
        if !status.is_success() {
            return Err(Error::Protocol(format!(
                "{origin}: media location answered {status}"
            )));
        }
        Ok((bytes.to_vec(), content_type))
    }

    /// Verify an incoming request's `X-Matrix` signature. Returns the origin server.
    pub async fn verify_inbound(
        &self,
        fed: &crate::Federation,
        request: &http::Request<&[u8]>,
    ) -> Result<OwnedServerName> {
        let xm = XMatrix::extract_from_http_headers(request.headers())
            .map_err(|e| Error::Unauthorized(format!("invalid X-Matrix header: {e}")))?;
        if let Some(dest) = &xm.destination
            && *dest != self.services.config.server_name
        {
            return Err(Error::Unauthorized(
                "X-Matrix destination is not this server".into(),
            ));
        }
        let keys = fed
            .public_key_map_for(
                &xm.origin,
                std::slice::from_ref(&xm.key),
                asaroton_store::now_ts(),
            )
            .await?;
        xm.verify_http_request(request, &self.services.config.server_name, &keys)
            .map_err(|e| Error::Unauthorized(format!("bad request signature: {e}")))?;
        Ok(xm.origin)
    }
}
