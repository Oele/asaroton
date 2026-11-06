//! Server discovery (spec: server-server API, "Resolving server names").
//!
//! Outcome of resolving `server_name`: the host/port to connect to, the `Host` header to send
//! and the name the TLS certificate must be valid for.

use std::{
    net::{IpAddr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use asaroton_service::Services;
use asaroton_store::{DiscoveryEntry, now_ts};
use dashmap::DashMap;
use hickory_resolver::{TokioResolver, proto::rr::RecordType};
use ruma::{OwnedServerName, ServerName};

use crate::{Error, FederationConfig, Result};

/// Where to send requests for a server name.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Resolved {
    /// Host to connect to (IP literal or DNS name). The target of an SRV record, where the
    /// server has one; otherwise the same as `tls_name`.
    pub host: String,
    pub port: u16,
    /// Value of the `Host` header.
    pub host_header: String,
    /// Name the certificate must match, and the name asked for in the TLS handshake (SNI):
    /// the server name, or the name it delegates to. Never the target of an SRV record, which
    /// is unauthenticated DNS data: a certificate for the target proves nothing about the
    /// server.
    pub tls_name: String,
    /// Plain HTTP (test-only servers).
    pub insecure: bool,
}

impl Resolved {
    /// The URL that requests go to. It names `tls_name`, because the HTTP client takes both
    /// the name for the TLS handshake and the name to check the certificate against from the
    /// URL. Where the host to connect to is another one, the client's DNS hook is told its
    /// addresses under this name (`Discovery::overrides`), and the port here is the port
    /// there.
    pub fn base_url(&self) -> String {
        let scheme = if self.insecure { "http" } else { "https" };
        let host = if self.tls_name.contains(':') && !self.tls_name.starts_with('[') {
            format!("[{}]", self.tls_name)
        } else {
            self.tls_name.clone()
        };
        format!("{scheme}://{host}:{}", self.port)
    }
}

/// Resolver with a cache.
#[derive(Clone)]
pub struct Discovery {
    services: Arc<Services>,
    resolver: Arc<TokioResolver>,
    http: reqwest::Client,
    cache: Arc<DashMap<OwnedServerName, (Resolved, i64)>>,
    insecure: Vec<OwnedServerName>,
    /// Host name (lower case) -> addresses, consulted by the federation HTTP client's DNS
    /// hook so that SRV targets are honoured while SNI/Host stay on the delegated name.
    pub(crate) overrides: Arc<DashMap<String, Vec<SocketAddr>>>,
}

impl std::fmt::Debug for Discovery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Discovery")
    }
}

const WELL_KNOWN_TTL: Duration = Duration::from_hours(24);
const FAILURE_TTL: Duration = Duration::from_hours(1);

impl Discovery {
    pub fn new(services: Arc<Services>, config: &FederationConfig) -> Result<Self> {
        let resolver = TokioResolver::builder_tokio()
            .map_err(|e| Error::Protocol(format!("dns resolver: {e}")))?
            .build()
            .map_err(|e| Error::Protocol(format!("dns resolver: {e}")))?;
        let mut builder = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .redirect(reqwest::redirect::Policy::limited(5))
            .user_agent(concat!("asaroton/", env!("CARGO_PKG_VERSION")));
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
            resolver: Arc::new(resolver),
            http,
            cache: Arc::new(DashMap::new()),
            insecure: config.insecure_http_servers.clone(),
            overrides: Arc::new(DashMap::new()),
        })
    }

    /// Resolve, using the in-memory and database caches.
    pub async fn resolve(&self, server_name: &ServerName) -> Result<Resolved> {
        let now = now_ts();
        if let Some(entry) = self.cache.get(server_name)
            && entry.1 > now
        {
            return Ok(entry.0.clone());
        }
        if let Some(db) = self.services.store.get_discovery(server_name).await?
            && db.expires_ts > now
        {
            let r = Resolved {
                host: db.host,
                port: db.port,
                host_header: db.host_header,
                tls_name: db.tls_name,
                insecure: self.insecure.iter().any(|s| s == server_name),
            };
            self.install_override(server_name, &r).await?;
            self.cache
                .insert(server_name.to_owned(), (r.clone(), db.expires_ts));
            return Ok(r);
        }
        let (resolved, ttl) = self.resolve_uncached(server_name).await?;
        self.install_override(server_name, &resolved).await?;
        let expires = now + i64::try_from(ttl.as_millis()).unwrap_or(i64::MAX);
        self.cache
            .insert(server_name.to_owned(), (resolved.clone(), expires));
        let _ = self
            .services
            .store
            .set_discovery(&DiscoveryEntry {
                server_name: server_name.to_owned(),
                host: resolved.host.clone(),
                port: resolved.port,
                host_header: resolved.host_header.clone(),
                tls_name: resolved.tls_name.clone(),
                expires_ts: expires,
            })
            .await;
        Ok(resolved)
    }

    /// Drop a cached result (after connection failures).
    pub fn invalidate(&self, server_name: &ServerName) {
        self.cache.remove(server_name);
    }

    /// When the host to connect to differs from the name in the URL (it is the target of an
    /// SRV record), tell the HTTP client's DNS hook where to find that name: at the target's
    /// addresses. Without that the client would look the name up itself and go wherever its
    /// own address records point, so a target that has no address is an error.
    ///
    /// The hook knows names only, not ports. A name that is reached both directly on a port
    /// of its own and through an SRV record would get the one entry for both; nobody is known
    /// to run a server that way.
    async fn install_override(&self, server_name: &ServerName, r: &Resolved) -> Result<()> {
        let name = r.tls_name.to_ascii_lowercase();
        if r.host == r.tls_name {
            self.overrides.remove(&name);
            return Ok(());
        }
        let addrs: Vec<SocketAddr> = match r.host.parse::<IpAddr>() {
            Ok(ip) => vec![SocketAddr::new(ip, r.port)],
            Err(_) => match self.resolver.lookup_ip(r.host.as_str()).await {
                Ok(ips) => ips.iter().map(|ip| SocketAddr::new(ip, r.port)).collect(),
                Err(_) => Vec::new(),
            },
        };
        if addrs.is_empty() {
            self.overrides.remove(&name);
            return Err(Error::Discovery(
                server_name.to_owned(),
                format!("no address for {}, the target of its SRV record", r.host),
            ));
        }
        self.overrides.insert(name, addrs);
        Ok(())
    }

    async fn resolve_uncached(&self, server_name: &ServerName) -> Result<(Resolved, Duration)> {
        let insecure = self.insecure.iter().any(|s| s == server_name);
        let host = server_name.host();
        let explicit_port = server_name.port();

        // 1. IP literal.
        if let Ok(ip) = host
            .trim_start_matches('[')
            .trim_end_matches(']')
            .parse::<IpAddr>()
        {
            let port = explicit_port.unwrap_or(8448);
            return Ok((
                Resolved {
                    host: ip.to_string(),
                    port,
                    host_header: server_name.to_string(),
                    tls_name: ip.to_string(),
                    insecure,
                },
                WELL_KNOWN_TTL,
            ));
        }
        // 2. Explicit port: connect directly.
        if let Some(port) = explicit_port {
            return Ok((
                Resolved {
                    host: host.to_owned(),
                    port,
                    host_header: server_name.to_string(),
                    tls_name: host.to_owned(),
                    insecure,
                },
                WELL_KNOWN_TTL,
            ));
        }
        // 3. .well-known/matrix/server
        match self.well_known(server_name, insecure).await {
            Ok(Some((delegated, ttl))) => {
                let d_host = delegated.host();
                if let Ok(ip) = d_host
                    .trim_start_matches('[')
                    .trim_end_matches(']')
                    .parse::<IpAddr>()
                {
                    let port = delegated.port().unwrap_or(8448);
                    return Ok((
                        Resolved {
                            host: ip.to_string(),
                            port,
                            host_header: delegated.to_string(),
                            tls_name: ip.to_string(),
                            insecure,
                        },
                        ttl,
                    ));
                }
                if let Some(port) = delegated.port() {
                    return Ok((
                        Resolved {
                            host: d_host.to_owned(),
                            port,
                            host_header: delegated.to_string(),
                            tls_name: d_host.to_owned(),
                            insecure,
                        },
                        ttl,
                    ));
                }
                if let Some((target, port)) = self.srv(d_host).await {
                    return Ok((
                        Resolved {
                            host: target,
                            port,
                            host_header: d_host.to_owned(),
                            tls_name: d_host.to_owned(),
                            insecure,
                        },
                        ttl,
                    ));
                }
                return Ok((
                    Resolved {
                        host: d_host.to_owned(),
                        port: 8448,
                        host_header: d_host.to_owned(),
                        tls_name: d_host.to_owned(),
                        insecure,
                    },
                    ttl,
                ));
            }
            Ok(None) => {}
            Err(e) => {
                tracing::debug!(server = %server_name, error = %e, "well-known lookup failed");
            }
        }
        // 4. SRV records.
        if let Some((target, port)) = self.srv(host).await {
            return Ok((
                Resolved {
                    host: target,
                    port,
                    host_header: host.to_owned(),
                    tls_name: host.to_owned(),
                    insecure,
                },
                FAILURE_TTL,
            ));
        }
        // 5. Fallback: host:8448.
        Ok((
            Resolved {
                host: host.to_owned(),
                port: 8448,
                host_header: host.to_owned(),
                tls_name: host.to_owned(),
                insecure,
            },
            FAILURE_TTL,
        ))
    }

    /// `GET https://<server>/.well-known/matrix/server`.
    async fn well_known(
        &self,
        server_name: &ServerName,
        insecure: bool,
    ) -> Result<Option<(OwnedServerName, Duration)>> {
        let scheme = if insecure { "http" } else { "https" };
        let url = format!("{scheme}://{server_name}/.well-known/matrix/server");
        let resp = match self.http.get(&url).send().await {
            Ok(r) => r,
            Err(e) => return Err(Error::Discovery(server_name.to_owned(), e.to_string())),
        };
        if !resp.status().is_success() {
            return Ok(None);
        }
        let ttl = resp
            .headers()
            .get(http::header::CACHE_CONTROL)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| {
                v.split(',').find_map(|d| {
                    d.trim()
                        .strip_prefix("max-age=")
                        .and_then(|n| n.parse::<u64>().ok())
                })
            })
            .map_or(WELL_KNOWN_TTL, |s| {
                Duration::from_secs(s.clamp(3600, 48 * 3600))
            });
        let body: serde_json::Value = match resp.json().await {
            Ok(b) => b,
            Err(_) => return Ok(None),
        };
        let Some(delegated) = body.get("m.server").and_then(|v| v.as_str()) else {
            return Ok(None);
        };
        match OwnedServerName::try_from(delegated) {
            Ok(d) => Ok(Some((d, ttl))),
            Err(_) => Ok(None),
        }
    }

    /// `_matrix-fed._tcp` then legacy `_matrix._tcp` SRV lookups; picks the best record.
    async fn srv(&self, host: &str) -> Option<(String, u16)> {
        for prefix in ["_matrix-fed._tcp.", "_matrix._tcp."] {
            let name = format!("{prefix}{host}.");
            let Ok(lookup) = self.resolver.lookup(name.as_str(), RecordType::SRV).await else {
                continue;
            };
            let mut records: Vec<(u16, u16, String, u16)> = lookup
                .answers()
                .iter()
                .filter_map(|r| match &r.data {
                    hickory_resolver::proto::rr::RData::SRV(s) => {
                        Some((s.priority, s.weight, s.target.to_utf8(), s.port))
                    }
                    _ => None,
                })
                .collect();
            if records.is_empty() {
                continue;
            }
            records.sort_by(|a, b| a.0.cmp(&b.0).then(b.1.cmp(&a.1)));
            let (_, _, target, port) = records.remove(0);
            return Some((target.trim_end_matches('.').to_owned(), port));
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resolved(host: &str, tls_name: &str, port: u16) -> Resolved {
        Resolved {
            host: host.to_owned(),
            port,
            host_header: tls_name.to_owned(),
            tls_name: tls_name.to_owned(),
            insecure: false,
        }
    }

    #[test]
    fn the_url_names_the_host_the_certificate_is_for() {
        // A server behind an SRV record: connect to the target, ask for the delegated name.
        assert_eq!(
            resolved("meow.host.example", "federation.example", 443).base_url(),
            "https://federation.example:443"
        );
        assert_eq!(
            resolved("matrix.example", "matrix.example", 8448).base_url(),
            "https://matrix.example:8448"
        );
        assert_eq!(
            resolved("2001:db8::1", "2001:db8::1", 8448).base_url(),
            "https://[2001:db8::1]:8448"
        );
        let lab = Resolved {
            insecure: true,
            ..resolved("127.0.0.1", "127.0.0.1", 18448)
        };
        assert_eq!(lab.base_url(), "http://127.0.0.1:18448");
    }
}
