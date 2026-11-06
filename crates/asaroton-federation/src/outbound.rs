//! Fetching URLs that somebody else chose: URL previews, push gateways, and media a remote
//! server redirects us to.
//!
//! Such a URL may only lead to the public internet. Without that rule a user (or a page
//! they preview, or another homeserver) could make this server talk to services that listen
//! on its own host or network, and read their answers back. The check sits where the address
//! is known: literal addresses are checked in the URL, host names when they are resolved,
//! and every redirect is checked like the first request. An operator can exempt ranges, for
//! a push gateway on the local network for example.
//!
//! Requests to other homeservers' federation endpoints do not come through here.

use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use crate::{Error, Result};

const MAX_REDIRECTS: usize = 5;
const TIMEOUT: Duration = Duration::from_secs(15);

/// An address range in CIDR notation (`10.0.0.0/8`, `fd00::/8`), or a single address.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IpRange {
    network: IpAddr,
    prefix: u32,
}

impl FromStr for IpRange {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let (address, prefix) = match s.split_once('/') {
            Some((a, p)) => (a, Some(p)),
            None => (s, None),
        };
        let network: IpAddr = address
            .trim()
            .parse()
            .map_err(|_| format!("{s}: not an IP address or range"))?;
        let bits = if network.is_ipv4() { 32 } else { 128 };
        let prefix = match prefix {
            Some(p) => p
                .trim()
                .parse::<u32>()
                .ok()
                .filter(|p| *p <= bits)
                .ok_or_else(|| format!("{s}: the prefix must be a number up to {bits}"))?,
            None => bits,
        };
        Ok(Self { network, prefix })
    }
}

impl IpRange {
    fn contains(&self, ip: IpAddr) -> bool {
        fn masked(value: u128, bits: u32, prefix: u32) -> u128 {
            if prefix == 0 {
                0
            } else {
                value >> (bits - prefix)
            }
        }
        match (self.network, plain(ip)) {
            (IpAddr::V4(net), IpAddr::V4(ip)) => {
                masked(u128::from(u32::from(net)), 32, self.prefix)
                    == masked(u128::from(u32::from(ip)), 32, self.prefix)
            }
            (IpAddr::V6(net), IpAddr::V6(ip)) => {
                masked(u128::from(net), 128, self.prefix)
                    == masked(u128::from(ip), 128, self.prefix)
            }
            _ => false,
        }
    }
}

/// An IPv4 address written as IPv6 (`::ffff:10.0.0.1`) is that IPv4 address.
fn plain(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(v6) => v6.to_ipv4_mapped().map_or(ip, IpAddr::V4),
        IpAddr::V4(_) => ip,
    }
}

/// Whether an address is on the public internet.
fn is_public(ip: IpAddr) -> bool {
    match plain(ip) {
        IpAddr::V4(v4) => public_v4(v4),
        IpAddr::V6(v6) => public_v6(v6),
    }
}

fn public_v4(ip: Ipv4Addr) -> bool {
    let [a, b, c, _] = ip.octets();
    let internal = a == 0                                   // "this network"
        || a == 10                                          // private
        || (a == 100 && (64..128).contains(&b))             // carrier-grade NAT
        || a == 127                                         // loopback
        || (a == 169 && b == 254)                           // link-local, cloud metadata
        || (a == 172 && (16..32).contains(&b))              // private
        || (a == 192 && b == 0 && (c == 0 || c == 2))       // protocol assignments, documentation
        || (a == 192 && b == 168)                           // private
        || (a == 198 && (18..20).contains(&b))              // benchmarking
        || (a == 198 && b == 51 && c == 100)                // documentation
        || (a == 203 && b == 0 && c == 113)                 // documentation
        || a >= 224; // multicast, reserved, broadcast
    !internal
}

fn public_v6(ip: Ipv6Addr) -> bool {
    let s = ip.segments();
    let embedded = |hi: u16, lo: u16| Ipv4Addr::from((u32::from(hi) << 16) | u32::from(lo));
    // Forms that carry an IPv4 address are as public as that address.
    if s[..6].iter().all(|x| *x == 0) && !ip.is_unspecified() && !ip.is_loopback() {
        return public_v4(embedded(s[6], s[7])); // IPv4-compatible (deprecated)
    }
    if s[0] == 0x0064 && s[1] == 0xff9b && s[2..6].iter().all(|x| *x == 0) {
        return public_v4(embedded(s[6], s[7])); // NAT64
    }
    if s[0] == 0x2002 {
        return public_v4(embedded(s[1], s[2])); // 6to4
    }
    let internal = ip.is_unspecified()
        || ip.is_loopback()
        || (s[0] & 0xfe00) == 0xfc00                        // unique local
        || (s[0] & 0xffc0) == 0xfe80                        // link-local
        || (s[0] & 0xff00) == 0xff00                        // multicast
        || (s[0] == 0x2001 && s[1] == 0x0db8); // documentation
    !internal
}

fn permitted(allowed: &[IpRange], ip: IpAddr) -> bool {
    is_public(ip) || allowed.iter().any(|range| range.contains(ip))
}

/// Why a URL may not be fetched, if so. A host name passes here; its addresses are checked
/// when it is resolved.
fn refusal(allowed: &[IpRange], url: &reqwest::Url) -> Option<String> {
    if !matches!(url.scheme(), "http" | "https") {
        return Some("only http and https URLs can be fetched".to_owned());
    }
    let ip = match url.host()? {
        url::Host::Ipv4(ip) => IpAddr::V4(ip),
        url::Host::Ipv6(ip) => IpAddr::V6(ip),
        url::Host::Domain(_) => return None,
    };
    (!permitted(allowed, ip)).then(|| format!("{ip} is not a public address"))
}

/// Resolves host names, leaving out the addresses a fetch may not go to.
struct PublicResolver {
    inner: Arc<hickory_resolver::TokioResolver>,
    allowed: Arc<[IpRange]>,
}

impl reqwest::dns::Resolve for PublicResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let inner = Arc::clone(&self.inner);
        let allowed = Arc::clone(&self.allowed);
        Box::pin(async move {
            let host = name.as_str().to_owned();
            let found = inner
                .lookup_ip(host.as_str())
                .await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
            let usable: Vec<SocketAddr> = found
                .iter()
                .filter(|ip| permitted(&allowed, *ip))
                .map(|ip| SocketAddr::new(ip, 0))
                .collect();
            if usable.is_empty() {
                return Err(format!("{host} does not resolve to a public address").into());
            }
            Ok(Box::new(usable.into_iter()) as reqwest::dns::Addrs)
        })
    }
}

/// The HTTP client for URLs that somebody else chose.
#[derive(Clone)]
pub struct Outbound {
    client: reqwest::Client,
    allowed: Arc<[IpRange]>,
}

impl std::fmt::Debug for Outbound {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Outbound")
    }
}

impl Outbound {
    /// `allowed` lists the ranges that may be reached although they are not public.
    pub fn new(allowed: &[String]) -> Result<Self> {
        let allowed: Arc<[IpRange]> = allowed
            .iter()
            .map(|s| s.parse())
            .collect::<std::result::Result<Vec<IpRange>, String>>()
            .map_err(|e| Error::Protocol(format!("allowed_internal_ranges: {e}")))?
            .into();
        let resolver = hickory_resolver::TokioResolver::builder_tokio()
            .map_err(|e| Error::Protocol(format!("dns resolver: {e}")))?
            .build()
            .map_err(|e| Error::Protocol(format!("dns resolver: {e}")))?;
        let hop_rules = Arc::clone(&allowed);
        let client = reqwest::Client::builder()
            .user_agent(concat!("asaroton/", env!("CARGO_PKG_VERSION")))
            .timeout(TIMEOUT)
            .connect_timeout(Duration::from_secs(10))
            // A proxy from the environment would be handed the URL without these checks.
            .no_proxy()
            .redirect(reqwest::redirect::Policy::custom(move |attempt| {
                if attempt.previous().len() >= MAX_REDIRECTS {
                    return attempt.error("too many redirects");
                }
                match refusal(&hop_rules, attempt.url()) {
                    Some(why) => attempt.error(why),
                    None => attempt.follow(),
                }
            }))
            .dns_resolver(Arc::new(PublicResolver {
                inner: Arc::new(resolver),
                allowed: Arc::clone(&allowed),
            }))
            .build()
            .map_err(|e| Error::Protocol(format!("http client: {e}")))?;
        Ok(Self { client, allowed })
    }

    /// Parse a URL and refuse it when it plainly leads somewhere it may not.
    pub fn parse(&self, url: &str) -> Result<reqwest::Url> {
        let parsed = reqwest::Url::parse(url).map_err(|_| Error::Protocol("invalid URL".into()))?;
        match refusal(&self.allowed, &parsed) {
            Some(why) => Err(Error::Protocol(why)),
            None => Ok(parsed),
        }
    }

    /// A request to a URL that passed [`Self::parse`]. Its host name, and every redirect,
    /// is checked on the way.
    pub fn request(&self, method: reqwest::Method, url: reqwest::Url) -> reqwest::RequestBuilder {
        self.client.request(method, url)
    }

    /// Fetch media from the URL a homeserver redirected us to (typically its CDN): a plain
    /// `GET` without our signature, bounded in time and size. Returns the bytes and the
    /// content type.
    pub async fn media_location(
        &self,
        origin: &ruma::ServerName,
        url: &str,
        max_body: usize,
        timeout: Duration,
    ) -> Result<(Vec<u8>, String)> {
        let parsed = self
            .parse(url)
            .map_err(|e| Error::Protocol(format!("{origin}: unusable media location: {e}")))?;
        let resp = self
            .request(reqwest::Method::GET, parsed)
            .timeout(timeout)
            .send()
            .await
            .map_err(|e| Error::Transport(origin.to_owned(), crate::client::error_chain(&e)))?;
        let status = resp.status();
        let content_type = resp
            .headers()
            .get(http::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("application/octet-stream")
            .to_owned();
        let bytes = crate::client::read_body(origin, resp, Some(max_body)).await?;
        if !status.is_success() {
            return Err(Error::Protocol(format!(
                "{origin}: media location answered {status}"
            )));
        }
        Ok((bytes.to_vec(), content_type))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap_or_else(|e| panic!("{s}: {e}"))
    }

    fn ranges(list: &[&str]) -> Vec<IpRange> {
        list.iter()
            .map(|s| s.parse().unwrap_or_else(|e| panic!("{e}")))
            .collect()
    }

    #[test]
    fn internal_addresses_are_not_public() {
        for internal in [
            "0.0.0.0",
            "10.1.2.3",
            "100.64.0.1",
            "100.127.255.255",
            "127.0.0.1",
            "127.8.8.8",
            "169.254.169.254",
            "172.16.0.1",
            "172.31.255.255",
            "192.0.0.8",
            "192.0.2.1",
            "192.168.2.4",
            "198.18.0.1",
            "198.51.100.7",
            "203.0.113.9",
            "224.0.0.1",
            "255.255.255.255",
            "::",
            "::1",
            "::ffff:127.0.0.1",
            "::ffff:192.168.2.4",
            "::10.0.0.1",
            "64:ff9b::7f00:1",
            "2002:c0a8:0204::1",
            "fc00::1",
            "fd12:3456::1",
            "fe80::1",
            "ff02::1",
            "2001:db8::1",
        ] {
            assert!(!is_public(ip(internal)), "{internal}");
        }
        for public in [
            "1.1.1.1",
            "62.131.54.100",
            "100.63.255.255",
            "100.128.0.1",
            "172.15.0.1",
            "172.32.0.1",
            "192.0.1.1",
            "198.17.0.1",
            "198.20.0.1",
            "223.255.255.254",
            "::ffff:8.8.8.8",
            "64:ff9b::808:808",
            "2002:0808:0808::1",
            "2a00:1450:400e::200e",
            "2001:4860:4860::8888",
        ] {
            assert!(is_public(ip(public)), "{public}");
        }
    }

    #[test]
    fn ranges_parse_and_match() {
        let private: IpRange = "192.168.2.0/24".parse().unwrap_or_else(|e| panic!("{e}"));
        assert!(private.contains(ip("192.168.2.4")));
        assert!(private.contains(ip("::ffff:192.168.2.4")));
        assert!(!private.contains(ip("192.168.3.4")));
        assert!(!private.contains(ip("fd00::1")));
        let single: IpRange = "127.0.0.2".parse().unwrap_or_else(|e| panic!("{e}"));
        assert!(single.contains(ip("127.0.0.2")));
        assert!(!single.contains(ip("127.0.0.1")));
        let v6: IpRange = "fd00::/8".parse().unwrap_or_else(|e| panic!("{e}"));
        assert!(v6.contains(ip("fd12:3456::1")));
        assert!(!v6.contains(ip("fe80::1")));
        assert!(!v6.contains(ip("10.0.0.1")));
        let all: IpRange = "0.0.0.0/0".parse().unwrap_or_else(|e| panic!("{e}"));
        assert!(all.contains(ip("10.0.0.1")) && all.contains(ip("8.8.8.8")));
        let all6: IpRange = "::/0".parse().unwrap_or_else(|e| panic!("{e}"));
        assert!(all6.contains(ip("::1")) && !all6.contains(ip("127.0.0.1")));
        for bad in [
            "",
            "10.0.0.0/33",
            "fd00::/129",
            "example.org",
            "10.0.0.0/x",
            "10.0.0/8",
        ] {
            assert!(bad.parse::<IpRange>().is_err(), "{bad}");
        }
    }

    #[test]
    fn urls_with_literal_internal_addresses_are_refused() {
        let none: Vec<IpRange> = Vec::new();
        let url = |s: &str| reqwest::Url::parse(s).unwrap_or_else(|e| panic!("{s}: {e}"));
        for bad in [
            "http://127.0.0.1:8018/_matrix/client/versions",
            "http://[::1]/",
            "http://[::ffff:10.0.0.1]/",
            "https://192.168.2.4/",
            "http://169.254.169.254/latest/meta-data/",
            // Other spellings of 127.0.0.1.
            "http://2130706433/",
            "http://0x7f000001/",
            "http://0177.0.0.1/",
            "http://127.1/",
            "ftp://example.org/",
            "file:///etc/passwd",
        ] {
            assert!(refusal(&none, &url(bad)).is_some(), "{bad}");
        }
        for good in [
            "https://example.org/page",
            "http://93.184.216.34/",
            "https://[2a00:1450:400e::200e]/",
        ] {
            assert_eq!(refusal(&none, &url(good)), None, "{good}");
        }
        // Host names pass here and are judged by what they resolve to.
        assert_eq!(refusal(&none, &url("http://localhost:8000/")), None);
        // An operator's exemption.
        let lan = ranges(&["192.168.2.0/24", "127.0.0.2"]);
        assert_eq!(
            refusal(&lan, &url("http://192.168.2.4:5000/_matrix/push/v1/notify")),
            None
        );
        assert_eq!(refusal(&lan, &url("http://127.0.0.2/")), None);
        assert!(refusal(&lan, &url("http://127.0.0.1/")).is_some());
        assert!(permitted(&lan, ip("192.168.2.9")) && !permitted(&lan, ip("192.168.3.9")));
    }

    #[test]
    fn a_bad_range_is_a_configuration_error() {
        assert!(Outbound::new(&["10.0.0.0/8".to_owned(), "nonsense".to_owned()]).is_err());
    }
}
