//! URL previews (`/preview_url`): fetch a page, read its Open Graph tags, and cache any
//! `og:image` as local media.

use std::time::Duration;

use serde_json::{Value, json};

use crate::{Error, Federation, Result};

/// Bytes of HTML we are willing to read.
const MAX_HTML_BYTES: usize = 1024 * 1024;
/// Bytes of image we are willing to cache.
const MAX_IMAGE_BYTES: usize = 10 * 1024 * 1024;

impl Federation {
    /// Build the OpenGraph-style preview object for a URL.
    pub async fn preview_url(&self, url: &str) -> Result<Value> {
        // The URL is the user's choice, and so is everything the page points at: all of it
        // goes through the client that only reaches public addresses.
        let parsed = self.outbound.parse(url)?;
        let get = |url: reqwest::Url| {
            self.outbound
                .request(reqwest::Method::GET, url)
                .header(http::header::USER_AGENT, "asaroton-url-preview")
        };
        let resp = get(parsed.clone()).send().await.map_err(|e| {
            Error::Protocol(format!("fetching URL: {}", crate::client::error_chain(&e)))
        })?;
        if is_a_gate(&parsed, resp.url()) {
            // The site did not show us the page: it sent us to another site to be let in
            // first (a consent page, a login). What is there is not a preview of what the
            // user linked, so there is none; an answer without a title shows no card.
            tracing::debug!(asked = %parsed, ended = %resp.url(), "URL preview ended at a gate");
            return Ok(json!({}));
        }
        let content_type = resp
            .headers()
            .get(http::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("application/octet-stream")
            .to_owned();
        let essence = content_type
            .split(';')
            .next()
            .unwrap_or("")
            .trim()
            .to_ascii_lowercase();
        let mut og = serde_json::Map::new();
        if essence.starts_with("image/") {
            let bytes = read_limited(resp, MAX_IMAGE_BYTES).await?;
            self.attach_image(&mut og, bytes, &essence).await;
            return Ok(Value::Object(og));
        }
        if !(essence == "text/html" || essence == "application/xhtml+xml") {
            // Nothing to preview; report what we know.
            if let Some(len) = resp.content_length() {
                og.insert("matrix:size".into(), json!(len));
            }
            return Ok(Value::Object(og));
        }
        let bytes = read_limited(resp, MAX_HTML_BYTES).await?;
        let html = String::from_utf8_lossy(&bytes);
        for (prop, content) in meta_tags(&html) {
            if prop.starts_with("og:") && !og.contains_key(&prop) {
                og.insert(prop, Value::String(content));
            }
        }
        if !og.contains_key("og:title")
            && let Some(t) = title_tag(&html)
        {
            og.insert("og:title".into(), Value::String(t));
        }
        if !og.contains_key("og:description")
            && let Some(d) = meta_tags(&html)
                .into_iter()
                .find(|(p, _)| p == "description")
                .map(|(_, c)| c)
        {
            og.insert("og:description".into(), Value::String(d));
        }
        // Resolve and cache the image.
        if let Some(Value::String(img)) = og.get("og:image").cloned()
            && let Ok(img_url) = parsed.join(&img)
            && let Ok(img_url) = self.outbound.parse(img_url.as_str())
            && let Ok(resp) = get(img_url).send().await
        {
            let ct = resp
                .headers()
                .get(http::header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("image/unknown")
                .split(';')
                .next()
                .unwrap_or("image/unknown")
                .trim()
                .to_ascii_lowercase();
            if let Ok(bytes) = read_limited(resp, MAX_IMAGE_BYTES).await {
                self.attach_image(&mut og, bytes, &ct).await;
            }
        }
        Ok(Value::Object(og))
    }

    /// Store the image locally and describe it in the preview.
    async fn attach_image(
        &self,
        og: &mut serde_json::Map<String, Value>,
        bytes: Vec<u8>,
        ct: &str,
    ) {
        let size = bytes.len();
        // From the header only: decoding an untrusted image just to learn its size would let a
        // small file claim a large amount of memory.
        let dims = asaroton_service::media::image_dimensions(&bytes);
        match self.services.store_local_media(&bytes, ct, None).await {
            Ok(mxc) => {
                og.insert("og:image".into(), json!(mxc));
                og.insert("og:image:type".into(), json!(ct));
                og.insert("matrix:image:size".into(), json!(size));
                if let Some((w, h)) = dims {
                    og.insert("og:image:width".into(), json!(w));
                    og.insert("og:image:height".into(), json!(h));
                }
            }
            Err(e) => {
                tracing::debug!(error = %e, "storing preview image failed");
                og.remove("og:image");
            }
        }
    }
}

/// Whether a fetch of `asked` that ended at `ended` was sent to a gate: a page on another
/// host that is told, in its address, where to send the visitor back to. That is how consent
/// pages and logins work (`https://consent.example/?callbackUrl=https%3A%2F%2Fnews.example%2F…`),
/// and their title and picture are their own, not the linked page's.
///
/// A plain redirect to another host (a link shortener, a site that moved) names no way back
/// and is followed as before.
fn is_a_gate(asked: &reqwest::Url, ended: &reqwest::Url) -> bool {
    let (Some(from), Some(to)) = (asked.host_str(), ended.host_str()) else {
        return false;
    };
    if from.eq_ignore_ascii_case(to) {
        return false;
    }
    let from = from.to_ascii_lowercase();
    // The way back is often wrapped more than once (the gate's own "accept" address around
    // the page), so a value is unwrapped a few times.
    ended.query_pairs().any(|(_, value)| {
        let mut value = value.into_owned();
        for _ in 0..3 {
            if value.to_ascii_lowercase().contains(&from) {
                return true;
            }
            let unwrapped = percent_encoding::percent_decode_str(&value)
                .decode_utf8_lossy()
                .into_owned();
            if unwrapped == value {
                break;
            }
            value = unwrapped;
        }
        false
    })
}

/// A plain HTTP client for URLs from the operator's own configuration (application
/// services). URLs that anyone else chose go through [`crate::outbound::Outbound`].
pub(crate) fn plain_http() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .redirect(reqwest::redirect::Policy::limited(5))
        .user_agent("asaroton-url-preview")
        .build()
        .unwrap_or_default()
}

async fn read_limited(resp: reqwest::Response, max: usize) -> Result<Vec<u8>> {
    use futures::StreamExt;
    let mut out = Vec::new();
    let mut stream = resp.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| Error::Protocol(format!("reading body: {e}")))?;
        if out.len() + chunk.len() > max {
            return Err(Error::Protocol("response too large to preview".into()));
        }
        out.extend_from_slice(&chunk);
    }
    Ok(out)
}

/// `(property-or-name, content)` pairs of `<meta>` tags, in document order.
fn meta_tags(html: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let lower = html.to_ascii_lowercase();
    let mut pos = 0;
    while let Some(i) = lower[pos..].find("<meta") {
        let start = pos + i;
        let Some(end_rel) = lower[start..].find('>') else {
            break;
        };
        let tag = &html[start..start + end_rel];
        let prop = attr(tag, "property").or_else(|| attr(tag, "name"));
        let content = attr(tag, "content");
        if let (Some(p), Some(c)) = (prop, content) {
            out.push((p.to_ascii_lowercase(), decode_entities(&c)));
        }
        pos = start + end_rel;
    }
    out
}

fn title_tag(html: &str) -> Option<String> {
    let lower = html.to_ascii_lowercase();
    let start = lower.find("<title")?;
    let open_end = lower[start..].find('>')? + start + 1;
    let close = lower[open_end..].find("</title>")? + open_end;
    let t = decode_entities(html[open_end..close].trim());
    (!t.is_empty()).then_some(t)
}

/// Value of an HTML attribute inside a tag, quoted with `"` or `'`.
fn attr(tag: &str, name: &str) -> Option<String> {
    let lower = tag.to_ascii_lowercase();
    let mut search = 0;
    while let Some(i) = lower[search..].find(name) {
        let at = search + i;
        let before_ok = at == 0 || !lower.as_bytes()[at - 1].is_ascii_alphanumeric();
        let rest = &tag[at + name.len()..];
        let rest_trim = rest.trim_start();
        if before_ok && rest_trim.starts_with('=') {
            let v = rest_trim[1..].trim_start();
            let quote = v.chars().next()?;
            if quote == '"' || quote == '\'' {
                let end = v[1..].find(quote)?;
                return Some(v[1..=end].to_owned());
            }
            let end = v
                .find(|c: char| c.is_whitespace() || c == '/' || c == '>')
                .unwrap_or(v.len());
            return Some(v[..end].to_owned());
        }
        search = at + name.len();
    }
    None
}

/// Decodes named and numeric character references (`&amp;`, `&#39;`, `&#x27;`, `&eacute;`, ...).
fn decode_entities(s: &str) -> String {
    html_escape::decode_html_entities(s).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_open_graph_tags() {
        let html = r#"<html><head><title>T &amp; U</title>
            <meta property="og:title" content="Hello" />
            <meta name="description" content='Desc'>
            <meta property="og:image" content="/img.png"></head></html>"#;
        let tags = meta_tags(html);
        assert_eq!(tags[0], ("og:title".into(), "Hello".into()));
        assert_eq!(tags[1], ("description".into(), "Desc".into()));
        assert_eq!(tags[2].1, "/img.png");
        assert_eq!(title_tag(html).as_deref(), Some("T & U"));
    }

    #[test]
    fn a_page_that_sends_the_visitor_elsewhere_to_be_let_in_is_a_gate() {
        let url = |s: &str| reqwest::Url::parse(s).unwrap();
        let article = url("https://www.nu.nl/economie/6416170/een-artikel.html");
        // The consent page of a news publisher, as it answers: the way back wrapped twice.
        assert!(is_a_gate(
            &article,
            &url(
                "https://myprivacy.dpgmedia.nl/consent?siteKey=abc&callbackUrl=https%3A%2F%2Fwww.nu.nl%2Fprivacy-gate%2Faccept%3FredirectUri%3D%252Feconomie%252F6416170%252Feen-artikel.html"
            )
        ));
        // A login that is told where to return to.
        assert!(is_a_gate(
            &url("https://docs.example.org/page"),
            &url("https://login.example.com/?return_to=https://DOCS.example.org/page")
        ));
        // The page itself, also after a redirect within the site.
        assert!(!is_a_gate(&article, &article));
        assert!(!is_a_gate(
            &article,
            &url("https://www.nu.nl/artikel?ref=www.nu.nl")
        ));
        // Another host that names no way back: a shortened link, a site that moved.
        assert!(!is_a_gate(
            &url("https://short.example/abc"),
            &url("https://news.example.org/story?utm_source=feed")
        ));
        assert!(!is_a_gate(
            &url("http://example.org/"),
            &url("https://www.example.org/")
        ));
    }

    #[test]
    fn decodes_numeric_and_named_entities() {
        assert_eq!(
            decode_entities("auto&#x27;s &#233;n caf&eacute; &quot;x&quot; &#39;y&#39;"),
            "auto's én café \"x\" 'y'"
        );
    }
}
