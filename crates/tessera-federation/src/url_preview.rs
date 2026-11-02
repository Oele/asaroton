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
        let parsed = reqwest::Url::parse(url).map_err(|_| Error::Protocol("invalid URL".into()))?;
        if !matches!(parsed.scheme(), "http" | "https") {
            return Err(Error::Protocol(
                "only http and https URLs can be previewed".into(),
            ));
        }
        let client = plain_http();
        let resp = client
            .get(parsed.clone())
            .send()
            .await
            .map_err(|e| Error::Protocol(format!("fetching URL: {e}")))?;
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
            && matches!(img_url.scheme(), "http" | "https")
            && let Ok(resp) = client.get(img_url).send().await
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
        let dims = image::load_from_memory(&bytes)
            .ok()
            .map(|i| (i.width(), i.height()));
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

/// A plain HTTP client for fetching arbitrary URLs (not federation).
pub(crate) fn plain_http() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .redirect(reqwest::redirect::Policy::limited(5))
        .user_agent("tessera-url-preview")
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
    fn decodes_numeric_and_named_entities() {
        assert_eq!(
            decode_entities("auto&#x27;s &#233;n caf&eacute; &quot;x&quot; &#39;y&#39;"),
            "auto's én café \"x\" 'y'"
        );
    }
}
