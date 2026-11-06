//! Media repository: uploads, downloads and thumbnails. Bytes live on disk under
//! `<data_dir>/media/<server>/<id>`; metadata lives in the store.

use std::path::PathBuf;

use asaroton_store::{MediaInfo, ThumbnailInfo, now_ts};
use ruma::{OwnedMxcUri, OwnedServerName, ServerName, UserId};
use sha2::{Digest, Sha256};

use crate::{Error, Result, Services};

/// Maximum upload size in bytes.
pub const MAX_UPLOAD_BYTES: u64 = 100 * 1024 * 1024;
/// Async uploads must complete within this long.
const PENDING_UPLOAD_TTL_MS: i64 = 24 * 60 * 60 * 1000;

/// A media file ready to serve.
#[derive(Debug)]
pub struct MediaFile {
    pub bytes: Vec<u8>,
    pub content_type: String,
    pub filename: Option<String>,
}

fn random_media_id() -> String {
    use rand::{Rng, distr::Alphanumeric};
    rand::rng()
        .sample_iter(&Alphanumeric)
        .take(24)
        .map(char::from)
        .collect()
}

/// Only safe characters end up in file names.
fn safe_component(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_') {
                c
            } else {
                '_'
            }
        })
        .collect()
}

impl Services {
    fn media_path(&self, server_name: &ServerName, media_id: &str) -> PathBuf {
        self.config
            .media_dir
            .join(safe_component(server_name.as_str()))
            .join(safe_component(media_id))
    }

    fn thumbnail_path(
        &self,
        server_name: &ServerName,
        media_id: &str,
        t: &ThumbnailInfo,
    ) -> PathBuf {
        self.config
            .media_dir
            .join(safe_component(server_name.as_str()))
            .join(format!(
                "{}.thumb.{}x{}.{}",
                safe_component(media_id),
                t.width,
                t.height,
                t.method
            ))
    }

    /// Reserve a media ID for a later upload (`/media/v1/create`).
    pub async fn create_media_id(&self, user: &UserId) -> Result<(OwnedMxcUri, i64)> {
        let media_id = random_media_id();
        let now = now_ts();
        self.store
            .create_media(&MediaInfo {
                server_name: self.config.server_name.clone(),
                media_id: media_id.clone(),
                user_id: Some(user.to_owned()),
                content_type: None,
                size: None,
                upload_name: None,
                sha256: None,
                created_ts: now,
                uploaded_ts: None,
                quarantined: false,
            })
            .await?;
        let uri = OwnedMxcUri::from(format!("mxc://{}/{media_id}", self.config.server_name));
        Ok((uri, now + PENDING_UPLOAD_TTL_MS))
    }

    /// Store an upload. With `media_id` set, completes a previously reserved ID.
    pub async fn upload_media(
        &self,
        user: &UserId,
        media_id: Option<String>,
        content_type: Option<&str>,
        filename: Option<&str>,
        bytes: &[u8],
    ) -> Result<OwnedMxcUri> {
        if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > MAX_UPLOAD_BYTES {
            return Err(Error::matrix(
                ruma::api::error::ErrorKind::TooLarge,
                http::StatusCode::PAYLOAD_TOO_LARGE,
                "Upload too large.",
            ));
        }
        let server = self.config.server_name.clone();
        let now = now_ts();
        let sha256 = hex::encode(Sha256::digest(bytes));
        let (media_id, existing) = match media_id {
            Some(id) => {
                let Some(existing) = self.store.get_media(&server, &id).await? else {
                    return Err(Error::not_found("Unknown media ID."));
                };
                if existing.user_id.as_deref() != Some(user) {
                    return Err(Error::forbidden(
                        "This media ID was reserved by another user.",
                    ));
                }
                if existing.uploaded_ts.is_some() {
                    return Err(Error::matrix(
                        ruma::api::error::ErrorKind::CannotOverwriteMedia,
                        http::StatusCode::CONFLICT,
                        "Media already uploaded.",
                    ));
                }
                if now - existing.created_ts > PENDING_UPLOAD_TTL_MS {
                    return Err(Error::not_found("Media ID expired."));
                }
                (id, Some(existing))
            }
            None => (random_media_id(), None),
        };
        let path = self.media_path(&server, &media_id);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| Error::internal(format!("media dir: {e}")))?;
        }
        tokio::fs::write(&path, bytes)
            .await
            .map_err(|e| Error::internal(format!("writing media: {e}")))?;
        let info = MediaInfo {
            server_name: server.clone(),
            media_id: media_id.clone(),
            user_id: Some(user.to_owned()),
            content_type: Some(
                content_type
                    .unwrap_or("application/octet-stream")
                    .to_owned(),
            ),
            size: Some(i64::try_from(bytes.len()).unwrap_or(i64::MAX)),
            upload_name: filename.map(str::to_owned),
            sha256: Some(sha256),
            created_ts: existing.as_ref().map_or(now, |e| e.created_ts),
            uploaded_ts: Some(now),
            quarantined: false,
        };
        if existing.is_some() {
            self.store.update_media(&info).await?;
        } else {
            self.store.create_media(&info).await?;
        }
        Ok(OwnedMxcUri::from(format!("mxc://{server}/{media_id}")))
    }

    /// Store server-generated local media (URL preview images) under a fresh ID.
    pub async fn store_local_media(
        &self,
        bytes: &[u8],
        content_type: &str,
        filename: Option<&str>,
    ) -> Result<OwnedMxcUri> {
        let server = self.config.server_name.clone();
        let media_id = random_media_id();
        let path = self.media_path(&server, &media_id);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| Error::internal(format!("media dir: {e}")))?;
        }
        tokio::fs::write(&path, bytes)
            .await
            .map_err(|e| Error::internal(format!("writing media: {e}")))?;
        let now = now_ts();
        self.store
            .create_media(&MediaInfo {
                server_name: server.clone(),
                media_id: media_id.clone(),
                user_id: None,
                content_type: Some(content_type.to_owned()),
                size: Some(i64::try_from(bytes.len()).unwrap_or(i64::MAX)),
                upload_name: filename.map(str::to_owned),
                sha256: Some(hex::encode(Sha256::digest(bytes))),
                created_ts: now,
                uploaded_ts: Some(now),
                quarantined: false,
            })
            .await?;
        Ok(OwnedMxcUri::from(format!("mxc://{server}/{media_id}")))
    }

    /// Fetch media bytes. Remote media is fetched over federation and cached (later phase).
    pub async fn get_media(&self, server_name: &ServerName, media_id: &str) -> Result<MediaFile> {
        let Some(info) = self.store.get_media(server_name, media_id).await? else {
            if server_name != self.config.server_name {
                return self.fetch_remote_media(server_name, media_id).await;
            }
            return Err(Error::not_found("Media not found."));
        };
        if info.uploaded_ts.is_none() {
            return Err(Error::matrix(
                ruma::api::error::ErrorKind::NotYetUploaded,
                http::StatusCode::GATEWAY_TIMEOUT,
                "Media has not been uploaded yet.",
            ));
        }
        if info.quarantined {
            return Err(Error::not_found("Media not found."));
        }
        let bytes = tokio::fs::read(self.media_path(server_name, media_id))
            .await
            .map_err(|_| Error::not_found("Media not found."))?;
        Ok(MediaFile {
            bytes,
            content_type: info
                .content_type
                .unwrap_or_else(|| "application/octet-stream".into()),
            filename: info.upload_name,
        })
    }

    async fn fetch_remote_media(
        &self,
        server_name: &ServerName,
        media_id: &str,
    ) -> Result<MediaFile> {
        let (bytes, content_type, filename) = self
            .federation()
            .remote_media(server_name, media_id)
            .await?;
        // Cache locally.
        let path = self.media_path(server_name, media_id);
        if let Some(parent) = path.parent() {
            let _ = tokio::fs::create_dir_all(parent).await;
        }
        let _ = tokio::fs::write(&path, &bytes).await;
        let _ = self
            .store
            .create_media(&MediaInfo {
                server_name: server_name.to_owned(),
                media_id: media_id.to_owned(),
                user_id: None,
                content_type: Some(content_type.clone()),
                size: Some(i64::try_from(bytes.len()).unwrap_or(i64::MAX)),
                upload_name: filename.clone(),
                sha256: Some(hex::encode(Sha256::digest(&bytes))),
                created_ts: now_ts(),
                uploaded_ts: Some(now_ts()),
                quarantined: false,
            })
            .await;
        Ok(MediaFile {
            bytes,
            content_type,
            filename,
        })
    }

    /// A thumbnail, generated on first request and cached on disk.
    pub async fn get_thumbnail(
        &self,
        server_name: &ServerName,
        media_id: &str,
        width: u32,
        height: u32,
        method: &str,
    ) -> Result<MediaFile> {
        // Snap to the spec's suggested sizes so the cache stays small.
        let (width, height, method) = snap_thumbnail_size(width, height, method);
        let existing = self.store.thumbnails(server_name, media_id).await?;
        if let Some(t) = existing
            .iter()
            .find(|t| t.width == width && t.height == height && t.method == method)
            && let Ok(bytes) = tokio::fs::read(self.thumbnail_path(server_name, media_id, t)).await
        {
            return Ok(MediaFile {
                bytes,
                content_type: t.content_type.clone(),
                filename: None,
            });
        }
        let original = self.get_media(server_name, media_id).await?;
        if !original.content_type.starts_with("image/") {
            return Err(Error::matrix(
                ruma::api::error::ErrorKind::Unknown,
                http::StatusCode::BAD_REQUEST,
                "Cannot thumbnail non-image media.",
            ));
        }
        let src = original.bytes;
        let method_owned = method.to_owned();
        let generated = tokio::task::spawn_blocking(move || {
            generate_thumbnail(&src, width, height, &method_owned)
        })
        .await
        .map_err(|e| Error::internal(e.to_string()))??;
        let info = ThumbnailInfo {
            width,
            height,
            method: method.to_owned(),
            content_type: "image/png".to_owned(),
            size: i64::try_from(generated.len()).unwrap_or(i64::MAX),
        };
        let path = self.thumbnail_path(server_name, media_id, &info);
        if let Some(parent) = path.parent() {
            let _ = tokio::fs::create_dir_all(parent).await;
        }
        let _ = tokio::fs::write(&path, &generated).await;
        let _ = self.store.add_thumbnail(server_name, media_id, &info).await;
        Ok(MediaFile {
            bytes: generated,
            content_type: "image/png".to_owned(),
            filename: None,
        })
    }
}

/// Spec-recommended thumbnail sizes; pick the smallest that covers the request.
fn snap_thumbnail_size(width: u32, height: u32, method: &str) -> (u32, u32, &'static str) {
    const SIZES: [(u32, u32, &str); 5] = [
        (32, 32, "crop"),
        (96, 96, "crop"),
        (320, 240, "scale"),
        (640, 480, "scale"),
        (800, 600, "scale"),
    ];
    let method = if method == "crop" { "crop" } else { "scale" };
    for (w, h, m) in SIZES {
        if m == method && w >= width && h >= height {
            return (w, h, m);
        }
    }
    (800, 600, method)
}

fn generate_thumbnail(src: &[u8], width: u32, height: u32, method: &str) -> Result<Vec<u8>> {
    let img = image::load_from_memory(src).map_err(|e| {
        Error::matrix(
            ruma::api::error::ErrorKind::Unknown,
            http::StatusCode::BAD_REQUEST,
            format!("Cannot decode image: {e}"),
        )
    })?;
    let thumb = if method == "crop" {
        img.resize_to_fill(width, height, image::imageops::FilterType::Lanczos3)
    } else {
        img.resize(width, height, image::imageops::FilterType::Lanczos3)
    };
    let mut out = std::io::Cursor::new(Vec::new());
    thumb
        .write_to(&mut out, image::ImageFormat::Png)
        .map_err(|e| Error::internal(format!("encoding thumbnail: {e}")))?;
    Ok(out.into_inner())
}

/// Parse an `mxc://server/id` URI.
pub fn parse_mxc(uri: &str) -> Option<(OwnedServerName, String)> {
    let rest = uri.strip_prefix("mxc://")?;
    let (server, id) = rest.split_once('/')?;
    Some((OwnedServerName::try_from(server).ok()?, id.to_owned()))
}
