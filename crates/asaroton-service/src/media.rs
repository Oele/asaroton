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
/// Images with more pixels than this are not decoded for a thumbnail: a small file can
/// declare enormous dimensions. (Synapse's default is the same.)
const MAX_IMAGE_PIXELS: u64 = 32_000_000;
/// Memory one decode may allocate.
const MAX_DECODE_BYTES: u64 = 512 * 1024 * 1024;
/// Thumbnails generated at the same time; each holds a decoded image in memory.
static THUMBNAIL_SLOTS: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(2);

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
        // Cache locally. A file that could not be written must not be recorded: the record
        // would make every later request fail instead of fetching the media again.
        let path = self.media_path(server_name, media_id);
        if let Some(parent) = path.parent() {
            let _ = tokio::fs::create_dir_all(parent).await;
        }
        if let Err(e) = tokio::fs::write(&path, &bytes).await {
            tracing::warn!(%server_name, media_id, error = %e, "could not cache remote media");
            let _ = tokio::fs::remove_file(&path).await;
            return Ok(MediaFile {
                bytes,
                content_type,
                filename,
            });
        }
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
        let local = *server_name == self.config.server_name;
        let slot = THUMBNAIL_SLOTS
            .acquire()
            .await
            .map_err(|e| Error::internal(e.to_string()))?;
        let generated = tokio::task::spawn_blocking(move || {
            generate_thumbnail(&src, width, height, &method_owned, local)
        })
        .await
        .map_err(|e| Error::internal(e.to_string()))?;
        drop(slot);
        let generated = generated?;
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

/// The pixel dimensions an image declares, read from its header without decoding it.
pub fn image_dimensions(src: &[u8]) -> Option<(u32, u32)> {
    image::ImageReader::new(std::io::Cursor::new(src))
        .with_guessed_format()
        .ok()?
        .into_dimensions()
        .ok()
}

fn generate_thumbnail(
    src: &[u8],
    width: u32,
    height: u32,
    method: &str,
    local: bool,
) -> Result<Vec<u8>> {
    let undecodable = |e: &dyn std::fmt::Display| {
        Error::matrix(
            ruma::api::error::ErrorKind::Unknown,
            http::StatusCode::BAD_REQUEST,
            format!("Cannot decode image: {e}"),
        )
    };
    // Look at the declared size before decoding anything.
    let Some((w, h)) = image_dimensions(src) else {
        return Err(undecodable(&"unrecognised image"));
    };
    if u64::from(w) * u64::from(h) > MAX_IMAGE_PIXELS {
        return Err(Error::matrix(
            ruma::api::error::ErrorKind::TooLarge,
            if local {
                http::StatusCode::PAYLOAD_TOO_LARGE
            } else {
                http::StatusCode::BAD_GATEWAY
            },
            format!("The image is too large to thumbnail ({w}x{h} pixels)."),
        ));
    }
    let mut reader = image::ImageReader::new(std::io::Cursor::new(src))
        .with_guessed_format()
        .map_err(|e| undecodable(&e))?;
    let mut limits = image::Limits::default();
    limits.max_alloc = Some(MAX_DECODE_BYTES);
    reader.limits(limits);
    let img = reader.decode().map_err(|e| undecodable(&e))?;
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

#[cfg(test)]
mod tests {
    use super::*;

    /// A PNG file that consists of little more than its header.
    fn png_declaring(width: u32, height: u32) -> Vec<u8> {
        fn chunk(out: &mut Vec<u8>, kind: [u8; 4], data: &[u8]) {
            let mut crc = 0xFFFF_FFFF_u32;
            for byte in kind.iter().chain(data) {
                crc ^= u32::from(*byte);
                for _ in 0..8 {
                    crc = if crc & 1 == 1 {
                        (crc >> 1) ^ 0xEDB8_8320
                    } else {
                        crc >> 1
                    };
                }
            }
            out.extend_from_slice(&u32::try_from(data.len()).unwrap_or(0).to_be_bytes());
            out.extend_from_slice(&kind);
            out.extend_from_slice(data);
            out.extend_from_slice(&(!crc).to_be_bytes());
        }
        let mut out = b"\x89PNG\r\n\x1a\n".to_vec();
        let mut header = Vec::new();
        header.extend_from_slice(&width.to_be_bytes());
        header.extend_from_slice(&height.to_be_bytes());
        header.extend_from_slice(&[8, 2, 0, 0, 0]); // 8-bit RGB
        chunk(&mut out, *b"IHDR", &header);
        chunk(
            &mut out,
            *b"IDAT",
            &[0x78, 0x9c, 0x03, 0x00, 0x00, 0x00, 0x00, 0x01],
        );
        chunk(&mut out, *b"IEND", &[]);
        out
    }

    fn status(e: &Error) -> Option<http::StatusCode> {
        match e {
            Error::Matrix { status, .. } => Some(*status),
            _ => None,
        }
    }

    fn real_png(width: u32, height: u32) -> Vec<u8> {
        let img = image::RgbImage::from_fn(width, height, |x, y| {
            image::Rgb([x.to_le_bytes()[0], y.to_le_bytes()[0], 128])
        });
        let mut out = std::io::Cursor::new(Vec::new());
        img.write_to(&mut out, image::ImageFormat::Png)
            .unwrap_or_else(|e| panic!("encoding: {e}"));
        out.into_inner()
    }

    #[test]
    fn dimensions_come_from_the_header() {
        assert_eq!(
            image_dimensions(&png_declaring(40_000, 40_000)),
            Some((40_000, 40_000))
        );
        assert_eq!(image_dimensions(&real_png(64, 48)), Some((64, 48)));
        assert_eq!(image_dimensions(b"not an image"), None);
    }

    #[test]
    fn an_image_declaring_too_many_pixels_is_not_decoded() {
        // 1.6 billion pixels in a file of under a hundred bytes.
        let bomb = png_declaring(40_000, 40_000);
        assert!(bomb.len() < 100);
        for (local, expected) in [
            (true, http::StatusCode::PAYLOAD_TOO_LARGE),
            (false, http::StatusCode::BAD_GATEWAY),
        ] {
            let err = generate_thumbnail(&bomb, 96, 96, "crop", local)
                .err()
                .unwrap_or_else(|| panic!("the oversized image was thumbnailed"));
            assert_eq!(status(&err), Some(expected));
            assert!(err.to_string().contains("too large to thumbnail"), "{err}");
        }
        // Just under the limit it is decoded (and fails here, as the file holds no pixels).
        let err = generate_thumbnail(&png_declaring(8_000, 4_000), 96, 96, "crop", true)
            .err()
            .unwrap_or_else(|| panic!("an empty image was thumbnailed"));
        assert_eq!(status(&err), Some(http::StatusCode::BAD_REQUEST));
    }

    #[test]
    fn ordinary_images_are_thumbnailed() {
        let src = real_png(640, 480);
        for (method, expected) in [("scale", (320, 240)), ("crop", (96, 96))] {
            let (w, h) = if method == "crop" {
                (96, 96)
            } else {
                (320, 240)
            };
            let out = generate_thumbnail(&src, w, h, method, true)
                .unwrap_or_else(|e| panic!("thumbnail failed: {e}"));
            assert_eq!(image_dimensions(&out), Some(expected));
        }
        assert_eq!(
            generate_thumbnail(b"plain text", 96, 96, "crop", true)
                .err()
                .and_then(|e| status(&e)),
            Some(http::StatusCode::BAD_REQUEST)
        );
    }
}
