//! Media endpoints: legacy `/_matrix/media/*` and authenticated `/_matrix/client/v1/media/*`.

use asaroton_service::media::{MAX_UPLOAD_BYTES, MediaFile};
use ruma::{
    UInt,
    api::client::{
        authenticated_media as am,
        media::{self, create_content, create_content_async, create_mxc_uri, get_media_config},
    },
    http_headers::{ContentDisposition, ContentDispositionType},
};

use crate::{AppState, Error, Result, Ruma};

fn disposition(file: &MediaFile) -> ContentDisposition {
    // Only a safe allow-list of types is rendered inline; everything else downloads.
    let inline = matches!(
        file.content_type.as_str(),
        "text/css"
            | "text/plain"
            | "text/csv"
            | "application/json"
            | "application/ld+json"
            | "image/jpeg"
            | "image/gif"
            | "image/png"
            | "image/apng"
            | "image/webp"
            | "image/avif"
            | "video/mp4"
            | "video/webm"
            | "video/ogg"
            | "video/quicktime"
            | "audio/mp4"
            | "audio/webm"
            | "audio/aac"
            | "audio/mpeg"
            | "audio/ogg"
            | "audio/wave"
            | "audio/wav"
            | "audio/x-wav"
            | "audio/x-pn-wav"
            | "audio/flac"
            | "audio/x-flac"
    );
    let ty = if inline {
        ContentDispositionType::Inline
    } else {
        ContentDispositionType::Attachment
    };
    ContentDisposition::new(ty).with_filename(file.filename.clone())
}

#[allow(deprecated)]
pub async fn config(
    _app: AppState,
    _req: Ruma<get_media_config::v3::Request>,
) -> Result<get_media_config::v3::Response> {
    Ok(get_media_config::v3::Response::new(
        UInt::try_from(MAX_UPLOAD_BYTES).unwrap_or(UInt::MAX),
    ))
}

pub async fn config_v1(
    _app: AppState,
    req: Ruma<am::get_media_config::v1::Request>,
) -> Result<am::get_media_config::v1::Response> {
    let _ = req.sender()?;
    Ok(am::get_media_config::v1::Response::new(
        UInt::try_from(MAX_UPLOAD_BYTES).unwrap_or(UInt::MAX),
    ))
}

pub async fn upload(
    app: AppState,
    req: Ruma<create_content::v3::Request>,
) -> Result<create_content::v3::Response> {
    let sender = req.sender()?;
    let uri = app
        .upload_media(
            &sender.user_id,
            None,
            req.body.content_type.as_deref(),
            req.body.filename.as_deref(),
            &req.body.file,
        )
        .await?;
    Ok(create_content::v3::Response::new(uri))
}

pub async fn create(
    app: AppState,
    req: Ruma<create_mxc_uri::v1::Request>,
) -> Result<create_mxc_uri::v1::Response> {
    let sender = req.sender()?;
    let (uri, expires) = app.create_media_id(&sender.user_id).await?;
    let mut resp = create_mxc_uri::v1::Response::new(uri);
    resp.unused_expires_at = Some(ruma::MilliSecondsSinceUnixEpoch(
        UInt::try_from(expires.max(0)).unwrap_or(UInt::MAX),
    ));
    Ok(resp)
}

pub async fn upload_async(
    app: AppState,
    req: Ruma<create_content_async::v3::Request>,
) -> Result<axum::response::Response> {
    use axum::response::IntoResponse;
    let sender = req.sender()?;
    if req.body.server_name != app.server_name() {
        return Err(Error::forbidden(
            "Cannot upload to another server's media ID.",
        ));
    }
    app.upload_media(
        &sender.user_id,
        Some(req.body.media_id.clone()),
        req.body.content_type.as_deref(),
        req.body.filename.as_deref(),
        &req.body.file,
    )
    .await?;
    let mut resp = axum::Json(serde_json::json!({})).into_response();
    crate::extract::add_cors(resp.headers_mut());
    Ok(resp)
}

// --- authenticated (v1.11+) ---------------------------------------------------------------

pub async fn download_v1(
    app: AppState,
    req: Ruma<am::get_content::v1::Request>,
) -> Result<am::get_content::v1::Response> {
    let _ = req.sender()?;
    let file = app
        .get_media(&req.body.server_name, &req.body.media_id)
        .await?;
    let resp = am::get_content::v1::Response::new(
        file.bytes.clone(),
        file.content_type.clone(),
        disposition(&file),
    );
    Ok(resp)
}

pub async fn download_v1_named(
    app: AppState,
    req: Ruma<am::get_content_as_filename::v1::Request>,
) -> Result<am::get_content_as_filename::v1::Response> {
    let _ = req.sender()?;
    let mut file = app
        .get_media(&req.body.server_name, &req.body.media_id)
        .await?;
    file.filename = Some(req.body.filename.clone());
    let resp = am::get_content_as_filename::v1::Response::new(
        file.bytes.clone(),
        file.content_type.clone(),
        disposition(&file),
    );
    Ok(resp)
}

pub async fn thumbnail_v1(
    app: AppState,
    req: Ruma<am::get_content_thumbnail::v1::Request>,
) -> Result<am::get_content_thumbnail::v1::Response> {
    let _ = req.sender()?;
    let method = req
        .body
        .method
        .as_ref()
        .map_or("scale", |m| m.as_str())
        .to_owned();
    let width = u32::try_from(u64::from(req.body.width)).unwrap_or(u32::MAX);
    let height = u32::try_from(u64::from(req.body.height)).unwrap_or(u32::MAX);
    if width == 0 || height == 0 {
        return Err(Error::invalid_param("Width and height must be positive."));
    }
    let file = app
        .get_thumbnail(
            &req.body.server_name,
            &req.body.media_id,
            width,
            height,
            &method,
        )
        .await?;
    let resp = am::get_content_thumbnail::v1::Response::new(
        file.bytes.clone(),
        file.content_type.clone(),
        disposition(&file),
    );
    Ok(resp)
}

// --- legacy unauthenticated ---------------------------------------------------------------

#[allow(deprecated)]
pub async fn download_legacy(
    app: AppState,
    req: Ruma<media::get_content::v3::Request>,
) -> Result<media::get_content::v3::Response> {
    let file = app
        .get_media(&req.body.server_name, &req.body.media_id)
        .await?;
    let mut resp = media::get_content::v3::Response::new(
        file.bytes.clone(),
        file.content_type.clone(),
        disposition(&file),
    );
    resp.cross_origin_resource_policy = Some("cross-origin".to_owned());
    Ok(resp)
}

#[allow(deprecated)]
pub async fn download_legacy_named(
    app: AppState,
    req: Ruma<media::get_content_as_filename::v3::Request>,
) -> Result<media::get_content_as_filename::v3::Response> {
    let mut file = app
        .get_media(&req.body.server_name, &req.body.media_id)
        .await?;
    file.filename = Some(req.body.filename.clone());
    let mut resp = media::get_content_as_filename::v3::Response::new(
        file.bytes.clone(),
        file.content_type.clone(),
        disposition(&file),
    );
    resp.cross_origin_resource_policy = Some("cross-origin".to_owned());
    Ok(resp)
}

#[allow(deprecated)]
pub async fn thumbnail_legacy(
    app: AppState,
    req: Ruma<media::get_content_thumbnail::v3::Request>,
) -> Result<media::get_content_thumbnail::v3::Response> {
    let method = req
        .body
        .method
        .as_ref()
        .map_or("scale", |m| m.as_str())
        .to_owned();
    let width = u32::try_from(u64::from(req.body.width)).unwrap_or(u32::MAX);
    let height = u32::try_from(u64::from(req.body.height)).unwrap_or(u32::MAX);
    if width == 0 || height == 0 {
        return Err(Error::invalid_param("Width and height must be positive."));
    }
    let file = app
        .get_thumbnail(
            &req.body.server_name,
            &req.body.media_id,
            width,
            height,
            &method,
        )
        .await?;
    let mut resp = media::get_content_thumbnail::v3::Response::new(
        file.bytes.clone(),
        file.content_type.clone(),
        disposition(&file),
    );
    resp.cross_origin_resource_policy = Some("cross-origin".to_owned());
    Ok(resp)
}
