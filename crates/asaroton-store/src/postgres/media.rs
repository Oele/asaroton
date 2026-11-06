use async_trait::async_trait;
use ruma::{OwnedServerName, OwnedUserId, ServerName};
use sqlx::Row;

use super::PostgresStore;
use crate::{Error, MediaInfo, MediaStore, Result, ThumbnailInfo};

fn media_from_row(r: &sqlx::postgres::PgRow) -> Result<MediaInfo> {
    let corrupt = |e: ruma::IdParseError| Error::Corrupt(e.to_string());
    Ok(MediaInfo {
        server_name: OwnedServerName::try_from(r.get::<String, _>("server_name"))
            .map_err(corrupt)?,
        media_id: r.get("media_id"),
        user_id: r
            .get::<Option<String>, _>("user_id")
            .map(OwnedUserId::try_from)
            .transpose()
            .map_err(corrupt)?,
        content_type: r.get("content_type"),
        size: r.get("size"),
        upload_name: r.get("upload_name"),
        sha256: r.get("sha256"),
        created_ts: r.get("created_ts"),
        uploaded_ts: r.get("uploaded_ts"),
        quarantined: r.get("quarantined"),
    })
}

const COLS: &str = "server_name, media_id, user_id, content_type, size, upload_name, sha256, created_ts, uploaded_ts, quarantined";

#[async_trait]
impl MediaStore for PostgresStore {
    async fn create_media(&self, m: &MediaInfo) -> Result<()> {
        let res = sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO media ({COLS}) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)"
        )))
        .bind(m.server_name.as_str())
        .bind(&m.media_id)
        .bind(m.user_id.as_ref().map(|u| u.as_str()))
        .bind(&m.content_type)
        .bind(m.size)
        .bind(&m.upload_name)
        .bind(&m.sha256)
        .bind(m.created_ts)
        .bind(m.uploaded_ts)
        .bind(m.quarantined)
        .execute(&self.pool)
        .await;
        match res {
            Ok(_) => Ok(()),
            Err(e) => {
                let err = Error::from(e);
                Err(if err.is_unique_violation() {
                    Error::AlreadyExists("media")
                } else {
                    err
                })
            }
        }
    }

    async fn update_media(&self, m: &MediaInfo) -> Result<()> {
        sqlx::query(
            "UPDATE media SET user_id = $3, content_type = $4, size = $5, upload_name = $6, sha256 = $7,
                uploaded_ts = $8, quarantined = $9 WHERE server_name = $1 AND media_id = $2",
        )
        .bind(m.server_name.as_str())
        .bind(&m.media_id)
        .bind(m.user_id.as_ref().map(|u| u.as_str()))
        .bind(&m.content_type)
        .bind(m.size)
        .bind(&m.upload_name)
        .bind(&m.sha256)
        .bind(m.uploaded_ts)
        .bind(m.quarantined)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn get_media(
        &self,
        server_name: &ServerName,
        media_id: &str,
    ) -> Result<Option<MediaInfo>> {
        let row = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT {COLS} FROM media WHERE server_name = $1 AND media_id = $2"
        )))
        .bind(server_name.as_str())
        .bind(media_id)
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(media_from_row).transpose()
    }

    async fn add_thumbnail(
        &self,
        server_name: &ServerName,
        media_id: &str,
        t: &ThumbnailInfo,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO media_thumbnails (server_name, media_id, width, height, method, content_type, size)
             VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT DO NOTHING",
        )
        .bind(server_name.as_str())
        .bind(media_id)
        .bind(i32::try_from(t.width).unwrap_or(i32::MAX))
        .bind(i32::try_from(t.height).unwrap_or(i32::MAX))
        .bind(&t.method)
        .bind(&t.content_type)
        .bind(t.size)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn thumbnails(
        &self,
        server_name: &ServerName,
        media_id: &str,
    ) -> Result<Vec<ThumbnailInfo>> {
        let rows = sqlx::query(
            "SELECT width, height, method, content_type, size FROM media_thumbnails WHERE server_name = $1 AND media_id = $2",
        )
        .bind(server_name.as_str())
        .bind(media_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|r| ThumbnailInfo {
                width: u32::try_from(r.get::<i32, _>("width")).unwrap_or(0),
                height: u32::try_from(r.get::<i32, _>("height")).unwrap_or(0),
                method: r.get("method"),
                content_type: r.get("content_type"),
                size: r.get("size"),
            })
            .collect())
    }
}
