//! PostgreSQL backend.

mod account_data;
mod backup;
mod delayed_events;
mod e2ee;
mod events;
mod federation;
mod keys;
mod media;
mod rooms;
mod state;
mod thread_subscriptions;
mod users;

use std::time::Duration;

use futures::future::BoxFuture;
use ruma::RoomVersionId;
use sqlx::{PgPool, postgres::PgPoolOptions};
use tessera_core::{Pdu, room_version};

use crate::{Error, Result, StoredEvent};

/// Embedded migrations, applied by [`PostgresStore::migrate`].
pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

/// The PostgreSQL implementation of [`crate::Store`].
#[derive(Clone, Debug)]
pub struct PostgresStore {
    pool: PgPool,
}

impl PostgresStore {
    /// Connect to the database. Does not run migrations; call [`crate::Store::migrate`].
    pub async fn connect(url: &str, max_connections: u32) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .acquire_timeout(Duration::from_secs(30))
            .connect(url)
            .await?;
        Ok(Self { pool })
    }

    /// Wrap an existing pool (used by tests).
    pub fn from_pool(pool: PgPool) -> Self {
        Self { pool }
    }

    /// The underlying pool.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

impl crate::Store for PostgresStore {
    fn migrate(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            MIGRATOR.run(&self.pool).await?;
            Ok(())
        })
    }
}

/// A row of the `events` table joined with its room version, ready to decode.
#[derive(sqlx::FromRow)]
pub(crate) struct EventRow {
    stream_ordering: i64,
    event_id: String,
    room_version: String,
    json: String,
    state_group: Option<i64>,
    rejected: bool,
    soft_failed: bool,
    outlier: bool,
    original_json: Option<String>,
}

/// Column list for `EventRow` queries; `e` is the events alias, `r` the rooms alias.
pub(crate) const EVENT_COLUMNS: &str = "e.stream_ordering, e.event_id, r.room_version, e.json, \
     e.state_group, e.rejected, e.soft_failed, e.outlier, e.original_json";

impl TryFrom<EventRow> for StoredEvent {
    type Error = Error;

    fn try_from(row: EventRow) -> Result<Self> {
        let version = RoomVersionId::try_from(row.room_version.as_str())
            .map_err(|e| Error::Corrupt(format!("room version: {e}")))?;
        let rules = room_version::rules(&version)?;
        let event_id = ruma::OwnedEventId::try_from(row.event_id.as_str())
            .map_err(|e| Error::Corrupt(format!("event id: {e}")))?;
        let raw = tessera_core::canonical::from_str(&row.json)?;
        let pdu = Pdu::from_canonical_trusted(raw, &rules, event_id)?.with_rejected(row.rejected);
        Ok(StoredEvent {
            pdu,
            stream_ordering: row.stream_ordering,
            state_group: row.state_group,
            soft_failed: row.soft_failed,
            outlier: row.outlier,
            redacted: row.original_json.is_some(),
        })
    }
}

pub(crate) fn strings<I, S>(ids: I) -> Vec<String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    ids.into_iter().map(|s| s.as_ref().to_owned()).collect()
}
