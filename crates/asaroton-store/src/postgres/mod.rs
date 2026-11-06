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
mod rtc;
mod state;
mod streams;
mod thread_subscriptions;
mod users;

use std::time::Duration;

use asaroton_core::{Pdu, room_version};
use futures::future::BoxFuture;
use ruma::RoomVersionId;
use sqlx::{PgPool, postgres::PgPoolOptions};

use crate::{Error, Result, StoredEvent};

/// Embedded migrations, applied by [`PostgresStore::migrate`].
/// The schema migrations. A migration that has been applied anywhere is immutable: sqlx stores
/// a checksum of each file and refuses to start on a database where one differs, comments
/// included. `applied_migrations_are_unchanged` below pins those checksums.
pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

/// The PostgreSQL implementation of [`crate::Store`].
#[derive(Clone, Debug)]
pub struct PostgresStore {
    pool: PgPool,
    /// Keep the plain text of searchable events (substring search).
    search_plaintext: std::sync::Arc<std::sync::atomic::AtomicBool>,
    /// Which stream positions can be handed out (see [`streams`]); set up at first use.
    streams: std::sync::Arc<tokio::sync::OnceCell<streams::Streams>>,
}

impl PostgresStore {
    /// Connect to the database. Does not run migrations; call [`crate::Store::migrate`].
    pub async fn connect(url: &str, max_connections: u32) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .acquire_timeout(Duration::from_secs(30))
            .connect(url)
            .await?;
        Ok(Self {
            pool,
            search_plaintext: std::sync::Arc::default(),
            streams: std::sync::Arc::default(),
        })
    }

    /// Wrap an existing pool (used by tests).
    pub fn from_pool(pool: PgPool) -> Self {
        Self {
            pool,
            search_plaintext: std::sync::Arc::default(),
            streams: std::sync::Arc::default(),
        }
    }

    /// The underlying pool.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// The highest number in each stream, as the database has it right now. Not what a sync
    /// may be told: see [`streams`].
    async fn committed_positions(&self) -> Result<crate::StreamPositions> {
        use sqlx::Row;
        let row = sqlx::query(
            "SELECT (SELECT COALESCE(MAX(stream_ordering), 0) FROM events) AS events,
                    GREATEST((SELECT COALESCE(MAX(stream_id), 0) FROM account_data),
                             (SELECT COALESCE(MAX(stream_id), 0) FROM push_rules)) AS account_data,
                    (SELECT COALESCE(MAX(stream_id), 0) FROM receipts) AS receipts,
                    (SELECT COALESCE(MAX(stream_id), 0) FROM to_device_messages) AS to_device,
                    (SELECT COALESCE(MAX(stream_id), 0) FROM device_list_changes) AS device_lists,
                    (SELECT COALESCE(MAX(stream_id), 0) FROM thread_subscriptions) AS thread_subscriptions,
                    (SELECT COALESCE(MAX(stream_id), 0) FROM profile_updates) AS profile_updates",
        )
        .fetch_one(&self.pool)
        .await?;
        Ok(crate::StreamPositions {
            events: row.get("events"),
            account_data: row.get("account_data"),
            receipts: row.get("receipts"),
            to_device: row.get("to_device"),
            device_lists: row.get("device_lists"),
            thread_subscriptions: row.get("thread_subscriptions"),
            profile_updates: row.get("profile_updates"),
        })
    }

    /// The bookkeeping of writes under way, told at first use where each stream stands.
    ///
    /// Each stream starts from the last number its sequence gave out, not from the highest
    /// number among its rows. Rows get deleted (delivered to-device messages, a room that is
    /// removed), and a position that was handed to a client before a restart must not turn
    /// out to lie ahead of where the server thinks it is afterwards.
    pub(crate) async fn streams(&self) -> Result<&streams::Streams> {
        use sqlx::Row;

        use crate::SyncStream as Stream;
        self.streams
            .get_or_try_init(|| async {
                let rows = self.committed_positions().await?;
                let given_out = |sequence: &str| {
                    format!(
                        "(SELECT CASE WHEN is_called THEN last_value ELSE 0 END FROM {sequence})"
                    )
                };
                let sql = format!(
                    "SELECT {}, {}, {}, {}, {}, {}, {}",
                    given_out("events_stream_seq"),
                    given_out("account_data_seq"),
                    given_out("receipts_seq"),
                    given_out("to_device_seq"),
                    given_out("device_lists_seq"),
                    given_out("thread_subscriptions_seq"),
                    given_out("profile_updates_seq"),
                );
                let seq = sqlx::query(sqlx::AssertSqlSafe(sql))
                    .fetch_one(&self.pool)
                    .await?;
                let streams = streams::Streams::default();
                for (n, (stream, in_rows)) in [
                    (Stream::Events, rows.events),
                    (Stream::AccountData, rows.account_data),
                    (Stream::Receipts, rows.receipts),
                    (Stream::ToDevice, rows.to_device),
                    (Stream::DeviceLists, rows.device_lists),
                    (Stream::ThreadSubscriptions, rows.thread_subscriptions),
                    (Stream::ProfileUpdates, rows.profile_updates),
                ]
                .into_iter()
                .enumerate()
                {
                    streams.seed(stream, in_rows.max(seq.get::<i64, _>(n)));
                }
                Ok(streams)
            })
            .await
    }

    /// Writes one row of a sync stream. `statement` runs the statement on the connection it
    /// is given and returns the number the row got, or `None` when it wrote no row.
    ///
    /// The write is registered for as long as it is under way (see [`streams`]), and it runs
    /// in a task of its own: a request that is dropped while its statement is with the
    /// database would end the registration, and the row would then appear below a position
    /// that had been reported in the meantime.
    pub(crate) async fn write_to_stream<F>(
        &self,
        stream: crate::SyncStream,
        statement: F,
    ) -> Result<Option<i64>>
    where
        F: for<'c> FnOnce(&'c mut sqlx::PgConnection) -> BoxFuture<'c, Result<Option<i64>>>
            + Send
            + 'static,
    {
        let store = self.clone();
        tokio::spawn(async move {
            // The connection first: waiting for one is no part of the write.
            let mut conn = store.pool.acquire().await?;
            let mut writing = store.streams().await?.writing(stream);
            let position = statement(&mut conn).await?;
            if let Some(position) = position {
                writing.wrote(position);
            }
            Ok(position)
        })
        .await
        .map_err(|e| Error::Interrupted(e.to_string()))?
    }

    /// The position in each stream that a sync can be told: the highest one below which no
    /// write is still under way.
    pub(crate) async fn settled_positions(&self) -> Result<crate::StreamPositions> {
        use crate::SyncStream as Stream;
        // The numbers first, the writes under way second: in that order, every row up to the
        // answer is visible to what the caller reads next.
        let at = self.committed_positions().await?;
        let streams = self.streams().await?;
        Ok(crate::StreamPositions {
            events: streams.settled(Stream::Events, at.events),
            account_data: streams.settled(Stream::AccountData, at.account_data),
            receipts: streams.settled(Stream::Receipts, at.receipts),
            to_device: streams.settled(Stream::ToDevice, at.to_device),
            device_lists: streams.settled(Stream::DeviceLists, at.device_lists),
            thread_subscriptions: streams
                .settled(Stream::ThreadSubscriptions, at.thread_subscriptions),
            profile_updates: streams.settled(Stream::ProfileUpdates, at.profile_updates),
        })
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
        let raw = asaroton_core::canonical::from_str(&row.json)?;
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

#[cfg(test)]
mod tests {
    use std::fmt::Write as _;

    use super::MIGRATOR;

    /// Changing an applied migration, even a comment in it, makes the server refuse to start
    /// on every existing database ("migration N was previously applied but has been
    /// modified"). A fresh database does not notice, so no other test and no Complement run
    /// does either: the rename from tessera rewrote a comment in two of these files and was
    /// only caught, and rolled back, on the first live host. The old name stays in those
    /// comments for good. Add a line here for each new migration; never change an existing one.
    #[test]
    fn applied_migrations_are_unchanged() {
        let expected: &[(i64, &str)] = &[
            (
                1,
                "8f594feb661cf7c470201ee22db5ef05d75e6435806679b10f7059ecc8e5b16dda8a6806cb41c70bdb500a0b8c571f25",
            ),
            (
                2,
                "3911e4aaa1d62cf182c63294541c68d135340fa8e8a82e462eeffe5e2ff040dc68a43bc726d0c83b5fa33ccbfaecca99",
            ),
            (
                3,
                "18bf7fc07aadb38028eab1217bea34297f35aa5f110b4e35741f877ea6e72a990d47ac73fd3dcf8ff2afbd4bfd06369e",
            ),
            (
                4,
                "42ebcea848cadd992b897200663b78a9c81d1e9f899d9c9df6947f1d761c1edc0b65527fce002117ab97c1ce5bfa21e9",
            ),
            (
                5,
                "f5c2906cf05daa8e38c4e339445309ebd645534a585f06adc8854610cbc4c566dc99ab5021cc00aed35e248911f0474b",
            ),
            (
                6,
                "bb9de232eec960f3aba0c926fa34fb9c4ec4b1ee921e2fa9080b18267c37b3fdb367ac1d0c579a2e2c4aca63936725f1",
            ),
            (
                7,
                "0a1c7e4fda955a93bdc6881c1628d16fc46f11e1b2e1c984f03d5d5ddf54d4b35ab5421de8159c4c942bff2055a3192b",
            ),
            (
                8,
                "8f9e133ca7eaa034bc4c59db0b50f04e3d3c4c85b7bfad9b958e017491ce06c9ee56b8d23692e93e1ecdc156a62b4d58",
            ),
            (
                9,
                "9b61ab45039609069f7322f05a6ea944a295ce395783d5a4d17b09bd77c139473babb72e30c77e22c71de5d23e839c0e",
            ),
            (
                10,
                "5cfb1cd682ae2a40e676ecc1cb92d70f1943791bddbd465dcfb1910c21eb4bd082cd12cf971cc80de3a5c8c0c5ac270a",
            ),
            (
                11,
                "0d76d2fce542f0e515ce98de5fd84c4181d4c20d92cf7bc065c0b6236633115fb35d1a80f4bbf3fd5b12426d587627cf",
            ),
            (
                12,
                "f09838405e3e2410220bbffa22fd52230ca1e3943b4476150cebae7635b3165cc1ae227422ea2524518ff1257bad9f02",
            ),
            (
                13,
                "95652d4cf1abe73fa295d4c2eddcd9869a77ae8a68213372b4830e424e62fba21903f4df4a8000e3d079ece441c80d09",
            ),
            (
                14,
                "af882e8f26d2fa155401980d494f04376ea258842b99c899eec014133c640ed56edb84efec252c24b43b5db078c8f7a3",
            ),
            (
                15,
                "934ac66b71d597f7dec5e26c863ffa3379e7d797203d02c8985ce2a2a9e251502facef9913a2295e9a2e7f1fa5819f64",
            ),
        ];
        let actual: Vec<(i64, String)> = MIGRATOR
            .iter()
            .map(|m| {
                let hex = m.checksum.iter().fold(String::new(), |mut s, b| {
                    let _ = write!(s, "{b:02x}");
                    s
                });
                (m.version, hex)
            })
            .collect();
        for (version, checksum) in expected {
            let found = actual.iter().find(|(v, _)| v == version);
            assert_eq!(
                found.map(|(_, c)| c.as_str()),
                Some(*checksum),
                "migration {version} differs from the one existing databases have applied"
            );
        }
        assert_eq!(
            actual.len(),
            expected.len(),
            "a new migration needs its checksum added to this test"
        );
    }
}
