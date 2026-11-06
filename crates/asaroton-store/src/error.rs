//! Storage errors.

/// Errors from the storage layer.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Database failure.
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),

    /// Migration failure.
    #[error("migration error: {0}")]
    Migrate(#[from] sqlx::migrate::MigrateError),

    /// A stored row could not be turned back into a protocol object.
    #[error("corrupt stored data: {0}")]
    Corrupt(String),

    /// Protocol-level error while decoding stored events.
    #[error(transparent)]
    Core(#[from] asaroton_core::Error),

    /// JSON failure.
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// The requested row does not exist.
    #[error("not found: {0}")]
    NotFound(&'static str),

    /// A uniqueness constraint was violated (user exists, alias taken, ...).
    #[error("already exists: {0}")]
    AlreadyExists(&'static str),
}

/// Result alias for the storage layer.
pub type Result<T, E = Error> = std::result::Result<T, E>;

impl Error {
    /// Whether the underlying error is a unique-constraint violation.
    pub fn is_unique_violation(&self) -> bool {
        match self {
            Self::Database(sqlx::Error::Database(db)) => db.is_unique_violation(),
            _ => false,
        }
    }
}
