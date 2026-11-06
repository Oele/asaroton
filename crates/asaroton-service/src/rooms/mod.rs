//! Rooms: the local event pipeline, room creation, membership and timeline access.

pub mod create;
pub mod making;
pub mod membership;
pub mod pipeline;
pub mod power;
pub mod relations;
pub mod search;
pub mod timeline;
pub mod upgrade;

pub use pipeline::LocalEvent;
