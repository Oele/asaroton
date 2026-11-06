# Architecture

asaroton is a Cargo workspace with one binary and six library crates. Dependencies point
strictly downward in this list; a crate never depends on one above it.

| Crate | Responsibility |
|---|---|
| `asaroton-core` | Pure protocol logic, no I/O: PDU formats per room version, canonical JSON, hashing, signing, redaction, auth rules, state resolution. Wraps `ruma`. |
| `asaroton-store` | The `Store` trait (domain-typed, async) and the PostgreSQL implementation (`sqlx`, migrations). |
| `asaroton-auth` | Access-token resolution: legacy tokens + UIA flows, and MSC3861 delegation to Matrix Authentication Service. |
| `asaroton-service` | Business logic: room membership and timeline, the event pipeline, sync streams (classic and Simplified Sliding Sync), E2EE key store, push, presence, typing, receipts, notifier. |
| `asaroton-federation` | Outbound: server key fetch/cache, request signing, per-destination transaction queue. Inbound: PDU/EDU processing that feeds the event pipeline. |
| `asaroton-api` | axum routers for the client-server, server-server, media, appservice, admin and well-known APIs. Thin handlers. |
| `asaroton` | Binary: config, CLI, startup wiring, TLS listeners, metrics, tracing. |

## The event pipeline

Every event, whether sent by a local client or received over federation, goes through one
path in `asaroton-service`:

1. Parse and shape-validate for the room's version.
2. Ensure `prev_events` and `auth_events` are known (fetch over federation if not).
3. Check the event against the auth rules using the state before it.
4. Compute the new current state (state resolution when there are multiple forward extremities).
5. Decide "soft fail" (passes auth against its own prev state but not against current state).
6. Persist atomically, assign stream positions.
7. Notify sync, federation sender, push and appservices.

## Stream positions

Everything that can appear in `/sync` carries a monotonically increasing stream id drawn
from a PostgreSQL sequence per stream (events, receipts, typing, presence, account data,
to-device, device lists). Sync tokens are a tuple of these positions.

## Storage

`asaroton-store` exposes a trait; code outside the crate never sees SQL. The first and
primary backend is PostgreSQL. Alternative backends (SQLite, RocksDB) can be added behind
the same trait.
