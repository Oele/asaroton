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

## Requests that are cut off

A client can hang up in the middle of a request, and the HTTP server then stops working on
that request wherever it is. For a request that reads, that is what should happen. A request
that writes is a row of steps that belong together (store the event, move the room on to
it, wake those who wait, send it on, note the transaction ID it answers), so the router runs
every request that is not a `GET` in a task of its own: the connection waits for it and does
not take it down. Sliding sync is the exception, a `POST` that waits for news and has to end
with its client. A client that tries again while its first attempt is still running waits
its turn where transaction IDs are compared, and gets the first attempt's answer; so does
another server that sends a transaction again.

What can still be dropped anywhere (every `GET`, sliding sync) has to leave the database
as it found it. Where a reading request writes more than one statement, which is when it
fetches history from other servers, that part runs in a task of its own as well.
Transactions roll back when they are dropped, with one hole that the store closes itself:
sqlx 0.9.0 does not roll back a transaction that is dropped while `BEGIN` is on its way, so
`PostgresStore::begin` begins every transaction in a task that is not dropped with its
caller. `scripts/droppedrequesttest.py` cuts a few thousand requests off and checks what is
left.

Not covered: the server stopping. A restart ends every request where it is, and an event
that was stored a moment before may be left without the note of its transaction ID.

## Stream positions

Everything that can appear in `/sync` carries a monotonically increasing stream id drawn
from a PostgreSQL sequence per stream (events, receipts, typing, presence, account data,
to-device, device lists). Sync tokens are a tuple of these positions.

## Storage

`asaroton-store` exposes a trait; code outside the crate never sees SQL. The first and
primary backend is PostgreSQL. Alternative backends (SQLite, RocksDB) can be added behind
the same trait.
