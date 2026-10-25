# Spec coverage

Target: Matrix spec v1.19, default room version 12 (all versions 1–12 supported for joining).
The conformance gate is Complement (`scripts/complement.sh`); the numbers below are from the
most recent runs noted in the commit history.

| Area | Status |
|---|---|
| Client-Server: versions / well-known / capabilities | done |
| Client-Server: registration / login / UIA / devices / profile | done (password + dummy flows, shared-secret admin registration; no 3PID) |
| Client-Server: rooms / membership / events | done: create, join (local and remote), leave, invite, kick, ban, forget, send, state, redact, messages (filters, history visibility, backfill over federation), context, aliases, directory, upgrades, reports |
| Client-Server: relations / threads / search | done (`/relations` with recursion, `/threads`, bundled aggregations, Postgres full-text search) |
| Client-Server: /sync (classic) | done: rooms, state, account data, receipts (threaded), typing, presence, to-device, device lists, unread and per-thread notification counts, ignored users, lazy loading |
| Client-Server: Simplified Sliding Sync (MSC4186) | not started |
| Client-Server: media | done: authenticated and legacy download/thumbnail, uploads (sync and async), URL previews, remote media over federation |
| Client-Server: E2EE | done: device keys, one-time/fallback keys, cross-signing, signatures, to-device, key backups |
| Client-Server: push | rules served and evaluated for unread counts; room-specific rules follow room upgrades; pusher HTTP delivery pending |
| Server-Server: keys / signing | done (server keys, notary, key fetch/cache, X-Matrix) |
| Server-Server: join / leave / invite / knock | join, leave, invite (incl. rejection and rescinding) done; knock pending |
| Server-Server: transactions (/send) | done: PDU pipeline with auth chains, missing events, state fetching, rejected events, EDUs (typing, receipts, presence, device lists, to-device, signing keys) |
| Server-Server: backfill / missing events | done both directions, with history-visibility redaction |
| Server-Server: media | done |
| Appservice API | not started |
| Admin API | not started (Synapse-compatible registration only) |
| Auth: MSC3861 delegation | not started |

Known gaps (Complement): `/members?at=` with a token before the room's first event, server
notices, knocking, appservices, sliding sync.
