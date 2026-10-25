# Spec coverage

Target: Matrix spec v1.19, default room version 12 (all versions 1–12 supported for joining).
The conformance gate is Complement (`scripts/complement.sh`); the numbers below are from the
most recent runs noted in the commit history.

| Area | Status |
|---|---|
| Client-Server: versions / well-known / capabilities | done |
| Client-Server: registration / login / UIA / devices / profile | done (password + dummy flows, shared-secret admin registration; no 3PID) |
| Client-Server: rooms / membership / events | done: create, join (local and remote), knock, leave, invite, kick, ban, forget, send, state, redact, messages (filters, history visibility, backfill over federation), context, timestamp_to_event, aliases, directory, upgrades, reports, room summary |
| Client-Server: relations / threads / search / spaces | done (`/relations` with recursion, `/threads`, bundled aggregations, Postgres full-text search, `/hierarchy` incl. remote children) |
| Client-Server: /sync (classic) | done: rooms, state, account data, receipts (threaded), typing, presence, to-device, device lists, unread and per-thread notification counts, ignored users, lazy loading |
| Client-Server: Simplified Sliding Sync (MSC4186) | done: lists with ranges and filters, room subscriptions, per-connection deltas with position retry, `required_state` wildcards (`*`, `$ME`, `$LAZY`), to-device, e2ee, account data, receipts and typing extensions; thread subscriptions, profiles and sticky events extensions not implemented |
| Client-Server: media | done: authenticated and legacy download/thumbnail, uploads (sync and async), URL previews, remote media over federation |
| Client-Server: E2EE | done: device keys, one-time/fallback keys, cross-signing, signatures, to-device, key backups |
| Client-Server: push | done: rules served and evaluated for unread counts, room-specific rules follow room upgrades, HTTP pushers receive notifications (full and `event_id_only`, rejected pushkeys removed) |
| Server-Server: keys / signing | done (server keys, notary, key fetch/cache, X-Matrix) |
| Server-Server: join / leave / invite / knock | done, including restricted joins, invite rejection and rescinding, make_knock/send_knock, server ACLs, hierarchy |
| Server-Server: transactions (/send) | done: PDU pipeline with auth chains, missing events, state fetching, rejected events, EDUs (typing, receipts, presence, device lists, to-device, signing keys) |
| Server-Server: backfill / missing events | done both directions, with history-visibility redaction |
| Server-Server: media | done |
| Appservice API | done: registrations (YAML), namespaces, `as_token` auth with `user_id`, transactions, user/alias queries, ping, third-party protocol/location/user lookups |
| Admin API | Synapse-compatible subset: shared-secret registration, server notices, `server_version`, users (list, get, create/update, deactivate, reset password, admin flag, devices, joined rooms), rooms (list, details, members, state, delete, force join), history purge |
| Auth: MSC3861 delegation | done: `[mas]` config delegates token validation to Matrix Authentication Service (introspection with the shared secret, 2-minute cache, device scope handling), the `/_synapse/mas/*` management API MAS provisions users and devices through, `/auth_metadata` and MSC2965 well-known discovery, login/registration/password endpoints disabled, cross-signing resets gated on MAS approval. Tested against MAS 1.27 with compatibility tokens |
| Operations | rate limiting (per address and per token buckets, `M_LIMIT_EXCEEDED`), message retention (`m.room.retention` and a server default), fuzz targets for canonical JSON, PDU parsing and redaction (`scripts/fuzz.sh`), load-test swarm (`scripts/loadtest.py`), release Dockerfile and compose file (`docs/deployment.md`) |

Known gaps (Complement): `/members?at=` with a token before the room's first event,
state-resolution ordering of a v12 creator versus a >100 admin (ruma state-res), ephemeral events
to appservices.
