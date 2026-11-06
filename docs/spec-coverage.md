# Spec coverage

Target: Matrix spec v1.19, default room version 12 (all versions 1–12 supported for joining).
The conformance gate is Complement (`scripts/complement.sh`). The tests that fail by decision
are listed with their reasons in `tests/complement/expected-failures.txt` and skipped, so a
clean run exits 0 and every failure it reports is a real one; `COMPLEMENT_ALL=1` runs
everything. Last full run (2026-11-06): 206 top-level tests pass and none fail; the 13
expected failures (12 tests and one subtest) are skipped: 10 for features that are not
implemented, 3 deliberate deviations (see the end of this page).

| Area | Status |
|---|---|
| Client-Server: versions / well-known / capabilities | done |
| Client-Server: registration / login / UIA / devices / profile | done (password + dummy flows, shared-secret admin registration, custom profile fields incl. `m.tz`, appservice device masquerading and device creation; no 3PID) |
| Client-Server: rooms / membership / events | done: room tags (`/user/{userId}/rooms/{roomId}/tags`, stored as `m.tag` room account data), `/mutual_rooms`, redactions through `/send`, create, join (local and remote), knock, leave, invite, kick, ban, forget, send, state, redact, messages (filters, history visibility, backfill over federation), context, timestamp_to_event, aliases, directory, upgrades, reports, room summary |
| Client-Server: relations / threads / search / spaces | done (`/relations` with recursion, `/threads`, bundled aggregations, Postgres full-text search, `/hierarchy` incl. remote children) |
| Client-Server: /sync (classic) | done: timelines filtered by history visibility, left rooms frozen at the leave (no state at all after a declined invite or withdrawn knock), rooms, state (and `state_after` with `use_state_after`), account data, receipts (threaded), typing, presence, to-device, device lists, unread and per-thread notification counts, ignored users, lazy loading, topological initial timelines |
| Client-Server: Simplified Sliding Sync (MSC4186) | done: invites and knocks as stripped state only, left rooms frozen at the leave, timelines filtered by history visibility, lists with ranges and filters, room subscriptions, per-connection deltas with position retry, `required_state` wildcards (`*`, `$ME`, `$LAZY`), to-device, e2ee, account data, receipts and typing extensions; thread subscriptions, profiles and sticky events extensions not implemented |
| Client-Server: media | done: authenticated and legacy download/thumbnail, uploads (sync and async), URL previews, remote media over federation |
| Client-Server: E2EE | done: device keys, one-time/fallback keys, cross-signing, signatures, to-device, key backups |
| Client-Server: push | done: rules served and evaluated for unread counts, room-specific rules follow room upgrades, HTTP pushers receive notifications (full and `event_id_only`, rejected pushkeys removed) |
| Server-Server: keys / signing | done (server keys, notary, key fetch/cache, X-Matrix) |
| Server-Server: join / leave / invite / knock | done, including restricted joins, invite rejection and rescinding, make_knock/send_knock, server ACLs, hierarchy |
| Server-Server: transactions (/send) | done: PDU pipeline with auth chains, missing events, state fetching, rejected events, EDUs (typing, receipts, presence, device lists, to-device, signing keys) |
| Server-Server: backfill / missing events | done both directions, with history-visibility redaction |
| Server-Server: media | done |
| Appservice API | done: registrations (YAML), namespaces, `as_token` auth with `user_id` and `device_id`, transactions incl. ephemeral data (`receive_ephemeral`: typing, receipts, presence), user/alias queries, ping, third-party protocol/location/user lookups, device management for appservice users |
| Admin API | Synapse-compatible subset: shared-secret registration, server notices, `server_version`, users (list, get, create/update, deactivate, reset password, admin flag, devices, joined rooms), rooms (list, details, members, state, delete, force join), history purge |
| Auth: MSC3861 delegation | done: `[mas]` config delegates token validation to Matrix Authentication Service (introspection with the shared secret, 2-minute cache, device scope handling), the `/_synapse/mas/*` management API MAS provisions users and devices through, `/auth_metadata` and MSC2965 well-known discovery, login/registration/password endpoints disabled, cross-signing resets gated on MAS approval. Tested against MAS 1.27 with compatibility tokens |
| Unstable MSCs (Complement `msc*` packages, all passing) | profile updates in classic `/sync` (MSC4429: `profile_fields` in the filter, `users` in the response, under their unstable names), account-data deletion (MSC3391: `DELETE` endpoints and `PUT {}`; deletions appear as empty events in incremental syncs), relation-type filters on `/messages` (MSC3874), push rules for polls (MSC3930, `event_match` form), idempotent cross-signing uploads without UIA (MSC3967), device notification settings removed with the device (MSC3890), invite filtering (MSC4155: `org.matrix.msc4155.invite_permission_config`, blocked invites fail with `ORG.MATRIX.MSC4155.M_INVITE_BLOCKED`, ignored ones succeed silently, also over federation), delayed events (MSC4140: `?org.matrix.msc4140.delay=` on send/state, the `delayed_event` endpoint, listing, cancel/restart/send, state changes cancel pending delayed state, survives restarts), thread subscriptions (MSC4306 endpoints with automatic-subscription conflicts, MSC4308 sliding-sync extension and `/thread_subscriptions` paging). The MSC4306 push rules are not in the default ruleset: they would silence every unsubscribed thread for clients that cannot subscribe |
| Search | `/search` with full-text (`word`), substring (`pg_trgm`) or both; see docs/deployment.md |
| Calls: MatrixRTC over LiveKit (MSC4143, MSC4195) | done: `/rtc/transports` (stable and `org.matrix.msc4143` paths, `org.matrix.msc4143` in `unstable_features`), `rtc_foci` in the client well-known, `get_token` for local and remote SFUs (room membership checks, MSC-conformant room names and participant identities, LiveKit rooms created on demand), the federation `get_token` endpoint both ways, `delegate_delayed_leave` driven by SFU webhooks and polling, participants removed from the SFU on leave/kick/ban, the legacy `lk-jwt-service` endpoints (`/sfu/get`, OpenID-token `get_token` and `delegate_delayed_leave`) at the same prefix, OpenID `request_token` and federation `openid/userinfo`; configured with `[rtc.livekit]` |
| Operations | rate limiting (per address and per token buckets, `M_LIMIT_EXCEEDED`), message retention (`m.room.retention` and a server default), fuzz targets for canonical JSON, PDU parsing and redaction (`scripts/fuzz.sh`), load-test swarm (`scripts/loadtest.py`), release Dockerfile and compose file (`docs/deployment.md`) |

Known deviations (Complement): UIA accepts an `auth` dict without a session like Synapse does
(TestRegistration's strictness subtest, skipped for Synapse too); TestIsDirectFlagFederation sends
a room-version-12 invite without `m.room.create` in `invite_room_state`, which v1.16 (MSC4311)
requires receivers to reject (it passes on servers that still default to room version 11);
TestDeviceListsUpdateOverFederationOnRoomJoin is skipped for Synapse as well. Experimental Complement packages
still not targeted: faster joins (MSC3902), owned state (MSC3757, needs the `org.matrix.msc3757.10`
room version), MSC2836, MSC3389, MSC4242.
