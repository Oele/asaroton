# Migrating a Synapse homeserver to tessera

Plan for replacing the Synapse installation serving `cluephone.org` with tessera, keeping the
server name, every user, room, message and session. Written 2026-10-29; the open questions at the
end decide some details.

## Principles

- The server name stays `cluephone.org`. User IDs, room IDs and memberships on other servers
  reference it; changing it would mean a new server with no history.
- The signing key moves with the name. Events Synapse signed stay verifiable when tessera serves
  the same key (imported from Synapse's `signing.key`); any keys Synapse already retired go into
  `old_verify_keys`.
- Everything that other servers or clients remember about `cluephone.org` keeps working:
  access tokens and devices (sessions survive), E2EE device and cross-signing keys (no
  re-verification), stream ids remote servers track (device lists), pushers (notifications keep
  arriving), room aliases, media URLs.
- The import reads Synapse's PostgreSQL database directly and writes tessera's schema. There is
  no protocol-level path for this: a server cannot re-fetch its own history from the network
  under the same name.
- Rehearse on a copy until the import is boring, then cut over in one maintenance window with
  Synapse kept intact for rollback.

## What moves, and from where

| Data | Synapse source | tessera target | Notes |
|---|---|---|---|
| Users | `users`, `profiles`, `user_filters` | `users`, `filters` | Password hashes are bcrypt (Synapse) with an optional pepper; tessera stores argon2. tessera will verify `$2b$` hashes (with the pepper from the Synapse config) and re-hash on next login. |
| Devices, sessions | `devices`, `access_tokens`, `refresh_tokens` | same | Tokens are copied verbatim so no client logs out. `valid_until_ms` maps to `expires_ts`. |
| E2EE | `e2e_device_keys_json`, `e2e_one_time_keys_json`, `e2e_fallback_keys_json`, `e2e_cross_signing_keys`, `e2e_cross_signing_signatures`, `e2e_room_keys`, `e2e_room_keys_versions` | `device_keys`, `one_time_keys`, `fallback_keys`, `cross_signing_keys`, `key_signatures`, `key_backup_*` | Including the key backup. |
| Pending to-device messages | `device_inbox` | `to_device_messages` | Room keys sent to offline devices; losing them means undecryptable messages. |
| Device-list stream | `device_lists_stream`, `device_lists_outbound_pokes` | `device_list_changes` + the `device_lists_seq` sequence | Remote servers keep the last stream id per user; tessera's sequence starts above Synapse's maximum so updates are not ignored. Outbound pokes must be drained (queue empty) before the final export. |
| Account data | `account_data`, `room_account_data`, `room_tags`, `push_rules`, `push_rules_enable`, `push_rules_stream` | `account_data`, `push_rules` | Tags become `m.tag` room account data. Synapse keeps one row per push rule; tessera one ruleset per user: defaults + user rules + enabled overrides, in Synapse's priority order. |
| Pushers | `pushers` | `pushers` | Verbatim (app id, pushkey, gateway URL, data). |
| Rooms | `rooms`, `room_aliases`, `room_alias_servers`, `room_stats_state`, `blocked_rooms` | `rooms`, `room_aliases` | Room version, creator, publicity, aliases. |
| Events | `events`, `event_json`, `rejections`, `event_edges`, `event_auth`, `redactions` | `events`, `event_edges`, `event_auth` | Same conventions: positive `stream_ordering` live, negative backfilled; `outlier`; rejected rows kept with `rejected = true`; `soft_failed` from `internal_metadata`. `event_json.json` is the signed federation JSON tessera stores. |
| State | `state_groups`, `state_group_edges`, `state_groups_state`, `event_to_state_groups`, `current_state_events`, `event_forward_extremities` | `state_groups`, `state_groups_state`, `events.state_group`, `current_state_events`, `forward_extremities` | Synapse's groups are deltas over a previous group, exactly tessera's model; ids are mapped 1:1. |
| Memberships | `current_state_events` (member rows), `local_current_membership`, `room_memberships` | `room_memberships`, `current_state_events.membership` | Forgotten rooms from `room_memberships.forgotten`. |
| Receipts, read markers | `receipts_linearized` | `receipts` | Thread id from the `data` column; one stream id per row. |
| Relations, threads, search | `event_relations`, `threads`, `event_search` | rebuilt | Derived from the event JSON by tessera's own indexer after the import (`search-backfill` plus a relations pass). |
| Media | `local_media_repository`, `local_media_repository_thumbnails`, files under `media_store_path/local_content/<xx>/<yy>/<rest>` | `media` table, files under `data_dir/media/<server>/<id>` | Files are copied and renamed; thumbnails are regenerated on demand. Remote media cache is dropped (refetched when needed). Quarantine flags carry over. |
| Server keys | `signing.key` file, `old_signing_keys` config | `server_signing_keys` | Same key id and seed. |
| Appservices | registration YAML files, `appservice_stream_position` | config `appservices`, `appservice_positions` | Bridges keep their tokens; their stream position is set to the imported maximum so nothing is replayed. |
| Registration tokens, server notices room, retention policies | `registration_tokens`, config, `room_retention` | config / tables | Small; by hand or in the importer. |

Not migrated: `user_threepids` and email settings (tessera has no 3PID support; the addresses can
be kept in a side table for later), `user_ips` and other statistics, `received_transactions`
(idempotency cache; harmless to lose), `destinations` retry state, `remote_media_cache`,
`user_directory` (rebuilt), `event_push_actions` and unread caches (recomputed), UIA sessions.

## Work in tessera before the first rehearsal

1. `tessera import-synapse --from postgres://.../synapse --media /path/to/media_store`
   subcommand: streams each table above in `stream_ordering` order, batched, into an empty
   tessera database, then sets every sequence past the imported maxima. It must be restartable
   per table and print counts.
2. bcrypt verification with pepper in `tessera-auth`, re-hashing to argon2 on successful login.
3. `old_verify_keys` in `/_matrix/key/v2/server` from a config list, and an importer step for the
   current key.
4. Sync tokens in Synapse's format (`s123_456_...`) are treated as "no token": clients that carry
   one get an initial sync instead of an error. Element then rebuilds its timeline; nothing is
   lost because device keys and sessions survive.
5. Push rules conversion (per-rule rows to a ruleset) and `m.tag` conversion.
6. A relations/threads rebuild pass next to the existing `search-backfill`.
7. A comparison script: per room, event count, current state tuples, joined members, latest
   event id in Synapse versus tessera; per user, device count and token count; media count. The
   rehearsal is done when it reports no differences.

## Rehearsal

1. `pg_dump` of the Synapse database restored on this machine as `synapse_copy`; a copy of
   the media store (rsync, resumable).
2. Import into `tessera_cluephone_rehearsal`; record the wall time (it sets the length of the
   maintenance window).
3. Start tessera on a spare port with `server_name = "cluephone.org"`, `federation.disabled =
   true` (so the rehearsal never talks to real servers under the real name) and the imported
   signing key.
4. Checks: log in with an existing password (bcrypt path), log in with an imported access token,
   `/sync` for several users, scroll history to the start of a room, `/search`, download media,
   open an encrypted room with a session whose keys were imported, key backup listing, push rules
   as the client shows them, the comparison script.
5. Fix, re-import, repeat. Expect a few rounds: the first import surfaces events or rooms Synapse
   accepted under rules tessera applies differently.

## Cutover

Preconditions: rehearsal passes; a Complement-style smoke test exists for the imported data; the
Apache vhost, certificate and `.well-known` for `cluephone.org` are prepared on the new host (the
current `.well-known/matrix/server` target decides what must change: ideally only where the host
name points).

1. Announce a window to users and bridge operators; a federation outage of the window's length is
   invisible to other servers (they retry) as long as it is shorter than their retry limit (hours).
2. Stop Synapse's client access first (reverse proxy returns 503), wait until its outbound queues
   are empty (`device_lists_outbound_pokes`, federation destinations), then stop Synapse.
3. Final `pg_dump`, final media rsync.
4. Import into the production database; run the comparison script.
5. Point the host name at tessera (DNS or reverse proxy), start tessera, confirm
   `federationtester.matrix.org`, send and receive a message with a user on another server, log in
   from a fresh client, check push arrives, check a bridge works.
6. Keep Synapse and its database untouched for the rollback period (a week): rollback is pointing
   the host name back and starting Synapse; events tessera accepted in the meantime would be lost
   for local users, so rollback is only for serious breakage in the first days.

## Risks and how the plan handles them

- Rooms Synapse holds in partial state (`partial_state_rooms` non-empty) cannot be imported
  consistently; resolve them in Synapse first (wait for the resync).
- Events in old room versions with quirks (v1/v2 event ids, redacted event JSON, events missing
  from `event_json`): the importer logs and skips what it cannot parse; the comparison script shows
  the gaps so they can be judged.
- Password pepper: without the value from Synapse's config no password works; it is in
  `homeserver.yaml` under `password_config.pepper`.
- OIDC or SAML logins (if Synapse uses them) need tessera's MAS mode instead of passwords;
  that changes the auth part of the plan.
- Email: tessera sends none; password reset by email and email notifications stop. Admins
  reset passwords through the admin API instead.
- Database size decides the window: the issue measured 1.4M events; the import is a streaming
  copy, so expect minutes rather than hours, the media copy dominates.
- Clients see an initial sync after cutover (Synapse tokens are not understood); Element handles
  it, but it costs bandwidth on the first connect.
- Anything that pokes Synapse's admin API (scripts, dashboards) must be checked against
  tessera's `/_synapse/admin` subset.

## Open questions

1. Where does Synapse run (same machine, another host), which version, and what are the database
   and media store sizes?
2. Password logins only, or OIDC/SAML as well?
3. Which application services (bridges) and whether they must keep running during the window?
4. Are 3PIDs (email addresses) and email notifications in use, and may they be dropped?
5. Does anything use the Synapse admin API or metrics?
6. Is `cluephone.org`'s `.well-known/matrix/server` delegation under your control, and does the
   reverse proxy for it live on the Synapse host?
