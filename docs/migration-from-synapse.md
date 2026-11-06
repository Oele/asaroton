# Migrating a Synapse homeserver to asaroton

Plan for replacing the Synapse installation serving `cluephone.org` with asaroton, keeping the
server name, every user, room, message and session. Written 2026-10-29; the open questions at the
end decide some details.

## Principles

- The server name stays `cluephone.org`. User IDs, room IDs and memberships on other servers
  reference it; changing it would mean a new server with no history.
- The signing key moves with the name. Events Synapse signed stay verifiable when asaroton serves
  the same key (imported from Synapse's `signing.key`); any keys Synapse already retired go into
  `old_verify_keys`.
- Everything that other servers or clients remember about `cluephone.org` keeps working:
  access tokens and devices (sessions survive), E2EE device and cross-signing keys (no
  re-verification), stream ids remote servers track (device lists), pushers (notifications keep
  arriving), room aliases, media URLs.
- The import reads Synapse's PostgreSQL database directly and writes asaroton's schema. There is
  no protocol-level path for this: a server cannot re-fetch its own history from the network
  under the same name.
- Rehearse on a copy until the import is boring, then cut over in one maintenance window with
  Synapse kept intact for rollback.

## What moves, and from where

| Data | Synapse source | asaroton target | Notes |
|---|---|---|---|
| Users | `users`, `profiles`, `user_filters` | `users`, `filters` | Password hashes are bcrypt (Synapse) with an optional pepper; asaroton stores argon2. asaroton will verify `$2b$` hashes (with the pepper from the Synapse config) and re-hash on next login. |
| Devices, sessions | `devices`, `access_tokens`, `refresh_tokens` | same | Tokens are copied verbatim so no client logs out. `valid_until_ms` maps to `expires_ts`. |
| E2EE | `e2e_device_keys_json`, `e2e_one_time_keys_json`, `e2e_fallback_keys_json`, `e2e_cross_signing_keys`, `e2e_cross_signing_signatures`, `e2e_room_keys`, `e2e_room_keys_versions` | `device_keys`, `one_time_keys`, `fallback_keys`, `cross_signing_keys`, `key_signatures`, `key_backup_*` | Including the key backup. |
| Pending to-device messages | `device_inbox` | `to_device_messages` | Room keys sent to offline devices; losing them means undecryptable messages. |
| Device-list stream | `device_lists_stream`, `device_lists_outbound_pokes` | `device_list_changes` + the `device_lists_seq` sequence | Remote servers keep the last stream id per user; asaroton's sequence starts above Synapse's maximum so updates are not ignored. Outbound pokes must be drained (queue empty) before the final export. |
| Account data | `account_data`, `room_account_data`, `room_tags`, `push_rules`, `push_rules_enable`, `push_rules_stream` | `account_data`, `push_rules` | Tags become `m.tag` room account data. Synapse keeps one row per push rule; asaroton one ruleset per user: defaults + user rules + enabled overrides, in Synapse's priority order. |
| Pushers | `pushers` | `pushers` | Verbatim (app id, pushkey, gateway URL, data). |
| Rooms | `rooms`, `room_aliases`, `room_alias_servers`, `room_stats_state`, `blocked_rooms` | `rooms`, `room_aliases` | Room version, creator, publicity, aliases. |
| Events | `events`, `event_json`, `rejections`, `event_edges`, `event_auth`, `redactions` | `events`, `event_edges`, `event_auth` | Same conventions: positive `stream_ordering` live, negative backfilled; `outlier`; rejected rows kept with `rejected = true`; `soft_failed` from `internal_metadata`. `event_json.json` is the signed federation JSON asaroton stores. |
| State | `state_groups`, `state_group_edges`, `state_groups_state`, `event_to_state_groups`, `current_state_events`, `event_forward_extremities` | `state_groups`, `state_groups_state`, `events.state_group`, `current_state_events`, `forward_extremities` | Synapse's groups are deltas over a previous group, exactly asaroton's model; ids are mapped 1:1. |
| Memberships | `current_state_events` (member rows), `local_current_membership`, `room_memberships` | `room_memberships`, `current_state_events.membership` | Forgotten rooms from `room_memberships.forgotten`. |
| Receipts, read markers | `receipts_linearized` | `receipts` | Thread id from the `data` column; one stream id per row. |
| Relations, threads, search | `event_relations`, `threads`, `event_search` | rebuilt | Derived from the event JSON by asaroton's own indexer after the import (`search-backfill` plus a relations pass). |
| Media | `local_media_repository`, `local_media_repository_thumbnails`, files under `media_store_path/local_content/<xx>/<yy>/<rest>` | `media` table, files under `data_dir/media/<server>/<id>` | Files are copied and renamed; thumbnails are regenerated on demand. Remote media cache is dropped (refetched when needed). Quarantine flags carry over. |
| Server keys | `signing.key` file, `old_signing_keys` config | `server_signing_keys` | Same key id and seed. |
| Other servers' keys | `server_keys_json` | `remote_server_keys` | The signed key documents Synapse fetched over the years, newest per key. Every imported event was verified with one of these keys, and many can no longer be fetched from their servers. `import-synapse --only-server-keys` copies just this table into a database that is already in use (keys it holds are left alone), for a server migrated before the importer did this. |
| Appservices | registration YAML files, `appservice_stream_position` | config `appservices`, `appservice_positions` | Bridges keep their tokens; their stream position is set to the imported maximum so nothing is replayed. |
| Registration tokens, server notices room, retention policies | `registration_tokens`, config, `room_retention` | config / tables | Small; by hand or in the importer. |

Not migrated: `user_threepids` and email settings (asaroton has no 3PID support; the addresses can
be kept in a side table for later), `user_ips` and other statistics, `received_transactions`
(idempotency cache; harmless to lose), `destinations` retry state, `remote_media_cache`,
`user_directory` (rebuilt), `event_push_actions` and unread caches (recomputed), UIA sessions.

## Work in asaroton before the first rehearsal

1. `asaroton import-synapse --from postgres://.../synapse --media /path/to/media_store`
   subcommand: streams each table above in `stream_ordering` order, batched, into an empty
   asaroton database, then sets every sequence past the imported maxima. It must be restartable
   per table and print counts.
2. bcrypt verification with pepper in `asaroton-auth`, re-hashing to argon2 on successful login.
3. `old_verify_keys` in `/_matrix/key/v2/server` from a config list, and an importer step for the
   current key.
4. Sync tokens in Synapse's format (`s123_456_...`) are treated as "no token": clients that carry
   one get an initial sync instead of an error. Element then rebuilds its timeline; nothing is
   lost because device keys and sessions survive.
5. Push rules conversion (per-rule rows to a ruleset) and `m.tag` conversion.
6. A relations/threads rebuild pass next to the existing `search-backfill`.
7. A comparison script: per room, event count, current state tuples, joined members, latest
   event id in Synapse versus asaroton; per user, device count and token count; media count. The
   rehearsal is done when it reports no differences.

## Rehearsal

1. `pg_dump` of the Synapse database restored on this machine as `synapse_copy`; a copy of
   the media store (rsync, resumable).
2. Import into `asaroton_cluephone_rehearsal`; record the wall time (it sets the length of the
   maintenance window).
3. Start asaroton on a spare port with `server_name = "cluephone.org"`, `federation.disabled =
   true` (so the rehearsal never talks to real servers under the real name) and the imported
   signing key.
4. Checks: log in with an existing password (bcrypt path), log in with an imported access token,
   `/sync` for several users, scroll history to the start of a room, `/search`, download media,
   open an encrypted room with a session whose keys were imported, key backup listing, push rules
   as the client shows them, the comparison script.
5. Fix, re-import, repeat. Expect a few rounds: the first import surfaces events or rooms Synapse
   accepted under rules asaroton applies differently.

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
5. Point the host name at asaroton (DNS or reverse proxy), start asaroton, confirm
   `federationtester.matrix.org`, send and receive a message with a user on another server, log in
   from a fresh client, check push arrives, check a bridge works.
6. Keep Synapse and its database untouched for the rollback period (a week): rollback is pointing
   the host name back and starting Synapse; events asaroton accepted in the meantime would be lost
   for local users, so rollback is only for serious breakage in the first days.

## Risks and how the plan handles them

- Rooms Synapse holds in partial state (`partial_state_rooms` non-empty) cannot be imported
  consistently; resolve them in Synapse first (wait for the resync).
- Events in old room versions with quirks (v1/v2 event ids, redacted event JSON, events missing
  from `event_json`): the importer logs and skips what it cannot parse; the comparison script shows
  the gaps so they can be judged.
- Password pepper: without the value from Synapse's config no password works; it is in
  `homeserver.yaml` under `password_config.pepper`.
- OIDC or SAML logins (if Synapse uses them) need asaroton's MAS mode instead of passwords;
  that changes the auth part of the plan.
- Email: asaroton sends none; password reset by email and email notifications stop. Admins
  reset passwords through the admin API instead.
- Database size decides the window: the issue measured 1.4M events; the import is a streaming
  copy, so expect minutes rather than hours, the media copy dominates.
- Clients see an initial sync after cutover (Synapse tokens are not understood); Element handles
  it, but it costs bandwidth on the first connect.
- Anything that pokes Synapse's admin API (scripts, dashboards) must be checked against
  asaroton's `/_synapse/admin` subset.

## Inventory of the Synapse installation (checked 2026-10-29 over SSH)

- Host `oele` (cluephone.org, 45.137.205.247): Ubuntu 26.04, 8 cores, 30 GB RAM, one 250 GB
  volume with 41 GB free. It also serves twelve other Apache sites, MySQL, Icecast and Postfix,
  so the Matrix service is one tenant among several.
- Synapse 1.158.0 with the substring-search fork, running from a virtualenv as user `synapse`
  (`/home/synapse/synapse/homeserver.yaml` is the live config; the copy in
  `/etc/matrix-synapse` is stale). One process, no workers. Apache on the same host proxies
  `/_matrix` and `/_synapse/client` to 127.0.0.1:8008 with the `oele.net` certificate.
  `.well-known/matrix/server` is a static file saying `cluephone.org:443`.
- Password logins only (no OIDC, SAML or external ids); registration disabled; no application
  services; no registration tokens or retention policies; `enable_authenticated_media: false`;
  search mode `substring` with `min_length: 1`; URL previews with a YouTube oEmbed provider.
- Users: 2264 rows, of which 6 active and 2258 deactivated (a spam wave); 2 admins; 8 email
  addresses; 2470 devices and 2344 access tokens (most on deactivated accounts); 11 pushers.
- Rooms: 399 known, 144 with a local member joined. Room versions 1 through 12 are all present
  (1: 3, 5: 31, 6: 46, 7: 2, 8: 2, 9: 165, 10: 142, 11: 5, 12: 3). Two rooms are in partial
  state (`!hHefamtySLIPywfyCX:nope.chat`, `!kJwenoTPuEviPeTDxt:cutefunny.art`).
- Events: 2.48M (178k sent by local users), stream orderings from -729,450 to 2,105,313,
  320 rejected, none without JSON; 37k events in the old format (room versions 1 and 2).
- State: 263k state groups, 53M state delta rows (12 GB of the 22 GB database). The largest rooms
  have 38k, 9k and 4.5k joined members; three rooms hold over 5M delta rows each.
- Device-list stream at 2,390,785; 107 unsent pokes, all to servers that are gone.
- Media: 16,782 local files, 16 GB; 120k thumbnails, 33 GB (regenerable, `dynamic_thumbnails`);
  1.6 GB remote cache.

## What the inventory changes

- asaroton should run on the `oele` host itself, as another systemd service beside Apache: the
  import then reads the Synapse database over the local socket and reuses the media files in
  place, and the cutover is one `ProxyPass` target change in the existing vhost. The desktop at
  home has the faster machine but sits behind a slow uplink; moving 22 GB plus 16 GB of media over
  it is not practical for the final run. Rehearsals can still happen on the desktop from a dump.
- Disk space is the first obstacle: a second database of comparable size does not fit in 41 GB
  next to the thumbnails. Deleting the 33 GB of thumbnails (they come back on demand, and
  asaroton regenerates them from the originals) makes room; the import itself stores less than
  Synapse does for the same data because asaroton has no `event_search` vectors until the backfill
  runs and no chain-cover tables.
- The 53M state delta rows set the import time. A straight `COPY` of the mapped rows runs at
  hundreds of thousands of rows per second; expect the state import to take 10 to 20 minutes
  and the whole import under an hour. The maintenance window is therefore one to two hours.
- Rooms with tens of thousands of members decide whether asaroton copes: the hot-room state cache
  keeps whole state maps in memory (bounded, these rooms fit), and sending in the 38k-member room
  must be load-tested on the rehearsal copy before cutover.
- The two partial-state rooms have to finish resyncing in Synapse first, or be left before the
  export.
- Only 6 accounts are active; the 2258 deactivated rows are imported as deactivated so their names
  stay reserved. Pushers and sessions matter for those 6 users and are imported verbatim.
- `enable_authenticated_media: false` means clients and servers still fetch media through the
  unauthenticated `/_matrix/media/v3` endpoints; asaroton serves those too, so nothing changes for
  them.
- Synapse's substring search accepts one-character terms; asaroton's `substring_min_length` can be
  set to 1 to match, at the cost of sequential scans for such terms.
- The 107 unsent device-list pokes are for dead servers and can be dropped.

## Selection: what is left behind on purpose

Of the 399 rooms, 291 exist only because deactivated spam accounts joined large rooms elsewhere
and 46 have no local member at all; they carry 2.28M of the 2.48M events and all but 20k of the
53M state rows. The import therefore selects rather than copies:

- Rooms: those with a real (non-deactivated) local account joined, invited, or left (62 rooms:
  16 joined, 46 invited or left so the "historical" list keeps working). Rooms in partial state
  are never selected; the two present are spam-only anyway.
- Users: every row, but deactivated accounts come over as a name with `is_deactivated` only (no
  devices, tokens, keys, account data, pushers or media), so the names stay reserved and other
  servers' references to them still resolve.
- Events: every event of a selected room, including those sent by spam accounts: a room's
  history graph cannot lose events (other events point at them and other servers hold them).
- Media: uploads by active accounts (16 GB); the 8 files of deactivated accounts are dropped.
- Email addresses and email-based password reset: dropped (decided 2026-10-29).
- Search minimum term length: asaroton's default of 3, not the fork's 1 (decided 2026-10-29).
- Rollback period: one week with Synapse stopped but intact (decided 2026-10-29).

The comparison script compares the selected set, not the whole database.

## Rehearsal results (2026-10-29, on the desktop)

The selected data was streamed from the host with read-only `COPY` queries into a local
`synapse_copy` database (39 tables, under three minutes), then imported with
`asaroton import-synapse` into `asaroton_rehearsal`:

- 6 active users, 62 rooms, 197,722 events (none skipped, 456 redactions applied), 1,463 state
  groups with 19,725 delta rows, 16,772 media rows, 86 access tokens, 155 devices, 12,438 pending
  to-device messages. Wall time: 5 minutes, almost all of it the event batches (the trigram index
  on the search text is what makes those slow; it could be built after the import instead).
- `scripts/compare-synapse.py` reports no differences on any of its 14 comparisons.
- Functional checks against the rehearsal server (island mode, port 8028): a bcrypt password
  logs in and is re-hashed to argon2; the largest room's full history (186,802 events back to
  2023-08-24) paginates in 9 seconds and matches the database; search works over the imported
  text; a Synapse-format `since` token yields a 200 initial sync; an initial sync returns the
  room with state and timeline; no warnings in the log.

Not covered by the rehearsal: the media files (the host's permissions would have forced a copy
instead of hard links; the metadata was imported), the signing key (the rehearsal generated its
own), and a login by a real account (their passwords are not known here; the bcrypt path was
exercised with a synthetic account).

## Rehearsal on the host (2026-10-29)

Done with the commands below into `asaroton_rehearsal` (system user `asaroton`, role `asaroton`
with `pg_read_all_data`, config in /var/lib/asaroton-rehearsal, transient unit
`asaroton-rehearsal` on 127.0.0.1:8018, island mode): import in 3 minutes 16 seconds (197,725
events, the signing key imported from Synapse's file), comparison script OK on all 14 checks,
bcrypt login re-hashed, the largest room's history paginated in 26 seconds, search 3,346 hits,
Synapse token accepted, initial sync fine, no warnings. The rehearsal server stays up for a
login test by a real account through an SSH tunnel (`ssh -L 8018:127.0.0.1:8018
root@cluephone.org`, then Element Web with homeserver `http://localhost:8018`).

Left for the cutover itself: media hard links (the Synapse files belong to `synapse`, so link
creation as `asaroton` is refused by `fs.protected_hardlinks`; at cutover, with Synapse stopped,
`chown -R asaroton:asaroton /var/lib/matrix-synapse/media/local_content` first, or run the media
step as root), the production unit and config under /etc/asaroton and /var/lib/asaroton, and the
Apache switch. The database URL form sqlx needs for the socket is
`postgres://asaroton@%2Fvar%2Frun%2Fpostgresql/<db>`.

## Running it on the host

The import must run on the cluephone host itself for the final cutover (local socket, media in
place). The commands, to be run as root there; the binary is the release build of the commit
that passed the rehearsal:

```
install -m 755 asaroton /usr/local/bin/asaroton
useradd --system --home-dir /var/lib/asaroton --shell /usr/sbin/nologin asaroton
install -d -o asaroton -g asaroton -m 750 /var/lib/asaroton /var/lib/asaroton/media
install -d -o root -g asaroton -m 750 /etc/asaroton
sudo -u postgres createuser asaroton && sudo -u postgres createdb -O asaroton asaroton
# /etc/asaroton/asaroton.toml: server_name = "cluephone.org", database_url =
# "postgres://asaroton@%2Fvar%2Frun%2Fpostgresql/asaroton", data_dir = "/var/lib/asaroton",
# public_base_url = "https://cluephone.org", client listener 127.0.0.1:8018, federation
# listener disabled, [federation] well_known_server = "cluephone.org:443", registration
# disabled with a shared secret, [search] mode = "both".
sudo -u asaroton asaroton --config /etc/asaroton/asaroton.toml migrate
# Synapse stopped at this point (final state); the Synapse database is read as postgres,
# so run the import as a user that can read it and write media links:
sudo -u postgres psql -c "GRANT pg_read_all_data TO asaroton"
sudo -u asaroton asaroton --config /etc/asaroton/asaroton.toml import-synapse \
    --from "postgres://asaroton@%2Fvar%2Frun%2Fpostgresql/synapse" \
    --signing-key /etc/matrix-synapse/homeserver.signing.key \
    --media-src /var/lib/matrix-synapse/media
python3 compare-synapse.py "postgres:///synapse?host=/var/run/postgresql" "postgres:///asaroton?host=/var/run/postgresql"
```

Media hard links need the `asaroton` user to be able to read the Synapse files; the directories
are world-readable, and the signing key file must be readable by `asaroton` too (copy it with
`install -o asaroton -m 400` first). Then the systemd unit from docs/deployment.md, the
`ProxyPass` target in `/etc/apache2/sites-enabled/cluephone.org.conf` switched from 8008 to
8018 (and `/_synapse/admin` added), `apachectl graceful`, and the checks listed under Cutover.

A rehearsal on the host (same commands into a `asaroton_rehearsal` database, federation disabled)
is the last step before the real window; the desktop rehearsal covered everything except media
links and the key file.

## Cutover record (2026-10-29, 16:46 to 16:48 local)

`/root/asaroton-cutover.sh` on the host: Synapse stopped and disabled, media files handed to the
`asaroton` user, import into the production database (197,725 events, 16,670 of 16,772 media
files linked; the 102 missing ones were already absent from Synapse's store), comparison OK,
`asaroton.service` enabled and started, Apache switched to port 8018. Synapse was down for about
two minutes. Afterwards: federationtester.matrix.org reports OK with the original key
`ed25519:a_ceXC`; a local bot's messages went out to every server in its rooms; messages from
matrix.enimatek.nl queued during the window were delivered; the only failed inbound requests
(503) fall inside the window. The rollback script and Synapse's database, config and files stay
untouched until 2026-11-05; after that the `asaroton_rehearsal` database, /var/lib/asaroton-rehearsal,
the `asaroton-rehearsal` unit and the SSH write permission on the desktop can go.

## The key cache, after the fact

The first import did not carry over Synapse's cache of other servers' signing keys. That went
unnoticed until a server had to verify old events of a member whose server had replaced its key
since (issue #22): asaroton then depends on notaries for every key that is no longer published.
The importer copies the cache now. For a server that was migrated earlier, while the Synapse
database still exists:

```
sudo -u postgres asaroton --config /etc/asaroton/asaroton.toml import-synapse \
    --from 'postgres:///synapse?host=/var/run/postgresql' --only-server-keys
```

It adds the keys asaroton does not hold yet and changes nothing else; the server can stay up,
and running it twice does no harm. The Synapse database has to be kept until this has been done.

## Rooms that were left out, after the fact

The rooms that were not imported (see "Selection") had only deactivated accounts of this server
in them. asaroton knows nothing of those rooms, but the other servers in them still list the
accounts as members, or as invited: the rooms' events keep being sent here, where they are
refused, and the accounts stay in other people's member lists (issue #11). Synapse never sent
a leave for them when the accounts were deactivated, or it did not arrive.

What to leave is in the Synapse database, so this too has to be done while it exists:

```
-- the memberships Synapse still holds for its own users
SELECT m.room_id, m.user_id, m.membership, e.sender
  FROM local_current_membership m LEFT JOIN events e ON e.event_id = m.event_id
 WHERE m.membership IN ('join', 'invite');
-- the servers in a room, largest first (for a room without stored state, the inviter's server)
SELECT room_id, substring(state_key FROM position(':' IN state_key) + 1) AS server, count(*)
  FROM current_state_events WHERE type = 'm.room.member' AND membership = 'join' GROUP BY 1, 2;
```

Take the rows whose room is not in asaroton's `rooms` table, write them as
`room_id<TAB>user_id<TAB>server,server,...` and give the file to `scripts/leave-unrecorded.py`
on the host (`--dry-run` and `--limit` first). Every line becomes a leave event in somebody
else's room, signed by this server.

## Open questions

None at the moment; the decisions are recorded under "Selection".
