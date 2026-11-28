# Scripts

Everything here is run from the repository's root. `check.sh` is the one to run before every
commit; the rest are tests that need a running server, tools for an operator, or both.

## Checks

| Script | What it does |
|---|---|
| `scripts/check.sh` | `cargo fmt --check`, clippy with warnings as errors, and all unit and store tests (needs PostgreSQL, see `.env.example`) |
| `scripts/complement.sh [go test args]` | Builds the image in `tests/complement/` and runs the [Complement](https://github.com/matrix-org/complement) conformance suite; needs Docker, Go and a checkout of Complement in `~/src/complement` (or `COMPLEMENT_DIR`) |
| `scripts/fuzz.sh` | Runs the fuzz targets under `fuzz/` (nightly toolchain) |

## End-to-end tests

Each of these starts a throwaway server from `target/debug/asaroton` with a database of its
own (through `fedlab.py`, which also plays the other homeservers), unless it takes a `URL`
or `LAB`: then it runs against a server that is already there, with registration open.
They need `cargo build`, the development database role from `.env.example`, and PyNaCl.

| Script | What it checks |
|---|---|
| `scripts/emulab/` | Element X in an Android emulator + headless Element Web, see its README |
| `scripts/fedpresencetest.py LAB` | Presence fan-out over federation stays bounded (two-server lab) |
| `scripts/ssinvitetest.py URL` | A pending invite does not stop sliding sync from long-polling (test server with open registration) |
| `scripts/syncaccesstest.py URL` | Sync shows a user only what they may read: knocks, bans, declined invites, history visibility |
| `scripts/syncwaketest.py URL` | A waiting sync wakes for an invitation or a room created on another device |
| `scripts/ssleaveracetest.py URL [TRIALS]` | A leave or ban that commits while a sliding sync answer is built still reaches the client as the user's own membership in `required_state` (#104); kicked and banned rooms stay in the lists (MSC4186); a self-left room does not |
| `scripts/ssresubscribetest.py URL` | A room subscribed again after a `limited` list answer gets its latest events at once, as an expanded timeline (#103); without a gap nothing old is sent again |
| `scripts/sslazytest.py` | Sliding sync with lazily loaded members hands out the memberships a timeline is about |
| `scripts/v4synctest.py` | `/v4/sync` (MSC4186 as accepted), rooms: `pos`/`timeout` in the body, `M_UNKNOWN_POS` (made up, another device's, the other endpoint's), `pos` as `from` in `/messages`, waking; `M_INVALID_PARAM` for 101 lists or subscriptions, a bad list key, an unknown `set_presence`, a backwards range; one `range` or none, `count`; `membership` and `lists`; no counts or `txn_id`; `stripped_state` for an invite and a knock; `include`/`exclude`/`lazy_members` (`include: [{}]` stays lazy) and lazy rule 3; `name`/`avatar` as `null`, `is_dm: false`; the `tags`/`not_tags` and `spaces` filters; a `StateStub` after a staged state reset; heroes: joined, invited, then left, also with an alias |
| `scripts/v4exttest.py` | `/v4/sync` extensions as their MSCs have them: to-device `messages`, `M_BAD_JSON` for `limit: 0`, `M_INVALID_PARAM` for a `since` never given out, acknowledgement across connections, 250 messages in pages without waiting (MSC4538); e2ee without `device_lists` when nothing changed, `/keys/changes` between two `pos` (MSC4543); account data in full when enabled later, `m.push_rules` once (MSC4548); bare receipts, own receipts on entry, receipts on an expanded timeline, no private receipts of others (MSC4551); bare typing (MSC4508); nothing for a room Alice was kicked or banned from; a room that drops out of range and comes back gets the tag, receipt and typing of while it was out, or `M_UNKNOWN_POS` after more than 100 receipts changed (then a new connection gets the initial receipts); `rooms` names subscriptions only; an unknown extension is ignored |
| `scripts/createroomtest.py` | CreateRoom under pressure: several rooms at once, and a sync that shows a new room whole or not yet |
| `scripts/clientendpointstest.py` | Small client endpoints: capabilities under r0, the empty 3pid list, /keys/changes |
| `scripts/fedinvitetest.py` | An invitation to another server goes there once, over /invite |
| `scripts/v120test.py` | What Matrix v1.20 asks of a server besides sticky and delayed events: the `m.preview_url` capability and `[media] url_previews = false` (403), `/relations` with `dir=f` from the oldest, `unsigned.prev_sender` also for later joiners, a negative PDU depth refused |
| `scripts/stickytest.py` | Sticky events (MSC4354, tracker #90) in both dialects: sending (also delayed), the marker and the time left in `unsigned`, the `/sync` section for a late joiner and incremental syncs, the sliding sync extension (MSC4480) with its per-room limit, the push to a server that joins, a soft-failed sticky event accepted after a state change |
| `scripts/erasetest.py` | Account erasure (`erase: true` on `/account/deactivate`, v1.10): who was in the room keeps the originals; who joins afterwards gets redacted copies (`/sync`, sliding sync, `/messages`, `/event`, `/context`, `/relations`, no bundled edit, `prev_content`; not found by `/search`; no sticky event), the room's current state stays; a server that joins afterwards gets redacted copies over `/event` and `/backfill` and no sticky push; account data, push rules, filters, email addresses and profile are gone; no redactions are sent; without `erase` nothing changes |
| `scripts/moderationtest.py` | Account locking and suspension (v1.12, v1.13) and their admin endpoints and capability (v1.18): a non-admin gets 403 before any lookup, the same for an existing and an unknown account; 400/404/403 for a remote user, an unknown account, another admin, the admin's own; `m.account_moderation` for admins only. Locked: 401 `M_USER_LOCKED` with `soft_logout` on `/sync`, `/send`, `/profile` (also the token-less `GET`), `/versions`, `/login`, `/refresh` and the rest, `/logout` works, the tokens stay and work again once unlocked; the same for a session from the OAuth sign-in (`[oauth]`), which turns a locked account away. Suspended: sync, `/messages`, leave and reject, own redactions (`/send` and `/redact`), key backup, keys, cross-signing, to-device and a new login work (the new session and the OAuth one suspended too); sending, joining, accepting and sending invites, profile changes, `/createRoom`, state, typing, uploads and redacting others' events give 403 `M_USER_SUSPENDED`; a public receipt is kept private; a delayed message scheduled before is not sent, a call's delayed leave is; lifted, everything works again |
| `scripts/policytest.py` | Policy servers (v1.18): a room whose `m.room.policy` names a mock policy server with a member has every event signed (signed federation request, signature stored and sent, the `m.room.policy` event never asked about); a refusal reaches the client verbatim and nothing is stored or sent, unless it is an answer the spec does not define for `/sign` (then a plain 403); a wrong signature is refused; the room can drop its policy server without it; `/send_join`, invitations and upgrades are signed or refused; events from another server without a valid signature are asked about once and soft-failed or accepted; a policy server that does not answer gives a 502 after 5 s and is then not asked for a while; a room whose policy server has no member there, and a room without one, ask nothing |
| `scripts/softfailtest.py` | A soft-failed event (a banned user's message that builds on older history) is kept and not shown to clients |
| `scripts/todevicetest.py` | To-device messages all arrive, however many are waiting, over both sync flavours |
| `scripts/devicelisttest.py` | Device lists as other servers see them: cross-signing signatures, prev_id, one stream ID per device |
| `scripts/droppedrequesttest.py` | Requests cut off halfway leave nothing half done: no open transaction, nothing twice on a second attempt, from a client or from another server |
| `scripts/refreshtest.py` | A refresh whose answer is lost does not end the session: the old refresh token is good until the new pair is used |
| `scripts/stoptest.py` | A server that is told to stop answers who waits, finishes the writes and leaves no message to be sent twice |
| `scripts/dehydratedtest.py` | Dehydrated devices (MSC3814): others see the device and send it keys, its owner reads them out page by page, replacing and deleting |
| `scripts/closedservertest.py` | A server with federation disabled still calls push gateways and feeds application services |
| `scripts/pushlagtest.py` | A push gateway that never answers holds up nobody else's pushes; they arrive at once and in order |
| `scripts/publicroomstest.py` | The room directory: `server=` fetches another server's over federation (signed-in users only), a search looks at every room before paging and stays fast with thousands of public rooms, federation GET lists and POST searches |
| `scripts/presencetest.py` | A sync with `set_presence=offline` is no activity: a user who keeps their presence to themselves does not show as "last active just now"; an ordinary sync brings them back online |
| `scripts/statustest.py` | A status of one's own (MSC4426, unstable `org.matrix.msc4426.status`): set, read, cleared; text over 256 or emoji over 32 bytes is M_TOO_LARGE, emoji of several graphemes fit; `m.status` stays free-form while the MSC is open |
| `scripts/profilestest.py` | Profile updates in sliding sync (MSC4262): all set fields first, then only changes, cleared fields as removed, `null` after the last shared room, `fields`/`lists` filters, a waiting request wakes for a status change |
| `scripts/oauthtest.py` | Sign-in through OAuth 2.0 with the embedded authorization server (`[oauth]`): discovery, client registration, login and consent pages, tokens that are asaroton's own, refresh rotation with the retry window, revocation, a replayed code, the device authorization grant, the QR-login rendezvous (MSC4108 2024 and MSC4388 discovery), the account pages and a cross-signing reset approved there (`m.oauth`), a device ID with `+`, `/` and `=` (as QR login makes them), password login beside it |
| `scripts/knocktest.py` | Knocking as the knocker's sliding sync sees it: the knocked room in the lists as stripped state with its name, the answer (let in, refused, withdrawn) at once; invited rooms named too |
| `scripts/emailtest.py` | Email addresses on accounts (`[email]`): a mailed code (into a directory in the lab), idempotent by `send_attempt`, wrong codes refused and running out, the link in the mail, add with password, one account per address, delete, freed by deactivation |
| `scripts/smtptest.py` | Mail over plain SMTP to a mail server on localhost, as production sends it (defaults: `localhost:25`, no TLS): envelope and headers, the code in subject and body; a refused recipient gives a 500 and a log line |
| `scripts/invitepushtest.py` | An invitation pushes the invited user (`.m.rule.invite_for_me`), on this server and from another one over `/invite`; messages after the join push as before; nobody else is woken |
| `scripts/pushcatchuptest.py` | A push worker that falls behind its notifications (a tiny channel, `ASAROTON_NOTIFIER_CAPACITY=8`, and a burst of 1,200 messages, 400 of them to a user in 40 rooms) catches up from the database: every message pushed, each once; the push of a message right after the burst comes within 2 s (#78, #100) |
| `scripts/backupversiontest.py` | Key backup versions created at the same moment for one user each get their own number; the newest is current |
| `scripts/previewtest.py` | URL previews of pages a site does not give (a 406 from a site firewall, a redirect to it, a 404) are empty, not titled after the error; an image that answers with an error is left out |
| `scripts/nultest.py` | A remote member's state event with NUL (a display name) stalls no room; one with NUL in its state key is refused alone; local NUL is a 400; a remote message with NUL in its body is found by `/search`, also with the content filters |
| `scripts/missingeventstest.py` | `/get_missing_events` answers with the events nearest to `latest_events` (#86): with a fork in range a small limit still gives the direct prev events, oldest first; `earliest_events` stops the walk |
| `scripts/depthresettest.py` | A remote server that resets (or inflates) `depth` does not decide what a first sync shows (#99): events built on the room with depth 0, 1, 2 and 10^9 come first, in the order they were built, before older events with a higher depth |
| `scripts/fedreadtest.py` | Federation readers given an event ID hand out only what the caller may see (#98): `/event` applies history visibility (redacted before the join in a `joined` room), `/event_auth` answers only for an event of the named room |
| `scripts/searchfiltertest.py` | `/search` filters in the query (#81): senders, types, `contains_url` and ignored users with the right count, full pages and `next_batch`; the `net.oele.search_filters` extension (#82): period, message kind (an edit by its new content), links (true and false; `www.` addresses, #84), refused bad values; context only when asked; `keys`; search without words (#83: `net.oele.search_without_words`, newest first, a 400 without a filter) |
| `scripts/ignoretest.py` | Events of an ignored user stay out of /messages, /context, /event, /relations, /threads, /search and pushes; state events stay |
| `scripts/ssreceiptstest.py` | Sliding sync hands out read receipts for the rooms of a list, whichever of `lists` and `rooms` the extension names, and again after a lost answer |
| `scripts/ssaccountdatatest.py [ROUNDS]` | A change to global account data reaches the user's sliding sync, also when the connection is being answered for another reason at that moment |
| `scripts/fedsendertest.py` | The federation sender: order, retries, what a failing destination costs the others |
| `scripts/fedrechecktest.py` | Events that could not be checked when they came are checked again later |
| `scripts/fedrelaytest.py` | Events that arrive through a third server |
| `scripts/keyrotationtest.py` | A server that changes its signing key |
| `scripts/outboundtest.py` | Requests to addresses somebody else chose (previews, pushers, media) reach public addresses only |
| `scripts/remoteleavetest.py` | Leaving and being removed from rooms on other servers |
| `scripts/remotemediatest.py` | Media fetched from other servers |
| `scripts/inviteblocktest.py URL` | Invite filtering (MSC4155) |
| `scripts/delayedeventstest.py URL` | Delayed events (MSC4140) |
| `scripts/delayedstabletest.py` | Delayed events through the stable API (#87): schedule, GET with `finalised`, send/cancel/restart with the 200 and 409 cases, a failed send left scheduled, the worker sending and cancelling with the error, `unsigned.delay_id` for the sender only (`/messages`, `/sync`, sliding sync); the unstable API unchanged |
| `scripts/keyuploadtest.py URL` | A device is visible to others only once its one-time keys are there |
| `scripts/rtctest.py` | The MatrixRTC endpoints against a local LiveKit (see the note on its key in the script) |
| `scripts/fedrtctest.py` | A call across two homeservers |

## Tools

| Script | What it does |
|---|---|
| `scripts/adduser.py NAME [--admin]` | Registers a user through the shared-secret endpoint (see `docs/deployment.md`) |
| `scripts/deploy.sh [status]` | Checks, builds and deploys: this machine first, then the remote host named in `ASAROTON_DEPLOY_REMOTE` (see `docs/deployment.md`) |
| `scripts/loadtest.py --users 50` | A synthetic client swarm against a running server |
| `scripts/compare-synapse.py SYNAPSE_DSN ASAROTON_DSN` | After `asaroton import-synapse`: runs the same counts on both databases and prints the differences |
| `scripts/leave-unrecorded.py LIST` | After a migration that left rooms out: makes this server's accounts leave rooms that other servers still count them in |
| `scripts/emulab/` | Element X in an Android emulator and a headless Element Web against a test server; see its README |
- `appservicedevicetest.py`: an application service registers a user, creates devices for it (201), acts as one, and deletes them without UIA (v1.17, MSC4190/MSC4326); a normal user still needs UIA.
