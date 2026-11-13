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
| `scripts/sslazytest.py` | Sliding sync with lazily loaded members hands out the memberships a timeline is about |
| `scripts/createroomtest.py` | CreateRoom under pressure: several rooms at once, and a sync that shows a new room whole or not yet |
| `scripts/clientendpointstest.py` | Small client endpoints: capabilities under r0, the empty 3pid list, /keys/changes |
| `scripts/fedinvitetest.py` | An invitation to another server goes there once, over /invite |
| `scripts/softfailtest.py` | A soft-failed event (a banned user's message that builds on older history) is kept and not shown to clients |
| `scripts/todevicetest.py` | To-device messages all arrive, however many are waiting, over both sync flavours |
| `scripts/devicelisttest.py` | Device lists as other servers see them: cross-signing signatures, prev_id, one stream ID per device |
| `scripts/droppedrequesttest.py` | Requests cut off halfway leave nothing half done: no open transaction, nothing twice on a second attempt, from a client or from another server |
| `scripts/refreshtest.py` | A refresh whose answer is lost does not end the session: the old refresh token is good until the new pair is used |
| `scripts/stoptest.py` | A server that is told to stop answers who waits, finishes the writes and leaves no message to be sent twice |
| `scripts/dehydratedtest.py` | Dehydrated devices (MSC3814): others see the device and send it keys, its owner reads them out page by page, replacing and deleting |
| `scripts/closedservertest.py` | A server with federation disabled still calls push gateways and feeds application services |
| `scripts/ssreceiptstest.py` | Sliding sync hands out read receipts for the rooms of a list, whichever of `lists` and `rooms` the extension names, and again after a lost answer |
| `scripts/fedsendertest.py` | The federation sender: order, retries, what a failing destination costs the others |
| `scripts/fedrechecktest.py` | Events that could not be checked when they came are checked again later |
| `scripts/fedrelaytest.py` | Events that arrive through a third server |
| `scripts/keyrotationtest.py` | A server that changes its signing key |
| `scripts/outboundtest.py` | Requests to addresses somebody else chose (previews, pushers, media) reach public addresses only |
| `scripts/remoteleavetest.py` | Leaving and being removed from rooms on other servers |
| `scripts/remotemediatest.py` | Media fetched from other servers |
| `scripts/inviteblocktest.py URL` | Invite filtering (MSC4155) |
| `scripts/delayedeventstest.py URL` | Delayed events (MSC4140) |
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
