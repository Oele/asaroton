# The watch job

Keeps tessera current without anyone having to remember to look. Once a week it gathers what
changed around tessera, has Claude compare that with the code, files what it finds as GitHub
issues and posts a digest to a Matrix room.

It only reports. It never changes the repository, never installs or restarts anything on a
server, and never posts anywhere except the digest room and the issue tracker.

## What it reads

| Source | Where from | Why |
|---|---|---|
| `spec` | matrix-spec releases and changelog, proposals in final comment period | gaps against `docs/spec-coverage.md` |
| `synapse` | releases, security advisories, merged pull requests, new issues | a bug fixed there may exist here |
| `releases` | Element Web, Element X, Element Call, ruma, the SDKs, LiveKit, MAS, other homeservers | changes in what a server must do; advisories that apply to every implementation |
| `deps` | `cargo update --dry-run`, `cargo audit`, the newest ruma | stale or vulnerable dependencies |
| `check` | `scripts/check.sh` | the tree still builds and passes its own tests |
| `complement` | the Complement suite, pulled to its newest commit, compared with the previous run | regressions and newly added tests |
| `hosts` | the tessera service log on each host, and the federation outbox | recurring errors, endpoints clients ask for that tessera lacks, floods, restarts |
| rooms | the Synapse development room, read from cluephone.org's database | discussion that never reaches an issue tracker |
| `issues` | this repository's issues | so nothing is filed twice |

The room is read with a database query on cluephone.org, where the owner's account is already
a member. No account joins anything and nothing is ever sent to that room.

## How a run works

1. `collect.py` writes one Markdown file per source into
   `~/.local/state/tessera-watch/runs/<date>/material/`. No model is involved. A source that
   fails writes its error instead, and the digest says so.
2. `watch.sh` starts Claude Code headless with read-only file tools (`Read`, `Grep`, `Glob`),
   the prompt in `analyst-prompt.md` and the schema in `findings.schema.json`. It can read the
   material and the repository; it cannot run commands, edit files or reach the network. The
   material contains text written by strangers, so this is deliberate.
3. `publish.py` turns the findings into issues (labels `watch`, `source:…`, `severity:…`) and
   posts the digest. A finding whose `watch-key` already has an issue becomes a comment on it,
   or is skipped when there is nothing new. At most `max_new_per_run` issues are filed per run.
4. Only after publishing do the cursors in `state.json` move and the Complement results
   become the new baseline. A failed run posts a failure notice and is repeated in full.

## Running it

    watch/watch.sh                     # a full run
    watch/watch.sh --dry-run           # analyse and print the digest; file and post nothing
    watch/watch.sh --skip complement   # leave out a slow source
    watch/watch.sh --resume RUN_DIR    # analyse material that was already collected

## Changing how often it runs

It is one line in the crontab of the user who owns the checkout (`crontab -e`):

    23 5 * * 1  $HOME/src/tessera/watch/watch.sh      # Mondays 05:23

`23 5 * * *` makes it daily; `23 5 1 * *` monthly. The window always reaches back to the last
successful run, so no cadence loses anything. Complement takes the longest; for a daily run
consider `--skip complement` on weekdays.

## Settings and secrets

- `watch/config.toml`: the repositories to watch, limits, the model and its budget, the hosts.
- `~/.config/tessera-watch/secrets.toml` (mode 600): the bot's user ID, password, access token
  and the digest room ID.
- `~/.config/tessera-watch/id_ed25519`: an ssh key without a passphrase that can do exactly one
  thing: run `host_collect.py` on cluephone.org. `install-host-collector.sh` sets that up as a
  forced command. To watch another room, add it to `ROOMS` in `host_collect.py` and install the
  file again; the list lives on the server so the key cannot widen it.
- `~/.local/state/tessera-watch/`: cursors, the Complement baseline and the last twelve runs.

## Adding a source

Write a function decorated with `@source` in `collect.py` that returns Markdown, and describe
the new file in the material list of `analyst-prompt.md`.
