# Performance notes

Measured on a 16-core desktop with PostgreSQL 18 on the same machine, release build,
`scripts/loadtest.py` (threads issue sends and incremental syncs back to back, so the server is
saturated). The database had grown through many earlier runs, so the numbers are a floor.

| Scenario (100 users, 10 shared rooms, 48 client threads) | 2026-10-25 | 2026-10-26 |
|---|---|---|
| Sends | 177 msg/s; p50 67 ms, p95 140 ms | 338 msg/s; p50 26 ms, p95 62 ms |
| Incremental sync under that load | p50 544 ms, p95 724 ms | p50 349 ms, p95 429 ms |
| Initial sync under that load | p50 914 ms | p50 645 ms |
| Joins | 990 in 1.5 s | 990 in 1.2 s |

Quiet server, user in ten rooms: an incremental sync with one new event takes 8 ms, an idle one
3.5 ms.

## Where the time goes

`perf` on the release binary under load put over 20% of CPU in socket writes to PostgreSQL and
most of the rest in allocation: the server is bound by database round trips, not computation.
Statement logging (`log_min_duration_statement = 0`) gives the counts per request.

Statements per request, before and after the 2026-10-26 work:

| Request | Before | After |
|---|---|---|
| Message send (authentication, de-duplication, room load, persist, fan-out bookkeeping) | 35 | 10, of which 3 inside the persist transaction |
| Idle incremental sync | 20 | 12 |
| Incremental sync with one new event in one room | 40 | 30 |
| Initial sync of one fresh room with ten timeline events | 39 | 35 (was 88 with three rooms, 55 now) |

What changed:

- The persist path writes the event with its edges, auth links, missing-prev bookkeeping and the
  relation and search indexes in one statement; the state group and its delta are one statement;
  the current-state delta (including the denormalised membership rows) is one statement; the
  forward extremities are replaced in one statement.
- Access tokens are cached for 30 seconds after validation (logout, device deletion and account
  changes drop the entries), and a device's last-seen timestamp is written at most once a minute.
  That removes three statements from every authenticated request.
- A sync token is built from one query over all stream positions instead of six.
- Push rule evaluation loads only the create event, the power levels and the user's own membership
  (not the whole room state and member list), and parsed rulesets are cached per user.
- Presence collects the users sharing a room through one join query instead of one query per room,
  and a sync reads the user's joined rooms and the membership changes in them once for both the
  device-list and the presence sections.
- Hot rooms keep their context (current state, extremities with hashes, next depth) in memory
  after a local write. A send validates it with one forward-extremities query, which every write,
  local or over federation, moves; redactions and purges drop the entry; the cache is bounded by
  the total number of state events it holds.
- History-visibility filtering of a timeline slice settles in one lookup when the visibility is
  `shared` or `world_readable` throughout; `prev_content` for a contiguous slice comes from one
  state lookup at the slice's start instead of one state-group walk per state event.
- The push worker keeps a cached set of users that have pushers; unread counts are cached
  incrementally per room and user; idle rooms are skipped in incremental syncs through two set
  queries.

## Next steps

1. The incremental sync with new events still spends most of its statements per room (timeline,
   receipts, unread counts, state lookups for the summary); a per-room batch for several rooms at
   once is the next step.
2. The federation receive path reloads room state per incoming event; the hot-room cache could
   serve it as well once it is also updated by federation writes.
3. Profile federation fan-out separately; the sender batches per destination already.
