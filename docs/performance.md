# Performance notes

Measured on a 16-core desktop with PostgreSQL 18 on the same machine, release build,
`scripts/loadtest.py` (threads issue sends and incremental syncs back to back, so the server is
saturated). The database had grown through many earlier runs, so the numbers are a floor.

| Scenario (100 users, 10 shared rooms, 48 client threads) | 2026-10-25 | 2026-10-26 |
|---|---|---|
| Sends | 177 msg/s; p50 67 ms, p95 140 ms | 303 msg/s; p50 47 ms, p95 123 ms |
| Incremental sync under that load | p50 544 ms, p95 724 ms | p50 287 ms, p95 456 ms |
| Initial sync under that load | p50 914 ms | p50 648 ms |
| Joins | 990 in 1.5 s | 990 in 1.4 s |

Quiet server, user in ten rooms: an incremental sync with one new event takes 8 ms, an idle one
3.5 ms.

## Where the time goes

`perf` on the release binary under load put over 20% of CPU in socket writes to PostgreSQL and
most of the rest in allocation: the server is bound by database round trips, not computation.
Statement logging (`log_min_duration_statement = 0`) gives the counts per request.

Statements per request, before and after the 2026-10-26 work:

| Request | Before | After |
|---|---|---|
| Message send (authentication, de-duplication, room load, persist, fan-out bookkeeping) | 35 | 15, of which 3 inside the persist transaction |
| Idle incremental sync | 20 | 14 |
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
- Presence collects the users sharing a room through one join query instead of one query per room.
- History-visibility filtering of a timeline slice settles in one lookup when the visibility is
  `shared` or `world_readable` throughout; `prev_content` for a contiguous slice comes from one
  state lookup at the slice's start instead of one state-group walk per state event.
- The push worker keeps a cached set of users that have pushers; unread counts are cached
  incrementally per room and user; idle rooms are skipped in incremental syncs through two set
  queries.

## Next steps

1. Keep current room state in memory for hot rooms: the send path still loads the room's state,
   extremities and prev events (about seven statements) before the persist transaction.
2. Memoise per-sync lookups that recur (the user's memberships are read three times, the joined
   room list twice).
3. Profile federation fan-out separately; the sender batches per destination already.
