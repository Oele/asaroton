# Performance notes

Measured 2026-10-25 on a 16-core desktop with PostgreSQL 18 on the same machine, release
build, `scripts/loadtest.py` (threads issue sends and incremental syncs back to back, so the
server is saturated; a Complement run was loading the machine at the same time, so treat the
numbers as a floor).

| Scenario | Result |
|---|---|
| 60 users, 10 shared rooms, 48 client threads | 231 msg/s; send p50 50 ms, p95 117 ms; incremental sync p50 448 ms |
| 100 users, 10 shared rooms, 48 client threads | 177 msg/s; send p50 67 ms, p95 140 ms; incremental sync p50 544 ms |
| Quiet server, user in 10 rooms | incremental sync with one new event 8 ms, idle 3.5 ms; initial sync 80–200 ms |
| Registration | 100 users in 0.6 s |
| Joins | 990 joins in 1.5 s |

## What the profile showed

`perf` on the release binary under load put over 20% of CPU in socket writes to PostgreSQL and
most of the rest in allocation: the server is bound by database round trips, not computation.
Statement logging gave the counts per request:

- A message send issues about 30 statements inside one transaction (event, edges, auth edges,
  forward extremities, search index, transaction-id bookkeeping, state lookups) and runs under
  the room's lock, so one room sustains roughly 40 sends/s and throughput scales with the
  number of active rooms.
- An incremental sync used to visit every joined room (about 20 statements per room). It now
  asks two set queries which rooms have new events or receipts and skips the rest; idle rooms
  cost nothing, a room with new events about 15 statements.
- An initial sync of ten rooms runs about 580 statements, dominated by per-event state lookups
  for `unsigned.membership` and `prev_content`.
- The push worker was querying every member's pushers for every event; it now keeps a cached
  set of users that have pushers.
- The pipeline's `unread_counts` is cached incrementally per room and user.

## Next steps

1. Collapse the persist path into fewer statements (one CTE or a stored function), and move the
   search/relations indexing out of the room lock.
2. Compute `unsigned.membership` and `prev_content` from the state groups of the whole slice in
   one query instead of per event.
3. Keep current room state in memory for hot rooms (the state map is re-read per event).
4. Profile federation fan-out separately; the sender batches per destination already.
