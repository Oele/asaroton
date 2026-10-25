# Spec coverage

Target: Matrix v1.19. Updated at the end of each phase.

| Area | Status |
|---|---|
| Client-Server: versions / well-known / capabilities | done |
| Client-Server: registration / login / UIA / devices / profile | done (password + dummy flows; no 3PID) |
| Client-Server: rooms / membership / events | local rooms done (create, join, leave, invite, kick, ban, forget, send, state, redact, messages, context, aliases, directory); remote via federation pending |
| Client-Server: /sync (classic) | done for rooms, account data, receipts, typing; presence, to-device, device lists pending |
| Client-Server: Simplified Sliding Sync | not started |
| Client-Server: media (authenticated) | not started |
| Client-Server: E2EE key APIs | done (upload/query/claim/changes, cross-signing, signatures, to-device) |
| Client-Server: push | rules and pushers stored/served; evaluation and gateway delivery pending |
| Server-Server: keys / signing | done (server keys, notary, key fetch/cache, X-Matrix) |
| Server-Server: join / leave / invite | implemented, untested against peers |
| Server-Server: transactions (/send) | implemented (PDU pipeline, EDUs), untested against peers |
| Server-Server: backfill / missing events | serving implemented; client-side backfill pending |
| Appservice API | not started |
| Admin API | not started |
| Auth: MSC3861 delegation | not started |

Complement results: see `complement-status.md`.
