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
| Client-Server: E2EE key APIs | not started |
| Client-Server: push | not started |
| Server-Server: keys / signing | not started |
| Server-Server: join / leave / invite | not started |
| Server-Server: transactions (/send) | not started |
| Server-Server: backfill / missing events | not started |
| Appservice API | not started |
| Admin API | not started |
| Auth: MSC3861 delegation | not started |

Complement results: see `complement-status.md`.
