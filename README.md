# Dirt-Cheap Tickets

Single-operator, speed-first ticket deal monitor using authorized APIs/connectors only.

## Features
- Email intake via webhooks (`gateway`)
- Natural-language watch parsing + confirmation (`core`)
- Adaptive polling (30-90s) with queue priorities
- Multi-source polling (StubHub + Ticketmaster metadata pricing)
- Meaningful-drop + 24h drop detection + reconfirmation (`detector`)
- Email alerts (`notifier`)

## Quick start
1. Copy environment file:
   - `cp /Users/williamlaperch/Documents/GitHub/Dirt-Cheap-Tickets/.env.example /Users/williamlaperch/Documents/GitHub/Dirt-Cheap-Tickets/.env`
   - Keep `STUBHUB_USE_MOCK=true` until live StubHub credentials are ready.
   - Set `ENABLE_STUBHUB_SOURCE=false` if you want to disable StubHub (and avoid mock data).
   - Set `ENABLE_TICKETMASTER_PRICING=true` and `TICKETMASTER_API_KEY=...` for Ticketmaster comparisons.
   - For email alerts, either configure SendGrid (`SENDGRID_API_KEY`, `SENDGRID_FROM_EMAIL`) or SMTP/Gmail (`SMTP_USER`, `SMTP_PASS`, `SMTP_FROM_EMAIL`).
2. Install dependencies:
   - `pnpm install`
3. Build workspace packages:
   - `pnpm build`
4. Run DB migration:
   - `pnpm --filter @dct/db migrate`
   - Note: TimescaleDB is optional in local dev. If unavailable, migration falls back to regular PostgreSQL tables.
5. Start all services (local):
   - `pnpm dev`

If you need to stop old local services before restarting:
- `for p in 3000 3001 3002 3003 3004; do lsof -tiTCP:$p -sTCP:LISTEN; done | xargs kill -9 2>/dev/null || true`

Or run full stack with Docker:
- `docker compose -f /Users/williamlaperch/Documents/GitHub/Dirt-Cheap-Tickets/infra/docker-compose.yml up --build`

## Create a watch manually
```bash
curl -X POST http://localhost:3001/admin/watches \
  -H 'content-type: application/json' \
  -H 'x-operator-secret: change-me' \
  -d '{
    "eventQuery":"Knicks vs Celtics",
    "eventDateISO":"2026-03-12T20:00:00",
    "city":"New York",
    "desiredQuantity":2,
    "maxAllInPrice":250,
    "speedProfile":"FAST",
    "preferredChannel":"email",
    "emailDestination":"you@example.com"
  }'
```

## Trigger by sending email
Post your email payload to:
- `POST http://localhost:3000/webhooks/email`

Example:
```bash
curl -X POST http://localhost:3000/webhooks/email \
  -H 'content-type: application/json' \
  -d '{
    "from":"you@example.com",
    "subject":"watch",
    "text":"Knicks vs Celtics 2026-03-12 in NYC qty 2 max $250 lower bowl"
  }'
```

## Phrase control (email command)
If an inbound email body includes `Activate dirt cheap tickets`, the system resumes all paused watches.

If an inbound email body includes `Deactivate dirt cheap tickets`, the system pauses all active watches.

Optional hardening:
- set `OPERATOR_CONTROL_EMAIL=you@example.com` to only accept phrase commands from your address
- customize command phrases with `ACTIVATE_PHRASE` and `DEACTIVATE_PHRASE`

## Compare sources for a watch
Trigger an immediate cross-source fetch:
```bash
curl -X POST http://localhost:3001/admin/watches/<WATCH_ID>/scan-now \
  -H 'x-operator-secret: change-me'
```

After a watch has collected observations, fetch source comparison:
```bash
curl -X GET http://localhost:3001/admin/watches/<WATCH_ID>/comparison \
  -H 'x-operator-secret: change-me'
```
This returns min/max observed price by source and each source's premium vs the cheapest source.

## Operations health report
Get queue lag, per-source run freshness, and notification delivery stats:
```bash
curl -X GET http://localhost:3001/admin/health/report \
  -H 'x-operator-secret: change-me'
```

## One-command diagnostics
Run a full local diagnostic report:
```bash
pnpm checkup
```

Run diagnostics and send a live SMTP test email:
```bash
pnpm checkup:smtp
```

## Services
- `gateway` on `:3000`
- `core` on `:3001`
- `connectors` on `:3002`
- `detector` on `:3003`
- `notifier` on `:3004`

Each service exposes:
- `GET /healthz`
- `GET /metrics`

## Compliance
- No anti-bot bypass/evasion
- No CAPTCHA bypass
- No automated purchasing
- Authorized APIs and approved partner feeds only
