# Architecture

## Objective
Single-operator ticket deal monitor optimized for low-latency alerting with authorized access only.

## Service graph
1. `gateway`: receives inbound email webhooks, normalizes payloads, and enqueues intake jobs.
2. `core`: parses watch intents, resolves events, manages watch state, and schedules polling jobs.
3. `connectors`: executes marketplace connector fetches with source-level rate limiting (StubHub + Ticketmaster metadata pricing in Phase A).
4. `detector`: computes meaningful drop signals and reconfirms deals before publishing alerts.
5. `notifier`: sends email alerts for valid signals.

## Data flow
1. Inbound message -> `gateway` -> `ParseWatchIntent` job.
2. `core` parses watch request; if ambiguous, sends confirmation prompt and pauses.
3. `core` creates active watch + polling job with adaptive cadence.
4. `connectors` fetches listings and persists observations.
5. `detector` evaluates drop signals and reconfirms listing after 5-8s.
6. `notifier` deduplicates and delivers email alerts.

## Operator comparison API
- `GET /admin/watches/:watchId/comparison` returns per-source min/max observed price and premium vs cheapest source.
- `GET /admin/health/report` returns queue lag, connector run health, and notification delivery outcomes.

## Compliance boundary
- Allowed: authorized APIs, partner feeds, user-initiated deep-link alerts.
- Disallowed: scraping evasion, anti-bot bypass, CAPTCHA bypass, auto-purchasing.
