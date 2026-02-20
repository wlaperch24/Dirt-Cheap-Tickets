# Data Model

## Core tables
1. `watch_specs`: operator-defined watch constraints and thresholds.
2. `watch_parse_sessions`: inbound request parsing + confirmation state.
3. `canonical_events`: normalized events for cross-source mapping.
4. `event_aliases`: source-specific event IDs mapped to canonical IDs.
5. `listing_observations`: append-only listing snapshots (Timescale hypertable when available, regular Postgres table otherwise).
6. `deal_signals`: detector output with confidence + evidence.
7. `notifications`: outbound delivery status and provider IDs.
8. `connector_runs`: telemetry and error state per polling run.
9. `raw_payload_archive`: webhook and connector payload retention.

## Pricing completeness flags
- `all_in`: final buyer price includes fees.
- `estimated_total`: fees estimated.
- `display_only`: pre-fee listing price only.

## Defaults
- Currency: USD.
- Timezone fallback: America/New_York.
- Drop threshold default: 20%.
- Cooldown default: 30 minutes.
