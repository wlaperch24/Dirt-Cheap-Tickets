# Connectors

## Status policy
- `AUTHORIZED_API`: can run in production once credentials are configured.
- `PARTNER_REQUIRED`: gated until explicit partner authorization is granted.
- `UNSUPPORTED`: no connector implementation.

## Phase A
1. StubHub
- Status: `AUTHORIZED_API`
- Implementation: adapter contract + deterministic mock adapter (live implementation behind env flags)
- Fields: listing id, section/row, quantity, display/all-in price, fees, deep link, observed timestamp

2. Ticketmaster
- Status: `AUTHORIZED_API` (metadata/price-range level)
- Implementation: discovery-based pricing adapter using event price ranges
- Use: canonical event resolution + min/max display price comparison
- Note: this is not Partner API inventory or purchase flow

3. TickPick
- Status: `PARTNER_REQUIRED`

4. Gametime
- Status: `PARTNER_REQUIRED`

5. Vivid Seats
- Status: `PARTNER_REQUIRED`

## Connector contract
Each connector returns normalized `ConnectorSnapshot` payloads that map into shared `ListingObservation` records.
