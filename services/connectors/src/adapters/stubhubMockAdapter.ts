import type { CanonicalEvent, ConnectorAdapter, ConnectorSnapshot, EventCandidate, WatchSpec } from "@dct/contracts";
import { buildScenario } from "../scenarios/stubhubMock.js";

export class StubhubMockAdapter implements ConnectorAdapter {
  readonly source = "STUBHUB" as const;
  readonly accessStatus = "AUTHORIZED_API" as const;

  async resolveEventCandidates(input: {
    query: string;
    city?: string;
    eventDateISO?: string;
    quantity: number;
  }): Promise<EventCandidate[]> {
    return [
      {
        source: "STUBHUB",
        externalEventId: `stubhub-mock-${input.query.replace(/\s+/g, "-").toLowerCase()}`,
        name: input.query,
        venueName: input.city ? `${input.city} Arena` : "Mock Arena",
        city: input.city ?? "Unknown",
        localDateTimeISO: input.eventDateISO ?? new Date(Date.now() + 3 * 86400_000).toISOString(),
        score: 0.79,
        reason: "stubhub mock candidate"
      }
    ];
  }

  async fetchListings(input: {
    watch: WatchSpec;
    event: CanonicalEvent;
    reconfirmListingId?: string;
  }): Promise<ConnectorSnapshot> {
    const started = Date.now();
    const fetchedAtISO = new Date().toISOString();
    const scenario = buildScenario(input.watch, fetchedAtISO);

    const listings = scenario
      .filter((listing) => !input.reconfirmListingId || listing.id === input.reconfirmListingId)
      .map((listing) => ({
        watchId: input.watch.id,
        canonicalEventId: input.event.id,
        source: "STUBHUB" as const,
        externalEventId: `stubhub:${input.event.id}`,
        externalListingId: listing.id,
        deepLink: listing.link,
        section: listing.section,
        row: listing.row,
        quantityAvailable: listing.qty,
        displayPrice: listing.display,
        allInPrice: listing.allIn,
        feeEstimate: Math.max(0, listing.allIn - listing.display),
        currency: input.watch.currency,
        completeness: "ALL_IN" as const,
        observedAtISO: fetchedAtISO,
        requestRegion: "US"
      }));

    return {
      watchId: input.watch.id,
      source: "STUBHUB",
      fetchedAtISO,
      latencyMs: Date.now() - started,
      freshnessMs: 0,
      listings,
      rawPayload: {
        mode: "mock",
        listingCount: listings.length
      }
    };
  }
}
