import type { CanonicalEvent, ConnectorAdapter, ConnectorSnapshot, EventCandidate, WatchSpec } from "@dct/contracts";
import { env } from "@dct/config";

export class StubhubApiAdapter implements ConnectorAdapter {
  readonly source = "STUBHUB" as const;
  readonly accessStatus = "AUTHORIZED_API" as const;

  private assertConfigured(): void {
    if (!env.STUBHUB_CLIENT_ID || !env.STUBHUB_CLIENT_SECRET) {
      throw new Error("StubHub API credentials are not configured");
    }
  }

  async resolveEventCandidates(input: {
    query: string;
    city?: string;
    eventDateISO?: string;
    quantity: number;
  }): Promise<EventCandidate[]> {
    this.assertConfigured();
    return [
      {
        source: "STUBHUB",
        externalEventId: `stubhub-live-placeholder-${input.query.replace(/\s+/g, "-").toLowerCase()}`,
        name: input.query,
        venueName: input.city ? `${input.city} Venue` : "Unknown Venue",
        city: input.city ?? "Unknown",
        localDateTimeISO: input.eventDateISO ?? new Date(Date.now() + 86400_000).toISOString(),
        score: 0.65,
        reason: "placeholder live adapter candidate"
      }
    ];
  }

  async fetchListings(input: {
    watch: WatchSpec;
    event: CanonicalEvent;
    reconfirmListingId?: string;
  }): Promise<ConnectorSnapshot> {
    this.assertConfigured();

    const started = Date.now();
    const fetchedAtISO = new Date().toISOString();

    // Placeholder wiring for future official StubHub API integration.
    // Keeps contract stable while Phase A runs on mock data.
    return {
      watchId: input.watch.id,
      source: "STUBHUB",
      fetchedAtISO,
      latencyMs: Date.now() - started,
      freshnessMs: 0,
      listings: [],
      rawPayload: {
        mode: "live_placeholder",
        reconfirmListingId: input.reconfirmListingId ?? null,
        eventId: input.event.id
      }
    };
  }
}
