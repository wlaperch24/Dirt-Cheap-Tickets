import crypto from "node:crypto";
import type { CanonicalEvent, ConnectorAdapter, ConnectorSnapshot, EventCandidate, WatchSpec } from "@dct/contracts";
import { env } from "@dct/config";

type TicketmasterEvent = {
  id?: string;
  name?: string;
  url?: string;
  dates?: { start?: { dateTime?: string; localDate?: string } };
  priceRanges?: Array<{ min?: number; max?: number; currency?: string }>;
  _embedded?: { venues?: Array<{ name?: string; city?: { name?: string } }> };
};

function scoreCandidate(input: {
  query: string;
  city?: string;
  eventDateISO?: string;
  candidateName: string;
  candidateCity?: string;
  candidateDate?: string;
}): number {
  const q = input.query.toLowerCase();
  const name = input.candidateName.toLowerCase();
  const queryTokens = q.split(/\s+/).filter(Boolean);
  const nameTokens = new Set(name.split(/\s+/).filter(Boolean));
  const overlap = queryTokens.filter((token) => nameTokens.has(token)).length;
  const tokenScore = queryTokens.length > 0 ? overlap / queryTokens.length : 0;

  const cityScore = input.city && input.candidateCity
    ? input.candidateCity.toLowerCase().includes(input.city.toLowerCase())
      ? 0.2
      : 0
    : 0.1;

  let dateScore = 0.1;
  if (input.eventDateISO && input.candidateDate) {
    const a = new Date(input.eventDateISO).getTime();
    const b = new Date(input.candidateDate).getTime();
    const hours = Math.abs(a - b) / (1000 * 60 * 60);
    if (hours < 6) dateScore = 0.2;
    else if (hours < 24) dateScore = 0.15;
    else dateScore = 0;
  }

  return Math.min(0.99, tokenScore * 0.7 + cityScore + dateScore);
}

export class TicketmasterPricingAdapter implements ConnectorAdapter {
  readonly source = "TICKETMASTER_METADATA" as const;
  readonly accessStatus = "AUTHORIZED_API" as const;

  private isConfigured(): boolean {
    return Boolean(env.TICKETMASTER_API_KEY);
  }

  async resolveEventCandidates(input: {
    query: string;
    city?: string;
    eventDateISO?: string;
    quantity: number;
  }): Promise<EventCandidate[]> {
    if (!this.isConfigured()) {
      return [];
    }

    const params = new URLSearchParams({
      apikey: env.TICKETMASTER_API_KEY!,
      keyword: input.query,
      size: "5"
    });
    if (input.city) params.set("city", input.city);

    const res = await fetch(`https://app.ticketmaster.com/discovery/v2/events.json?${params.toString()}`);
    if (!res.ok) {
      return [];
    }

    const payload = (await res.json()) as { _embedded?: { events?: TicketmasterEvent[] } };
    const events = payload._embedded?.events ?? [];

    return events
      .map((event) => {
        const venue = event._embedded?.venues?.[0];
        const city = venue?.city?.name ?? input.city ?? "Unknown";
        const localDateTimeISO =
          event.dates?.start?.dateTime ?? event.dates?.start?.localDate ?? input.eventDateISO ?? new Date().toISOString();
        const score = scoreCandidate({
          query: input.query,
          city: input.city,
          eventDateISO: input.eventDateISO,
          candidateName: event.name ?? input.query,
          candidateCity: city,
          candidateDate: localDateTimeISO
        });

        return {
          source: "TICKETMASTER_METADATA" as const,
          externalEventId: event.id ?? `tm-${crypto.randomUUID()}`,
          name: event.name ?? input.query,
          venueName: venue?.name ?? "Unknown Venue",
          city,
          localDateTimeISO,
          score,
          reason: "ticketmaster discovery match"
        };
      })
      .sort((a, b) => b.score - a.score);
  }

  async fetchListings(input: {
    watch: WatchSpec;
    event: CanonicalEvent;
    reconfirmListingId?: string;
  }): Promise<ConnectorSnapshot> {
    const started = Date.now();
    const fetchedAtISO = new Date().toISOString();

    if (!this.isConfigured()) {
      return {
        watchId: input.watch.id,
        source: "TICKETMASTER_METADATA",
        fetchedAtISO,
        latencyMs: Date.now() - started,
        freshnessMs: 0,
        listings: [],
        rawPayload: {
          mode: "unconfigured",
          reason: "TICKETMASTER_API_KEY missing"
        }
      };
    }

    const candidates = await this.resolveEventCandidates({
      query: input.watch.eventQuery,
      city: input.watch.city,
      eventDateISO: input.watch.eventDateISO,
      quantity: input.watch.desiredQuantity
    });

    const selected = candidates[0];
    if (!selected) {
      return {
        watchId: input.watch.id,
        source: "TICKETMASTER_METADATA",
        fetchedAtISO,
        latencyMs: Date.now() - started,
        freshnessMs: 0,
        listings: [],
        rawPayload: {
          mode: "empty",
          reason: "No Ticketmaster event candidate"
        }
      };
    }

    const eventUrl = `https://www.ticketmaster.com/event/${selected.externalEventId}`;
    const eventRes = await fetch(
      `https://app.ticketmaster.com/discovery/v2/events/${selected.externalEventId}.json?apikey=${env.TICKETMASTER_API_KEY}`
    );

    if (!eventRes.ok) {
      return {
        watchId: input.watch.id,
        source: "TICKETMASTER_METADATA",
        fetchedAtISO,
        latencyMs: Date.now() - started,
        freshnessMs: 0,
        listings: [],
        rawPayload: {
          mode: "error",
          status: eventRes.status,
          eventId: selected.externalEventId
        }
      };
    }

    const eventPayload = (await eventRes.json()) as TicketmasterEvent;
    const priceRange = eventPayload.priceRanges?.[0];
    const minPrice = priceRange?.min;
    const maxPrice = priceRange?.max;
    const currency = priceRange?.currency ?? input.watch.currency;

    const listings = [] as ConnectorSnapshot["listings"];
    if (typeof minPrice === "number") {
      listings.push({
        watchId: input.watch.id,
        canonicalEventId: input.event.id,
        source: "TICKETMASTER_METADATA",
        externalEventId: selected.externalEventId,
        externalListingId: `${selected.externalEventId}:min`,
        deepLink: eventPayload.url ?? eventUrl,
        section: "best_available",
        row: "N/A",
        quantityAvailable: input.watch.desiredQuantity,
        displayPrice: minPrice,
        allInPrice: undefined,
        feeEstimate: undefined,
        currency,
        completeness: "DISPLAY_ONLY",
        observedAtISO: fetchedAtISO,
        requestRegion: "US"
      });
    }

    if (typeof maxPrice === "number") {
      listings.push({
        watchId: input.watch.id,
        canonicalEventId: input.event.id,
        source: "TICKETMASTER_METADATA",
        externalEventId: selected.externalEventId,
        externalListingId: `${selected.externalEventId}:max`,
        deepLink: eventPayload.url ?? eventUrl,
        section: "high_range",
        row: "N/A",
        quantityAvailable: input.watch.desiredQuantity,
        displayPrice: maxPrice,
        allInPrice: undefined,
        feeEstimate: undefined,
        currency,
        completeness: "DISPLAY_ONLY",
        observedAtISO: fetchedAtISO,
        requestRegion: "US"
      });
    }

    if (listings.length === 0) {
      return {
        watchId: input.watch.id,
        source: "TICKETMASTER_METADATA",
        fetchedAtISO,
        latencyMs: Date.now() - started,
        freshnessMs: 0,
        listings: [],
        rawPayload: {
          mode: "no_prices",
          eventId: selected.externalEventId
        }
      };
    }

    return {
      watchId: input.watch.id,
      source: "TICKETMASTER_METADATA",
      fetchedAtISO,
      latencyMs: Date.now() - started,
      freshnessMs: 0,
      listings,
      rawPayload: {
        mode: "ticketmaster_discovery",
        eventId: selected.externalEventId,
        priceRanges: eventPayload.priceRanges ?? []
      }
    };
  }
}
