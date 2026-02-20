import crypto from "node:crypto";
import type { EventCandidate } from "@dct/contracts";
import { env } from "@dct/config";

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

export async function resolveEventCandidates(input: {
  query: string;
  city?: string;
  eventDateISO?: string;
}): Promise<EventCandidate[]> {
  if (!env.TICKETMASTER_API_KEY) {
    return [
      {
        source: "TICKETMASTER_METADATA",
        externalEventId: `mock-${input.query.replace(/\s+/g, "-").toLowerCase()}`,
        name: input.query,
        venueName: input.city ? `${input.city} Arena` : "Unknown Venue",
        city: input.city ?? "Unknown",
        localDateTimeISO: input.eventDateISO ?? new Date(Date.now() + 7 * 86400_000).toISOString(),
        score: 0.74,
        reason: "mock fallback candidate (no TICKETMASTER_API_KEY)"
      }
    ];
  }

  const params = new URLSearchParams({
    apikey: env.TICKETMASTER_API_KEY,
    keyword: input.query,
    size: "5"
  });
  if (input.city) params.set("city", input.city);

  const res = await fetch(`https://app.ticketmaster.com/discovery/v2/events.json?${params.toString()}`);
  if (!res.ok) {
    return [];
  }

  const payload = (await res.json()) as Record<string, unknown>;
  const rawEvents = ((payload._embedded as { events?: unknown[] } | undefined)?.events ?? []) as Record<
    string,
    unknown
  >[];

  return rawEvents
    .map((event): EventCandidate => {
      const name = String(event.name ?? input.query);
      const externalEventId = String(event.id ?? crypto.randomUUID());
      const venue = ((event._embedded as { venues?: Record<string, unknown>[] } | undefined)?.venues ?? [])[0] ?? {};
      const venueName = String(venue.name ?? "Unknown Venue");
      const city = String((venue.city as { name?: string } | undefined)?.name ?? input.city ?? "Unknown");
      const localDateTimeISO = String(
        (event.dates as { start?: { dateTime?: string; localDate?: string } } | undefined)?.start?.dateTime ??
          (event.dates as { start?: { dateTime?: string; localDate?: string } } | undefined)?.start?.localDate ??
          input.eventDateISO ??
          new Date().toISOString()
      );
      const score = scoreCandidate({
        query: input.query,
        city: input.city,
        eventDateISO: input.eventDateISO,
        candidateName: name,
        candidateCity: city,
        candidateDate: localDateTimeISO
      });
      return {
        source: "TICKETMASTER_METADATA",
        externalEventId,
        name,
        venueName,
        city,
        localDateTimeISO,
        score,
        reason: "ticketmaster discovery match"
      };
    })
    .sort((a, b) => b.score - a.score);
}
