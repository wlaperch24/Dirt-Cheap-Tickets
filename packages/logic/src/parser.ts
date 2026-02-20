import type { WatchSpecInput } from "@dct/contracts";

export type ParsedIntent = {
  confidence: number;
  data: Omit<WatchSpecInput, "rawText" | "channel" | "sender">;
  missing: string[];
};

function parseCurrencyValue(text: string): number | undefined {
  const maxMatch = text.match(/(?:max|under|<=?)\s*\$?(\d+(?:\.\d+)?)/i);
  if (maxMatch) {
    return Number(maxMatch[1]);
  }
  return undefined;
}

function parseQuantity(text: string): number {
  const qtyMatch = text.match(/(?:qty|quantity|tickets?)\s*[:=]?\s*(\d+)/i);
  if (qtyMatch) {
    return Math.max(1, Number(qtyMatch[1]));
  }
  const forMatch = text.match(/for\s*(\d+)/i);
  if (forMatch) {
    return Math.max(1, Number(forMatch[1]));
  }
  return 2;
}

function parseDateISO(text: string): string | undefined {
  const iso = text.match(/(20\d{2}-\d{2}-\d{2})/);
  if (iso) {
    return `${iso[1]}T20:00:00`;
  }
  const slash = text.match(/(\d{1,2}\/\d{1,2}(?:\/\d{2,4})?)/);
  if (!slash) {
    return undefined;
  }

  const [monthRaw, dayRaw, yearRaw] = slash[1].split("/");
  const now = new Date();
  const year = yearRaw ? Number(yearRaw.length === 2 ? `20${yearRaw}` : yearRaw) : now.getFullYear();
  const month = Number(monthRaw);
  const day = Number(dayRaw);

  if (month < 1 || month > 12 || day < 1 || day > 31) {
    return undefined;
  }

  const dt = new Date(Date.UTC(year, month - 1, day, 20, 0, 0));
  return Number.isNaN(dt.valueOf()) ? undefined : dt.toISOString().slice(0, 19);
}

function parseCity(text: string): string | undefined {
  const cityMatch = text.match(/(?:in|city)\s+([a-z\s]+?)(?:\s+on\s|\s+at\s|$)/i);
  return cityMatch?.[1]?.trim();
}

function parseVenue(text: string): string | undefined {
  const venueMatch = text.match(/(?:venue|at)\s+([a-z0-9\s'.-]{3,})/i);
  return venueMatch?.[1]?.trim();
}

function parseSeating(text: string): string | undefined {
  const seatMatch = text.match(/(?:section|seats?|lower|upper|floor|club)[^,;]*/i);
  return seatMatch?.[0]?.trim();
}

export function parseWatchIntent(text: string): ParsedIntent {
  const cleaned = text.replace(/\s+/g, " ").trim();
  const date = parseDateISO(cleaned);
  const qty = parseQuantity(cleaned);
  const maxAllInPrice = parseCurrencyValue(cleaned);
  const city = parseCity(cleaned);
  const venue = parseVenue(cleaned);
  const seatingConstraints = parseSeating(cleaned);

  const eventQuery = cleaned
    .replace(/(?:max|under|<=?)\s*\$?\d+(?:\.\d+)?/gi, "")
    .replace(/(?:qty|quantity|tickets?)\s*[:=]?\s*\d+/gi, "")
    .replace(/\b(20\d{2}-\d{2}-\d{2}|\d{1,2}\/\d{1,2}(?:\/\d{2,4})?)\b/g, "")
    .replace(/\s{2,}/g, " ")
    .trim();

  const missing: string[] = [];
  if (!eventQuery || eventQuery.length < 3) {
    missing.push("eventQuery");
  }

  let confidence = 0.4;
  if (eventQuery.length >= 4) confidence += 0.2;
  if (date) confidence += 0.15;
  if (city || venue) confidence += 0.1;
  if (maxAllInPrice) confidence += 0.1;
  if (qty >= 1) confidence += 0.05;

  if (missing.length > 0) {
    confidence = Math.min(confidence, 0.45);
  }

  return {
    confidence: Math.min(0.95, confidence),
    data: {
      eventQuery,
      eventDateISO: date,
      city,
      venue,
      desiredQuantity: qty,
      maxAllInPrice,
      seatingConstraints,
      speedProfile: "FAST"
    },
    missing
  };
}
