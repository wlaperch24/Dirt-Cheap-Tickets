import crypto from "node:crypto";
import test from "node:test";
import assert from "node:assert/strict";
import { detectSignals } from "@dct/logic";
import type { ListingObservation } from "@dct/contracts";

function mkListing(overrides: Partial<ListingObservation> = {}): ListingObservation {
  return {
    watchId: "11111111-1111-1111-1111-111111111111",
    canonicalEventId: "22222222-2222-2222-2222-222222222222",
    source: "STUBHUB",
    externalEventId: "evt",
    externalListingId: crypto.randomUUID(),
    deepLink: "https://example.com/listing",
    section: "Lower Bowl",
    row: "5",
    quantityAvailable: 2,
    displayPrice: 180,
    allInPrice: 210,
    feeEstimate: 30,
    currency: "USD",
    completeness: "ALL_IN",
    observedAtISO: new Date().toISOString(),
    requestRegion: "US",
    ...overrides
  };
}

test("detectSignals triggers outlier and drop", () => {
  const current = [
    mkListing({ externalListingId: "a", allInPrice: 220, displayPrice: 190 }),
    mkListing({ externalListingId: "b", allInPrice: 225, displayPrice: 194 }),
    mkListing({ externalListingId: "c", allInPrice: 230, displayPrice: 198 }),
    mkListing({ externalListingId: "d", allInPrice: 228, displayPrice: 196 }),
    mkListing({ externalListingId: "e", allInPrice: 235, displayPrice: 202 }),
    mkListing({ externalListingId: "f", allInPrice: 240, displayPrice: 206 }),
    mkListing({ externalListingId: "g", allInPrice: 232, displayPrice: 199 }),
    mkListing({ externalListingId: "h", allInPrice: 238, displayPrice: 204 }),
    mkListing({ externalListingId: "panic", allInPrice: 140, displayPrice: 123 })
  ];

  const history24h = Array.from({ length: 24 }).map((_, idx) =>
    mkListing({
      externalListingId: `hist-${idx}`,
      allInPrice: 245 + (idx % 3) * 5,
      displayPrice: 215 + (idx % 3) * 5,
      observedAtISO: new Date(Date.now() - idx * 60 * 60 * 1000).toISOString()
    })
  );

  const signals = detectSignals({
    current,
    history24h,
    thresholds: {
      dropPercent: 0.2,
      outlierMadMultiplier: 2.5,
      minSampleSize: 8,
      cooldownMinutes: 30
    },
    source: "STUBHUB"
  });

  assert.ok(signals.some((s) => s.type === "OUTLIER"));
  assert.ok(signals.some((s) => s.type === "DROP"));
});
