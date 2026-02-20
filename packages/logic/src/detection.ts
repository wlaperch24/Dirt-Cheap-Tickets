import type { ListingObservation, WatchThresholds } from "@dct/contracts";

export type CandidateSignal = {
  type: "OUTLIER" | "DROP";
  externalListingId?: string;
  currentPrice: number;
  baselinePrice: number;
  dropPercent: number;
  confidence: number;
  evidence: Record<string, unknown>;
  dedupeKey: string;
};

function quantile(sorted: number[], q: number): number {
  if (sorted.length === 0) return 0;
  if (sorted.length === 1) return sorted[0] ?? 0;
  const pos = (sorted.length - 1) * q;
  const base = Math.floor(pos);
  const rest = pos - base;
  const left = sorted[base] ?? sorted[sorted.length - 1] ?? 0;
  const right = sorted[base + 1] ?? left;
  return left + rest * (right - left);
}

function median(values: number[]): number {
  const sorted = [...values].sort((a, b) => a - b);
  return quantile(sorted, 0.5);
}

function iqrFence(values: number[]): number {
  const sorted = [...values].sort((a, b) => a - b);
  const q1 = quantile(sorted, 0.25);
  const q3 = quantile(sorted, 0.75);
  const iqr = q3 - q1;
  return q1 - 1.5 * iqr;
}

function madFence(values: number[], multiplier: number): number {
  const med = median(values);
  const abs = values.map((v) => Math.abs(v - med));
  const mad = median(abs);
  const robustSigma = mad * 1.4826;
  return med - multiplier * robustSigma;
}

function bucketKey(item: ListingObservation): string {
  const section = (item.section ?? "UNK").toUpperCase();
  const rowRaw = item.row ?? "UNK";
  const rowNumber = Number(rowRaw.replace(/\D/g, ""));
  let rowBand = "UNK";
  if (!Number.isNaN(rowNumber)) {
    if (rowNumber <= 10) rowBand = "1_10";
    else if (rowNumber <= 20) rowBand = "11_20";
    else rowBand = "21_PLUS";
  }
  return `${section}|${rowBand}|Q${item.quantityAvailable}`;
}

function priceValue(item: ListingObservation): number {
  if (typeof item.allInPrice === "number") return item.allInPrice;
  if (typeof item.feeEstimate === "number") return item.displayPrice + item.feeEstimate;
  return item.displayPrice;
}

function completenessScore(item: ListingObservation): number {
  if (item.completeness === "ALL_IN") return 1;
  if (item.completeness === "ESTIMATED_TOTAL") return 0.8;
  return 0.65;
}

export function detectSignals(params: {
  current: ListingObservation[];
  history24h: ListingObservation[];
  thresholds: WatchThresholds;
  source: string;
  meaningfulPrice?: number;
  dropOnly?: boolean;
}): CandidateSignal[] {
  const { current, history24h, thresholds, source, meaningfulPrice, dropOnly } = params;
  if (current.length === 0) return [];

  const grouped = new Map<string, ListingObservation[]>();
  for (const item of current) {
    const key = bucketKey(item);
    const existing = grouped.get(key) ?? [];
    existing.push(item);
    grouped.set(key, existing);
  }

  const signals: CandidateSignal[] = [];

  if (!dropOnly) {
    for (const [key, listings] of grouped.entries()) {
      if (listings.length < thresholds.minSampleSize) {
        continue;
      }

      const prices = listings.map(priceValue);
      const med = median(prices);
      const iqrLow = iqrFence(prices);
      const madLow = madFence(prices, thresholds.outlierMadMultiplier);
      const lowFence = Math.min(iqrLow, madLow);

      for (const listing of listings) {
        const price = priceValue(listing);
        const percentBelowMedian = med > 0 ? (med - price) / med : 0;
        if (price <= lowFence && percentBelowMedian >= 0.15) {
          const confidence = Math.min(0.98, 0.6 + percentBelowMedian * 0.5 * completenessScore(listing));
          signals.push({
            type: "OUTLIER",
            externalListingId: listing.externalListingId,
            currentPrice: price,
            baselinePrice: med,
            dropPercent: percentBelowMedian,
            confidence,
            evidence: {
              bucketKey: key,
              sampleSize: listings.length,
              iqrLow,
              madLow,
              median: med
            },
            dedupeKey: `${source}:${listing.watchId}:OUTLIER:${listing.externalListingId}`
          });
        }
      }
    }
  }

  const historyPrices = history24h.map(priceValue).filter((v) => v > 0);
  if (historyPrices.length >= thresholds.minSampleSize) {
    const baselineMedian = median(historyPrices);
    const currentMin = Math.min(...current.map(priceValue));
    if (baselineMedian > 0) {
      const drop = (baselineMedian - currentMin) / baselineMedian;
      const meaningfulHit = typeof meaningfulPrice === "number" && currentMin <= meaningfulPrice;
      if (drop >= thresholds.dropPercent || meaningfulHit) {
        const confidence = Math.min(0.95, 0.55 + Math.max(drop, thresholds.dropPercent) * 0.7);
        const dropStep = Math.max(0, Math.floor(drop / 0.02));
        const thresholdStep =
          typeof meaningfulPrice === "number"
            ? Math.max(0, Math.floor((meaningfulPrice - currentMin) / Math.max(5, meaningfulPrice * 0.02)))
            : 0;
        signals.push({
          type: "DROP",
          currentPrice: currentMin,
          baselinePrice: baselineMedian,
          dropPercent: drop,
          confidence,
          evidence: {
            baselineMedian,
            historySamples: historyPrices.length,
            currentSampleSize: current.length,
            meaningfulPrice: meaningfulPrice ?? null,
            meaningfulHit
          },
          dedupeKey: `${source}:${current[0]?.watchId}:DROP:${dropStep}:THRESH:${thresholdStep}`
        });
      }
    }
  }

  return signals;
}
