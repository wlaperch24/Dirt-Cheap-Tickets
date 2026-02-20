import type { WatchSpec } from "@dct/contracts";

type MockListing = {
  id: string;
  section: string;
  row: string;
  qty: number;
  display: number;
  allIn: number;
  link: string;
};

function seededNumber(seed: string): number {
  let hash = 0;
  for (let i = 0; i < seed.length; i += 1) {
    hash = (hash * 31 + seed.charCodeAt(i)) >>> 0;
  }
  return hash / 2 ** 32;
}

export function buildScenario(watch: WatchSpec, atISO: string): MockListing[] {
  const base = watch.maxAllInPrice ?? 220;
  const now = new Date(atISO);
  const minute = now.getUTCMinutes();

  const baseDrop = minute % 7 === 0 ? 0.86 : 1;
  const outlierDrop = minute % 11 === 0 ? 0.72 : 1;

  const rows = ["3", "5", "7", "9", "11", "13", "15", "17", "19", "21"];

  return rows.map((row, idx) => {
    const jitter = seededNumber(`${watch.id}:${row}:${minute}`) * 18;
    const normalAllIn = Math.round((base + idx * 4 + jitter) * baseDrop);
    const isOutlier = idx === 1;
    const allIn = isOutlier ? Math.round(normalAllIn * outlierDrop) : normalAllIn;
    const display = Math.max(10, Math.round(allIn * 0.88));

    return {
      id: `stub-${watch.id.slice(0, 8)}-${idx}`,
      section: idx < 5 ? "Lower Bowl" : "Upper Bowl",
      row,
      qty: watch.desiredQuantity,
      display,
      allIn,
      link: `https://www.stubhub.com/mock/listing/${watch.id}/${idx}`
    };
  });
}
