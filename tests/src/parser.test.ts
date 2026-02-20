import test from "node:test";
import assert from "node:assert/strict";
import { parseWatchIntent } from "@dct/logic";

test("parseWatchIntent extracts quantity/date/price", () => {
  const parsed = parseWatchIntent("Knicks vs Celtics 2026-03-12 in NYC qty 2 max $250 lower bowl");
  assert.equal(parsed.data.desiredQuantity, 2);
  assert.equal(parsed.data.maxAllInPrice, 250);
  assert.equal(parsed.data.city?.toLowerCase(), "nyc");
  assert.equal(parsed.missing.length, 0);
  assert.ok(parsed.confidence >= 0.6);
});
