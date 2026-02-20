import test from "node:test";
import assert from "node:assert/strict";
import { calculateAdaptiveCadenceSeconds } from "@dct/logic";

test("scheduler picks faster cadence near event", () => {
  const eventDateISO = new Date(Date.now() + 6 * 60 * 60 * 1000).toISOString();
  const cadence = calculateAdaptiveCadenceSeconds({
    speedProfile: "FAST",
    eventDateISO,
    volatilityScore: 0.35,
    nearThreshold: true
  });
  assert.equal(cadence, 30);
});
