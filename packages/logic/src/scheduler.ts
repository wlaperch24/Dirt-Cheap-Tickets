import type { SpeedProfile } from "@dct/contracts";

type PollingInput = {
  speedProfile: SpeedProfile;
  eventDateISO?: string;
  volatilityScore?: number;
  nearThreshold?: boolean;
};

export function calculateAdaptiveCadenceSeconds(input: PollingInput): number {
  const now = Date.now();
  const eventMs = input.eventDateISO ? new Date(input.eventDateISO).getTime() : undefined;
  const hoursToEvent = eventMs ? (eventMs - now) / (1000 * 60 * 60) : Number.POSITIVE_INFINITY;

  let cadence = input.speedProfile === "FAST" ? 60 : 90;

  if (hoursToEvent <= 48) cadence = Math.min(cadence, 45);
  if (hoursToEvent <= 12) cadence = Math.min(cadence, 30);
  if ((input.volatilityScore ?? 0) > 0.25) cadence = Math.min(cadence, 45);
  if (input.nearThreshold) cadence = Math.min(cadence, 30);

  return Math.max(30, Math.min(90, cadence));
}
