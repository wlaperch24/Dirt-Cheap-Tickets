import { z } from "zod";

export const speedProfileSchema = z.enum(["FAST", "NORMAL"]);
export type SpeedProfile = z.infer<typeof speedProfileSchema>;

export const sourceNameSchema = z.enum([
  "STUBHUB",
  "TICKETMASTER_METADATA",
  "TICKPICK",
  "GAMETIME",
  "VIVID_SEATS"
]);
export type SourceName = z.infer<typeof sourceNameSchema>;

export const connectorAccessStatusSchema = z.enum([
  "AUTHORIZED_API",
  "PARTNER_REQUIRED",
  "UNSUPPORTED"
]);
export type ConnectorAccessStatus = z.infer<typeof connectorAccessStatusSchema>;

export const watchThresholdsSchema = z.object({
  dropPercent: z.number().positive().max(0.95).default(0.2),
  outlierMadMultiplier: z.number().positive().default(2.5),
  minSampleSize: z.number().int().min(5).default(8),
  cooldownMinutes: z.number().int().min(1).default(30)
});
export type WatchThresholds = z.infer<typeof watchThresholdsSchema>;

export const watchSpecInputSchema = z.object({
  rawText: z.string().min(3),
  channel: z.enum(["sms", "email"]),
  sender: z.string().min(3),
  desiredQuantity: z.number().int().positive().default(2),
  maxAllInPrice: z.number().positive().optional(),
  seatingConstraints: z.string().optional(),
  city: z.string().optional(),
  venue: z.string().optional(),
  eventQuery: z.string().min(2),
  eventDateISO: z.string().optional(),
  speedProfile: speedProfileSchema.default("NORMAL")
});
export type WatchSpecInput = z.infer<typeof watchSpecInputSchema>;

export const watchSpecSchema = z.object({
  id: z.string().uuid(),
  operatorLabel: z.string().default("default"),
  status: z.enum(["ACTIVE", "PAUSED", "PENDING_CONFIRMATION"]),
  source: sourceNameSchema.default("STUBHUB"),
  speedProfile: speedProfileSchema.default("NORMAL"),
  eventQuery: z.string(),
  eventDateISO: z.string().optional(),
  city: z.string().optional(),
  venue: z.string().optional(),
  desiredQuantity: z.number().int().positive(),
  maxAllInPrice: z.number().positive().optional(),
  seatingConstraints: z.string().optional(),
  thresholds: watchThresholdsSchema,
  pollCadenceMinSeconds: z.number().int().min(30).default(30),
  pollCadenceMaxSeconds: z.number().int().min(30).default(90),
  timezone: z.string().default("America/New_York"),
  currency: z.string().default("USD"),
  smsDestination: z.string().optional(),
  emailDestination: z.string().optional(),
  preferredChannel: z.enum(["sms", "email"]).default("email"),
  canonicalEventId: z.string().uuid().optional(),
  nextPollAtISO: z.string().optional(),
  createdAt: z.string()
});
export type WatchSpec = z.infer<typeof watchSpecSchema>;

export const canonicalEventSchema = z.object({
  id: z.string().uuid(),
  name: z.string(),
  venueName: z.string(),
  city: z.string(),
  localDateTimeISO: z.string(),
  timezone: z.string(),
  sourceHints: z.array(z.string()).default([])
});
export type CanonicalEvent = z.infer<typeof canonicalEventSchema>;

export const eventCandidateSchema = z.object({
  source: sourceNameSchema,
  externalEventId: z.string(),
  name: z.string(),
  venueName: z.string(),
  city: z.string(),
  localDateTimeISO: z.string(),
  score: z.number().min(0).max(1),
  reason: z.string()
});
export type EventCandidate = z.infer<typeof eventCandidateSchema>;

export const pricingCompletenessSchema = z.enum([
  "ALL_IN",
  "ESTIMATED_TOTAL",
  "DISPLAY_ONLY"
]);
export type PricingCompleteness = z.infer<typeof pricingCompletenessSchema>;

export const listingObservationSchema = z.object({
  watchId: z.string().uuid(),
  canonicalEventId: z.string().uuid(),
  source: sourceNameSchema,
  externalEventId: z.string(),
  externalListingId: z.string(),
  deepLink: z.string().url(),
  section: z.string().optional(),
  row: z.string().optional(),
  quantityAvailable: z.number().int().positive(),
  displayPrice: z.number().positive(),
  allInPrice: z.number().positive().optional(),
  feeEstimate: z.number().nonnegative().optional(),
  currency: z.string().default("USD"),
  completeness: pricingCompletenessSchema,
  observedAtISO: z.string(),
  requestRegion: z.string().default("US")
});
export type ListingObservation = z.infer<typeof listingObservationSchema>;

export const connectorSnapshotSchema = z.object({
  watchId: z.string().uuid(),
  source: sourceNameSchema,
  fetchedAtISO: z.string(),
  latencyMs: z.number().int().nonnegative(),
  freshnessMs: z.number().int().nonnegative(),
  listings: z.array(listingObservationSchema),
  rawPayload: z.unknown()
});
export type ConnectorSnapshot = z.infer<typeof connectorSnapshotSchema>;

export const dealSignalTypeSchema = z.enum(["OUTLIER", "DROP"]);
export type DealSignalType = z.infer<typeof dealSignalTypeSchema>;

export const dealSignalSchema = z.object({
  id: z.string().uuid(),
  watchId: z.string().uuid(),
  source: sourceNameSchema,
  type: dealSignalTypeSchema,
  externalListingId: z.string().optional(),
  currentPrice: z.number().positive(),
  baselinePrice: z.number().positive(),
  dropPercent: z.number().min(0).max(1),
  confidence: z.number().min(0).max(1),
  evidence: z.record(z.unknown()),
  dedupeKey: z.string(),
  observedAtISO: z.string(),
  reconfirmed: z.boolean().default(false)
});
export type DealSignal = z.infer<typeof dealSignalSchema>;

export const notificationPayloadSchema = z.object({
  signalId: z.string().uuid(),
  watchId: z.string().uuid(),
  channel: z.enum(["sms", "email"]),
  destination: z.string(),
  message: z.string(),
  source: sourceNameSchema,
  createdAtISO: z.string()
});
export type NotificationPayload = z.infer<typeof notificationPayloadSchema>;

export type ConnectorAdapter = {
  readonly source: SourceName;
  readonly accessStatus: ConnectorAccessStatus;
  resolveEventCandidates(input: {
    query: string;
    city?: string;
    eventDateISO?: string;
    quantity: number;
  }): Promise<EventCandidate[]>;
  fetchListings(input: {
    watch: WatchSpec;
    event: CanonicalEvent;
    reconfirmListingId?: string;
  }): Promise<ConnectorSnapshot>;
};

export * from "./queue.js";
