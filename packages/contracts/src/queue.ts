import { z } from "zod";

export const queueNames = {
  intake: "intake",
  connectorFetch: "connector-fetch",
  detectSignals: "detect-signals",
  notify: "notify"
} as const;

export const parseWatchIntentJobSchema = z.object({
  inboundMessageId: z.string(),
  channel: z.enum(["sms", "email"]),
  sender: z.string(),
  rawText: z.string(),
  receivedAtISO: z.string()
});
export type ParseWatchIntentJob = z.infer<typeof parseWatchIntentJobSchema>;

export const connectorFetchJobSchema = z.object({
  watchId: z.string().uuid(),
  source: z
    .enum(["STUBHUB", "TICKETMASTER_METADATA", "TICKPICK", "GAMETIME", "VIVID_SEATS"])
    .default("STUBHUB"),
  reason: z.enum(["scheduled", "reconfirm", "manual"]).default("scheduled"),
  scheduledForISO: z.string(),
  priorityScore: z.number().int().min(1).max(100).default(50),
  reconfirmListingId: z.string().optional()
});
export type ConnectorFetchJob = z.infer<typeof connectorFetchJobSchema>;

export const detectSignalsJobSchema = z.object({
  watchId: z.string().uuid(),
  source: z.enum(["STUBHUB", "TICKETMASTER_METADATA", "TICKPICK", "GAMETIME", "VIVID_SEATS"]),
  snapshotId: z.string().uuid(),
  fetchedAtISO: z.string()
});
export type DetectSignalsJob = z.infer<typeof detectSignalsJobSchema>;

export const notifyJobSchema = z.object({
  signalId: z.string().uuid(),
  watchId: z.string().uuid(),
  source: z.enum(["STUBHUB", "TICKETMASTER_METADATA", "TICKPICK", "GAMETIME", "VIVID_SEATS"]),
  message: z.string(),
  smsDestination: z.string().optional(),
  emailDestination: z.string().optional()
});
export type NotifyJob = z.infer<typeof notifyJobSchema>;
