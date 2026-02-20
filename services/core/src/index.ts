import crypto from "node:crypto";
import Fastify from "fastify";
import { Worker } from "bullmq";
import { z } from "zod";
import { env } from "@dct/config";
import {
  parseWatchIntentJobSchema,
  queueNames,
  type SourceName,
  type EventCandidate,
  type ParseWatchIntentJob,
  type WatchThresholds
} from "@dct/contracts";
import { query } from "@dct/db";
import { calculateAdaptiveCadenceSeconds, parseWatchIntent } from "@dct/logic";
import { incrementMetric, logger, makeQueue, redisConnection, renderMetrics } from "@dct/runtime";
import { resolveEventCandidates } from "./resolver.js";

const connectorQueue = makeQueue(queueNames.connectorFetch);
const notifyQueue = makeQueue(queueNames.notify);

const defaultThresholds: WatchThresholds = {
  dropPercent: env.DEFAULT_DROP_PERCENT,
  outlierMadMultiplier: 2.5,
  minSampleSize: 8,
  cooldownMinutes: 30
};

const app = Fastify({ logger: false });

app.get("/healthz", async () => ({ ok: true, service: "core" }));

app.get("/metrics", async (_, reply) => {
  reply.type("text/plain; version=0.0.4");
  return renderMetrics("core");
});

function requireOperatorAuth(headerValue: string | string[] | undefined): boolean {
  const provided = Array.isArray(headerValue) ? headerValue[0] : headerValue;
  return provided === env.OPERATOR_SECRET;
}

function confirmationChoice(raw: string): number | undefined {
  const match = raw.trim().match(/^[1-9]$/);
  return match ? Number(match[0]) : undefined;
}

function normalizeText(raw: string): string {
  return raw.trim().toLowerCase();
}

function normalizeEmail(raw: string): string {
  return raw.trim().toLowerCase();
}

function isControlSenderAllowed(sender: string): boolean {
  const allowed = env.OPERATOR_CONTROL_EMAIL ?? env.INBOUND_ALERT_EMAIL;
  if (!allowed) {
    return true;
  }
  return normalizeEmail(sender) === normalizeEmail(allowed);
}

function buildConfirmationMessage(candidates: EventCandidate[]): string {
  const lines = candidates.slice(0, 3).map((candidate, idx) => {
    const date = candidate.localDateTimeISO.slice(0, 10);
    return `${idx + 1}) ${candidate.name} @ ${candidate.venueName} (${candidate.city}) ${date}`;
  });
  return `Reply with 1-3 to confirm event:\n${lines.join("\n")}`;
}

async function createCanonicalEvent(candidate: EventCandidate): Promise<string> {
  const existing = await query<{ canonical_event_id: string }>(
    "SELECT canonical_event_id FROM event_aliases WHERE source = $1 AND external_event_id = $2 LIMIT 1",
    [candidate.source, candidate.externalEventId]
  );

  if (existing[0]?.canonical_event_id) {
    return existing[0].canonical_event_id;
  }

  const event = await query<{ id: string }>(
    `INSERT INTO canonical_events(name, venue_name, city, local_datetime_iso, timezone, source_hints)
     VALUES($1, $2, $3, $4, $5, $6::jsonb)
     RETURNING id`,
    [
      candidate.name,
      candidate.venueName,
      candidate.city,
      candidate.localDateTimeISO,
      env.DEFAULT_TIMEZONE,
      JSON.stringify([candidate.source])
    ]
  );

  const canonicalEventId = event[0]!.id;

  await query(
    `INSERT INTO event_aliases(canonical_event_id, source, external_event_id)
     VALUES ($1, $2, $3)
     ON CONFLICT (source, external_event_id) DO NOTHING`,
    [canonicalEventId, candidate.source, candidate.externalEventId]
  );

  return canonicalEventId;
}

async function createWatchFromSession(input: {
  sender: string;
  channel: "sms" | "email";
  parsed: ReturnType<typeof parseWatchIntent>;
  candidate: EventCandidate;
}): Promise<string> {
  const channel = env.ALERT_EMAIL_ONLY ? "email" : input.channel;
  const canonicalEventId = await createCanonicalEvent(input.candidate);
  const cadence = calculateAdaptiveCadenceSeconds({
    speedProfile: input.parsed.data.speedProfile,
    eventDateISO: input.parsed.data.eventDateISO
  });

  const now = new Date();
  const nextPoll = new Date(now.getTime() + cadence * 1000);

  const inserted = await query<{ id: string }>(
    `INSERT INTO watch_specs(
      status, source, speed_profile, event_query, event_date_iso, city, venue,
      desired_quantity, max_all_in_price, seating_constraints, thresholds,
      poll_cadence_min_seconds, poll_cadence_max_seconds, timezone, currency,
      sms_destination, email_destination, preferred_channel,
      next_poll_at, canonical_event_id
    ) VALUES (
      'ACTIVE', 'STUBHUB', $1, $2, $3, $4, $5,
      $6, $7, $8, $9::jsonb,
      30, 90, $10, $11,
      $12, $13, $14,
      $15, $16
    ) RETURNING id`,
    [
      input.parsed.data.speedProfile,
      input.parsed.data.eventQuery,
      input.parsed.data.eventDateISO ?? null,
      input.parsed.data.city ?? null,
      input.parsed.data.venue ?? null,
      input.parsed.data.desiredQuantity,
      input.parsed.data.maxAllInPrice ?? null,
      input.parsed.data.seatingConstraints ?? null,
      JSON.stringify(defaultThresholds),
      env.DEFAULT_TIMEZONE,
      env.DEFAULT_CURRENCY,
      channel === "sms" && input.sender.length > 0 ? input.sender : null,
      channel === "email" && input.sender.length > 0 ? input.sender : null,
      channel,
      nextPoll.toISOString(),
      canonicalEventId
    ]
  );

  return inserted[0]!.id;
}

async function enqueueOperatorNotification(input: {
  watchId: string;
  source: string;
  message: string;
  sender: string;
  channel: "sms" | "email";
}): Promise<void> {
  const emailDestination =
    input.channel === "email" ? input.sender : env.INBOUND_ALERT_EMAIL ?? env.SENDGRID_TO_EMAIL;
  await notifyQueue.add(
    "notify-operator",
    {
      signalId: crypto.randomUUID(),
      watchId: input.watchId,
      source: input.source,
      message: input.message,
      smsDestination: env.ALERT_EMAIL_ONLY ? undefined : input.channel === "sms" ? input.sender : env.TWILIO_TO_NUMBER,
      emailDestination
    },
    { removeOnComplete: 1000, removeOnFail: 1000 }
  );
}

async function handleConfirmation(job: ParseWatchIntentJob, selectedIndex: number): Promise<void> {
  const sessions = await query<{
    id: string;
    parsed_payload: Record<string, unknown>;
    candidate_events: EventCandidate[];
  }>(
    `SELECT id, parsed_payload, candidate_events
     FROM watch_parse_sessions
     WHERE sender = $1 AND status = 'NEEDS_CONFIRMATION'
     ORDER BY created_at DESC
     LIMIT 1`,
    [job.sender]
  );

  const session = sessions[0];
  if (!session) {
    await enqueueOperatorNotification({
      watchId: crypto.randomUUID(),
      source: "STUBHUB",
      message: "No pending confirmation found. Send a new watch request.",
      sender: job.sender,
      channel: job.channel
    });
    return;
  }

  const candidates = Array.isArray(session.candidate_events) ? session.candidate_events : [];
  const selected = candidates[selectedIndex - 1];
  if (!selected) {
    await enqueueOperatorNotification({
      watchId: crypto.randomUUID(),
      source: "STUBHUB",
      message: "Invalid selection. Reply with 1-3.",
      sender: job.sender,
      channel: job.channel
    });
    return;
  }

  const parsed = parseWatchIntent(String(session.parsed_payload.rawText ?? ""));
  if (parsed.missing.length > 0) {
    await enqueueOperatorNotification({
      watchId: crypto.randomUUID(),
      source: "STUBHUB",
      message: "Could not recover pending watch details. Send a new watch request.",
      sender: job.sender,
      channel: job.channel
    });
    return;
  }
  const watchId = await createWatchFromSession({
    sender: job.sender,
    channel: job.channel,
    parsed,
    candidate: selected
  });

  await query(
    `UPDATE watch_parse_sessions
     SET status = 'COMPLETED', watch_id = $1
     WHERE id = $2`,
    [watchId, session.id]
  );

  await enqueueOperatorNotification({
    watchId,
    source: "STUBHUB",
    message: `Watch activated for ${selected.name}. Polling started.`,
    sender: job.sender,
    channel: job.channel
  });
}

async function handleControlPhrase(job: ParseWatchIntentJob): Promise<boolean> {
  const text = normalizeText(job.rawText);
  const activate = normalizeText(env.ACTIVATE_PHRASE);
  const deactivate = normalizeText(env.DEACTIVATE_PHRASE);

  if (!text.includes(activate) && !text.includes(deactivate)) {
    return false;
  }

  if (!isControlSenderAllowed(job.sender)) {
    await enqueueOperatorNotification({
      watchId: crypto.randomUUID(),
      source: "STUBHUB",
      message: "Control phrase ignored: sender is not authorized.",
      sender: job.sender,
      channel: job.channel
    });
    return true;
  }

  if (text.includes(activate)) {
    const resumed = await query<{ id: string }>(
      `UPDATE watch_specs
       SET status = 'ACTIVE', next_poll_at = NOW()
       WHERE status = 'PAUSED'
       RETURNING id`
    );
    await enqueueOperatorNotification({
      watchId: crypto.randomUUID(),
      source: "STUBHUB",
      message: `Monitoring activated. Resumed ${resumed.length} watch(es).`,
      sender: job.sender,
      channel: job.channel
    });
    incrementMetric("core_control_activate_total");
    return true;
  }

  const paused = await query<{ id: string }>(
    `UPDATE watch_specs
     SET status = 'PAUSED'
     WHERE status = 'ACTIVE'
     RETURNING id`
  );
  await enqueueOperatorNotification({
    watchId: crypto.randomUUID(),
    source: "STUBHUB",
    message: `Monitoring paused. Paused ${paused.length} active watch(es).`,
    sender: job.sender,
    channel: job.channel
  });
  incrementMetric("core_control_deactivate_total");
  return true;
}

async function processIntakeJob(job: ParseWatchIntentJob): Promise<void> {
  if (await handleControlPhrase(job)) {
    return;
  }

  const selected = confirmationChoice(job.rawText);
  if (selected) {
    await handleConfirmation(job, selected);
    return;
  }

  const parsed = parseWatchIntent(job.rawText);

  await query(
    `INSERT INTO watch_parse_sessions(inbound_message_id, sender, channel, raw_text, parsed_payload, status, received_at)
     VALUES ($1, $2, $3, $4, $5::jsonb, 'PENDING', $6)
     ON CONFLICT (inbound_message_id) DO NOTHING`,
    [job.inboundMessageId, job.sender, job.channel, job.rawText, JSON.stringify({ ...parsed.data, rawText: job.rawText }), job.receivedAtISO]
  );

  if (parsed.missing.length > 0) {
    await query(
      `UPDATE watch_parse_sessions
       SET status = 'FAILED'
       WHERE inbound_message_id = $1`,
      [job.inboundMessageId]
    );
    await enqueueOperatorNotification({
      watchId: crypto.randomUUID(),
      source: "STUBHUB",
      message: "Need event details. Example: Knicks vs Celtics 2026-03-12 in NYC qty 2 max $250",
      sender: job.sender,
      channel: job.channel
    });
    return;
  }

  const candidates = await resolveEventCandidates({
    query: parsed.data.eventQuery,
    city: parsed.data.city,
    eventDateISO: parsed.data.eventDateISO
  });

  if (candidates.length === 0) {
    await query(
      `UPDATE watch_parse_sessions
       SET status = 'FAILED'
       WHERE inbound_message_id = $1`,
      [job.inboundMessageId]
    );
    await enqueueOperatorNotification({
      watchId: crypto.randomUUID(),
      source: "STUBHUB",
      message: "Could not resolve event candidates. Refine event name/date/city.",
      sender: job.sender,
      channel: job.channel
    });
    return;
  }

  const top = candidates[0]!;
  const second = candidates[1];
  const ambiguous = second ? top.score - second.score < 0.12 : false;

  if (ambiguous) {
    await query(
      `UPDATE watch_parse_sessions
       SET status = 'NEEDS_CONFIRMATION', candidate_events = $2::jsonb
       WHERE inbound_message_id = $1`,
      [job.inboundMessageId, JSON.stringify(candidates.slice(0, 3))]
    );

    await enqueueOperatorNotification({
      watchId: crypto.randomUUID(),
      source: "STUBHUB",
      message: buildConfirmationMessage(candidates),
      sender: job.sender,
      channel: job.channel
    });
    return;
  }

  const watchId = await createWatchFromSession({
    sender: job.sender,
    channel: job.channel,
    parsed,
    candidate: top
  });

  await query(
    `UPDATE watch_parse_sessions
     SET status = 'COMPLETED', watch_id = $2, candidate_events = $3::jsonb
     WHERE inbound_message_id = $1`,
    [job.inboundMessageId, watchId, JSON.stringify(candidates.slice(0, 3))]
  );

  await enqueueOperatorNotification({
    watchId,
    source: "STUBHUB",
    message: `Watch activated for ${top.name}. Monitoring every 30-90 seconds.`,
    sender: job.sender,
    channel: job.channel
  });
}

const intakeWorker = new Worker(
  queueNames.intake,
  async (job) => {
    const parsed = parseWatchIntentJobSchema.parse(job.data);
    await processIntakeJob(parsed);
    incrementMetric("core_intake_processed_total");
  },
  {
    connection: redisConnection,
    concurrency: 10
  }
);

intakeWorker.on("failed", (_, err) => {
  incrementMetric("core_intake_failed_total");
  logger.error({ err }, "intake worker failed");
});

const createWatchSchema = z.object({
  eventQuery: z.string().min(3),
  eventDateISO: z.string().optional(),
  city: z.string().optional(),
  venue: z.string().optional(),
  desiredQuantity: z.number().int().positive().default(2),
  maxAllInPrice: z.number().positive().optional(),
  seatingConstraints: z.string().optional(),
  speedProfile: z.enum(["FAST", "NORMAL"]).default("FAST"),
  smsDestination: z.string().optional(),
  emailDestination: z.string().optional(),
  preferredChannel: z.enum(["sms", "email"]).default("email")
});

app.post("/admin/watches", async (request, reply) => {
  if (!requireOperatorAuth(request.headers["x-operator-secret"])) {
    return reply.code(401).send({ ok: false, error: "unauthorized" });
  }

  const parsed = createWatchSchema.safeParse(request.body);
  if (!parsed.success) {
    return reply.code(400).send({ ok: false, error: parsed.error.flatten() });
  }

  const candidates = await resolveEventCandidates({
    query: parsed.data.eventQuery,
    city: parsed.data.city,
    eventDateISO: parsed.data.eventDateISO
  });

  if (!candidates[0]) {
    return reply.code(400).send({ ok: false, error: "no_event_candidates" });
  }

  const selectedChannel = env.ALERT_EMAIL_ONLY ? "email" : parsed.data.preferredChannel;
  const watchId = await createWatchFromSession({
    sender: selectedChannel === "sms" ? parsed.data.smsDestination ?? "" : parsed.data.emailDestination ?? "",
    channel: selectedChannel,
    parsed: {
      confidence: 0.95,
      missing: [],
      data: {
        eventQuery: parsed.data.eventQuery,
        eventDateISO: parsed.data.eventDateISO,
        city: parsed.data.city,
        venue: parsed.data.venue,
        desiredQuantity: parsed.data.desiredQuantity,
        maxAllInPrice: parsed.data.maxAllInPrice,
        seatingConstraints: parsed.data.seatingConstraints,
        speedProfile: parsed.data.speedProfile
      }
    },
    candidate: candidates[0]
  });

  await query(
    `UPDATE watch_specs
     SET sms_destination = COALESCE($2, sms_destination),
         email_destination = COALESCE($3, email_destination),
         preferred_channel = $4
     WHERE id = $1`,
    [watchId, parsed.data.smsDestination ?? null, parsed.data.emailDestination ?? null, selectedChannel]
  );

  return reply.code(201).send({ ok: true, watchId, event: candidates[0] });
});

app.get("/admin/watches/:watchId", async (request, reply) => {
  if (!requireOperatorAuth(request.headers["x-operator-secret"])) {
    return reply.code(401).send({ ok: false, error: "unauthorized" });
  }

  const params = request.params as { watchId: string };
  const rows = await query<Record<string, unknown>>(
    `SELECT * FROM watch_specs WHERE id = $1`,
    [params.watchId]
  );

  if (!rows[0]) {
    return reply.code(404).send({ ok: false, error: "not_found" });
  }

  return { ok: true, watch: rows[0] };
});

app.post("/admin/watches/:watchId/pause", async (request, reply) => {
  if (!requireOperatorAuth(request.headers["x-operator-secret"])) {
    return reply.code(401).send({ ok: false, error: "unauthorized" });
  }
  const params = request.params as { watchId: string };
  await query("UPDATE watch_specs SET status = 'PAUSED' WHERE id = $1", [params.watchId]);
  return { ok: true };
});

app.post("/admin/watches/:watchId/resume", async (request, reply) => {
  if (!requireOperatorAuth(request.headers["x-operator-secret"])) {
    return reply.code(401).send({ ok: false, error: "unauthorized" });
  }
  const params = request.params as { watchId: string };
  await query(
    "UPDATE watch_specs SET status = 'ACTIVE', next_poll_at = NOW() WHERE id = $1",
    [params.watchId]
  );
  return { ok: true };
});

app.get("/admin/watches/:watchId/comparison", async (request, reply) => {
  if (!requireOperatorAuth(request.headers["x-operator-secret"])) {
    return reply.code(401).send({ ok: false, error: "unauthorized" });
  }

  const params = request.params as { watchId: string };
  const rows = await query<{
    source: SourceName;
    min_price: string;
    max_price: string;
    listing_count: string;
    observed_at: string;
  }>(
    `WITH latest_per_source AS (
      SELECT DISTINCT ON (source)
        source,
        observed_at
      FROM listing_observations
      WHERE watch_id = $1
      ORDER BY source, observed_at DESC
    )
    SELECT
      l.source,
      MIN(COALESCE(l.all_in_price, l.display_price + COALESCE(l.fee_estimate, 0)))::text AS min_price,
      MAX(COALESCE(l.all_in_price, l.display_price + COALESCE(l.fee_estimate, 0)))::text AS max_price,
      COUNT(*)::text AS listing_count,
      MAX(l.observed_at)::text AS observed_at
    FROM listing_observations l
    INNER JOIN latest_per_source s
      ON s.source = l.source
     AND s.observed_at = l.observed_at
    WHERE l.watch_id = $1
    GROUP BY l.source
    ORDER BY MIN(COALESCE(l.all_in_price, l.display_price + COALESCE(l.fee_estimate, 0))) ASC`,
    [params.watchId]
  );

  if (rows.length === 0) {
    return { ok: true, watchId: params.watchId, comparison: [], message: "No observations yet." };
  }

  const parsed = rows.map((row) => ({
    source: row.source,
    minPrice: Number(row.min_price),
    maxPrice: Number(row.max_price),
    listingCount: Number(row.listing_count),
    observedAt: new Date(row.observed_at).toISOString()
  }));

  const cheapest = Math.min(...parsed.map((item) => item.minPrice));
  const comparison = parsed.map((item) => ({
    ...item,
    premiumVsCheapestPct: cheapest > 0 ? Number((((item.minPrice - cheapest) / cheapest) * 100).toFixed(2)) : 0
  }));

  return { ok: true, watchId: params.watchId, comparison };
});

app.post("/admin/watches/:watchId/scan-now", async (request, reply) => {
  if (!requireOperatorAuth(request.headers["x-operator-secret"])) {
    return reply.code(401).send({ ok: false, error: "unauthorized" });
  }

  const params = request.params as { watchId: string };
  const watchRows = await query<{ id: string }>(
    `SELECT id FROM watch_specs WHERE id = $1 LIMIT 1`,
    [params.watchId]
  );
  if (!watchRows[0]) {
    return reply.code(404).send({ ok: false, error: "not_found" });
  }

  const sources: SourceName[] = ["STUBHUB"];
  if (env.ENABLE_TICKETMASTER_PRICING && env.TICKETMASTER_API_KEY) {
    sources.push("TICKETMASTER_METADATA");
  }

  for (const source of sources) {
    await connectorQueue.add(
      "connector-fetch",
      {
        watchId: params.watchId,
        source,
        reason: "manual",
        scheduledForISO: new Date().toISOString(),
        priorityScore: 5
      },
      {
        removeOnComplete: 1000,
        removeOnFail: 1000,
        priority: 5
      }
    );
  }

  return { ok: true, watchId: params.watchId, enqueuedSources: sources };
});

app.get("/admin/health/report", async (request, reply) => {
  if (!requireOperatorAuth(request.headers["x-operator-secret"])) {
    return reply.code(401).send({ ok: false, error: "unauthorized" });
  }

  const [connectorCounts, notifyCounts] = await Promise.all([
    connectorQueue.getJobCounts("waiting", "active", "completed", "failed", "delayed", "paused"),
    notifyQueue.getJobCounts("waiting", "active", "completed", "failed", "delayed", "paused")
  ]);

  const [connectorWaiting] = await connectorQueue.getJobs(["waiting"], 0, 0, true);
  const [notifyWaiting] = await notifyQueue.getJobs(["waiting"], 0, 0, true);
  const now = Date.now();

  const connectorLagSeconds = connectorWaiting?.timestamp
    ? Math.max(0, Math.round((now - connectorWaiting.timestamp) / 1000))
    : 0;
  const notifyLagSeconds = notifyWaiting?.timestamp
    ? Math.max(0, Math.round((now - notifyWaiting.timestamp) / 1000))
    : 0;

  const watchSummaryRows = await query<{
    active_watches: string;
    stale_watches: string;
  }>(
    `SELECT
      COUNT(*) FILTER (WHERE status = 'ACTIVE')::text AS active_watches,
      COUNT(*) FILTER (
        WHERE status = 'ACTIVE'
          AND (last_polled_at IS NULL OR last_polled_at < NOW() - interval '5 minutes')
      )::text AS stale_watches
     FROM watch_specs`
  );

  const connectorRows = await query<{
    source: string;
    last_success_at: string | null;
    last_failed_at: string | null;
    success_runs_1h: string;
    failed_runs_1h: string;
    avg_latency_ms_1h: string | null;
  }>(
    `SELECT
      source,
      MAX(created_at) FILTER (WHERE status = 'SUCCESS')::text AS last_success_at,
      MAX(created_at) FILTER (WHERE status = 'FAILED')::text AS last_failed_at,
      COUNT(*) FILTER (
        WHERE status = 'SUCCESS'
          AND created_at >= NOW() - interval '1 hour'
      )::text AS success_runs_1h,
      COUNT(*) FILTER (
        WHERE status = 'FAILED'
          AND created_at >= NOW() - interval '1 hour'
      )::text AS failed_runs_1h,
      ROUND(AVG(latency_ms) FILTER (
        WHERE status = 'SUCCESS'
          AND created_at >= NOW() - interval '1 hour'
      ))::text AS avg_latency_ms_1h
     FROM connector_runs
     GROUP BY source
     ORDER BY source ASC`
  );

  const notificationRows = await query<{
    status: "SENT" | "FAILED" | "QUEUED";
    count_24h: string;
  }>(
    `SELECT
      status,
      COUNT(*)::text AS count_24h
     FROM notifications
     WHERE created_at >= NOW() - interval '24 hours'
     GROUP BY status`
  );

  const recentFailures = await query<{
    channel: string;
    destination: string;
    error_message: string | null;
    created_at: string;
  }>(
    `SELECT
      channel,
      destination,
      error_message,
      created_at::text
     FROM notifications
     WHERE status = 'FAILED'
     ORDER BY created_at DESC
     LIMIT 5`
  );

  const notificationSummary = {
    sent24h: Number(notificationRows.find((row) => row.status === "SENT")?.count_24h ?? "0"),
    failed24h: Number(notificationRows.find((row) => row.status === "FAILED")?.count_24h ?? "0"),
    queued24h: Number(notificationRows.find((row) => row.status === "QUEUED")?.count_24h ?? "0")
  };

  const watchSummary = watchSummaryRows[0] ?? { active_watches: "0", stale_watches: "0" };

  return {
    ok: true,
    generatedAt: new Date().toISOString(),
    watches: {
      active: Number(watchSummary.active_watches),
      stale: Number(watchSummary.stale_watches)
    },
    queues: {
      connectorFetch: {
        ...connectorCounts,
        lagSeconds: connectorLagSeconds
      },
      notify: {
        ...notifyCounts,
        lagSeconds: notifyLagSeconds
      }
    },
    sources: connectorRows.map((row) => ({
      source: row.source,
      lastSuccessAt: row.last_success_at ? new Date(row.last_success_at).toISOString() : null,
      lastFailedAt: row.last_failed_at ? new Date(row.last_failed_at).toISOString() : null,
      successRuns1h: Number(row.success_runs_1h),
      failedRuns1h: Number(row.failed_runs_1h),
      avgLatencyMs1h: row.avg_latency_ms_1h ? Number(row.avg_latency_ms_1h) : null
    })),
    notifications: {
      ...notificationSummary,
      recentFailures: recentFailures.map((row) => ({
        channel: row.channel,
        destination: row.destination,
        error: row.error_message,
        at: new Date(row.created_at).toISOString()
      }))
    }
  };
});

const schedulerInterval = setInterval(async () => {
  try {
    const due = await query<{
      id: string;
      speed_profile: "FAST" | "NORMAL";
      event_date_iso: string | null;
      volatility_score: number | null;
      max_all_in_price: string | null;
      last_min_price: string | null;
    }>(
      `SELECT id, speed_profile, event_date_iso, volatility_score, max_all_in_price, last_min_price
       FROM watch_specs
       WHERE status = 'ACTIVE' AND canonical_event_id IS NOT NULL AND next_poll_at <= NOW()
       ORDER BY next_poll_at ASC
       LIMIT 100`
    );

    for (const watch of due) {
      const maxPrice = watch.max_all_in_price ? Number(watch.max_all_in_price) : undefined;
      const lastMinPrice = watch.last_min_price ? Number(watch.last_min_price) : undefined;
      const nearThreshold = !!(maxPrice && lastMinPrice && lastMinPrice <= maxPrice * 1.1);
      const cadence = calculateAdaptiveCadenceSeconds({
        speedProfile: watch.speed_profile,
        eventDateISO: watch.event_date_iso ?? undefined,
        volatilityScore: watch.volatility_score ?? 0,
        nearThreshold
      });

      const priorityScore = Math.max(1, 100 - Math.round((90 - cadence) * 1.1));
      const sources: SourceName[] = ["STUBHUB"];
      if (env.ENABLE_TICKETMASTER_PRICING && env.TICKETMASTER_API_KEY) {
        sources.push("TICKETMASTER_METADATA");
      }

      for (const source of sources) {
        await connectorQueue.add(
          "connector-fetch",
          {
            watchId: watch.id,
            source,
            reason: "scheduled",
            scheduledForISO: new Date().toISOString(),
            priorityScore
          },
          {
            removeOnComplete: 1000,
            removeOnFail: 1000,
            priority: priorityScore
          }
        );
      }

      await query(
        `UPDATE watch_specs
         SET next_poll_at = NOW() + make_interval(secs => $2)
         WHERE id = $1`,
        [watch.id, cadence]
      );
      incrementMetric("core_scheduler_enqueued_total");
    }
  } catch (err) {
    incrementMetric("core_scheduler_errors_total");
    logger.error({ err }, "scheduler loop failed");
  }
}, 15_000);

const close = async (): Promise<void> => {
  clearInterval(schedulerInterval);
  await intakeWorker.close();
  await connectorQueue.close();
  await notifyQueue.close();
  await app.close();
};

process.on("SIGINT", () => {
  void close().finally(() => process.exit(0));
});
process.on("SIGTERM", () => {
  void close().finally(() => process.exit(0));
});

app.listen({ host: "0.0.0.0", port: env.CORE_PORT }).then(() => {
  logger.info({ port: env.CORE_PORT }, "core started");
});
