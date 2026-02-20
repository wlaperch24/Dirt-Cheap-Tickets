import crypto from "node:crypto";
import Fastify from "fastify";
import { Worker } from "bullmq";
import { env } from "@dct/config";
import { detectSignalsJobSchema, queueNames, type ListingObservation, type WatchThresholds } from "@dct/contracts";
import { query } from "@dct/db";
import { detectSignals } from "@dct/logic";
import { incrementMetric, logger, makeQueue, redis, renderMetrics } from "@dct/runtime";

const connectorQueue = makeQueue(queueNames.connectorFetch);
const notifyQueue = makeQueue(queueNames.notify);

const app = Fastify({ logger: false });

app.get("/healthz", async () => ({ ok: true, service: "detector" }));
app.get("/metrics", async (_, reply) => {
  reply.type("text/plain; version=0.0.4");
  return renderMetrics("detector");
});

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function toObservation(row: Record<string, unknown>): ListingObservation {
  return {
    watchId: String(row.watch_id),
    canonicalEventId: String(row.canonical_event_id),
    source: String(row.source) as ListingObservation["source"],
    externalEventId: String(row.external_event_id),
    externalListingId: String(row.external_listing_id),
    deepLink: String(row.deep_link),
    section: row.section_name ? String(row.section_name) : undefined,
    row: row.row_name ? String(row.row_name) : undefined,
    quantityAvailable: Number(row.quantity_available),
    displayPrice: Number(row.display_price),
    allInPrice: row.all_in_price !== null ? Number(row.all_in_price) : undefined,
    feeEstimate: row.fee_estimate !== null ? Number(row.fee_estimate) : undefined,
    currency: String(row.currency),
    completeness: String(row.completeness) as ListingObservation["completeness"],
    observedAtISO: new Date(String(row.observed_at)).toISOString(),
    requestRegion: String(row.request_region)
  };
}

async function isWithinCooldown(dedupeKey: string, cooldownMinutes: number): Promise<boolean> {
  const rows = await query<{ exists: boolean }>(
    `SELECT EXISTS(
      SELECT 1
      FROM deal_signals
      WHERE dedupe_key = $1
        AND created_at >= NOW() - make_interval(mins => $2)
    ) AS exists`,
    [dedupeKey, cooldownMinutes]
  );
  return rows[0]?.exists ?? false;
}

function notificationMessage(input: {
  type: "OUTLIER" | "DROP";
  watchId: string;
  price: number;
  baseline: number;
  dropPercent: number;
  source: string;
}): string {
  const pct = Math.round(input.dropPercent * 1000) / 10;
  return `${input.type} deal (${input.source}) watch ${input.watchId.slice(0, 8)}: $${input.price.toFixed(2)} vs $${input.baseline.toFixed(2)} baseline (${pct}% drop).`;
}

async function reconfirm(input: {
  watchId: string;
  source: string;
  listingId?: string;
  observedAtISO: string;
}): Promise<boolean> {
  if (!input.listingId) {
    return true;
  }

  await connectorQueue.add(
    "connector-fetch-reconfirm",
    {
      watchId: input.watchId,
      source: input.source,
      reason: "reconfirm",
      scheduledForISO: new Date().toISOString(),
      priorityScore: 1,
      reconfirmListingId: input.listingId
    },
    {
      priority: 1,
      delay: 0,
      removeOnComplete: 1000,
      removeOnFail: 1000
    }
  );

  await sleep(6500);

  const rows = await query<{ count: string }>(
    `SELECT COUNT(*)::text AS count
     FROM listing_observations
     WHERE watch_id = $1
       AND source = $2
       AND external_listing_id = $3
       AND observed_at > $4::timestamptz`,
    [input.watchId, input.source, input.listingId, input.observedAtISO]
  );

  return Number(rows[0]?.count ?? "0") > 0;
}

const worker = new Worker(
  queueNames.detectSignals,
  async (job) => {
    const data = detectSignalsJobSchema.parse(job.data);

    const watchRows = await query<{
      thresholds: WatchThresholds;
      sms_destination: string | null;
      email_destination: string | null;
      preferred_channel: "sms" | "email";
      max_all_in_price: string | null;
    }>(
      `SELECT thresholds, sms_destination, email_destination, preferred_channel, max_all_in_price
       FROM watch_specs WHERE id = $1 LIMIT 1`,
      [data.watchId]
    );

    const watch = watchRows[0];
    if (!watch) {
      return;
    }

    const thresholds = watch.thresholds ?? {
      dropPercent: env.DEFAULT_DROP_PERCENT,
      outlierMadMultiplier: 2.5,
      minSampleSize: 8,
      cooldownMinutes: 30
    };

    const currentRows = await query<Record<string, unknown>>(
      `SELECT * FROM listing_observations
       WHERE watch_id = $1
         AND source = $2
         AND observed_at >= $3::timestamptz - interval '2 minutes'
         AND observed_at <= $3::timestamptz + interval '2 minutes'`,
      [data.watchId, data.source, data.fetchedAtISO]
    );

    const historyRows = await query<Record<string, unknown>>(
      `SELECT * FROM listing_observations
       WHERE watch_id = $1
         AND source = $2
         AND observed_at >= NOW() - interval '24 hours'`,
      [data.watchId, data.source]
    );

    const current = currentRows.map(toObservation);
    const history24h = historyRows.map(toObservation);

    const candidates = detectSignals({
      current,
      history24h,
      thresholds,
      source: data.source,
      meaningfulPrice: watch.max_all_in_price ? Number(watch.max_all_in_price) : undefined,
      dropOnly: env.ALERT_DROP_ONLY
    });

    for (const candidate of candidates) {
      const cooledDown = await isWithinCooldown(candidate.dedupeKey, thresholds.cooldownMinutes);
      if (cooledDown) {
        continue;
      }

      const listingId = candidate.externalListingId;
      const reconfirmed = await reconfirm({
        watchId: data.watchId,
        source: data.source,
        listingId,
        observedAtISO: data.fetchedAtISO
      });

      if (!reconfirmed) {
        incrementMetric("detector_reconfirm_fail_total");
        continue;
      }

      const signalId = crypto.randomUUID();
      await query(
        `INSERT INTO deal_signals(
          id, watch_id, source, type, external_listing_id,
          current_price, baseline_price, drop_percent,
          confidence, evidence, dedupe_key, reconfirmed, observed_at
        ) VALUES (
          $1, $2, $3, $4, $5,
          $6, $7, $8,
          $9, $10::jsonb, $11, true, $12
        )`,
        [
          signalId,
          data.watchId,
          data.source,
          candidate.type,
          candidate.externalListingId ?? null,
          candidate.currentPrice,
          candidate.baselinePrice,
          candidate.dropPercent,
          candidate.confidence,
          JSON.stringify(candidate.evidence),
          candidate.dedupeKey,
          data.fetchedAtISO
        ]
      );

      const message = notificationMessage({
        type: candidate.type,
        watchId: data.watchId,
        price: candidate.currentPrice,
        baseline: candidate.baselinePrice,
        dropPercent: candidate.dropPercent,
        source: data.source
      });

      await notifyQueue.add(
        "notify-deal",
        {
          signalId,
          watchId: data.watchId,
          source: data.source,
          message,
          smsDestination: env.ALERT_EMAIL_ONLY ? undefined : watch.sms_destination ?? env.TWILIO_TO_NUMBER,
          emailDestination: watch.email_destination ?? env.SENDGRID_TO_EMAIL
        },
        {
          removeOnComplete: 1000,
          removeOnFail: 1000,
          priority: env.ALERT_EMAIL_ONLY ? 1 : watch.preferred_channel === "sms" ? 1 : 5
        }
      );

      incrementMetric("detector_signals_emitted_total");
    }
  },
  {
    connection: redis,
    concurrency: 6
  }
);

worker.on("failed", (_, err) => {
  incrementMetric("detector_failed_total");
  logger.error({ err }, "detector worker failed");
});

process.on("SIGINT", () => {
  void Promise.all([worker.close(), app.close()]).finally(() => process.exit(0));
});
process.on("SIGTERM", () => {
  void Promise.all([worker.close(), app.close()]).finally(() => process.exit(0));
});

app.listen({ host: "0.0.0.0", port: env.DETECTOR_PORT }).then(() => {
  logger.info({ port: env.DETECTOR_PORT }, "detector started");
});
