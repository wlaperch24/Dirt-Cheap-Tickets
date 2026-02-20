import { Worker } from "bullmq";
import Fastify from "fastify";
import { env } from "@dct/config";
import {
  connectorFetchJobSchema,
  queueNames,
  watchSpecSchema,
  canonicalEventSchema,
  type SourceName,
  type ConnectorAdapter
} from "@dct/contracts";
import { query } from "@dct/db";
import { incrementMetric, logger, makeQueue, redis, renderMetrics } from "@dct/runtime";
import { StubhubMockAdapter } from "./adapters/stubhubMockAdapter.js";
import { StubhubApiAdapter } from "./adapters/stubhubApiAdapter.js";
import { TicketmasterPricingAdapter } from "./adapters/ticketmasterPricingAdapter.js";

const adapters: Partial<Record<SourceName, ConnectorAdapter>> = {
  STUBHUB: env.STUBHUB_USE_MOCK ? new StubhubMockAdapter() : new StubhubApiAdapter(),
  TICKETMASTER_METADATA: new TicketmasterPricingAdapter()
};
const detectQueue = makeQueue(queueNames.detectSignals);
const app = Fastify({ logger: false });

app.get("/healthz", async () => ({ ok: true, service: "connectors" }));
app.get("/metrics", async (_, reply) => {
  reply.type("text/plain; version=0.0.4");
  return renderMetrics("connectors");
});

async function runFetchJob(data: unknown): Promise<void> {
  const job = connectorFetchJobSchema.parse(data);
  const source = job.source;

  const watchRows = await query<Record<string, unknown>>(`SELECT * FROM watch_specs WHERE id = $1 LIMIT 1`, [job.watchId]);
  const watchRaw = watchRows[0];
  if (!watchRaw) {
    return;
  }

  const eventRows = await query<Record<string, unknown>>(
    `SELECT * FROM canonical_events WHERE id = $1 LIMIT 1`,
    [watchRaw.canonical_event_id]
  );
  const eventRaw = eventRows[0];
  if (!eventRaw) {
    return;
  }

  const watch = watchSpecSchema.parse({
    id: watchRaw.id,
    operatorLabel: watchRaw.operator_label,
    status: watchRaw.status,
    source: watchRaw.source,
    speedProfile: watchRaw.speed_profile,
    eventQuery: watchRaw.event_query,
    eventDateISO: watchRaw.event_date_iso ?? undefined,
    city: watchRaw.city ?? undefined,
    venue: watchRaw.venue ?? undefined,
    desiredQuantity: Number(watchRaw.desired_quantity),
    maxAllInPrice: watchRaw.max_all_in_price ? Number(watchRaw.max_all_in_price) : undefined,
    seatingConstraints: watchRaw.seating_constraints ?? undefined,
    thresholds: watchRaw.thresholds,
    pollCadenceMinSeconds: Number(watchRaw.poll_cadence_min_seconds),
    pollCadenceMaxSeconds: Number(watchRaw.poll_cadence_max_seconds),
    timezone: watchRaw.timezone,
    currency: watchRaw.currency,
    smsDestination: watchRaw.sms_destination ?? undefined,
    emailDestination: watchRaw.email_destination ?? undefined,
    preferredChannel: watchRaw.preferred_channel,
    canonicalEventId: watchRaw.canonical_event_id,
    nextPollAtISO: watchRaw.next_poll_at ? new Date(String(watchRaw.next_poll_at)).toISOString() : undefined,
    createdAt: new Date(String(watchRaw.created_at)).toISOString()
  });

  const event = canonicalEventSchema.parse({
    id: eventRaw.id,
    name: eventRaw.name,
    venueName: eventRaw.venue_name,
    city: eventRaw.city,
    localDateTimeISO: eventRaw.local_datetime_iso,
    timezone: eventRaw.timezone,
    sourceHints: eventRaw.source_hints ?? []
  });

  const adapter = adapters[source];
  if (!adapter) {
    throw new Error(`No adapter configured for source=${source}`);
  }

  const snapshot = await adapter.fetchListings({
    watch,
    event,
    reconfirmListingId: job.reconfirmListingId
  });

  const run = await query<{ id: string }>(
    `INSERT INTO connector_runs(
      watch_id, source, reason, latency_ms, freshness_ms, listing_count, status
    ) VALUES($1, $2, $3, $4, $5, $6, 'SUCCESS') RETURNING id`,
    [job.watchId, snapshot.source, job.reason, snapshot.latencyMs, snapshot.freshnessMs, snapshot.listings.length]
  );

  const snapshotId = run[0]!.id;

  await query("INSERT INTO raw_payload_archive(source, payload) VALUES ($1, $2::jsonb)", [
    `connector:${snapshot.source}`,
    JSON.stringify(snapshot.rawPayload)
  ]);

  for (const listing of snapshot.listings) {
    await query(
      `INSERT INTO listing_observations(
        watch_id, canonical_event_id, source, external_event_id, external_listing_id, deep_link,
        section_name, row_name, quantity_available,
        display_price, all_in_price, fee_estimate,
        currency, completeness, observed_at, request_region
      ) VALUES (
        $1, $2, $3, $4, $5, $6,
        $7, $8, $9,
        $10, $11, $12,
        $13, $14, $15, $16
      )`,
      [
        listing.watchId,
        listing.canonicalEventId,
        listing.source,
        listing.externalEventId,
        listing.externalListingId,
        listing.deepLink,
        listing.section ?? null,
        listing.row ?? null,
        listing.quantityAvailable,
        listing.displayPrice,
        listing.allInPrice ?? null,
        listing.feeEstimate ?? null,
        listing.currency,
        listing.completeness,
        listing.observedAtISO,
        listing.requestRegion
      ]
    );
  }

  const prices = snapshot.listings
    .map((listing) => listing.allInPrice ?? listing.displayPrice + (listing.feeEstimate ?? 0))
    .filter((value) => Number.isFinite(value));
  const minPrice = prices.length > 0 ? Math.min(...prices) : null;

  await query(
    `UPDATE watch_specs
     SET last_polled_at = NOW(),
         last_min_price = $2,
         volatility_score = GREATEST(0, LEAST(1, COALESCE(volatility_score, 0) * 0.7 + CASE
           WHEN $2 IS NULL THEN 0
           WHEN last_min_price IS NULL THEN 0.1
           WHEN last_min_price > 0 THEN ABS(last_min_price - $2) / last_min_price
           ELSE 0.1
         END))
     WHERE id = $1`,
    [job.watchId, minPrice]
  );

  if (snapshot.listings.length > 0) {
    await detectQueue.add(
      "detect-signals",
      {
        watchId: job.watchId,
        source: snapshot.source,
        snapshotId,
        fetchedAtISO: snapshot.fetchedAtISO
      },
      {
        removeOnComplete: 1000,
        removeOnFail: 1000,
        priority: job.reason === "reconfirm" ? 1 : 30
      }
    );
  }

  incrementMetric("connectors_fetch_success_total");
}

const worker = new Worker(
  queueNames.connectorFetch,
  async (job) => {
    await runFetchJob(job.data);
  },
  {
    connection: redis,
    concurrency: 8,
    limiter: {
      max: 20,
      duration: 1000
    }
  }
);

worker.on("failed", async (job, err) => {
  incrementMetric("connectors_fetch_failed_total");
  logger.error({ err, jobId: job?.id }, "connector fetch failed");
  const watchId = (job?.data as { watchId?: string } | undefined)?.watchId;
  const source = (job?.data as { source?: string } | undefined)?.source ?? "STUBHUB";
  const reason = (job?.data as { reason?: string } | undefined)?.reason ?? "scheduled";
  if (watchId) {
    await query(
      `INSERT INTO connector_runs(
        watch_id, source, reason, latency_ms, freshness_ms, listing_count, status, error_class, error_message
      ) VALUES($1, $2, $3, 0, 0, 0, 'FAILED', $4, $5)`,
      [watchId, source, reason, err.name, err.message]
    );
  }
});

process.on("SIGINT", () => {
  void Promise.all([worker.close(), app.close()]).finally(() => process.exit(0));
});
process.on("SIGTERM", () => {
  void Promise.all([worker.close(), app.close()]).finally(() => process.exit(0));
});

app.listen({ host: "0.0.0.0", port: env.CONNECTORS_PORT }).then(() => {
  logger.info({ port: env.CONNECTORS_PORT }, "connectors started");
});
