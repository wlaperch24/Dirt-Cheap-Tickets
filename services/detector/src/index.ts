import crypto from "node:crypto";
import Fastify from "fastify";
import { Worker } from "bullmq";
import { env } from "@dct/config";
import { detectSignalsJobSchema, queueNames, type ListingObservation, type WatchThresholds } from "@dct/contracts";
import { query } from "@dct/db";
import { detectSignals, type CandidateSignal } from "@dct/logic";
import { incrementMetric, logger, makeQueue, redisConnection, renderMetrics } from "@dct/runtime";

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

function effectivePrice(item: ListingObservation): number {
  if (typeof item.allInPrice === "number") return item.allInPrice;
  if (typeof item.feeEstimate === "number") return item.displayPrice + item.feeEstimate;
  return item.displayPrice;
}

function formatMoney(value: number, currency = "USD"): string {
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency
  }).format(value);
}

function escapeHtml(raw: string): string {
  return raw
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll("\"", "&quot;")
    .replaceAll("'", "&#39;");
}

type OptionSummary = {
  section: string;
  row: string;
  quantity: number;
  currentPrice: number;
  previousPrice?: number;
  dropPercent?: number;
  deepLink: string;
};

function topChangedOptions(
  current: ListingObservation[],
  previousPriceByListingId: Map<string, number>
): OptionSummary[] {
  const compared = current.map((listing) => {
    const currentPrice = effectivePrice(listing);
    const previousPrice = previousPriceByListingId.get(listing.externalListingId);
    const dropPercent =
      typeof previousPrice === "number" && previousPrice > 0
        ? (previousPrice - currentPrice) / previousPrice
        : undefined;

    return {
      section: listing.section ?? "Unknown",
      row: listing.row ?? "N/A",
      quantity: listing.quantityAvailable,
      currentPrice,
      previousPrice,
      dropPercent,
      deepLink: listing.deepLink
    };
  });

  const droppedNow = compared.filter(
    (item) => typeof item.previousPrice === "number" && item.currentPrice < item.previousPrice
  );
  const pool = droppedNow.length > 0 ? droppedNow : compared;

  return pool
    .sort((a, b) => {
      const aDrop = a.dropPercent ?? -1;
      const bDrop = b.dropPercent ?? -1;
      if (bDrop !== aDrop) return bDrop - aDrop;
      return a.currentPrice - b.currentPrice;
    })
    .slice(0, 3);
}

function buildAlertContent(input: {
  source: string;
  eventName: string;
  eventCity?: string;
  eventDateISO?: string;
  primarySignal: CandidateSignal;
  signalCount: number;
  options: OptionSummary[];
  watchId: string;
}): { subject: string; text: string; html: string } {
  const pct = Math.round(input.primarySignal.dropPercent * 1000) / 10;
  const when = input.eventDateISO ? new Date(input.eventDateISO).toLocaleString("en-US") : "TBD";
  const title = `${input.eventName}${input.eventCity ? ` (${input.eventCity})` : ""}`;
  const subject = `Ticket Drop Alert: ${input.eventName}`;

  const lines = [
    `JUST DROPPED: ${title}`,
    `When: ${when}`,
    `Source: ${input.source}`,
    `Trigger: ${input.primarySignal.type} (${pct}% vs baseline ${formatMoney(
      input.primarySignal.baselinePrice
    )})`,
    `Signals this cycle: ${input.signalCount}`,
    "",
    "Top 3 options:"
  ];

  input.options.forEach((option, idx) => {
    const now = formatMoney(option.currentPrice);
    const was =
      typeof option.previousPrice === "number"
        ? ` (was ${formatMoney(option.previousPrice)}${
            typeof option.dropPercent === "number" ? `, ${Math.round(option.dropPercent * 1000) / 10}% drop` : ""
          })`
        : "";
    lines.push(`${idx + 1}. ${option.section} Row ${option.row} Qty ${option.quantity}: ${now}${was}`);
    lines.push(`   ${option.deepLink}`);
  });

  lines.push("");
  lines.push(`Watch ID: ${input.watchId}`);
  const text = lines.join("\n");

  const rows = input.options
    .map((option) => {
      const now = formatMoney(option.currentPrice);
      const was =
        typeof option.previousPrice === "number"
          ? `${formatMoney(option.previousPrice)}`
          : "n/a";
      const drop =
        typeof option.dropPercent === "number"
          ? `${Math.round(option.dropPercent * 1000) / 10}%`
          : "n/a";
      return `<tr>
  <td>${escapeHtml(option.section)}</td>
  <td>${escapeHtml(option.row)}</td>
  <td>${option.quantity}</td>
  <td>${escapeHtml(now)}</td>
  <td>${escapeHtml(was)}</td>
  <td>${escapeHtml(drop)}</td>
  <td><a href="${escapeHtml(option.deepLink)}">Open</a></td>
</tr>`;
    })
    .join("");

  const html = `<h2>Ticket Deal Alert</h2>
<p><strong>${escapeHtml(title)}</strong><br/>When: ${escapeHtml(when)}<br/>Source: ${escapeHtml(
    input.source
  )}</p>
<p><strong>Trigger:</strong> ${escapeHtml(input.primarySignal.type)} (${pct}% vs baseline ${escapeHtml(
    formatMoney(input.primarySignal.baselinePrice)
  )})<br/>Signals this cycle: ${input.signalCount}</p>
<table border="1" cellpadding="6" cellspacing="0" style="border-collapse: collapse;">
  <thead>
    <tr>
      <th>Section</th><th>Row</th><th>Qty</th><th>Now</th><th>Prev</th><th>Drop</th><th>Link</th>
    </tr>
  </thead>
  <tbody>
    ${rows}
  </tbody>
</table>
<p>Watch ID: ${escapeHtml(input.watchId)}</p>`;

  return { subject, text, html };
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
      event_query: string;
      event_date_iso: string | null;
      event_name: string | null;
      event_city: string | null;
      event_local_datetime_iso: string | null;
    }>(
      `SELECT
         w.thresholds,
         w.sms_destination,
         w.email_destination,
         w.preferred_channel,
         w.max_all_in_price,
         w.event_query,
         w.event_date_iso,
         ce.name AS event_name,
         ce.city AS event_city,
         ce.local_datetime_iso AS event_local_datetime_iso
       FROM watch_specs w
       LEFT JOIN canonical_events ce ON ce.id = w.canonical_event_id
       WHERE w.id = $1
       LIMIT 1`,
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

    const previousSnapshot = await query<{ observed_at: string }>(
      `SELECT observed_at::text
       FROM listing_observations
       WHERE watch_id = $1
         AND source = $2
         AND observed_at < $3::timestamptz
       ORDER BY observed_at DESC
       LIMIT 1`,
      [data.watchId, data.source, data.fetchedAtISO]
    );

    const previousRows = previousSnapshot[0]
      ? await query<Record<string, unknown>>(
          `SELECT *
           FROM listing_observations
           WHERE watch_id = $1
             AND source = $2
             AND observed_at = $3::timestamptz`,
          [data.watchId, data.source, previousSnapshot[0].observed_at]
        )
      : [];
    const previous = previousRows.map(toObservation);
    const previousPriceByListingId = new Map<string, number>(
      previous.map((row) => [row.externalListingId, effectivePrice(row)])
    );

    const candidates = detectSignals({
      current,
      history24h,
      thresholds,
      source: data.source,
      meaningfulPrice: watch.max_all_in_price ? Number(watch.max_all_in_price) : undefined,
      dropOnly: env.ALERT_DROP_ONLY
    });

    const acceptedSignals: Array<{ signalId: string; candidate: CandidateSignal }> = [];

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

      acceptedSignals.push({ signalId, candidate });
      incrementMetric("detector_signals_emitted_total");
    }

    if (acceptedSignals.length > 0) {
      const primary = [...acceptedSignals].sort(
        (a, b) => b.candidate.dropPercent - a.candidate.dropPercent
      )[0]!;
      const eventName = watch.event_name ?? watch.event_query;
      const eventDateISO =
        watch.event_local_datetime_iso ?? watch.event_date_iso ?? undefined;
      const topOptions = topChangedOptions(current, previousPriceByListingId);
      const alert = buildAlertContent({
        source: data.source,
        eventName,
        eventCity: watch.event_city ?? undefined,
        eventDateISO,
        primarySignal: primary.candidate,
        signalCount: acceptedSignals.length,
        options: topOptions,
        watchId: data.watchId
      });

      await notifyQueue.add(
        "notify-deal",
        {
          signalId: primary.signalId,
          watchId: data.watchId,
          source: data.source,
          subject: alert.subject,
          message: alert.text,
          html: alert.html,
          smsDestination: env.ALERT_EMAIL_ONLY ? undefined : watch.sms_destination ?? env.TWILIO_TO_NUMBER,
          emailDestination: watch.email_destination ?? env.SENDGRID_TO_EMAIL
        },
        {
          removeOnComplete: 1000,
          removeOnFail: 1000,
          priority: env.ALERT_EMAIL_ONLY ? 1 : watch.preferred_channel === "sms" ? 1 : 5
        }
      );
    }
  },
  {
    connection: redisConnection,
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
