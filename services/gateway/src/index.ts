import crypto from "node:crypto";
import Fastify from "fastify";
import formBody from "@fastify/formbody";
import { env } from "@dct/config";
import { parseWatchIntentJobSchema, queueNames } from "@dct/contracts";
import { query } from "@dct/db";
import { incrementMetric, logger, makeQueue, renderMetrics } from "@dct/runtime";

const intakeQueue = makeQueue(queueNames.intake);

const app = Fastify({ logger: false });
await app.register(formBody);

app.get("/healthz", async () => ({ ok: true, service: "gateway" }));

app.get("/metrics", async (_, reply) => {
  reply.type("text/plain; version=0.0.4");
  return renderMetrics("gateway");
});

function validateTwilioSignature(url: string, payload: Record<string, unknown>, provided: string): boolean {
  if (!env.TWILIO_AUTH_TOKEN) return false;
  const sorted = Object.keys(payload)
    .sort()
    .map((key) => `${key}${String(payload[key] ?? "")}`)
    .join("");
  const digest = crypto
    .createHmac("sha1", env.TWILIO_AUTH_TOKEN)
    .update(url + sorted)
    .digest("base64");
  if (digest.length !== provided.length) {
    return false;
  }
  return crypto.timingSafeEqual(Buffer.from(digest), Buffer.from(provided));
}

function validateSendgridSignature(body: string, provided?: string): boolean {
  if (!env.SENDGRID_WEBHOOK_KEY || !provided) return false;
  const digest = crypto.createHmac("sha256", env.SENDGRID_WEBHOOK_KEY).update(body).digest("hex");
  if (digest.length !== provided.length) {
    return false;
  }
  return crypto.timingSafeEqual(Buffer.from(digest), Buffer.from(provided));
}

app.post("/webhooks/twilio/sms", async (request, reply) => {
  if (env.ALERT_EMAIL_ONLY) {
    return reply.code(410).send({ ok: false, error: "email_only_mode" });
  }

  const payload = request.body as Record<string, unknown>;
  const signature = request.headers["x-twilio-signature"];
  if (env.TWILIO_WEBHOOK_VALIDATE && typeof signature === "string") {
    const ok = validateTwilioSignature(`https://${request.hostname}${request.url}`, payload, signature);
    if (!ok) {
      incrementMetric("gateway_twilio_invalid_signature_total");
      return reply.code(401).send({ ok: false, error: "invalid_signature" });
    }
  }

  const inbound = {
    inboundMessageId: String(payload.MessageSid ?? crypto.randomUUID()),
    channel: "sms" as const,
    sender: String(payload.From ?? "unknown"),
    rawText: String(payload.Body ?? "").trim(),
    receivedAtISO: new Date().toISOString()
  };

  const valid = parseWatchIntentJobSchema.safeParse(inbound);
  if (!valid.success) {
    incrementMetric("gateway_twilio_invalid_payload_total");
    return reply.code(400).send({ ok: false, error: "invalid_payload" });
  }

  await query("INSERT INTO raw_payload_archive(source, payload) VALUES ($1, $2::jsonb)", [
    "twilio_sms",
    JSON.stringify(payload)
  ]);

  await intakeQueue.add("parse-watch-intent", valid.data, {
    removeOnComplete: 1000,
    removeOnFail: 1000
  });

  incrementMetric("gateway_twilio_messages_total");
  return reply.code(202).send({ ok: true });
});

async function handleInboundEmail(
  payload: Record<string, unknown>,
  source: "sendgrid_inbound" | "generic_email"
): Promise<{ ok: boolean; status: number; error?: string }> {
  const subject = String(payload.subject ?? "").trim();
  const textBody = String(payload.text ?? payload.body ?? "").trim();
  const rawText = `${subject}\n${textBody}`.trim();
  if (!rawText) {
    return { ok: false, status: 400, error: "empty_body" };
  }

  const senderRaw = String(payload.from ?? payload.sender ?? payload.email ?? "unknown@example.com");
  const sender = senderRaw.replace(/^.*<([^>]+)>.*$/, "$1").trim();

  const inbound = {
    inboundMessageId: String(payload["Message-Id"] ?? payload.message_id ?? crypto.randomUUID()),
    channel: "email" as const,
    sender,
    rawText,
    receivedAtISO: new Date().toISOString()
  };

  const valid = parseWatchIntentJobSchema.safeParse(inbound);
  if (!valid.success) {
    incrementMetric("gateway_sendgrid_invalid_payload_total");
    return { ok: false, status: 400, error: "invalid_payload" };
  }

  await query("INSERT INTO raw_payload_archive(source, payload) VALUES ($1, $2::jsonb)", [
    source,
    JSON.stringify(payload)
  ]);

  await intakeQueue.add("parse-watch-intent", valid.data, {
    removeOnComplete: 1000,
    removeOnFail: 1000
  });

  incrementMetric("gateway_sendgrid_messages_total");
  return { ok: true, status: 202 };
}

app.post("/webhooks/sendgrid/inbound", async (request, reply) => {
  const payload = request.body as Record<string, unknown>;
  const raw = JSON.stringify(payload);
  if (env.SENDGRID_INBOUND_VALIDATE) {
    const signature = request.headers["x-signature-ed25519"];
    const ok = validateSendgridSignature(raw, typeof signature === "string" ? signature : undefined);
    if (!ok) {
      incrementMetric("gateway_sendgrid_invalid_signature_total");
      return reply.code(401).send({ ok: false, error: "invalid_signature" });
    }
  }

  const result = await handleInboundEmail(payload, "sendgrid_inbound");
  if (!result.ok) {
    return reply.code(result.status).send({ ok: false, error: result.error });
  }
  return reply.code(202).send({ ok: true });
});

app.post("/webhooks/email", async (request, reply) => {
  const payload = request.body as Record<string, unknown>;
  const result = await handleInboundEmail(payload, "generic_email");
  if (!result.ok) {
    return reply.code(result.status).send({ ok: false, error: result.error });
  }
  return reply.code(202).send({ ok: true });
});

const close = async (): Promise<void> => {
  await app.close();
  await intakeQueue.close();
};

process.on("SIGINT", () => {
  void close().finally(() => process.exit(0));
});
process.on("SIGTERM", () => {
  void close().finally(() => process.exit(0));
});

app.listen({ port: env.GATEWAY_PORT, host: "0.0.0.0" }).then(() => {
  logger.info({ port: env.GATEWAY_PORT }, "gateway started");
});
