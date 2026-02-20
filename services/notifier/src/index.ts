import Fastify from "fastify";
import { Worker } from "bullmq";
import nodemailer from "nodemailer";
import { env } from "@dct/config";
import { notifyJobSchema, queueNames } from "@dct/contracts";
import { query } from "@dct/db";
import { incrementMetric, logger, redisConnection, renderMetrics } from "@dct/runtime";

const app = Fastify({ logger: false });

app.get("/healthz", async () => ({ ok: true, service: "notifier" }));
app.get("/metrics", async (_, reply) => {
  reply.type("text/plain; version=0.0.4");
  return renderMetrics("notifier");
});

async function sendSms(destination: string, message: string): Promise<{ sent: boolean; providerId?: string; error?: string }> {
  if (!env.TWILIO_ACCOUNT_SID || !env.TWILIO_AUTH_TOKEN || !env.TWILIO_FROM_NUMBER) {
    logger.info({ destination, message }, "sms not configured; logging only");
    return { sent: false, error: "twilio_not_configured" };
  }

  const body = new URLSearchParams({
    To: destination,
    From: env.TWILIO_FROM_NUMBER,
    Body: message
  });

  const auth = Buffer.from(`${env.TWILIO_ACCOUNT_SID}:${env.TWILIO_AUTH_TOKEN}`).toString("base64");
  const response = await fetch(
    `https://api.twilio.com/2010-04-01/Accounts/${env.TWILIO_ACCOUNT_SID}/Messages.json`,
    {
      method: "POST",
      headers: {
        Authorization: `Basic ${auth}`,
        "Content-Type": "application/x-www-form-urlencoded"
      },
      body: body.toString()
    }
  );

  if (!response.ok) {
    const text = await response.text();
    return { sent: false, error: text.slice(0, 400) };
  }

  const payload = (await response.json()) as { sid?: string };
  return { sent: true, providerId: payload.sid };
}

async function sendEmail(destination: string, message: string): Promise<{ sent: boolean; providerId?: string; error?: string }> {
  if (env.SMTP_USER && env.SMTP_PASS) {
    const smtpHost = env.SMTP_HOST || "smtp.gmail.com";
    const smtpPort = env.SMTP_PORT ?? 465;
    const smtpSecure = env.SMTP_SECURE;
    const smtpFrom = env.SMTP_FROM_EMAIL ?? env.SMTP_USER;

    const transport = nodemailer.createTransport({
      host: smtpHost,
      port: smtpPort,
      secure: smtpSecure,
      auth: {
        user: env.SMTP_USER,
        pass: env.SMTP_PASS
      }
    });

    try {
      const info = await transport.sendMail({
        from: smtpFrom,
        to: destination,
        subject: "Ticket Deal Alert",
        text: message
      });
      return { sent: true, providerId: info.messageId };
    } catch (error) {
      const messageText = error instanceof Error ? error.message : "smtp_send_failed";
      return { sent: false, error: messageText };
    }
  }

  if (!env.SENDGRID_API_KEY || !env.SENDGRID_FROM_EMAIL) {
    logger.info({ destination, message }, "email not configured; logging only");
    return { sent: false, error: "sendgrid_not_configured" };
  }

  const response = await fetch("https://api.sendgrid.com/v3/mail/send", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${env.SENDGRID_API_KEY}`,
      "Content-Type": "application/json"
    },
    body: JSON.stringify({
      from: { email: env.SENDGRID_FROM_EMAIL },
      personalizations: [{ to: [{ email: destination }] }],
      subject: "Ticket Deal Alert",
      content: [{ type: "text/plain", value: message }]
    })
  });

  if (!response.ok) {
    const text = await response.text();
    return { sent: false, error: text.slice(0, 400) };
  }

  const providerId = response.headers.get("x-message-id") ?? undefined;
  return { sent: true, providerId };
}

function requireOperatorAuth(headerValue: string | string[] | undefined): boolean {
  const provided = Array.isArray(headerValue) ? headerValue[0] : headerValue;
  return provided === env.OPERATOR_SECRET;
}

app.post("/admin/test-email", async (request, reply) => {
  if (!requireOperatorAuth(request.headers["x-operator-secret"])) {
    return reply.code(401).send({ ok: false, error: "unauthorized" });
  }

  const payload = (request.body ?? {}) as {
    to?: string;
    message?: string;
  };
  const destination =
    payload.to ?? env.INBOUND_ALERT_EMAIL ?? env.SMTP_USER ?? env.SENDGRID_TO_EMAIL;

  if (!destination) {
    return reply.code(400).send({ ok: false, error: "no_destination" });
  }

  const result = await sendEmail(
    destination,
    payload.message ?? "Dirt-Cheap-Tickets notifier SMTP test."
  );

  if (!result.sent) {
    return reply.code(502).send({ ok: false, destination, error: result.error ?? "send_failed" });
  }

  return { ok: true, destination, providerId: result.providerId ?? null };
});

const worker = new Worker(
  queueNames.notify,
  async (job) => {
    const data = notifyJobSchema.parse(job.data);

    if (!env.ALERT_EMAIL_ONLY && data.smsDestination) {
      const result = await sendSms(data.smsDestination, data.message);
      await query(
        `INSERT INTO notifications(signal_id, watch_id, channel, destination, provider_message_id, status, error_message, sent_at)
         VALUES($1, $2, 'sms', $3, $4, $5, $6, CASE WHEN $5 = 'SENT' THEN NOW() ELSE NULL END)`,
        [
          data.signalId,
          data.watchId,
          data.smsDestination,
          result.providerId ?? null,
          result.sent ? "SENT" : "FAILED",
          result.error ?? null
        ]
      );
      if (result.sent) incrementMetric("notifier_sms_sent_total");
      else incrementMetric("notifier_sms_failed_total");
    }

    const targetEmail = data.emailDestination ?? env.INBOUND_ALERT_EMAIL ?? env.SENDGRID_TO_EMAIL;
    if (targetEmail) {
      const result = await sendEmail(targetEmail, data.message);
      await query(
        `INSERT INTO notifications(signal_id, watch_id, channel, destination, provider_message_id, status, error_message, sent_at)
         VALUES($1, $2, 'email', $3, $4, $5, $6, CASE WHEN $5 = 'SENT' THEN NOW() ELSE NULL END)`,
        [
          data.signalId,
          data.watchId,
          targetEmail,
          result.providerId ?? null,
          result.sent ? "SENT" : "FAILED",
          result.error ?? null
        ]
      );
      if (result.sent) incrementMetric("notifier_email_sent_total");
      else incrementMetric("notifier_email_failed_total");
    }
  },
  {
    connection: redisConnection,
    concurrency: 12
  }
);

worker.on("failed", (_, err) => {
  incrementMetric("notifier_worker_failed_total");
  logger.error({ err }, "notifier worker failed");
});

process.on("SIGINT", () => {
  void Promise.all([worker.close(), app.close()]).finally(() => process.exit(0));
});
process.on("SIGTERM", () => {
  void Promise.all([worker.close(), app.close()]).finally(() => process.exit(0));
});

app.listen({ host: "0.0.0.0", port: env.NOTIFIER_PORT }).then(() => {
  logger.info({ port: env.NOTIFIER_PORT }, "notifier started");
});
