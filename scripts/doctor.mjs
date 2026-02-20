import fs from "node:fs/promises";
import path from "node:path";

function parseEnv(text) {
  const env = {};
  for (const raw of text.split(/\r?\n/)) {
    const line = raw.trim();
    if (!line || line.startsWith("#")) continue;
    const index = line.indexOf("=");
    if (index <= 0) continue;
    const key = line.slice(0, index).trim();
    let value = line.slice(index + 1).trim();
    if (
      (value.startsWith("\"") && value.endsWith("\"")) ||
      (value.startsWith("'") && value.endsWith("'"))
    ) {
      value = value.slice(1, -1);
    }
    env[key] = value;
  }
  return env;
}

function boolSet(value) {
  return Boolean(value && value.trim().length > 0);
}

async function fetchJson(url, options) {
  try {
    const response = await fetch(url, { ...options, signal: AbortSignal.timeout(5000) });
    const text = await response.text();
    let parsed = text;
    try {
      parsed = text ? JSON.parse(text) : {};
    } catch {
      // keep raw text
    }
    return { ok: response.ok, status: response.status, body: parsed };
  } catch (error) {
    return { ok: false, error: error instanceof Error ? error.message : "request_failed" };
  }
}

async function checkService(service, port) {
  const result = await fetchJson(`http://localhost:${port}/healthz`);
  return {
    service,
    ok: result.ok,
    status: result.status,
    body: result.body,
    error: result.error
  };
}

function formatBool(label, value) {
  return `${value ? "OK " : "NO "} ${label}`;
}

async function run() {
  const cwd = process.cwd();
  const envPath = path.join(cwd, ".env");
  const args = new Set(process.argv.slice(2));
  const runSmtpTest = args.has("--smtp-test");

  const envText = await fs.readFile(envPath, "utf8");
  const env = parseEnv(envText);

  const ports = {
    gateway: Number(env.GATEWAY_PORT || 3000),
    core: Number(env.CORE_PORT || 3001),
    connectors: Number(env.CONNECTORS_PORT || 3002),
    detector: Number(env.DETECTOR_PORT || 3003),
    notifier: Number(env.NOTIFIER_PORT || 3004)
  };

  console.log("Dirt-Cheap-Tickets Doctor");
  console.log("=========================");

  console.log("\nConfig");
  console.log(formatBool("DATABASE_URL set", boolSet(env.DATABASE_URL)));
  console.log(formatBool("REDIS_URL set", boolSet(env.REDIS_URL)));
  console.log(formatBool("OPERATOR_SECRET set", boolSet(env.OPERATOR_SECRET)));
  console.log(formatBool("SMTP_USER set", boolSet(env.SMTP_USER)));
  console.log(formatBool("SMTP_PASS set", boolSet(env.SMTP_PASS)));
  console.log(`SMTP_FROM_EMAIL: ${env.SMTP_FROM_EMAIL || "(empty)"}`);
  console.log(`INBOUND_ALERT_EMAIL: ${env.INBOUND_ALERT_EMAIL || "(empty)"}`);

  console.log("\nService Health");
  const checks = await Promise.all([
    checkService("gateway", ports.gateway),
    checkService("core", ports.core),
    checkService("connectors", ports.connectors),
    checkService("detector", ports.detector),
    checkService("notifier", ports.notifier)
  ]);

  for (const check of checks) {
    if (check.ok) {
      console.log(`OK  ${check.service} :${ports[check.service]}`);
    } else {
      console.log(`NO  ${check.service} :${ports[check.service]} ${check.error ?? `status ${check.status}`}`);
    }
  }

  const secret = env.OPERATOR_SECRET;
  let adminOk = false;
  if (secret) {
    console.log("\nCore Health Report");
    const health = await fetchJson(`http://localhost:${ports.core}/admin/health/report`, {
      headers: { "x-operator-secret": secret }
    });

    if (health.ok && typeof health.body === "object" && health.body !== null) {
      adminOk = true;
      const body = health.body;
      const watches = body.watches ?? {};
      const queues = body.queues ?? {};
      const notifications = body.notifications ?? {};
      const sources = body.sources ?? [];
      const connectorQueue = queues.connectorFetch ?? {};

      console.log(`Active watches: ${String(watches.active ?? 0)}`);
      console.log(`Stale watches: ${String(watches.stale ?? 0)}`);
      console.log(
        `Connector queue - waiting: ${String(connectorQueue.waiting ?? 0)}, active: ${String(
          connectorQueue.active ?? 0
        )}, failed: ${String(connectorQueue.failed ?? 0)}`
      );
      console.log(
        `Notifications - sent24h: ${String(notifications.sent24h ?? 0)}, failed24h: ${String(
          notifications.failed24h ?? 0
        )}`
      );

      if (Array.isArray(sources) && sources.length > 0) {
        for (const source of sources) {
          console.log(
            `Source ${String(source.source)} - success1h: ${String(
              source.successRuns1h ?? 0
            )}, failed1h: ${String(source.failedRuns1h ?? 0)}`
          );
        }
      }

      const recentFailures = notifications.recentFailures ?? [];
      if (Array.isArray(recentFailures) && recentFailures.length > 0) {
        console.log("Recent notification failures:");
        for (const failure of recentFailures.slice(0, 3)) {
          console.log(
            `- ${String(failure.channel)} to ${String(failure.destination)}: ${String(
              failure.error ?? "unknown_error"
            )}`
          );
        }
      }
    } else {
      console.log(`NO  core admin health report failed: ${health.error ?? `status ${health.status}`}`);
    }
  } else {
    console.log("\nNO  OPERATOR_SECRET missing, skipping core admin health report");
  }

  if (runSmtpTest) {
    console.log("\nSMTP Test");
    if (!secret) {
      console.log("NO  OPERATOR_SECRET missing, cannot call notifier test endpoint");
    } else {
      const destination = env.INBOUND_ALERT_EMAIL || env.SMTP_USER || env.SENDGRID_TO_EMAIL;
      const result = await fetchJson(`http://localhost:${ports.notifier}/admin/test-email`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "x-operator-secret": secret
        },
        body: JSON.stringify({
          to: destination || undefined,
          message: "DCT doctor SMTP test email."
        })
      });

      if (result.ok && typeof result.body === "object" && result.body !== null) {
        console.log(`OK  SMTP test sent to ${String(result.body.destination ?? destination ?? "(unknown)")}`);
      } else {
        console.log(`NO  SMTP test failed: ${result.error ?? JSON.stringify(result.body ?? {})}`);
      }
    }
  } else {
    console.log("\nTip: run `pnpm checkup:smtp` to send a live test email.");
  }

  const hardFailures = checks.filter((check) => !check.ok).length > 0 || !adminOk;
  console.log(`\nResult: ${hardFailures ? "FAIL" : "PASS"}`);
  if (hardFailures) {
    process.exitCode = 1;
  }
}

run().catch((error) => {
  console.error("Doctor failed", error);
  process.exitCode = 1;
});
