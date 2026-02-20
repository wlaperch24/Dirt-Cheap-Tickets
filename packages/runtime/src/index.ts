import { Queue, type ConnectionOptions } from "bullmq";
import pino from "pino";
import { env } from "@dct/config";

export const logger = pino({ level: env.LOG_LEVEL });

function toRedisConnectionOptions(redisUrl: string): ConnectionOptions {
  const parsed = new URL(redisUrl);
  const dbPath = parsed.pathname.replace("/", "").trim();
  return {
    host: parsed.hostname,
    port: parsed.port ? Number(parsed.port) : 6379,
    username: parsed.username || undefined,
    password: parsed.password || undefined,
    db: dbPath.length > 0 ? Number(dbPath) : 0,
    maxRetriesPerRequest: null,
    enableReadyCheck: false
  };
}

export const redisConnection: ConnectionOptions = toRedisConnectionOptions(env.REDIS_URL);

const queueConnectionOptions = {
  connection: redisConnection
} as const;

export const redisWorkerOptions = {
  connection: redisConnection,
  maxRetriesPerRequest: null,
  enableReadyCheck: false
} as const;

const metricState = new Map<string, number>();

export function incrementMetric(name: string, value = 1): void {
  metricState.set(name, (metricState.get(name) ?? 0) + value);
}

export function setMetric(name: string, value: number): void {
  metricState.set(name, value);
}

export function renderMetrics(serviceName: string): string {
  const lines = [`service_info{service=\"${serviceName}\"} 1`];
  for (const [key, value] of metricState.entries()) {
    lines.push(`${key} ${value}`);
  }
  return `${lines.join("\n")}\n`;
}

export function makeQueue(name: string): Queue {
  return new Queue(name, queueConnectionOptions);
}
