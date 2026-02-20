import { Queue } from "bullmq";
import IORedis from "ioredis";
import pino from "pino";
import { env } from "@dct/config";

export const logger = pino({ level: env.LOG_LEVEL });

export const redis = new IORedis(env.REDIS_URL, {
  maxRetriesPerRequest: null,
  enableReadyCheck: false
});

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

export function makeQueue<T>(name: string): Queue<T> {
  return new Queue<T>(name, { connection: redis });
}
