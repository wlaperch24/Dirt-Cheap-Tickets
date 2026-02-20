import dotenv from "dotenv";
import { z } from "zod";

dotenv.config();

const envSchema = z.object({
  NODE_ENV: z.string().default("development"),
  LOG_LEVEL: z.string().default("info"),
  DATABASE_URL: z.string().url().or(z.string().startsWith("postgres://")),
  REDIS_URL: z.string().url().or(z.string().startsWith("redis://")),
  OPERATOR_SECRET: z.string().min(6),
  DEFAULT_TIMEZONE: z.string().default("America/New_York"),
  DEFAULT_CURRENCY: z.string().default("USD"),

  GATEWAY_PORT: z.coerce.number().int().positive().default(3000),
  CORE_PORT: z.coerce.number().int().positive().default(3001),
  CONNECTORS_PORT: z.coerce.number().int().positive().default(3002),
  DETECTOR_PORT: z.coerce.number().int().positive().default(3003),
  NOTIFIER_PORT: z.coerce.number().int().positive().default(3004),

  TICKETMASTER_API_KEY: z.string().optional(),
  STUBHUB_CLIENT_ID: z.string().optional(),
  STUBHUB_CLIENT_SECRET: z.string().optional(),
  STUBHUB_USE_MOCK: z
    .string()
    .optional()
    .transform((v) => v !== "false"),
  ENABLE_TICKETMASTER_PRICING: z
    .string()
    .optional()
    .transform((v) => v !== "false"),
  ALERT_EMAIL_ONLY: z
    .string()
    .optional()
    .transform((v) => v !== "false"),
  ALERT_DROP_ONLY: z
    .string()
    .optional()
    .transform((v) => v !== "false"),
  DEFAULT_DROP_PERCENT: z.coerce.number().positive().max(0.95).default(0.2),
  ACTIVATE_PHRASE: z.string().default("Activate dirt cheap tickets"),
  DEACTIVATE_PHRASE: z.string().default("Deactivate dirt cheap tickets"),

  TWILIO_WEBHOOK_VALIDATE: z
    .string()
    .optional()
    .transform((v) => v === "true"),
  TWILIO_AUTH_TOKEN: z.string().optional(),
  TWILIO_ACCOUNT_SID: z.string().optional(),
  TWILIO_FROM_NUMBER: z.string().optional(),
  TWILIO_TO_NUMBER: z.string().optional(),

  SENDGRID_INBOUND_VALIDATE: z
    .string()
    .optional()
    .transform((v) => v === "true"),
  SENDGRID_WEBHOOK_KEY: z.string().optional(),
  SENDGRID_API_KEY: z.string().optional(),
  SENDGRID_FROM_EMAIL: z.string().optional(),
  SENDGRID_TO_EMAIL: z.string().optional(),
  INBOUND_ALERT_EMAIL: z.string().optional(),
  OPERATOR_CONTROL_EMAIL: z.string().optional()
});

export type AppEnv = z.infer<typeof envSchema>;

export const env: AppEnv = envSchema.parse(process.env);
