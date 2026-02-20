CREATE EXTENSION IF NOT EXISTS "pgcrypto";
DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM pg_available_extensions
    WHERE name = 'timescaledb'
  ) THEN
    CREATE EXTENSION IF NOT EXISTS "timescaledb";
  ELSE
    RAISE NOTICE 'timescaledb not available, using regular PostgreSQL tables';
  END IF;
END $$;

CREATE TABLE IF NOT EXISTS watch_specs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  operator_label TEXT NOT NULL DEFAULT 'default',
  status TEXT NOT NULL CHECK (status IN ('ACTIVE', 'PAUSED', 'PENDING_CONFIRMATION')),
  source TEXT NOT NULL,
  speed_profile TEXT NOT NULL CHECK (speed_profile IN ('FAST', 'NORMAL')),
  event_query TEXT NOT NULL,
  event_date_iso TEXT,
  city TEXT,
  venue TEXT,
  desired_quantity INTEGER NOT NULL,
  max_all_in_price NUMERIC,
  seating_constraints TEXT,
  thresholds JSONB NOT NULL,
  poll_cadence_min_seconds INTEGER NOT NULL DEFAULT 30,
  poll_cadence_max_seconds INTEGER NOT NULL DEFAULT 90,
  timezone TEXT NOT NULL,
  currency TEXT NOT NULL,
  sms_destination TEXT,
  email_destination TEXT,
  preferred_channel TEXT NOT NULL DEFAULT 'email' CHECK (preferred_channel IN ('sms', 'email')),
  next_poll_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_polled_at TIMESTAMPTZ,
  last_min_price NUMERIC,
  volatility_score NUMERIC NOT NULL DEFAULT 0,
  canonical_event_id UUID,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS watch_parse_sessions (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  inbound_message_id TEXT NOT NULL UNIQUE,
  sender TEXT NOT NULL,
  channel TEXT NOT NULL CHECK (channel IN ('sms', 'email')),
  raw_text TEXT NOT NULL,
  parsed_payload JSONB,
  candidate_events JSONB,
  status TEXT NOT NULL CHECK (status IN ('PENDING', 'NEEDS_CONFIRMATION', 'COMPLETED', 'FAILED')),
  watch_id UUID,
  received_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS canonical_events (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name TEXT NOT NULL,
  venue_name TEXT NOT NULL,
  city TEXT NOT NULL,
  local_datetime_iso TEXT NOT NULL,
  timezone TEXT NOT NULL,
  source_hints JSONB NOT NULL DEFAULT '[]'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS event_aliases (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  canonical_event_id UUID NOT NULL REFERENCES canonical_events(id) ON DELETE CASCADE,
  source TEXT NOT NULL,
  external_event_id TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE(source, external_event_id)
);

CREATE TABLE IF NOT EXISTS listing_observations (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  watch_id UUID NOT NULL REFERENCES watch_specs(id) ON DELETE CASCADE,
  canonical_event_id UUID NOT NULL REFERENCES canonical_events(id) ON DELETE CASCADE,
  source TEXT NOT NULL,
  external_event_id TEXT NOT NULL,
  external_listing_id TEXT NOT NULL,
  deep_link TEXT NOT NULL,
  section_name TEXT,
  row_name TEXT,
  quantity_available INTEGER NOT NULL,
  display_price NUMERIC NOT NULL,
  all_in_price NUMERIC,
  fee_estimate NUMERIC,
  currency TEXT NOT NULL,
  completeness TEXT NOT NULL CHECK (completeness IN ('ALL_IN', 'ESTIMATED_TOTAL', 'DISPLAY_ONLY')),
  observed_at TIMESTAMPTZ NOT NULL,
  request_region TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM pg_extension
    WHERE extname = 'timescaledb'
  ) THEN
    PERFORM create_hypertable('listing_observations', 'observed_at', if_not_exists => TRUE);
  END IF;
END $$;

CREATE TABLE IF NOT EXISTS deal_signals (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  watch_id UUID NOT NULL REFERENCES watch_specs(id) ON DELETE CASCADE,
  source TEXT NOT NULL,
  type TEXT NOT NULL CHECK (type IN ('OUTLIER', 'DROP')),
  external_listing_id TEXT,
  current_price NUMERIC NOT NULL,
  baseline_price NUMERIC NOT NULL,
  drop_percent NUMERIC NOT NULL,
  confidence NUMERIC NOT NULL,
  evidence JSONB NOT NULL,
  dedupe_key TEXT NOT NULL,
  reconfirmed BOOLEAN NOT NULL DEFAULT FALSE,
  observed_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_deal_signals_dedupe_time ON deal_signals(dedupe_key, created_at DESC);

CREATE TABLE IF NOT EXISTS notifications (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  signal_id UUID NOT NULL REFERENCES deal_signals(id) ON DELETE CASCADE,
  watch_id UUID NOT NULL REFERENCES watch_specs(id) ON DELETE CASCADE,
  channel TEXT NOT NULL CHECK (channel IN ('sms', 'email')),
  destination TEXT NOT NULL,
  provider_message_id TEXT,
  status TEXT NOT NULL CHECK (status IN ('QUEUED', 'SENT', 'FAILED')),
  error_message TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  sent_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS connector_runs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  watch_id UUID NOT NULL REFERENCES watch_specs(id) ON DELETE CASCADE,
  source TEXT NOT NULL,
  reason TEXT NOT NULL,
  latency_ms INTEGER NOT NULL,
  freshness_ms INTEGER NOT NULL,
  listing_count INTEGER NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('SUCCESS', 'FAILED')),
  error_class TEXT,
  error_message TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw_payload_archive (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  source TEXT NOT NULL,
  payload JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE OR REPLACE FUNCTION set_updated_at() RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_watch_specs_updated_at ON watch_specs;
CREATE TRIGGER trg_watch_specs_updated_at
BEFORE UPDATE ON watch_specs
FOR EACH ROW EXECUTE FUNCTION set_updated_at();
