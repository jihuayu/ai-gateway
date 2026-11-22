-- AI Gateway canonical schema (PostgreSQL 15+). Money is integer micro-USD.
-- All timestamps are timestamptz. No prompt/response content is stored.

CREATE TABLE IF NOT EXISTS workspaces (
  id uuid PRIMARY KEY,
  name text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS users (
  id uuid PRIMARY KEY,
  workspace_id uuid NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
  email text NOT NULL,
  password_hash text NOT NULL,
  role text NOT NULL CHECK (role IN ('admin','developer','viewer')),
  disabled_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (workspace_id, email)
);
CREATE UNIQUE INDEX IF NOT EXISTS users_email_lower_idx ON users (lower(email));

CREATE TABLE IF NOT EXISTS projects (
  id uuid PRIMARY KEY,
  workspace_id uuid NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
  name text NOT NULL,
  budget_micros bigint CHECK (budget_micros IS NULL OR budget_micros >= 0),
  rpm integer CHECK (rpm IS NULL OR rpm > 0),
  concurrency integer CHECK (concurrency IS NULL OR concurrency > 0),
  max_output_tokens integer CHECK (max_output_tokens IS NULL OR max_output_tokens > 0),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (workspace_id, name)
);

CREATE TABLE IF NOT EXISTS project_members (
  project_id uuid NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
  user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (project_id, user_id)
);

CREATE TABLE IF NOT EXISTS sessions (
  id uuid PRIMARY KEY,
  token_hash text NOT NULL UNIQUE,
  csrf_hash text NOT NULL,
  user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  workspace_id uuid NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
  created_at timestamptz NOT NULL DEFAULT now(),
  expires_at timestamptz NOT NULL,
  revoked_at timestamptz
);
CREATE INDEX IF NOT EXISTS sessions_user_idx ON sessions (user_id) WHERE revoked_at IS NULL;

CREATE TABLE IF NOT EXISTS invitations (
  id uuid PRIMARY KEY,
  workspace_id uuid NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
  email text NOT NULL,
  role text NOT NULL CHECK (role IN ('admin','developer','viewer')),
  token_hash text NOT NULL UNIQUE,
  project_ids jsonb NOT NULL DEFAULT '[]'::jsonb,
  created_by uuid REFERENCES users(id) ON DELETE SET NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  expires_at timestamptz NOT NULL,
  accepted_at timestamptz
);

CREATE TABLE IF NOT EXISTS providers (
  id uuid PRIMARY KEY,
  workspace_id uuid NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
  name text NOT NULL,
  protocol text NOT NULL DEFAULT 'openai' CHECK (protocol IN ('openai')),
  base_url text NOT NULL,
  encrypted_secret text NOT NULL,
  enabled boolean NOT NULL DEFAULT true,
  last_test_status text,
  last_test_at timestamptz,
  last_test_error text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (workspace_id, name)
);

CREATE TABLE IF NOT EXISTS model_targets (
  id uuid PRIMARY KEY,
  provider_id uuid NOT NULL REFERENCES providers(id) ON DELETE CASCADE,
  upstream_model text NOT NULL,
  input_price bigint NOT NULL CHECK (input_price >= 0),
  output_price bigint NOT NULL CHECK (output_price >= 0),
  capabilities jsonb NOT NULL DEFAULT '[]'::jsonb,
  enabled boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (provider_id, upstream_model)
);

CREATE TABLE IF NOT EXISTS aliases (
  id uuid PRIMARY KEY,
  project_id uuid NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
  name text NOT NULL,
  current_version_id uuid,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (project_id, name)
);

CREATE TABLE IF NOT EXISTS route_versions (
  id uuid PRIMARY KEY,
  alias_id uuid NOT NULL REFERENCES aliases(id) ON DELETE CASCADE,
  version integer NOT NULL CHECK (version > 0),
  targets jsonb NOT NULL,
  capabilities jsonb NOT NULL DEFAULT '[]'::jsonb,
  price_snapshot jsonb NOT NULL,
  status text NOT NULL DEFAULT 'draft' CHECK (status IN ('draft','published')),
  created_by uuid REFERENCES users(id) ON DELETE SET NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  published_at timestamptz,
  UNIQUE (alias_id, version)
);
ALTER TABLE aliases DROP CONSTRAINT IF EXISTS aliases_current_version_fk;
ALTER TABLE aliases ADD CONSTRAINT aliases_current_version_fk
  FOREIGN KEY (current_version_id) REFERENCES route_versions(id) ON DELETE SET NULL;

CREATE TABLE IF NOT EXISTS api_keys (
  id uuid PRIMARY KEY,
  project_id uuid NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
  name text NOT NULL,
  key_hash text NOT NULL UNIQUE,
  prefix text NOT NULL,
  created_by uuid REFERENCES users(id) ON DELETE SET NULL,
  allowed_models jsonb NOT NULL DEFAULT '[]'::jsonb,
  expires_at timestamptz,
  revoked_at timestamptz,
  key_rpm integer CHECK (key_rpm IS NULL OR key_rpm > 0),
  created_at timestamptz NOT NULL DEFAULT now(),
  last_used_at timestamptz
);
CREATE INDEX IF NOT EXISTS api_keys_project_idx ON api_keys (project_id);

CREATE TABLE IF NOT EXISTS requests (
  request_id uuid PRIMARY KEY,
  project_id uuid NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
  key_id uuid REFERENCES api_keys(id) ON DELETE SET NULL,
  period text NOT NULL,
  reserve_micros bigint NOT NULL CHECK (reserve_micros >= 0),
  route_version jsonb,
  price_snapshot jsonb NOT NULL,
  status text NOT NULL,
  alias_name text,
  error_code text,
  input_tokens bigint CHECK (input_tokens IS NULL OR input_tokens >= 0),
  output_tokens bigint CHECK (output_tokens IS NULL OR output_tokens >= 0),
  actual_micros bigint CHECK (actual_micros IS NULL OR actual_micros >= 0),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS requests_project_created_idx ON requests (project_id, created_at DESC, request_id DESC);

CREATE TABLE IF NOT EXISTS attempts (
  id uuid PRIMARY KEY,
  request_id uuid NOT NULL REFERENCES requests(request_id) ON DELETE CASCADE,
  attempt_no integer NOT NULL CHECK (attempt_no > 0),
  target_id uuid REFERENCES model_targets(id) ON DELETE SET NULL,
  status text NOT NULL,
  error_code text,
  started_at timestamptz NOT NULL DEFAULT now(),
  finished_at timestamptz,
  input_tokens bigint,
  output_tokens bigint,
  cost_micros bigint,
  UNIQUE (request_id, attempt_no)
);

CREATE TABLE IF NOT EXISTS reservations (
  request_id uuid PRIMARY KEY REFERENCES requests(request_id) ON DELETE CASCADE,
  project_id uuid NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
  period text NOT NULL,
  reserved_micros bigint NOT NULL CHECK (reserved_micros >= 0),
  actual_micros bigint CHECK (actual_micros IS NULL OR actual_micros >= 0),
  status text NOT NULL,
  lease_expires_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS reservations_project_idx ON reservations (project_id, status);

CREATE TABLE IF NOT EXISTS ledger (
  event_id text PRIMARY KEY,
  request_id uuid REFERENCES requests(request_id) ON DELETE SET NULL,
  project_id uuid NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
  period text NOT NULL,
  amount_micros bigint NOT NULL,
  kind text NOT NULL,
  input_tokens bigint,
  output_tokens bigint,
  estimated boolean NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ledger_project_period_idx ON ledger (project_id, period, created_at);

CREATE TABLE IF NOT EXISTS outbox (
  id uuid PRIMARY KEY,
  event_id text NOT NULL UNIQUE,
  event_type text NOT NULL,
  project_id uuid,
  payload jsonb NOT NULL,
  status text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','leased','published','failed')),
  attempts integer NOT NULL DEFAULT 0,
  available_at timestamptz NOT NULL DEFAULT now(),
  created_at timestamptz NOT NULL DEFAULT now(),
  published_at timestamptz
);
CREATE INDEX IF NOT EXISTS outbox_pending_idx ON outbox (available_at) WHERE status IN ('pending','leased');

CREATE TABLE IF NOT EXISTS consumer_inbox (
  consumer_name text NOT NULL,
  event_id text NOT NULL,
  project_id uuid,
  processed_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer_name, event_id)
);

CREATE TABLE IF NOT EXISTS audit_events (
  id uuid PRIMARY KEY,
  workspace_id uuid NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
  project_id uuid,
  actor_user_id uuid REFERENCES users(id) ON DELETE SET NULL,
  action text NOT NULL,
  entity_type text NOT NULL,
  entity_id text,
  changes jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS audit_workspace_created_idx ON audit_events (workspace_id, created_at DESC);

CREATE TABLE IF NOT EXISTS settings (
  workspace_id uuid NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
  key text NOT NULL,
  value jsonb NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (workspace_id, key)
);
