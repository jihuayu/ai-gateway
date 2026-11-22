CREATE TABLE IF NOT EXISTS billing_state (
  project_id uuid PRIMARY KEY REFERENCES projects(id) ON DELETE CASCADE,
  generation bigint NOT NULL DEFAULT 1 CHECK(generation>0),
  paused boolean NOT NULL DEFAULT false, dirty boolean NOT NULL DEFAULT false,
  redis_run_id text, last_reconciled_at timestamptz, updated_at timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE billing_state ADD COLUMN IF NOT EXISTS redis_run_id text;
