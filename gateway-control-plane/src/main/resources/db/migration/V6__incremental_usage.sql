-- #21 Incremental usage aggregation & index optimization for 10k+ ledger history
CREATE TABLE IF NOT EXISTS usage_request_state (
  request_id uuid PRIMARY KEY,
  project_id uuid NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
  bucket timestamptz NOT NULL,
  actual_micros bigint NOT NULL DEFAULT 0,
  estimated_micros bigint NOT NULL DEFAULT 0,
  input_tokens bigint NOT NULL DEFAULT 0,
  output_tokens bigint NOT NULL DEFAULT 0,
  counted boolean NOT NULL DEFAULT false,
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ledger_request_id_idx ON ledger (request_id) WHERE request_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS outbox_project_unpublished_idx ON outbox (project_id, created_at) WHERE status != 'published';
