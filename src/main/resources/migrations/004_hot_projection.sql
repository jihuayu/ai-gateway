-- The stream is acknowledged only after the canonical transaction commits.
-- Keep revision tombstones independent of metadata retention to prevent resurrection.
CREATE TABLE hot_projection_state (
  request_id uuid PRIMARY KEY,
  epoch uuid NOT NULL,
  revision bigint NOT NULL CHECK(revision > 0),
  updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE hot_runtime_manifests (
  namespace text PRIMARY KEY,
  epoch uuid NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
