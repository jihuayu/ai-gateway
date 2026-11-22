-- Published aliases must point at their own immutable route version.
ALTER TABLE route_versions ADD CONSTRAINT route_version_id_alias_unique UNIQUE(id,alias_id);
ALTER TABLE aliases ADD CONSTRAINT aliases_version_owner_fk FOREIGN KEY(current_version_id,id)
  REFERENCES route_versions(id,alias_id);

-- Bounded, indexed analytics projection and replay diagnostics.
CREATE TABLE usage_hourly (
  project_id uuid NOT NULL REFERENCES projects(id), bucket timestamptz NOT NULL,
  actual_micros bigint NOT NULL DEFAULT 0, estimated_micros bigint NOT NULL DEFAULT 0,
  input_tokens bigint NOT NULL DEFAULT 0, output_tokens bigint NOT NULL DEFAULT 0,
  event_count bigint NOT NULL DEFAULT 0, updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY(project_id,bucket)
);
CREATE TABLE dead_letters(event_id text PRIMARY KEY,envelope jsonb NOT NULL,reason text NOT NULL,created_at timestamptz DEFAULT now());
CREATE INDEX project_members_user_idx ON project_members(user_id,project_id);
CREATE INDEX model_targets_provider_idx ON model_targets(provider_id);
CREATE INDEX aliases_project_idx ON aliases(project_id);
CREATE INDEX requests_key_idx ON requests(key_id);
CREATE INDEX sessions_workspace_idx ON sessions(workspace_id);
CREATE INDEX audit_project_idx ON audit_events(project_id,created_at DESC);
