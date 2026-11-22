-- P2 Batch 3: MCP Proxy Gateway Governance & End-to-End TTFT Telemetry (#38, #39)

CREATE TABLE IF NOT EXISTS mcp_servers (
    id UUID PRIMARY KEY,
    workspace_id UUID NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    alias TEXT NOT NULL,
    transport TEXT NOT NULL DEFAULT 'streamable_http' CHECK (transport IN ('streamable_http', 'sse')),
    endpoint_url TEXT NOT NULL,
    encrypted_secret TEXT,
    allowed_tools JSONB NOT NULL DEFAULT '[]'::jsonb,
    timeout_ms INT NOT NULL DEFAULT 15000,
    enabled BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (workspace_id, alias)
);

CREATE INDEX IF NOT EXISTS idx_mcp_servers_workspace_enabled ON mcp_servers(workspace_id, enabled);

ALTER TABLE api_keys
    ADD COLUMN IF NOT EXISTS allowed_mcp_tools JSONB NOT NULL DEFAULT '[]'::jsonb;

ALTER TABLE attempts
    ADD COLUMN IF NOT EXISTS ttft_ms BIGINT;

ALTER TABLE requests
    ADD COLUMN IF NOT EXISTS ttft_ms BIGINT;
