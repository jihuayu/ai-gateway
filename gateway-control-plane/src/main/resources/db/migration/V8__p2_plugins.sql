-- P2 Batch 2: 3-Tier Plugin Architecture (BUILTIN, JAVA_SPI, JS, WEBHOOK) & Scoped Bindings (#36, #41, #42, #37)

CREATE TABLE IF NOT EXISTS plugins (
    id UUID PRIMARY KEY,
    workspace_id UUID REFERENCES workspaces(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    type TEXT NOT NULL CHECK (type IN ('BUILTIN', 'JAVA_SPI', 'JS', 'WEBHOOK')),
    version INT NOT NULL DEFAULT 1,
    priority INT NOT NULL DEFAULT 100,
    stages JSONB NOT NULL DEFAULT '["PRE_ADMIT"]'::jsonb,
    source_code TEXT,
    code_sha256 TEXT,
    jar_path TEXT,
    class_name TEXT,
    config JSONB NOT NULL DEFAULT '{}'::jsonb,
    fail_mode TEXT NOT NULL DEFAULT 'FAIL_CLOSED' CHECK (fail_mode IN ('FAIL_OPEN', 'FAIL_CLOSED')),
    timeout_ms INT NOT NULL DEFAULT 50,
    max_statements INT NOT NULL DEFAULT 10000,
    enabled BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (workspace_id, name)
);

CREATE TABLE IF NOT EXISTS plugin_bindings (
    id UUID PRIMARY KEY,
    plugin_id UUID NOT NULL REFERENCES plugins(id) ON DELETE CASCADE,
    scope_type TEXT NOT NULL CHECK (scope_type IN ('GLOBAL', 'WORKSPACE', 'PROJECT', 'ALIAS', 'API_KEY')),
    scope_id TEXT NOT NULL DEFAULT '*',
    priority INT NOT NULL DEFAULT 100,
    config_override JSONB NOT NULL DEFAULT '{}'::jsonb,
    enabled BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_plugins_workspace_enabled ON plugins(workspace_id, enabled);
CREATE INDEX IF NOT EXISTS idx_plugin_bindings_scope ON plugin_bindings(scope_type, scope_id, enabled);
