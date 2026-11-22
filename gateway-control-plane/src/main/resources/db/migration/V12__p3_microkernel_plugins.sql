-- P3 Microkernel & Enterprise Plugin Suite (#51, #52, #53, #54):
-- Binding sample_rate (canary rollout) and FinOps cost attribution_tags on requests & ledger

ALTER TABLE plugin_bindings
    ADD COLUMN IF NOT EXISTS sample_rate DOUBLE PRECISION NOT NULL DEFAULT 1.0
    CHECK (sample_rate >= 0.0 AND sample_rate <= 1.0);

ALTER TABLE requests
    ADD COLUMN IF NOT EXISTS attribution_tags JSONB NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE ledger
    ADD COLUMN IF NOT EXISTS attribution_tags JSONB NOT NULL DEFAULT '{}'::jsonb;
