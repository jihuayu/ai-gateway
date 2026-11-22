-- P2 Batch 1 (#33, #34, #35): Multi-protocol providers, Project/Key TPM, Key-level budgets, and Prompt Cache / Reasoning token pricing

ALTER TABLE providers DROP CONSTRAINT IF EXISTS providers_protocol_check;
ALTER TABLE providers ADD CONSTRAINT providers_protocol_check CHECK (protocol IN ('openai', 'anthropic', 'gemini'));

ALTER TABLE projects ADD COLUMN IF NOT EXISTS tpm integer CHECK (tpm IS NULL OR tpm > 0);

ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS key_tpm integer CHECK (key_tpm IS NULL OR key_tpm > 0);
ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS budget_micros bigint CHECK (budget_micros IS NULL OR budget_micros >= 0);

ALTER TABLE model_targets ADD COLUMN IF NOT EXISTS cached_input_price bigint CHECK (cached_input_price IS NULL OR cached_input_price >= 0);
ALTER TABLE model_targets ADD COLUMN IF NOT EXISTS cache_write_price bigint CHECK (cache_write_price IS NULL OR cache_write_price >= 0);
ALTER TABLE model_targets ADD COLUMN IF NOT EXISTS reasoning_price bigint CHECK (reasoning_price IS NULL OR reasoning_price >= 0);

ALTER TABLE requests ADD COLUMN IF NOT EXISTS cached_input_tokens bigint CHECK (cached_input_tokens IS NULL OR cached_input_tokens >= 0);
ALTER TABLE requests ADD COLUMN IF NOT EXISTS reasoning_tokens bigint CHECK (reasoning_tokens IS NULL OR reasoning_tokens >= 0);

ALTER TABLE attempts ADD COLUMN IF NOT EXISTS cached_input_tokens bigint CHECK (cached_input_tokens IS NULL OR cached_input_tokens >= 0);
ALTER TABLE attempts ADD COLUMN IF NOT EXISTS reasoning_tokens bigint CHECK (reasoning_tokens IS NULL OR reasoning_tokens >= 0);
