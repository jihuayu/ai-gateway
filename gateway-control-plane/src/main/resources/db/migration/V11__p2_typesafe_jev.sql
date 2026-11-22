-- P2 Batch 5 (#49): Add TypeSafe AI Jev (typesafe / jev) protocol support

ALTER TABLE providers DROP CONSTRAINT IF EXISTS providers_protocol_check;
ALTER TABLE providers ADD CONSTRAINT providers_protocol_check CHECK (protocol IN ('openai', 'anthropic', 'gemini', 'cohere', 'jina', 'typesafe', 'jev'));
