-- P2 Batch 4 (#47): Expand provider protocols (cohere, jina) for multi-protocol Embeddings, Rerank/Rank, and Image Generation

ALTER TABLE providers DROP CONSTRAINT IF EXISTS providers_protocol_check;
ALTER TABLE providers ADD CONSTRAINT providers_protocol_check CHECK (protocol IN ('openai', 'anthropic', 'gemini', 'cohere', 'jina'));
