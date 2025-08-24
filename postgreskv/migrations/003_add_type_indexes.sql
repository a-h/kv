-- kv type + key index
CREATE INDEX IF NOT EXISTS idx_kv_type_key ON kv(type, key);

-- stream type + seq index
CREATE INDEX IF NOT EXISTS idx_stream_type_seq ON stream(type, seq);
