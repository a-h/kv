-- kv type + key index
create index if not exists idx_kv_type_key on kv(type, key);

-- stream type + seq index
create index if not exists idx_stream_type_seq on stream(type, seq);
