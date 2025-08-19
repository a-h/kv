--- kv table
CREATE TABLE IF NOT EXISTS kv (
   key TEXT PRIMARY KEY,
   version INTEGER NOT NULL,
   value JSONB NOT NULL,
   created TIMESTAMPTZ NOT NULL
);

-- stream table
CREATE TABLE IF NOT EXISTS stream (
    seq      BIGSERIAL PRIMARY KEY,
    action   TEXT NOT NULL CHECK (action IN ('create','update','delete')),
    key      TEXT NOT NULL,
    version  INTEGER NOT NULL,
    value    JSONB NOT NULL,
    created  TIMESTAMPTZ NOT NULL
);

-- insert trigger function
CREATE OR REPLACE FUNCTION kv_stream_insert_fn()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO stream (action, key, version, value, created)
    VALUES ('create', NEW.key, NEW.version, NEW.value, NEW.created);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- update trigger function
CREATE OR REPLACE FUNCTION kv_stream_update_fn()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO stream (action, key, version, value, created)
    VALUES ('update', NEW.key, NEW.version, NEW.value, NEW.created);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- delete trigger function
CREATE OR REPLACE FUNCTION kv_stream_delete_fn()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO stream (action, key, version, value, created)
    VALUES ('delete', OLD.key, OLD.version, OLD.value, OLD.created);
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

-- attach triggers with WHEN clause
CREATE OR REPLACE TRIGGER kv_stream_insert
AFTER INSERT ON kv
FOR EACH ROW
WHEN (NEW.key NOT LIKE 'github.com/a-h/kv/stream/%')
EXECUTE FUNCTION kv_stream_insert_fn();

CREATE OR REPLACE TRIGGER kv_stream_update
AFTER UPDATE ON kv
FOR EACH ROW
WHEN (NEW.key NOT LIKE 'github.com/a-h/kv/stream/%')
EXECUTE FUNCTION kv_stream_update_fn();

CREATE OR REPLACE TRIGGER kv_stream_delete
AFTER DELETE ON kv
FOR EACH ROW
WHEN (OLD.key NOT LIKE 'github.com/a-h/kv/stream/%')
EXECUTE FUNCTION kv_stream_delete_fn();

-- locks table
CREATE TABLE locks (
    name TEXT PRIMARY KEY,
    locked_by TEXT NOT NULL,
    locked_at TIMESTAMPTZ NOT NULL,
    lock_until TIMESTAMPTZ NOT NULL
);
