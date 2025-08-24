-- Create migration version table to track schema version
CREATE TABLE IF NOT EXISTS migration_version (
    version INTEGER PRIMARY KEY,
    applied_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Insert initial version if table is empty
INSERT INTO migration_version (version, applied_at) 
SELECT 1, datetime('now') 
WHERE NOT EXISTS (SELECT 1 FROM migration_version);
