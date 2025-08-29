-- Create migration version table to track schema version
create table if not exists migration_version (
    version integer primary key,
    applied_at text not null default (datetime('now'))
);

-- Insert initial version if table is empty
insert into migration_version (version, applied_at) 
select 1, datetime('now') 
where not exists (select 1 from migration_version);
