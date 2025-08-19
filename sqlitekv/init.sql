-- kv table
create table if not exists kv (key text primary key, version integer not null, value jsonb not null, created text not null) without rowid;

-- stream table
create table if not exists stream (
  seq      integer primary key autoincrement,
  action   text not null check (action in ('create','update','delete')),
  key      text not null,
  version  integer not null,
  value    jsonb not null,
  created  text not null
);

-- insert -> stream
create trigger if not exists kv_stream_insert
after insert on kv
when new.key not like 'github.com/a-h/kv/stream/%'
begin
  insert into stream (action, key, version, value, created)
  values ('create', new.key, new.version, new.value, new.created);
end;

-- update -> stream
create trigger if not exists kv_stream_update
after update on kv
when new.key not like 'github.com/a-h/kv/stream/%'
begin
  insert into stream (action, key, version, value, created)
  values ('update', new.key, new.version, new.value, new.created);
end;

-- delete -> stream
create trigger if not exists kv_stream_delete
after delete on kv
when old.key not like 'github.com/a-h/kv/stream/%'
begin
  insert into stream (action, key, version, value, created)
  values ('delete', old.key, old.version, old.value, old.created);
end;

-- locks
create table if not exists locks (
  name text primary key,
  locked_by text not null,
  locked_at text not null,
  expires_at text not null
);
