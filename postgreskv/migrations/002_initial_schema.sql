--- kv table
create table if not exists kv (
   key text primary key,
   version integer not null,
   value jsonb not null,
   type text not null,
   created timestamptz not null
);

-- stream table
create table if not exists stream (
    seq      bigserial primary key,
    action   text not null check (action in ('create','update','delete')),
    key      text not null,
    version  integer not null,
    value    jsonb not null,
    type     text not null,
    created  timestamptz not null
);

-- insert trigger function
create or replace function kv_stream_insert_fn()
returns trigger as $$
begin
    insert into stream (action, key, version, value, type, created)
    values ('create', new.key, new.version, new.value, new.type, new.created);
    return new;
end;
$$ language plpgsql;

-- update trigger function
create or replace function kv_stream_update_fn()
returns trigger as $$
begin
    insert into stream (action, key, version, value, type, created)
    values ('update', new.key, new.version, new.value, new.type, new.created);
    return new;
end;
$$ language plpgsql;

-- delete trigger function
create or replace function kv_stream_delete_fn()
returns trigger as $$
begin
    insert into stream (action, key, version, value, type, created)
    values ('delete', old.key, old.version, old.value, old.type, old.created);
    return old;
end;
$$ language plpgsql;

-- attach triggers with WHEN clause
create or replace trigger kv_stream_insert
after insert on kv
for each row
when (new.key not like 'github.com/a-h/kv/stream/%')
execute function kv_stream_insert_fn();

create or replace trigger kv_stream_update
after update on kv
for each row
when (new.key not like 'github.com/a-h/kv/stream/%')
execute function kv_stream_update_fn();

create or replace trigger kv_stream_delete
after delete on kv
for each row
when (old.key not like 'github.com/a-h/kv/stream/%')
execute function kv_stream_delete_fn();

-- locks table
create table if not exists locks (
    name text primary key,
    locked_by text not null,
    locked_at timestamptz not null,
    lock_until timestamptz not null
);
