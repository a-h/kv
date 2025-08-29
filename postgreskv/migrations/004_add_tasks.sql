create table if not exists tasks (
  id text primary key,
  name text not null,
  payload text not null,
  status text not null check (status in ('pending','running','completed','failed','cancelled')) default 'pending',
  created timestamptz not null,
  scheduled_for timestamptz not null,
  started_at timestamptz,
  completed_at timestamptz,
  last_error text not null default '',
  retry_count integer not null default 0,
  max_retries integer not null default 3,
  timeout_seconds integer not null default 300,
  locked_by text not null default '',
  locked_at timestamptz,
  lock_expires_at timestamptz
);

create index if not exists idx_tasks_status_scheduled on tasks(status, scheduled_for);
create index if not exists idx_tasks_name_created on tasks(name, created);
create index if not exists idx_tasks_locked on tasks(locked_by, lock_expires_at);
create index if not exists idx_tasks_pending_collection on tasks(status, scheduled_for, name, lock_expires_at);
