create extension if not exists pgcrypto;

create table if not exists public.batches (
  id uuid primary key default gen_random_uuid(),
  institute_name text not null,
  batch_name text not null,
  status text not null default 'submitted',
  total_cards integer not null default 0,
  created_at text,
  submitted_at text
);

create table if not exists public.records (
  id uuid primary key default gen_random_uuid(),
  batch_id uuid not null references public.batches(id) on delete cascade,
  institute_name text not null,
  serial_no text not null,
  name text,
  profile_type text,
  saved_at text,
  submitted_at text,
  payload jsonb not null default '{}'::jsonb
);

create index if not exists idx_batches_institute_name on public.batches (institute_name);
create index if not exists idx_records_batch_id on public.records (batch_id);
create index if not exists idx_records_institute_name on public.records (institute_name);
create index if not exists idx_records_serial_no on public.records (serial_no);
