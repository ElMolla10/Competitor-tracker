-- ============================================================
-- Supabase schema for the competitor price tracker
-- Run this once in the Supabase SQL editor to create all tables
-- ============================================================

-- Enable the uuid-ossp extension (already on by default in Supabase)
create extension if not exists "uuid-ossp";

-- ----------------------------------------------------------
-- companies
-- ----------------------------------------------------------
create table if not exists companies (
    id          uuid primary key default uuid_generate_v4(),
    name        text not null,
    category    text not null,
    pricing_url text not null unique,
    created_at  timestamptz not null default now()
);

-- ----------------------------------------------------------
-- snapshots
-- ----------------------------------------------------------
create table if not exists snapshots (
    id          uuid primary key default uuid_generate_v4(),
    company_id  uuid not null references companies(id) on delete cascade,
    content     text not null,
    scraped_at  timestamptz not null default now()
);

create index if not exists snapshots_company_scraped
    on snapshots (company_id, scraped_at desc);

-- ----------------------------------------------------------
-- changes
-- ----------------------------------------------------------
create table if not exists changes (
    id                   uuid primary key default uuid_generate_v4(),
    company_id           uuid not null references companies(id) on delete cascade,
    change_summary       text not null,
    previous_snapshot_id uuid references snapshots(id),
    new_snapshot_id      uuid references snapshots(id),
    detected_at          timestamptz not null default now()
);

create index if not exists changes_company_detected
    on changes (company_id, detected_at desc);
