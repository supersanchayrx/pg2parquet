-- Phase 0: Bootstrap the test table, publication, and replication slot.
-- This script runs automatically on first `docker compose up`.

CREATE TABLE IF NOT EXISTS users (
    id    SERIAL PRIMARY KEY,
    name  TEXT NOT NULL,
    email TEXT NOT NULL
);

-- Publication tells pgoutput which tables to stream.
CREATE PUBLICATION my_pub FOR TABLE users;

-- Replication slot pins the WAL so our consumer can read from it.
-- Using pgoutput plugin (same as OLake's Postgres connector).
SELECT pg_create_logical_replication_slot('my_slot', 'pgoutput');
