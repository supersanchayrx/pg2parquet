CREATE TABLE IF NOT EXISTS users (
    id    SERIAL PRIMARY KEY,
    name  TEXT NOT NULL,
    email TEXT NOT NULL
);

-- Type coverage for the decoder: bigint, numeric, boolean, timestamptz, jsonb, uuid, text.
CREATE TABLE IF NOT EXISTS orders (
    id          BIGSERIAL PRIMARY KEY,
    customer_id BIGINT NOT NULL,
    amount      NUMERIC(12,2) NOT NULL,
    paid        BOOLEAN NOT NULL DEFAULT false,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    metadata    JSONB,
    order_ref   UUID NOT NULL DEFAULT gen_random_uuid(),
    note        TEXT
);

CREATE PUBLICATION my_pub FOR TABLE users, orders;

SELECT pg_create_logical_replication_slot('my_slot', 'pgoutput');
