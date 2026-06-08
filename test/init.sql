
CREATE TABLE IF NOT EXISTS users (
    id    SERIAL PRIMARY KEY,
    name  TEXT NOT NULL,
    email TEXT NOT NULL
);

CREATE PUBLICATION my_pub FOR TABLE users;


SELECT pg_create_logical_replication_slot('my_slot', 'pgoutput');
