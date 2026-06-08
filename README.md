# pg2parquet

A miniature CDC engine — Postgres logical replication → Parquet, using the same `pgoutput` pattern that [OLake](https://github.com/datazip-inc/olake) uses for its Postgres connector.

```
Postgres  ──(logical replication / pgoutput)──>  Go CDC reader
                                                       │
                                                   decode WAL into
                                                   Insert/Update/Delete structs
                                                       │
                                                   buffered channel (backpressure)
                                                       │
                                                   writer ──> Parquet files
                                                               └─(optional) Iceberg table
```

## Why

This project demonstrates a working understanding of:

- **Change Data Capture** via Postgres logical replication (`pgoutput` plugin)
- **Streaming pipeline design** with Go channels and goroutines
- **Backpressure** — a bounded channel between the reader and writer naturally throttles the pipeline when the writer falls behind
- **Columnar file formats** — writing CDC events as Parquet for efficient downstream analytics

## Architecture

The engine follows a clean three-stage pipeline:

| Stage | Package | Responsibility |
|-------|---------|----------------|
| **Reader** | `internal/replication` | Opens a replication connection, consumes WAL via `pglogrepl`, sends standby keepalives |
| **Decoder** | `internal/decode` | Parses `pgoutput` messages (Relation, Insert, Update, Delete) into `ChangeEvent` structs |
| **Writer** | `internal/writer` | Batches events from a channel and flushes to `.parquet` files on disk |

Shared types live in `internal/model`, configuration in `internal/config`.

### Snapshot → CDC (design note)

A production system (like OLake) performs a **full-load snapshot** of the table first, then switches to **streaming from the replication slot**. This project focuses on the CDC streaming phase only, but the handoff point is well understood: you'd snapshot using a normal `SELECT *` inside a transaction whose snapshot is exported, note the LSN, then start consuming the slot from that LSN onwards.

## Usage

### Prerequisites

- [Go 1.21+](https://go.dev/dl/)
- A Postgres database (15+) with `wal_level=logical`
- A publication and replication slot already created on the target table
- (Optional) [DuckDB](https://duckdb.org/) or `parquet-tools` to inspect output files

### Prepare your Postgres

Your database must have logical replication enabled. If you control the server, set this in `postgresql.conf`:

```
wal_level = logical
```

Then create a publication and slot for the table(s) you want to capture:

```sql
CREATE PUBLICATION my_pub FOR TABLE your_table;
SELECT pg_create_logical_replication_slot('my_slot', 'pgoutput');
```

### Run

```bash
PG_CONN="postgres://user:pass@host:5432/mydb?replication=database" \
PG_PUBLICATION="my_pub" \
PG_SLOT="my_slot" \
  go run ./cmd/pg2parquet
```

The engine connects, starts consuming the WAL stream, and writes batched Parquet files to `./output/`.

### Inspect the output

```bash
duckdb -c "SELECT * FROM read_parquet('./output/*.parquet');"
```

## Local Development

For development and testing, a Docker Compose setup is included that spins up a pre-configured Postgres:

```bash
docker compose up -d
```

This launches Postgres 16 with `wal_level=logical` and runs [test/init.sql](file:///c:/Users/sanch/Documents/GitHub/pg2parquet/test/init.sql) to create a `users` table, publication, and replication slot.

```bash
# verify the slot exists
docker exec cdc-pg psql -U postgres -d cdc_demo \
  -c "SELECT slot_name, plugin FROM pg_replication_slots;"

# run the engine (defaults point to the docker Postgres)
go run ./cmd/pg2parquet

# in another terminal, generate some changes
docker exec -it cdc-pg psql -U postgres -d cdc_demo
```

```sql
INSERT INTO users (name, email) VALUES ('alice', 'alice@example.com');
UPDATE users SET email = 'alice@new.com' WHERE name = 'alice';
DELETE FROM users WHERE name = 'alice';
```

## Configuration

All config is via environment variables (no CLI framework, deliberately):

| Variable | Default | Description |
|----------|---------|-------------|
| `PG_CONN` | `postgres://postgres:postgres@localhost:5432/cdc_demo?replication=database` | Postgres connection string (must include `replication=database`) |
| `PG_PUBLICATION` | `my_pub` | Publication name |
| `PG_SLOT` | `my_slot` | Replication slot name |
| `OUTPUT_DIR` | `./output` | Directory for Parquet files |

## Project Structure

```
pg2parquet/
├── cmd/
│   └── pg2parquet/
│       └── main.go              # entry point, signal handling
├── internal/
│   ├── config/
│   │   └── config.go            # env-based configuration
│   ├── replication/
│   │   └── reader.go            # WAL stream consumer (pglogrepl)
│   ├── decode/
│   │   └── decoder.go           # pgoutput message → ChangeEvent
│   ├── model/
│   │   └── event.go             # ChangeEvent struct
│   └── writer/
│       └── parquet.go           # batched Parquet file writer
├── test/
│   └── init.sql                 # dev DB bootstrap (used by docker-compose)
├── docker-compose.yml           # local dev only
├── ROADMAP.md                   # phased build plan
└── README.md
```

## Tech Stack

| Dependency | Purpose |
|------------|---------|
| [`pglogrepl`](https://github.com/jackc/pglogrepl) | Logical replication protocol + `pgoutput` message parsing |
| [`pgx/v5`](https://github.com/jackc/pgx) | Standard Postgres queries (snapshot step, if added) |
| [`parquet-go`](https://github.com/parquet-go/parquet-go) | Write columnar Parquet files |
| *(optional)* [`iceberg-go`](https://github.com/apache/iceberg-go) | Commit data into a local Iceberg table |

## Backpressure

The reader and writer are connected through a **buffered Go channel** (`make(chan ChangeEvent, 100)`). This gives us natural backpressure:

- If the writer is slow (e.g., flushing a large Parquet file), the channel fills up.
- The reader blocks on the channel send, which means it stops pulling from the WAL.
- Postgres holds onto the WAL — this is safe because the slot pins it.
- Once the writer catches up, the reader resumes.

No external queue, no polling, no dropped events. The Go scheduler handles the coordination.

## Scope

This is deliberately a **toy** — one table, one slot, one writer. Things it does **not** do:

- ❌ Multiple tables / dynamic schema discovery
- ❌ Schema evolution
- ❌ Exactly-once / durable LSN checkpointing
- ❌ A config file or CLI framework
- ❌ MySQL / MongoDB sources