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

## Quick Start

### Prerequisites

- [Go 1.21+](https://go.dev/dl/)
- [Docker](https://docs.docker.com/get-docker/) & Docker Compose
- (Optional) [DuckDB](https://duckdb.org/) or `parquet-tools` to inspect output files

### 1. Start Postgres

```bash
docker compose up -d
```

This launches Postgres 16 with `wal_level=logical` and automatically creates:
- A `users` table
- A publication `my_pub`
- A replication slot `my_slot` (using the `pgoutput` plugin)

Verify the slot exists:

```bash
docker exec cdc-pg psql -U postgres -d cdc_demo \
  -c "SELECT slot_name, plugin FROM pg_replication_slots;"
```

### 2. Run the CDC engine

```bash
go run ./cmd/pg2parquet
```

The engine connects to Postgres, starts consuming the WAL stream, and writes batched Parquet files to `./output/`.

### 3. Generate some changes

In another terminal:

```bash
docker exec -it cdc-pg psql -U postgres -d cdc_demo
```

```sql
INSERT INTO users (name, email) VALUES ('alice', 'alice@example.com');
INSERT INTO users (name, email) VALUES ('bob', 'bob@example.com');
UPDATE users SET email = 'alice@newdomain.com' WHERE name = 'alice';
DELETE FROM users WHERE name = 'bob';
```

You should see the engine log each decoded event in real time.

### 4. Inspect the Parquet output

```bash
duckdb -c "SELECT * FROM read_parquet('./output/*.parquet');"
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
├── scripts/
│   └── init.sql                 # auto-run DB bootstrap
├── docker-compose.yml
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