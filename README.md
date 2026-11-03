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
duckdb -c "SELECT * FROM read_parquet('./output/public.users/*.parquet', union_by_name=true);"
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
| `BATCH_ROWS` | `50000` | Flush once this many rows are buffered |
| `FLUSH_INTERVAL` | `5s` | Flush at most this long after the first buffered row |
| `CHANNEL_CAPACITY` | `64` | Transactions buffered between reader and writer |
| `PARQUET_COMPRESSION` | `snappy` | `snappy`, `zstd` or `none` |
| `WRITER_DELAY` | `0` | Debug only: sleep per transaction in the writer, to force backpressure |
| `DEDUPE` | `true` | Debug only: `false` disables skipping already written transactions on restart |

## Output

One directory per table, one file per table per flush:

```
output/
├── _checkpoint.json
├── public.users/
│   └── 000000000192EF00_000000000192F548.parquet
└── public.orders/
    └── 000000000192F298_000000000192F298.parquet
```

File names are the first and last commit LSN in the file (16-digit hex), so they sort in commit order. Every row has these metadata columns, followed by the table's own columns (all nullable):

| Column | Meaning |
|--------|---------|
| `_op` | `insert`, `update` or `delete` |
| `_commit_lsn` | Commit LSN of the transaction |
| `_commit_ts` | Commit time |
| `_xid` | Transaction id |
| `_seq` | Position of the change inside its transaction |
| `_unchanged` | Comma-separated columns that were **not sent** because they're unchanged TOAST values. They're NULL in the row, but that NULL means "same as before", not "set to NULL" |
| `_old_key` | JSON of the old primary key when an update changed the key (or with `REPLICA IDENTITY FULL`) |

What each operation carries:
- **insert**: the new row.
- **update**: the new row. `_old_key` is set only when the key changed, because with the default replica identity Postgres sends no old tuple otherwise.
- **delete**: only the primary key columns (default replica identity); the rest are NULL. With `REPLICA IDENTITY FULL` the whole old row.

Type mapping: integers → BIGINT, floats → DOUBLE, bool → BOOLEAN, timestamp/timestamptz → TIMESTAMP (UTC, microseconds), date → DATE, everything else (text, numeric, uuid, json, unknown types) → string. `numeric` stays a string so it's exact. `timestamp without time zone` is treated as UTC.

`ALTER TABLE` that changes columns closes the current files and starts new ones with the new schema. Read across schema versions with `read_parquet('…/*.parquet', union_by_name=true)`. `TRUNCATE` is logged and skipped.

## Design decisions

**Transactions, not rows, go through the channel.** The writer only ever flushes between transactions, so a file never holds half a transaction and the checkpoint is always a commit LSN. The trade-off: one huge transaction is one huge channel item, so the memory bound is roughly `CHANNEL_CAPACITY × transaction size`.

**Keepalives while blocked.** When the channel is full the reader is stuck on the send and isn't reading from the socket, so it can't answer the server's keepalives. If nothing is sent for `wal_sender_timeout` (60s), Postgres drops the connection. The reader sends with a `select` that also has a 10s ticker, and on every tick it sends a status update. The reader goroutine is the only one that ever touches the connection, so there's no locking around it. The writer reports progress by storing the durable LSN in an `atomic.Uint64` that the reader reads.

**Shutdown order on Ctrl+C:** the reader stops and closes the channel → the writer drains it, flushes every file and records the checkpoint → the final durable LSN is acknowledged → the connection closes.

## Durability

Guarantee: every committed change appears in the output exactly once, across any number of crashes of pg2parquet.

1. Each file is written as `*.parquet.tmp`, fsynced, then renamed. A file only counts once it's renamed. (On Windows directory fsync isn't available, so the rename is the commit point.)
2. A flush always closes **all** open files at the same transaction boundary. After every rename, `_checkpoint.json` is written the same way (temp file, fsync, rename) with the last commit LSN and its end LSN.
3. Only then is the end LSN handed to the reader, which reports it to Postgres as flushed. Postgres can then drop that WAL.
4. On startup: delete leftover `.tmp` files, delete any `.parquet` whose last LSN is past the checkpoint (a flush that crashed between renames and the checkpoint write), resume from the checkpoint, and skip any transaction whose commit LSN is ≤ the checkpoint.

Postgres delivers at-least-once across reconnects: the slot only knows what was acknowledged, and an ack can be lost in a crash. Two things stop the resent transactions from being written twice. First, replication restarts from the checkpoint's LSN rather than from the slot's older position, so Postgres skips everything before it. Second, any transaction that still arrives with a commit LSN ≤ the checkpoint is dropped by the decoder.

Crash test results (`taskkill /F` under load, 10-row transactions):

| Run | Restarts | Rows | Lost | Duplicates |
|-----|----------|------|------|------------|
| dedupe on | 20 | 60,000 | 0 | 0 |
| dedupe off | 10 | 30,000 | 0 | 0 |

The dedupe-off run also had no duplicates, because restarting from the checkpoint LSN already makes Postgres skip the written transactions. The client-side dedupe is a second line of defence in case that start position isn't honoured.

I picked an explicit checkpoint file over deriving it from file names, because a flush writes several files. If the process dies after renaming some of them but not all, the highest file name would claim data is durable when another table's file never made it.

Not covered: losing the output disk, the slot being invalidated because it fell more than `max_slot_wal_keep_size` (2GB) behind, or someone dropping the slot. Any of those needs a fresh snapshot.

`test/crash_test.sh` checks this: it loads rows while killing pg2parquet with `taskkill /F` 20 times, then uses DuckDB to check that every row appears exactly once.

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
│   │   └── reader.go            # WAL stream consumer, status updates
│   ├── decode/
│   │   ├── decoder.go           # pgoutput message → transactions of events
│   │   └── values.go            # Postgres text values → Go values
│   ├── model/
│   │   └── event.go             # Event and Txn structs
│   ├── checkpoint/
│   │   └── checkpoint.go        # checkpoint file, file naming, startup cleanup
│   └── writer/
│       └── parquet.go           # per-table Parquet files, atomic flush
├── test/
│   ├── init.sql                 # dev DB bootstrap (used by docker-compose)
│   └── crash_test.sh            # forced-restart test
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

The reader and writer are connected through a **buffered Go channel** of transactions (`make(chan model.Txn, CHANNEL_CAPACITY)`). This gives us natural backpressure:

- If the writer is slow (e.g., flushing a large Parquet file), the channel fills up.
- The reader blocks on the channel send, which means it stops pulling from the WAL.
- Postgres holds onto the WAL — this is safe because the slot pins it.
- Once the writer catches up, the reader resumes.

No external queue, no polling, no dropped events. The Go scheduler handles the coordination.

## Scope

One slot, one writer. Things it does **not** do:

- ❌ Initial snapshot of existing rows (only changes after the slot was created)
- ❌ Schema evolution (rewriting old files); a schema change just starts a new file
- ❌ DDL replication
- ❌ A config file or CLI framework
- ❌ MySQL / MongoDB sources