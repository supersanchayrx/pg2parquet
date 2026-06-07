# Toy CDC Engine — Postgres → Parquet/Iceberg

A small Change Data Capture engine in Go that tails Postgres' logical replication
stream and writes the changes out to disk. Deliberately minimal — the goal is to
show I understand CDC, streaming, and Iceberg/data-lake basics, not to build a
production system.

Inspired by **OLake** and **Debezium**. For Postgres, OLake uses the `pgoutput`
logical-replication plugin — this project uses the exact same mechanism.

---

## What it does (end state)

```
Postgres  ──(logical replication / pgoutput)──>  Go CDC reader
                                                      │
                                                  decode WAL into
                                                  Insert/Update/Delete structs
                                                      │
                                                  channel (backpressure)
                                                      │
                                                  writer ──> Parquet files
                                                              └─(optional) Iceberg table
```

That's it. No UI, no config system, no multi-table magic. One table, one slot,
one writer.

---

## Tech / libraries

- **Go** (1.21+)
- **Postgres** (15+ is fine), run via Docker
- `github.com/jackc/pglogrepl` — speaks the logical replication protocol + parses `pgoutput`
- `github.com/jackc/pgx/v5` — normal queries (for the snapshot step)
- `github.com/parquet-go/parquet-go` — write Parquet files
- *(optional)* `github.com/apache/iceberg-go` — commit data into a local Iceberg table

---

## Phase 0 — Setup (½ hour)

1. Run Postgres in Docker with logical replication on:
   ```bash
   docker run -d --name cdc-pg -p 5432:5432 \
     -e POSTGRES_PASSWORD=postgres \
     postgres:16 -c wal_level=logical
   ```
2. Create a test table and a publication:
   ```sql
   CREATE TABLE users (id serial PRIMARY KEY, name text, email text);
   CREATE PUBLICATION my_pub FOR TABLE users;
   ```
3. Create a logical replication slot using the `pgoutput` plugin:
   ```sql
   SELECT pg_create_logical_replication_slot('my_slot', 'pgoutput');
   ```

**Done when:** you can `SELECT * FROM pg_replication_slots;` and see `my_slot`.

---

## Phase 1 — Read the raw stream (the core, ~½ day)

1. Open a **replication connection** (connection string needs `replication=database`).
2. Call `pglogrepl.IdentifySystem` and `pglogrepl.StartReplication`, passing the
   plugin args: `proto_version '1'` and `publication_names 'my_pub'`.
3. Loop on `conn.ReceiveMessage`:
   - Handle `PrimaryKeepaliveMessage` → reply with a Standby Status Update so the
     server knows you're alive.
   - Handle `XLogData` → that's your actual change data.
4. Just `log.Printf` the raw bytes for now.

**Done when:** you `INSERT INTO users ...` in psql and see bytes show up in your
Go program in real time.

---

## Phase 2 — Decode into Go structs (~½ day)

1. Parse each `XLogData.WALData` with `pglogrepl.ParseV1` (or the v2 parser).
2. You'll get message types: `RelationMessage`, `InsertMessage`, `UpdateMessage`,
   `DeleteMessage`, `BeginMessage`, `CommitMessage`.
3. Keep a `map[uint32]*RelationMessage` so you can map column metadata to tuple data.
4. Turn each Insert/Update/Delete into your own struct:
   ```go
   type ChangeEvent struct {
       Op     string                 // "insert" | "update" | "delete"
       Table  string
       Values map[string]interface{}
       LSN    pglogrepl.LSN
   }
   ```
5. Acknowledge the LSN periodically via `pglogrepl.SendStandbyStatusUpdate` so the
   slot doesn't bloat the WAL.

**Done when:** insert/update/delete in psql each print a clean, correct
`ChangeEvent` with the right column values.

---

## Phase 3 — Stream through a channel + write Parquet (~½ day)

1. Send each `ChangeEvent` into a **buffered channel** (`make(chan ChangeEvent, 100)`).
   The reader goroutine produces; a separate writer goroutine consumes.
   This is your backpressure story — if the writer is slow, the buffer fills and
   the reader naturally blocks. Talk about this in the interview.
2. Writer goroutine batches events (e.g. flush every N events or every few seconds)
   and writes them to a Parquet file with `parquet-go`.
3. Use `context.Context` so Ctrl-C cleanly flushes the last batch and exits.

**Done when:** a burst of changes ends up as a readable `.parquet` file on disk
(verify with `duckdb` or `parquet-tools`).

---

## Phase 4 (OPTIONAL) — Make it Iceberg (the "I dabble in Iceberg" bit)

This is the part that backs up the claim, but it's optional — do Phases 0–3 first.

1. Use `apache/iceberg-go` with a **local SQLite catalog** (no cloud needed).
2. Create a table whose schema matches `users`.
3. In your writer, instead of (or in addition to) loose Parquet files, append your
   data and commit a new snapshot to the Iceberg table.
4. Open the table afterwards and show the snapshot history / row count.

**Done when:** you can point at a directory and say "that's an Iceberg table with
N snapshots, each one a CDC batch." Even getting a single append committed is a
strong signal.

If `iceberg-go` write support gives you trouble, a fully honest fallback: write
Parquet (Phase 3), then load it into an Iceberg table once with a 5-line `pyiceberg`
script and screenshot it. Lighter, still demonstrates the concept.

---

## What to put in the README (for the OLake reviewers)

- One-line pitch: "A miniature CDC engine — Postgres logical replication →
  Iceberg, the same pattern OLake uses for its Postgres connector (`pgoutput`)."
- The little ASCII diagram above.
- A note on the **snapshot → CDC** idea (even if you only do CDC): explain that a
  real system does a full-load snapshot first, then switches to streaming from the
  slot. OLake's pitch is "Full + CDC," so showing you *understand* the handoff
  matters even if you don't implement the snapshot.
- The backpressure note from Phase 3.

---

## Scope guard — things to deliberately NOT do

- ❌ Multiple tables / dynamic schema discovery
- ❌ Schema evolution
- ❌ Exactly-once / fancy LSN checkpointing to disk
- ❌ A config file or CLI framework
- ❌ MySQL/Mongo sources

Keep it one table, one slot, one writer. Finished-and-simple beats
ambitious-and-half-working.

---

## Rough time budget

| Phase | Effort |
|-------|--------|
| 0 — Setup | 30 min |
| 1 — Raw stream | ½ day |
| 2 — Decode | ½ day |
| 3 — Channel + Parquet | ½ day |
| 4 — Iceberg (optional) | ½–1 day |

Core (Phases 0–3) is realistically a weekend. Iceberg is the stretch.
