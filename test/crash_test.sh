#!/usr/bin/env bash
set -u

RESTARTS=${RESTARTS:-20}
TXNS=${TXNS:-6000}
DEDUPE=${DEDUPE:-true}
DUCKDB=${DUCKDB:-duckdb}
LOG=crash_test.log

go build -o pg2parquet.exe ./cmd/pg2parquet || exit 1
docker compose down -v > /dev/null 2>&1
docker compose up -d --wait > /dev/null 2>&1
rm -rf output "$LOG"

docker exec cdc-pg psql -U postgres -d cdc_demo -q \
  -c "CREATE TABLE load_test (id bigint PRIMARY KEY, created_at timestamptz DEFAULT now());" \
  -c "ALTER PUBLICATION my_pub ADD TABLE load_test;"

echo "loading $TXNS transactions of 10 rows, restarting $RESTARTS times, dedupe=$DEDUPE"
docker exec cdc-pg psql -U postgres -d cdc_demo -q -c "DO \$\$ BEGIN FOR i IN 0..$((TXNS - 1)) LOOP INSERT INTO load_test (id) SELECT i * 10 + g FROM generate_series(0, 9) g; COMMIT; PERFORM pg_sleep(0.01); END LOOP; END \$\$;" &
LOAD_PID=$!

for i in $(seq 1 "$RESTARTS"); do
  FLUSH_INTERVAL=1s BATCH_ROWS=2000 DEDUPE=$DEDUPE ./pg2parquet.exe >> "$LOG" 2>&1 &
  sleep $((RANDOM % 9 + 2))
  taskkill //IM pg2parquet.exe //F > /dev/null 2>&1
  echo "killed $i/$RESTARTS"
done

wait $LOAD_PID
echo "load finished, draining"
FLUSH_INTERVAL=1s DEDUPE=$DEDUPE ./pg2parquet.exe >> "$LOG" 2>&1 &
sleep 15
taskkill //IM pg2parquet.exe //F > /dev/null 2>&1

"$DUCKDB" -c "SELECT count(*) AS insert_rows, count(DISTINCT id) AS distinct_ids, $((TXNS * 10)) AS expected FROM read_parquet('output/public.load_test/*.parquet') WHERE _op = 'insert';" \
  -c "SELECT count(*) AS duplicate_lsn_seq_pairs FROM (SELECT _commit_lsn, _seq FROM read_parquet('output/public.load_test/*.parquet') GROUP BY ALL HAVING count(*) > 1);" \
  -c "SELECT max(_commit_lsn) AS max_commit_lsn FROM read_parquet('output/public.load_test/*.parquet');"
docker exec cdc-pg psql -U postgres -d cdc_demo -t -c "SELECT 'slot confirmed_flush_lsn: ' || (confirmed_flush_lsn - '0/0') FROM pg_replication_slots;"

echo "runs that removed leftover files: $(grep -c 'removed [1-9]' "$LOG")"
echo "already written transactions skipped: $(grep -c 'skipped already written' "$LOG")"
