package replication

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/supersanchayrx/pg2parquet/internal/decode"
	"github.com/supersanchayrx/pg2parquet/internal/model"
)

const statusInterval = 10 * time.Second

type Reader struct {
	conn     *pgconn.PgConn
	received pglogrepl.LSN
	durable  atomic.Uint64
}

func Connect(ctx context.Context, connString string) (*Reader, error) {
	conn, err := pgconn.Connect(ctx, connString)
	if err != nil {
		return nil, fmt.Errorf("reader: connect: %w", err)
	}
	return &Reader{conn: conn}, nil
}

func (r *Reader) Start(ctx context.Context, slot string, publication string, startLSN uint64) error {
	sys, err := pglogrepl.IdentifySystem(ctx, r.conn)
	if err != nil {
		return fmt.Errorf("reader: identify system: %w", err)
	}
	log.Printf("system id: %s, timeline: %d, wal position: %s", sys.SystemID, sys.Timeline, sys.XLogPos)

	pluginArgs := []string{
		"proto_version '1'",
		"publication_names '" + publication + "'",
	}
	err = pglogrepl.StartReplication(ctx, r.conn, slot, pglogrepl.LSN(startLSN), pglogrepl.StartReplicationOptions{PluginArgs: pluginArgs})
	if err != nil {
		return fmt.Errorf("reader: start replication: %w", err)
	}
	log.Printf("started replication on slot %s from %s", slot, pglogrepl.LSN(startLSN))
	return nil
}

func (r *Reader) MarkDurable(lsn uint64) {
	r.durable.Store(lsn)
}

func (r *Reader) Run(ctx context.Context, decoder *decode.Decoder, out chan<- model.Txn) error {
	defer close(out)

	nextStatus := time.Now().Add(statusInterval)

	for {
		if time.Now().After(nextStatus) {
			if err := r.sendStatus(); err != nil {
				return err
			}
			nextStatus = time.Now().Add(statusInterval)
		}

		recvCtx, cancel := context.WithDeadline(ctx, nextStatus)
		msg, err := r.conn.ReceiveMessage(recvCtx)
		cancel()

		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}
			return fmt.Errorf("reader: receive: %w", err)
		}

		if errMsg, ok := msg.(*pgproto3.ErrorResponse); ok {
			return fmt.Errorf("reader: server error: %s", errMsg.Message)
		}

		copyData, ok := msg.(*pgproto3.CopyData)
		if !ok {
			log.Printf("unexpected message: %T", msg)
			continue
		}

		switch copyData.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			keepalive, err := pglogrepl.ParsePrimaryKeepaliveMessage(copyData.Data[1:])
			if err != nil {
				return fmt.Errorf("reader: parse keepalive: %w", err)
			}
			if keepalive.ReplyRequested {
				nextStatus = time.Now()
			}

		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(copyData.Data[1:])
			if err != nil {
				return fmt.Errorf("reader: parse xlogdata: %w", err)
			}

			end := xld.WALStart + pglogrepl.LSN(len(xld.WALData))
			if end > r.received {
				r.received = end
			}

			txn, err := decoder.Decode(xld.WALData)
			if err != nil {
				return err
			}
			if txn != nil {
				if err := r.send(ctx, out, *txn); err != nil {
					return err
				}
			}
		}
	}
}

func (r *Reader) send(ctx context.Context, out chan<- model.Txn, txn model.Txn) error {
	ticker := time.NewTicker(statusInterval)
	defer ticker.Stop()

	for {
		select {
		case out <- txn:
			return nil
		case <-ticker.C:
			if err := r.sendStatus(); err != nil {
				return err
			}
		case <-ctx.Done():
			return nil
		}
	}
}

func (r *Reader) sendStatus() error {
	durable := pglogrepl.LSN(r.durable.Load())
	write := r.received
	if durable == 0 {
		write = 0
	}
	err := pglogrepl.SendStandbyStatusUpdate(context.Background(), r.conn, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: write,
		WALFlushPosition: durable,
		WALApplyPosition: durable,
		ClientTime:       time.Now(),
	})
	if err != nil {
		return fmt.Errorf("reader: status update: %w", err)
	}
	return nil
}

func (r *Reader) Close() {
	if err := r.sendStatus(); err != nil {
		log.Printf("final status update failed: %v", err)
	} else {
		log.Printf("acknowledged up to %s", pglogrepl.LSN(r.durable.Load()))
	}
	r.conn.Close(context.Background())
}
