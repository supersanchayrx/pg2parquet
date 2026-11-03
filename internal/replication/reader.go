package replication

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

func Run(ctx context.Context, connString string, slot string, publication string) error {
	conn, err := pgconn.Connect(ctx, connString)
	if err != nil {
		return fmt.Errorf("reader: connect: %w", err)
	}
	defer conn.Close(context.Background())

	sys, err := pglogrepl.IdentifySystem(ctx, conn)
	if err != nil {
		return fmt.Errorf("reader: identify system: %w", err)
	}
	log.Printf("system id: %s, timeline: %d, wal position: %s", sys.SystemID, sys.Timeline, sys.XLogPos)

	pluginArgs := []string{
		"proto_version '1'",
		"publication_names '" + publication + "'",
	}
	err = pglogrepl.StartReplication(ctx, conn, slot, 0, pglogrepl.StartReplicationOptions{PluginArgs: pluginArgs})
	if err != nil {
		return fmt.Errorf("reader: start replication: %w", err)
	}
	log.Printf("started replication on slot %s", slot)

	var received pglogrepl.LSN
	var flushed pglogrepl.LSN
	statusInterval := 10 * time.Second
	nextStatus := time.Now().Add(statusInterval)

	for {
		if time.Now().After(nextStatus) {
			err = sendStatus(conn, received, flushed)
			if err != nil {
				return err
			}
			nextStatus = time.Now().Add(statusInterval)
		}

		recvCtx, cancel := context.WithDeadline(ctx, nextStatus)
		msg, err := conn.ReceiveMessage(recvCtx)
		cancel()

		if ctx.Err() != nil {
			log.Println("stopping reader")
			return sendStatus(conn, received, flushed)
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
			log.Printf("xlogdata: start=%s end=%s len=%d type=%c", xld.WALStart, xld.ServerWALEnd, len(xld.WALData), xld.WALData[0])

			end := xld.WALStart + pglogrepl.LSN(len(xld.WALData))
			if end > received {
				received = end
			}
			flushed = received
		}
	}
}

func sendStatus(conn *pgconn.PgConn, received pglogrepl.LSN, flushed pglogrepl.LSN) error {
	err := pglogrepl.SendStandbyStatusUpdate(context.Background(), conn, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: received,
		WALFlushPosition: flushed,
		WALApplyPosition: flushed,
		ClientTime:       time.Now(),
	})
	if err != nil {
		return fmt.Errorf("reader: status update: %w", err)
	}
	return nil
}
