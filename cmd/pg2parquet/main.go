package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pglogrepl"
	"github.com/supersanchayrx/pg2parquet/internal/checkpoint"
	"github.com/supersanchayrx/pg2parquet/internal/config"
	"github.com/supersanchayrx/pg2parquet/internal/decode"
	"github.com/supersanchayrx/pg2parquet/internal/model"
	"github.com/supersanchayrx/pg2parquet/internal/replication"
	"github.com/supersanchayrx/pg2parquet/internal/writer"
	"golang.org/x/sync/errgroup"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	cfg := config.FromEnv()

	err := run(ctx, cfg)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("shut down cleanly")
}

func run(ctx context.Context, cfg config.Config) error {
	cp, err := checkpoint.Load(cfg.OutputDir)
	if err != nil {
		return err
	}
	removed, err := checkpoint.Cleanup(cfg.OutputDir, cp)
	if err != nil {
		return err
	}
	log.Printf("checkpoint: commit %s, end %s, removed %d leftover file(s)", pglogrepl.LSN(cp.CommitLSN), pglogrepl.LSN(cp.EndLSN), removed)

	reader, err := replication.Connect(ctx, cfg.ConnString)
	if err != nil {
		return err
	}
	defer reader.Close()

	reader.MarkDurable(cp.EndLSN)
	err = reader.Start(ctx, cfg.SlotName, cfg.Publication, cp.EndLSN)
	if err != nil {
		return err
	}

	w, err := writer.New(cfg.OutputDir, cfg.BatchRows, cfg.FlushInterval, cfg.WriterDelay, cfg.Compression, reader.MarkDurable)
	if err != nil {
		return err
	}

	decoder := decode.New(cp.CommitLSN, cfg.Dedupe)
	if !cfg.Dedupe {
		log.Println("dedupe is OFF, already written transactions will be written again")
	}

	txns := make(chan model.Txn, cfg.ChannelCapacity)

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return reader.Run(gctx, decoder, txns)
	})
	g.Go(func() error {
		return w.Run(txns)
	})
	err = g.Wait()

	log.Printf("skipped %d already written transaction(s)", decoder.Dropped)
	return err
}
