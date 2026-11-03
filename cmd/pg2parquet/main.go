package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/supersanchayrx/pg2parquet/internal/config"
	"github.com/supersanchayrx/pg2parquet/internal/replication"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	cfg := config.FromEnv()

	err := replication.Run(ctx, cfg.ConnString, cfg.SlotName, cfg.Publication)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("shut down cleanly")
}
