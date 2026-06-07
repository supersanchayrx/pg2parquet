package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// TODO (Phase 1): Parse connection string from env/flag
	// TODO (Phase 1): Open replication connection
	// TODO (Phase 1): Start replication and consume WAL stream
	// TODO (Phase 3): Wire up the channel + writer goroutine

	fmt.Println("pg2parquet starting...")
	<-ctx.Done()
	fmt.Println("shutting down...")
}
