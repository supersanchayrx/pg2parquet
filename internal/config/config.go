// Package config provides connection and runtime configuration.
package config

import "os"

// Config holds all runtime settings for the CDC engine.
type Config struct {
	// Postgres connection string (must include replication=database).
	// Example: "postgres://postgres:postgres@localhost:5432/cdc_demo?replication=database"
	ConnString string

	// Name of the publication to subscribe to.
	Publication string

	// Name of the replication slot to consume from.
	SlotName string

	// Directory to write Parquet output files.
	OutputDir string

	// Number of events to buffer before flushing to Parquet.
	BatchSize int
}

// FromEnv reads configuration from environment variables with sensible defaults.
func FromEnv() Config {
	return Config{
		ConnString:  getEnv("PG_CONN", "postgres://postgres:postgres@localhost:5432/cdc_demo?replication=database"),
		Publication: getEnv("PG_PUBLICATION", "my_pub"),
		SlotName:    getEnv("PG_SLOT", "my_slot"),
		OutputDir:   getEnv("OUTPUT_DIR", "./output"),
		BatchSize:   100,
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
