package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	ConnString      string
	Publication     string
	SlotName        string
	OutputDir       string
	BatchRows       int
	FlushInterval   time.Duration
	ChannelCapacity int
	WriterDelay     time.Duration
	Compression     string
	Dedupe          bool
}

func FromEnv() Config {
	return Config{
		ConnString:      getEnv("PG_CONN", "postgres://postgres:postgres@localhost:5432/cdc_demo?replication=database"),
		Publication:     getEnv("PG_PUBLICATION", "my_pub"),
		SlotName:        getEnv("PG_SLOT", "my_slot"),
		OutputDir:       getEnv("OUTPUT_DIR", "./output"),
		BatchRows:       getInt("BATCH_ROWS", 50000),
		FlushInterval:   getDuration("FLUSH_INTERVAL", 5*time.Second),
		ChannelCapacity: getInt("CHANNEL_CAPACITY", 64),
		WriterDelay:     getDuration("WRITER_DELAY", 0),
		Compression:     getEnv("PARQUET_COMPRESSION", "snappy"),
		Dedupe:          getEnv("DEDUPE", "true") != "false",
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getInt(key string, fallback int) int {
	n, err := strconv.Atoi(os.Getenv(key))
	if err != nil {
		return fallback
	}
	return n
}

func getDuration(key string, fallback time.Duration) time.Duration {
	d, err := time.ParseDuration(os.Getenv(key))
	if err != nil {
		return fallback
	}
	return d
}
