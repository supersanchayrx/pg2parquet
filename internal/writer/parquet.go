// Package writer consumes ChangeEvents from a channel and writes them to Parquet files.
//
// Phase 3: Read from a buffered channel, batch events, flush to Parquet
//          on a count or time threshold.
package writer
