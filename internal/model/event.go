// Package model defines the shared data types for the CDC pipeline.
package model

// ChangeEvent represents a single decoded CDC event from the WAL stream.
type ChangeEvent struct {
	Op     string                 // "insert" | "update" | "delete"
	Table  string                 // source table name
	Values map[string]interface{} // column name → value
	LSN    uint64                 // log sequence number for acknowledgement
}
