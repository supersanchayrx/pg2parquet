package model

import "time"

type Column struct {
	Name    string
	TypeOID uint32
}

type Event struct {
	Op        string
	Schema    string
	Table     string
	Columns   []Column
	Values    map[string]any
	Unchanged []string
	OldKey    map[string]any
	Seq       int
}

type Txn struct {
	Xid        uint32
	CommitLSN  uint64
	EndLSN     uint64
	CommitTime time.Time
	Events     []Event
}
