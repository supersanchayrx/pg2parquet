// Package replication handles the Postgres logical replication stream.
//
// Phase 1: Open a replication connection, call IdentifySystem + StartReplication,
//          loop on ReceiveMessage, handle keepalives + XLogData.
// Phase 2: Decode XLogData.WALData into ChangeEvent structs using pglogrepl.ParseV1.
package replication
