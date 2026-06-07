// Package decode turns raw WAL messages into structured ChangeEvent values.
//
// Phase 2: Maintain a map[uint32]*RelationMessage for column metadata,
//          parse InsertMessage / UpdateMessage / DeleteMessage into ChangeEvent.
package decode
