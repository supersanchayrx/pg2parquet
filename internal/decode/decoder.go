package decode

import (
	"fmt"
	"log"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/supersanchayrx/pg2parquet/internal/model"
)

type Decoder struct {
	relations    map[uint32]*pglogrepl.RelationMessage
	columns      map[uint32][]model.Column
	typeMap      *pgtype.Map
	current      *model.Txn
	skipUpTo     uint64
	dedupe       bool
	Dropped      int
	unknownTypes map[uint32]bool
}

func New(skipUpTo uint64, dedupe bool) *Decoder {
	return &Decoder{
		relations:    map[uint32]*pglogrepl.RelationMessage{},
		columns:      map[uint32][]model.Column{},
		typeMap:      pgtype.NewMap(),
		skipUpTo:     skipUpTo,
		dedupe:       dedupe,
		unknownTypes: map[uint32]bool{},
	}
}

func (d *Decoder) Decode(data []byte) (*model.Txn, error) {
	msg, err := pglogrepl.Parse(data)
	if err != nil {
		return nil, fmt.Errorf("decode: parse: %w", err)
	}

	switch m := msg.(type) {
	case *pglogrepl.RelationMessage:
		d.relations[m.RelationID] = m
		var cols []model.Column
		for _, c := range m.Columns {
			cols = append(cols, model.Column{Name: c.Name, TypeOID: c.DataType})
			if !IsKnownType(c.DataType) && !d.unknownTypes[c.DataType] {
				d.unknownTypes[c.DataType] = true
				log.Printf("column %s.%s has type oid %d, storing it as text", m.RelationName, c.Name, c.DataType)
			}
		}
		d.columns[m.RelationID] = cols

	case *pglogrepl.BeginMessage:
		d.current = &model.Txn{
			Xid:        m.Xid,
			CommitLSN:  uint64(m.FinalLSN),
			CommitTime: m.CommitTime,
		}

	case *pglogrepl.InsertMessage:
		values, unchanged, err := d.tupleValues(m.RelationID, m.Tuple)
		if err != nil {
			return nil, err
		}
		return nil, d.addEvent("insert", m.RelationID, values, unchanged, nil)

	case *pglogrepl.UpdateMessage:
		values, unchanged, err := d.tupleValues(m.RelationID, m.NewTuple)
		if err != nil {
			return nil, err
		}
		var oldKey map[string]any
		if m.OldTuple != nil {
			oldValues, _, err := d.tupleValues(m.RelationID, m.OldTuple)
			if err != nil {
				return nil, err
			}
			oldKey = d.keyOnly(m.RelationID, oldValues)
		}
		return nil, d.addEvent("update", m.RelationID, values, unchanged, oldKey)

	case *pglogrepl.DeleteMessage:
		values, _, err := d.tupleValues(m.RelationID, m.OldTuple)
		if err != nil {
			return nil, err
		}
		return nil, d.addEvent("delete", m.RelationID, values, nil, nil)

	case *pglogrepl.TruncateMessage:
		log.Printf("skipping truncate of %d table(s)", len(m.RelationIDs))

	case *pglogrepl.CommitMessage:
		if d.current == nil {
			return nil, fmt.Errorf("decode: commit without begin")
		}
		if uint64(m.CommitLSN) != d.current.CommitLSN {
			return nil, fmt.Errorf("decode: commit lsn %s does not match begin %s", m.CommitLSN, pglogrepl.LSN(d.current.CommitLSN))
		}
		txn := d.current
		txn.EndLSN = uint64(m.TransactionEndLSN)
		d.current = nil
		if d.dedupe && txn.CommitLSN <= d.skipUpTo {
			d.Dropped++
			log.Printf("skipped already written transaction %d at %s (%d so far)", txn.Xid, pglogrepl.LSN(txn.CommitLSN), d.Dropped)
			return nil, nil
		}
		return txn, nil
	}
	return nil, nil
}

func (d *Decoder) addEvent(op string, relID uint32, values map[string]any, unchanged []string, oldKey map[string]any) error {
	if d.current == nil {
		return fmt.Errorf("decode: %s outside of a transaction", op)
	}
	rel := d.relations[relID]
	d.current.Events = append(d.current.Events, model.Event{
		Op:        op,
		Schema:    rel.Namespace,
		Table:     rel.RelationName,
		Columns:   d.columns[relID],
		Values:    values,
		Unchanged: unchanged,
		OldKey:    oldKey,
		Seq:       len(d.current.Events),
	})
	return nil
}

func (d *Decoder) tupleValues(relID uint32, tuple *pglogrepl.TupleData) (map[string]any, []string, error) {
	rel, ok := d.relations[relID]
	if !ok {
		return nil, nil, fmt.Errorf("decode: unknown relation %d", relID)
	}
	values := map[string]any{}
	var unchanged []string
	if tuple == nil {
		return values, nil, nil
	}
	if len(tuple.Columns) != len(rel.Columns) {
		return nil, nil, fmt.Errorf("decode: %s has %d columns but tuple has %d", rel.RelationName, len(rel.Columns), len(tuple.Columns))
	}
	for i, col := range tuple.Columns {
		name := rel.Columns[i].Name
		switch col.DataType {
		case pglogrepl.TupleDataTypeNull:
			values[name] = nil
		case pglogrepl.TupleDataTypeToast:
			unchanged = append(unchanged, name)
		case pglogrepl.TupleDataTypeText:
			v, err := ConvertValue(d.typeMap, rel.Columns[i].DataType, col.Data)
			if err != nil {
				return nil, nil, fmt.Errorf("decode: column %s.%s: %w", rel.RelationName, name, err)
			}
			values[name] = v
		default:
			return nil, nil, fmt.Errorf("decode: unsupported tuple data type %c", col.DataType)
		}
	}
	return values, unchanged, nil
}

func (d *Decoder) keyOnly(relID uint32, values map[string]any) map[string]any {
	key := map[string]any{}
	for _, c := range d.relations[relID].Columns {
		if c.Flags&1 == 1 {
			key[c.Name] = values[c.Name]
		}
	}
	return key
}
