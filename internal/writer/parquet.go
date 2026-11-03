package writer

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress"
	"github.com/supersanchayrx/pg2parquet/internal/checkpoint"
	"github.com/supersanchayrx/pg2parquet/internal/model"
)

type tableFile struct {
	columns  []model.Column
	types    map[string]uint32
	schema   *parquet.Schema
	file     *os.File
	writer   *parquet.Writer
	dir      string
	firstLSN uint64
	lastLSN  uint64
}

type Writer struct {
	outDir        string
	batchRows     int
	flushInterval time.Duration
	delay         time.Duration
	codec         compress.Codec
	onDurable     func(uint64)
	files         map[string]*tableFile
	rows          int
	dirty         bool
	lastCommit    uint64
	lastEnd       uint64
	batchStart    time.Time
}

func New(outDir string, batchRows int, flushInterval time.Duration, delay time.Duration, compression string, onDurable func(uint64)) (*Writer, error) {
	var codec compress.Codec
	switch compression {
	case "snappy":
		codec = &parquet.Snappy
	case "zstd":
		codec = &parquet.Zstd
	case "none":
		codec = &parquet.Uncompressed
	default:
		return nil, fmt.Errorf("writer: unknown compression %q", compression)
	}
	if err := os.MkdirAll(outDir, 0755); err != nil {
		return nil, fmt.Errorf("writer: create output dir: %w", err)
	}
	return &Writer{
		outDir:        outDir,
		batchRows:     batchRows,
		flushInterval: flushInterval,
		delay:         delay,
		codec:         codec,
		onDurable:     onDurable,
		files:         map[string]*tableFile{},
	}, nil
}

func (w *Writer) Run(in <-chan model.Txn) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case txn, ok := <-in:
			if !ok {
				return w.flush(0)
			}
			if w.delay > 0 {
				time.Sleep(w.delay)
			}
			if err := w.writeTxn(txn); err != nil {
				return err
			}
			if w.rows >= w.batchRows {
				if err := w.flush(len(in)); err != nil {
					return err
				}
			}
		case <-ticker.C:
			if w.dirty && time.Since(w.batchStart) >= w.flushInterval {
				if err := w.flush(len(in)); err != nil {
					return err
				}
			}
		}
	}
}

func (w *Writer) writeTxn(txn model.Txn) error {
	for _, ev := range txn.Events {
		f := w.files[ev.Schema+"."+ev.Table]
		if f != nil && !sameColumns(f.columns, ev.Columns) {
			if err := w.flush(0); err != nil {
				return err
			}
			break
		}
	}

	if !w.dirty {
		w.batchStart = time.Now()
	}

	for _, ev := range txn.Events {
		key := ev.Schema + "." + ev.Table
		f := w.files[key]
		if f == nil {
			var err error
			f, err = w.open(key, ev.Columns)
			if err != nil {
				return err
			}
			w.files[key] = f
			f.firstLSN = txn.CommitLSN
		} else if !sameColumns(f.columns, ev.Columns) {
			return fmt.Errorf("writer: schema of %s changed inside transaction %d", key, txn.Xid)
		}

		row, err := buildRow(f, txn, ev)
		if err != nil {
			return err
		}
		if _, err := f.writer.WriteRows([]parquet.Row{row}); err != nil {
			return fmt.Errorf("writer: write row to %s: %w", key, err)
		}
		f.lastLSN = txn.CommitLSN
		w.rows++
	}

	w.dirty = true
	w.lastCommit = txn.CommitLSN
	w.lastEnd = txn.EndLSN
	return nil
}

func (w *Writer) open(key string, columns []model.Column) (*tableFile, error) {
	dir := filepath.Join(w.outDir, key)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("writer: create table dir: %w", err)
	}

	group := parquet.Group{
		"_op":         parquet.String(),
		"_commit_lsn": parquet.Int(64),
		"_commit_ts":  parquet.Timestamp(parquet.Microsecond),
		"_xid":        parquet.Int(64),
		"_seq":        parquet.Int(64),
		"_unchanged":  parquet.Optional(parquet.String()),
		"_old_key":    parquet.Optional(parquet.String()),
	}
	types := map[string]uint32{}
	for _, c := range columns {
		group[c.Name] = parquet.Optional(nodeFor(c.TypeOID))
		types[c.Name] = c.TypeOID
	}
	schema := parquet.NewSchema(key, group)

	file, err := os.CreateTemp(dir, "*.parquet.tmp")
	if err != nil {
		return nil, fmt.Errorf("writer: create temp file: %w", err)
	}

	return &tableFile{
		columns: columns,
		types:   types,
		schema:  schema,
		file:    file,
		writer:  parquet.NewWriter(file, schema, parquet.Compression(w.codec)),
		dir:     dir,
	}, nil
}

func (w *Writer) flush(queued int) error {
	if !w.dirty {
		return nil
	}

	for key, f := range w.files {
		if err := f.writer.Close(); err != nil {
			return fmt.Errorf("writer: close parquet for %s: %w", key, err)
		}
		if err := f.file.Sync(); err != nil {
			return fmt.Errorf("writer: sync %s: %w", key, err)
		}
		if err := f.file.Close(); err != nil {
			return fmt.Errorf("writer: close file for %s: %w", key, err)
		}
		final := filepath.Join(f.dir, checkpoint.FileName(f.firstLSN, f.lastLSN))
		if err := os.Rename(f.file.Name(), final); err != nil {
			return fmt.Errorf("writer: rename %s: %w", key, err)
		}
		checkpoint.SyncDir(f.dir)
	}

	err := checkpoint.Save(w.outDir, checkpoint.Checkpoint{CommitLSN: w.lastCommit, EndLSN: w.lastEnd})
	if err != nil {
		return err
	}

	if w.rows > 0 {
		log.Printf("flushed %d rows into %d file(s), durable up to %s, %d txn(s) waiting", w.rows, len(w.files), pglogrepl.LSN(w.lastEnd), queued)
	}
	w.onDurable(w.lastEnd)

	w.files = map[string]*tableFile{}
	w.rows = 0
	w.dirty = false
	return nil
}

func buildRow(f *tableFile, txn model.Txn, ev model.Event) (parquet.Row, error) {
	fields := f.schema.Fields()
	row := make(parquet.Row, 0, len(fields))

	for i, field := range fields {
		name := field.Name()
		var v parquet.Value
		switch name {
		case "_op":
			v = parquet.ByteArrayValue([]byte(ev.Op)).Level(0, 0, i)
		case "_commit_lsn":
			v = parquet.Int64Value(int64(txn.CommitLSN)).Level(0, 0, i)
		case "_commit_ts":
			v = parquet.Int64Value(txn.CommitTime.UnixMicro()).Level(0, 0, i)
		case "_xid":
			v = parquet.Int64Value(int64(txn.Xid)).Level(0, 0, i)
		case "_seq":
			v = parquet.Int64Value(int64(ev.Seq)).Level(0, 0, i)
		case "_unchanged":
			if len(ev.Unchanged) == 0 {
				v = parquet.NullValue().Level(0, 0, i)
			} else {
				v = parquet.ByteArrayValue([]byte(strings.Join(ev.Unchanged, ","))).Level(0, 1, i)
			}
		case "_old_key":
			if ev.OldKey == nil {
				v = parquet.NullValue().Level(0, 0, i)
			} else {
				data, err := json.Marshal(ev.OldKey)
				if err != nil {
					return nil, fmt.Errorf("writer: old key: %w", err)
				}
				v = parquet.ByteArrayValue(data).Level(0, 1, i)
			}
		default:
			v = toValue(f.types[name], ev.Values[name], i)
		}
		row = append(row, v)
	}
	return row, nil
}

func toValue(oid uint32, value any, column int) parquet.Value {
	if value == nil {
		return parquet.NullValue().Level(0, 0, column)
	}

	var v parquet.Value
	switch x := value.(type) {
	case int64:
		v = parquet.Int64Value(x)
	case float64:
		v = parquet.DoubleValue(x)
	case bool:
		v = parquet.BooleanValue(x)
	case time.Time:
		if oid == pgtype.DateOID {
			v = parquet.Int32Value(int32(x.Unix() / 86400))
		} else {
			v = parquet.Int64Value(x.UnixMicro())
		}
	case string:
		v = parquet.ByteArrayValue([]byte(x))
	default:
		v = parquet.ByteArrayValue([]byte(fmt.Sprint(x)))
	}
	return v.Level(0, 1, column)
}

func nodeFor(oid uint32) parquet.Node {
	switch oid {
	case pgtype.Int2OID, pgtype.Int4OID, pgtype.Int8OID:
		return parquet.Int(64)
	case pgtype.Float4OID, pgtype.Float8OID:
		return parquet.Leaf(parquet.DoubleType)
	case pgtype.BoolOID:
		return parquet.Leaf(parquet.BooleanType)
	case pgtype.TimestamptzOID, pgtype.TimestampOID:
		return parquet.Timestamp(parquet.Microsecond)
	case pgtype.DateOID:
		return parquet.Date()
	default:
		return parquet.String()
	}
}

func sameColumns(a, b []model.Column) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
