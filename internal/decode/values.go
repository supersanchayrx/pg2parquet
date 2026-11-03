package decode

import (
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

func IsKnownType(oid uint32) bool {
	switch oid {
	case pgtype.Int2OID, pgtype.Int4OID, pgtype.Int8OID,
		pgtype.Float4OID, pgtype.Float8OID, pgtype.BoolOID,
		pgtype.TextOID, pgtype.VarcharOID, pgtype.BPCharOID, pgtype.NumericOID,
		pgtype.TimestamptzOID, pgtype.TimestampOID, pgtype.DateOID,
		pgtype.UUIDOID, pgtype.JSONOID, pgtype.JSONBOID:
		return true
	}
	return false
}

func ConvertValue(typeMap *pgtype.Map, oid uint32, text []byte) (any, error) {
	s := string(text)
	switch oid {
	case pgtype.Int2OID, pgtype.Int4OID, pgtype.Int8OID:
		return strconv.ParseInt(s, 10, 64)
	case pgtype.Float4OID, pgtype.Float8OID:
		return strconv.ParseFloat(s, 64)
	case pgtype.BoolOID:
		return s == "t", nil
	case pgtype.TimestamptzOID, pgtype.TimestampOID, pgtype.DateOID:
		var t time.Time
		err := typeMap.Scan(oid, pgtype.TextFormatCode, text, &t)
		if err != nil {
			return nil, err
		}
		return t.UTC(), nil
	default:
		return s, nil
	}
}
