// Package codec implements key encoding and value serialization for OxenDB rows.
//
// Key space layout:
//   \x01 prefix             — catalog (used by catalog.go)
//   \x02 + tableID(4BE) + encodedPK              — data rows
//   \x03 + tableID(4BE) + indexID(4BE) + encodedIdxVals + encodedPK — index entries
//   \x04 + tableID(4BE)     — auto-increment sequence counters
package codec

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"time"

	"github.com/vmihailenco/msgpack/v5"

	"oxendb/server/internal/sql/catalog"
	"oxendb/server/internal/sql/types"
)

const (
	prefixDataRow    = byte(0x02)
	prefixIndexEntry = byte(0x03)
	prefixSequence   = byte(0x04)
)

// DataRowKey builds the key for a data row: \x02 + tableID(4BE) + encodedPK.
func DataRowKey(tableID uint32, pkVals []types.Value) []byte {
	prefix := tableIDPrefix(prefixDataRow, tableID)
	var buf bytes.Buffer
	buf.Write(prefix)
	for _, v := range pkVals {
		buf.Write(EncodeSortKey(v))
	}
	return buf.Bytes()
}

// DataRowPrefix returns the scan prefix for all rows in a table: \x02 + tableID(4BE).
func DataRowPrefix(tableID uint32) []byte {
	return tableIDPrefix(prefixDataRow, tableID)
}

// DataRowPrefixEnd returns the exclusive scan end for all rows in a table (tableID+1).
func DataRowPrefixEnd(tableID uint32) []byte {
	return tableIDPrefix(prefixDataRow, tableID+1)
}

// IndexEntryKey builds the key for an index entry.
func IndexEntryKey(tableID, indexID uint32, idxVals, pkVals []types.Value) []byte {
	var buf bytes.Buffer
	buf.WriteByte(prefixIndexEntry)
	var tid [4]byte
	binary.BigEndian.PutUint32(tid[:], tableID)
	buf.Write(tid[:])
	var iid [4]byte
	binary.BigEndian.PutUint32(iid[:], indexID)
	buf.Write(iid[:])
	for _, v := range idxVals {
		buf.Write(EncodeSortKey(v))
	}
	for _, v := range pkVals {
		buf.Write(EncodeSortKey(v))
	}
	return buf.Bytes()
}

// IndexEntryPrefix returns the scan prefix for all entries in an index.
func IndexEntryPrefix(tableID, indexID uint32) []byte {
	var buf bytes.Buffer
	buf.WriteByte(prefixIndexEntry)
	var tid [4]byte
	binary.BigEndian.PutUint32(tid[:], tableID)
	buf.Write(tid[:])
	var iid [4]byte
	binary.BigEndian.PutUint32(iid[:], indexID)
	buf.Write(iid[:])
	return buf.Bytes()
}

// SequenceKey returns the key for a table's auto-increment counter: \x04 + tableID(4BE).
func SequenceKey(tableID uint32) []byte {
	return tableIDPrefix(prefixSequence, tableID)
}

// tableIDPrefix creates prefix + 4-byte big-endian tableID.
func tableIDPrefix(prefix byte, tableID uint32) []byte {
	buf := make([]byte, 5)
	buf[0] = prefix
	binary.BigEndian.PutUint32(buf[1:], tableID)
	return buf
}

// EncodeSortKey encodes a types.Value into a sort-preserving byte sequence.
//
// Rules:
//   NULL:             \x00
//   INTEGER/BIGINT:   \x01 + 8-byte big-endian with sign bit flipped
//   FLOAT/DOUBLE:     \x01 + IEEE754 with sign-bit manipulation for correct ordering
//   TEXT/VARCHAR:     \x01 + raw UTF-8 with \x00 escaped as \x00\x01, then \x00 terminator
//   BOOLEAN:          \x01\x00 (false) or \x01\x01 (true)
//   TIMESTAMP:        \x01 + 8-byte big-endian UnixNano with sign bit flipped
func EncodeSortKey(v types.Value) []byte {
	if v.IsNull {
		return []byte{0x00}
	}
	switch v.Type {
	case types.TypeNull:
		return []byte{0x00}

	case types.TypeInteger, types.TypeBigInt:
		buf := make([]byte, 9)
		buf[0] = 0x01
		// Flip sign bit so negatives sort before positives in unsigned comparison
		u := uint64(v.IntVal) ^ (1 << 63)
		binary.BigEndian.PutUint64(buf[1:], u)
		return buf

	case types.TypeFloat, types.TypeDouble:
		buf := make([]byte, 9)
		buf[0] = 0x01
		bits := math.Float64bits(v.FloatVal)
		if bits>>63 == 0 {
			// Positive: flip only the sign bit
			bits ^= (1 << 63)
		} else {
			// Negative: flip all bits so negatives sort in correct order
			bits ^= 0xFFFFFFFFFFFFFFFF
		}
		binary.BigEndian.PutUint64(buf[1:], bits)
		return buf

	case types.TypeText, types.TypeVarChar:
		var buf bytes.Buffer
		buf.WriteByte(0x01)
		for i := 0; i < len(v.StrVal); i++ {
			b := v.StrVal[i]
			if b == 0x00 {
				// Escape embedded \x00 as \x00\x01
				buf.WriteByte(0x00)
				buf.WriteByte(0x01)
			} else {
				buf.WriteByte(b)
			}
		}
		buf.WriteByte(0x00) // null terminator marks end of string
		return buf.Bytes()

	case types.TypeBoolean:
		if v.BoolVal {
			return []byte{0x01, 0x01}
		}
		return []byte{0x01, 0x00}

	case types.TypeTimestamp:
		buf := make([]byte, 9)
		buf[0] = 0x01
		nanos := v.TimeVal.UnixNano()
		u := uint64(nanos) ^ (1 << 63)
		binary.BigEndian.PutUint64(buf[1:], u)
		return buf

	default:
		return []byte{0x00}
	}
}

// EncodeRowValue serializes a complete row as a msgpack array.
// Each element in the array corresponds to the column at the same ordinal position.
// NULL values are encoded as nil; integer timestamps are stored as UnixNano int64.
func EncodeRowValue(cols []catalog.ColumnDef, row []types.Value) ([]byte, error) {
	if len(cols) != len(row) {
		return nil, fmt.Errorf("codec: column count mismatch: %d cols, %d values", len(cols), len(row))
	}
	arr := make([]interface{}, len(row))
	for i, v := range row {
		if v.IsNull {
			arr[i] = nil
			continue
		}
		switch v.Type {
		case types.TypeNull:
			arr[i] = nil
		case types.TypeInteger, types.TypeBigInt:
			arr[i] = v.IntVal
		case types.TypeFloat, types.TypeDouble:
			arr[i] = v.FloatVal
		case types.TypeText, types.TypeVarChar:
			arr[i] = v.StrVal
		case types.TypeBoolean:
			arr[i] = v.BoolVal
		case types.TypeTimestamp:
			arr[i] = v.TimeVal.UnixNano()
		default:
			arr[i] = nil
		}
	}
	return msgpack.Marshal(arr)
}

// DecodeRowValue deserializes a row from msgpack bytes using column definitions for type context.
func DecodeRowValue(cols []catalog.ColumnDef, data []byte) ([]types.Value, error) {
	var arr []interface{}
	if err := msgpack.Unmarshal(data, &arr); err != nil {
		return nil, fmt.Errorf("codec: unmarshal row: %w", err)
	}
	if len(arr) != len(cols) {
		return nil, fmt.Errorf("codec: column count mismatch on decode: got %d, want %d", len(arr), len(cols))
	}
	row := make([]types.Value, len(cols))
	for i, raw := range arr {
		if raw == nil {
			row[i] = types.Null
			continue
		}
		col := cols[i]
		v, err := coerceDecoded(raw, col.Type)
		if err != nil {
			return nil, fmt.Errorf("codec: col %q: %w", col.Name, err)
		}
		row[i] = v
	}
	return row, nil
}

// coerceDecoded converts a msgpack-decoded interface{} to a strongly-typed Value.
// msgpack decodes integers as int8/int16/int32/int64/uint64 and floats as float32/float64.
func coerceDecoded(raw interface{}, dt types.DataType) (types.Value, error) {
	switch dt {
	case types.TypeInteger, types.TypeBigInt:
		switch v := raw.(type) {
		case int64:
			return types.Value{Type: dt, IntVal: v}, nil
		case uint64:
			return types.Value{Type: dt, IntVal: int64(v)}, nil
		case int8:
			return types.Value{Type: dt, IntVal: int64(v)}, nil
		case int16:
			return types.Value{Type: dt, IntVal: int64(v)}, nil
		case int32:
			return types.Value{Type: dt, IntVal: int64(v)}, nil
		case float64:
			return types.Value{Type: dt, IntVal: int64(v)}, nil
		default:
			return types.Null, fmt.Errorf("cannot decode %T as integer", raw)
		}

	case types.TypeFloat, types.TypeDouble:
		switch v := raw.(type) {
		case float64:
			return types.Value{Type: dt, FloatVal: v}, nil
		case float32:
			return types.Value{Type: dt, FloatVal: float64(v)}, nil
		case int64:
			return types.Value{Type: dt, FloatVal: float64(v)}, nil
		case int8:
			return types.Value{Type: dt, FloatVal: float64(v)}, nil
		case int16:
			return types.Value{Type: dt, FloatVal: float64(v)}, nil
		case int32:
			return types.Value{Type: dt, FloatVal: float64(v)}, nil
		default:
			return types.Null, fmt.Errorf("cannot decode %T as float", raw)
		}

	case types.TypeText, types.TypeVarChar:
		switch v := raw.(type) {
		case string:
			return types.Value{Type: dt, StrVal: v}, nil
		case []byte:
			return types.Value{Type: dt, StrVal: string(v)}, nil
		default:
			return types.Value{Type: dt, StrVal: fmt.Sprintf("%v", raw)}, nil
		}

	case types.TypeBoolean:
		switch v := raw.(type) {
		case bool:
			return types.Value{Type: dt, BoolVal: v}, nil
		case int64:
			return types.Value{Type: dt, BoolVal: v != 0}, nil
		case uint64:
			return types.Value{Type: dt, BoolVal: v != 0}, nil
		default:
			return types.Null, fmt.Errorf("cannot decode %T as boolean", raw)
		}

	case types.TypeTimestamp:
		switch v := raw.(type) {
		case int64:
			return types.Value{Type: dt, TimeVal: time.Unix(0, v).UTC()}, nil
		case uint64:
			return types.Value{Type: dt, TimeVal: time.Unix(0, int64(v)).UTC()}, nil
		case int8:
			return types.Value{Type: dt, TimeVal: time.Unix(0, int64(v)).UTC()}, nil
		case int16:
			return types.Value{Type: dt, TimeVal: time.Unix(0, int64(v)).UTC()}, nil
		case int32:
			return types.Value{Type: dt, TimeVal: time.Unix(0, int64(v)).UTC()}, nil
		default:
			return types.Null, fmt.Errorf("cannot decode %T as timestamp", raw)
		}

	default:
		return types.Null, fmt.Errorf("unknown data type %v", dt)
	}
}
