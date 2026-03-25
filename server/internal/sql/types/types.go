package types

import (
	"fmt"
	"math"
	"regexp"
	"strings"
	"time"
)

type DataType int

const (
	TypeNull      DataType = iota
	TypeInteger
	TypeBigInt
	TypeFloat
	TypeDouble
	TypeText
	TypeVarChar
	TypeBoolean
	TypeTimestamp
)

func (dt DataType) String() string {
	switch dt {
	case TypeNull:
		return "NULL"
	case TypeInteger:
		return "INTEGER"
	case TypeBigInt:
		return "BIGINT"
	case TypeFloat:
		return "FLOAT"
	case TypeDouble:
		return "DOUBLE"
	case TypeText:
		return "TEXT"
	case TypeVarChar:
		return "VARCHAR"
	case TypeBoolean:
		return "BOOLEAN"
	case TypeTimestamp:
		return "TIMESTAMP"
	default:
		return "UNKNOWN"
	}
}

// Value is a typed SQL value.
type Value struct {
	Type     DataType
	IsNull   bool
	IntVal   int64
	FloatVal float64
	StrVal   string
	BoolVal  bool
	TimeVal  time.Time
}

var Null = Value{Type: TypeNull, IsNull: true}

func IntValue(v int64) Value      { return Value{Type: TypeInteger, IntVal: v} }
func BigIntValue(v int64) Value   { return Value{Type: TypeBigInt, IntVal: v} }
func FloatValue(v float64) Value  { return Value{Type: TypeFloat, FloatVal: v} }
func DoubleValue(v float64) Value { return Value{Type: TypeDouble, FloatVal: v} }
func TextValue(s string) Value    { return Value{Type: TypeText, StrVal: s} }
func VarCharValue(s string) Value { return Value{Type: TypeVarChar, StrVal: s} }
func BoolValue(b bool) Value      { return Value{Type: TypeBoolean, BoolVal: b} }
func TimeValue(t time.Time) Value { return Value{Type: TypeTimestamp, TimeVal: t} }

// NativeValue returns the Go-native representation for JSON serialization.
func (v Value) NativeValue() interface{} {
	if v.IsNull {
		return nil
	}
	switch v.Type {
	case TypeInteger, TypeBigInt:
		return v.IntVal
	case TypeFloat, TypeDouble:
		return v.FloatVal
	case TypeText, TypeVarChar:
		return v.StrVal
	case TypeBoolean:
		return v.BoolVal
	case TypeTimestamp:
		return v.TimeVal.UTC().Format(time.RFC3339Nano)
	default:
		return nil
	}
}

func CoerceValue(raw interface{}, dt DataType) (Value, error) {
	if raw == nil {
		return Null, nil
	}
	switch dt {
	case TypeInteger, TypeBigInt:
		switch v := raw.(type) {
		case int64:
			return Value{Type: dt, IntVal: v}, nil
		case int:
			return Value{Type: dt, IntVal: int64(v)}, nil
		case float64:
			return Value{Type: dt, IntVal: int64(v)}, nil
		case string:
			var i int64
			_, err := fmt.Sscanf(v, "%d", &i)
			if err != nil {
				return Null, fmt.Errorf("cannot coerce %q to integer", v)
			}
			return Value{Type: dt, IntVal: i}, nil
		}
	case TypeFloat, TypeDouble:
		switch v := raw.(type) {
		case float64:
			return Value{Type: dt, FloatVal: v}, nil
		case int64:
			return Value{Type: dt, FloatVal: float64(v)}, nil
		case string:
			var f float64
			_, err := fmt.Sscanf(v, "%f", &f)
			if err != nil {
				return Null, fmt.Errorf("cannot coerce %q to float", v)
			}
			return Value{Type: dt, FloatVal: f}, nil
		}
	case TypeText, TypeVarChar:
		switch v := raw.(type) {
		case string:
			return Value{Type: dt, StrVal: v}, nil
		case []byte:
			return Value{Type: dt, StrVal: string(v)}, nil
		default:
			return Value{Type: dt, StrVal: fmt.Sprintf("%v", v)}, nil
		}
	case TypeBoolean:
		switch v := raw.(type) {
		case bool:
			return Value{Type: dt, BoolVal: v}, nil
		case int64:
			return Value{Type: dt, BoolVal: v != 0}, nil
		case string:
			lower := strings.ToLower(v)
			return Value{Type: dt, BoolVal: lower == "true" || lower == "1" || lower == "yes"}, nil
		}
	case TypeTimestamp:
		switch v := raw.(type) {
		case time.Time:
			return Value{Type: dt, TimeVal: v}, nil
		case string:
			t, err := time.Parse(time.RFC3339, v)
			if err != nil {
				return Null, fmt.Errorf("cannot parse timestamp %q", v)
			}
			return Value{Type: dt, TimeVal: t}, nil
		case int64:
			return Value{Type: dt, TimeVal: time.Unix(0, v)}, nil
		}
	}
	return Null, fmt.Errorf("cannot coerce %T to %s", raw, dt)
}

// CompareValues returns -1, 0, or 1. Both values must be same type (or null).
func CompareValues(a, b Value) (int, error) {
	if a.IsNull && b.IsNull {
		return 0, nil
	}
	if a.IsNull {
		return -1, nil
	}
	if b.IsNull {
		return 1, nil
	}
	switch a.Type {
	case TypeInteger, TypeBigInt:
		if a.IntVal < b.IntVal {
			return -1, nil
		}
		if a.IntVal > b.IntVal {
			return 1, nil
		}
		return 0, nil
	case TypeFloat, TypeDouble:
		if a.FloatVal < b.FloatVal {
			return -1, nil
		}
		if a.FloatVal > b.FloatVal {
			return 1, nil
		}
		return 0, nil
	case TypeText, TypeVarChar:
		if a.StrVal < b.StrVal {
			return -1, nil
		}
		if a.StrVal > b.StrVal {
			return 1, nil
		}
		return 0, nil
	case TypeBoolean:
		if !a.BoolVal && b.BoolVal {
			return -1, nil
		}
		if a.BoolVal && !b.BoolVal {
			return 1, nil
		}
		return 0, nil
	case TypeTimestamp:
		if a.TimeVal.Before(b.TimeVal) {
			return -1, nil
		}
		if a.TimeVal.After(b.TimeVal) {
			return 1, nil
		}
		return 0, nil
	}
	return 0, fmt.Errorf("compare: unsupported type %s", a.Type)
}

func EqualValues(a, b Value) bool {
	c, err := CompareValues(a, b)
	return err == nil && c == 0
}

// LikeMatch implements SQL LIKE with % (any sequence) and _ (any single char).
func LikeMatch(value, pattern string) bool {
	var sb strings.Builder
	sb.WriteString("(?s)^")
	for _, ch := range pattern {
		switch ch {
		case '%':
			sb.WriteString(".*")
		case '_':
			sb.WriteString(".")
		default:
			sb.WriteString(regexp.QuoteMeta(string(ch)))
		}
	}
	sb.WriteString("$")
	matched, _ := regexp.MatchString(sb.String(), value)
	return matched
}

// Suppress unused import of math
var _ = math.MaxInt64
