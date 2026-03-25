package query

import (
	"errors"
	"fmt"

	"oxendb/server/internal/bridge"
)

// Result is the outcome of executing a statement.
type Result struct {
	// For GET: single key-value pair
	Key   []byte
	Value []byte

	// For SCAN / BATCH: multiple pairs
	Pairs [][2][]byte

	// Human-readable status message
	Message string
}

// Execute runs the given Statement against db and returns a Result.
func Execute(stmt Statement, db *bridge.DB) (*Result, error) {
	switch s := stmt.(type) {
	case *GetStmt:
		return executeGet(s, db)
	case *PutStmt:
		return executePut(s, db)
	case *DeleteStmt:
		return executeDelete(s, db)
	case *ScanStmt:
		return executeScan(s, db)
	case *BatchStmt:
		return executeBatch(s, db)
	default:
		return nil, fmt.Errorf("executor: unknown statement type %T", stmt)
	}
}

func executeGet(s *GetStmt, db *bridge.DB) (*Result, error) {
	val, err := db.Get(s.Key)
	if err != nil {
		if errors.Is(err, bridge.ErrNotFound) {
			return nil, &NotFoundError{Key: s.Key}
		}
		return nil, fmt.Errorf("executor GET: %w", err)
	}
	return &Result{Key: s.Key, Value: val, Message: "OK"}, nil
}

func executePut(s *PutStmt, db *bridge.DB) (*Result, error) {
	if err := db.Put(s.Key, s.Value); err != nil {
		return nil, fmt.Errorf("executor PUT: %w", err)
	}
	return &Result{Key: s.Key, Value: s.Value, Message: "OK"}, nil
}

func executeDelete(s *DeleteStmt, db *bridge.DB) (*Result, error) {
	if err := db.Delete(s.Key); err != nil {
		if errors.Is(err, bridge.ErrNotFound) {
			return nil, &NotFoundError{Key: s.Key}
		}
		return nil, fmt.Errorf("executor DELETE: %w", err)
	}
	return &Result{Key: s.Key, Message: "OK"}, nil
}

func executeScan(s *ScanStmt, db *bridge.DB) (*Result, error) {
	pairs, err := db.Scan(s.From, s.To)
	if err != nil {
		return nil, fmt.Errorf("executor SCAN: %w", err)
	}
	if s.Limit > 0 && len(pairs) > s.Limit {
		pairs = pairs[:s.Limit]
	}
	return &Result{Pairs: pairs, Message: fmt.Sprintf("%d pairs", len(pairs))}, nil
}

func executeBatch(s *BatchStmt, db *bridge.DB) (*Result, error) {
	var applied int
	for _, op := range s.Ops {
		var err error
		switch op.Op {
		case "PUT":
			err = db.Put(op.Key, op.Value)
		case "DELETE":
			err = db.Delete(op.Key)
		default:
			return nil, fmt.Errorf("executor BATCH: unknown op %q", op.Op)
		}
		if err != nil {
			return nil, fmt.Errorf("executor BATCH op %s key=%q: %w", op.Op, op.Key, err)
		}
		applied++
	}
	return &Result{Message: fmt.Sprintf("applied %d ops", applied)}, nil
}

// NotFoundError is returned when a key lookup yields no result.
type NotFoundError struct {
	Key []byte
}

func (e *NotFoundError) Error() string {
	return fmt.Sprintf("key not found: %q", e.Key)
}
