// Package sql is the OxenDB SQL engine: parser, planner, and executor.
// It speaks the PostgreSQL wire protocol dialect via pg_query_go.
package sql

import (
	"context"
	"strings"

	"oxendb/server/internal/bridge"
	"oxendb/server/internal/sql/catalog"
	"oxendb/server/internal/sql/result"
	"oxendb/server/internal/sql/txn"
	"oxendb/server/internal/sql/types"
)

// SQLEngine is the top-level SQL engine entry point.
type SQLEngine struct {
	db      *bridge.DB
	cat     *catalog.Catalog
	planner *Planner
}

// NewSQLEngine creates a new SQLEngine backed by the given database.
func NewSQLEngine(db *bridge.DB) *SQLEngine {
	return &SQLEngine{
		db:      db,
		cat:     catalog.New(db),
		planner: &Planner{},
	}
}

// Execute parses, plans, and executes a SQL string.
// sess is the current transaction (may be nil for auto-commit).
// args are $1/$2/... parameters.
// Returns one ResultSet per statement in the SQL string.
func (e *SQLEngine) Execute(ctx context.Context, sql string, t *txn.Txn, args []types.Value) ([]*result.ResultSet, error) {
	// Fast path: handle simple system queries before full parsing
	trimmed := strings.TrimSpace(sql)
	trimmedLower := strings.ToLower(trimmed)

	// Strip trailing semicolons for comparison
	cleaned := strings.TrimRight(trimmedLower, "; \t\n")

	switch cleaned {
	case "select 1":
		return []*result.ResultSet{oneIntResult("?column?", 1)}, nil
	}

	// Parse
	plans, err := e.planner.Parse(sql)
	if err != nil {
		return nil, err
	}

	results := make([]*result.ResultSet, 0, len(plans))
	for _, plan := range plans {
		rs, err := e.execPlan(ctx, plan, t, args)
		if err != nil {
			return nil, err
		}
		results = append(results, rs)
	}
	return results, nil
}

// ParseOnly parses SQL and returns plans without executing them.
// Used by the extended query protocol (Parse message).
func (e *SQLEngine) ParseOnly(sql string) ([]Plan, error) {
	return e.planner.Parse(sql)
}

// ExecPlanDirect executes a single pre-parsed plan.
func (e *SQLEngine) ExecPlanDirect(ctx context.Context, plan Plan, t *txn.Txn, args []types.Value) (*result.ResultSet, error) {
	return e.execPlan(ctx, plan, t, args)
}

// NewTxn begins a new buffered transaction.
func (e *SQLEngine) NewTxn() *txn.Txn {
	return txn.Begin(e.db)
}

// Catalog returns the engine's catalog (for system catalog queries).
func (e *SQLEngine) Catalog() *catalog.Catalog {
	return e.cat
}

// ---- Helpers ----

func oneIntResult(colName string, val int64) *result.ResultSet {
	return &result.ResultSet{
		Columns: []result.ColMeta{{Name: colName, DataType: types.TypeInteger}},
		Rows:    []result.Row{{types.IntValue(val)}},
		Tag:     "SELECT 1",
	}
}
