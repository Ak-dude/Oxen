// Package result defines the types returned by the SQL executor.
package result

import "oxendb/server/internal/sql/types"

// ColMeta holds metadata for a result column.
type ColMeta struct {
	Name     string
	DataType types.DataType
	Nullable bool
	TableID  uint32
	ColIdx   int
}

// Row is a single result row, one Value per column.
type Row []types.Value

// ResultSet is the full output of a SQL statement execution.
type ResultSet struct {
	// Columns describes the result schema (empty for non-SELECT statements).
	Columns []ColMeta

	// Rows contains the result data rows (empty for non-SELECT statements).
	Rows []Row

	// Tag is the command completion tag, e.g., "SELECT 5", "INSERT 0 1", "CREATE TABLE".
	Tag string

	// RowsAffected is the number of rows affected by DML statements.
	RowsAffected int64

	// LastInsertID is the last auto-generated primary key (for INSERT with SERIAL/auto-increment).
	LastInsertID int64
}
