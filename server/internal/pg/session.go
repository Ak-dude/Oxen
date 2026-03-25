package pg

import (
	"math/rand"

	"oxendb/server/internal/sql/result"
	"oxendb/server/internal/sql/txn"
	"oxendb/server/internal/sql/types"
)

// TxState tracks the transaction state of a connection.
type TxState int

const (
	TxIdle   TxState = iota // 'I' — no active transaction
	TxActive                // 'T' — inside an explicit transaction
	TxFailed                // 'E' — transaction aborted, waiting for ROLLBACK
)

// PreparedStatement holds a named prepared statement.
type PreparedStatement struct {
	Name  string
	SQL   string
	Plans []interface{} // []sql.Plan — stored as interface{} to avoid import cycle
}

// Portal holds a bound portal ready for execution.
type Portal struct {
	Name  string
	Stmt  *PreparedStatement
	Args  []types.Value
	Rows  *result.ResultSet
	Pos   int
}

// Session holds per-connection state.
type Session struct {
	User      string
	Database  string
	AppName   string
	TxState   TxState
	ActiveTxn *txn.Txn
	Prepared  map[string]*PreparedStatement
	Portals   map[string]*Portal
	PID       int32
	SecretKey int32
}

// NewSession creates a fresh session with the given parameters.
func NewSession(user, db, app string) *Session {
	return &Session{
		User:      user,
		Database:  db,
		AppName:   app,
		TxState:   TxIdle,
		Prepared:  make(map[string]*PreparedStatement),
		Portals:   make(map[string]*Portal),
		PID:       rand.Int31n(65535) + 1,
		SecretKey: rand.Int31(),
	}
}

// TxStatusByte returns the transaction status byte for ReadyForQuery messages.
// 'I' = idle, 'T' = in transaction, 'E' = error (failed transaction).
func (s *Session) TxStatusByte() byte {
	switch s.TxState {
	case TxActive:
		return 'T'
	case TxFailed:
		return 'E'
	default:
		return 'I'
	}
}
