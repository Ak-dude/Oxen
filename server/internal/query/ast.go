// Package query implements the OxenQL query language: lexer, parser, AST, and executor.
//
// OxenQL Grammar (simplified BNF):
//
//	stmt       ::= get_stmt | put_stmt | delete_stmt | scan_stmt | batch_stmt
//	get_stmt   ::= "GET" key
//	put_stmt   ::= "PUT" key value
//	delete_stmt::= "DELETE" key
//	scan_stmt  ::= "SCAN" [FROM key] [TO key] [LIMIT number]
//	batch_stmt ::= "BATCH" "{" (put_stmt | delete_stmt)+ "}"
//	key        ::= quoted_string | bare_word
//	value      ::= quoted_string
package query

// Statement is the interface implemented by all AST nodes.
type Statement interface {
	stmtNode()
}

// GetStmt represents: GET <key>
type GetStmt struct {
	Key []byte
}

func (*GetStmt) stmtNode() {}

// PutStmt represents: PUT <key> <value>
type PutStmt struct {
	Key   []byte
	Value []byte
}

func (*PutStmt) stmtNode() {}

// DeleteStmt represents: DELETE <key>
type DeleteStmt struct {
	Key []byte
}

func (*DeleteStmt) stmtNode() {}

// ScanStmt represents: SCAN [FROM <key>] [TO <key>] [LIMIT <n>]
type ScanStmt struct {
	From  []byte // nil = open lower bound
	To    []byte // nil = open upper bound
	Limit int    // 0 = no limit
}

func (*ScanStmt) stmtNode() {}

// BatchOp is one operation inside a BATCH block.
type BatchOp struct {
	// Op is either "PUT" or "DELETE"
	Op    string
	Key   []byte
	Value []byte // empty for DELETE
}

// BatchStmt represents: BATCH { <put_or_delete>+ }
type BatchStmt struct {
	Ops []BatchOp
}

func (*BatchStmt) stmtNode() {}
