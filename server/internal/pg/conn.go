package pg

import (
	"log"
	"net"

	"github.com/jackc/pgproto3/v2"

	"oxendb/server/internal/config"
	sql "oxendb/server/internal/sql"
)

// Conn represents a single client connection.
type Conn struct {
	net     net.Conn
	be      *pgproto3.Backend
	session *Session
	engine  *sql.SQLEngine
	cfg     *config.Config
}

// newConn creates a new Conn for the given network connection.
func newConn(netConn net.Conn, engine *sql.SQLEngine, cfg *config.Config) *Conn {
	be := pgproto3.NewBackend(pgproto3.NewChunkReader(netConn), netConn)
	return &Conn{
		net:     netConn,
		be:      be,
		session: NewSession("oxen", "oxendb", ""),
		engine:  engine,
		cfg:     cfg,
	}
}

// Serve runs the connection lifecycle: startup handshake then message loop.
func (c *Conn) Serve() {
	defer c.net.Close()

	// Startup/auth handshake
	if err := c.runStartup(); err != nil {
		log.Printf("pg: startup error from %s: %v", c.net.RemoteAddr(), err)
		return
	}

	// Main message loop
	for {
		msg, err := c.be.Receive()
		if err != nil {
			// Client disconnected
			return
		}

		switch m := msg.(type) {
		case *pgproto3.Query:
			c.handleSimpleQuery(m)

		case *pgproto3.Parse:
			c.handleParse(m)

		case *pgproto3.Bind:
			c.handleBind(m)

		case *pgproto3.Describe:
			c.handleDescribe(m)

		case *pgproto3.Execute:
			c.handleExecute(m)

		case *pgproto3.Sync:
			c.handleSync()

		case *pgproto3.Terminate:
			c.handleTerminate()
			return

		case *pgproto3.Flush:
			// Nothing to flush in our unbuffered backend

		case *pgproto3.CopyData:
			c.sendError("0A000", "ERROR", "COPY not supported")
			c.sendReadyForQuery()

		case *pgproto3.CopyDone:
			// ignore

		case *pgproto3.CopyFail:
			c.sendError("0A000", "ERROR", "COPY not supported")
			c.sendReadyForQuery()

		default:
			log.Printf("pg: unhandled message type %T", msg)
		}
	}
}

// sendError writes an ErrorResponse to the client.
func (c *Conn) sendError(sqlstate, severity, msg string) {
	_ = c.be.Send(&pgproto3.ErrorResponse{
		Severity: severity,
		Code:     sqlstate,
		Message:  msg,
	})
}

// sendReadyForQuery writes a ReadyForQuery message with the current transaction status.
func (c *Conn) sendReadyForQuery() {
	_ = c.be.Send(&pgproto3.ReadyForQuery{TxStatus: c.session.TxStatusByte()})
}
