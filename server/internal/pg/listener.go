// Package pg implements the PostgreSQL wire protocol listener for OxenDB.
// After startup psql, DBeaver, pgAdmin, psycopg2/3, asyncpg, and pgx
// can all connect natively to OxenDB on the configured port (default 5432).
package pg

import (
	"context"
	"log"
	"net"
	"sync"

	"oxendb/server/internal/config"
	sql "oxendb/server/internal/sql"
)

// Listener accepts incoming PostgreSQL wire protocol connections.
type Listener struct {
	cfg    *config.Config
	engine *sql.SQLEngine
	ln     net.Listener
	wg     sync.WaitGroup
}

// NewListener creates a new Listener.
func NewListener(cfg *config.Config, engine *sql.SQLEngine) *Listener {
	return &Listener{
		cfg:    cfg,
		engine: engine,
	}
}

// ListenAndServe starts accepting connections on the configured PG address.
// It blocks until ctx is cancelled or a fatal error occurs.
func (l *Listener) ListenAndServe(ctx context.Context) error {
	if !l.cfg.PG.Enabled {
		log.Println("pg: wire protocol disabled by config")
		<-ctx.Done()
		return nil
	}

	addr := l.cfg.PGAddr()
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	l.ln = ln
	log.Printf("pg: listening on %s (PostgreSQL wire protocol)", addr)

	// Stop accepting when ctx is cancelled
	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return nil
			default:
				log.Printf("pg: accept error: %v", err)
				continue
			}
		}

		l.wg.Add(1)
		go func(c net.Conn) {
			defer l.wg.Done()
			nc := newConn(c, l.engine, l.cfg)
			nc.Serve()
		}(conn)
	}
}

// Shutdown waits for all in-flight connections to finish.
func (l *Listener) Shutdown(ctx context.Context) error {
	if l.ln != nil {
		_ = l.ln.Close()
	}
	done := make(chan struct{})
	go func() {
		l.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
