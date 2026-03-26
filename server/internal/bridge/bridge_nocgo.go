// Package bridge provides a stub implementation used when cgo is disabled.
// When building with CGO_ENABLED=1 (the default), bridge.go is compiled instead.

//go:build !cgo

package bridge

import "errors"

// ErrNotFound is returned when a requested key does not exist.
var ErrNotFound = errors.New("key not found")

// ErrEngineClosed is returned after the engine has been closed.
var ErrEngineClosed = errors.New("engine closed")

// ErrCGODisabled is returned from all operations when cgo is not enabled.
var ErrCGODisabled = errors.New("oxendb: cgo is required to connect to the Rust storage engine; rebuild with CGO_ENABLED=1")

// DB is a no-op handle used when cgo is not enabled.
type DB struct{}

// Open always returns an error when cgo is disabled.
func Open(_ string) (*DB, error) {
	return nil, ErrCGODisabled
}

func (db *DB) Close() error                        { return ErrCGODisabled }
func (db *DB) Put(_, _ []byte) error               { return ErrCGODisabled }
func (db *DB) Get(_ []byte) ([]byte, error)        { return nil, ErrCGODisabled }
func (db *DB) Delete(_ []byte) error               { return ErrCGODisabled }
func (db *DB) Scan(_, _ []byte) ([][2][]byte, error) { return nil, ErrCGODisabled }
func (db *DB) Compact() error                      { return ErrCGODisabled }
