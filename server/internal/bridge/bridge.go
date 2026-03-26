// Package bridge provides a cgo bridge to the Rust OxenDB storage engine.
//
// Build requirements:
//   - The Rust shared library must be compiled first:
//       cd core && cargo build --release
//   - The resulting liboxendb_core.dylib (macOS) or liboxendb_core.so (Linux)
//     must be present in core/target/release/ relative to the repo root.
//
// The cgo LDFLAGS below assume the library has been built and the working
// directory is the repository root. Adjust the path if your layout differs.

//go:build cgo

package bridge

/*
#cgo LDFLAGS: -L${SRCDIR}/../../../core/target/release -loxendb_core
#cgo CFLAGS: -I${SRCDIR}
#include "oxendb.h"
#include <stdlib.h>
*/
import "C"

import (
	"errors"
	"fmt"
	"unsafe"
)

// ErrNotFound is returned when a requested key does not exist.
var ErrNotFound = errors.New("key not found")

// ErrEngineClosed is returned after the engine has been closed.
var ErrEngineClosed = errors.New("engine closed")

// DB is a handle to the Rust OxenDB storage engine.
type DB struct {
	handle *C.OxenHandle
}

// Open opens (or creates) a database at the given directory path.
func Open(dataDir string) (*DB, error) {
	cDir := C.CString(dataDir)
	defer C.free(unsafe.Pointer(cDir))

	var handle *C.OxenHandle
	rc := C.oxen_open(cDir, &handle)
	if err := codeToError(rc); err != nil {
		return nil, fmt.Errorf("bridge.Open %q: %w", dataDir, err)
	}
	return &DB{handle: handle}, nil
}

// Close closes the database and frees all resources.
// The DB must not be used after Close returns.
func (db *DB) Close() error {
	if db.handle == nil {
		return nil
	}
	rc := C.oxen_close(db.handle)
	db.handle = nil
	return codeToError(rc)
}

// Put stores key → value.
func (db *DB) Put(key, value []byte) error {
	if db.handle == nil {
		return ErrEngineClosed
	}
	kp, kl := bytesPtr(key)
	vp, vl := bytesPtr(value)
	rc := C.oxen_put(db.handle, kp, kl, vp, vl)
	return codeToError(rc)
}

// Get retrieves the value for key.
// Returns ErrNotFound if the key does not exist.
func (db *DB) Get(key []byte) ([]byte, error) {
	if db.handle == nil {
		return nil, ErrEngineClosed
	}
	kp, kl := bytesPtr(key)

	var outPtr *C.uint8_t
	var outLen C.size_t
	rc := C.oxen_get(db.handle, kp, kl, &outPtr, &outLen)
	if rc == C.OXEN_ERR_NOT_FOUND {
		return nil, ErrNotFound
	}
	if err := codeToError(rc); err != nil {
		return nil, err
	}
	defer C.oxen_free(outPtr, outLen)
	return C.GoBytes(unsafe.Pointer(outPtr), C.int(outLen)), nil
}

// Delete deletes key, writing a tombstone to the storage engine.
func (db *DB) Delete(key []byte) error {
	if db.handle == nil {
		return ErrEngineClosed
	}
	kp, kl := bytesPtr(key)
	rc := C.oxen_delete(db.handle, kp, kl)
	return codeToError(rc)
}

// Scan returns all key-value pairs in [start, end).
// Either bound may be nil for open-ended scans.
func (db *DB) Scan(start, end []byte) ([][2][]byte, error) {
	if db.handle == nil {
		return nil, ErrEngineClosed
	}

	var sp *C.uint8_t
	var sl C.size_t
	if start != nil {
		sp, sl = bytesPtr(start)
	}

	var ep *C.uint8_t
	var el C.size_t
	if end != nil {
		ep, el = bytesPtr(end)
	}

	var outKeys *C.OxenByteSlice
	var outVals *C.OxenByteSlice
	var outCount C.size_t

	rc := C.oxen_scan(db.handle, sp, sl, ep, el, &outKeys, &outVals, &outCount)
	if err := codeToError(rc); err != nil {
		return nil, err
	}

	count := int(outCount)
	pairs := make([][2][]byte, count)

	// Convert C arrays to Go slices.  We iterate by pointer arithmetic.
	keySlice := (*[1 << 28]C.OxenByteSlice)(unsafe.Pointer(outKeys))[:count:count]
	valSlice := (*[1 << 28]C.OxenByteSlice)(unsafe.Pointer(outVals))[:count:count]

	for i := 0; i < count; i++ {
		kData := C.GoBytes(unsafe.Pointer(keySlice[i].ptr), C.int(keySlice[i].len))
		vData := C.GoBytes(unsafe.Pointer(valSlice[i].ptr), C.int(valSlice[i].len))
		// Free individual key/value buffers
		C.oxen_free(keySlice[i].ptr, keySlice[i].len)
		C.oxen_free(valSlice[i].ptr, valSlice[i].len)
		pairs[i] = [2][]byte{kData, vData}
	}

	// Free the outer arrays (individual buffers already freed above)
	C.oxen_free_scan(outKeys, outVals, outCount)

	return pairs, nil
}

// Compact triggers a synchronous compaction round.
func (db *DB) Compact() error {
	if db.handle == nil {
		return ErrEngineClosed
	}
	rc := C.oxen_compact(db.handle)
	return codeToError(rc)
}

// PutBatch writes multiple key-value pairs atomically with a single WAL fsync.
// This is the highest-throughput write path — critical for bulk INSERT performance.
// Returns on the first error encountered.
func (db *DB) PutBatch(pairs [][2][]byte) error {
	if db.handle == nil {
		return ErrEngineClosed
	}
	n := len(pairs)
	if n == 0 {
		return nil
	}

	// Build C-compatible pointer arrays on the Go heap.
	// We keep the Go slices alive for the duration of the C call.
	keyPtrs := make([]*C.uint8_t, n)
	keyLens := make([]C.size_t, n)
	valPtrs := make([]*C.uint8_t, n)
	valLens := make([]C.size_t, n)

	for i, pair := range pairs {
		kp, kl := bytesPtr(pair[0])
		vp, vl := bytesPtr(pair[1])
		keyPtrs[i] = kp
		keyLens[i] = kl
		valPtrs[i] = vp
		valLens[i] = vl
	}

	rc := C.oxen_put_batch(
		db.handle,
		(**C.uint8_t)(unsafe.Pointer(&keyPtrs[0])),
		(*C.size_t)(unsafe.Pointer(&keyLens[0])),
		(**C.uint8_t)(unsafe.Pointer(&valPtrs[0])),
		(*C.size_t)(unsafe.Pointer(&valLens[0])),
		C.size_t(n),
	)
	return codeToError(rc)
}

// ---- helpers ----

// bytesPtr returns a C pointer and length for a Go byte slice.
// Returns nil, 0 for an empty or nil slice.
func bytesPtr(b []byte) (*C.uint8_t, C.size_t) {
	if len(b) == 0 {
		return nil, 0
	}
	return (*C.uint8_t)(unsafe.Pointer(&b[0])), C.size_t(len(b))
}

// codeToError maps a C int return code to a Go error.
func codeToError(rc C.int) error {
	switch rc {
	case C.OXEN_OK:
		return nil
	case C.OXEN_ERR_NOT_FOUND:
		return ErrNotFound
	case C.OXEN_ERR_IO:
		return errors.New("oxendb: I/O error")
	case C.OXEN_ERR_CORRUPTION:
		return errors.New("oxendb: data corruption detected")
	case C.OXEN_ERR_INVALID_ARG:
		return errors.New("oxendb: invalid argument")
	case C.OXEN_ERR_CLOSED:
		return ErrEngineClosed
	case C.OXEN_ERR_NULL_PTR:
		return errors.New("oxendb: null pointer")
	default:
		return fmt.Errorf("oxendb: unknown error code %d", rc)
	}
}
