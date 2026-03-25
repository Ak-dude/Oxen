// Package txn provides a buffered write transaction with an in-memory overlay.
// Reads check the overlay first before hitting the underlying bridge.DB.
// Commit flushes all buffered writes to the storage engine.
// Rollback discards all buffered changes.
package txn

import (
	"sort"

	"oxendb/server/internal/bridge"
)

// Txn is a buffered write transaction with in-memory overlay.
type Txn struct {
	db      *bridge.DB
	puts    map[string][]byte
	deletes map[string]bool
}

// Begin starts a new transaction against the given database.
func Begin(db *bridge.DB) *Txn {
	return &Txn{
		db:      db,
		puts:    make(map[string][]byte),
		deletes: make(map[string]bool),
	}
}

// Put buffers a key-value write into the overlay.
func (t *Txn) Put(key, value []byte) error {
	k := string(key)
	t.puts[k] = append([]byte(nil), value...) // defensive copy
	delete(t.deletes, k)
	return nil
}

// Get reads a value, checking the overlay first then falling back to bridge.DB.
func (t *Txn) Get(key []byte) ([]byte, error) {
	k := string(key)
	if t.deletes[k] {
		return nil, bridge.ErrNotFound
	}
	if v, ok := t.puts[k]; ok {
		out := make([]byte, len(v))
		copy(out, v)
		return out, nil
	}
	return t.db.Get(key)
}

// Delete marks a key as deleted in the overlay.
func (t *Txn) Delete(key []byte) error {
	k := string(key)
	delete(t.puts, k)
	t.deletes[k] = true
	return nil
}

// Scan returns all key-value pairs in [start, end) with overlay applied.
// It merges bridge results with the overlay, applying deletes and puts.
func (t *Txn) Scan(start, end []byte) ([][2][]byte, error) {
	// Fetch from underlying storage
	basePairs, err := t.db.Scan(start, end)
	if err != nil {
		return nil, err
	}

	// Build a merged map from base results
	merged := make(map[string][]byte, len(basePairs))
	order := make([]string, 0, len(basePairs))
	for _, pair := range basePairs {
		k := string(pair[0])
		merged[k] = pair[1]
		order = append(order, k)
	}

	// Apply overlay puts — add new keys or overwrite existing
	for k, v := range t.puts {
		kb := []byte(k)
		if inRange(kb, start, end) {
			if _, exists := merged[k]; !exists {
				order = append(order, k)
			}
			merged[k] = v
		}
	}

	// Apply overlay deletes — remove keys
	for k := range t.deletes {
		delete(merged, k)
	}

	// Build sorted result
	sort.Strings(order)

	result := make([][2][]byte, 0, len(merged))
	seen := make(map[string]bool)
	for _, k := range order {
		if seen[k] {
			continue
		}
		seen[k] = true
		v, ok := merged[k]
		if !ok {
			continue
		}
		kCopy := []byte(k)
		vCopy := make([]byte, len(v))
		copy(vCopy, v)
		result = append(result, [2][]byte{kCopy, vCopy})
	}
	return result, nil
}

// Commit flushes all buffered writes to the underlying storage engine.
func (t *Txn) Commit() error {
	for k, v := range t.puts {
		if err := t.db.Put([]byte(k), v); err != nil {
			return err
		}
	}
	for k := range t.deletes {
		if err := t.db.Delete([]byte(k)); err != nil && err != bridge.ErrNotFound {
			return err
		}
	}
	// Clear buffers
	t.puts = make(map[string][]byte)
	t.deletes = make(map[string]bool)
	return nil
}

// Rollback discards all buffered changes without writing to storage.
func (t *Txn) Rollback() {
	t.puts = make(map[string][]byte)
	t.deletes = make(map[string]bool)
}

// inRange checks whether key falls in [start, end).
// nil start means no lower bound; nil end means no upper bound.
func inRange(key, start, end []byte) bool {
	if start != nil && string(key) < string(start) {
		return false
	}
	if end != nil && string(key) >= string(end) {
		return false
	}
	return true
}
