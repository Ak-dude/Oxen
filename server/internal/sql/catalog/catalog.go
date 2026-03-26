package catalog

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"oxendb/server/internal/bridge"
)

const (
	prefixCatalog = byte(0x01)
	subTable      = byte(0x01)
	subIndex      = byte(0x02)
	subTableIDSeq = byte(0x03)
)

var ErrTableNotFound = errors.New("table not found")
var ErrTableExists   = errors.New("table already exists")
var ErrIndexNotFound = errors.New("index not found")

type Catalog struct {
	db     *bridge.DB
	mu     sync.RWMutex
	nextID uint32 // in-memory counter, persisted to KV
}

func New(db *bridge.DB) *Catalog {
	c := &Catalog{db: db}
	c.nextID = c.loadNextTableID()
	return c
}

func catalogTableKey(name string) []byte {
	key := make([]byte, 2+len(name))
	key[0] = prefixCatalog
	key[1] = subTable
	copy(key[2:], name)
	return key
}

func catalogIndexKey(tableName, indexName string) []byte {
	key := make([]byte, 3+len(tableName)+len(indexName))
	key[0] = prefixCatalog
	key[1] = subIndex
	copy(key[2:], tableName)
	key[2+len(tableName)] = '/'
	copy(key[3+len(tableName):], indexName)
	return key
}

func catalogTableIDSeqKey() []byte { return []byte{prefixCatalog, subTableIDSeq} }

func (c *Catalog) loadNextTableID() uint32 {
	data, err := c.db.Get(catalogTableIDSeqKey())
	if err != nil || len(data) < 4 {
		return 1
	}
	return binary.BigEndian.Uint32(data)
}

func (c *Catalog) AllocTableID() uint32 {
	id := atomic.AddUint32(&c.nextID, 1)
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, id)
	_ = c.db.Put(catalogTableIDSeqKey(), buf)
	return id - 1
}

func (c *Catalog) CreateTable(td *TableDescriptor) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := catalogTableKey(td.Name)
	existing, err := c.db.Get(key)
	if err == nil && len(existing) > 0 {
		return fmt.Errorf("%w: %s", ErrTableExists, td.Name)
	}
	td.TableID = c.AllocTableID()
	td.CreatedAt = time.Now().UnixNano()
	data, err := td.Marshal()
	if err != nil {
		return err
	}
	return c.db.Put(key, data)
}

func (c *Catalog) GetTable(name string) (*TableDescriptor, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	data, err := c.db.Get(catalogTableKey(name))
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrTableNotFound, name)
	}
	var td TableDescriptor
	if err := td.Unmarshal(data); err != nil {
		return nil, err
	}
	return &td, nil
}

func (c *Catalog) DropTable(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := catalogTableKey(name)
	_, err := c.db.Get(key)
	if err != nil {
		return fmt.Errorf("%w: %s", ErrTableNotFound, name)
	}
	return c.db.Delete(key)
}

func (c *Catalog) ListTables() ([]*TableDescriptor, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	prefix := []byte{prefixCatalog, subTable}
	end    := []byte{prefixCatalog, subTable + 1}
	pairs, err := c.db.Scan(prefix, end)
	if err != nil {
		return nil, err
	}
	var tables []*TableDescriptor
	for _, pair := range pairs {
		var td TableDescriptor
		if err := td.Unmarshal(pair[1]); err != nil {
			continue
		}
		tables = append(tables, &td)
	}
	return tables, nil
}

func (c *Catalog) CreateIndex(id *IndexDescriptor) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	data, err := id.Marshal()
	if err != nil {
		return err
	}
	return c.db.Put(catalogIndexKey(id.TableName, id.Name), data)
}

func (c *Catalog) GetIndex(tableName, indexName string) (*IndexDescriptor, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	data, err := c.db.Get(catalogIndexKey(tableName, indexName))
	if err != nil {
		return nil, fmt.Errorf("%w: %s/%s", ErrIndexNotFound, tableName, indexName)
	}
	var id IndexDescriptor
	if err := id.Unmarshal(data); err != nil {
		return nil, err
	}
	return &id, nil
}

func (c *Catalog) DropIndex(tableName, indexName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := catalogIndexKey(tableName, indexName)
	_, err := c.db.Get(key)
	if err != nil {
		return fmt.Errorf("%w: %s/%s", ErrIndexNotFound, tableName, indexName)
	}
	return c.db.Delete(key)
}

func (c *Catalog) ListIndexes(tableName string) ([]*IndexDescriptor, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	prefix := append([]byte{prefixCatalog, subIndex}, []byte(tableName+"/")...)
	end    := append([]byte{prefixCatalog, subIndex}, []byte(tableName+"0")...)
	pairs, err := c.db.Scan(prefix, end)
	if err != nil {
		return nil, err
	}
	var indexes []*IndexDescriptor
	for _, pair := range pairs {
		var id IndexDescriptor
		if err := id.Unmarshal(pair[1]); err != nil {
			continue
		}
		indexes = append(indexes, &id)
	}
	return indexes, nil
}
